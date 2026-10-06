// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Deadlines for storage reads made while answering a client request.
//!
//! A slow storage engine must not hold a request past the point where the
//! client gives up on it: the client reconnects and sends the request again,
//! while the abandoned one keeps running. Each client abandons a request at
//! its own read deadline:
//!
//! | Client     | ListOffsets | Fetch                          |
//! |------------|-------------|--------------------------------|
//! | Java       | 30s         | 30s (`max_wait` not added)     |
//! | librdkafka | 60s         | 60s + `fetch.wait.max.ms`      |
//! | franz-go   | 10s         | 10s + `max_wait`               |
//!
//! The deadlines here sit under all three with room for the response.

use std::{future::Future, sync::LazyLock};

use opentelemetry::{KeyValue, metrics::Counter};
use tokio::time::{Duration, Instant, timeout_at};

use crate::METER;

/// How long ListOffsets may spend in storage, under franz-go's flat 10s.
///
/// A partition still unread at this deadline answers `REQUEST_TIMED_OUT`
/// with offset and timestamp -1. Kafka 4.0 sends the same answer for a
/// partition whose remote-storage lookup expires in its list-offsets
/// purgatory ([DelayedRemoteListOffsets.scala#L46], [#L144-L149]), so
/// clients already retry it.
///
/// [DelayedRemoteListOffsets.scala#L46]: https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/DelayedRemoteListOffsets.scala#L46
/// [#L144-L149]: https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/DelayedRemoteListOffsets.scala#L144-L149
pub(crate) const LIST_OFFSETS_READ_DEADLINE: Duration = Duration::from_secs(5);

static READ_DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_read_deadline_exceeded")
        .with_description("Storage reads abandoned at the request deadline")
        .build()
});

/// Runs `read` until `deadline`, returning `None` if it did not finish.
///
/// A read is not started at all once the deadline has passed:
/// [`tokio::time::timeout_at`] polls its future once before looking at the
/// clock, which would send a storage request only to drop it.
///
/// Dropping the read cancels nisshi's side of it. Work the storage engine
/// runs on its own tasks, or a statement already sent to a database server,
/// carries on until it finishes.
pub(crate) async fn within<F>(
    operation: &'static str,
    deadline: Instant,
    read: F,
) -> Option<F::Output>
where
    F: Future,
{
    within_counting(&READ_DEADLINE_EXCEEDED, operation, deadline, read).await
}

/// Runs [`within`], counting each abandoned read in `exceeded`.
async fn within_counting<F>(
    exceeded: &Counter<u64>,
    operation: &'static str,
    deadline: Instant,
    read: F,
) -> Option<F::Output>
where
    F: Future,
{
    let outcome = if Instant::now() >= deadline {
        None
    } else {
        timeout_at(deadline, read).await.ok()
    };

    if outcome.is_none() {
        exceeded.add(1, &[KeyValue::new("operation", operation)]);
    }

    outcome
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn finishes_before_the_deadline() {
        let deadline = Instant::now() + Duration::from_secs(1);
        assert_eq!(Some(7), within("test", deadline, async { 7 }).await);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_the_deadline() {
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_secs(1);

        assert_eq!(None, within("test", deadline, pending::<()>()).await);
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// Once the deadline has passed, the read is never polled.
    #[tokio::test(start_paused = true)]
    async fn does_not_start_after_the_deadline() {
        let deadline = Instant::now();
        tokio::time::advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert_eq!(None, within("test", deadline, read).await);
        assert!(!polled.load(Ordering::SeqCst));
    }

    /// Sums the `nisshi_storage_read_deadline_exceeded` counter for `operation`.
    fn exceeded(exporter: &InMemoryMetricExporter, operation: &str) -> u64 {
        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == "nisshi_storage_read_deadline_exceeded")
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(
                    sum.data_points()
                        .filter(|point| {
                            point.attributes().any(|attribute| {
                                attribute.key.as_str() == "operation"
                                    && attribute.value.as_str() == operation
                            })
                        })
                        .map(SumDataPoint::value)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum()
    }

    /// Each abandoned read adds one to the counter, and a read that finishes
    /// in time adds nothing.
    #[tokio::test(start_paused = true)]
    async fn counts_each_abandoned_read() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("nisshi_storage_read_deadline_exceeded")
            .build();

        let deadline = Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Some(7),
            within_counting(&counter, "on_time", deadline, async { 7 }).await
        );
        assert_eq!(
            None,
            within_counting(&counter, "stalled", deadline, pending::<()>()).await
        );
        assert_eq!(
            None,
            within_counting(&counter, "stalled", deadline, async {}).await
        );

        provider.force_flush().expect("flush");

        assert_eq!(0, exceeded(&exporter, "on_time"));
        assert_eq!(2, exceeded(&exporter, "stalled"));
    }
}
