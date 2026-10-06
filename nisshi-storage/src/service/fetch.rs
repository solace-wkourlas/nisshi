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

use std::{cmp::min, future::Future, sync::LazyLock};

use nisshi_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel, RequestInput,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::Service;
use tokio::time::{Duration, Instant, sleep, timeout_at};
use tracing::{debug, error, instrument, warn};

use crate::{Error, METER, Result, Storage, Topition};

/// How long past the client's `max_wait` a Fetch may spend reading storage.
///
/// A client abandons a Fetch at its own read deadline, then sends it again
/// while the abandoned one keeps running:
///
/// | Client     | Fetch read deadline            |
/// |------------|--------------------------------|
/// | Java       | 30s (`max_wait` not added)     |
/// | librdkafka | 60s + `fetch.wait.max.ms`      |
/// | franz-go   | 10s + `max_wait`               |
///
/// `max_wait` plus this sits under all three with room for the response,
/// for any `max_wait` under Java's 30s less this.
const READ_DEADLINE_OVERHEAD: Duration = Duration::from_secs(5);

/// The smallest part of a partition's share of the read deadline that its
/// storage budget leaves unused.
///
/// An engine checks its budget only between records or object reads, and
/// it starts its clock after it gets a connection or a permit. A read that
/// spends its whole budget therefore returns a little after the budget
/// ends. The margin lets that read return what it assembled before the
/// broker abandons it.
const MIN_BUDGET_MARGIN: Duration = Duration::from_millis(50);

static READ_DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_read_deadline_exceeded")
        .with_description("Storage reads abandoned at the request deadline")
        .build()
});

/// Why [`before`] did not return the output of its read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Missed {
    /// The deadline had passed before the read started, so the read was
    /// never polled.
    NotStarted,

    /// The read started and did not finish by the deadline.
    Expired,
}

/// Runs `read` until `deadline`.
///
/// A read is not started at all once the deadline has passed:
/// [`tokio::time::timeout_at`] polls its future once before looking at the
/// clock, which would send a storage request only to drop it.
///
/// Dropping the read cancels nisshi's side of it, and only that side.
///
/// - Postgres: the connection goes back to the pool while its statement
///   still runs, and the next checkout of that connection waits for the
///   statement to finish.
/// - libSQL: a local statement runs synchronously inside the read's
///   `async fn`, so the deadline interrupts only a wait for a permit or a
///   connection, never a slow statement.
/// - SlateDB: the engine ignores its budget, so a slow read is abandoned
///   with nothing kept.
/// - Object store: work the engine runs on its own tasks carries on until
///   it finishes.
async fn before<F>(deadline: Instant, read: F) -> Result<F::Output, Missed>
where
    F: Future,
{
    if Instant::now() >= deadline {
        Err(Missed::NotStarted)
    } else {
        timeout_at(deadline, read)
            .await
            .map_err(|_| Missed::Expired)
    }
}

/// Records a read of `tp` that [`before`] abandoned at `stage`, in the
/// `nisshi_storage_read_deadline_exceeded` counter and the log.
///
/// A read that was never started logs at debug: once a slow backend has
/// spent the read deadline, every partition after it is not started, and a
/// warning for each of them would blame partitions that were never read.
fn missed(tp: &Topition, stage: &'static str, missed: Missed, started_at: Instant) {
    let stage = match missed {
        Missed::NotStarted => "not_started",
        Missed::Expired => stage,
    };

    READ_DEADLINE_EXCEEDED.add(
        1,
        &[
            KeyValue::new("operation", "fetch"),
            KeyValue::new("stage", stage),
        ],
    );

    match missed {
        Missed::NotStarted => {
            debug!(?tp, stage, elapsed = ?started_at.elapsed(), "fetch read not started")
        }
        Missed::Expired => {
            warn!(?tp, stage, elapsed = ?started_at.elapsed(), "fetch read deadline exceeded")
        }
    }
}

/// When the next partition's reads must finish, with `left` partitions,
/// this one included, still to read before `read_deadline`.
///
/// Each partition may spend half of the time left, and the last all of it,
/// so one partition that never answers delays the rest of the request by
/// that half, rather than holding it until the client gives up. The half is
/// taken of the time left now, so a partition that answers quickly leaves
/// its unused share to those after it.
fn partition_deadline(read_deadline: Instant, left: usize) -> Instant {
    if left <= 1 {
        read_deadline
    } else {
        let now = Instant::now();
        now + read_deadline.saturating_duration_since(now) / 2
    }
}

/// The deadlines of one Fetch request.
#[derive(Clone, Copy, Debug)]
struct Deadlines {
    /// The client's `max_wait`.
    max_wait: Duration,

    /// When the client's `max_wait` ends.
    client: Instant,

    /// When every storage read must have finished.
    read: Instant,
}

impl Deadlines {
    fn new(started_at: Instant, max_wait: Duration) -> Self {
        Self {
            max_wait,
            client: started_at + max_wait,
            read: started_at + max_wait + READ_DEADLINE_OVERHEAD,
        }
    }

    /// The storage budget of a partition whose reads start now and must
    /// finish by `cap`.
    ///
    /// The client's `max_wait` bounds the response: engines stop assembling
    /// batches at their budget and return what they have. A partition that
    /// starts with less than half of `max_wait` left, because one before it
    /// was slow, still gets that half, rather than a budget engines would
    /// read nothing with.
    ///
    /// The budget ends a quarter of the partition's share, and at least
    /// [`MIN_BUDGET_MARGIN`], before `cap`. A budget that ends at `cap`
    /// makes a read that uses all of it return after `cap`, so the broker
    /// abandons it and loses every batch it read.
    fn storage(&self, cap: Instant) -> Instant {
        let now = Instant::now();
        let share = cap.saturating_duration_since(now);
        let margin = (share / 4).max(MIN_BUDGET_MARGIN).min(share);

        self.client.max(now + self.max_wait / 2).min(cap - margin)
    }
}

/// A [`Service`] using its [`Storage`] taking [`FetchRequest`] returning [`FetchResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, FetchRequest,
///     create_topics_request::CreatableTopic,
///     fetch_request::{FetchPartition, FetchTopic},
/// };
/// use nisshi_storage::{CreateTopicsService, Error, FetchService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = CreateTopicsService {
///     storage: storage.clone(),
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let fetch = FetchService {
///     storage: storage.clone(),
/// };
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
///         FetchRequest::default()
///             .topics(Some(
///                 [FetchTopic::default()
///                     .topic(Some(name.into()))
///                     .partitions(Some(
///                         [FetchPartition::default().partition(partition)].into(),
///                     ))]
///                 .into(),
///             ))
///             .max_bytes(Some(0))
///             .max_wait_ms(5_000),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FetchService<G> {
    pub storage: G,
}

impl<G> ApiKey for FetchService<G> {
    const KEY: i16 = FetchRequest::KEY;
}

impl<G> FetchService<G>
where
    G: Storage,
{
    /// Reads one partition, with `left` partitions, this one included, still
    /// to read before the read deadline.
    ///
    /// A read that has not finished by this partition's share of the read
    /// deadline (see [`partition_deadline`]) is abandoned. Each read is
    /// given the time left of its storage budget (see [`Deadlines::storage`])
    /// and the reads stop once that is spent. What is left of the client's
    /// `max_wait` alone may have been spent by a slow partition earlier in
    /// the request, and engines read nothing with no budget.
    ///
    /// A partition whose read does not finish in time is answered with
    /// [`ErrorCode::None`] and whatever batches it had already read, so the
    /// client fetches it again on its next request. Its offsets are unknown,
    /// since reading them is what stalled; see [`Self::partition_data`].
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self,min_bytes,isolation,fetch_partition), fields(partition = fetch_partition.partition))]
    async fn fetch_partition(
        &self,
        deadlines: Deadlines,
        left: usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        let started_at = Instant::now();
        let cap = partition_deadline(deadlines.read, left);
        let budget = deadlines.storage(cap);

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();

        let mut offset = fetch_partition.fetch_offset;

        let mut stalled = false;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let fetched = match before(
                cap,
                self.storage.fetch(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    budget.saturating_duration_since(Instant::now()),
                ),
            )
            .await
            {
                Ok(fetched) => fetched,
                Err(reason) => {
                    missed(&tp, "read", reason, started_at);
                    stalled = true;
                    break;
                }
            };

            let mut fetched = fetched
                .inspect(|r| debug!(?tp, ?offset, ?r))
                .inspect_err(|error| error!(?tp, ?error))?;

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            debug!(?offset, ?fetched, max_bytes);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            }

            // the offset after the last one returned; a batch can have gaps
            // (compaction), so this is not the record count
            if let Some(latest) = fetched
                .iter()
                .map(|batch| batch.max_offset() + 1)
                .max()
                .inspect(|latest| debug!(latest))
            {
                offset = latest;
            }

            batches.append(&mut fetched);

            // The budget bounds the response: engines that assemble
            // batches from rows stop at the budget but return what they
            // have, so another round now would return one record per round
            // trip until max_bytes is spent
            if Instant::now() >= budget {
                debug!(?offset, elapsed = ?started_at.elapsed(), ?budget);
                break;
            }
        }

        // A read that stalled is not followed by another to the same
        // partition. A read that used its whole budget assembling batches
        // did not stall, so the offsets get a fresh share of the time left
        // rather than what may be none at all.
        let offset_stage = if stalled {
            None
        } else {
            match before(
                partition_deadline(deadlines.read, left),
                self.storage.offset_stage(&tp),
            )
            .await
            {
                Ok(offset_stage) => Some(offset_stage.inspect_err(|error| error!(?error, ?tp))?),
                Err(reason) => {
                    missed(&tp, "offset_stage", reason, started_at);
                    None
                }
            }
        };

        Ok(Self::partition_data(partition_index, offset_stage, batches))
            .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    /// The answer for one partition, with `offset_stage` if it was read in
    /// time.
    ///
    /// Without it, the high watermark and last stable offset are the offset
    /// after the last record returned, which they are at least, or `-1`
    /// (unknown) with no records, along with an unknown log start offset.
    /// Unlike Kafka, which answers a remote read that runs out of time with
    /// the offsets it already holds, nisshi has not read them: they come
    /// from the storage that stalled. Java ignores negative offsets and
    /// keeps the ones it had; librdkafka and franz-go report `-1` as the
    /// partition's watermarks until the next answer that has them.
    fn partition_data(
        partition_index: i32,
        offset_stage: Option<crate::OffsetStage>,
        batches: Vec<Batch>,
    ) -> PartitionData {
        let (high_watermark, last_stable, log_start) = offset_stage.map_or_else(
            || {
                let next = batches
                    .iter()
                    .map(|batch| batch.max_offset() + 1)
                    .max()
                    .unwrap_or(-1);
                (next, next, -1)
            },
            |offset_stage| {
                (
                    offset_stage.high_watermark(),
                    offset_stage.last_stable(),
                    offset_stage.log_start(),
                )
            },
        );

        PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(high_watermark)
            .last_stable_offset(Some(last_stable))
            .log_start_offset(Some(log_start))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            })
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(Some(
                                EpochEndOffset::default().epoch(-1).end_offset(-1),
                            ))
                            .current_leader(Some(
                                LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                            ))
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, min_bytes, isolation, fetch))]
    async fn fetch_topic(
        &self,
        deadlines: Deadlines,
        left: &mut usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        is_first_non_empty: &mut bool,
    ) -> Result<FetchableTopicResponse> {
        let metadata = self.storage.metadata(Some(&[fetch.into()])).await?;

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            ..
        }) = metadata.topics().first()
        {
            let mut partitions = Vec::new();

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                let partition_max_bytes = if *is_first_non_empty {
                    *max_bytes
                } else {
                    min(fetch_partition.partition_max_bytes as u32, *max_bytes)
                };

                let mut partition_bytes = partition_max_bytes;
                debug!(partition_bytes, is_first_non_empty);

                let partition = self
                    .fetch_partition(
                        deadlines,
                        *left,
                        min_bytes,
                        &mut partition_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                *left = left.saturating_sub(1);

                *is_first_non_empty = *is_first_non_empty
                    && partition
                        .records
                        .as_ref()
                        .is_some_and(|records| records.batches.is_empty());

                debug!(partition_bytes, is_first_non_empty);

                *max_bytes =
                    max_bytes.saturating_sub(partition_max_bytes.saturating_sub(partition_bytes));

                partitions.push(partition);
            }

            Ok(FetchableTopicResponse::default()
                .topic(fetch.topic.to_owned())
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            *left = left.saturating_sub(fetch.partitions.as_ref().map_or(0, Vec::len));
            self.unknown_topic_response(fetch)
        }
    }

    #[instrument(skip(self, isolation, topics))]
    pub(crate) async fn fetch(
        &self,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>> {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            // Instant rather than SystemTime: the deadlines below must not
            // move with the wall clock
            let started_at = Instant::now();
            let deadlines = Deadlines::new(started_at, max_wait);
            let mut responses = vec![];
            let mut iteration = 0;
            let mut bytes = 0;
            let mut is_first_non_empty = true;

            while !max_wait.saturating_sub(started_at.elapsed()).is_zero() && bytes <= min_bytes {
                debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()));

                responses.clear();

                // Every round reads every partition again
                let mut left = topics
                    .iter()
                    .map(|topic| topic.partitions.as_ref().map_or(0, Vec::len))
                    .sum::<usize>();

                let fetch_started_at = Instant::now();
                for fetch in topics.iter() {
                    let fetch_response = self
                        .fetch_topic(
                            deadlines,
                            &mut left,
                            min_bytes,
                            max_bytes,
                            isolation,
                            fetch,
                            &mut is_first_non_empty,
                        )
                        .await?;

                    responses.push(fetch_response);
                }

                bytes += u32::try_from(responses.byte_size())?;

                let remaining = max_wait.saturating_sub(started_at.elapsed());

                debug!(?iteration, ?max_wait, ?remaining, ?bytes, ?min_bytes);

                if bytes > min_bytes {
                    break;
                }

                {
                    let fetch_elapsed = fetch_started_at.elapsed();

                    // we have some data to return to the client,
                    // we haven't met the minimum size requirement,
                    // but we don't have enough (estimated) time remaining to do another round
                    if !responses.is_empty() && remaining < fetch_elapsed {
                        debug!(responses.len = responses.len(), ?remaining, ?fetch_elapsed);
                        break;
                    }
                }

                sleep(remaining / 2).await;

                iteration += 1;
            }

            Ok(responses)
        }
    }
}

impl<G, I> Service<I> for FetchService<G>
where
    G: Storage,
    I: Into<RequestInput<FetchRequest>> + Send + 'static,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let started_at = Instant::now();

        let input = input.into();

        let responses = Some(if let Some(topics) = input.request.topics {
            let isolation_level = input
                .request
                .isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms =
                u64::try_from(input.request.max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(input.request.min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes =
                input
                    .request
                    .max_bytes
                    .map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                        u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
                    })?;

            self.fetch(
                max_wait_ms,
                min_bytes,
                &mut max_bytes,
                isolation_level,
                topics.as_ref(),
            )
            .await?
        } else {
            vec![]
        });

        Ok(FetchResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .session_id(Some(0))
            .node_endpoints(Some([].into()))
            .responses(responses))
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }
}

trait ByteSize {
    fn byte_size(&self) -> u64;
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.iter().map(|item| item.byte_size()).sum()
    }
}

impl<T> ByteSize for Option<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.as_ref().map_or(0, |some| some.byte_size())
    }
}

impl ByteSize for Batch {
    fn byte_size(&self) -> u64 {
        self.record_data.len() as u64
    }
}

impl ByteSize for Frame {
    fn byte_size(&self) -> u64 {
        self.batches.byte_size()
    }
}

impl ByteSize for PartitionData {
    fn byte_size(&self) -> u64 {
        self.records.byte_size()
    }
}

impl ByteSize for FetchableTopicResponse {
    fn byte_size(&self) -> u64 {
        self.partitions.byte_size()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::SystemTime,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, FetchRequest, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::{AbortedTransaction, PartitionData},
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_groups_response::ListedGroup,
        metadata_response::MetadataResponseTopic,
        record::{Record, deflated, inflated},
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::Service as _;
    use tokio::time::{Duration, Instant, advance};
    use url::Url;
    use uuid::Uuid;

    use super::{Deadlines, FetchService, Missed, READ_DEADLINE_OVERHEAD, before};
    use crate::{
        BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, MetadataResponse,
        NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
        ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
        TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
    };

    /// A batch at `base_offset` holding one record per entry in `deltas`,
    /// each at that offset delta.
    fn batch(base_offset: i64, deltas: &[i32]) -> Result<deflated::Batch> {
        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .last_offset_delta(deltas.last().copied().unwrap_or_default());

        for delta in deltas {
            builder = builder.record(
                Record::builder()
                    .offset_delta(*delta)
                    .value(Some(Bytes::from_static(b"foobarfoobarfoobarfoobar"))),
            );
        }

        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Implements [`Storage`] for a double with the methods given,
    /// leaving those the doubles here never call unimplemented.
    macro_rules! storage_double {
        ($double:ty { $($method:item)* }) => {
            #[async_trait]
            impl Storage for $double {
                $($method)*

                async fn register_broker(
                    &self,
                    _broker_registration: BrokerRegistrationRequest,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
                    unimplemented!()
                }

                async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
                    unimplemented!()
                }

                async fn delete_records(
                    &self,
                    _topics: &[DeleteRecordsTopic],
                ) -> Result<Vec<DeleteRecordsTopicResult>> {
                    unimplemented!()
                }

                async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn incremental_alter_resource(
                    &self,
                    _resource: AlterConfigsResource,
                ) -> Result<AlterConfigsResourceResponse> {
                    unimplemented!()
                }

                async fn produce(
                    &self,
                    _transaction_id: Option<&str>,
                    _topition: &Topition,
                    _deflated: deflated::Batch,
                ) -> Result<i64> {
                    unimplemented!()
                }

                async fn offset_commit(
                    &self,
                    _group: &str,
                    _retention: Option<Duration>,
                    _offsets: &[(Topition, OffsetCommitRequest)],
                ) -> Result<Vec<(Topition, ErrorCode)>> {
                    unimplemented!()
                }

                async fn committed_offset_topitions(
                    &self,
                    _group_id: &str,
                ) -> Result<BTreeMap<Topition, i64>> {
                    unimplemented!()
                }

                async fn offset_fetch(
                    &self,
                    _group_id: Option<&str>,
                    _topics: &[Topition],
                    _require_stable: Option<bool>,
                ) -> Result<BTreeMap<Topition, i64>> {
                    unimplemented!()
                }

                async fn list_offsets(
                    &self,
                    _isolation_level: IsolationLevel,
                    _offsets: &[(Topition, ListOffset)],
                ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
                    unimplemented!()
                }

                async fn describe_config(
                    &self,
                    _name: &str,
                    _resource: ConfigResource,
                    _keys: Option<&[String]>,
                ) -> Result<DescribeConfigsResult> {
                    unimplemented!()
                }

                async fn describe_topic_partitions(
                    &self,
                    _topics: Option<&[TopicId]>,
                    _partition_limit: i32,
                    _cursor: Option<Topition>,
                ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
                    unimplemented!()
                }

                async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
                    unimplemented!()
                }

                async fn delete_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                ) -> Result<Vec<DeletableGroupResult>> {
                    unimplemented!()
                }

                async fn describe_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                    _include_authorized_operations: bool,
                ) -> Result<Vec<NamedGroupDetail>> {
                    unimplemented!()
                }

                async fn update_group(
                    &self,
                    _group_id: &str,
                    _detail: GroupDetail,
                    _version: Option<Version>,
                ) -> Result<Version, UpdateError<GroupDetail>> {
                    unimplemented!()
                }

                async fn init_producer(
                    &self,
                    _transaction_id: Option<&str>,
                    _transaction_timeout_ms: i32,
                    _producer_id: Option<i64>,
                    _producer_epoch: Option<i16>,
                ) -> Result<ProducerIdResponse> {
                    unimplemented!()
                }

                async fn txn_add_offsets(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _group_id: &str,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn txn_add_partitions(
                    &self,
                    _partitions: TxnAddPartitionsRequest,
                ) -> Result<TxnAddPartitionsResponse> {
                    unimplemented!()
                }

                async fn txn_offset_commit(
                    &self,
                    _offsets: TxnOffsetCommitRequest,
                ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
                    unimplemented!()
                }

                async fn txn_end(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _committed: bool,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn maintain(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn aborted_transactions(
                    &self,
                    _topition: &Topition,
                    _offset: i64,
                    _last_stable_offset: i64,
                ) -> Result<Vec<AbortedTransaction>> {
                    unimplemented!()
                }

                async fn cluster_id(&self) -> Result<String> {
                    unimplemented!()
                }

                async fn node(&self) -> Result<i32> {
                    unimplemented!()
                }

                async fn advertised_listener(&self) -> Result<Url> {
                    unimplemented!()
                }

                async fn ping(&self) -> Result<()> {
                    unimplemented!()
                }

                async fn delete_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn upsert_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                    _credential: ScramCredential,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<Option<ScramCredential>> {
                    unimplemented!()
                }
            }
        };
    }

    /// Storage whose `fetch` replays scripted responses, records the offset
    /// and remaining `max_wait` of each call, and optionally advances the
    /// (paused) clock past the deadline on the first call, as an engine
    /// that spends the whole budget assembling one truncated batch does.
    #[derive(Clone, Debug)]
    struct Scripted {
        responses: Arc<Mutex<Vec<Vec<deflated::Batch>>>>,
        calls: Arc<Mutex<Vec<(i64, Duration)>>>,
        consume_budget_on_first_call: Option<Duration>,
    }

    impl Scripted {
        fn calls(&self) -> Vec<(i64, Duration)> {
            self.calls.lock().expect("calls").clone()
        }
    }

    storage_double!(Scripted {
        async fn fetch(
            &self,
            _topition: &Topition,
            offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            let calls = {
                let mut calls = self.calls.lock()?;
                calls.push((offset, max_wait));
                calls.len()
            };

            let response = {
                let mut responses = self.responses.lock()?;

                if responses.is_empty() {
                    return Err(Error::Message(format!(
                        "storage fetch called {calls} times, past the end of the script"
                    )));
                }

                responses.remove(0)
            };

            if calls == 1
                && let Some(budget) = self.consume_budget_on_first_call
            {
                advance(budget).await;
            }

            Ok(response)
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            Ok(OffsetStage {
                last_stable: 1_000,
                high_watermark: 1_000,
                log_start: 0,
            })
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
        }

    });

    async fn fetch_partition(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
    ) -> Result<Vec<deflated::Batch>> {
        let mut remaining = max_bytes;

        FetchService { storage }
            .fetch_partition(
                Deadlines::new(Instant::now(), max_wait),
                1,
                1,
                &mut remaining,
                IsolationLevel::ReadUncommitted,
                "abc",
                &FetchPartition::default()
                    .partition(0)
                    .fetch_offset(0)
                    .partition_max_bytes(max_bytes as i32),
            )
            .await
            .map(|partition| {
                partition
                    .records
                    .map(|frame| frame.batches)
                    .unwrap_or_default()
            })
    }

    /// Engines that rebuild batches from rows stop assembling at the
    /// deadline, but still return the record they were on, so after the
    /// deadline every storage call yields one record. Once the deadline
    /// has passed the loop must return what it has rather than issue a
    /// storage round trip per remaining record until `max_bytes` is spent.
    #[tokio::test(start_paused = true)]
    async fn stops_at_the_deadline() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // one truncated batch spends the whole budget, then a single record
        // per call forever
        let responses = (0..10_000)
            .map(|offset| batch(offset, &[0]).map(|batch| vec![batch]))
            .collect::<Result<Vec<_>>>()?;

        let storage = Scripted {
            responses: Arc::new(Mutex::new(responses)),
            calls: Arc::new(Mutex::new(vec![])),
            consume_budget_on_first_call: Some(max_wait),
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);
        assert_eq!(
            vec![(0, max_wait)],
            storage.calls(),
            "storage was called again after the deadline"
        );

        Ok(())
    }

    /// The next fetch offset follows the last offset in the batch, not its
    /// record count: compaction leaves gaps, and re-reading from inside a
    /// batch that has already been returned duplicates records.
    #[tokio::test(start_paused = true)]
    async fn next_offset_follows_the_last_offset_in_the_batch() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // records at offsets 0 and 3 (1 and 2 compacted away), then nothing
        let responses = vec![vec![batch(0, &[0, 3])?], vec![]];

        let storage = Scripted {
            responses: Arc::new(Mutex::new(responses)),
            calls: Arc::new(Mutex::new(vec![])),
            consume_budget_on_first_call: None,
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(
            vec![0, 4],
            storage
                .calls()
                .into_iter()
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// What one storage read of a partition does, in [`Partitions`].
    #[derive(Clone, Debug)]
    enum Read {
        /// Returns this batch, or nothing given no budget, as engines that
        /// stop assembling at the deadline do.
        Batch(deflated::Batch),

        /// Returns nothing.
        Empty,

        /// Does not answer for an hour.
        Stall,

        /// Returns this batch once its budget is spent, plus [`OVERSHOOT`],
        /// as an engine does that checks its budget only between records.
        Assemble(deflated::Batch),
    }

    /// How long past its budget a [`Read::Assemble`] returns.
    const OVERSHOOT: Duration = Duration::from_millis(8);

    /// One storage read of a partition, as [`Partitions`] saw it.
    #[derive(Clone, Debug, PartialEq)]
    struct Call {
        topic: String,
        partition: i32,
        max_bytes: u32,
        max_wait: Duration,
    }

    /// The reads of each partition, by topic and partition.
    type Scripts = BTreeMap<(String, i32), Vec<Read>>;

    /// Storage whose topics each answer their partitions' reads in turn
    /// from their own script, with nothing once it runs out. A topic that
    /// has no script is unknown.
    #[derive(Clone, Debug, Default)]
    struct Partitions {
        scripts: Arc<Mutex<Scripts>>,
        stalled_offset_stages: Arc<Mutex<Vec<(String, i32)>>>,
        calls: Arc<Mutex<Vec<Call>>>,
    }

    impl Partitions {
        /// Storage with one topic, [`TOPIC`].
        fn new(scripts: impl IntoIterator<Item = (i32, Vec<Read>)>) -> Self {
            Self::topics(
                scripts
                    .into_iter()
                    .map(|(partition, script)| (TOPIC, partition, script)),
            )
        }

        fn topics<'a>(scripts: impl IntoIterator<Item = (&'a str, i32, Vec<Read>)>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(
                    scripts
                        .into_iter()
                        .map(|(topic, partition, script)| ((topic.into(), partition), script))
                        .collect(),
                )),
                ..Default::default()
            }
        }

        /// Makes the offset stage of `partition` of [`TOPIC`] never answer.
        fn stall_offset_stage(self, partition: i32) -> Self {
            self.stalled_offset_stages
                .lock()
                .expect("stalled offset stages")
                .push((TOPIC.into(), partition));
            self
        }

        fn calls(&self, partition: i32) -> Vec<Call> {
            self.calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.topic == TOPIC && call.partition == partition)
                .cloned()
                .collect()
        }
    }

    storage_double!(Partitions {
        async fn fetch(
            &self,
            topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            max_bytes: u32,
            _isolation_level: IsolationLevel,
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            self.calls.lock()?.push(Call {
                topic: topition.topic().into(),
                partition: topition.partition(),
                max_bytes,
                max_wait,
            });

            let read = {
                let mut scripts = self.scripts.lock()?;
                let script = scripts
                    .entry((topition.topic().into(), topition.partition()))
                    .or_default();

                if script.is_empty() {
                    Read::Empty
                } else {
                    script.remove(0)
                }
            };

            match read {
                Read::Batch(_) if max_wait.is_zero() => Ok(vec![]),
                Read::Batch(batch) => Ok(vec![batch]),
                Read::Empty => Ok(vec![]),
                Read::Stall => {
                    tokio::time::sleep(Duration::from_secs(3_600)).await;
                    Ok(vec![])
                }
                Read::Assemble(batch) => {
                    tokio::time::sleep(max_wait + OVERSHOOT).await;
                    Ok(vec![batch])
                }
            }
        }

        async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
            let stalled = self
                .stalled_offset_stages
                .lock()?
                .contains(&(topition.topic().into(), topition.partition()));

            if stalled {
                tokio::time::sleep(Duration::from_secs(3_600)).await;
            }

            Ok(OffsetStage {
                last_stable: 1_000,
                high_watermark: 1_000,
                log_start: 0,
            })
        }

        async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            let known = {
                let scripts = self.scripts.lock()?;

                topics
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|topic| match topic {
                        TopicId::Name(name) => Some(name.clone()),
                        TopicId::Id(_) => None,
                    })
                    .filter(|name| scripts.keys().any(|(topic, _)| topic == name))
                    .collect::<Vec<_>>()
            };

            Ok(MetadataResponse {
                cluster: None,
                controller: None,
                brokers: vec![],
                topics: known
                    .into_iter()
                    .map(|name| {
                        MetadataResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(name))
                            .topic_id(Some([1; 16]))
                    })
                    .collect(),
            })
        }
    });

    const TOPIC: &str = "abc";
    const MAX_WAIT: Duration = Duration::from_millis(500);
    const MAX_BYTES: i32 = 1024 * 1024;

    /// Fetches `partitions` of [`TOPIC`] from offset 0 with [`MAX_WAIT`],
    /// returning each partition's answer and how long the request took.
    async fn fetch(
        storage: Partitions,
        partitions: &[i32],
    ) -> Result<(Vec<PartitionData>, Duration)> {
        let (mut topics, elapsed) = fetch_topics(storage, MAX_WAIT, &[(TOPIC, partitions)]).await?;
        assert_eq!(1, topics.len());

        Ok((topics.remove(0), elapsed))
    }

    /// Fetches the partitions of each topic from offset 0 with `max_wait`,
    /// returning each topic's partition answers and how long the request
    /// took.
    async fn fetch_topics(
        storage: Partitions,
        max_wait: Duration,
        topics: &[(&str, &[i32])],
    ) -> Result<(Vec<Vec<PartitionData>>, Duration)> {
        let started_at = Instant::now();

        let response = FetchService { storage }
            .serve(
                FetchRequest::default()
                    .max_wait_ms(i32::try_from(max_wait.as_millis())?)
                    .min_bytes(1)
                    .max_bytes(Some(MAX_BYTES))
                    .topics(Some(
                        topics
                            .iter()
                            .map(|(topic, partitions)| {
                                FetchTopic::default()
                                    .topic(Some((*topic).into()))
                                    .partitions(Some(
                                        partitions
                                            .iter()
                                            .map(|partition| {
                                                FetchPartition::default()
                                                    .partition(*partition)
                                                    .fetch_offset(0)
                                                    .partition_max_bytes(MAX_BYTES)
                                            })
                                            .collect(),
                                    ))
                            })
                            .collect(),
                    )),
            )
            .await?;

        let elapsed = started_at.elapsed();

        Ok((
            response
                .responses
                .unwrap_or_default()
                .into_iter()
                .map(|topic| topic.partitions.unwrap_or_default())
                .collect(),
            elapsed,
        ))
    }

    fn batches(partition: &PartitionData) -> usize {
        partition
            .records
            .as_ref()
            .map_or(0, |records| records.batches.len())
    }

    /// The read deadline of a request with [`MAX_WAIT`].
    const READ_DEADLINE: Duration = MAX_WAIT.saturating_add(READ_DEADLINE_OVERHEAD);

    /// A partition that never answers holds the request for half of the
    /// read deadline, and the partitions after it are then read with a
    /// budget of their own: engines read nothing given none, so a budget
    /// taken from the spent `max_wait` would leave them as starved as
    /// before.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_partition_does_not_starve_the_rest() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
            (2, vec![Read::Batch(batch(0, &[0])?)]),
            (3, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2, 3]).await?;

        assert_eq!(READ_DEADLINE / 2, elapsed);

        assert_eq!(0, partitions[0].partition_index);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(-1, partitions[0].high_watermark);
        assert_eq!(Some(-1), partitions[0].last_stable_offset);
        assert_eq!(Some(-1), partitions[0].log_start_offset);

        for partition in &partitions[1..] {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(
                1,
                batches(partition),
                "partition {}",
                partition.partition_index
            );
            assert_eq!(1_000, partition.high_watermark);
        }

        // max_wait was spent by the stall, and each read after it had half
        // of max_wait of its own
        for partition in 1..=3 {
            assert_eq!(MAX_WAIT / 2, storage.calls(partition)[0].max_wait);
        }

        Ok(())
    }

    /// However many partitions never answer, the request is answered by
    /// its read deadline.
    #[tokio::test(start_paused = true)]
    async fn stalled_partitions_end_at_the_read_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
        ]);

        let (partitions, elapsed) = fetch(storage, &[0, 1, 2]).await?;

        assert_eq!(READ_DEADLINE, elapsed);
        assert_eq!(3, partitions.len());

        for partition in &partitions {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(0, batches(partition));
        }

        Ok(())
    }

    /// A partition that stalls after reading some batches returns them,
    /// with the offset after them as its high watermark, and the bytes
    /// they took are not given again to the partitions after it.
    #[tokio::test(start_paused = true)]
    async fn a_partition_that_stalls_keeps_what_it_read() -> Result<()> {
        let first = batch(0, &[0, 1])?;
        let size = u32::try_from(first.record_data.len())?;

        let storage = Partitions::new([
            (0, vec![Read::Batch(first), Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1]).await?;

        assert_eq!(READ_DEADLINE / 2, elapsed);

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(2, partitions[0].high_watermark);
        assert_eq!(Some(2), partitions[0].last_stable_offset);
        assert_eq!(Some(-1), partitions[0].log_start_offset);

        assert_eq!(1, batches(&partitions[1]));

        let max_bytes = u32::try_from(MAX_BYTES)?;
        assert_eq!(max_bytes - size, storage.calls(0)[1].max_bytes);
        assert_eq!(max_bytes - size, storage.calls(1)[0].max_bytes);

        Ok(())
    }

    /// Every round of the long poll shares the read deadline among all of
    /// the partitions again: a count of partitions left over from the
    /// round before would give the first partition the whole deadline.
    #[tokio::test(start_paused = true)]
    async fn every_round_shares_the_read_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Empty, Read::Stall]),
            (1, vec![Read::Empty, Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage, &[0, 1]).await?;

        // The first round finds nothing, and the long poll waits half of
        // max_wait before the second
        let second_round = MAX_WAIT / 2;
        assert_eq!(second_round + (READ_DEADLINE - second_round) / 2, elapsed);

        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// A partition whose budget equals its share of the read deadline gets
    /// a budget that ends before that share does, so a read that spends
    /// its whole budget returns what it assembled. With `max_wait` of 5s,
    /// the first of two partitions has a share of 5s.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_spends_its_budget_keeps_what_it_read() -> Result<()> {
        let max_wait = Duration::from_secs(5);
        let share = (max_wait + READ_DEADLINE_OVERHEAD) / 2;

        let storage = Partitions::new([
            (0, vec![Read::Assemble(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (mut topics, _) = fetch_topics(storage.clone(), max_wait, &[(TOPIC, &[0, 1])]).await?;
        let partitions = topics.remove(0);

        let budget = storage.calls(0)[0].max_wait;
        assert!(budget < share, "budget {budget:?} is not inside {share:?}");

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// An offset stage that never answers is abandoned at a fresh share of
    /// the time left after the read, and the partition keeps its batches.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_offset_stage_is_abandoned_at_its_own_share() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Assemble(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ])
        .stall_offset_stage(0);

        let (partitions, elapsed) = fetch(storage, &[0, 1]).await?;

        // The read spends the client's max_wait and its overshoot, then
        // the offset stage gets half of the time left
        let read = MAX_WAIT + OVERSHOOT;
        assert_eq!(read + (READ_DEADLINE - read) / 2, elapsed);

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1, partitions[0].high_watermark);
        assert_eq!(Some(-1), partitions[0].log_start_offset);

        assert_eq!(1, batches(&partitions[1]));
        assert_eq!(1_000, partitions[1].high_watermark);

        Ok(())
    }

    /// The partitions of every topic share one read deadline: a stall in
    /// the only partition of the first topic takes half of it, not all of
    /// it, so the next topic is still read.
    #[tokio::test(start_paused = true)]
    async fn topics_share_the_read_deadline() -> Result<()> {
        let storage = Partitions::topics([
            (TOPIC, 0, vec![Read::Stall]),
            ("def", 0, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (topics, elapsed) =
            fetch_topics(storage, MAX_WAIT, &[(TOPIC, &[0]), ("def", &[0])]).await?;

        assert_eq!(READ_DEADLINE / 2, elapsed);
        assert_eq!(0, batches(&topics[0][0]));
        assert_eq!(1, batches(&topics[1][0]));

        Ok(())
    }

    /// The partitions of an unknown topic count as read, so the last
    /// partition of the request gets all of the read deadline left.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_topic_counts_as_read() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Stall])]);

        let (topics, elapsed) =
            fetch_topics(storage, MAX_WAIT, &[("unknown", &[0]), (TOPIC, &[0])]).await?;

        assert_eq!(READ_DEADLINE, elapsed);
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(topics[0][0].error_code)?
        );
        assert_eq!(0, batches(&topics[1][0]));

        Ok(())
    }

    /// A read that does not finish by the deadline is abandoned there.
    #[tokio::test(start_paused = true)]
    async fn before_abandons_a_read_at_the_deadline() {
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Expired),
            before(deadline, std::future::pending::<()>()).await
        );
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// Once the deadline has passed, the read is never polled.
    #[tokio::test(start_paused = true)]
    async fn before_does_not_start_after_the_deadline() {
        let deadline = Instant::now();
        advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert_eq!(Err(Missed::NotStarted), before(deadline, read).await);
        assert!(!polled.load(Ordering::SeqCst));
    }
}
