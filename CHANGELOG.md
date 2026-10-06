# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- A listener with SASL configured closes a connection that sends a frame larger than 512KiB before the client authenticates, matching the Apache Kafka default for `sasl.server.max.receive.size`. The same limit applies while a client re-authenticates. The broker logs this rejection as `PreAuthenticationFrameTooBig`, and counts it in `nisshi_frames_rejected`.
- The S3 and in-memory (DynoStore) storage engine stores each consumer
  group id as exactly one segment of its object key. Keys for ids without a
  `/` are unchanged. These keys move, and data under the old keys is not
  migrated:
  - an id containing `/`: its committed offsets and group state;
  - the empty id `""`, which consumers using `assign()` commit under: its
    committed offsets and group state;
  - `.` and `..`: the group state only.
  An affected group's consumers find no committed offset and start from
  `auto.offset.reset`. With the Java client default, `latest`, they skip
  records they had not yet consumed. Old data stays visible in ListGroups:
  old `""` data lists as a group named `offsets`, and old `a/b` data as a
  group named `a`. Deleting that group removes it.
- Upgrade every broker sharing a bucket before using these ids. An old and
  a new broker read and write different keys for them, so members connected
  to different brokers form two separate groups. A rollback returns these
  groups to the offsets they had before the upgrade.

### Security

- Decoding a produce batch is bounded. Previously a few KB of compressed
  data could decompress into gigabytes, and a record carrying millions of
  empty headers expanded 32x on decode. Now a batch is rejected with
  `MESSAGE_TOO_LARGE` once its decompressed bytes, or its decoded records
  and headers, pass a 100 MiB budget, or up front when its `record_count`
  alone would. A producer can recover by splitting the batch (the Java
  producer does this on its own for a batch of more than one record).
- Peak memory while decoding one batch is a small multiple of that budget
  (roughly 2 to 3x), not 100 MiB exactly:
  - an uncompressed record's headers are charged after they are allocated,
    so the record that crosses the budget can add up to about 100 MiB more;
  - a Snappy batch holds its decompressed block (up to 100 MiB) alongside
    the records decoded from it;
  - zstd's decoder window, up to 128 MiB, sits outside the budget.
  The budget applies per batch; concurrent batches each have their own.

### Fixed

- On S3 and in-memory storage, a consumer group whose id contains `/` or is
  empty no longer shares offsets or group state with another group. Group
  `a/` no longer reads or overwrites the committed offsets of group `a`, and
  deleting group `a` no longer deletes group `a/b`.
- On S3 and in-memory storage, ListGroups returns each group's real id. For
  an id with a reserved character it returned the encoded form (`a%23b` for
  `a#b`, `%2E` for `.`), which DescribeGroups and DeleteGroups could not
  find.
- On S3 and in-memory storage, DeleteGroups accepts the empty group id, as
  the other storage engines do, instead of answering `INVALID_GROUP_ID`.
- A Snappy batch with a truncated xerial header is rejected with an error
  instead of panicking the decoder.
- SlateDB compaction skips a stored batch it cannot inflate, with a warning,
  instead of abandoning the whole maintenance pass.
