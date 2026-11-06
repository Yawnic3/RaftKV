# Architecture and safety decisions

## Components

| Package | Responsibility |
|---|---|
| `cmd/raftkv` | Flags, JSON logging, signals, listener and shutdown ownership |
| `cmd/raftkvctl` | Deadline-bound CLI for the leader-aware client |
| `internal/config` | Strict configuration decoding and membership/timing validation |
| `internal/cluster` | Composition, durable membership identity, HTTP lifecycle |
| `internal/raft` | Elections, term/vote state, replication, commitment, barriers, snapshots |
| `internal/transport` | Typed bounded JSON HTTP RPC, connection reuse, partition test hook |
| `internal/storage` | Framed/checksummed WAL, fsync, compaction, OS directory ownership lock |
| `internal/state` | Deterministic map operations and snapshot copying |
| `internal/api` | REST validation, bounded contexts, leader hints, status/health |
| `internal/client` | Seed discovery, hint validation, concurrent-safe leader cache, retry policy |

## Concurrency and lifecycle

One mutex owns each node's consensus fields, state machine, and storage calls.
Keeping storage I/O under this mutex prevents an acknowledgement from racing a
hard-state transition. It also makes slow storage an availability bottleneck.
The state machine intentionally has no separate locks.

A timer loop drives election deadlines and heartbeat triggers. One long-lived
worker per remote member serializes replication to that member. Vote requests
run concurrently within the node's worker WaitGroup. Network calls occur outside
the consensus mutex with bounded contexts; replies recheck role and term under
the mutex. A higher reply term is processed even when the original campaign or
replication round is obsolete. The node checks membership before processing
protocol senders; this is a correctness check, not authentication.

Proposal handlers wait on a broadcast channel replaced under the mutex whenever
progress, role, or failure changes. They always recheck state after wakeup.
Commitment and application are synchronous. A proposal that loses its original
leadership term returns an unknown outcome, even if its entry later commits.

The application calls Start once and Close once. Shutdown marks the node stopped,
cancels worker RPCs, wakes waiters, joins workers, closes storage and idle client
connections, and drains HTTP handlers. RPC handlers arriving during shutdown
observe stopped state before touching storage. Directory locks survive neither
normal close nor abrupt process termination.

## Durable state and protocol invariants

1. Current term never decreases; term advancement clears the old vote. Votes
   and terms reach synced storage before a successful protocol reply.
2. A vote requires an up-to-date candidate log, compared by last term then index.
3. An AppendEntries acknowledgement follows prefix verification and durable
   suffix storage. Conflict replacement cannot overwrite a committed entry.
4. Follower commitment is bounded by the last index verified by that request,
   rather than by an unrelated unverified local suffix.
5. A leader counts replicas to commit only entries in its current term. A
   committed current-term entry establishes commitment of its complete prefix.
6. `snapshot_index <= last_applied <= commit_index <= last_log_index`.
   Snapshot installation advances the first three together and preserves a
   suffix only when the included index and term match.
7. Success reaches the client only after durable majority commitment and local
   application. Fresh read entries require a fresh majority just like writes.
8. Storage errors leave in-memory state potentially ahead of disk, so the node
   becomes failed and refuses further consensus work until restart/operator repair.

These invariants follow the fixed-membership protocol from
[the Raft paper](https://raft.github.io/raft.pdf). The implementation has targeted
tests rather than a formal refinement proof or exhaustive schedule exploration.

## Write and read flow

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant W as Leader WAL
    participant F as Follower
    C->>L: PUT / GET barrier / DELETE
    L->>W: append image + fsync
    L->>F: AppendEntries(previous index, term, entries)
    F->>F: validate prefix; persist + fsync
    F-->>L: success
    L->>L: count majority; advance commit
    L->>W: persist commitment + fsync
    L->>L: apply committed prefix
    L-->>C: success / confirmed read value
    L->>F: heartbeat with commit index
    F->>F: persist commit; apply
```

With three members, the leader plus one follower is a majority. The second
follower may lag when the client receives success, but an elected future leader
must contain the committed prefix. A GET waits for its own committed barrier
and reads under the same mutex, possibly seeing a later overlapping write that
has already applied. It never returns a pre-barrier state.

## Persistence format and crash boundaries

Each WAL frame has a four-byte big-endian payload length, four-byte IEEE CRC32,
then one JSON recovery image. The 256 MiB maximum is checked before allocation.
Records include membership identity, hard state, committed index, a snapshot,
and the uncompacted suffix. No external database is involved.

The full-image design makes each complete frame an independent recovery point.
It is intentionally less efficient than delta logging. A trailing incomplete
frame is discarded; checksum corruption or an invalid sequence is an error.
Recovery does not guess whether corrupted data had been acknowledged. Complete
uncommitted entries remain in the log but are not applied until commitment.

Compaction writes `snapshot.bin.tmp`, syncs it, renames it to `snapshot.bin`,
then writes/syncs `raft.wal.tmp` and replaces `raft.wal`. On POSIX the directory
is synced after publication. The WAL embeds the snapshot values and is the
authoritative recovery source; snapshots do not require loading a second file.
Orphan temporary files are ignored and overwritten by a later compaction.
If a process exits before WAL replacement, recovery uses the old WAL. If it
exits after replacement, recovery uses the compacted image. During received
snapshot installation, no acknowledgement is sent until replacement completes.

Automatic snapshots compact only applied state. A follower behind the leader's
snapshot receives InstallSnapshot instead of unavailable log entries. Snapshot
maps are immutable once published and copied on receipt. Snapshot payloads and
suffix batches can therefore be serialized outside the Raft mutex safely.

## Fault model and excluded guarantees

The implementation assumes stable node identities, identical fixed membership,
trusted non-Byzantine peers, reliable fsync behavior, and retained durable disks.
Messages can be lost, delayed, reordered, or duplicated; nodes can stop and
restart. No clock synchronization is required for safety. Election timing affects
availability; a very slow disk or scheduler can cause unnecessary elections.

There is no pre-vote or check-quorum. A partitioned follower can inflate terms
and briefly disrupt availability when it rejoins. A partitioned leader may keep
its local leader role until learning a higher term, but cannot satisfy a fresh
operation without majority acknowledgement. WAL-backed barriers make that role
staleness safe for reads.

Tests cover explicit failures and repeated runs on localhost. They do not prove
correctness for every network schedule, power loss, filesystem/controller
behavior, malicious peers, storage bit rot, or catastrophic majority disk loss.
Full-state WAL writes and whole-image snapshot transfers need replacement before
considering large datasets or demanding throughput targets.
