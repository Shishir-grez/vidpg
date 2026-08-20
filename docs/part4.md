# VidPG Code Flow Explanation - Part 4

This explanation assumes no prior knowledge of the project.

The report is being explained in groups of three sections. Sections 1 and 3
are intentionally excluded. This installment covers sections 12, 13, and 14.

The most relevant implementation files for this installment are:

- `vidpg/db/connection.py`
- `vidpg/db/writer.py`
- `vidpg/db/fetcher.py`
- `vidpg/db/notifications.py`
- `vidpg/db/buckets.py`
- `vidpg/db/schema.py`
- `vidpg/relay/workers.py`
- `vidpg/relay/fanout.py`
- `vidpg/relay/sessions.py`
- `vidpg/relay/websocket.py`
- `db/001_schema.sql`
- `tests/contract/db/test_binary_contract.py`
- `tests/contract/db/test_notify_contract.py`
- `tests/integration/db/test_insert_fetch_latest.py`
- `tests/integration/db/test_listen_notify.py`
- `tests/unit/relay/test_fanout.py`
- `tests/unit/relay/test_workers.py`
- `tests/unit/relay/test_websocket.py`
- `tests/integration/relay/test_two_client_relay.py`

The central idea in these sections is:

```text
accepted frame
    -> input latest slot
    -> PostgreSQL insert in the active bucket
    -> metadata-only committed notification
    -> current/previous bucket fetch
    -> destination output latest slot
    -> one serialized WebSocket writer
```

PostgreSQL is used as the relay's short-lived shared frame plane. It stores
the bytes long enough for the other direction to fetch them, but it is not
being used as an archive of every camera frame. The queue and fetch policies
continue to prefer the newest useful frame over complete frame preservation.

## 12. PostgreSQL Insert Flow

### 12.1 Why PostgreSQL is in the middle

The two browsers do not write directly to each other. A frame takes this
route:

```text
publisher browser
    -> publisher WebSocket
    -> relay admission
    -> input LatestSlot
    -> PostgreSQL writer
    -> PostgreSQL bucket table
    -> notification
    -> PostgreSQL fetcher
    -> destination output LatestSlot
    -> destination WebSocket
    -> destination browser
```

The database therefore provides a shared handoff point between the two
directional relay paths.

That handoff is useful for this experiment because it lets the system measure
and observe a database-mediated frame path. It is also the reason that the
frame is written and then read again instead of being copied directly from
one in-memory socket state to the other.

The database is not the only buffering layer. Before this section, the
publisher frame has already passed through a bounded input slot. After this
section, the fetched frame enters a bounded output slot. PostgreSQL is one
stage in a bounded pipeline, not an excuse to create an unlimited database
backlog.

### 12.2 The four database responsibilities

The relay separates database work into four responsibilities:

```text
writer pool       -> inserts accepted frames
fetch pool        -> reads newest frames for subscribers
listener          -> receives LISTEN/NOTIFY metadata
maintenance link  -> rotates the three-bucket storage ring
```

The source report describes the connection topology as:

- A lazy writer pool with a maximum size of two.
- A separate lazy fetch pool with a maximum size of two.
- One dedicated autocommit listener connection.
- One transaction-capable maintenance connection.

The pools are separate because inserting and fetching are different workloads.
If they shared one tiny pool, a collection of slow reads could prevent new
frames from reaching the database. Separate pools make the intended resource
limits visible and keep the two operations independently bounded.

The listener is not borrowed from either pool. A PostgreSQL connection that
has executed `LISTEN` has a persistent subscription state and must remain
available to receive notifications. It is therefore owned by the listener
loop for its entire lifetime.

The maintenance connection is also dedicated. Bucket rotation needs one
transaction-capable connection that can acquire the rotation advisory lock,
clear the next bucket, and update the control row as one coordinated
operation. The detailed rotation behavior is covered by a later report
section; this section only needs to know that the maintenance connection is
separate from frame inserts and fetches.

### 12.3 The lazy bounded pool

`vidpg/db/connection.py` implements the small `PgPool` wrapper.

"Lazy" means that creating the pool object does not immediately open all
possible PostgreSQL connections. A connection is opened only when a worker
actually borrows one.

"Bounded" means that the pool cannot create more than its configured maximum.
For V1, the maximum is two:

```python
PgPool(settings.database_url, max_size=2)
```

The pool tracks:

- Connections currently available for reuse.
- How many connections have been created.
- The maximum allowed count.
- Whether the pool has been closed.

When a worker calls `getconn()`:

1. The pool rejects the request if it is closed.
2. It first tries to reuse an idle connection.
3. If no idle connection exists and the creation limit has not been reached,
   it opens one.
4. If the limit has been reached, the caller waits until another worker
   returns a connection.

The pool opens connections with:

- `autocommit=False`.
- Tuple rows as the default row shape.
- A two-second connection timeout.

After opening a connection, the pool calls `verify_session_settings()`.
That function runs:

```sql
SHOW server_version_num
```

and requires PostgreSQL 18 or newer. The V1 database plane therefore fails
early rather than silently running against a server with an untested version.

The context manager gives workers the normal usage shape:

```python
with pool.connection() as conn:
    # use conn
```

The connection is returned even when the database operation raises. When the
pool itself is closed, idle connections are closed and future borrows fail.

The pool limit is important even though each stream has its own worker. A
session can have multiple directional workers, and a deployment can have
multiple sessions. The pool prevents all those logical workers from turning
into an unbounded number of physical PostgreSQL connections.

### 12.4 How a session gets an insert worker

`RelayService.ensure_session_workers()` performs the worker wiring.

For every directional stream in a session, it:

1. Registers the stream with `Fanout`.
2. Starts one insert worker.
3. Starts one fetch worker.

The insert worker is `run_insert_worker()` in `vidpg/relay/workers.py`.
Its job is intentionally narrow: drain one stream's input slot and pass one
frame at a time to the configured writer.

Its main loop looks conceptually like this:

```text
take one frame from input slot
    if there is no frame:
        wait for input_event
    else:
        write that frame to PostgreSQL
        clear in-flight ownership on success
```

The worker does not read directly from the WebSocket. The WebSocket endpoint
and admission code have already performed the protocol, ownership, metadata,
payload, and sequence checks. This separation keeps network parsing out of
the database worker.

The input slot can have one frame in flight while a newer frame waits behind
it. If another newer frame arrives, it can replace that waiting frame. The
database worker therefore receives a bounded, freshness-oriented workload.

### 12.5 Selecting the active bucket

`PostgresFrameWriter.write()` borrows a writer-pool connection and reads the
single control row:

```sql
SELECT generation, active
FROM vidpg.bucket_state
WHERE singleton
```

The row contains:

- `generation`: the number of completed bucket generations.
- `active`: the physical table number currently accepting new rows.

The code computes the expected active bucket as:

```python
bucket = active_bucket(generation)
```

The bucket helper uses modulo three:

```text
generation 0 -> bucket 0
generation 1 -> bucket 1
generation 2 -> bucket 2
generation 3 -> bucket 0
```

Before inserting, the writer checks that the stored `active` value matches
the value derived from `generation`.

This check catches an inconsistent control row before a frame is written to a
possibly wrong table. The writer also reads the state for each insert rather
than caching it forever. Rotation can change the active bucket while the
process is running, so a long-lived cached bucket choice would eventually be
wrong.

If the control row is missing or inconsistent, the writer raises an error and
does not attempt the frame insert.

### 12.6 Why the frame is validated again

The frame was already validated at relay admission. The database writer still
validates it again at the database boundary.

`vidpg/db/writer.py` checks:

1. The value is a `FrameEnvelope`.
2. The shared envelope contract passes.
3. The protocol metadata limits pass.
4. The payload limits and codec rules pass.
5. The SHA-256 digest matches the payload bytes.

This is defense in depth. The writer should not assume that every caller
reached it through the normal WebSocket endpoint. Tests, future internal
workers, maintenance tools, or a programming mistake could call the writer
directly.

The database boundary is the last point before bytes become persisted frame
data. Rejecting invalid data here protects the table contract even if an
earlier caller made a mistake.

### 12.7 Translating the codec for storage

The shared frame contract names codecs with values such as:

```text
Codec.JPEG      -> "jpeg"
Codec.SYNTHETIC -> "synthetic"
```

The database stores compact numeric codec codes instead:

```text
JPEG      -> 1
SYNTHETIC -> 127
```

The writer rejects codecs that the V1 PostgreSQL path does not support. In
particular, it does not allow `WEBRTC_NATIVE` to enter the frame-transit
tables.

The reverse mapping is performed by the fetcher when it reconstructs a
`FrameEnvelope`.

### 12.8 The insert parameters

The writer creates a parameter tuple in this order:

```text
stream_id
sequence
captured_wall_us
current relay receive time
database codec code
width
height
raw payload bytes
```

The captured timestamp comes from the publisher's frame metadata. The relay
receive timestamp is generated by the writer using the current UTC time.
Those two timestamps answer different questions:

```text
captured_wall_us     -> when the publisher says capture happened
relay_received_at    -> when the database writer received the frame
```

The `inserted_at` column has a database default and records the database's
insert time.

The raw payload is converted to `bytes` before it is passed to Psycopg. The
writer does not convert the payload to:

- Base64 text.
- Hexadecimal text.
- JSON.
- A hand-built SQL literal.

This matters because an encoded image is already binary data. Turning it into
text would add size and CPU overhead and would change the measured database
path.

### 12.9 The prepared binary statement

The writer creates a fixed SQL template for the selected bucket. The table
name comes from the allowlisted `bucket_table()` helper, not from arbitrary
request input.

The ordinary insert shape is:

```sql
INSERT INTO vidpg.frame_bucket_0
    (stream_id, seq, captured_us, relay_received_at,
     codec, width, height, frame)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
```

The actual bucket number changes the fixed table name, but the frame values
remain parameters.

The execution call uses:

```python
conn.execute(
    statement.sql,
    params,
    prepare=True,
    binary=True,
)
```

The two options have distinct purposes:

- `prepare=True` lets Psycopg use a prepared execution path for the stable
  statement.
- `binary=True` asks Psycopg to use binary parameter/result formats where
  supported by the driver.

The test `test_bytea_parameter_uses_binary_format_and_never_sql_interpolation`
checks that the payload is a parameter and that the payload's hexadecimal
representation never appears in the SQL text.

### 12.10 Why the notification is part of the same statement

The normal V1 writer calls `insert_and_notify()`, not only `insert_frame()`.
The notification variant builds a statement shaped like this:

```sql
WITH inserted AS (
    INSERT INTO vidpg.frame_bucket_0 (...)
    VALUES (...)
    RETURNING stream_id, seq
)
SELECT pg_notify(
    'vidpg_frame',
    stream_id::text || ',0,' || seq::text
)
FROM inserted
```

The `RETURNING` clause obtains the identity of the row that was just inserted.
The notification payload contains only:

```text
stream UUID,bucket number,sequence number
```

It does not contain the image bytes.

The insert and `pg_notify()` run in the same PostgreSQL transaction. The
writer then commits the transaction.

PostgreSQL makes a transactional notification visible to listeners only when
the transaction commits. Therefore:

```text
insert succeeds + commit succeeds -> row and notification become visible
insert or transaction rolls back  -> neither is published as a committed event
```

This prevents the listener from receiving an early signal for a row that is
later rolled back.

The integration test
`test_notification_arrives_after_commit` verifies the successful case. The
test `test_rollback_does_not_emit_notification` verifies that a rolled-back
insert does not produce a notification.

### 12.11 Why notifications contain metadata only

PostgreSQL `NOTIFY` is not being used as a binary frame transport.

The image remains in the `bytea` column. The notification is just a small
wake-up message telling the relay:

```text
There may now be a newer row for this stream.
```

That design has several consequences:

- The notification queue does not carry 500-kilobyte image payloads.
- The listener does not need to decode or retain frame bytes.
- The fetcher can choose the newest row rather than processing every signal.
- The notification remains within PostgreSQL's small payload limit.

The notification's bucket field is useful diagnostic metadata, but the fetcher
still consults the current bucket state before reading. The bucket may rotate
between the original insert and the later fetch.

### 12.12 Commit and connection release

`_insert()` commits when the connection is not in autocommit mode. If any
database operation raises, it rolls back before re-raising the error.

On success it returns an `InsertReceipt` containing:

- The stream ID.
- The committed sequence.
- The bucket used.
- The payload length.
- Whether a notification was requested.

The pool context manager then returns the connection to the writer pool.

The insert worker clears the input slot's in-flight ownership only after the
writer call returns successfully. This ordering matters:

```text
database commit succeeds -> clear in-flight frame
database write fails      -> do not claim successful persistence
```

The worker records success and error metrics around this operation. The
metrics include insert counts and insert duration observations, but they do
not use session or stream IDs as labels, so metric cardinality remains bounded.

### 12.13 Insert failure behavior

If a writer call raises, `run_insert_worker()`:

1. Increments the PostgreSQL insert error metric.
2. Marks the in-flight frame as failed in the input slot.
3. Waits for the short retry interval.
4. Continues its loop.

The retry delay avoids a tight error loop when PostgreSQL is unavailable.
The failed frame is not treated as an endlessly retryable archival job. This
matches the live-preview policy: the system should recover and prefer newer
frames rather than allowing failed work to create an unbounded backlog.

If the worker is cancelled, it releases the in-flight ownership with a
worker-cancelled reason and propagates cancellation so service shutdown can
finish cleanly.

### 12.14 The control table and payload tables

`db/001_schema.sql` creates the `vidpg` schema and four important tables:

```text
vidpg.bucket_state
vidpg.frame_bucket_0
vidpg.frame_bucket_1
vidpg.frame_bucket_2
```

`bucket_state` is the logged control table. It contains one singleton row with
the generation and active bucket.

The three frame tables are unlogged payload tables. They are a rotating live
cache, not permanent durable storage.

A logged table participates in PostgreSQL's normal write-ahead logging and
crash-recovery durability model. An unlogged table reduces that durability
work and is appropriate here because the V1 frame rows are disposable. If a
process or database crash loses the unlogged payload rows, the live preview
can resume with new frames; the system is not promising that those rows form
an archive.

The control row remains logged because the relay needs a reliable answer to:

```text
Which generation is active, and which bucket is safe to write or read?
```

### 12.15 The payload table schema

Each bucket contains rows with fields equivalent to:

```text
stream_id          UUID
seq                BIGINT
captured_us        BIGINT
relay_received_at  TIMESTAMPTZ
inserted_at        TIMESTAMPTZ
codec              SMALLINT
width              SMALLINT
height             SMALLINT
frame              BYTEA
```

The SQL schema also imposes database-level constraints:

- `seq` must be positive.
- `width` and `height` must be positive.
- `frame` must be non-empty.
- The stored byte length must be no more than the SQL table's upper bound.

The application-level V1 limit is stricter for normal frame admission. The
database constraint is an additional safety boundary, not a replacement for
the protocol limit.

The payload column is declared:

```sql
frame BYTEA STORAGE EXTERNAL NOT NULL
```

`BYTEA` means raw PostgreSQL binary data. `STORAGE EXTERNAL` permits PostgreSQL
to keep the value in its external TOAST representation instead of requiring
the full image to remain inline in the main table tuple.

The application still treats the payload as one bounded byte sequence. The
storage detail is a PostgreSQL layout choice, not a change to the wire
protocol.

### 12.16 The newest-row index

Every payload bucket has an index shaped like:

```sql
CREATE INDEX frame_bucket_0_latest
    ON vidpg.frame_bucket_0 (stream_id, seq DESC);
```

The fetch query asks for one stream's highest sequence above a watermark:

```sql
WHERE stream_id = %s AND seq > %s
ORDER BY seq DESC
LIMIT 1
```

The index matches the lookup pattern:

1. Find one stream.
2. Consider sequences from newest to oldest.
3. Return one row.

The system does not need to scan and transfer every historical frame for that
stream. This is the database equivalent of the latest-value queue policy.

### 12.17 What is not persisted in the row

The frame row is intentionally smaller than the full in-memory envelope.
The table stores the fields needed to identify, time, validate, and replay the
payload through V1:

- Stream identity and sequence.
- Capture and relay timing.
- Codec and dimensions.
- Raw payload.

The writer does not store the in-memory SHA-256 field separately. It validates
the hash before insert. The fetcher recomputes the hash from the retrieved
payload when it reconstructs the `FrameEnvelope`.

The monotonic capture timestamp is also not used as a cross-process database
ordering value. Monotonic clocks are meaningful within one process, not as a
portable timestamp across browsers, relay processes, and database sessions.

The fetched envelope supplies the wire-compatible values needed to send the
frame onward. The payload bytes and sequence remain the important continuity
data.

### 12.18 Section 12 tests

The database contract tests cover the write boundary:

- `test_bytea_parameter_uses_binary_format_and_never_sql_interpolation()`
  verifies binary payload parameters, prepared execution, and non-interpolated
  SQL.
- `test_insert_without_notification_has_no_notify_call()` verifies that the
  lower-level insert variant does not accidentally publish a notification.

The database integration tests cover the live PostgreSQL behavior:

- `test_schema_catalog_matches_p3_contract()` verifies logged control state,
  unlogged payload buckets, external payload storage, and the catalog shape.
- `test_insert_three_fetch_latest_only_and_respect_watermark()` inserts three
  rows and verifies that the newest one is returned and that a watermark equal
  to that sequence returns no newer frame.
- `test_newer_previous_bucket_candidate_beats_older_active_candidate()` checks
  that the newer row wins even when it is in the previous physical bucket.
- `test_notification_arrives_after_commit()` verifies commit-visible notify
  behavior.
- `test_rollback_does_not_emit_notification()` verifies rollback isolation.
- `test_pool_and_dedicated_listener_enforce_postgresql_18()` verifies the
  PostgreSQL version gate and the listener's autocommit mode.

The practical guarantee from these tests is:

```text
valid frame
    -> binary parameterized insert
    -> committed metadata notification
    -> row remains fetchable through the bucket contract
```

## 13. PostgreSQL Notification and Fetch Flow

### 13.1 A notification is a wake-up signal

The listener does not receive the JPEG itself. It receives a small signal that
means a stream may have a newer persisted row.

The relay then performs a database read to obtain the actual frame. The split
is deliberate:

```text
NOTIFY -> wake the correct stream worker
SELECT -> choose and retrieve the newest usable row
```

The notification tells the relay when to look. The indexed query decides what
to read.

This allows several notifications to collapse into one fetch. If sequences
100 through 105 commit while the fetcher is busy, the fetcher does not need to
deserialize and publish all six payloads. It can read sequence 105.

### 13.2 Opening the dedicated listener connection

`RelayService._listen_loop()` owns the notification connection.

At startup, it:

1. Opens a dedicated listener connection using
   `open_dedicated_listener()`.
2. Uses autocommit mode for the listener connection.
3. Verifies PostgreSQL 18 or newer.
4. Executes `LISTEN vidpg_frame`.
5. Performs a startup rescan of the active and previous buckets.
6. Delivers the rescan results to `Fanout`.
7. Begins waiting for notifications.

The listener is dedicated because the process needs to leave it available for
notification polling. It is not borrowed briefly from the writer or fetch
pool.

### 13.3 Why `LISTEN` happens before the rescan

The order is a race-prevention mechanism:

```text
open connection
    -> LISTEN vidpg_frame
    -> commit listener setup if required
    -> rescan existing active/previous rows
    -> process live notifications
```

Suppose a frame committed before the process started. It has no new live
notification for this listener, so the rescan must find it.

Suppose another frame commits after `LISTEN` is active. The listener receives
that notification, even if the commit occurs while the rescan is running.

The two mechanisms cover both cases:

```text
rescan -> rows that already existed
notify -> commits observed after listening began
```

There can be overlap. A row found by the rescan may also have a notification
waiting. That is safe because the fanout state keeps only the greatest
sequence and does not start unlimited duplicate fetches.

The integration test `test_listen_startup_rescan_finds_preexisting_frame`
creates a row before the listener setup and verifies that the rescan discovers
it.

### 13.4 The listener loop and blocking database calls

Psycopg notification iteration is a blocking database operation. The relay is
otherwise asyncio-based, so the listener loop calls the blocking functions
through `asyncio.to_thread()`.

The loop conceptually does this:

```text
while service is running:
    open listener connection
    LISTEN
    rescan
    mark listener ready
    while connection is healthy:
        wait up to one second for one notification
        if a signal arrives:
            pass it to Fanout
    close the connection
    wait one second
    reconnect
```

The one-second timeout gives the loop a regular opportunity to notice service
shutdown even when no frames are arriving.

When a listener error occurs, the service records an error state, marks the
listener as not ready, closes the connection, waits briefly, and retries.
The rest of the process can remain alive while the notification plane
recovers, although the database readiness state will show that the relay is
not fully ready.

### 13.5 The notification payload format

The writer creates payloads in this exact shape:

```text
<stream UUID>,<bucket number>,<sequence>
```

An example has the form:

```text
11111111-1111-1111-1111-111111111111,2,17
```

The payload is metadata only:

- The stream ID tells the relay which directional stream became dirty.
- The bucket number identifies the physical bucket used by the insert.
- The sequence identifies the committed row.

The notification does not include dimensions, codec, timestamps, or frame
bytes. Those values are read from the persisted row after the signal arrives.

### 13.6 Parsing and rejecting unsafe signals

`parse_frame_notification()` treats the payload as an untrusted input even
though it normally comes from the relay's own writer.

It checks:

1. The value is text or bytes-like.
2. Bytes decode as ASCII.
3. The encoded size is below PostgreSQL's notification payload limit.
4. There are exactly three comma-separated fields.
5. The first field is a UUID.
6. The bucket is exactly 0, 1, or 2.
7. The sequence is between 1 and the signed 64-bit maximum.

Malformed payloads are not allowed to crash the listener loop. The
`NotificationStream` records an invalid count and the last error, then ignores
the malformed notification and continues reading.

The contract test `test_malformed_notification_is_rejected()` covers invalid
field counts, bucket values, sequences, and non-ASCII bytes.

### 13.7 Recording signals for streams that are not registered yet

The listener may observe a notification before a corresponding session stream
has been registered with `Fanout`.

`Fanout.notify()` handles that case by storing the greatest pending sequence
for the stream ID:

```text
notification arrives
    -> no RelayStreamState yet
    -> save the greatest pending sequence
    -> register stream later
    -> apply the pending signal
```

This avoids losing a database wake-up merely because the process observed the
notification slightly before session worker setup completed.

If the stream is already registered, the signal is applied directly to its
dirty state.

### 13.8 Dirty state and notification coalescing

Each registered stream tracks values equivalent to:

```text
latest_signaled_seq
dirty_generation
fetch_in_progress
last_fetched_seq
last_published_seq
```

When a signal has a sequence greater than the stored latest signal:

1. `latest_signaled_seq` advances.
2. `dirty_generation` increments.
3. The stream's `fetch_event` is set.

Signals at or below the current latest signal do nothing.

For signals 1, 2, 3, 3, and 2, the state ends with:

```text
latest_signaled_seq = 3
```

The first three increasing signals create three dirty generations. The
duplicate sequence 3 and older sequence 2 do not create additional work.

The word "dirty" means "there may be a newer database row to fetch." It does
not mean that the dirty state contains a frame payload.

### 13.9 The fetch worker waits for dirty state

`run_fetch_worker()` is created once per directional stream.

Its loop begins by waiting for `stream_state.fetch_event`:

```text
no dirty signal -> sleep without querying PostgreSQL
dirty signal    -> ask Fanout whether a fetch is allowed
```

This avoids polling the database continuously when no new frame has arrived.

If the stream is closed while waiting, the worker exits. If a notification
arrives while the worker is already active, the event remains part of the
stream state and the policy checks it after the current fetch finishes.

### 13.10 At most one fetch in flight

The call to `Fanout.fetch_once_policy(stream_id)` decides whether the worker
may issue a database fetch.

It returns a `FetchDecision` with:

- `should_fetch`.
- `watermark`.
- The current dirty generation.

The policy rejects a new fetch when:

- The stream is not registered.
- Another fetch for that stream is already in progress.
- The latest signal is not newer than the last fetched sequence.

When another fetch is already running, the policy increments the coalesced
fetch metric and returns without opening another read operation.

This is the read-side equivalent of the bounded latest-frame slots:

```text
many notifications -> one dirty stream -> one fetch in flight
```

### 13.11 The fetch watermark

The fetch decision uses `last_published_seq` as its watermark.

The database query asks for rows strictly greater than this number. A frame
that has already been published should not be published again merely because
another notification was duplicated or because a rescan overlapped with a
live notification.

The stream also tracks `last_fetched_seq`. That value records how far the
fetch process has observed, including a fetch that found a row that could not
be delivered because the subscriber was gone.

The distinction is useful:

```text
last_fetched_seq   -> latest sequence the fetch process has observed
last_published_seq -> latest sequence passed toward a subscriber
```

The system does not keep retrying an already observed row forever when there
is no subscriber. That is consistent with the no-replay live-preview policy.

### 13.12 Reading the current and previous buckets

`PostgresFrameFetcher.fetch()` borrows a connection from the separate fetch
pool and calls `fetch_latest_after()`.

The fetcher first reads `bucket_state` and verifies the same invariant used by
the writer:

```text
active == generation modulo 3
```

It then reads two physical tables:

```text
current active bucket
previous bucket in the rotation ring
```

It does not read the bucket named in the notification blindly. The signal may
have been delayed until after rotation. The current control state is the
authoritative source for which two buckets are still readable.

### 13.13 One candidate from each bucket

For each readable bucket, `fetch_bucket_candidate()` executes a query shaped
like this:

```sql
SELECT stream_id, seq, captured_us, relay_received_at,
       inserted_at, codec, width, height, frame
FROM vidpg.frame_bucket_<selected>
WHERE stream_id = %s AND seq > %s
ORDER BY seq DESC
LIMIT 1
```

The query returns at most one candidate per bucket:

- The stream ID must match.
- The sequence must be greater than the watermark.
- The newest sequence in that bucket is preferred.

The fetcher then compares the two candidates in Python and keeps the greater
sequence.

This two-query approach handles a rotation boundary correctly. The newest
row could be in the active bucket, or it could be in the previous bucket if
the stream's latest committed frame was written just before rotation.

The integration test
`test_newer_previous_bucket_candidate_beats_older_active_candidate()` creates
that situation and verifies that sequence 10 in the previous bucket beats
sequence 9 in the active bucket.

### 13.14 Empty fetches are normal

The notification is a committed signal, but the row may no longer be in the
two readable buckets by the time the fetch runs. A rotation can clear the
older bucket during that interval.

Therefore a fetch can validly return `None`:

```text
notification observed
    -> rotation advances
    -> signaled row is cleared
    -> current/previous queries find no row above watermark
```

`Fanout.complete_fetch()` treats this as an observed signal. It advances the
fetch watermark to the greatest signaled sequence and clears the fetch event
when no newer signal remains.

That prevents an old notification from causing an infinite fetch loop after
its row has legitimately expired from the live bucket ring.

The frame may be gone, but that is not a data-integrity violation for this
V1 live-preview design. The rotation policy intentionally allows old live
frames to expire.

### 13.15 Reconstructing a FrameEnvelope

When a row is found, `_row_to_frame()` converts database values back into the
shared in-memory contract.

The reconstruction performs these steps:

1. Read the stream ID, sequence, capture timestamp, codec, dimensions, and
   payload from the row.
2. Convert the database codec code back to a `Codec` value.
3. Convert the payload to immutable bytes.
4. Compute the SHA-256 digest from those bytes.
5. Create a V1 `FrameEnvelope`.
6. Validate the reconstructed envelope.
7. Reject it if it exceeds the V1 payload limit.

The fetched envelope uses the PostgreSQL-path identity values needed by the
wire builder, including the V1 wire run ID. The original monotonic capture
timestamp is not recreated because it was not a meaningful cross-process
database value.

The hash is computed after retrieval rather than trusted from a separately
stored hash column. This verifies the payload bytes that actually came back
from PostgreSQL and gives the outgoing protocol builder a valid frame object.

### 13.16 Rejecting corrupt persisted rows

The fetcher treats the database as a contract boundary too. A row that cannot
be converted into a valid frame raises `FrameFetchError`.

It rejects conditions such as:

- Unsupported codec code.
- Invalid UUID or integer shape.
- Invalid envelope fields.
- Payload length beyond the V1 limit.
- A reconstructed frame that fails shared validation.

The error is not converted into a fake frame. The fetch worker records a fetch
error, clears its in-progress marker, keeps the fetch event set, waits briefly,
and retries according to the worker loop.

This behavior makes a database or contract problem observable instead of
silently delivering malformed bytes to the destination browser.

### 13.17 Completing a successful fetch

After the database fetch returns, the worker calls:

```python
fanout.complete_fetch(stream_id, frame)
```

If the frame is present and belongs to the expected stream:

1. `last_fetched_seq` advances to at least the frame sequence.
2. If the sequence is newer than `last_published_seq`, the stream records it
   as published.
3. Fetch metrics are incremented.
4. `Fanout.publish()` sends the frame to the current subscriber's output slot.
5. The fetch-in-progress flag is cleared.

If another newer signal arrived while the fetch was running, the worker sets
the fetch event again. Otherwise it clears the event and waits for the next
notification.

The result is a one-at-a-time newest-row handoff rather than a replay loop.

### 13.18 What happens when a subscriber is absent

`Fanout.publish()` checks the current subscriber before offering the frame to
an output slot.

If the stream has no subscriber, or if the subscriber is closed, the frame is
not retained. The fanout increments its disconnected count and returns.

This means that a browser joining later does not automatically receive the
last frame from the database. A new subscriber sees future frames only,
unless a separate explicit replay feature is added later.

That behavior avoids retaining stale output and keeps the live relay's memory
and delivery semantics predictable.

### 13.19 Why the notification does not directly publish

The listener does not call `publish()` as soon as a signal arrives. It first
allows the fetch policy to choose a row.

This separation is necessary because a notification alone does not contain a
complete frame and does not guarantee that the signaled row is still in a
readable bucket.

The complete read path is:

```text
notification
    -> mark stream dirty
    -> choose one permitted fetch
    -> query current bucket
    -> query previous bucket
    -> choose greatest sequence
    -> validate reconstructed envelope
    -> publish to destination output slot
```

### 13.20 Notification reconnect behavior

If the listener connection fails, the service closes it and retries after the
configured listener retry delay.

On every new connection it repeats:

```text
LISTEN
rescan current and previous buckets
resume notification reads
```

The rescan is required after reconnect because notifications emitted while
the connection was unavailable cannot be assumed to be present in the new
listener's queue.

The rescan returns the newest row per stream from the two readable buckets.
It uses `DISTINCT ON (stream_id)` and descending sequence ordering to avoid
turning reconnect recovery into a full frame-history replay.

### 13.21 Section 13 tests

The notification contract tests verify:

- Metadata-only payload parsing.
- Exact UUID, bucket, and signed sequence validation.
- Rejection of malformed and non-ASCII payloads.
- Coalescing of repeated or older signals.

The database integration tests verify:

- Notifications arrive after a successful commit.
- Rolled-back inserts do not notify.
- Startup rescans find rows that existed before `LISTEN` became active.
- The newest row is selected from active and previous buckets.
- A watermark prevents already observed sequences from being returned again.

The relay unit tests verify that notification coalescing results in one fetch
in flight and that the fetched frame reaches the subscriber's output slot.

### 13.22 The practical read-side guarantee

The read path is designed to provide this bounded behavior:

```text
many committed frames
    -> many small metadata signals
    -> one greatest dirty sequence per stream
    -> at most one database fetch in flight
    -> at most one newest candidate per readable bucket
    -> one selected frame
```

It preserves freshness and limits work. It does not promise that every
committed frame will be delivered to the browser.

## 14. Server Output Flow

### 14.1 From fetch completion to a destination

Once `complete_fetch()` has a valid newer frame, it calls:

```python
Fanout.publish(stream_id, frame)
```

The fanout registry maps the directional stream to its current destination
subscriber.

For a two-sided session, the mapping is conceptually:

```text
side A upload stream -> side B destination
side B upload stream -> side A destination
```

The source frame and destination socket are therefore selected by stream
ownership and session wiring, not by a caller-provided arbitrary socket ID.

### 14.2 Fanout checks before publishing

`Fanout.publish()` first verifies:

1. The stream is registered.
2. The frame's `stream_id` matches the requested stream.
3. A subscriber exists for that stream.
4. The subscriber is not closed.
5. The subscriber still has a socket.

If any destination condition fails, the frame is not retained. A live output
frame is useful only to a currently connected receiver, so holding it for a
future connection would create implicit replay behavior.

### 14.3 The output LatestSlot

For a connected destination, `subscriber.offer_frame(frame)` places the frame
in the client's output `LatestSlot`.

The output slot has the same freshness policy as the input slot:

```text
no waiting frame -> accept
older waiting frame -> replace it with the newer frame
in-flight frame -> leave it alone and keep the newer frame waiting
```

The output slot is separate from the input slot because these are different
boundaries:

```text
input slot  -> protects database progress
output slot -> protects destination socket progress
```

A slow browser must not mutate the frame currently being written by the
output worker. A slow database must not directly control what the destination
socket is already sending.

When an older waiting frame is replaced, the client and fanout replacement
counters increase. The metric labels identify the stage as `relay_output`,
not a session or stream, so the metric series stay bounded.

### 14.4 Replacement example

Suppose the output worker is busy and fetched frames arrive quickly:

```text
frame 100 -> output waiting
worker takes frame 100 -> output in flight
frame 103 -> output waiting
frame 105 -> replaces waiting frame 103
worker finishes frame 100
worker takes frame 105
```

The destination can see frame 100 followed by frame 105. Missing 101, 102,
103, and 104 is an intentional freshness decision, not an accidental protocol
reordering.

The in-flight frame is never replaced because the socket writer may already be
using its bytes. Only the waiting frame is eligible for replacement.

### 14.5 One output worker owns one socket

After a client joins and receives its ready message, the endpoint creates one
`run_output_worker()` task for that socket.

The worker is the sole owner of writes to that socket. The input side of the
WebSocket endpoint does not send output frames concurrently, and the fanout
registry does not call `send_bytes()` directly.

This creates a single write order for each client:

```text
control message 1
control message 2
binary frame 1
binary frame 2
...
```

Without single-writer ownership, an outgoing JSON control message and a
binary frame could be submitted concurrently. Depending on the WebSocket
implementation, that could interleave operations, violate library ownership
rules, or make the logical order difficult to reason about.

### 14.6 Control messages have priority

The output worker checks the client's bounded control queue before it checks
the frame slot:

```python
control = client_state.take_control()
if control is not None:
    send JSON control message
    continue
```

Only when no control message is waiting does it take a frame from the output
slot.

Control messages are FIFO. They are not replaced by newer frames, because a
control response and a video image have different meanings.

For example, when the client sends a ping control message, the WebSocket
input loop enqueues a pong. The output worker sends that pong before taking a
waiting frame.

The control queue is bounded at 32 messages. If it fills, enqueueing another
control message raises `ControlOverflowError`, and the slow client is treated
as unhealthy rather than allowed to grow memory without limit.

### 14.7 Waiting for output work

If the control queue is empty and the output slot has no frame, the worker
waits on `client_state.output_event`.

The event is set when:

- A control message is enqueued.
- A frame is accepted into the output slot.

The worker therefore sleeps while idle and wakes only when there is potential
output work. It does not poll the socket or database continuously.

The worker also checks that its client state still belongs to the same socket
generation. A reconnect creates a new active socket state, so an old output
worker must not continue sending to a replaced connection.

### 14.8 Checking the socket's buffered amount

Before encoding and sending a frame, `write_binary_frame()` reads the socket's
buffered amount.

The helper accepts either naming style:

```text
buffered_amount
bufferedAmount
```

It compares the value with the configured threshold, which is 524,288 bytes
in V1.

If the buffered amount is already above the threshold, the function returns:

```python
WriteResult(
    sent=False,
    bytes_written=0,
    skipped=True,
    reason="BUFFERED_AMOUNT_HIGH",
)
```

It does not call `send_bytes()` in that case.

This is an intentional output drop. The receiver is already behind, so adding
another frame would increase queued network work and latency. The output
worker clears the frame's in-flight ownership after recording the skip and
then continues with newer work.

The unit test `test_high_buffer_output_is_skipped_without_encoding_send()`
verifies this exact behavior.

### 14.9 Rebuilding the binary frame

If the socket buffer is below the threshold, the output path calls:

```python
build_frame_message(frame.meta(), frame.payload)
```

This rebuilds the V1 message as:

```text
48-byte header + raw payload bytes
```

The server does not forward a database row object or a JSON representation.
It converts the fetched `FrameEnvelope` back into the same binary wire format
that the browser upload side uses.

`build_frame_message()` validates the metadata, payload length, codec rules,
and payload boundary before returning the bytes. The output path therefore
has another protocol check immediately before network transmission.

### 14.10 The output write timeout

The binary send is wrapped in:

```python
await asyncio.wait_for(
    _send_bytes(socket, message),
    timeout=OUTPUT_WRITE_TIMEOUT_SECONDS,
)
```

The V1 timeout is 250 milliseconds.

The timeout prevents one destination socket from holding an output worker
forever. If a send does not complete in time, `write_binary_frame()` raises an
`OutputTimeoutError`.

The output worker then:

1. Increments the client's timeout count.
2. Fails the in-flight frame ownership.
3. Closes the socket with the output-timeout close code.
4. Stops the worker.

Other send exceptions follow the same unhealthy-client path. The relay does
not keep attempting writes to a socket that cannot accept bounded output.

### 14.11 Successful output bookkeeping

After `write_binary_frame()` returns successfully, the output worker clears the
in-flight frame from the slot.

It then distinguishes two outcomes:

```text
sent frame    -> increment delivered-frame metric
skipped frame -> increment skipped-frame and buffered-drop metrics
```

The write-call metric is incremented before attempting the write. This lets
operators compare attempted writes with successful deliveries and intentional
buffer-based skips.

The byte count in `WriteResult` records the complete binary message size when
the send is accepted by the socket API. A skipped frame reports zero bytes
written.

### 14.12 Why control and binary output cannot share a queue

A single latest-value queue would be wrong for control messages.

Replacing an old video frame with a newer video frame is acceptable because
the newest image is usually more useful. Replacing a control message could
change the meaning of the protocol. For example, these are not interchangeable:

```text
{"type":"pong"}
{"type":"error", ...}
{"type":"ready", ...}
```

V1 therefore uses:

```text
bounded FIFO control queue -> preserve control messages and order
latest-value output slot   -> preserve freshness for video frames
```

The output worker gives the control queue priority while keeping the two
policies separate.

### 14.13 Disconnect cleanup

When a client disconnects, `Fanout.unsubscribe()` removes the client from all
stream subscriber mappings and calls `client_state.reset_output()`.

Resetting output does all of the following:

- Replaces the output slot with a fresh empty slot.
- Drops any waiting or in-flight destination frame state.
- Clears pending control messages.
- Clears the output event.

The endpoint also cancels the output worker and waits for its termination.
This prevents the old worker from writing after the socket has been replaced
or closed.

The session registry removes the socket ownership only if the disconnecting
socket is still the current socket for that side. A stale socket cannot remove
a newer reconnect's ownership.

### 14.14 No output replay after reconnect

The output path intentionally does not retain a disconnected client's last
frame.

The behavior is:

```text
destination disconnects
    -> unsubscribe
    -> clear output slot
    -> discard waiting frame
    -> later reconnect receives future output only
```

This is consistent with the rest of V1:

- Input frames can be replaced while waiting.
- Database rows can expire through rotation.
- Notifications can refer to already-cleared rows.
- Disconnected subscribers do not receive an implicit history replay.

The result is a live preview, not a reliable recording service.

### 14.15 Output-side sequence rules

The fetched frame is published only when its sequence is newer than the
stream's published watermark. The output slot also rejects frames that are not
newer than retained frames.

These layers protect different boundaries:

```text
fetch watermark       -> do not fetch/publish an already observed sequence
fanout published state -> do not send an older fetched frame again
output LatestSlot      -> do not retain an older waiting frame
```

A destination can therefore see gaps without seeing a backward sequence. A
gap means intermediate frames were intentionally replaced, skipped, or
expired; it does not mean an older frame was replayed after a newer one.

### 14.16 A complete output example

Assume PostgreSQL returns sequence 40 for side A's upload stream. Side B is
currently connected.

```text
1. fetch worker reconstructs FrameEnvelope(seq=40)
2. Fanout.complete_fetch() updates fetch state
3. Fanout.publish() finds side B's ClientState
4. ClientState.offer_frame() places seq=40 in the output slot
5. output_event is set
6. output worker wakes
7. no control message is waiting
8. output worker takes seq=40 in flight
9. socket buffered amount is checked
10. V1 header and payload are rebuilt
11. send_bytes() is given the complete binary message
12. write completes within 250 ms
13. output slot clears seq=40
14. delivered metrics are incremented
```

If the socket buffer is too high, step 10 is skipped and the frame is
intentionally dropped. If the send exceeds the timeout, the socket is closed
instead of allowing the worker to remain blocked.

### 14.17 Control-message example

Assume side B sends a ping while frame 40 is waiting.

```text
side B input loop receives text ping
    -> enqueue {"type":"pong"}
    -> set output_event

output worker wakes
    -> takes pong from control FIFO
    -> sends JSON pong
    -> loops again
    -> takes frame 40
    -> performs bounded binary write
```

The pong is not overwritten by frame 40, and frame 40 does not overtake the
control response because the output worker checks control first.

### 14.18 Output failures are local to the client

A destination socket that times out is closed. The relay does not stop the
database listener or all other sessions because one client is slow.

The failure is local to:

- The client state.
- The current socket generation.
- The output worker for that socket.

Other clients can continue receiving frames. The disconnected stream may
continue receiving future frames into the database and fetch path, but with no
subscriber those frames are not retained for replay.

The bounded queues and per-socket worker ownership keep a bad destination from
turning into a process-wide unbounded wait.

### 14.19 Section 14 tests

The output and relay tests cover the important behavior:

- `test_output_worker_serializes_control_before_binary_frame()` verifies that
  a pong is sent before a queued binary frame and that one binary message is
  delivered.
- `test_high_buffer_output_is_skipped_without_encoding_send()` verifies the
  buffered-amount guard.
- `test_slow_output_replaces_waiting_frame_and_counts_it()` verifies newest
  output replacement and metrics.
- `test_disconnected_subscriber_does_not_retain_output()` verifies that a
  disconnected destination gets no implicit output replay.
- `test_a_to_b_and_b_to_a_frames_cross_postgres()` exercises both directions
  through the actual session, database, notification, fetch, fanout, and
  WebSocket output path.

The two-client integration test checks both directions:

```text
side A upload -> side B receive
side B upload -> side A receive
```

It also parses the received messages with the same binary protocol parser,
which verifies that the output worker rebuilt a valid V1 wire message.

### 14.20 The complete boundary chain

Sections 12, 13, and 14 form one connected path:

```text
input slot
    -> insert worker
    -> active PostgreSQL bucket
    -> committed NOTIFY metadata
    -> listener
    -> coalesced dirty state
    -> one fetch worker operation
    -> current/previous newest-row selection
    -> validated FrameEnvelope
    -> fanout subscriber lookup
    -> output slot
    -> single output worker
    -> buffer admission
    -> 48-byte header plus payload
    -> bounded WebSocket send
```

Each stage has a different job:

```text
input slot       -> bound database-side waiting work
insert worker    -> validate and commit one frame
bucket table     -> temporarily store raw bytes
NOTIFY           -> wake the correct stream without carrying bytes
fetch policy     -> collapse many signals into newest-row work
fanout           -> select the current destination
output slot      -> bound network-side waiting work
output worker    -> serialize control and binary socket writes
buffer check     -> skip work when the receiver is already behind
write timeout    -> close a socket that cannot accept output
```

### 14.21 The main operational lesson

The PostgreSQL path is not implemented as a reliable frame queue. It is a
bounded freshness pipeline.

The system intentionally allows all of these outcomes:

- A waiting input frame is replaced by a newer frame.
- Several committed notifications become one fetch.
- A fetched frame is replaced in the destination output slot.
- A frame is skipped because the socket buffer is high.
- A frame disappears because bucket rotation has moved past it.
- A frame is not retained because the subscriber disconnected.

Those outcomes are not accidental data loss in the V1 model. They are the
mechanisms that keep live latency and memory bounded when the camera, database,
network, or receiver is slower than the producer.

The corresponding tradeoff is:

```text
better live freshness and bounded resources
    in exchange for
not guaranteeing delivery of every captured frame
```

A future archival or reliable-delivery mode would need a different policy. It
would require explicit durable retention, replay semantics, and queues that
preserve every item. Removing the current capacity limits without adding
those policies would only turn temporary slowness into unbounded backlog.

## Main operational lesson from sections 12, 13, and 14

The database and output path can be summarized as:

```text
accepted frame
    -> bounded input ownership
    -> validated binary PostgreSQL insert
    -> commit-visible metadata notification
    -> coalesced stream dirtiness
    -> newest-row fetch from two readable buckets
    -> validated frame reconstruction
    -> current subscriber lookup
    -> bounded output ownership
    -> control-first, buffer-aware WebSocket write
```

The most important design decisions are:

1. PostgreSQL notifications carry identity metadata, not image bytes.
2. Inserts and notifications become visible together at commit.
3. The listener rescans after `LISTEN` so startup and reconnect do not miss
   already committed rows.
4. Fetching reads only the active and previous buckets and chooses the highest
   sequence above the watermark.
5. A stream has at most one fetch in flight.
6. A destination has at most one waiting and one in-flight output frame.
7. One output worker owns all writes for one socket.
8. Control messages use a bounded FIFO, while frames use a latest-value slot.
9. A high socket buffer causes an intentional frame skip instead of more
   queued latency.
10. A write that exceeds 250 milliseconds closes the unhealthy destination.

The resulting end-to-end rule is:

```text
move the newest valid frame forward,
but never allow a slow stage to create unlimited retained work
```
