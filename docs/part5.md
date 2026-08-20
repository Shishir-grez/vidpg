# VidPG Code Flow Explanation - Part 5

This explanation assumes no prior knowledge of the project.

The report is being explained in groups of three sections. Sections 1 and 3
are intentionally excluded. This installment covers sections 15, 16, and 17.

The most relevant implementation files for this installment are:

- `web/app.js`
- `web/js/websocket-client.js`
- `web/js/protocol.js`
- `web/js/receiver.js`
- `web/js/uploader.js`
- `web/js/session-link.js`
- `web/js/metrics.js`
- `vidpg/db/buckets.py`
- `vidpg/db/rotation.py`
- `vidpg/relay/websocket.py`
- `vidpg/relay/sessions.py`
- `vidpg/relay/workers.py`
- `vidpg/relay/fanout.py`
- `vidpg/relay/errors.py`
- `vidpg/main.py`
- `tests/e2e/browser/test_remote_render.py`
- `tests/e2e/browser/test_backpressure.py`
- `tests/integration/db/test_rotation.py`
- `tests/integration/relay/test_disconnect_recovery.py`
- `tests/integration/relay/test_websocket_session.py`
- `tests/unit/relay/test_sessions.py`

The central idea in these sections is:

```text
server binary frame
    -> browser protocol decoder
    -> incoming-stream and freshness checks
    -> asynchronous image decode
    -> animation-frame commit
    -> visible image swap

database bucket ring
    -> clear next bucket
    -> advance logged generation state
    -> keep current and previous buckets readable

failure or disconnect
    -> reject or skip at the earliest safe boundary
    -> release ownership
    -> close or retry the affected resource
    -> avoid replaying stale work
```

These sections describe what happens after a frame leaves the server, how the
database keeps its temporary storage bounded, and how the system stops failed
or disconnected components without turning them into permanent backlog.

## 15. Browser Receive and Paint Flow

### 15.1 The receiver is the final freshness boundary

The server output worker sends a valid binary V1 frame to the destination
browser. The browser still does not immediately put those bytes on screen.

The browser must answer several questions first:

```text
Is this a binary frame or a control message?
Is the wire structure valid?
Is it really JPEG data within the V1 limits?
Is it for the stream this browser is supposed to receive?
Is it newer than the image already painted?
Is another image already being decoded?
Can the image be decoded before it becomes visible?
```

The receive path therefore has its own validation and freshness rules. The
server may have performed equivalent checks, but the browser cannot assume
that every message reaching its event handler is safe to display. A client
must protect its own rendering state as well.

The browser code separates these responsibilities:

```text
websocket-client.js -> transport message handling
protocol.js          -> binary wire decoding and validation
receiver.js          -> stream, freshness, decode, and paint state
app.js               -> UI and connection lifecycle
metrics.js           -> browser counters and displayed status
```

### 15.2 The WebSocket is configured for binary data

`connectRelay()` creates a `RelaySocket` around the browser's native
`WebSocket` object.

During construction it sets:

```javascript
socket.binaryType = "arraybuffer";
```

This asks the browser to expose incoming binary WebSocket messages as
`ArrayBuffer` values instead of `Blob` values when possible. The message
handler still supports a Blob-like object as a defensive compatibility path.

The relay object exposes callbacks for the application:

```text
onFrame   -> decoded binary frame
onControl -> JSON control or close notification
onError   -> transport or decode error
```

The object does not itself know how to paint an image. It decodes the
transport message and delegates the resulting frame to the callback installed
by `app.js`.

### 15.3 Text and binary messages take different paths

`RelaySocket._handleMessage()` obtains the event data and separates messages
by type.

For a text message:

1. Parse the text as JSON.
2. Pass the resulting object to `_handleControl()`.
3. Call `onError` if parsing fails.

For a Blob-like binary value:

1. Await `arrayBuffer()`.
2. Decode the resulting bytes with `decodeFrameMessage()`.
3. Call `onFrame` with the decoded object.

For an `ArrayBuffer` or typed-array value:

1. Decode it directly with `decodeFrameMessage()`.
2. Call `onFrame` with the decoded object.

Any binary decode exception is caught and reported through `onError`. It does
not become an unhandled exception in the browser event callback.

The transport layer therefore keeps control JSON and frame bytes separate:

```text
text message   -> JSON control handling
binary message -> fixed V1 frame decoder
```

### 15.4 Recognizing the ready and error controls

Before the browser can receive a session frame, it must complete the join
exchange.

`sendJoin()` waits for the WebSocket to open, sends:

```json
{"type":"join","token":"..."}
```

and waits for one of two relevant control responses.

For a `ready` message:

1. The relay marks itself joined.
2. The pending join promise resolves with the ready object.
3. `app.js` stores that ready object.
4. The browser stores `ready.incoming_stream` as the expected remote stream.

For an `error` message:

1. The relay creates a `RelayError` with the server's code and message.
2. The pending join promise rejects.
3. `app.js` closes and clears the failed relay state.
4. The UI displays the error.

The ready message is therefore more than a status response. It supplies the
stream identity that the browser later uses to reject frames from the wrong
direction.

### 15.5 Decoding the 48-byte header

`decodeFrameMessage()` in `web/js/protocol.js` receives the complete binary
message.

It first checks that the message has at least 48 bytes. It then treats the
first 48 bytes as the fixed header and validates:

1. Protocol version is V1.
2. Message type is a video frame.
3. Codec is JPEG.
4. Reserved flags are zero.
5. Header CRC32 matches the first 44 header bytes.

The decoder then reads:

- Stream UUID.
- Unsigned 64-bit sequence.
- Signed 64-bit capture timestamp.
- Width.
- Height.
- Payload length.

JavaScript uses `BigInt` for the sequence and timestamps where needed. This
avoids losing precision when an integer is larger than the safe integer range
of a normal JavaScript `Number`.

### 15.6 Checking length and JPEG boundaries

After reading the header, the browser validates the body:

```text
payload length must be at least one byte
payload length must be no greater than 524,288 bytes
actual message length must equal 48 + payload length
JPEG must start with FF D8
JPEG must end with FF D9
```

The exact-length check is important. A message shorter than the declared
payload would be truncated. A message longer than the declared payload would
contain trailing bytes that do not belong to the frame.

The marker check is deliberately lightweight. It does not perform a full JPEG
decoder operation; it verifies the boundary markers before the browser asks
the image subsystem to decode the data.

If any check fails, `decodeFrameMessage()` throws a `ProtocolError` with a
machine-readable code such as:

```text
BAD_VERSION
BAD_MESSAGE_TYPE
BAD_CODEC
BAD_FLAGS
BAD_HEADER_CRC
BAD_DIMENSION
BAD_LENGTH
OVERSIZE_PAYLOAD
BAD_JPEG_MARKER
```

### 15.7 Dimensions are checked at receive time

The browser decoder requires V1 image dimensions within:

```text
width:  160 through 1280
height: 120 through 720
```

These limits match the intended camera and protocol envelope. A JPEG with a
valid marker pair but an unsafe or unsupported dimension is still rejected
before rendering.

The browser performs a second equivalent check in
`validateIncomingFrame()`. This is useful because the receiver can be called
with either:

- Raw binary data that needs decoding.
- An already decoded frame object from another caller or test.

Both entry forms must satisfy the same incoming contract.

### 15.8 Validating the expected stream

`app.js` installs the stream from the ready message here:

```javascript
state.remote.expectedStream = ready.incoming_stream;
```

When a frame reaches `handleRemoteFrame()`, the receiver calls:

```javascript
validateIncomingFrame(decoded, state.expectedStream)
```

The function compares the frame's `streamId` with the expected incoming
stream. A mismatch produces:

```text
UNAUTHORIZED_STREAM
```

This is a client-side consistency check. The server has already mapped the
stream to the authenticated destination, but the browser still refuses to
display a frame that claims to belong to another stream.

The rest of `validateIncomingFrame()` checks:

- Frame object shape.
- JPEG codec.
- Sequence type and positive value.
- Dimension range.
- Payload byte type.
- Declared payload length.
- Maximum payload size.
- JPEG start and end markers.

Invalid incoming frames are counted with a reason such as
`REMOTE_BAD_LENGTH` or `REMOTE_UNAUTHORIZED_STREAM` and are discarded.

### 15.9 The first freshness check

After structural and stream validation, the receiver calls:

```javascript
paintIfNewest(decoded, state)
```

The function converts the incoming sequence and the last painted sequence to
`BigInt` and returns true only when:

```text
there is no pending render
and incoming sequence > last painted sequence
```

An older frame that arrives after a newer frame was painted is rejected. This
prevents a delayed network message from moving the visible image backward.

### 15.10 A pending decode blocks another pending frame

The `pendingRender` condition has an important consequence. While one image is
being decoded, a second incoming frame is not placed into another pending
queue. It is skipped.

The behavior is:

```text
frame 10 arrives -> begin decoding frame 10
frame 11 arrives -> skip because a render is pending
frame 10 commits -> pending state clears
frame 12 arrives -> accept frame 12
```

This is another latest-value tradeoff. The browser does not preserve every
remote frame while image decoding is slower than delivery. It protects the
rendering pipeline from building a browser-side backlog.

The wording "newer than any pending frame" can be understood as a pending
render ownership rule in this implementation: the receiver does not allow a
second pending image at all. It waits for the current pending image to finish,
then evaluates the next frame that arrives.

### 15.11 Creating an object URL

When the frame passes freshness checks, the receiver creates a browser `Blob`:

```javascript
new Blob([decoded.payload], { type: "image/jpeg" })
```

It then asks the URL API to create a temporary object URL for that Blob.

The object URL gives an HTML image element a local URL that refers to the
decoded JPEG bytes without converting the payload to a data URL string.

If `URL.createObjectURL` is unavailable, the frame is counted as
`RENDER_UNAVAILABLE` and skipped. The receiver does not pretend that the frame
was painted.

### 15.12 Choosing the pending image element

The page normally provides two remote image elements:

```text
#remote-image
#remote-image-buffer
```

The receiver tracks which image is currently visible. It prefers the inactive
image element as the place to load the next frame.

If no inactive element is available but the browser exposes a global `Image`
constructor, it creates a temporary image with asynchronous decoding enabled.

If neither option is available, the receiver releases the new object URL and
counts `RENDER_UNAVAILABLE`.

Loading into an inactive image prevents the currently visible image from
being changed before the next image has successfully decoded.

### 15.13 Revoking an older pending URL

The receiver keeps a map from image elements to their object URLs.

Before assigning a new URL to an inactive image, it checks whether that image
already holds an older URL. If it does, and that URL is not the currently
visible URL, the old URL is revoked.

Object URLs are browser resources. Revoking them matters because otherwise a
long-running video call could retain many image Blob references even though
only the newest image is useful.

The new frame is then represented by a `pending` record containing:

- Decoded frame metadata and bytes.
- New object URL.
- Image element that will load it.
- The inactive image chosen for the render.

The record is stored in:

```text
state.pendingFrame
state.pendingUrl
state.pendingRender
```

The identity of `pendingRender` is later used to prevent an old asynchronous
callback from committing after the pending state has changed.

### 15.14 Waiting for image load and decode

The receiver assigns the object URL to the pending image's `src`.

When the image's `onload` event fires:

1. If the image has a `decode()` method, call it.
2. Wait for the returned promise when it is promise-like.
3. On successful decode, schedule `commitPaint()` on an animation frame.
4. On decode failure, call `failPending()`.

If no asynchronous `decode()` method is available, the receiver schedules the
paint commit directly after `onload`.

The animation-frame step aligns the visible swap with the browser's rendering
cycle. It avoids changing the visible image in the middle of an unrelated
layout or paint phase.

If the image's `onerror` event fires, the pending frame is failed and its
object URL is revoked.

### 15.15 The commit identity check

`commitPaint(pending, state, image)` first checks:

```javascript
if (state.pendingRender !== pending) {
  return false;
}
```

This prevents an asynchronous callback belonging to an older pending load
from committing after another cleanup path has already replaced or cleared
the pending state.

The function then checks the sequence one more time against
`lastPaintedSequence`. This is necessary because time passed between the first
freshness check and the asynchronous decode callback.

If the sequence is no longer newer, the object URL is revoked and the commit
is abandoned.

### 15.16 Swapping the visible image

When a pending frame is valid at commit time, the receiver chooses the
preloaded image as the next visible image.

For a double-buffered page:

1. Make the next image visible.
2. Make the old image invisible.
3. Store the new image as `activeImage`.

For a single-image or temporary-image path, the receiver assigns the pending
URL directly when needed.

Then it updates:

```text
currentUrl       -> new object URL
pendingUrl       -> null
pendingFrame     -> null
pendingRender    -> null
lastPaintedSequence -> committed sequence
```

The old visible URL is revoked when it is no longer needed. The old image's
load and error callbacks are cleared so that stale events cannot mutate later
state.

Only after these state changes does the receiver call `recordRendered()`.
The rendered metric therefore represents a committed visible frame, not merely
a frame that arrived or began decoding.

### 15.17 The visible-image example

With two image elements, the normal sequence looks like this:

```text
visible image A shows frame 20
frame 21 arrives
load frame 21 into inactive image B
decode frame 21
animation frame arrives
show image B
hide image A
revoke frame 20 URL
image B is now the active visible image
```

The next frame uses image A as the inactive buffer. This alternating pattern
keeps the currently visible image stable while the next image is prepared.

The browser end-to-end test
`test_remote_double_buffers_visible_images()` checks this visible swap.

### 15.18 Failed image decode cleanup

`failPending()` runs only if the callback still belongs to the current pending
record.

It:

1. Revokes the failed object's URL.
2. Removes the URL from the image map.
3. Clears pending render, frame, and URL state.
4. Records `REMOTE_DECODE_ERROR`.

It does not change `lastPaintedSequence`, because no new frame became visible.
The previous visible frame remains the last committed image.

### 15.19 Explicit receiver reset

`releasePreviousImage()` is called when the user disconnects or when the relay
connection closes.

It clears handlers on the pending image and gathers all known URLs from:

- Current visible URL.
- Pending URL.
- URLs held in the image map.

It revokes every URL, clears pending state, removes image sources, resets
visibility, restores the primary image as active, and clears the URL map.

This is a full rendering-state reset. A later connection does not inherit an
old object URL or an old pending decode operation.

### 15.20 Browser connection errors and close events

`RelaySocket` handles native WebSocket `error` and `close` events separately.

For an error:

- A connection that has not opened rejects its `open` promise.
- A pending join rejects.
- `onError` receives an error object.

For a close:

- An unopened connection rejects its `open` promise.
- A pending join rejects as closed before ready.
- `onControl` receives a close object containing the close code.

`app.js` handles the close notification for the current relay instance by:

1. Displaying a connection-closed status.
2. Stopping the camera.
3. Releasing previous and pending remote images.
4. Clearing `state.relay`.
5. Clearing the ready message.
6. Updating the UI controls.

The app does not automatically reconnect in this path. The user can connect
again explicitly after the failed or closed socket has been cleared.

### 15.21 Camera and upload failures visible to the receiver flow

The browser's upload loop has related failure behavior:

- A frame larger than `maxFrameBytes` is skipped locally.
- A closed socket causes a local skip.
- A high socket `bufferedAmount` causes a local skip.
- An encoding error records the error and stops the capture loop.
- A send exception records a send error.

These local checks reduce unnecessary traffic and make the user-facing metrics
explain why a frame did not leave the browser. The server still performs its
own checks because a client-side decision is not an authorization boundary.

### 15.22 Browser metrics for receive and paint

`metrics.js` distinguishes several stages:

```text
captured -> image was encoded
sent     -> WebSocket accepted an outgoing frame
rendered -> remote image was committed visibly
skipped  -> a frame was intentionally or defensively dropped
```

The browser also records:

- Last sent sequence.
- Last rendered sequence.
- Bytes sent.
- Bytes rendered.
- Counts by skip reason.
- Last capture and render times.

The page displays a compact snapshot containing values such as:

```text
encoded 10 | sent 8 | rendered 7 | skipped 3
```

This makes it possible to distinguish transport throughput from visible
render throughput. A frame can be sent successfully but not yet rendered, or
it can be skipped before any network send.

### 15.23 Section 15 tests

The browser receive tests cover the most important state transitions:

- `test_remote_old_sequence_not_painted()` verifies that a late older frame
  cannot replace the visible newer frame.
- `test_remote_frame_preloads_before_visible_swap()` verifies that the visible
  image is not changed until the pending image loads.
- `test_remote_drops_new_frames_while_decode_is_pending()` verifies that the
  receiver does not create an unbounded pending decode queue.
- `test_remote_double_buffers_visible_images()` verifies inactive-image loading
  and visibility swapping.
- `test_browser_frame_protocol_roundtrip_and_crc()` verifies browser protocol
  decoding and CRC rejection.
- `test_browser_frame_matches_python_wire_contract()` verifies that a browser
  encoded message can be parsed by the Python protocol implementation.
- `test_high_buffer_amount_skips_new_capture()` verifies browser-side send
  backpressure.

The receiver's practical guarantee is:

```text
only a valid, expected-stream, newer frame
that finishes decoding can become visible
```

## 16. PostgreSQL Bucket Rotation

### 16.1 Why the database uses a bucket ring

The relay stores frames temporarily. It does not want to execute a delete for
every expired frame or let old rows grow forever.

Instead, the schema has three physical payload tables:

```text
frame_bucket_0
frame_bucket_1
frame_bucket_2
```

The tables form a ring. At any generation:

```text
active   -> accepts new inserts
previous -> remains readable for fetches
next     -> is safe to clear before it becomes active
```

The maintenance loop clears the next bucket as a whole with `TRUNCATE`. This
keeps the amount of retained frame data bounded by a small number of time
windows rather than by the total lifetime of the process.

### 16.2 Generation-to-bucket arithmetic

`vidpg/db/buckets.py` provides fixed allowlisted arithmetic.

For generation `g`:

```text
active   = g modulo 3
previous = (g + 2) modulo 3
next     = (g + 1) modulo 3
```

At generation zero:

```text
active   = bucket 0
previous = bucket 2
next     = bucket 1
```

At generation one:

```text
active   = bucket 1
previous = bucket 0
next     = bucket 2
```

At generation two:

```text
active   = bucket 2
previous = bucket 1
next     = bucket 0
```

The helpers reject negative, oversized, boolean, and otherwise invalid
generation values. `bucket_table()` accepts only the integers 0, 1, and 2 and
returns one of three fixed SQL table names.

This allowlist is both a correctness rule and a SQL-safety rule. A bucket
number derived from state is never interpolated into SQL until it has been
validated against the fixed table list.

### 16.3 The maintenance loop

`RelayService._rotation_loop()` owns one maintenance connection.

The configured bucket interval defaults to five seconds. The loop:

1. Opens the maintenance connection if one is not already open.
2. Marks the rotation connection as available.
3. Calls `run_rotation_once(connection, 250)` through `asyncio.to_thread()`.
4. Stores the resulting generation and active bucket for status reporting.
5. Sleeps for `settings.bucket_seconds`.
6. Repeats while the service is alive.

The call is moved to a worker thread because Psycopg operations are blocking
database operations and the rest of the service uses asyncio tasks.

The first rotation attempt happens as soon as the maintenance connection is
opened. Later attempts occur after the configured sleep interval.

The connection is reused across attempts. If an exception makes it unusable,
the service closes it, clears the connection reference, waits briefly, and
opens a new one.

### 16.4 The advisory lock

`run_rotation_once()` first attempts to acquire a PostgreSQL advisory lock:

```sql
SELECT pg_try_advisory_lock(%s)
```

The lock uses a fixed numeric key shared by all potential rotation workers.
Advisory locks are application-coordination locks. PostgreSQL does not know
that this key means "VidPG bucket rotation"; the VidPG code agrees to use it
for that purpose.

`pg_try_advisory_lock()` is non-blocking. It returns a boolean:

```text
true  -> this maintenance attempt owns the rotation lock
false -> another rotation attempt already owns it
```

The non-blocking choice prevents two maintenance processes from waiting on
each other while the frame plane continues operating.

If the lock is busy, the function reads the current state and returns a
`RotationResult` with:

```text
advanced       = false
lock_acquired  = false
failure_reason = "advisory_lock_busy"
```

The active generation is not changed.

### 16.5 Beginning a clean rotation transaction

The maintenance connection is transaction-capable. Before attempting the
advisory lock, `run_rotation_once()` rolls back any leftover transaction on a
non-autocommit connection.

After acquiring the advisory lock, it opens a transaction for the actual
rotation work.

Inside that transaction it sets a local PostgreSQL lock timeout:

```sql
SELECT set_config('lock_timeout', '250ms', true)
```

The third argument makes the setting local to the transaction. A future frame
insert or unrelated operation on the same connection does not inherit an
unexpected permanent lock timeout.

The timeout protects rotation from waiting indefinitely for a relation lock
held by another database operation.

### 16.6 Reading and validating the control state

The rotation transaction reads:

```sql
SELECT generation, active
FROM vidpg.bucket_state
WHERE singleton
```

It requires exactly the expected singleton row and verifies:

```text
active == generation modulo 3
```

If the control state is missing or inconsistent, rotation raises rather than
guessing which table is safe to clear.

This same invariant is checked by the writer and fetcher. All three database
roles use the same generation arithmetic so that:

```text
writer destination
fetch readable tables
rotation cleanup table
```

remain mutually consistent.

### 16.7 Clearing the next bucket before activation

The transaction selects:

```python
next_selected = next_bucket(generation)
```

It then executes a fixed-table statement such as:

```sql
TRUNCATE TABLE vidpg.frame_bucket_1
```

Only after that succeeds does it update the logged control row:

```sql
UPDATE vidpg.bucket_state
SET generation = generation + 1,
    active = (active + 1) % 3,
    switched_at = clock_timestamp()
WHERE singleton
```

The order is essential:

```text
clear old contents of next bucket
    -> publish that bucket as the new active bucket
```

The system never intentionally activates a bucket while its old generation's
rows are still present.

### 16.8 The transaction commit boundary

The truncate and control-row update run inside one transaction context.

When both succeed, the transaction commits and the function returns a result
such as:

```text
advanced          = true
lock_acquired     = true
active_before     = 0
active_after      = 1
truncated_bucket  = 1
```

The logged control state therefore changes only together with the cleanup
operation.

If the transaction fails before commit, PostgreSQL rolls it back. The old
generation and active bucket remain authoritative. This is the safety reason
the fetch path can continue reading the old active and previous tables after a
failed rotation.

### 16.9 Releasing the advisory lock

The function releases the advisory lock in a `finally` block:

```sql
SELECT pg_advisory_unlock(%s)
```

The unlock helper first rolls back any open transaction on a non-autocommit
connection, performs the unlock, and commits that connection state.

The `finally` block means both successful and failed attempts attempt to
release the lock. If unlock itself fails, the code logs the failure rather
than hiding the original rotation result.

### 16.10 The three-generation example

Start at generation zero:

```text
active bucket 0: new frames
previous bucket 2: readable old frames
next bucket 1: disposable old contents
```

Rotation one:

```text
TRUNCATE bucket 1
generation becomes 1
active becomes bucket 1
```

Now:

```text
active bucket 1: new frames
previous bucket 0: readable frames from the prior window
next bucket 2: disposable old contents
```

Rotation two:

```text
TRUNCATE bucket 2
active becomes bucket 2
```

Rotation three:

```text
TRUNCATE bucket 0
active becomes bucket 0
```

The ring repeats. At each point, only the active and immediately previous
physical buckets are part of the normal read path.

### 16.11 Why the fetcher reads two buckets

The rotation policy and fetch policy are designed together.

The active bucket can receive new rows. The previous bucket contains rows from
the immediately preceding generation and remains readable so a frame near the
rotation boundary is not lost immediately.

The third bucket is being cleared or has already been cleared. It is not part
of ordinary fetch selection.

This gives the relay a short retention window without requiring every fetch to
search all historical tables.

The notification can mention the bucket where the row was inserted, but a
delayed fetch must use the current generation state to decide whether that
bucket is still one of the two safe readable tables.

### 16.12 Rotation and concurrent inserts

The writer chooses the active bucket from the control state. Rotation always
clears the next bucket, not the active bucket, before moving the active marker.

This makes the normal race safe:

```text
writer reads bucket 0 as active
rotation clears bucket 1 and publishes bucket 1
writer inserts into bucket 0
```

After rotation, bucket 0 is the previous readable bucket, so the frame remains
eligible for fetch.

Conversely, if the writer reads the state after the rotation commits, it
chooses the new active bucket. The writer does not use a permanently cached
table name.

The control-row invariant and fixed bucket roles prevent the cleanup worker
from intentionally truncating the table selected as active for that same
generation.

### 16.13 Lock timeout behavior

`TRUNCATE` needs relation-level locks. Another operation can hold a conflicting
lock long enough that rotation cannot proceed immediately.

The local lock timeout turns that wait into a controlled result. For recognized
lock-timeout SQL states, `run_rotation_once()`:

1. Rolls back the failed transaction.
2. Leaves generation unchanged.
3. Leaves the old active bucket unchanged.
4. Returns `advanced=False` with `failure_reason="lock_timeout"`.
5. Releases the advisory lock in the `finally` block.

The integration test `test_lock_timeout_keeps_old_active()` creates a blocking
connection and verifies that a timed-out rotation does not publish a new
active bucket.

Other database errors are re-raised because they may indicate a more serious
problem than a temporary lock conflict.

### 16.14 Busy advisory lock behavior

An advisory-lock conflict is different from a relation lock timeout.

With a busy advisory lock, this attempt never enters the rotation transaction.
It reads and reports current state instead. No truncate and no active-bucket
update occur.

This allows a second maintenance process to safely observe that another
process is responsible for rotation rather than performing duplicate cleanup.

The standalone `rotation_loop()` counts non-advancing attempts and logs a
warning after the configured repeated-failure threshold. The service loop also
stores the last rotation failure reason for operational status reporting.

### 16.15 Connection failure and reconnect behavior

The service loop separates rotation connection health from rotation state.

If opening or using the maintenance connection raises:

1. `rotation_connected` becomes false.
2. `last_rotation_error` is set to a connection-failure marker.
3. The current connection is closed if possible.
4. The connection reference is cleared.
5. The loop waits for the retry interval.
6. A fresh maintenance connection is attempted.

When the connection opens, PostgreSQL version and session settings are checked
before it is used. The service does not treat a socket to an unverified server
as a valid rotation plane.

### 16.16 Rotation readiness and status

`RelayService.rotation_ready` returns true only when the maintenance
connection is connected and at least one rotation result has established a
generation value.

The operational endpoints expose rotation facts alongside database status,
including:

- Rotation connection state.
- Current generation.
- Current active bucket.
- Last rotation error when available.
- Bucket sizes from the database probe.

This is separate from the basic process health check. The HTTP process can be
running while rotation is unavailable, and the readiness/status path is where
that database-plane distinction is reported.

### 16.17 Why rotation uses `TRUNCATE` instead of row deletes

Deleting expired rows one by one would create work proportional to the number
of frames and would produce additional index and vacuum pressure.

The bucket ring allows one table-level cleanup operation per rotation window:

```text
old generation rows -> one TRUNCATE
```

The tradeoff is intentional. Frames older than the readable window are not
preserved. The gain is predictable storage and a simpler expiration path for
a live-preview experiment.

### 16.18 Section 16 tests

The rotation contract tests verify:

- Truncation is issued before the active-state update.
- The SQL table name comes from the fixed bucket allowlist.
- Generation arithmetic maps active, previous, and next tables correctly.
- Client-supplied invalid bucket values are rejected.
- The schema declares one logged control table and three unlogged payload
  tables.

The integration tests verify:

- The next bucket is emptied before the next generation becomes active.
- A successful rotation advances generation and active bucket exactly once.
- A lock timeout keeps the previous active state.

The practical rotation guarantee is:

```text
clear the table that will become active
    -> publish the new generation
    -> keep only current and previous generations readable
```

## 17. Failure and Disconnect Behavior

### 17.1 Failure handling is boundary-specific

VidPG does not have one universal failure response. The correct response
depends on where the failure occurs:

```text
bad input       -> reject before it enters the next stage
temporary DB    -> release ownership and retry or continue with newer work
bad destination -> skip or close that socket
disconnect      -> clear per-client state
idle session    -> cancel workers and remove the room
process shutdown -> cancel and close all owned resources
```

The general rule is to stop invalid or unproductive work at the earliest
boundary that can make a safe decision.

### 17.2 Invalid browser session links

The browser session link stores the capability secret in the URL fragment:

```text
https://example.test/?session=<uuid>&side=a#token=<secret>
```

`parseSessionLink()` rejects links when:

- The token appears in a query parameter instead of the fragment.
- The token fragment is missing.
- The session value is not a UUID.
- The side is not exactly `a` or `b`.
- The secret is not 64 lowercase hexadecimal characters.

Keeping the secret in the fragment means the browser sends it to the page's
JavaScript but does not include it in the normal HTTP request path.

`app.js` catches link parsing errors during initialization and shows a neutral
initial status. It also catches errors while creating a session or copying a
peer link and displays the error in the UI.

An invalid link does not open a relay socket and therefore does not consume a
server session or worker.

### 17.3 Invalid join messages

The server accepts the WebSocket transport before it performs application
authentication. A client still has only a short window to send the first
valid join message.

`read_join_message()` rejects:

- A first message that is binary instead of text.
- A control message over 8,192 UTF-8 bytes.
- Invalid JSON.
- A JSON value that is not an object.
- A missing or wrong `type` value.
- A missing or incorrectly shaped token.
- A token that fails the capability-secret format.

The endpoint converts expected failures into an error JSON message and a
stable WebSocket close code. Examples include:

```text
MALFORMED_JOIN -> 4000
BAD_SECRET     -> 4001
JOIN_TIMEOUT   -> 4005
```

The join timeout is five seconds. A client that opens a transport but never
authenticates cannot hold a server-side session indefinitely.

### 17.4 Invalid secrets and duplicate sides

After the join message has valid syntax, `SessionRegistry.join()` performs
capability authorization.

The secret is compared against the stored session secret hash. A wrong secret
does not reveal whether other session details are valid; it produces the
stable `BAD_SECRET` response.

The registry also rejects a side that is already actively occupied. The
expected error is:

```text
DUPLICATE_SIDE -> 4002
```

This prevents two normal active socket generations from silently sharing one
side's upload and output state.

The integration test
`test_wrong_secret_closes_with_the_explicit_bad_secret_code()` verifies both
the error JSON and close code.

### 17.5 Invalid binary frames

After authentication, binary messages pass through the server's admission
pipeline.

The server rejects or closes for conditions such as:

- Non-binary data where a frame is expected.
- A WebSocket message larger than the maximum header-plus-payload size.
- Truncated or malformed V1 headers.
- Header CRC mismatch.
- Unsupported version, type, codec, or flags.
- Invalid dimensions.
- Invalid JPEG markers.
- Declared length mismatch.
- A wrong stream ID.
- A sequence that is not newer than the stream watermark.
- An input slot offer that is rejected as stale.

The ownership check occurs before any PostgreSQL writer call. A client cannot
make the relay write a valid frame into another side's directional stream by
choosing a different stream UUID in the header.

For a rejected admission, the endpoint increments validation and rejection
metrics, then raises the relay error path. The frame has no database side
effect.

### 17.6 Browser-side skips before network transmission

The browser tries to avoid sending work that is already known to be unusable.

The uploader can skip a capture because of:

```text
RATE_GATE
ENCODE_BUSY
OVERSIZE_PAYLOAD
SOCKET_NOT_OPEN
BUFFERED_AMOUNT_HIGH
ENCODE_ERROR
SEND_ERROR
```

An oversize image, for example, is recorded as a skip rather than sent only to
be rejected by the server. A high browser `bufferedAmount` also causes a
local skip so the client does not add more network backlog.

These are optimizations and observability decisions, not security decisions.
The relay repeats its own validation because a malicious or buggy client can
send bytes without using the browser uploader.

### 17.7 Encoding and camera failures

If camera acquisition fails, the browser's camera module converts the native
error into a user-facing camera error state. The app does not start a capture
loop without a camera stream.

If an active encoder raises while a capture is being processed, the uploader:

1. Stores the error in capture state.
2. Records `ENCODE_ERROR`.
3. Stops the capture loop.

This prevents a repeatedly failing encoder from scheduling an endless stream
of failed capture attempts.

Stopping the camera also stops its track and disposes of the encoder through
the app lifecycle cleanup.

### 17.8 PostgreSQL pool and connection failures

The writer and fetcher borrow connections from bounded pools. Opening a
connection can fail because PostgreSQL is unavailable, the server version is
unsupported, or the connection timeout expires.

Those errors propagate to the worker that requested the connection.

The insert worker handles a write exception by:

1. Incrementing `vidpg_pg_insert_error_total`.
2. Releasing the failed frame's in-flight ownership.
3. Waiting for the short retry delay.
4. Continuing its loop.

The failed frame is not counted as inserted or committed. A newer waiting
frame can still be retained according to the input slot policy.

The fetch worker handles a fetch exception by:

1. Incrementing `vidpg_pg_fetch_error_total`.
2. Clearing `fetch_in_progress`.
3. Setting `fetch_event` again.
4. Waiting for the retry delay.
5. Trying again while the stream remains active.

Leaving the event set is important. A transient fetch error must not turn a
known dirty stream into a permanently idle stream.

### 17.9 A failed insert is not a committed frame

The writer validates and commits an insert before the input worker clears
in-flight ownership.

The state transitions are therefore:

```text
frame in flight
    -> database commit succeeds
    -> clear in-flight ownership
    -> notification becomes visible
```

or:

```text
frame in flight
    -> database operation raises
    -> rollback occurs in the writer
    -> fail in-flight ownership
    -> no committed insert is claimed
```

The worker does not create a fake notification for the failed attempt. The
next successful frame will create its own committed signal.

### 17.10 Listener failures and recovery

The dedicated PostgreSQL listener can fail independently from the writer and
fetch pools.

When `_listen_loop()` catches a listener exception, it:

1. Marks `listener_connected` false.
2. Stores a listener failure state.
3. Logs that the listener is unavailable.
4. Closes the dedicated connection if possible.
5. Waits one second.
6. Opens a new listener connection.
7. Executes `LISTEN` again.
8. Rescans the active and previous buckets.
9. Resumes notification processing.

The rescan is essential because notifications that occurred while the old
connection was down cannot be assumed to be waiting on the new connection.

The listener path can therefore recover without restarting the entire web
process, although readiness and status endpoints show that the relay was not
fully ready during the interruption.

### 17.11 Rotation failures

Rotation failure does not immediately publish a new generation.

For a busy advisory lock or recognized lock timeout:

- The active bucket remains unchanged.
- The logged generation remains unchanged.
- The old active and previous read path remains authoritative.
- The failure reason is exposed to operational state.
- A later rotation attempt can retry.

For a maintenance connection error, the service closes and recreates the
connection after a delay.

This is safer than guessing that the truncate succeeded or advancing the
control row after a partial operation. The rotation transaction is the source
of truth for whether a generation actually changed.

### 17.12 Output buffering failures

The server checks the destination socket's buffered amount before building and
sending a binary frame.

If the buffered amount is above the threshold:

```text
skip frame
clear output in-flight ownership
increment buffered-drop metrics
continue with newer output work
```

The socket remains connected. This is a controlled freshness drop, not a
transport failure.

The browser has a symmetric check before sending its own frame. Both ends
therefore attempt to avoid adding work to a network path that is already
behind.

### 17.13 Output write timeouts

If a control or binary output send does not complete within 250 milliseconds,
the output worker treats the socket as unhealthy.

For a binary frame it:

1. Increments the client's timeout count.
2. Fails the frame's in-flight ownership.
3. Closes the socket with the output-timeout close code.
4. Stops the output worker.

For a control message, the worker also closes the socket after the bounded
send fails or times out.

The relay does not allow one destination connection to block all future output
forever. The affected client must reconnect to obtain a fresh socket
generation.

### 17.14 Control queue overflow

Control messages use a bounded FIFO capacity of 32.

If the queue is full, `enqueue_control()` raises `ControlOverflowError`. The
error uses the output-timeout close path because a client that cannot accept
its required control responses is not making safe progress.

Video frames do not replace control messages. Conversely, control messages do
not cause an unlimited frame queue. The two types of output have separate
failure and retention policies.

### 17.15 Network disconnect cleanup on the server

The WebSocket endpoint catches `WebSocketDisconnect` around its receive loop.
Regardless of whether the disconnect is normal or caused by a network error,
the `finally` path cancels the output worker and calls:

```python
service.detach_client(session_id, side, socket)
```

`detach_client()` first checks that the session still exists and that the
client state still owns this exact socket object. If so, it unsubscribes the
client from fanout and removes the socket from the session registry.

Fanout cleanup removes the subscriber mapping and resets the client's output
state:

- Waiting output frames are discarded.
- In-flight output ownership is cleared as the worker ends.
- Pending control messages are cleared.
- The output event is cleared.

The disconnected peer therefore does not receive an old queued frame if it
later reconnects.

### 17.16 Browser disconnect cleanup

When the native WebSocket emits `close`, the browser relay object forwards a
close control event to the app.

For the currently active relay instance, `app.js`:

1. Stops the camera capture.
2. Releases all remote object URLs and image state.
3. Clears the relay reference.
4. Clears the ready message.
5. Updates controls and status.

The `beforeunload` handler performs the same basic resource release before the
page exits:

```text
stop camera
close relay
```

The browser does not keep rendering from a stale pending image after the relay
has been cleared because `releasePreviousImage()` removes pending callbacks
and revokes the URLs.

### 17.17 Socket replacement and stale generations

The server tracks the current socket object for each session side.

When a new socket is attached to a side that already has an active socket,
`SessionRegistry.attach_socket()`:

1. Schedules the old socket to close with `CLOSE_REPLACED` (4009).
2. Marks the old client state closed.
3. Resets its output and control state.
4. Installs a new `ClientState` containing the new socket.

The identity check in `remove_socket()` is important after this replacement.
If the old endpoint finishes its `finally` block later, it passes the old
socket object. The registry sees that the current side now owns a different
socket and does nothing.

Without this check, an old disconnect could accidentally remove the new
connection's ownership and subscription.

The unit test `test_same_side_reconnect_replaces_socket_and_disconnect_can_rejoin()`
verifies the replacement close and later rejoin behavior.

### 17.18 Duplicate join versus replacement

The normal join path rejects an already active side as a duplicate. Replacement
is a separate socket-attachment lifecycle used when the registry decides that
the new socket should take over the current generation.

These are different safeguards:

```text
duplicate join -> do not let two active normal joins share a side
socket replacement -> close stale transport and preserve one current owner
```

Both policies preserve the invariant that one side has one current client
state and one current output worker.

### 17.19 Session idle expiry

Sessions are stored in memory and are intentionally temporary.

When the last active socket is removed, `SessionRegistry.remove_socket()` sets
`last_disconnect_at`. If a side reconnects before expiry, the timestamp is
cleared.

The default idle expiry is 60 seconds. The service sweeper runs approximately
once per second and calls `prune_expired()`.

When a session has been disconnected for at least the idle period, the
registry:

1. Removes it from the session map.
2. Marks the session removed.
3. Marks both directional stream states closed.
4. Sets their input and fetch events so waiting workers wake up.
5. Marks all client states disconnected.
6. Returns the expired sessions to the service.

The service then cancels and gathers the session's stream worker tasks.

This prevents abandoned sessions and their workers from accumulating forever.

### 17.20 Worker shutdown after expiry

An insert or fetch worker can be waiting on an asyncio event when a session
expires. Marking the stream closed and setting the event wakes it.

The worker checks the closed flag and returns. The service also explicitly
cancels the task group through `_stop_session_workers()` and waits for all
tasks to finish.

The output worker uses the client closed flag and socket identity in its loop
condition. It exits when the client is disconnected or replaced.

The service does not leave background tasks attached to a removed room.

### 17.21 Full service shutdown

FastAPI's application lifespan calls `relay.close()` during process shutdown.

The relay close sequence is:

1. Mark the service closed.
2. Gather all stream worker tasks.
3. Cancel the listener task.
4. Cancel the maintenance task.
5. Cancel the session sweeper.
6. Await all cancelled tasks with exceptions collected.
7. Clear the stream-task registry.
8. Remove all sessions and mark their clients and streams closed.
9. Close the writer pool.
10. Close the fetch pool.
11. Clear listener and rotation connected flags.

The listener and maintenance loops also close their dedicated connections in
their `finally` blocks. This gives process shutdown explicit ownership over
every background resource rather than relying on interpreter cleanup.

### 17.22 Failure metrics and operational evidence

Failures are recorded at the boundary where they happen.

Examples include:

```text
browser skipped frame by reason
relay validation rejection
relay ingress replacement
PostgreSQL insert error
PostgreSQL fetch error
notification received
fetch coalesced
output replacement
buffered output drop
output timeout
render decode error
```

The browser exposes its counters in the page. The relay exposes counters in
`/metrics` and status information through `/api/status`. Database probes expose
connection, schema, bucket, WAL, and notification-queue facts.

This separation makes it possible to distinguish:

```text
producer failure
relay rejection
database outage
listener outage
receiver backpressure
browser decode failure
```

instead of treating all missing frames as one undiagnosed "video problem."

### 17.23 Failure behavior matrix

The main V1 outcomes can be summarized as:

```text
invalid session link       -> browser rejects; no socket
invalid join                -> error control + close
bad secret                  -> BAD_SECRET + close 4001
wrong stream                -> reject before PostgreSQL
oversize browser frame     -> local skip
oversize server frame      -> admission rejection
stale sequence             -> reject or latest-slot rejection
insert failure             -> rollback, release frame, retry worker
fetch failure              -> dirty event remains set, retry worker
listener failure           -> close, wait, reconnect, rescan
rotation lock busy         -> no generation change, report failure
rotation lock timeout      -> rollback, no generation change, retry later
high output buffer         -> skip one frame, keep socket
output timeout             -> close affected socket
socket disconnect           -> clear output/control state
socket replacement          -> close old generation, preserve new owner
idle session                -> cancel workers and remove room
service shutdown            -> cancel tasks and close resources
```

### 17.24 Section 17 tests

The session and browser tests exercise these failure contracts:

- Session-link tests verify that secrets remain in fragments and malformed
  links are rejected.
- WebSocket session tests verify authenticated ready responses omit the secret.
- Wrong-secret tests verify stable error JSON and close code 4001.
- Admission tests verify wrong streams are rejected before the database
  boundary.
- Browser remote-render tests verify stale frames and decode-pending frames do
  not replace the visible image.
- Backpressure tests verify a high browser buffered amount skips capture work.
- Rotation tests verify failed rotation leaves old active state unchanged.
- Session tests verify socket replacement and 60-second idle expiry.
- Disconnect recovery verifies that a replacement browser receives a future
  frame rather than an old queued frame from before disconnect.

The disconnect recovery scenario is:

```text
side B connects
side B disconnects
side A sends sequence 2
side B reconnects
side A sends sequence 3
side B receives sequence 3
```

The test does not expect sequence 2 to be replayed to the replacement socket.

### 17.25 The main failure-handling lesson

V1 treats freshness and ownership as more important than retrying every old
piece of work.

The system retries infrastructure operations when retrying can make progress:

- Fetches retry after a temporary database error.
- Listener connections reconnect and rescan.
- Rotation attempts happen again later.

The system drops or closes work when retaining it would be harmful or
misleading:

- Invalid frames are rejected.
- Old waiting frames are replaced.
- High-buffer output frames are skipped.
- Failed in-flight frames are released.
- Disconnected output is discarded.
- Expired bucket rows are truncated.

The result is not "nothing can be lost." The result is that every loss or
retry occurs at an explicit boundary with a bounded resource policy and an
observable reason.

## Main operational lesson from sections 15, 16, and 17

The final stages of the system have three independent safety policies:

```text
browser receiver
    -> only valid, expected-stream, newer frames can become visible

bucket rotation
    -> clear the next table before advancing the logged generation

failure cleanup
    -> release, retry, close, or expire the affected owner without replaying
       stale work into a new connection
```

The complete downstream path is:

```text
server WebSocket bytes
    -> browser header and payload validation
    -> expected stream validation
    -> sequence freshness check
    -> object URL and image decode
    -> animation-frame commit
    -> visible image swap
```

At the same time, the database maintenance path is:

```text
maintenance connection
    -> advisory lock
    -> read generation and active bucket
    -> truncate next bucket
    -> update logged control row
    -> commit
    -> release advisory lock
```

And the failure path is:

```text
failure
    -> identify the owning boundary
    -> reject bad input or release failed work
    -> retry only when newer progress remains useful
    -> close unhealthy sockets
    -> remove idle sessions
    -> cancel and close resources on shutdown
```

Together, these rules prevent three common live-video failures:

1. A delayed image overwrites a newer image on screen.
2. Temporary frame storage grows forever.
3. A dead socket, failed database operation, or abandoned room retains work
   indefinitely.

The V1 design instead accepts bounded gaps and temporary loss in exchange for
freshness, predictable storage, and recoverable ownership.
