# Design notes

## Decisions

- **Metrics are append-only cumulative counters.** Each `MetricsWriter` holds
  `produced`, `consumed`, `backpressure_events`, `crc_errors`, `seq_errors`,
  `min/max/sum_latency_ns`, `last_ts_ns` and `lat_bucket[]`. They are never
  zeroed on claim/reclaim; a writer accumulates into `MxLocal` and flushes
  base+delta, so totals survive slot reuse.
- **`mx_local_begin(MxLocal *, SharedMetrics *, int)`** takes a **non-const**
  `SharedMetrics *` (it must read the current values to seed the base) and the
  writer slot index.
- **Seqlock normalization.** An odd `seq` left by a crashed writer is
  normalized to even at `mx_local_begin` / `mx_flush`, so readers never spin
  forever on a dead writer.
- **`shm_leave`** performs an identity seqlock flush (publishing any final
  delta) and then sets `pid = 0`. Callers must call `mx_flush()` first;
  producer/consumer/stress all do. This keeps the last counters visible while
  marking the slot free.
- **Stale-segment detection.** `shm_join` warns (stderr) when it finds
  `refcount > 1` yet no *other* writer slot has a live pid, then re-initializes.
- **`sem_timedwait` returns `ETIMEDOUT`, not `EAGAIN`, on timeout** — both the
  producer and consumer wait loops retry on `EAGAIN || EINTR || ETIMEDOUT`.
- **Idle-flush.** `acquire_free_slot` / `acquire_occupied_slot` call
  `mx_flush()` whenever `local->unsynced > 0` on a timeout tick, so an idle
  consumer still publishes its final (<64-message) delta and the produce/
  consume totals converge at end of run.
- **Consumer `duration_s` is wall-clock from start** (not `now - first_msg`), so
  a consumer that starts before any message still terminates on time. The
  consumer also writes `last_ts_ns` so the UI can show "last seen".
- **Bridge default frame interval is 50 ms**; the socket is
  `/tmp/shm_bridge_<instance_id>.sock` with `umask(077)`.
- **Server `BIN_DIR`** is resolved as `<project-root>/build` by walking up from
  `__dirname` (the compiled file lives at `server/dist/server/src/`), so
  `npm start` finds the native binaries regardless of cwd.
- **Client build is `tsc --noEmit && vite build`** per the spec.
- **Message layout is verified at compile time** with `_Static_assert`s:
  `sizeof(MessageSlot) == 320`, `offsetof(SharedRingBuffer, {magic,enqueue_pos,
  dequeue_pos,slots}) == 0/64/128/192`, `sizeof(SharedRingBuffer) == 327872`,
  `sizeof(MetricsWriter) == 128`, `sizeof(SharedMetrics) == 8256`, and
  `ATOMIC_LLONG_LOCK_FREE == 2 && ATOMIC_INT_LOCK_FREE == 2`.

## Deviations

- **Bridge copies the full payload (≤ 256 B) into the frame snapshot**, not just
  the 32-byte preview, so `crc_ok` is a real end-to-end integrity check. The
  human-readable `preview` field is still capped at 32 bytes.
- **The Definition-of-Done "start the three binaries, then `kill %1`" step is
  replaced by `make demo-check`**, which runs the three binaries with a built-in
  duration/`timeout`, validates the bridge output in Node, and cleans up. The
  old `kill %1` line hung waiting on an infinite (duration 0) bridge/producer.
- **`scripts/validate_bridge.js` is an extra file** beyond the spec FILE LIST.
  It is referenced by `make demo-check` and implements GATE 2b (JSON parse,
  `1 ≤ filled slot_states count ≤ 1024`, `enqueue_pos` strictly increasing,
  writer/CRC checks). Because `socat` is not installed, all socket gates are
  validated in Node with the built-in `net` module.
- **`BIN_DIR` default** is the project-root `build/`, not a literal `../build`
  relative to `server/dist` (that would resolve to `server/build/`).

## Package pin deltas

- **None.** The client pins exactly match the spec (`react` 18.3.1,
  `react-dom` 18.3.1, `lucide-react` 0.468.0, `vite` 5.4.11,
  `@vitejs/plugin-react` 4.3.4, `typescript` 5.6.3, `tailwindcss` 3.4.17,
  `postcss` 8.4.49, `autoprefixer` 10.4.20, `@types/react` 18.3.12,
  `@types/react-dom` 18.3.1).
- Server pins (not specified): `ws` 8.18.0, `typescript` 5.6.3,
  `@types/node` 20.17.6, `@types/ws` 8.5.13, `tsx` 4.19.2.

## Known limitations

- **No `socat`** on the build host, so raw Unix-socket gates are exercised via
  the Node validator rather than `socat - UNIX-CONNECT:`.
- The bridge accepts **one client at a time**; a new connection replaces the old
  one (the daemon keeps the most recent).
- The HTTP control rate limit is a **single global bucket** (1 req / 250 ms),
  not per-IP.
- Frame `slots[]` only carries the **most recent 64 positions**; the 32×32 grid's
  per-slot tooltip shows data only for those positions.
- `min_latency_ns` is only reported once at least one message with a recorded
  latency exists (`consumed > 0`); otherwise it renders as `—`.
- The latency histogram buckets are **per-run cumulative** and are rebased by
  `reset`, not sliding-windowed.
- Only validated on Linux x86_64 (WSL2). Big-endian / non-glibc / musl are
  untested; the code assumes little-endian and POSIX unnamed-semaphore support.
