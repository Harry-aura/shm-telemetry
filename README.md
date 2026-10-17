# shm-telemetry

A POSIX shared-memory MPMC ring buffer with lock-free sequence-numbered slots,
per-writer sharded metrics, a Unix-socket telemetry bridge, an HTTP/WebSocket
control server, and a React + Tailwind monitoring UI.

```
producer ┐                                   ┌─> bridge_daemon ──(NDJSON over AF_UNIX)─┐
         ├─> /dev/shm ring (1024 slots)  ────┤                                          ├─> WS /ws
consumer ┘    /dev/shm metrics             └─> server (ws + /api) ──(broadcast)─────────┘
```

## Components

| Path | Description |
| --- | --- |
| `native/shm_common.{h,c}` | Ring buffer + metrics layout, lifecycle, CRC32, seqlock metric writers |
| `native/producer.c` | Paced/unthrottled producer, backpressure accounting |
| `native/consumer.c` | Consumer with CRC/order validation, latency histogram |
| `native/stress_test.c` | Producer+consumer threads in one process, DoD stress gate |
| `native/bridge_daemon.c` | Read-only attach, emits NDJSON frames on `/tmp/shm_bridge_<id>.sock` |
| `server/` | Node WS + HTTP control server (no CORS, token-gated control) |
| `shared/schema.ts` | TypeScript types shared by server and client |
| `client/` | Vite + React + Tailwind dashboard |
| `scripts/validate_bridge.js` | Parses/validates bridge NDJSON frames (used by `make demo-check`) |
| `cleanup.sh` | Removes only this project's shm/sem/socket artifacts |

## Requirements

- Linux/WSL2 with glibc ≥ 2.34 (tested: gcc 15.2.0, GNU Make 4.4.1, OpenSSL 3.5.5)
- Node.js ≥ 20 (tested: v22.22.1)

## Build

```bash
make clean all        # release binaries -> build/
make debug            # ASan+UBSan binaries -> build-debug/
cd server && npm install && npm run build
cd client && npm install && npm run build
```

## Run (manual demo)

```bash
make run-demo         # producer+consumer+bridge in the foreground (Ctrl-C to stop)
make demo-check       # 3 binaries for ~4s + validate 30 bridge frames, then clean up
```

> On Windows hosts, run all commands inside WSL: `wsl -e bash -lc '...'`.

## Run the UI

```bash
export CONTROL_TOKEN=$(openssl rand -hex 16)
node server/dist/server/src/index.js            # 127.0.0.1:8787
cd client && npm run dev                        # http://localhost:5173
```

Paste the same `CONTROL_TOKEN` into the control panel; the token is stored in
`sessionStorage` and sent as `X-Control-Token` on every control request.

## Server configuration

| Env var | Default | Notes |
| --- | --- | --- |
| `CONTROL_TOKEN` | *(required)* | ≥ 16 chars, else the server exits |
| `PORT` | `8787` | HTTP/WebSocket port |
| `INSTANCE_ID` | `0` | `^[A-Za-z0-9_]{1,16}$` |
| `BRIDGE_SOCKET` | `/tmp/shm_bridge_<INSTANCE_ID>.sock` | bridge path |
| `BIN_DIR` | `<project-root>/build` | where native binaries live |
| `ALLOW_REMOTE` | *(unset)* | `1` binds `0.0.0.0`, else `127.0.0.1` |
| `ALLOWED_ORIGINS` | `http://localhost:5173,http://127.0.0.1:5173` | WS `Origin` allowlist |

## HTTP API

- `GET /api/health` — liveness, bridge/child state.
- `POST /api/control/spawn` — `{ kind: 'producer'|'consumer'|'stress', rate_hz?, duration_s? }`.
- `POST /api/control/reset` — re-base the metric baseline to the latest frame.
- `POST /api/control/cleanup` — run `cleanup.sh -f`.

All `POST`s require `Content-Type: application/json`, `X-Control-Token`, a body
≤ 4 KiB, and are globally rate-limited to 1 request / 250 ms.

## Gates

```bash
make stress           # 10s multi-threaded stress, must print STRESS PASS
make stress-debug     # same under ASan+UBSan
make demo-check       # bridge frame validation (Node)
make check            # clean + all + debug + stress + stress-debug + web-build
```

See `NOTES.md` for design decisions, spec deviations, and known limitations.
