import http from 'node:http';
import crypto from 'node:crypto';
import path from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { WebSocketServer, WebSocket } from 'ws';
import type {
  ChildInfo,
  ControlResponse,
  MetricsSnapshot,
  ServerMessage,
  SpawnRequest,
  TelemetryFrame,
} from '../../shared/schema';
import { BridgeClient } from './bridge-client';

interface ManagedChild {
  info: ChildInfo;
  proc: ChildProcess;
}

interface RebaseBaseline {
  produced_total: number;
  consumed_total: number;
  backpressure_events: number;
  crc_errors: number;
  seq_errors: number;
  latency_buckets: [number, number, number, number, number];
}

const CONTROL_TOKEN = process.env['CONTROL_TOKEN'] ?? '';
if (CONTROL_TOKEN.length < 16) {
  console.error(
    'FATAL: CONTROL_TOKEN must be set and at least 16 characters long. ' +
      'Example: export CONTROL_TOKEN=$(openssl rand -hex 16)',
  );
  process.exit(1);
}

const PORT = Number.parseInt(process.env['PORT'] ?? '8787', 10);
const INSTANCE_ID = process.env['INSTANCE_ID'] ?? '0';
const BRIDGE_SOCKET = process.env['BRIDGE_SOCKET'] ?? `/tmp/shm_bridge_${INSTANCE_ID}.sock`;
const PROJECT_ROOT = path.resolve(__dirname, '../../../..');
const BIN_DIR = process.env['BIN_DIR'] ?? path.resolve(PROJECT_ROOT, 'build');
const ALLOW_REMOTE = process.env['ALLOW_REMOTE'] === '1';
const ALLOWED_ORIGINS = (process.env['ALLOWED_ORIGINS'] ??
  'http://localhost:5173,http://127.0.0.1:5173')
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);

if (!/^[A-Za-z0-9_]{1,16}$/.test(INSTANCE_ID)) {
  console.error(`FATAL: INSTANCE_ID '${INSTANCE_ID}' must match ^[A-Za-z0-9_]{1,16}$`);
  process.exit(1);
}
if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
  console.error(`FATAL: invalid PORT '${process.env['PORT'] ?? ''}'`);
  process.exit(1);
}

const BINARY_ALLOWLIST: Record<string, string> = {
  producer: path.join(BIN_DIR, 'producer'),
  consumer: path.join(BIN_DIR, 'consumer'),
  stress: path.join(BIN_DIR, 'stress_test'),
};

const MAX_CHILDREN = 16;
const BODY_LIMIT = 4096;
const RATE_LIMIT_MS = 250;
const LOG_BUFFER = 200;

const children = new Map<number, ManagedChild>();
const logs: ServerMessage[] = [];
const rebase: RebaseBaseline = {
  produced_total: 0,
  consumed_total: 0,
  backpressure_events: 0,
  crc_errors: 0,
  seq_errors: 0,
  latency_buckets: [0, 0, 0, 0, 0],
};
let bridgeConnected = false;
let lastPostAt = 0;
let latestRaw: TelemetryFrame | null = null;

function pushLog(msg: ServerMessage): void {
  logs.push(msg);
  if (logs.length > LOG_BUFFER) logs.splice(0, logs.length - LOG_BUFFER);
}

function childInfos(): ChildInfo[] {
  return [...children.values()].map((c) => c.info);
}

function statusMessage(): ServerMessage {
  return { type: 'status', bridge_connected: bridgeConnected, children: childInfos() };
}

function tokenOk(header: string | string[] | undefined): boolean {
  const value = Array.isArray(header) ? header[0] : header;
  if (value === undefined) return false;
  const a = crypto.createHash('sha256').update(value).digest();
  const b = crypto.createHash('sha256').update(CONTROL_TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}

function rebaseMetrics(m: MetricsSnapshot): MetricsSnapshot {
  const b = rebase;
  const sub = (v: number, base: number): number => (v - base > 0 ? v - base : 0);
  return {
    ...m,
    produced_total: sub(m.produced_total, b.produced_total),
    consumed_total: sub(m.consumed_total, b.consumed_total),
    backpressure_events: sub(m.backpressure_events, b.backpressure_events),
    crc_errors: sub(m.crc_errors, b.crc_errors),
    seq_errors: sub(m.seq_errors, b.seq_errors),
    latency_buckets: [
      sub(m.latency_buckets[0], b.latency_buckets[0]),
      sub(m.latency_buckets[1], b.latency_buckets[1]),
      sub(m.latency_buckets[2], b.latency_buckets[2]),
      sub(m.latency_buckets[3], b.latency_buckets[3]),
      sub(m.latency_buckets[4], b.latency_buckets[4]),
    ],
  };
}

function rebaseFrame(frame: TelemetryFrame): TelemetryFrame {
  return { ...frame, metrics: rebaseMetrics(frame.metrics) };
}

const bridge = new BridgeClient(BRIDGE_SOCKET);

const server = http.createServer((req, res) => {
  const url = req.url ?? '/';
  const method = req.method ?? 'GET';

  if (method === 'GET' && url === '/api/health') {
    json(res, 200, {
      ok: true,
      instance_id: INSTANCE_ID,
      bridge_connected: bridgeConnected,
      children: childInfos().length,
      bin_dir: BIN_DIR,
      bridge_socket: BRIDGE_SOCKET,
    });
    return;
  }

  const controlRoutes = new Set(['/api/control/spawn', '/api/control/reset', '/api/control/cleanup']);
  if (method === 'POST' && controlRoutes.has(url)) {
    handleControl(req, res, url);
    return;
  }

  json(res, 404, { ok: false, error: 'not found' });
});

function json(res: http.ServerResponse, code: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(text);
}

function handleControl(req: http.IncomingMessage, res: http.ServerResponse, route: string): void {
  const now = Date.now();
  if (now - lastPostAt < RATE_LIMIT_MS) {
    json(res, 429, { ok: false, error: 'rate limited' });
    return;
  }
  lastPostAt = now;

  const contentType = req.headers['content-type'] ?? '';
  const mediaType = contentType.split(';')[0]?.trim().toLowerCase() ?? '';
  if (mediaType !== 'application/json') {
    json(res, 415, { ok: false, error: 'Content-Type must be application/json' });
    return;
  }
  if (!tokenOk(req.headers['x-control-token'])) {
    json(res, 401, { ok: false, error: 'invalid or missing X-Control-Token' });
    return;
  }

  const chunks: Buffer[] = [];
  let size = 0;
  let aborted = false;
  req.on('data', (chunk: Buffer) => {
    if (aborted) return;
    size += chunk.length;
    if (size > BODY_LIMIT) {
      aborted = true;
      json(res, 413, { ok: false, error: 'body exceeds 4 KiB' });
      req.destroy();
      return;
    }
    chunks.push(chunk);
  });
  req.on('end', () => {
    if (aborted) return;
    let parsed: unknown = null;
    if (chunks.length > 0) {
      try {
        parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      } catch {
        json(res, 400, { ok: false, error: 'invalid JSON' });
        return;
      }
    }
    if (route === '/api/control/spawn') {
      handleSpawn(parsed, res);
    } else if (route === '/api/control/reset') {
      handleReset(res);
    } else {
      handleCleanup(res);
    }
  });
  req.on('error', () => {
    if (!aborted) json(res, 400, { ok: false, error: 'request error' });
  });
}

function isSpawnRequest(v: unknown): v is SpawnRequest {
  if (typeof v !== 'object' || v === null) return false;
  const o = v as Record<string, unknown>;
  const kind = o['kind'];
  if (kind !== 'producer' && kind !== 'consumer' && kind !== 'stress') return false;
  const rate = o['rate_hz'];
  const dur = o['duration_s'];
  if (rate !== undefined && typeof rate !== 'number') return false;
  if (dur !== undefined && typeof dur !== 'number') return false;
  return true;
}

function handleSpawn(body: unknown, res: http.ServerResponse): void {
  if (!isSpawnRequest(body)) {
    json(res, 400, { ok: false, error: 'body must be a SpawnRequest' });
    return;
  }
  const bin = BINARY_ALLOWLIST[body.kind];
  if (bin === undefined) {
    json(res, 400, { ok: false, error: 'unknown kind' });
    return;
  }
  if (children.size >= MAX_CHILDREN) {
    json(res, 409, { ok: false, error: `at most ${MAX_CHILDREN} managed children` });
    return;
  }
  const runningStress = [...children.values()].some((c) => c.info.kind === 'stress');
  if (body.kind === 'stress' && runningStress) {
    json(res, 409, { ok: false, error: 'a stress run is already active' });
    return;
  }

  let rate = 100;
  let duration = 0;
  if (body.rate_hz !== undefined) {
    if (!Number.isInteger(body.rate_hz) || body.rate_hz < 1 || body.rate_hz > 10000) {
      json(res, 400, { ok: false, error: 'rate_hz must be an integer 1..10000' });
      return;
    }
    rate = body.rate_hz;
  }
  if (body.duration_s !== undefined) {
    if (!Number.isInteger(body.duration_s) || body.duration_s < 0 || body.duration_s > 3600) {
      json(res, 400, { ok: false, error: 'duration_s must be an integer 0..3600' });
      return;
    }
    duration = body.duration_s;
  }

  let args: string[];
  let label: string;
  if (body.kind === 'producer') {
    args = [INSTANCE_ID, String(rate), String(duration)];
    label = 'producer';
  } else if (body.kind === 'consumer') {
    args = [INSTANCE_ID, String(duration)];
    label = 'consumer';
  } else {
    args = ['stress', String(duration > 0 ? duration : 10)];
    label = 'stress';
  }

  let proc: ChildProcess;
  try {
    proc = spawn(bin, args, { shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (err) {
    json(res, 500, { ok: false, error: `spawn failed: ${String(err)}` });
    return;
  }
  const pid = proc.pid;
  if (pid === undefined) {
    json(res, 500, { ok: false, error: 'spawn returned no pid' });
    return;
  }

  const info: ChildInfo = { pid, kind: body.kind, started_ms: Date.now(), args: [bin, ...args] };
  children.set(pid, { info, proc });

  const forward = (source: string) => (data: Buffer) => {
    const text = data.toString('utf8');
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      const msg: ServerMessage = { type: 'log', ts: Date.now(), source, line };
      pushLog(msg);
      broadcast(msg);
    }
  };
  proc.stdout?.on('data', forward(`${label}:${pid}:out`));
  proc.stderr?.on('data', forward(`${label}:${pid}:err`));

  proc.on('error', (err) => {
    const msg: ServerMessage = {
      type: 'log',
      ts: Date.now(),
      source: `${label}:${pid}`,
      line: `process error: ${String(err)}`,
    };
    pushLog(msg);
    broadcast(msg);
  });

  proc.on('exit', (code, signal) => {
    children.delete(pid);
    const msg: ServerMessage = {
      type: 'log',
      ts: Date.now(),
      source: `${label}:${pid}`,
      line: `exited code=${code ?? 'null'} signal=${signal ?? 'null'}`,
    };
    pushLog(msg);
    broadcast(msg);
    broadcast(statusMessage());
  });

  broadcast(statusMessage());
  const ok: ControlResponse = { ok: true, pid };
  json(res, 200, ok);
}

function handleReset(res: http.ServerResponse): void {
  rebase.produced_total = latestRaw?.metrics.produced_total ?? 0;
  rebase.consumed_total = latestRaw?.metrics.consumed_total ?? 0;
  rebase.backpressure_events = latestRaw?.metrics.backpressure_events ?? 0;
  rebase.crc_errors = latestRaw?.metrics.crc_errors ?? 0;
  rebase.seq_errors = latestRaw?.metrics.seq_errors ?? 0;
  const lb = latestRaw?.metrics.latency_buckets;
  rebase.latency_buckets = lb
    ? [lb[0], lb[1], lb[2], lb[3], lb[4]]
    : [0, 0, 0, 0, 0];
  json(res, 200, { ok: true });
}

function handleCleanup(res: http.ServerResponse): void {
  const script = path.join(PROJECT_ROOT, 'cleanup.sh');
  const proc = spawn(script, ['-f'], { shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
  const forward = (data: Buffer) => {
    for (const line of data.toString('utf8').split('\n')) {
      if (line.length === 0) continue;
      const msg: ServerMessage = { type: 'log', ts: Date.now(), source: 'cleanup', line };
      pushLog(msg);
      broadcast(msg);
    }
  };
  proc.stdout?.on('data', forward);
  proc.stderr?.on('data', forward);
  proc.on('error', (err) => {
    json(res, 500, { ok: false, error: `cleanup failed: ${String(err)}` });
  });
  proc.on('exit', (code) => {
    json(res, code === 0 ? 200 : 500, { ok: code === 0, error: code === 0 ? undefined : `exit ${code}` });
  });
}

bridge.on('frame', (frame) => {
  latestRaw = frame;
  broadcast({ type: 'frame', frame: rebaseFrame(frame) });
});
bridge.on('connected', () => {
  bridgeConnected = true;
  broadcast(statusMessage());
});
bridge.on('disconnected', () => {
  bridgeConnected = false;
  broadcast(statusMessage());
});
bridge.start();

const wss = new WebSocketServer({ noServer: true, clientTracking: true });
const alive = new WeakMap<WebSocket, boolean>();

server.on('upgrade', (req, socket, head) => {
  const origin = req.headers['origin'];
  if (origin !== undefined && !ALLOWED_ORIGINS.includes(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    socket.destroy();
    return;
  }
  const reqUrl = req.url ?? '/';
  if (!reqUrl.startsWith('/ws')) {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws: WebSocket) => {
  alive.set(ws, true);
  ws.on('pong', () => alive.set(ws, true));
  ws.on('error', () => {
    /* ignore; close handles cleanup */
  });
  sendTo(ws, statusMessage());
  for (const msg of logs) sendTo(ws, msg);
  if (latestRaw !== null) sendTo(ws, { type: 'frame', frame: rebaseFrame(latestRaw) });
});

function sendTo(ws: WebSocket, msg: ServerMessage): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > 1_000_000) return;
  ws.send(JSON.stringify(msg));
}

function broadcast(msg: ServerMessage): void {
  for (const ws of wss.clients) sendTo(ws, msg);
}

const pingTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (alive.get(ws) === false) {
      ws.terminate();
      continue;
    }
    alive.set(ws, false);
    ws.ping();
  }
}, 15000);
pingTimer.unref();

function shutdown(): void {
  console.error('shutting down: terminating children');
  for (const c of children.values()) {
    if (c.proc.exitCode === null) c.proc.kill('SIGTERM');
  }
  bridge.stop();
  clearInterval(pingTimer);
  const killTimer = setTimeout(() => {
    for (const c of children.values()) {
      if (c.proc.exitCode === null) c.proc.kill('SIGKILL');
    }
    process.exit(0);
  }, 3000);
  killTimer.unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

const bindHost = ALLOW_REMOTE ? '0.0.0.0' : '127.0.0.1';
server.listen(PORT, bindHost, () => {
  console.error(
    `shm-telemetry server listening on http://${bindHost}:${PORT} ` +
      `(instance=${INSTANCE_ID}, bridge=${BRIDGE_SOCKET}, bin=${BIN_DIR})`,
  );
});
