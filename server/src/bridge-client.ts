import { EventEmitter } from 'node:events';
import net from 'node:net';
import type {
  LatencyBuckets,
  MetricsSnapshot,
  SlotSnapshot,
  TelemetryFrame,
  WriterSnapshot,
} from '../../shared/schema';

const MAX_BUFFER = 4 * 1024 * 1024;
const BACKOFF_MIN = 250;
const BACKOFF_MAX = 2000;

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null;
}

function isNum(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

function isStr(v: unknown): v is string {
  return typeof v === 'string';
}

function isBool(v: unknown): v is boolean {
  return typeof v === 'boolean';
}

function parseLatencyBuckets(v: unknown): LatencyBuckets | null {
  if (!Array.isArray(v) || v.length !== 5) return null;
  const out: number[] = [];
  for (const item of v) {
    if (!isNum(item)) return null;
    out.push(item);
  }
  return [out[0] as number, out[1] as number, out[2] as number, out[3] as number, out[4] as number];
}

function parseMetrics(v: unknown): MetricsSnapshot | null {
  if (!isObject(v)) return null;
  const buckets = parseLatencyBuckets(v['latency_buckets']);
  if (buckets === null) return null;
  if (
    !isNum(v['produced_total']) ||
    !isNum(v['consumed_total']) ||
    !isNum(v['backpressure_events']) ||
    !isNum(v['crc_errors']) ||
    !isNum(v['seq_errors']) ||
    !isNum(v['min_latency_ns']) ||
    !isNum(v['max_latency_ns']) ||
    !isNum(v['avg_latency_ns']) ||
    !isNum(v['enqueue_pos']) ||
    !isNum(v['dequeue_pos']) ||
    !isNum(v['occupancy']) ||
    !isNum(v['capacity']) ||
    !isNum(v['refcount'])
  ) {
    return null;
  }
  return {
    produced_total: v['produced_total'],
    consumed_total: v['consumed_total'],
    backpressure_events: v['backpressure_events'],
    crc_errors: v['crc_errors'],
    seq_errors: v['seq_errors'],
    min_latency_ns: v['min_latency_ns'],
    max_latency_ns: v['max_latency_ns'],
    avg_latency_ns: v['avg_latency_ns'],
    latency_buckets: buckets,
    enqueue_pos: v['enqueue_pos'],
    dequeue_pos: v['dequeue_pos'],
    occupancy: v['occupancy'],
    capacity: v['capacity'],
    refcount: v['refcount'],
  };
}

function parseWriter(v: unknown): WriterSnapshot | null {
  if (!isObject(v)) return null;
  const role = v['role'];
  if (role !== 'producer' && role !== 'consumer') return null;
  if (
    !isNum(v['slot']) ||
    !isNum(v['pid']) ||
    !isBool(v['alive']) ||
    !isNum(v['produced']) ||
    !isNum(v['consumed']) ||
    !isNum(v['backpressure_events']) ||
    !isNum(v['crc_errors']) ||
    !isNum(v['seq_errors']) ||
    !isNum(v['last_seen_ms_ago'])
  ) {
    return null;
  }
  return {
    slot: v['slot'],
    pid: v['pid'],
    role,
    alive: v['alive'],
    produced: v['produced'],
    consumed: v['consumed'],
    backpressure_events: v['backpressure_events'],
    crc_errors: v['crc_errors'],
    seq_errors: v['seq_errors'],
    last_seen_ms_ago: v['last_seen_ms_ago'],
  };
}

function parseSlot(v: unknown): SlotSnapshot | null {
  if (!isObject(v)) return null;
  if (
    !isNum(v['idx']) ||
    !isNum(v['pos']) ||
    !isNum(v['msg_id']) ||
    !isNum(v['sender_pid']) ||
    !isNum(v['producer_idx']) ||
    !isNum(v['age_ns']) ||
    !isNum(v['payload_len']) ||
    !isNum(v['crc32']) ||
    !isBool(v['crc_ok']) ||
    !isStr(v['preview'])
  ) {
    return null;
  }
  return {
    idx: v['idx'],
    pos: v['pos'],
    msg_id: v['msg_id'],
    sender_pid: v['sender_pid'],
    producer_idx: v['producer_idx'],
    age_ns: v['age_ns'],
    payload_len: v['payload_len'],
    crc32: v['crc32'],
    crc_ok: v['crc_ok'],
    preview: v['preview'],
  };
}

export function parseFrame(line: string): TelemetryFrame | null {
  let raw: unknown;
  try {
    raw = JSON.parse(line);
  } catch {
    return null;
  }
  if (!isObject(raw)) return null;
  const metrics = parseMetrics(raw['metrics']);
  if (metrics === null) return null;
  if (!isNum(raw['t']) || !isNum(raw['frame_seq']) || !isNum(raw['interval_ms'])) return null;
  if (!isBool(raw['attached'])) return null;
  if (!isStr(raw['slot_states'])) return null;
  if (!Array.isArray(raw['writers']) || !Array.isArray(raw['slots'])) return null;

  const writers: WriterSnapshot[] = [];
  for (const w of raw['writers']) {
    const parsed = parseWriter(w);
    if (parsed === null) return null;
    writers.push(parsed);
  }
  const slots: SlotSnapshot[] = [];
  for (const s of raw['slots']) {
    const parsed = parseSlot(s);
    if (parsed === null) return null;
    slots.push(parsed);
  }
  return {
    t: raw['t'],
    frame_seq: raw['frame_seq'],
    interval_ms: raw['interval_ms'],
    attached: raw['attached'],
    metrics,
    writers,
    slot_states: raw['slot_states'],
    slots,
  };
}

export interface BridgeClientEvents {
  connected: () => void;
  disconnected: () => void;
  frame: (frame: TelemetryFrame) => void;
}

export declare interface BridgeClient {
  on<E extends keyof BridgeClientEvents>(event: E, listener: BridgeClientEvents[E]): this;
  emit<E extends keyof BridgeClientEvents>(event: E, ...args: Parameters<BridgeClientEvents[E]>): boolean;
}

export class BridgeClient extends EventEmitter {
  private readonly socketPath: string;
  private socket: net.Socket | null = null;
  private buffer = '';
  private backoff = BACKOFF_MIN;
  private stopped = false;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(socketPath: string) {
    super();
    this.socketPath = socketPath;
  }

  get connected(): boolean {
    return this.socket !== null && !this.socket.destroyed;
  }

  start(): void {
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket !== null) {
      this.socket.destroy();
      this.socket = null;
    }
  }

  private connect(): void {
    if (this.stopped) return;
    const socket = net.connect({ path: this.socketPath });
    this.socket = socket;
    this.buffer = '';

    socket.on('connect', () => {
      this.backoff = BACKOFF_MIN;
      this.emit('connected');
    });
    socket.on('data', (chunk: Buffer) => {
      this.buffer += chunk.toString('utf8');
      if (this.buffer.length > MAX_BUFFER) {
        this.buffer = '';
        return;
      }
      let idx: number;
      while ((idx = this.buffer.indexOf('\n')) >= 0) {
        const line = this.buffer.slice(0, idx);
        this.buffer = this.buffer.slice(idx + 1);
        if (line.length === 0) continue;
        const frame = parseFrame(line);
        if (frame !== null) this.emit('frame', frame);
      }
    });
    socket.on('error', () => {
      /* handled by close */
    });
    socket.on('close', () => {
      if (this.socket === socket) this.socket = null;
      this.emit('disconnected');
      if (!this.stopped) {
        const delay = this.backoff;
        this.backoff = Math.min(this.backoff * 2, BACKOFF_MAX);
        this.reconnectTimer = setTimeout(() => this.connect(), delay);
      }
    });
  }
}
