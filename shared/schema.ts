export type SlotState = 0 | 1 | 2 | 3; // 0 empty, 1 filled, 2 being written, 3 being read
export type WriterRole = 'producer' | 'consumer';
export type LatencyBuckets = [number, number, number, number, number];

export interface SlotSnapshot {
  idx: number;
  pos: number;
  msg_id: number;
  sender_pid: number;
  producer_idx: number;
  age_ns: number;
  payload_len: number;
  crc32: number;
  crc_ok: boolean;
  preview: string;
}

export interface WriterSnapshot {
  slot: number;
  pid: number;
  role: WriterRole;
  alive: boolean;
  produced: number;
  consumed: number;
  backpressure_events: number;
  crc_errors: number;
  seq_errors: number;
  last_seen_ms_ago: number;
}

export interface MetricsSnapshot {
  produced_total: number;
  consumed_total: number;
  backpressure_events: number;
  crc_errors: number;
  seq_errors: number;
  min_latency_ns: number;
  max_latency_ns: number;
  avg_latency_ns: number;
  latency_buckets: LatencyBuckets;
  enqueue_pos: number;
  dequeue_pos: number;
  occupancy: number;
  capacity: number;
  refcount: number;
}

export interface TelemetryFrame {
  t: number;
  frame_seq: number;
  interval_ms: number;
  attached: boolean;
  metrics: MetricsSnapshot;
  writers: WriterSnapshot[];
  slot_states: string; // base64, capacity bytes
  slots: SlotSnapshot[]; // <= 64
}

export interface ChildInfo {
  pid: number;
  kind: 'producer' | 'consumer' | 'stress';
  started_ms: number;
  args: string[];
}

export type ServerMessage =
  | { type: 'frame'; frame: TelemetryFrame }
  | { type: 'status'; bridge_connected: boolean; children: ChildInfo[] }
  | { type: 'log'; ts: number; source: string; line: string };

export interface SpawnRequest {
  kind: 'producer' | 'consumer' | 'stress';
  rate_hz?: number;
  duration_s?: number;
}

export interface ControlResponse {
  ok: boolean;
  error?: string;
  pid?: number;
}
