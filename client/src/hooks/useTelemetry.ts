import { useEffect, useRef, useState } from 'react';
import type { ChildInfo, ServerMessage, TelemetryFrame } from '@shared/schema';

export interface HistorySample {
  t: number;
  consumed_total: number;
  produced_total: number;
  occupancy: number;
  msgs_per_sec: number;
}

export interface LogEntry {
  ts: number;
  source: string;
  line: string;
}

export interface TelemetryResult {
  latest: TelemetryFrame | null;
  history: HistorySample[];
  logs: LogEntry[];
  connected: boolean;
  bridgeConnected: boolean;
  children: ChildInfo[];
  slotStates: Uint8Array | null;
}

const HISTORY_LIMIT = 300;
const FLUSH_MS = 50;
const BACKOFF_MIN = 250;
const BACKOFF_MAX = 2000;
const LOG_LIMIT = 500;

function decodeBase64(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function useTelemetry(): TelemetryResult {
  const [latest, setLatest] = useState<TelemetryFrame | null>(null);
  const [history, setHistory] = useState<HistorySample[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [connected, setConnected] = useState(false);
  const [bridgeConnected, setBridgeConnected] = useState(false);
  const [children, setChildren] = useState<ChildInfo[]>([]);
  const [slotStates, setSlotStates] = useState<Uint8Array | null>(null);

  const pendingFrame = useRef<TelemetryFrame | null>(null);
  const pendingLogs = useRef<LogEntry[]>([]);
  const historyRef = useRef<HistorySample[]>([]);
  const dirty = useRef(false);
  const flushTimer = useRef<number | null>(null);

  useEffect(() => {
    let stopped = false;
    let socket: WebSocket | null = null;
    let backoff = BACKOFF_MIN;
    let reconnectTimer: number | null = null;

    const flush = () => {
      flushTimer.current = null;
      if (!dirty.current) return;
      dirty.current = false;

      const frame = pendingFrame.current;
      if (frame !== null) {
        pendingFrame.current = null;
        setSlotStates(frame.attached ? decodeBase64(frame.slot_states) : null);

        const now = Date.now();
        const prev = historyRef.current;
        let refSample: HistorySample | null = null;
        for (let i = prev.length - 1; i >= 0; i--) {
          const s = prev[i];
          if (s !== undefined && now - s.t >= 1000) {
            refSample = s;
            break;
          }
        }
        if (refSample === null && prev.length > 0) refSample = prev[0] ?? null;

        let mps = 0;
        if (refSample !== null) {
          const dt = (now - refSample.t) / 1000;
          if (dt > 0) mps = (frame.metrics.consumed_total - refSample.consumed_total) / dt;
        }
        if (mps < 0) mps = 0;

        const sample: HistorySample = {
          t: now,
          consumed_total: frame.metrics.consumed_total,
          produced_total: frame.metrics.produced_total,
          occupancy: frame.metrics.occupancy,
          msgs_per_sec: mps,
        };
        historyRef.current = [...prev, sample].slice(-HISTORY_LIMIT);
        setHistory(historyRef.current);
        setLatest(frame);
      }

      const batch = pendingLogs.current;
      pendingLogs.current = [];
      if (batch.length > 0) {
        setLogs((old) => [...old, ...batch].slice(-LOG_LIMIT));
      }
    };

    const scheduleFlush = () => {
      if (flushTimer.current !== null) return;
      flushTimer.current = window.setTimeout(flush, FLUSH_MS);
    };

    const handleMessage = (raw: string) => {
      let msg: ServerMessage;
      try {
        msg = JSON.parse(raw) as ServerMessage;
      } catch {
        return;
      }
      if (msg.type === 'frame') {
        pendingFrame.current = msg.frame;
        dirty.current = true;
        scheduleFlush();
      } else if (msg.type === 'log') {
        pendingLogs.current.push({ ts: msg.ts, source: msg.source, line: msg.line });
        dirty.current = true;
        scheduleFlush();
      } else {
        setBridgeConnected(msg.bridge_connected);
        setChildren(msg.children);
      }
    };

    const connect = () => {
      if (stopped) return;
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      const ws = new WebSocket(`${proto}://${location.host}/ws`);
      socket = ws;
      ws.onopen = () => {
        backoff = BACKOFF_MIN;
        setConnected(true);
      };
      ws.onmessage = (ev) => {
        if (typeof ev.data === 'string') handleMessage(ev.data);
      };
      ws.onerror = () => {
        ws.close();
      };
      ws.onclose = () => {
        if (socket === ws) socket = null;
        setConnected(false);
        setSlotStates(null);
        if (!stopped) {
          const delay = backoff;
          backoff = Math.min(backoff * 2, BACKOFF_MAX);
          reconnectTimer = window.setTimeout(connect, delay);
        }
      };
    };

    connect();

    return () => {
      stopped = true;
      if (reconnectTimer !== null) window.clearTimeout(reconnectTimer);
      if (flushTimer.current !== null) window.clearTimeout(flushTimer.current);
      if (socket !== null) {
        socket.onclose = null;
        socket.close();
      }
    };
  }, []);

  return { latest, history, logs, connected, bridgeConnected, children, slotStates };
}
