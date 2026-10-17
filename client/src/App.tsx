import { useEffect, useRef } from 'react';
import { Activity, Radio } from 'lucide-react';
import type { MetricsSnapshot } from '@shared/schema';
import { useTelemetry } from './hooks/useTelemetry';
import RingBufferGrid from './components/RingBufferGrid';
import ProcessPanel from './components/ProcessPanel';
import ThroughputGauge from './components/ThroughputGauge';
import LatencyHistogram from './components/LatencyHistogram';
import ControlBar from './components/ControlBar';

export default function App() {
  const { latest, history, logs, connected, bridgeConnected, children, slotStates } = useTelemetry();
  const logRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = logRef.current;
    if (el !== null) el.scrollTop = el.scrollHeight;
  }, [logs]);

  const mps = history.length > 0 ? history[history.length - 1]?.msgs_per_sec ?? 0 : 0;
  const attached = latest !== null && latest.attached;
  const metrics = latest?.metrics ?? null;
  const writers = latest?.writers ?? [];
  const slots = latest?.slots ?? [];

  return (
    <div className="mx-auto max-w-[1600px] p-4">
      <header className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-slate-800 bg-slate-900/40 px-4 py-2">
        <div className="flex items-center gap-2">
          <Activity size={18} className="text-emerald-400" />
          <h1 className="text-base font-semibold">SHM Telemetry Monitor</h1>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <StatusDot ok={connected} label={connected ? 'WebSocket connected' : 'WebSocket offline'} />
          <StatusDot
            ok={bridgeConnected}
            label={bridgeConnected ? 'Bridge daemon attached' : 'Bridge daemon offline'}
          />
          <span className="flex items-center gap-1 text-slate-400">
            <Radio size={12} /> {children.length} child process{children.length === 1 ? '' : 'es'}
          </span>
        </div>
      </header>

      {!attached && (
        <div className="mb-4 rounded-lg border border-amber-700/40 bg-amber-900/20 px-4 py-3 text-sm text-amber-300">
          Waiting for the shared-memory segment… Start a producer/consumer via the control panel (or{' '}
          <code className="font-mono">make run-demo</code>) so the bridge reports an attached ring.
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)_minmax(0,1fr)]">
        <section className="flex flex-col gap-4">
          <RingBufferGrid slotStates={slotStates} slots={slots} />
          <ProcessPanel writers={writers} />
          <ControlBar disabled={false} />
        </section>

        <section className="flex flex-col gap-4">
          <ThroughputGauge value={mps} />
          <LatencyHistogram
            buckets={metrics?.latency_buckets ?? [0, 0, 0, 0, 0]}
            minNs={metrics?.min_latency_ns ?? 0}
            avgNs={metrics?.avg_latency_ns ?? 0}
            maxNs={metrics?.max_latency_ns ?? 0}
          />
          <Counters metrics={metrics} occupancy={metrics?.occupancy ?? 0} capacity={metrics?.capacity ?? 1024} />
        </section>

        <section className="flex flex-col gap-2 rounded-lg border border-slate-800 bg-slate-900/40 p-3">
          <h2 className="text-sm font-semibold text-slate-200">Live log stream</h2>
          <div
            ref={logRef}
            className="h-[70vh] overflow-y-auto rounded border border-slate-800 bg-slate-950/70 p-2 font-mono text-[11px] leading-relaxed"
          >
            {logs.length === 0 ? (
              <div className="text-slate-600">no log output yet…</div>
            ) : (
              logs.map((l, i) => (
                <div key={i} className={l.source.endsWith(':err') ? 'text-rose-400' : 'text-slate-300'}>
                  <span className="text-slate-600">
                    {new Date(l.ts).toLocaleTimeString()} [{l.source}]{' '}
                  </span>
                  {l.line}
                </div>
              ))
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

function StatusDot({ ok, label }: { ok: boolean; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-slate-400">
      <span
        className={`inline-block h-2.5 w-2.5 rounded-full ${ok ? 'bg-emerald-500' : 'bg-rose-500'}`}
      />
      {label}
    </span>
  );
}

function Counters({
  metrics,
  occupancy,
  capacity,
}: {
  metrics: MetricsSnapshot | null;
  occupancy: number;
  capacity: number;
}) {
  const pct = capacity > 0 ? Math.round((occupancy / capacity) * 100) : 0;
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h2 className="mb-2 text-sm font-semibold text-slate-200">Totals</h2>
      <dl className="grid grid-cols-2 gap-2 text-xs">
        <Counter label="produced" value={fmt(metrics?.produced_total)} />
        <Counter label="consumed" value={fmt(metrics?.consumed_total)} />
        <Counter label="occupancy" value={`${occupancy} (${pct}%)`} />
        <Counter label="refcount" value={fmt(metrics?.refcount)} />
        <Counter label="backpressure" value={fmt(metrics?.backpressure_events)} warn={(metrics?.backpressure_events ?? 0) > 0} />
        <Counter label="crc errors" value={fmt(metrics?.crc_errors)} warn={(metrics?.crc_errors ?? 0) > 0} />
        <Counter label="seq errors" value={fmt(metrics?.seq_errors)} warn={(metrics?.seq_errors ?? 0) > 0} />
        <Counter label="capacity" value={String(capacity)} />
      </dl>
    </div>
  );
}

function Counter({ label, value, warn = false }: { label: string; value: string; warn?: boolean }) {
  return (
    <div className="rounded border border-slate-800 bg-slate-950/50 px-2 py-1">
      <dt className="text-[10px] uppercase text-slate-500">{label}</dt>
      <dd className={`font-mono ${warn ? 'text-rose-400' : 'text-slate-200'}`}>{value}</dd>
    </div>
  );
}

function fmt(v: number | undefined): string {
  if (v === undefined) return '—';
  return v.toLocaleString();
}
