import type { LatencyBuckets } from '@shared/schema';

interface LatencyHistogramProps {
  buckets: LatencyBuckets;
  minNs: number;
  avgNs: number;
  maxNs: number;
}

const LABELS = ['<1µs', '1-10µs', '10-100µs', '100µs-1ms', '≥1ms'];

const WIDTH = 340;
const HEIGHT = 150;
const PAD_LEFT = 8;
const PAD_BOTTOM = 28;
const PAD_TOP = 10;
const PLOT_H = HEIGHT - PAD_TOP - PAD_BOTTOM;
const BAR_GAP = 8;

export default function LatencyHistogram({ buckets, minNs, avgNs, maxNs }: LatencyHistogramProps) {
  const max = Math.max(1, ...buckets);
  const n = buckets.length;
  const slotW = (WIDTH - PAD_LEFT * 2) / n;
  const barW = slotW - BAR_GAP;

  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h2 className="mb-1 text-sm font-semibold text-slate-200">Latency distribution</h2>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="mx-auto w-full max-w-md">
        {buckets.map((v, i) => {
          const h = Math.max(1, (v / max) * PLOT_H);
          const x = PAD_LEFT + i * slotW + BAR_GAP / 2;
          const y = PAD_TOP + (PLOT_H - h);
          return (
            <g key={i}>
              <rect x={x} y={y} width={barW} height={h} rx={3} className="fill-sky-500/70" />
              <text
                x={x + barW / 2}
                y={y - 3}
                textAnchor="middle"
                className="fill-slate-400 text-[10px]"
              >
                {v > 0 ? compact(v) : ''}
              </text>
              <text
                x={x + barW / 2}
                y={HEIGHT - 10}
                textAnchor="middle"
                className="fill-slate-500 text-[9px]"
              >
                {LABELS[i] ?? ''}
              </text>
            </g>
          );
        })}
      </svg>
      <dl className="mt-2 grid grid-cols-3 gap-2 text-center text-xs">
        <Stat label="min" value={formatNs(minNs)} className="text-emerald-400" />
        <Stat label="avg" value={formatNs(avgNs)} className="text-slate-200" />
        <Stat label="max" value={formatNs(maxNs)} className="text-amber-400" />
      </dl>
    </div>
  );
}

function Stat({ label, value, className }: { label: string; value: string; className: string }) {
  return (
    <div className="rounded border border-slate-800 bg-slate-950/50 py-1">
      <dt className="text-[10px] uppercase text-slate-500">{label}</dt>
      <dd className={`font-mono ${className}`}>{value}</dd>
    </div>
  );
}

function compact(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}k`;
  return String(v);
}

function formatNs(ns: number): string {
  if (ns === 0) return '—';
  if (ns < 1000) return `${ns}ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(1)}µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(2)}ms`;
  return `${(ns / 1_000_000_000).toFixed(2)}s`;
}
