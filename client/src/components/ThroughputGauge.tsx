interface ThroughputGaugeProps {
  value: number;
}

const WIDTH = 260;
const HEIGHT = 150;
const CX = WIDTH / 2;
const CY = 130;
const R = 100;
const ARC_LEN = Math.PI * R;

function nextPowerOfTen(v: number): number {
  if (v <= 1) return 10;
  return Math.pow(10, Math.ceil(Math.log10(v)));
}

function formatRate(v: number): string {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  return v.toFixed(0);
}

export default function ThroughputGauge({ value }: ThroughputGaugeProps) {
  const scale = nextPowerOfTen(value);
  const fraction = Math.max(0, Math.min(1, value / scale));
  const offset = ARC_LEN * (1 - fraction);
  const arcPath = `M ${CX - R} ${CY} A ${R} ${R} 0 0 1 ${CX + R} ${CY}`;
  const pct = Math.round(fraction * 100);

  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h2 className="mb-1 text-sm font-semibold text-slate-200">Throughput</h2>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="mx-auto w-full max-w-xs">
        <path d={arcPath} fill="none" stroke="#1e293b" strokeWidth={14} strokeLinecap="round" />
        <path
          d={arcPath}
          fill="none"
          stroke="#10b981"
          strokeWidth={14}
          strokeLinecap="round"
          strokeDasharray={ARC_LEN}
          strokeDashoffset={offset}
        />
        <text x={CX} y={CY - 30} textAnchor="middle" className="fill-slate-100 text-3xl font-semibold">
          {formatRate(value)}
        </text>
        <text x={CX} y={CY - 8} textAnchor="middle" className="fill-slate-500 text-xs">
          msgs/sec
        </text>
        <text x={CX - R} y={CY + 20} textAnchor="middle" className="fill-slate-600 text-[10px]">
          0
        </text>
        <text x={CX + R} y={CY + 20} textAnchor="middle" className="fill-slate-600 text-[10px]">
          {formatRate(scale)}
        </text>
      </svg>
      <p className="text-center text-xs text-slate-500">
        {pct}% of {formatRate(scale)}/s scale
      </p>
    </div>
  );
}
