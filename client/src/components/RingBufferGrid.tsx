import { memo, useMemo, useState } from 'react';
import type { SlotSnapshot } from '@shared/schema';

const GRID_SIZE = 32;
const CELL_COUNT = GRID_SIZE * GRID_SIZE;

function stateClass(state: number): string {
  switch (state) {
    case 1:
      return 'bg-emerald-500';
    case 2:
      return 'bg-amber-500';
    case 3:
      return 'bg-sky-500';
    default:
      return 'bg-slate-800';
  }
}

const Cell = memo(function Cell({ state, idx }: { state: number; idx: number }) {
  return <div data-idx={idx} className={`aspect-square w-full ${stateClass(state)}`} />;
});

interface HoverInfo {
  idx: number;
  x: number;
  y: number;
}

interface RingBufferGridProps {
  slotStates: Uint8Array | null;
  slots: SlotSnapshot[];
}

export default function RingBufferGrid({ slotStates, slots }: RingBufferGridProps) {
  const [hover, setHover] = useState<HoverInfo | null>(null);

  const slotsByIdx = useMemo(() => {
    const map = new Map<number, SlotSnapshot>();
    for (const s of slots) map.set(s.idx, s);
    return map;
  }, [slots]);

  const cells = useMemo(() => {
    const out = new Uint8Array(CELL_COUNT);
    if (slotStates !== null) {
      const n = Math.min(slotStates.length, CELL_COUNT);
      for (let i = 0; i < n; i++) out[i] = slotStates[i] ?? 0;
    }
    return out;
  }, [slotStates]);

  const hoveredSlot = hover !== null ? slotsByIdx.get(hover.idx) : undefined;

  return (
    <div className="relative rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <div className="mb-2 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-slate-200">Ring Buffer (1024 slots)</h2>
        <div className="flex items-center gap-2 text-[10px] text-slate-400">
          <Legend color="bg-slate-800" label="empty" />
          <Legend color="bg-emerald-500" label="filled" />
          <Legend color="bg-amber-500" label="writing" />
          <Legend color="bg-sky-500" label="reading" />
        </div>
      </div>
      <div
        className="grid gap-px"
        style={{ gridTemplateColumns: `repeat(${GRID_SIZE}, minmax(0, 1fr))` }}
        onMouseOver={(e) => {
          const target = e.target as HTMLElement;
          const idx = target.dataset['idx'];
          if (idx !== undefined) setHover({ idx: Number(idx), x: e.clientX, y: e.clientY });
        }}
        onMouseMove={(e) => {
          const target = e.target as HTMLElement;
          const idx = target.dataset['idx'];
          if (idx !== undefined) setHover({ idx: Number(idx), x: e.clientX, y: e.clientY });
        }}
        onMouseLeave={() => setHover(null)}
      >
        {Array.from(cells, (state, i) => (
          <Cell key={i} idx={i} state={state} />
        ))}
      </div>

      {hover !== null && (
        <div
          className="pointer-events-none fixed z-50 w-64 rounded-md border border-slate-700 bg-slate-950/95 p-2 text-xs shadow-lg"
          style={{ left: hover.x + 14, top: hover.y + 14 }}
        >
          <div className="mb-1 font-semibold text-slate-200">slot #{hover.idx}</div>
          {hoveredSlot !== undefined ? (
            <dl className="grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 text-slate-300">
              <dt className="text-slate-500">state</dt>
              <dd className="text-slate-200">{stateName(cells[hover.idx] ?? 0)}</dd>
              <dt className="text-slate-500">msg_id</dt>
              <dd>{hoveredSlot.msg_id}</dd>
              <dt className="text-slate-500">pid</dt>
              <dd>{hoveredSlot.sender_pid}</dd>
              <dt className="text-slate-500">crc</dt>
              <dd>{hoveredSlot.crc_ok ? 'ok' : 'BAD'}</dd>
              <dt className="text-slate-500">age</dt>
              <dd>{formatAge(hoveredSlot.age_ns)}</dd>
              <dt className="text-slate-500">preview</dt>
              <dd className="break-all font-mono text-[10px]">{hoveredSlot.preview || '—'}</dd>
            </dl>
          ) : (
            <div className="text-slate-500">{stateName(cells[hover.idx] ?? 0)} (no recent snapshot)</div>
          )}
        </div>
      )}
    </div>
  );
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <span className="flex items-center gap-1">
      <span className={`inline-block h-2 w-2 rounded-sm ${color}`} />
      {label}
    </span>
  );
}

function stateName(state: number): string {
  switch (state) {
    case 1:
      return 'filled';
    case 2:
      return 'writing';
    case 3:
      return 'reading';
    default:
      return 'empty';
  }
}

function formatAge(ns: number): string {
  if (ns < 1000) return `${ns} ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(1)} µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(2)} ms`;
  return `${(ns / 1_000_000_000).toFixed(2)} s`;
}
