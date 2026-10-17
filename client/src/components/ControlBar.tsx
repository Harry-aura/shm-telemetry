import { useState, type ReactNode } from 'react';
import { AlertTriangle, Download, Play, RotateCcw, Trash2, Zap } from 'lucide-react';
import type { ControlResponse, SpawnRequest } from '@shared/schema';

const TOKEN_KEY = 'shm_control_token';

interface ControlBarProps {
  disabled: boolean;
}

export default function ControlBar({ disabled }: ControlBarProps) {
  const [token, setToken] = useState(() => sessionStorage.getItem(TOKEN_KEY) ?? '');
  const [rate, setRate] = useState(1000);
  const [duration, setDuration] = useState(0);
  const [inflight, setInflight] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const saveToken = (value: string) => {
    setToken(value);
    sessionStorage.setItem(TOKEN_KEY, value);
  };

  const post = async (route: string, body?: unknown): Promise<void> => {
    setInflight(true);
    setError(null);
    try {
      const res = await fetch(route, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Control-Token': token,
        },
        body: JSON.stringify(body ?? {}),
      });
      const data = (await res.json().catch(() => ({}))) as Partial<ControlResponse>;
      if (!res.ok || data.ok === false) {
        setError(data.error ?? `request failed with HTTP ${res.status}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setInflight(false);
    }
  };

  const spawn = (kind: SpawnRequest['kind']): void => {
    const body: SpawnRequest = kind === 'consumer' ? { kind } : { kind, rate_hz: rate, duration_s: duration };
    void post('/api/control/spawn', body);
  };

  const cleanup = (): void => {
    if (!window.confirm('Clean up all /dev/shm telemetry segments, semaphores and bridge sockets?')) return;
    void post('/api/control/cleanup');
  };

  const busy = inflight || disabled;
  const tokenMissing = token.trim().length < 16;

  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h2 className="mb-2 text-sm font-semibold text-slate-200">Control</h2>

      <label className="mb-2 block text-xs text-slate-400">
        CONTROL_TOKEN
        <input
          type="password"
          value={token}
          onChange={(e) => saveToken(e.target.value)}
          placeholder="paste token (≥16 chars)"
          className="mt-1 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-xs text-slate-100 outline-none focus:border-sky-500"
        />
      </label>

      <div className="mb-3 grid grid-cols-2 gap-2">
        <label className="text-xs text-slate-400">
          Rate (Hz)
          <input
            type="number"
            min={1}
            max={10000}
            value={rate}
            onChange={(e) => setRate(clampInt(e.target.value, 1, 10000, 1000))}
            className="mt-1 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 outline-none focus:border-sky-500"
          />
        </label>
        <label className="text-xs text-slate-400">
          Duration (s, 0=∞)
          <input
            type="number"
            min={0}
            max={3600}
            value={duration}
            onChange={(e) => setDuration(clampInt(e.target.value, 0, 3600, 0))}
            className="mt-1 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 text-xs text-slate-100 outline-none focus:border-sky-500"
          />
        </label>
      </div>

      <div className="grid grid-cols-2 gap-2">
        <Button onClick={() => spawn('producer')} disabled={busy} label="Spawn Producer" icon={<Play size={14} />} />
        <Button onClick={() => spawn('consumer')} disabled={busy} label="Spawn Consumer" icon={<Download size={14} />} />
        <Button onClick={() => spawn('stress')} disabled={busy} label="Start Stress" icon={<Zap size={14} />} />
        <Button onClick={() => void post('/api/control/reset')} disabled={busy} label="Reset Metrics" icon={<RotateCcw size={14} />} />
      </div>

      <div className="mt-2">
        <Button
          onClick={cleanup}
          disabled={inflight}
          label="Cleanup"
          icon={<Trash2 size={14} />}
          variant="danger"
        />
      </div>

      {tokenMissing && (
        <p className="mt-2 flex items-center gap-1 text-xs text-amber-400">
          <AlertTriangle size={12} /> Set a CONTROL_TOKEN to enable control actions.
        </p>
      )}
      {error !== null && (
        <p className="mt-2 flex items-center gap-1 text-xs text-rose-400">
          <AlertTriangle size={12} /> {error}
        </p>
      )}
    </div>
  );
}

function Button({
  onClick,
  disabled,
  label,
  icon,
  variant = 'default',
}: {
  onClick: () => void;
  disabled: boolean;
  label: string;
  icon: ReactNode;
  variant?: 'default' | 'danger';
}) {
  const base =
    'flex items-center justify-center gap-1 rounded px-2 py-1.5 text-xs font-medium transition disabled:cursor-not-allowed disabled:opacity-40';
  const color =
    variant === 'danger'
      ? 'bg-rose-900/50 text-rose-200 hover:bg-rose-800/60'
      : 'bg-slate-800 text-slate-100 hover:bg-slate-700';
  return (
    <button type="button" onClick={onClick} disabled={disabled} className={`${base} ${color}`}>
      {icon}
      {label}
    </button>
  );
}

function clampInt(raw: string, lo: number, hi: number, fallback: number): number {
  const v = Number.parseInt(raw, 10);
  if (!Number.isFinite(v)) return fallback;
  return Math.max(lo, Math.min(hi, v));
}
