import type { WriterSnapshot } from '@shared/schema';

interface ProcessPanelProps {
  writers: WriterSnapshot[];
}

export default function ProcessPanel({ writers }: ProcessPanelProps) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <h2 className="mb-2 text-sm font-semibold text-slate-200">Writers</h2>
      {writers.length === 0 ? (
        <p className="text-xs text-slate-500">No writers attached.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="text-slate-500">
              <tr>
                <th className="py-1 pr-3 font-medium">Status</th>
                <th className="py-1 pr-3 font-medium">PID</th>
                <th className="py-1 pr-3 font-medium">Role</th>
                <th className="py-1 pr-3 text-right font-medium">Produced</th>
                <th className="py-1 pr-3 text-right font-medium">Consumed</th>
                <th className="py-1 pr-3 text-right font-medium">CRC err</th>
                <th className="py-1 text-right font-medium">Last seen</th>
              </tr>
            </thead>
            <tbody className="font-mono">
              {writers.map((w) => (
                <tr key={w.slot} className="border-t border-slate-800 text-slate-300">
                  <td className="py-1 pr-3">
                    <span
                      className={`inline-block h-2.5 w-2.5 rounded-full ${
                        w.alive ? 'bg-emerald-500' : 'bg-rose-500'
                      }`}
                      title={w.alive ? 'alive' : 'dead'}
                    />
                  </td>
                  <td className="py-1 pr-3">{w.pid}</td>
                  <td className="py-1 pr-3">
                    <span
                      className={`rounded px-1.5 py-0.5 text-[10px] uppercase ${
                        w.role === 'producer'
                          ? 'bg-emerald-500/15 text-emerald-400'
                          : 'bg-sky-500/15 text-sky-400'
                      }`}
                    >
                      {w.role}
                    </span>
                  </td>
                  <td className="py-1 pr-3 text-right">{w.produced.toLocaleString()}</td>
                  <td className="py-1 pr-3 text-right">{w.consumed.toLocaleString()}</td>
                  <td
                    className={`py-1 pr-3 text-right ${
                      w.crc_errors > 0 ? 'text-rose-400' : 'text-slate-400'
                    }`}
                  >
                    {w.crc_errors}
                  </td>
                  <td className="py-1 text-right text-slate-400">{formatSeen(w.last_seen_ms_ago)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function formatSeen(ms: number): string {
  if (ms < 1000) return `${ms} ms ago`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s ago`;
  return `${(ms / 60_000).toFixed(1)} min ago`;
}
