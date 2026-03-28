import type { AgentStream, AgentProgress } from '../types/dashboard';

interface LivePreviewProps {
  stream: AgentStream | null;
  narration: AgentProgress[];
  label?: string;
}

const STEP_LABELS: Record<string, string> = {
  discover_sistic: 'SISTIC.com.sg',
  discover_ticketmaster: 'Ticketmaster SG',
  extract_listing: 'Listing Page',
  investigate_seller: 'Seller Profile',
  verify_event: 'Event Verification',
  check_market: 'Market Scan',
  cross_platform: 'Cross-Platform Search',
};

export function LivePreview({ stream, narration, label }: LivePreviewProps) {
  if (!stream && narration.length === 0) return null;

  const stepLabel = stream ? STEP_LABELS[stream.step] || stream.step : '';

  return (
    <div className="glass border border-outline-variant/20 rounded-xl overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-outline-variant/10 bg-surface-container/50">
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-500 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
        </span>
        <span className="text-[10px] uppercase tracking-widest font-headline font-bold text-red-400">
          LIVE
        </span>
        <span className="text-xs text-on-surface-variant font-body">
          {label || `Agent navigating ${stepLabel}`}
        </span>
      </div>

      {/* Browser Preview */}
      {stream?.streaming_url && (
        <div className="relative w-full" style={{ paddingBottom: '56.25%' }}>
          <iframe
            src={stream.streaming_url}
            title="TinyFish Live Browser Preview"
            className="absolute inset-0 w-full h-full border-0"
            allow="autoplay"
            sandbox="allow-scripts allow-same-origin"
          />
        </div>
      )}

      {/* Agent Narration Log */}
      {narration.length > 0 && (
        <div className="px-4 py-3 max-h-32 overflow-y-auto border-t border-outline-variant/10 bg-surface-container-lowest/50">
          <div className="flex flex-col gap-1">
            {narration.map((entry, i) => (
              <div
                key={i}
                className={`flex items-start gap-2 text-xs font-mono ${
                  i === narration.length - 1 ? 'text-primary' : 'text-on-surface-variant/60'
                }`}
              >
                <span className="text-outline flex-shrink-0">
                  {STEP_LABELS[entry.step] || entry.step}
                </span>
                <span className="truncate">{entry.message}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
