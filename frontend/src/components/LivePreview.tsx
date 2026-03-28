import type { AgentStream, AgentProgress } from '../types/dashboard';

interface LivePreviewProps {
  streams: AgentStream[];
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
  extract_details: 'Event Details',
};

export function LivePreview({ streams, narration, label }: LivePreviewProps) {
  if (streams.length === 0 && narration.length === 0) return null;

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
          {label || `${streams.length} agent${streams.length !== 1 ? 's' : ''} running`}
        </span>
      </div>

      {/* Browser Previews — grid layout for multiple agents */}
      {streams.length > 0 && (
        <div className={`grid gap-0 ${streams.length === 1 ? 'grid-cols-1' : 'grid-cols-2'}`}>
          {streams.map((stream, i) => (
            <div key={stream.streaming_url} className="relative">
              {/* Stream label */}
              <div className="absolute top-2 left-2 z-10 flex items-center gap-1.5 bg-black/70 backdrop-blur-sm rounded px-2 py-0.5">
                <span className="relative flex h-1.5 w-1.5">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-green-400" />
                </span>
                <span className="text-[9px] uppercase tracking-wider font-mono text-green-300">
                  {STEP_LABELS[stream.step] || stream.step}
                </span>
              </div>
              <div className="relative w-full" style={{ paddingBottom: streams.length === 1 ? '56.25%' : '65%' }}>
                <iframe
                  src={stream.streaming_url}
                  title={`TinyFish Agent: ${STEP_LABELS[stream.step] || stream.step}`}
                  className="absolute inset-0 w-full h-full border-0"
                  allow="autoplay"
                  sandbox="allow-scripts allow-same-origin"
                />
              </div>
            </div>
          ))}
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
