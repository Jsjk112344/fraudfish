import type { DiscoveredEvent } from '../types/dashboard';
import type { RiskLevel } from '../types/dashboard';

interface EventRiskCardProps {
  event: DiscoveredEvent;
  onScanEvent?: (event: DiscoveredEvent) => void;
  isExpanded?: boolean;
  onToggle?: () => void;
}

const RISK_STYLES: Record<RiskLevel, { border: string; badge: string; text: string }> = {
  CRITICAL: {
    border: 'border-l-4 border-l-error',
    badge: 'bg-error-container text-error',
    text: 'text-error',
  },
  HIGH: {
    border: 'border-l-4 border-l-orange-400',
    badge: 'bg-orange-400/10 text-orange-400',
    text: 'text-orange-400',
  },
  MODERATE: {
    border: 'border-l-4 border-l-amber-400',
    badge: 'bg-amber-400/10 text-amber-400',
    text: 'text-amber-400',
  },
  LOW: {
    border: 'border-l-4 border-l-green-400',
    badge: 'bg-green-400/10 text-green-400',
    text: 'text-green-400',
  },
};

const CATEGORY_ICONS: Record<string, string> = {
  concert: 'music_note',
  sports: 'sports',
  festival: 'festival',
  theatre: 'theater_comedy',
  comedy: 'sentiment_very_satisfied',
  other: 'event',
};

const VERDICT_STYLES: Record<string, string> = {
  LEGITIMATE: 'text-green-400',
  SCALPING_VIOLATION: 'text-amber-400',
  LIKELY_SCAM: 'text-error',
  COUNTERFEIT_RISK: 'text-orange-400',
};

export function EventRiskCard({ event, isExpanded, onToggle }: EventRiskCardProps) {
  const risk = RISK_STYLES[event.risk_level] || RISK_STYLES.LOW;
  const icon = CATEGORY_ICONS[event.category] || CATEGORY_ICONS.other;
  const isScanning = event.scan_status === 'scanning';
  const isScanned = event.scan_status === 'complete';

  return (
    <div
      className={`glass border border-outline-variant/20 rounded-xl overflow-hidden transition-all ${risk.border}`}
    >
      {/* Main Card */}
      <div
        className="p-4 cursor-pointer hover:bg-surface-container-high/30 transition-colors"
        onClick={onToggle}
      >
        <div className="flex items-start gap-3">
          {/* Category Icon */}
          <div className="flex-shrink-0 w-10 h-10 rounded-lg bg-surface-container-high flex items-center justify-center">
            <span className="material-symbols-outlined text-on-surface-variant">{icon}</span>
          </div>

          {/* Event Info */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <h3 className="text-sm font-headline font-bold text-on-surface truncate">
                {event.event_name}
              </h3>
              {event.sold_out && (
                <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-error-container text-error font-mono">
                  SOLD OUT
                </span>
              )}
              {event.popularity_hint && !event.sold_out && (
                <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-amber-400/10 text-amber-400 font-mono">
                  {event.popularity_hint}
                </span>
              )}
            </div>
            <div className="flex items-center gap-3 mt-1 text-xs text-on-surface-variant font-body">
              {event.venue && <span>{event.venue}</span>}
              {event.date && (
                <>
                  <span className="text-outline">|</span>
                  <span>{event.date}</span>
                </>
              )}
            </div>
            {(event.face_value_low || event.face_value_high) && (
              <p className="text-xs text-on-surface-variant font-mono mt-1">
                Face value: S$
                {event.face_value_low ?? '?'}
                {event.face_value_high ? ` – S$${event.face_value_high}` : ''}
              </p>
            )}
          </div>

          {/* Risk Score */}
          <div className="flex-shrink-0 text-right">
            <div className={`text-2xl font-headline font-bold font-mono ${risk.text}`}>
              {event.risk_score}
            </div>
            <span className={`text-[10px] uppercase tracking-widest font-headline font-bold px-2 py-0.5 rounded-full ${risk.badge}`}>
              {event.risk_level}
            </span>
          </div>
        </div>

        {/* Scan Status Row */}
        {(isScanning || isScanned) && (
          <div className="mt-3 pt-3 border-t border-outline-variant/10 flex items-center gap-4 text-xs font-body">
            {isScanning && (
              <div className="flex items-center gap-2 text-primary">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-primary" />
                </span>
                Scanning listings...
              </div>
            )}
            {isScanned && (
              <>
                <span className="text-on-surface-variant">
                  <span className="font-mono text-on-surface">{event.listings_found ?? 0}</span> listings
                </span>
                {(event.flagged ?? 0) > 0 && (
                  <span className="text-amber-400">
                    <span className="font-mono">{event.flagged}</span> flagged
                  </span>
                )}
                {(event.confirmed_scams ?? 0) > 0 && (
                  <span className="text-error">
                    <span className="font-mono">{event.confirmed_scams}</span> scams
                  </span>
                )}
                {(event.fraud_exposure ?? 0) > 0 && (
                  <span className="text-error font-mono">
                    S${event.fraud_exposure?.toLocaleString('en-SG', { minimumFractionDigits: 0 })}
                  </span>
                )}
              </>
            )}
          </div>
        )}
      </div>

      {/* Expanded Listings */}
      {isExpanded && event.listings && event.listings.length > 0 && (
        <div className="border-t border-outline-variant/10 bg-surface-container-lowest/50 px-4 py-3">
          <p className="text-[10px] uppercase tracking-widest text-on-surface-variant font-headline mb-2">
            Listings Found
          </p>
          <div className="flex flex-col gap-1.5">
            {event.listings.map((listing) => (
              <div
                key={listing.listing_id}
                className="flex items-center gap-3 text-xs font-body py-1.5 px-2 rounded bg-surface-container/50"
              >
                <span className="text-on-surface-variant w-16 truncate">{listing.platform}</span>
                <span className="text-on-surface flex-1 truncate">{listing.title}</span>
                <span className="text-on-surface font-mono">S${listing.price}</span>
                {listing.verdict && (
                  <span
                    className={`font-mono text-[10px] ${
                      VERDICT_STYLES[listing.verdict.category] || 'text-on-surface-variant'
                    }`}
                  >
                    {listing.verdict.category.replace('_', ' ')}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
