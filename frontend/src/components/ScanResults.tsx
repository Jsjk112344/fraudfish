import { useState, useRef, useEffect } from 'react';
import type { ScanListing, ScanStats } from '../types/scan';
import { ThreatSummary } from './ThreatSummary';
import { ScanListingRow } from './ScanListingRow';

interface ScanResultsProps {
  listings: ScanListing[];
  stats: ScanStats;
  isScanning: boolean;
  progressMessage?: string | null;
  progressLog?: Array<{ phase: string; message: string }>;
}

const PHASE_ICONS: Record<string, string> = {
  discovery: 'travel_explore',
  investigating: 'policy',
  general: 'info',
};

export function ScanResults({ listings, stats, isScanning, progressMessage, progressLog }: ScanResultsProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [progressLog?.length]);

  // Empty state
  if (listings.length === 0 && !isScanning) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[300px] text-center">
        <span className="material-symbols-outlined text-6xl text-outline/30">radar</span>
        <h2 className="text-xl font-headline font-bold text-on-surface mt-4">
          Scan an Event
        </h2>
        <p className="text-sm font-body text-on-surface-variant max-w-md mt-2">
          Enter an event name above to discover and investigate listings across Carousell and Viagogo.
        </p>
      </div>
    );
  }

  return (
    <div>
      {/* Agent Activity Panel — shown while scanning */}
      {isScanning && (
        <div className="glass border border-outline-variant/20 rounded-xl overflow-hidden mb-6">
          {/* Header */}
          <div className="flex items-center gap-2 px-4 py-2.5 border-b border-outline-variant/10 bg-surface-container/50">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-500 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
            </span>
            <span className="text-[10px] uppercase tracking-widest font-headline font-bold text-red-400">
              AGENT ACTIVE
            </span>
            <span className="text-xs text-on-surface-variant font-body">
              {progressMessage || 'Initializing scan...'}
            </span>
          </div>

          {/* Activity Log */}
          {progressLog && progressLog.length > 0 && (
            <div className="px-4 py-3 max-h-40 overflow-y-auto bg-surface-container-lowest/50">
              <div className="flex flex-col gap-1.5">
                {progressLog.map((entry, i) => (
                  <div
                    key={i}
                    className={`flex items-start gap-2 text-xs font-mono ${
                      i === progressLog.length - 1 ? 'text-primary' : 'text-on-surface-variant/50'
                    }`}
                  >
                    <span className="material-symbols-outlined text-sm flex-shrink-0 mt-px">
                      {PHASE_ICONS[entry.phase] || 'info'}
                    </span>
                    <span className="truncate">{entry.message}</span>
                  </div>
                ))}
                <div ref={logEndRef} />
              </div>
            </div>
          )}

          {/* Scanning without log entries yet — loading state */}
          {(!progressLog || progressLog.length === 0) && (
            <div className="flex items-center justify-center py-8">
              <div className="flex flex-col items-center gap-3">
                <span className="material-symbols-outlined text-4xl text-primary/40 animate-pulse">
                  satellite_alt
                </span>
                <p className="text-sm font-body text-on-surface-variant">
                  Launching TinyFish agents...
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      <ThreatSummary stats={stats} isScanning={isScanning} />

      <div className="mt-6 flex flex-col gap-3">
        {listings.map((listing) => (
          <ScanListingRow
            key={listing.listing_id}
            listing={listing}
            isExpanded={expandedId === listing.listing_id}
            onToggle={() =>
              setExpandedId((prev) =>
                prev === listing.listing_id ? null : listing.listing_id
              )
            }
          />
        ))}
      </div>
    </div>
  );
}
