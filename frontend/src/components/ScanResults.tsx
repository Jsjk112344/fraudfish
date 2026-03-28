import { useState } from 'react';
import type { ScanListing, ScanStats } from '../types/scan';
import { ThreatSummary } from './ThreatSummary';
import { ScanListingRow } from './ScanListingRow';

interface ScanResultsProps {
  listings: ScanListing[];
  stats: ScanStats;
  isScanning: boolean;
}

export function ScanResults({ listings, stats, isScanning }: ScanResultsProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

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
