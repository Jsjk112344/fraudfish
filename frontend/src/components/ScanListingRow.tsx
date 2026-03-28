import type { ScanListing } from '../types/scan';
import type { ClassificationCategory } from '../types/investigation';
import { CATEGORY_COLORS } from '../types/investigation';

interface ScanListingRowProps {
  listing: ScanListing;
  isExpanded: boolean;
  onToggle: () => void;
}

const PLATFORM_ICONS: Record<string, string> = {
  carousell: 'storefront',
  viagogo: 'language',
};

const CATEGORY_BG: Record<ClassificationCategory, string> = {
  LEGITIMATE: 'bg-green-400/10',
  SCALPING_VIOLATION: 'bg-amber-400/10',
  LIKELY_SCAM: 'bg-error-container',
  COUNTERFEIT_RISK: 'bg-orange-400/10',
};

export function ScanListingRow({ listing, isExpanded, onToggle }: ScanListingRowProps) {
  const isPending = listing.status === 'pending';
  const isInvestigating = listing.status === 'investigating';
  const isComplete = listing.status === 'complete';
  const isError = listing.status === 'error';

  const icon = PLATFORM_ICONS[listing.platform.toLowerCase()] ?? 'language';

  return (
    <div
      className={`glass border rounded-xl overflow-hidden transition-colors ${
        isError
          ? 'border-error/30'
          : 'border-outline-variant/20'
      } ${isPending ? 'opacity-60' : ''} hover:bg-surface-container/30 cursor-pointer`}
    >
      {/* Main row */}
      <div
        className="flex items-center p-4 gap-4"
        onClick={onToggle}
      >
        {/* Left section */}
        <div className="flex-1 flex items-center gap-4">
          <div className="w-10 h-10 rounded-full bg-surface-container-highest flex items-center justify-center flex-shrink-0">
            <span className="material-symbols-outlined text-secondary text-xl">{icon}</span>
          </div>
          <div className="flex-1 min-w-0">
            <p className="font-headline font-bold text-sm text-on-surface truncate">
              {listing.title}
            </p>
            <p className="text-xs text-on-surface-variant font-body">
              Seller: {listing.seller} | {listing.platform}
            </p>
          </div>
        </div>

        {/* Middle section */}
        <div className="flex-shrink-0">
          <span className="font-mono text-sm text-on-surface">
            S${listing.price.toLocaleString()}
          </span>
        </div>

        {/* Right section */}
        <div className="flex items-center gap-3 flex-shrink-0">
          {isPending && (
            <span className="text-xs text-outline">Queued</span>
          )}

          {isInvestigating && (
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-primary" />
              </span>
              <span className="text-xs text-primary">Investigating...</span>
            </div>
          )}

          {isComplete && listing.verdict && (
            <div className="flex items-center gap-2">
              <span
                className={`rounded-full px-3 py-1 text-xs font-headline font-bold uppercase ${
                  CATEGORY_COLORS[listing.verdict.category]
                } ${CATEGORY_BG[listing.verdict.category]}`}
              >
                {listing.verdict.category.replace(/_/g, ' ')}
              </span>
              <span className="text-xs font-mono text-on-surface-variant">
                {listing.verdict.confidence.toFixed(1)}%
              </span>
            </div>
          )}

          {isError && (
            <span className="text-xs text-error">Error</span>
          )}

          <span
            className={`material-symbols-outlined text-on-surface-variant hover:text-on-surface transition-transform duration-200 ${
              isExpanded ? 'rotate-180' : ''
            }`}
          >
            expand_more
          </span>
        </div>
      </div>

      {/* Expanded content */}
      {isExpanded && isComplete && listing.verdict && (
        <div className="border-t border-outline-variant/10 p-5">
          <p className="text-sm text-on-surface-variant font-body whitespace-pre-wrap">
            {listing.verdict.reasoning}
          </p>
          <button
            onClick={onToggle}
            className="mt-3 text-xs text-on-surface-variant underline cursor-pointer"
          >
            Collapse
          </button>
        </div>
      )}
    </div>
  );
}
