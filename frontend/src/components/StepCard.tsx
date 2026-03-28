import type { StepWithState } from '../types/investigation';

function MarketDataBlock({ data }: { data: Record<string, unknown> }) {
  const listingsScanned = data.listings_scanned as number;
  const breakdown = data.platform_breakdown as Record<string, number> | undefined;
  const isOutlier = data.is_outlier as boolean;
  const outlierDir = data.outlier_direction as string | null;
  const deviation = data.price_deviation_percent as number | undefined;
  const percentile = data.this_listing_percentile as number | undefined;

  if (listingsScanned < 4) {
    return (
      <p className="font-mono text-[11px] text-on-surface-variant italic">
        Insufficient market data ({listingsScanned} listings found)
      </p>
    );
  }

  const breakdownStr = breakdown
    ? Object.entries(breakdown).map(([k, v]) => `${k}: ${v}`).join(', ')
    : '';

  const devClass = deviation !== undefined
    ? (deviation < -30 ? 'text-error' : deviation > 100 ? 'text-amber-400' : 'text-on-surface-variant')
    : 'text-on-surface-variant';

  const pctClass = percentile !== undefined
    ? (percentile < 10 ? 'text-error' : percentile > 90 ? 'text-amber-400' : 'text-on-surface-variant')
    : 'text-on-surface-variant';

  return (
    <div className="font-mono text-[11px] text-on-surface-variant leading-relaxed space-y-0.5">
      <p>Listings Scanned: {listingsScanned} ({breakdownStr})</p>
      <p>Median Price: S${(data.median_price as number)?.toFixed(2)}</p>
      <p>Average Price: S${(data.average_price as number)?.toFixed(2)}</p>
      <p>Range: S${(data.min_price as number)?.toFixed(2)} - S${(data.max_price as number)?.toFixed(2)}</p>
      <p className={pctClass}>This Listing: {percentile}th percentile</p>
      <p className={devClass}>Deviation: {deviation}% from median</p>
      <p className={isOutlier ? 'text-error' : 'text-green-400'}>
        Outlier: {isOutlier ? 'YES' : 'NO'}{outlierDir ? ` - ${outlierDir.replace('_', ' ')}` : ''}
      </p>
    </div>
  );
}

function CrossPlatformDataBlock({ data }: { data: Record<string, unknown> }) {
  const found = data.duplicates_found as boolean;
  const matches = (data.matches as Array<Record<string, unknown>>) || [];
  const displayed = matches.slice(0, 3);
  const remaining = matches.length - 3;

  return (
    <div className="font-mono text-[11px] leading-relaxed space-y-1">
      <p className={found ? 'text-amber-400' : 'text-green-400'}>
        Duplicates Found: {found ? 'YES' : 'NO'}
      </p>
      {!found && (
        <p className="text-on-surface-variant">No matching sellers or listings detected.</p>
      )}
      {displayed.map((match, i) => {
        const sim = match.similarity_score as number;
        const simPct = Math.round(sim * 100);
        const simClass = sim >= 0.9 ? 'text-error' : 'text-amber-400';
        return (
          <div key={i} className="text-on-surface-variant pl-2 mt-1">
            <p className="text-on-surface">Match {i + 1}: {match.platform as string}</p>
            <p>  Seller: &quot;{match.seller_name as string}&quot;</p>
            <p>  Title: &quot;{match.listing_title as string}&quot;</p>
            <p>  Price: S${(match.price as number)?.toFixed(2)}</p>
            <p className={simClass}>  Similarity: {simPct}%</p>
          </div>
        );
      })}
      {remaining > 0 && (
        <p className="text-on-surface-variant">... and {remaining} more matches</p>
      )}
    </div>
  );
}

interface StepCardProps {
  step: StepWithState;
}

export function StepCard({ step }: StepCardProps) {
  const isPending = step.status === 'pending';
  const isActive = step.status === 'active';
  const isComplete = step.status === 'complete';
  const isError = step.status === 'error';
  const isLive = isComplete && step.data?._live === true;

  return (
    <div className="flex gap-6">
      {/* Circle */}
      <div
        className={`w-14 h-14 rounded-full flex-shrink-0 flex items-center justify-center border transition-all ${
          isActive
            ? 'bg-surface-container-highest border-primary/50 shadow-[0_0_15px_rgba(175,198,255,0.3)]'
            : isComplete
              ? 'bg-surface-container-highest border-green-400/50'
              : isError
                ? 'bg-surface-container-highest border-error/50'
                : 'bg-surface-container-highest border-outline-variant/50'
        }`}
      >
        {isComplete ? (
          <span
            className="material-symbols-outlined text-green-400 text-2xl"
            style={{ fontVariationSettings: "'FILL' 1" }}
            aria-label="Step complete"
          >
            check_circle
          </span>
        ) : isError ? (
          <span className="material-symbols-outlined text-error text-2xl">error</span>
        ) : (
          <span
            className={`material-symbols-outlined text-2xl ${
              isActive ? 'text-primary animate-pulse' : 'text-secondary'
            }`}
          >
            {step.icon}
          </span>
        )}
      </div>

      {/* Card */}
      <div
        className={`glass border rounded-xl p-5 flex-1 transition-colors ${
          isActive
            ? 'border-primary/20 hover:border-primary/30'
            : isError
              ? 'border-error/30'
              : isPending
                ? 'border-outline-variant/10'
                : 'border-outline-variant/20 hover:border-primary/30'
        }`}
      >
        {/* Header */}
        <div className="flex items-center justify-between">
          <span
            className={`font-headline font-bold text-sm ${
              isPending ? 'text-on-surface-variant/50' : 'text-on-surface'
            }`}
          >
            {step.label}
          </span>

          {/* Status indicator */}
          {isActive && (
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-primary" />
              </span>
              <span className="text-primary text-xs uppercase tracking-wider font-body">
                Processing
              </span>
            </div>
          )}
          {isError && (
            <span className="text-error text-xs uppercase tracking-wider font-body">Error</span>
          )}
          {isLive && (
            <span
              className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-bold uppercase tracking-wider text-green-400 border border-green-500/25 transition-opacity duration-300 ease-out"
              style={{ backgroundColor: 'rgba(34, 197, 94, 0.15)' }}
              aria-label="Data sourced from live investigation"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-green-400" />
              LIVE
            </span>
          )}
        </div>

        {/* Active: progress bar */}
        {isActive && (
          <div className="h-1 bg-surface-container rounded-full mt-3">
            <div className="bg-primary h-full rounded-full transition-all duration-1000 w-full" />
          </div>
        )}

        {/* Complete: data block */}
        {isComplete && step.data && (
          <div className="bg-black/20 rounded p-3 mt-3">
            {step.id === 'check_market' ? (
              <MarketDataBlock data={step.data} />
            ) : step.id === 'cross_platform' ? (
              <CrossPlatformDataBlock data={step.data} />
            ) : (
              <pre className="font-mono text-[11px] text-on-surface-variant leading-relaxed whitespace-pre-wrap">
                {JSON.stringify(step.data, null, 2).slice(0, 200)}
                {JSON.stringify(step.data, null, 2).length > 200 ? '...' : ''}
              </pre>
            )}
          </div>
        )}

        {/* Error: message */}
        {isError && step.data?.error && (
          <p className="mt-2 text-error text-xs font-body">
            {String(step.data.error)}
          </p>
        )}
      </div>
    </div>
  );
}
