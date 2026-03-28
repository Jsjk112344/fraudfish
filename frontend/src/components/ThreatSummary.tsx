import type { ScanStats } from '../types/scan';
import type { ClassificationCategory } from '../types/investigation';

interface ThreatSummaryProps {
  stats: ScanStats;
  isScanning: boolean;
}

const PLATFORM_ICONS: Record<string, string> = {
  carousell: 'storefront',
  viagogo: 'language',
};

const CATEGORY_PILL_STYLES: Record<ClassificationCategory, string> = {
  LEGITIMATE: 'text-green-400 bg-green-400/10',
  SCALPING_VIOLATION: 'text-amber-400 bg-amber-400/10',
  LIKELY_SCAM: 'text-error bg-error-container',
  COUNTERFEIT_RISK: 'text-orange-400 bg-orange-400/10',
};

const CATEGORY_LABELS: Record<ClassificationCategory, string> = {
  LEGITIMATE: 'Legitimate',
  SCALPING_VIOLATION: 'Scalping',
  LIKELY_SCAM: 'Likely Scam',
  COUNTERFEIT_RISK: 'Counterfeit Risk',
};

export function ThreatSummary({ stats, isScanning }: ThreatSummaryProps) {
  const progressPercent =
    stats.total_listings > 0
      ? (stats.investigated / stats.total_listings) * 100
      : 0;

  return (
    <div
      className={`glass border border-outline-variant/20 rounded-xl p-6 ${
        !isScanning && stats.confirmed_scams > 0
          ? 'border-l-4 border-l-error'
          : !isScanning
            ? 'border-l-4 border-l-green-400'
            : ''
      }`}
    >
      {/* Header */}
      {isScanning ? (
        <>
          <div className="flex items-center gap-3">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-primary" />
            </span>
            <span className="text-xs uppercase tracking-widest font-headline font-bold text-primary">
              SCANNING:
            </span>
            <span className="text-sm font-body text-on-surface-variant">
              {stats.investigated}/{stats.total_listings} investigated
            </span>
          </div>
          <div className="h-1 bg-surface-container rounded-full mt-3">
            <div
              className="bg-primary h-full rounded-full transition-all duration-500"
              style={{ width: `${progressPercent}%` }}
            />
          </div>
        </>
      ) : (
        <h2 className="text-xs uppercase tracking-[0.2em] font-headline text-on-surface-variant">
          THREAT INTELLIGENCE SUMMARY
        </h2>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-4">
        <div className="bg-surface-container-low rounded-lg p-4">
          <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
            Total Listings
          </p>
          <p className="text-[32px] font-headline font-bold text-on-surface">
            {stats.total_listings}
          </p>
        </div>
        <div className="bg-surface-container-low rounded-lg p-4">
          <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
            Flagged Suspicious
          </p>
          <p className="text-[32px] font-headline font-bold text-amber-400">
            {stats.flagged}
          </p>
        </div>
        <div className="bg-surface-container-low rounded-lg p-4">
          <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
            Confirmed Scams
          </p>
          <p className="text-[32px] font-headline font-bold text-error">
            {stats.confirmed_scams}
          </p>
        </div>
        <div className="bg-surface-container-low rounded-lg p-4">
          <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
            Fraud Exposure
          </p>
          <p className="text-[32px] font-headline font-bold text-error font-mono">
            S${stats.fraud_exposure.toLocaleString('en-SG', { minimumFractionDigits: 0 })}
          </p>
        </div>
      </div>

      {/* Platform breakdown + Category distribution */}
      <div className="mt-4 flex flex-wrap items-center gap-2">
        {Object.entries(stats.by_platform)
          .filter(([, count]) => count > 0)
          .map(([platform, count]) => (
            <span
              key={platform}
              className="py-1 px-2 rounded-full bg-surface-container-highest text-xs inline-flex items-center gap-1"
            >
              <span className="material-symbols-outlined text-sm">
                {PLATFORM_ICONS[platform.toLowerCase()] ?? 'language'}
              </span>
              {platform}: {count}
            </span>
          ))}

        {Object.entries(stats.by_category)
          .filter(([, count]) => count > 0)
          .map(([category, count]) => (
            <span
              key={category}
              className={`rounded-full px-2 py-0.5 text-xs font-mono ${
                CATEGORY_PILL_STYLES[category as ClassificationCategory] ?? 'text-on-surface-variant bg-surface-container'
              }`}
            >
              {count} {CATEGORY_LABELS[category as ClassificationCategory] ?? category}
            </span>
          ))}
      </div>
    </div>
  );
}
