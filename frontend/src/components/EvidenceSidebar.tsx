import { BarChart, Bar, XAxis, YAxis, Cell } from 'recharts';
import type { StepWithState, VerdictResult } from '../types/investigation';

interface EvidenceSidebarProps {
  steps: StepWithState[];
  verdict: VerdictResult | null;
}

function getStepData(steps: StepWithState[], stepId: string): Record<string, unknown> | undefined {
  const step = steps.find((s) => s.id === stepId);
  return step?.status === 'complete' ? step.data : undefined;
}

const BAR_COLORS = ['#ffb4ab', '#afc6ff', '#4cd6ff'];

export function EvidenceSidebar({ steps }: EvidenceSidebarProps) {
  const listingData = getStepData(steps, 'extract_listing');
  const sellerData = getStepData(steps, 'investigate_seller');
  const eventData = getStepData(steps, 'verify_event');
  const marketData = getStepData(steps, 'check_market');
  const crossPlatformData = getStepData(steps, 'cross_platform');

  const accountAgeDays = sellerData?.account_age_days as number | undefined;
  const priceDeviation = marketData?.price_deviation_percent as number | undefined;

  const chartData =
    listingData && marketData && eventData
      ? [
          { name: 'This Listing', value: listingData.price as number },
          { name: 'Avg. Market Price', value: marketData.average_price as number },
          { name: 'Face Value (Official)', value: eventData.face_value as number },
        ]
      : null;

  return (
    <div className="bg-surface-container-low border border-outline-variant/20 rounded-xl overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-outline-variant/10 flex items-center gap-2">
        <span
          className="material-symbols-outlined text-on-surface text-lg"
          style={{ fontVariationSettings: "'FILL' 1" }}
        >
          lock
        </span>
        <span className="font-headline font-bold text-sm text-on-surface">Evidence Lock</span>
      </div>

      {/* Content */}
      <div className="p-5 space-y-6">
        {/* Listing Proof */}
        {listingData && (
          <div>
            <p className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-2">
              LISTING PROOF
            </p>
            <div className="aspect-video bg-surface-container rounded-lg flex items-center justify-center">
              <span className="text-xs text-on-surface-variant">Listing Screenshot</span>
            </div>
          </div>
        )}

        {/* Seller Profile */}
        {sellerData && (
          <div>
            <p className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-2">
              SELLER PROFILE
            </p>
            <div className="bg-surface-container rounded-lg p-3 space-y-2">
              <div className="flex justify-between text-xs">
                <span className="text-on-surface-variant">Username</span>
                <span className="font-mono text-on-surface">{sellerData.username as string}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-on-surface-variant">Account Age</span>
                <span
                  className={`font-mono ${
                    accountAgeDays !== undefined && accountAgeDays < 30
                      ? 'text-error'
                      : 'text-on-surface'
                  }`}
                >
                  {accountAgeDays} days
                </span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-on-surface-variant">Reviews</span>
                <span className="font-mono text-on-surface">
                  {sellerData.reviews_count as number}
                </span>
              </div>
              {listingData && (
                <div className="flex justify-between text-xs">
                  <span className="text-on-surface-variant">Payment</span>
                  <span className="font-mono text-on-surface">
                    {listingData.transfer_method as string}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Price Comparison */}
        {chartData && (
          <div>
            <p className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-2">
              PRICE COMPARISON
            </p>
            <BarChart
              width={280}
              height={120}
              data={chartData}
              layout="vertical"
              margin={{ top: 0, right: 0, bottom: 0, left: 0 }}
            >
              <YAxis
                type="category"
                dataKey="name"
                width={110}
                tick={{ fill: '#c2c6d8', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
              />
              <XAxis type="number" hide={true} />
              <Bar dataKey="value" radius={[0, 4, 4, 0]} barSize={12}>
                {chartData.map((_entry, index) => (
                  <Cell key={index} fill={BAR_COLORS[index]} />
                ))}
              </Bar>
            </BarChart>
            {priceDeviation !== undefined && (
              <>
                <p className="text-xs font-mono text-on-surface-variant mt-2">
                  Price is {Math.abs(priceDeviation)}% {priceDeviation < 0 ? 'lower' : 'higher'} than market average
                </p>
                {(marketData?.is_outlier as boolean) && (
                  <p className="font-mono text-xs text-error uppercase tracking-wider mt-1">
                    STATISTICAL OUTLIER - {(marketData?.outlier_direction as string)?.replace('_', ' ')}
                  </p>
                )}
              </>
            )}
          </div>
        )}

        {/* Cross-Platform Matches */}
        {crossPlatformData && (
          <div>
            <p className="text-[10px] uppercase tracking-wider text-on-surface-variant mb-2">
              CROSS-PLATFORM MATCHES
            </p>
            {(crossPlatformData.duplicates_found as boolean) ? (
              <div className="space-y-2">
                {((crossPlatformData.matches as Array<Record<string, unknown>>) || []).map((match, i) => {
                  const sim = match.similarity_score as number;
                  const simPct = Math.round(sim * 100);
                  const simClass = sim >= 0.9 ? 'text-error' : 'text-amber-400';
                  return (
                    <div key={i} className="bg-surface-container rounded-lg p-3">
                      <div className="flex items-center gap-1.5 mb-1">
                        <span className="material-symbols-outlined text-on-surface-variant text-sm">travel_explore</span>
                        <span className="text-xs font-body text-on-surface">{match.platform as string}</span>
                      </div>
                      <p className="font-mono text-[11px] text-on-surface-variant">
                        Seller: &quot;{match.seller_name as string}&quot;
                      </p>
                      <p className={`font-mono text-[11px] ${simClass}`}>
                        Similarity: {simPct}%
                      </p>
                      <p className="font-mono text-[11px] text-on-surface">
                        S${(match.price as number)?.toFixed(2)}
                      </p>
                    </div>
                  );
                })}
              </div>
            ) : (
              <p className="text-xs font-body text-on-surface-variant italic">
                No cross-platform duplicates detected
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
