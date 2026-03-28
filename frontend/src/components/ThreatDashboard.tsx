import { useState } from 'react';
import type { DiscoveredEvent, DashboardAggregateStats, DashboardState, AgentStream, AgentProgress } from '../types/dashboard';
import { EventRiskCard } from './EventRiskCard';
import { LivePreview } from './LivePreview';

interface ThreatDashboardProps {
  state: DashboardState;
  events: DiscoveredEvent[];
  aggregateStats: DashboardAggregateStats;
  isLive: boolean;
  progressMessage: string | null;
  activeStream: AgentStream | null;
  agentNarration: AgentProgress[];
  onScanAll: () => void;
}

export function ThreatDashboard({
  state,
  events,
  aggregateStats,
  isLive,
  progressMessage,
  activeStream,
  agentNarration,
  onScanAll,
}: ThreatDashboardProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const isScanning = state === 'scanning';
  const isComplete = state === 'complete';
  const hasEvents = events.length > 0;

  // Count risk levels
  const riskCounts = events.reduce(
    (acc, ev) => {
      acc[ev.risk_level] = (acc[ev.risk_level] || 0) + 1;
      return acc;
    },
    {} as Record<string, number>
  );

  return (
    <div>
      {/* Aggregate Stats Banner (shown during/after scanning) */}
      {(isScanning || isComplete) && (
        <div
          className={`glass border border-outline-variant/20 rounded-xl p-6 mb-6 ${
            isComplete && aggregateStats.total_confirmed_scams > 0
              ? 'border-l-4 border-l-error'
              : isComplete
                ? 'border-l-4 border-l-green-400'
                : ''
          }`}
        >
          {isScanning && (
            <div className="flex items-center gap-3 mb-4">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-primary" />
              </span>
              <span className="text-xs uppercase tracking-widest font-headline font-bold text-primary">
                SCANNING THREAT LANDSCAPE
              </span>
            </div>
          )}
          {isComplete && (
            <h2 className="text-xs uppercase tracking-[0.2em] font-headline text-on-surface-variant mb-4">
              THREAT LANDSCAPE REPORT
            </h2>
          )}

          <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
            <div className="bg-surface-container-low rounded-lg p-4">
              <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
                Events Scanned
              </p>
              <p className="text-[28px] font-headline font-bold text-on-surface">
                {aggregateStats.total_events_scanned}
              </p>
            </div>
            <div className="bg-surface-container-low rounded-lg p-4">
              <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
                Total Listings
              </p>
              <p className="text-[28px] font-headline font-bold text-on-surface">
                {aggregateStats.total_listings}
              </p>
            </div>
            <div className="bg-surface-container-low rounded-lg p-4">
              <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
                Flagged
              </p>
              <p className="text-[28px] font-headline font-bold text-amber-400">
                {aggregateStats.total_flagged}
              </p>
            </div>
            <div className="bg-surface-container-low rounded-lg p-4">
              <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
                Confirmed Scams
              </p>
              <p className="text-[28px] font-headline font-bold text-error">
                {aggregateStats.total_confirmed_scams}
              </p>
            </div>
            <div className="bg-surface-container-low rounded-lg p-4">
              <p className="text-xs uppercase tracking-wider text-on-surface-variant font-body">
                Fraud Exposure
              </p>
              <p className="text-[28px] font-headline font-bold text-error font-mono">
                S${aggregateStats.total_fraud_exposure.toLocaleString('en-SG', { minimumFractionDigits: 0 })}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Risk Distribution + Scan All Button */}
      {hasEvents && !isScanning && (
        <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-xs uppercase tracking-[0.2em] font-headline text-on-surface-variant mr-2">
              {isComplete ? 'Events by Risk' : 'Threat Ranking'}
            </h2>
            {riskCounts.CRITICAL && (
              <span className="rounded-full px-2 py-0.5 text-xs font-mono bg-error-container text-error">
                {riskCounts.CRITICAL} Critical
              </span>
            )}
            {riskCounts.HIGH && (
              <span className="rounded-full px-2 py-0.5 text-xs font-mono bg-orange-400/10 text-orange-400">
                {riskCounts.HIGH} High
              </span>
            )}
            {riskCounts.MODERATE && (
              <span className="rounded-full px-2 py-0.5 text-xs font-mono bg-amber-400/10 text-amber-400">
                {riskCounts.MODERATE} Moderate
              </span>
            )}
            {riskCounts.LOW && (
              <span className="rounded-full px-2 py-0.5 text-xs font-mono bg-green-400/10 text-green-400">
                {riskCounts.LOW} Low
              </span>
            )}
            {isLive && (
              <span className="rounded-full px-2 py-0.5 text-[10px] font-mono uppercase tracking-wider bg-primary/10 text-primary border border-primary/20">
                LIVE DATA
              </span>
            )}
          </div>

          {state === 'discovered' && (
            <button
              onClick={onScanAll}
              className="bg-primary-container text-on-primary px-5 py-2 rounded-lg font-headline font-bold text-sm hover:brightness-110 transition flex items-center gap-2"
            >
              <span className="material-symbols-outlined text-lg">radar</span>
              Scan All Events
            </button>
          )}
        </div>
      )}

      {/* Event Cards */}
      <div className="flex flex-col gap-3">
        {events.map((event) => (
          <EventRiskCard
            key={event.event_id}
            event={event}
            isExpanded={expandedId === event.event_id}
            onToggle={() =>
              setExpandedId((prev) => (prev === event.event_id ? null : event.event_id))
            }
          />
        ))}
      </div>

      {/* Discovering state */}
      {state === 'discovering' && (
        <div className="flex flex-col gap-6">
          {/* Live browser preview */}
          {(activeStream || agentNarration.length > 0) && (
            <LivePreview
              stream={activeStream}
              narration={agentNarration}
              label={progressMessage || undefined}
            />
          )}

          {/* Fallback when no stream yet */}
          {!activeStream && agentNarration.length === 0 && (
            <div className="flex flex-col items-center justify-center min-h-[200px] text-center">
              <div className="relative mb-6">
                <span className="material-symbols-outlined text-6xl text-primary/30 animate-pulse">
                  satellite_alt
                </span>
              </div>
              <h2 className="text-xl font-headline font-bold text-on-surface">
                Scanning for Events
              </h2>
              <p className="text-sm font-body text-on-surface-variant max-w-md mt-2">
                {progressMessage || 'Launching TinyFish agents to scan SISTIC and Ticketmaster SG...'}
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
