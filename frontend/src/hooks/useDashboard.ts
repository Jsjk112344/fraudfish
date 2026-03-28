import { useState, useRef, useCallback } from 'react';
import type {
  DashboardState,
  DiscoveredEvent,
  DashboardListing,
  DashboardAggregateStats,
  AgentStream,
  AgentProgress,
} from '../types/dashboard';
import type { VerdictResult } from '../types/investigation';
import { startDiscovery, startDashboardScan } from '../api/dashboard';

const EMPTY_AGGREGATE: DashboardAggregateStats = {
  total_events_scanned: 0,
  total_listings: 0,
  total_flagged: 0,
  total_confirmed_scams: 0,
  total_fraud_exposure: 0,
};

export function useDashboard() {
  const [state, setState] = useState<DashboardState>('idle');
  const [events, setEvents] = useState<DiscoveredEvent[]>([]);
  const [aggregateStats, setAggregateStats] = useState<DashboardAggregateStats>(EMPTY_AGGREGATE);
  const [error, setError] = useState<string | null>(null);
  const [isLive, setIsLive] = useState(false);
  const [progressMessage, setProgressMessage] = useState<string | null>(null);
  const [activeStream, setActiveStream] = useState<AgentStream | null>(null);
  const [agentNarration, setAgentNarration] = useState<AgentProgress[]>([]);
  const abortRef = useRef<AbortController | null>(null);

  const handleDiscoveryEvent = useCallback(
    (event: { event: string; data: Record<string, unknown> }) => {
      switch (event.event) {
        case 'discovery_progress':
          setProgressMessage(event.data.message as string);
          break;

        case 'agent_streaming': {
          const stream: AgentStream = {
            step: event.data.step as string,
            streaming_url: event.data.streaming_url as string,
          };
          setActiveStream(stream);
          break;
        }

        case 'agent_progress': {
          const progress: AgentProgress = {
            step: event.data.step as string,
            message: event.data.message as string,
          };
          setAgentNarration((prev) => [...prev.slice(-19), progress]); // Keep last 20
          break;
        }

        case 'events_discovered': {
          const discovered = (event.data.events as unknown as DiscoveredEvent[]) || [];
          setEvents(discovered);
          setIsLive(event.data.is_live as boolean);
          setActiveStream(null); // Clear preview once discovery is done
          break;
        }

        case 'discovery_complete':
          setState('discovered');
          setProgressMessage(null);
          setActiveStream(null);
          break;
      }
    },
    []
  );

  const handleScanEvent = useCallback(
    (event: { event: string; data: Record<string, unknown> }) => {
      switch (event.event) {
        case 'event_scan_started': {
          const eventId = event.data.event_id as string;
          setEvents((prev) =>
            prev.map((ev) =>
              ev.event_id === eventId ? { ...ev, scan_status: 'scanning' as const } : ev
            )
          );
          break;
        }

        case 'event_listings_found': {
          const eventId = event.data.event_id as string;
          const listings = (event.data.listings as unknown as DashboardListing[]) || [];
          const count = event.data.count as number;
          setEvents((prev) =>
            prev.map((ev) =>
              ev.event_id === eventId
                ? { ...ev, listings_found: count, listings }
                : ev
            )
          );
          break;
        }

        case 'event_listing_verdict': {
          const eventId = event.data.event_id as string;
          const listingId = event.data.listing_id as string;
          const verdict = event.data.verdict as unknown as VerdictResult;
          setEvents((prev) =>
            prev.map((ev) => {
              if (ev.event_id !== eventId) return ev;
              const updatedListings = (ev.listings || []).map((l) =>
                l.listing_id === listingId ? { ...l, verdict } : l
              );
              return { ...ev, listings: updatedListings };
            })
          );
          break;
        }

        case 'event_scan_complete': {
          const eventId = event.data.event_id as string;
          const stats = event.data.stats as Record<string, unknown>;
          setEvents((prev) =>
            prev.map((ev) =>
              ev.event_id === eventId
                ? {
                    ...ev,
                    scan_status: 'complete' as const,
                    flagged: stats.flagged as number,
                    confirmed_scams: stats.confirmed_scams as number,
                    fraud_exposure: stats.fraud_exposure as number,
                  }
                : ev
            )
          );
          break;
        }

        case 'dashboard_scan_complete': {
          const agg = event.data.aggregate_stats as unknown as DashboardAggregateStats;
          if (agg) setAggregateStats(agg);
          setState('complete');
          break;
        }
      }
    },
    []
  );

  /** Phase 1: Discover events and rank by risk */
  const discover = useCallback(() => {
    setEvents([]);
    setAggregateStats(EMPTY_AGGREGATE);
    setError(null);
    setIsLive(false);
    setProgressMessage(null);
    setActiveStream(null);
    setAgentNarration([]);
    setState('discovering');

    const controller = new AbortController();
    abortRef.current = controller;

    startDiscovery(
      handleDiscoveryEvent,
      (err) => {
        if (!controller.signal.aborted) {
          setError(err.message);
          setState('error');
        }
      },
      controller.signal
    ).catch(() => {
      if (!controller.signal.aborted) {
        setState((prev) => (prev === 'discovering' ? 'error' : prev));
      }
    });
  }, [handleDiscoveryEvent]);

  /** Phase 2: Scan top-risk events for fraudulent listings */
  const scanAll = useCallback(() => {
    if (events.length === 0) return;
    setState('scanning');
    setAggregateStats(EMPTY_AGGREGATE);

    // Mark all events as pending scan
    setEvents((prev) =>
      prev.map((ev) => ({
        ...ev,
        scan_status: 'pending' as const,
        listings: [],
        listings_found: 0,
        flagged: 0,
        confirmed_scams: 0,
        fraud_exposure: 0,
      }))
    );

    const controller = new AbortController();
    abortRef.current = controller;

    startDashboardScan(
      events,
      handleScanEvent,
      (err) => {
        if (!controller.signal.aborted) {
          setError(err.message);
          setState('error');
        }
      },
      controller.signal
    ).catch(() => {
      if (!controller.signal.aborted) {
        setState((prev) => (prev === 'scanning' ? 'error' : prev));
      }
    });
  }, [events, handleScanEvent]);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setState('idle');
    setEvents([]);
    setAggregateStats(EMPTY_AGGREGATE);
    setError(null);
    setProgressMessage(null);
  }, []);

  return {
    state,
    events,
    aggregateStats,
    error,
    isLive,
    progressMessage,
    activeStream,
    agentNarration,
    discover,
    scanAll,
    cancel,
  };
}
