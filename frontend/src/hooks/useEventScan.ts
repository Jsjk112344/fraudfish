import { useState, useRef, useCallback } from 'react';
import type { ScanListing, ScanStats, ScanState } from '../types/scan';
import type { VerdictResult } from '../types/investigation';
import { startEventScan } from '../api/scan';

const EMPTY_STATS: ScanStats = {
  total_listings: 0,
  investigated: 0,
  flagged: 0,
  confirmed_scams: 0,
  fraud_exposure: 0,
  by_platform: {},
  by_category: {},
};

export function useEventScan() {
  const [state, setState] = useState<ScanState>('idle');
  const [listings, setListings] = useState<ScanListing[]>([]);
  const [stats, setStats] = useState<ScanStats>(EMPTY_STATS);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const handleEvent = useCallback((event: { event: string; data: Record<string, unknown> }) => {
    switch (event.event) {
      case 'scan_started':
        // Already in scanning state from start()
        break;

      case 'listings_found': {
        const foundListings = (event.data.listings as Array<Record<string, unknown>>) || [];
        setListings(
          foundListings.map((l) => ({
            listing_id: l.listing_id as string,
            url: l.url as string | undefined,
            platform: l.platform as string,
            title: l.title as string,
            price: l.price as number,
            seller: l.seller as string,
            status: 'pending' as const,
          }))
        );
        setStats((prev) => ({
          ...prev,
          total_listings: event.data.total_count as number,
          by_platform: (event.data.by_platform as Record<string, number>) || {},
        }));
        break;
      }

      case 'listing_update': {
        const id = event.data.listing_id as string;
        const status = event.data.status as ScanListing['status'];
        setListings((prev) =>
          prev.map((l) => (l.listing_id === id ? { ...l, status } : l))
        );
        break;
      }

      case 'listing_verdict': {
        const id = event.data.listing_id as string;
        const verdict = event.data.verdict as unknown as VerdictResult;
        setListings((prev) =>
          prev.map((l) =>
            l.listing_id === id ? { ...l, status: 'complete' as const, verdict } : l
          )
        );
        break;
      }

      case 'scan_stats': {
        setStats(event.data as unknown as ScanStats);
        break;
      }

      case 'scan_complete': {
        const finalStats = event.data.final_stats as unknown as ScanStats;
        if (finalStats) setStats(finalStats);
        setState('complete');
        break;
      }

      default:
        break;
    }
  }, []);

  const start = useCallback((eventName: string) => {
    // Reset state
    setListings([]);
    setStats(EMPTY_STATS);
    setError(null);
    setState('scanning');

    const controller = new AbortController();
    abortControllerRef.current = controller;

    startEventScan(
      eventName,
      handleEvent,
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
  }, [handleEvent]);

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    setListings([]);
    setStats(EMPTY_STATS);
    setError(null);
    setState('idle');
  }, []);

  return { state, listings, stats, error, start, cancel };
}
