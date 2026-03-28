import { fetchEventSource } from '@microsoft/fetch-event-source';
import type { DiscoveredEvent } from '../types/dashboard';

type SSEEvent = { event: string; data: Record<string, unknown> };

export async function startDiscovery(
  onEvent: (event: SSEEvent) => void,
  onError: (error: Error) => void,
  signal: AbortSignal
): Promise<void> {
  await fetchEventSource('/api/dashboard/discover', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
    signal,
    openWhenHidden: true, // Don't reconnect when tab is backgrounded
    onmessage(ev) {
      if (!ev.data || !ev.data.trim()) return;
      try {
        const data = JSON.parse(ev.data) as Record<string, unknown>;
        onEvent({ event: ev.event, data });
      } catch {
        // Non-JSON events (pings, comments) are normal — skip silently
      }
    },
    onerror(err) {
      // Don't retry — each retry fires new TinyFish runs
      onError(err instanceof Error ? err : new Error('Discovery SSE connection error'));
      throw err; // throwing stops fetch-event-source from retrying
    },
  });
}

export async function startDashboardScan(
  events: DiscoveredEvent[],
  onEvent: (event: SSEEvent) => void,
  onError: (error: Error) => void,
  signal: AbortSignal
): Promise<void> {
  await fetchEventSource('/api/dashboard/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ events }),
    signal,
    openWhenHidden: true,
    onmessage(ev) {
      if (!ev.data || !ev.data.trim()) return;
      try {
        const data = JSON.parse(ev.data) as Record<string, unknown>;
        onEvent({ event: ev.event, data });
      } catch {
        // Non-JSON events (pings, comments) are normal — skip silently
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('Dashboard scan SSE connection error'));
      throw err;
    },
  });
}
