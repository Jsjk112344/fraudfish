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
    onmessage(ev) {
      try {
        const data = JSON.parse(ev.data) as Record<string, unknown>;
        onEvent({ event: ev.event, data });
      } catch {
        onError(new Error('Failed to parse discovery SSE event'));
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('Discovery SSE connection error'));
      throw err;
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
    onmessage(ev) {
      try {
        const data = JSON.parse(ev.data) as Record<string, unknown>;
        onEvent({ event: ev.event, data });
      } catch {
        onError(new Error('Failed to parse dashboard scan SSE event'));
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('Dashboard scan SSE connection error'));
      throw err;
    },
  });
}
