import { fetchEventSource } from '@microsoft/fetch-event-source';

export async function startEventScan(
  eventName: string,
  city: string,
  onEvent: (event: { event: string; data: Record<string, unknown> }) => void,
  onError: (error: Error) => void,
  signal: AbortSignal
): Promise<void> {
  await fetchEventSource('/api/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ event_name: eventName, city }),
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
      onError(err instanceof Error ? err : new Error('Scan SSE connection error'));
      throw err;
    },
  });
}
