import { fetchEventSource } from '@microsoft/fetch-event-source';

export async function startEventScan(
  eventName: string,
  onEvent: (event: { event: string; data: Record<string, unknown> }) => void,
  onError: (error: Error) => void,
  signal: AbortSignal
): Promise<void> {
  await fetchEventSource('/api/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ event_name: eventName }),
    signal,
    onmessage(ev) {
      try {
        const data = JSON.parse(ev.data) as Record<string, unknown>;
        onEvent({ event: ev.event, data });
      } catch {
        onError(new Error('Failed to parse scan SSE event'));
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('Scan SSE connection error'));
      throw err;
    },
  });
}
