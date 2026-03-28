import { fetchEventSource } from '@microsoft/fetch-event-source';
import type { InvestigationEvent } from '../types/investigation';

export async function startInvestigation(
  url: string,
  onEvent: (event: InvestigationEvent) => void,
  onError: (error: Error) => void,
  signal: AbortSignal
): Promise<void> {
  await fetchEventSource('/api/investigate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url }),
    signal,
    onmessage(ev) {
      try {
        const data = JSON.parse(ev.data) as InvestigationEvent;
        onEvent(data);
      } catch {
        onError(new Error('Failed to parse SSE event'));
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('SSE connection error'));
      throw err; // stop retrying
    },
  });
}
