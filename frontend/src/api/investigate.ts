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
    openWhenHidden: true,
    onmessage(ev) {
      if (!ev.data || !ev.data.trim()) return;
      try {
        const data = JSON.parse(ev.data) as InvestigationEvent;
        onEvent(data);
      } catch {
        // Non-JSON events (pings, comments) are normal — skip silently
      }
    },
    onerror(err) {
      onError(err instanceof Error ? err : new Error('SSE connection error'));
      throw err; // stop retrying
    },
  });
}
