import { useState, useRef, useCallback } from 'react';
import type { StepWithState, VerdictResult, InvestigationState, InvestigationEvent } from '../types/investigation';
import { INVESTIGATION_STEPS } from '../types/investigation';
import { startInvestigation } from '../api/investigate';

function createInitialSteps(): StepWithState[] {
  return INVESTIGATION_STEPS.map((step) => ({
    ...step,
    status: 'pending' as const,
    data: undefined,
  }));
}

export function useInvestigation() {
  const [state, setState] = useState<InvestigationState>('idle');
  const [steps, setSteps] = useState<StepWithState[]>(createInitialSteps);
  const [verdict, setVerdict] = useState<VerdictResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);

  const handleEvent = useCallback((event: InvestigationEvent) => {
    if (event.step === 'error') {
      const errorMsg = (event.data?.error as string) ?? 'Investigation failed';
      setError(errorMsg);
      setState('error');
      return;
    }

    if (event.step === 'verdict' && event.status === 'complete') {
      setVerdict(event.data as unknown as VerdictResult);
      setState('complete');
      return;
    }

    setSteps((prev) =>
      prev.map((s) =>
        s.id === event.step
          ? { ...s, status: event.status, data: event.data }
          : s
      )
    );
  }, []);

  const start = useCallback(
    (url: string) => {
      // Reset state
      setSteps(createInitialSteps());
      setVerdict(null);
      setError(null);
      setState('running');

      const controller = new AbortController();
      abortControllerRef.current = controller;

      startInvestigation(
        url,
        handleEvent,
        (err) => {
          // Only set error if not aborted
          if (!controller.signal.aborted) {
            setError(err.message);
            setState('error');
          }
        },
        controller.signal
      ).catch(() => {
        // Connection closed -- only error if not aborted
        if (!controller.signal.aborted) {
          setState((prev) => (prev === 'running' ? 'error' : prev));
        }
      });
    },
    [handleEvent]
  );

  const cancel = useCallback(() => {
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    setSteps(createInitialSteps());
    setVerdict(null);
    setError(null);
    setState('idle');
  }, []);

  return { state, steps, verdict, error, start, cancel };
}
