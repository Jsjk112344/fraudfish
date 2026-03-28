export type StepStatus = 'pending' | 'active' | 'complete' | 'error';

export type ClassificationCategory =
  | 'LEGITIMATE'
  | 'SCALPING_VIOLATION'
  | 'LIKELY_SCAM'
  | 'COUNTERFEIT_RISK';

export type SignalSeverity = 'CRITICAL' | 'WARNING' | 'NEUTRAL' | 'CLEAR';

export interface InvestigationEvent {
  step: string;
  status: StepStatus;
  data?: Record<string, unknown>;
}

export interface Signal {
  name: string;
  severity: SignalSeverity;
  segmentsFilled: number;
}

export interface VerdictResult {
  category: ClassificationCategory;
  confidence: number;
  reasoning: string;
  signals: Signal[];
}

export interface StepDefinition {
  id: string;
  label: string;
  icon: string;
}

export interface StepWithState extends StepDefinition {
  status: StepStatus;
  data?: Record<string, unknown>;
}

export type InvestigationState = 'idle' | 'running' | 'complete' | 'error';

export const INVESTIGATION_STEPS: StepDefinition[] = [
  { id: 'extract_listing', label: 'Extracting Listing Data', icon: 'page_info' },
  { id: 'investigate_seller', label: 'Investigating Seller Profile', icon: 'person_search' },
  { id: 'verify_event', label: 'Verifying Event Details', icon: 'event_available' },
  { id: 'check_market', label: 'Checking Market Rates', icon: 'analytics' },
  { id: 'cross_platform', label: 'Cross-Platform Search', icon: 'travel_explore' },
  { id: 'synthesize', label: 'Synthesizing Verdict', icon: 'psychology' },
];

export const CATEGORY_COLORS: Record<ClassificationCategory, string> = {
  LEGITIMATE: 'text-green-400',
  SCALPING_VIOLATION: 'text-amber-400',
  LIKELY_SCAM: 'text-error',
  COUNTERFEIT_RISK: 'text-orange-400',
};

export const SEVERITY_COLORS: Record<SignalSeverity, string> = {
  CRITICAL: 'bg-error',
  WARNING: 'bg-amber-400',
  NEUTRAL: 'bg-tertiary',
  CLEAR: 'bg-green-400',
};
