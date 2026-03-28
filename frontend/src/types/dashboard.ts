import type { VerdictResult } from './investigation';

export type DashboardState = 'idle' | 'discovering' | 'discovered' | 'scanning' | 'complete' | 'error';

export type RiskLevel = 'CRITICAL' | 'HIGH' | 'MODERATE' | 'LOW';

export interface DiscoveredEvent {
  event_id: string;
  event_name: string;
  venue?: string;
  date?: string;
  category: string;
  face_value_low?: number;
  face_value_high?: number;
  sold_out?: boolean;
  popularity_hint?: string;
  source: string;
  risk_score: number;
  risk_level: RiskLevel;
  // Populated during scanning phase
  scan_status?: 'pending' | 'scanning' | 'complete';
  listings_found?: number;
  flagged?: number;
  confirmed_scams?: number;
  fraud_exposure?: number;
  listings?: DashboardListing[];
}

export interface DashboardListing {
  listing_id: string;
  platform: string;
  title: string;
  price: number;
  seller: string;
  verdict?: VerdictResult;
}

export interface DashboardAggregateStats {
  total_events_scanned: number;
  total_listings: number;
  total_flagged: number;
  total_confirmed_scams: number;
  total_fraud_exposure: number;
}

export interface AgentStream {
  step: string;
  streaming_url: string;
}

export interface AgentProgress {
  step: string;
  message: string;
}
