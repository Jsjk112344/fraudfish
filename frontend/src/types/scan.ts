import type { ClassificationCategory, VerdictResult } from './investigation';

export type ScanState = 'idle' | 'scanning' | 'complete' | 'error';

export type ListingStatus = 'pending' | 'investigating' | 'complete' | 'error';

export interface ScanListing {
  listing_id: string;
  url?: string;
  platform: string;
  title: string;
  price: number;
  seller: string;
  status: ListingStatus;
  verdict?: VerdictResult;
}

export interface ScanStats {
  total_listings: number;
  investigated: number;
  flagged: number;
  confirmed_scams: number;
  fraud_exposure: number;
  by_platform: Record<string, number>;
  by_category: Record<string, number>;
}

export interface ScanEvent {
  event: string;
  data: Record<string, unknown>;
}
