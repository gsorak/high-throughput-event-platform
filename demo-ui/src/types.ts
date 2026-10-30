// Mirrors the JSON of demo-gateway (SessionView, Run, DedupState, IsolationReport, SystemMetrics.Overview).

export interface Tenant {
  key: 'a' | 'b';
  id: string;
  name: string;
  plan: string;
  eventsPerSecond: number;
  burst: number;
  dailyQuota: number;
}

export type RunPhase = 'SENDING' | 'PROCESSING' | 'COMPLETED' | 'INCOMPLETE' | 'FAILED';

export interface RunPoint {
  seconds: number;
  submitted: number;
  accepted: number;
  processed: number;
}

export interface Run {
  type: string;
  eventsPerSecond: number;
  durationSeconds: number;
  plannedEvents: number;
  startedAt: string;
  phase: RunPhase;
  submitted: number;
  accepted: number;
  rejected: number;
  rejectedByRate: number;
  rejectedByQuota: number;
  failed: number;
  processed: number;
  pending: number;
  sendingEndedAt: string | null;
  finishedAt: string | null;
  message: string | null;
  lastRejection: string | null;
  stopRequested: boolean;
  timeline: RunPoint[];
}

export type DedupPhase = 'DELIVERING' | 'DELIVERED' | 'REPLAYING' | 'VERIFIED' | 'FAILED';

export interface DedupState {
  phase: DedupPhase;
  batchSize: number;
  sampleEventIds: string[];
  countBefore: number | null;
  countAfterFirst: number | null;
  replayStatus: number | null;
  countAfterReplay: number | null;
  duplicatesSkipped: number | null;
  message: string;
  updatedAt: string;
}

export interface IsolationCheck {
  title: string;
  request: string;
  claims: Record<string, unknown>;
  tokenNote: string;
  expectedStatus: number;
  actualStatus: number;
  answeredForTenant: string | null;
  totalCount: number | null;
  passed: boolean;
}

export interface IsolationReport {
  checkedAt: string;
  allPassed: boolean;
  checks: IsolationCheck[];
}

export interface LimitAttempt {
  at: string;
  step: 'within' | 'burst' | 'quota';
  events: number;
  status: number;
  accepted: number;
  limit: 'rate' | 'quota' | null;
  retryAfterSeconds: number | null;
  detail: string | null;
  /** Tenant B's daily quota left just before this request, counted from this session's accepted events. */
  quotaRemainingBefore: number;
}

export interface SessionView {
  expiresAt: string;
  secondsRemaining: number;
  tenants: Tenant[];
  budget: { used: number; limit: number };
  runLimits: {
    maxEventsPerSecond: number;
    maxDurationSeconds: number;
    maxEventsPerRun: number;
    eventTypes: string[];
  };
  run: Run | null;
  systemRun: { busy: boolean; yours: boolean; secondsRunning: number | null };
  dedup: DedupState | null;
  isolation: IsolationReport | null;
  rateLimits: {
    attempts: LimitAttempt[];
    acceptedEvents: number;
    quotaRemaining: number;
    quotaExhausted: boolean;
    quotaResetsAt: string;
  };
  busy: string[];
}

export interface SeriesPoint {
  epochSecond: number;
  ingestRate: number | null;
  processRate: number | null;
  backlog: number | null;
}

export interface Overview {
  at: string;
  available: boolean;
  ingestRate: number | null;
  processRate: number | null;
  backlog: number | null;
  ingestedTotal: number | null;
  processedTotal: number | null;
  ingestPeakRate: number | null;
  processPeakRate: number | null;
  duplicatesSkipped: number | null;
  rejectedRequests: number | null;
  serverErrors: number | null;
  deadLettered: number | null;
  deliveryFailures: number | null;
  window: OverviewWindow;
  stepSeconds: number;
  series: SeriesPoint[];
  grafanaUrl: string | null;
}

export type OverviewWindow = '5m' | '15m';
