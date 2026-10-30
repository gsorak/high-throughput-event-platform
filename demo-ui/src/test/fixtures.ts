import type { Run, SessionView } from '../types';

export function makeSession(overrides: Partial<SessionView> = {}): SessionView {
  return {
    expiresAt: new Date(Date.now() + 15 * 60_000).toISOString(),
    secondsRemaining: 900,
    tenants: [
      {
        key: 'a',
        id: 'demo-0123456789abcdef-a',
        name: 'Tenant A',
        plan: 'demo-standard',
        eventsPerSecond: 1000,
        burst: 2000,
        dailyQuota: 60000,
      },
      {
        key: 'b',
        id: 'demo-fedcba9876543210-b',
        name: 'Tenant B',
        plan: 'demo-small',
        eventsPerSecond: 100,
        burst: 500,
        dailyQuota: 1000,
      },
    ],
    budget: { used: 0, limit: 50000 },
    runLimits: {
      maxEventsPerSecond: 1000,
      maxDurationSeconds: 30,
      maxEventsPerRun: 20000,
      eventTypes: ['page.view', 'api.call', 'order.placed', 'payment.failed'],
    },
    run: null,
    systemRun: { busy: false, yours: false, secondsRunning: null },
    dedup: null,
    isolation: null,
    rateLimits: {
      attempts: [],
      acceptedEvents: 0,
      quotaRemaining: 1000,
      quotaExhausted: false,
      quotaResetsAt: '2030-01-02T00:00:00Z',
    },
    busy: [],
    ...overrides,
  };
}

export function makeRun(overrides: Partial<Run> = {}): Run {
  return {
    type: 'page.view',
    eventsPerSecond: 1000,
    durationSeconds: 20,
    plannedEvents: 20000,
    startedAt: '2030-01-01T12:00:00Z',
    phase: 'SENDING',
    submitted: 5000,
    accepted: 5000,
    rejected: 0,
    rejectedByRate: 0,
    rejectedByQuota: 0,
    failed: 0,
    processed: 2500,
    pending: 2500,
    sendingEndedAt: null,
    finishedAt: null,
    message: null,
    lastRejection: null,
    stopRequested: false,
    timeline: [
      { seconds: 0, submitted: 0, accepted: 0, processed: 0 },
      { seconds: 5, submitted: 5000, accepted: 5000, processed: 2500 },
    ],
    ...overrides,
  };
}

export function jsonResponse(status: number, body: unknown): Response {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: { 'Content-Type': status >= 400 ? 'application/problem+json' : 'application/json' },
  });
}
