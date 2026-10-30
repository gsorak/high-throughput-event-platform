import type { Overview, OverviewWindow, SessionView } from './types';

/** A refusal from the gateway, carrying its problem detail. */
export class ApiError extends Error {
  readonly status: number;
  readonly title: string | undefined;

  constructor(status: number, message: string, title?: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.title = title;
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { 'X-Demo-Client': '1', Accept: 'application/json' };
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
  }
  let response: Response;
  try {
    response = await fetch(path, {
      method,
      headers,
      credentials: 'same-origin',
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    throw new ApiError(0, 'The demo gateway cannot be reached. Check your connection and try again.');
  }
  if (response.status === 204) {
    return undefined as T;
  }
  const data: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const problem = (data ?? {}) as { detail?: string; title?: string };
    throw new ApiError(response.status, problem.detail ?? `The request failed (${response.status}).`, problem.title);
  }
  return data as T;
}

export interface RunRequest {
  type: string;
  eventsPerSecond: number;
  durationSeconds: number;
}

export type LimitStep = 'within' | 'burst' | 'quota';

export const api = {
  startSession: () => request<SessionView>('POST', '/demo/session'),
  getSession: () => request<SessionView>('GET', '/demo/session'),
  endSession: () => request<void>('DELETE', '/demo/session'),
  startRun: (run: RunRequest) => request<SessionView>('POST', '/demo/runs', run),
  stopRun: () => request<SessionView>('POST', '/demo/runs/current/stop'),
  sendDedupBatch: () => request<SessionView>('POST', '/demo/dedup/send'),
  replayDedupBatch: () => request<SessionView>('POST', '/demo/dedup/replay'),
  checkIsolation: () => request<SessionView>('POST', '/demo/isolation/check'),
  runLimitStep: (step: LimitStep) => request<SessionView>('POST', `/demo/limits/${step}`),
  overview: (window: OverviewWindow) => request<Overview>('GET', `/demo/overview?window=${window}`),
};
