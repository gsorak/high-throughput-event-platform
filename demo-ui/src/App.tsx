import { useCallback, useEffect, useState } from 'react';
import { ApiError, api, type LimitStep, type RunRequest } from './api';
import { DedupSection } from './components/DedupSection';
import { IngestSection } from './components/IngestSection';
import { IsolationSection } from './components/IsolationSection';
import { LimitsSection } from './components/LimitsSection';
import { OverviewSection } from './components/OverviewSection';
import { SessionBar } from './components/SessionBar';
import type { SessionView } from './types';

type Status = 'loading' | 'none' | 'ready' | 'unavailable';
type ActionKey = 'session' | 'ingest' | 'dedup' | 'isolation' | 'limits';

const FAST_POLL_MS = 1000;
const SLOW_POLL_MS = 5000;

function isActive(session: SessionView): boolean {
  const run = session.run?.phase;
  const dedup = session.dedup?.phase;
  return (
    run === 'SENDING' ||
    run === 'PROCESSING' ||
    dedup === 'DELIVERING' ||
    dedup === 'REPLAYING' ||
    session.busy.length > 0 ||
    session.systemRun.busy
  );
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : 'Something went wrong.';
}

export function App() {
  const [session, setSession] = useState<SessionView | null>(null);
  const [status, setStatus] = useState<Status>('loading');
  const [notice, setNotice] = useState<string | null>(null);
  const [errors, setErrors] = useState<Partial<Record<ActionKey, string>>>({});
  const [pending, setPending] = useState<Partial<Record<ActionKey, boolean>>>({});
  const [limitStep, setLimitStep] = useState<LimitStep | null>(null);

  const sessionLost = useCallback(() => {
    setSession(null);
    setStatus('none');
    setNotice('Your demo session has ended or expired. Start a new one to continue.');
  }, []);

  useEffect(() => {
    api
      .getSession()
      .then((view) => {
        setSession(view);
        setStatus('ready');
      })
      .catch((e: unknown) => {
        if (e instanceof ApiError && e.status === 401) {
          setStatus('none');
        } else {
          setStatus('unavailable');
          setNotice(message(e));
        }
      });
  }, []);

  const active = session != null && isActive(session);
  useEffect(() => {
    if (status !== 'ready') return;
    const timer = setInterval(
      () => {
        api
          .getSession()
          .then(setSession)
          .catch((e: unknown) => {
            if (e instanceof ApiError && e.status === 401) sessionLost();
          });
      },
      active ? FAST_POLL_MS : SLOW_POLL_MS,
    );
    return () => clearInterval(timer);
  }, [status, active, sessionLost]);

  const act = async (key: ActionKey, action: () => Promise<SessionView | void>) => {
    setPending((p) => ({ ...p, [key]: true }));
    setErrors((e) => ({ ...e, [key]: undefined }));
    try {
      const next = await action();
      if (next) {
        setSession(next);
        setStatus('ready');
        setNotice(null);
      }
    } catch (e) {
      if (e instanceof ApiError && e.status === 401 && key !== 'session') {
        sessionLost();
      } else {
        setErrors((errs) => ({ ...errs, [key]: message(e) }));
      }
    } finally {
      setPending((p) => ({ ...p, [key]: false }));
    }
  };

  const endSession = () =>
    act('session', async () => {
      await api.endSession();
      setSession(null);
      setStatus('none');
      setNotice('Session ended. Its temporary tenants are no longer used.');
    });

  const runLimitStep = async (step: LimitStep) => {
    setLimitStep(step);
    try {
      await act('limits', () => api.runLimitStep(step));
    } finally {
      setLimitStep(null);
    }
  };

  return (
    <div className="page">
      <header className="masthead">
        <h1>Event platform – interactive demo</h1>
        <p>
          A multi-tenant event pipeline running on the real services: a JWT-secured, rate-limited ingestion API, Kafka,
          and a processor that deduplicates and aggregates into PostgreSQL.
        </p>
        <ol className="pipeline" aria-label="Pipeline">
          <li>Ingestion API</li>
          <li>Kafka</li>
          <li>Processor</li>
          <li>PostgreSQL</li>
          <li>Statistics API</li>
        </ol>
      </header>

      {notice && (
        <p className="notice" role="status">
          {notice}
        </p>
      )}

      {status === 'loading' && <p className="muted">Loading …</p>}

      {(status === 'none' || status === 'unavailable') && (
        <section className="card start">
          <h2>Start a demo session</h2>
          <p>
            You get two temporary tenants for 15 minutes: <strong>tenant A</strong> on a standard plan and{' '}
            <strong>tenant B</strong> on a small plan. A session can send up to 50,000 events; only one load run
            executes at a time across all visitors. Reloading the page keeps your session and results.
          </p>
          <button type="button" onClick={() => act('session', api.startSession)} disabled={pending.session}>
            {pending.session ? 'Starting …' : 'Start demo session'}
          </button>
          {errors.session && (
            <p className="error" role="alert">
              {errors.session}
            </p>
          )}
        </section>
      )}

      {status === 'ready' && session && (
        <main>
          <SessionBar session={session} onEnd={endSession} ending={pending.session === true} />
          {errors.session && (
            <p className="error" role="alert">
              {errors.session}
            </p>
          )}
          <IngestSection
            session={session}
            pending={pending.ingest === true}
            error={errors.ingest ?? null}
            onStart={(request: RunRequest) => act('ingest', () => api.startRun(request))}
            onStop={() => act('ingest', api.stopRun)}
          />
          <DedupSection
            session={session}
            pending={pending.dedup === true}
            error={errors.dedup ?? null}
            onSend={() => act('dedup', api.sendDedupBatch)}
            onReplay={() => act('dedup', api.replayDedupBatch)}
          />
          <IsolationSection
            session={session}
            pending={pending.isolation === true}
            error={errors.isolation ?? null}
            onCheck={() => act('isolation', api.checkIsolation)}
          />
          <LimitsSection session={session} pending={limitStep} error={errors.limits ?? null} onStep={runLimitStep} />
          <OverviewSection onSessionLost={sessionLost} />
        </main>
      )}

      <footer className="muted">
        Limits per session: 15 minutes, 50,000 events, at most 1,000 events/s and 30 s per run. Load is generated by the
        server, never by your browser.
      </footer>
    </div>
  );
}
