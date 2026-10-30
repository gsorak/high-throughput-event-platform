import type { LimitStep } from '../api';
import { count, time, utcTime } from '../format';
import type { LimitAttempt, SessionView } from '../types';
import { Section } from './Section';

interface Props {
  session: SessionView;
  onStep: (step: LimitStep) => void;
  pending: LimitStep | null;
  error: string | null;
}

export function LimitsSection({ session, onStep, pending, error }: Props) {
  const tenant = session.tenants.find((t) => t.key === 'b');
  const limits = session.rateLimits;
  const running = pending != null || session.busy.includes('limits');
  if (!tenant) return null;
  const latest = limits.attempts[0];
  const batchTooLarge =
    !limits.quotaExhausted &&
    latest != null &&
    latest.status === 429 &&
    latest.limit === 'quota' &&
    latest.quotaRemainingBefore > 0 &&
    latest.events > latest.quotaRemainingBefore
      ? latest
      : null;

  return (
    <Section
      id="limits"
      number={4}
      title="Rate limits and quotas"
      scope="session"
      error={error}
      intro={
        <>
          <p>
            <strong>{tenant.name}</strong> has a small plan: {count(tenant.eventsPerSecond)} events/s, bursts up to{' '}
            {count(tenant.burst)}, and {count(tenant.dailyQuota)} events per UTC day. The ingestion API enforces both
            and answers HTTP 429 with the limit that was hit.
          </p>
          <details className="more">
            <summary>How a batch is admitted</summary>
            <ul>
              <li>
                Every event takes one token from the rate limit and one from the daily quota. A batch is admitted only
                if all of its events fit; otherwise the whole batch is refused and none of it reaches Kafka.
              </li>
              <li>
                So a batch can be refused for the quota while some quota is left: with 450 events left, a batch of 500
                is refused, but a batch of 450 would still fit.
              </li>
              <li>
                A rate refusal says when to retry (Retry-After). A quota refusal lasts until the quota refills at 00:00
                UTC.
              </li>
              <li>
                The remaining quota shown here is counted from the events this session got accepted for {tenant.name},
                which exists only in this session.
              </li>
            </ul>
          </details>
        </>
      }
    >
      <div className="actions">
        <button type="button" onClick={() => onStep('within')} disabled={running}>
          Send 50 events
        </button>
        <button type="button" onClick={() => onStep('burst')} disabled={running}>
          Send a burst (2 × 500)
        </button>
        <button type="button" onClick={() => onStep('quota')} disabled={running}>
          Use up the daily quota
        </button>
      </div>
      {pending === 'quota' && (
        <p className="muted" aria-live="polite">
          Sending and waiting out rate limits as Retry-After asks; this takes a few seconds …
        </p>
      )}
      <label className="progress">
        Daily quota: {count(limits.acceptedEvents)} used, {count(limits.quotaRemaining)} remaining of{' '}
        {count(tenant.dailyQuota)}
        <meter min={0} max={tenant.dailyQuota} value={limits.acceptedEvents} />
      </label>
      {limits.quotaExhausted && (
        <p className="callout" role="status">
          {tenant.name}'s daily quota is used up: no events remain. The platform refuses its events until the quota
          resets at {utcTime(limits.quotaResetsAt)}. Nothing resets it early; start a new session to get new tenants.
        </p>
      )}
      {batchTooLarge && (
        <p className="callout callout-info" role="status">
          The last batch of {count(batchTooLarge.events)} events was refused for the daily quota although{' '}
          {count(batchTooLarge.quotaRemainingBefore)} events of quota remain. A batch is admitted as a whole, so none of
          it was accepted; a batch of at most {count(batchTooLarge.quotaRemainingBefore)} events still fits.
        </p>
      )}
      {limits.attempts.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th scope="col">Time</th>
                <th scope="col">Step</th>
                <th scope="col">Events</th>
                <th scope="col">Answer</th>
              </tr>
            </thead>
            <tbody>
              {limits.attempts.map((attempt, i) => (
                <tr key={`${attempt.at}-${i}`}>
                  <td>{time(attempt.at)}</td>
                  <td>{attempt.step}</td>
                  <td>{count(attempt.events)}</td>
                  <td>
                    <Answer attempt={attempt} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

function quotaReason(attempt: LimitAttempt): string {
  if (attempt.quotaRemainingBefore === 0) {
    return 'daily quota used up';
  }
  if (attempt.events > attempt.quotaRemainingBefore) {
    return `batch exceeds remaining quota (${count(attempt.quotaRemainingBefore)} left)`;
  }
  return 'daily quota';
}

function Answer({ attempt }: { attempt: LimitAttempt }) {
  if (attempt.status === 202) {
    return <span className="badge badge-ok">202 accepted {count(attempt.accepted)}</span>;
  }
  if (attempt.status === 429) {
    if (attempt.limit === 'quota') {
      return <span className="badge badge-bad">429 {quotaReason(attempt)}</span>;
    }
    const retry = attempt.retryAfterSeconds != null ? `, retry after ${count(attempt.retryAfterSeconds)} s` : '';
    return <span className="badge badge-warn">429 rate limit{retry}</span>;
  }
  return <span className="badge badge-bad">{attempt.status === 0 ? 'unreachable' : `HTTP ${attempt.status}`}</span>;
}
