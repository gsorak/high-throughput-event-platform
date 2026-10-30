import { useEffect, useState } from 'react';
import { count, countdown } from '../format';
import type { SessionView } from '../types';

interface Props {
  session: SessionView;
  onEnd: () => void;
  ending: boolean;
}

/** The visitor's temporary tenants, remaining time and event budget. */
export function SessionBar({ session, onEnd, ending }: Props) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const remaining = (new Date(session.expiresAt).getTime() - now) / 1000;

  return (
    <section className="card session" aria-label="Your demo session">
      <div className="session-tenants">
        {session.tenants.map((tenant) => (
          <div key={tenant.key} className="tenant">
            <div className="tenant-name">
              {tenant.name} <span className="plan">{tenant.plan}</span>
            </div>
            <code className="tenant-id">{tenant.id}</code>
            <div className="muted">
              {count(tenant.eventsPerSecond)} events/s · burst {count(tenant.burst)} · {count(tenant.dailyQuota)} per
              day
            </div>
          </div>
        ))}
      </div>
      <div className="session-meta">
        <div>
          <span className="muted">Session ends in </span>
          <strong aria-label="Time remaining">{countdown(remaining)}</strong>
        </div>
        <label className="budget">
          <span className="muted">
            Events used: {count(session.budget.used)} of {count(session.budget.limit)}
          </span>
          <meter min={0} max={session.budget.limit} value={session.budget.used} />
        </label>
        <button type="button" className="secondary" onClick={onEnd} disabled={ending}>
          End session
        </button>
      </div>
    </section>
  );
}
