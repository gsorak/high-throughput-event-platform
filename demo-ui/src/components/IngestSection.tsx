import { useState } from 'react';
import type { RunRequest } from '../api';
import { count } from '../format';
import type { Run, RunPhase, SessionView, Tenant } from '../types';
import { LineChart } from './LineChart';
import { Figure, Section } from './Section';

interface Props {
  session: SessionView;
  onStart: (request: RunRequest) => void;
  onStop: () => void;
  pending: boolean;
  error: string | null;
}

const RATES = [100, 250, 500, 750, 1000];
const DURATIONS = [5, 10, 20, 30];
const DEFAULT_RATE = 500;
const DEFAULT_DURATION = 20;

const PHASES: Record<RunPhase, { label: string; tone: string; text: string }> = {
  SENDING: { label: 'Sending', tone: 'busy', text: 'The gateway is sending events to the ingestion API.' },
  PROCESSING: {
    label: 'Processing backlog',
    tone: 'busy',
    text: 'Sending has finished. The processor is still aggregating events that Kafka has accepted.',
  },
  COMPLETED: { label: 'Completed', tone: 'ok', text: 'Every accepted event is aggregated in PostgreSQL.' },
  INCOMPLETE: {
    label: 'Not drained in time',
    tone: 'warn',
    text: 'Some accepted events were not aggregated before the time limit. They are still in Kafka and will be processed later.',
  },
  FAILED: { label: 'Failed', tone: 'bad', text: 'The run could not be carried out.' },
};

export function IngestSection({ session, onStart, onStop, pending, error }: Props) {
  const limits = session.runLimits;
  const tenantA = session.tenants.find((t) => t.key === 'a');
  const [type, setType] = useState(limits.eventTypes[0] ?? 'page.view');
  const [eventsPerSecond, setRate] = useState(Math.min(DEFAULT_RATE, limits.maxEventsPerSecond));
  const [durationSeconds, setDuration] = useState(Math.min(DEFAULT_DURATION, limits.maxDurationSeconds));
  const run = session.run;
  const planned = eventsPerSecond * durationSeconds;
  const remainingBudget = session.budget.limit - session.budget.used;
  const yourRunActive = run != null && (run.phase === 'SENDING' || run.phase === 'PROCESSING');
  const otherRunActive = session.systemRun.busy && !session.systemRun.yours;

  let blocked: string | null = null;
  if (planned > limits.maxEventsPerRun) {
    blocked = `A run may send at most ${count(limits.maxEventsPerRun)} events.`;
  } else if (planned > remainingBudget) {
    blocked = `Your session has ${count(remainingBudget)} events left.`;
  } else if (otherRunActive) {
    blocked = `Another visitor's run is in progress (${session.systemRun.secondsRunning ?? 0} s). Only one run executes at a time; please wait a moment.`;
  }

  return (
    <Section
      id="ingest"
      number={1}
      title="Ingest and process"
      scope="session"
      error={error}
      intro={
        <>
          <p>
            Send events for <strong>tenant A</strong>. Kafka accepts them quickly (HTTP 202); the processor aggregates
            them into PostgreSQL more slowly, so pending events build up and then drain.
          </p>
          <details className="more">
            <summary>How a run is measured</summary>
            <ul>
              <li>
                <strong>Accepted</strong> means Kafka stored the event. <strong>Aggregated</strong> means the statistics
                API reports it in PostgreSQL. <strong>Pending</strong> is accepted minus aggregated, for this run only.
              </li>
              <li>
                A run is <em>completed</em> only when every accepted event is aggregated. Rejected events never reached
                Kafka and are not part of that.
              </li>
              <li>
                The gateway sends batches of up to 500 events every 250 ms. After a slow response it catches up by at
                most half a second of events, so it stays within the plan's burst instead of being rate-limited.
              </li>
              <li>The Kafka backlog of the whole system is shown in section 5.</li>
            </ul>
          </details>
        </>
      }
    >
      <form
        className="controls"
        onSubmit={(e) => {
          e.preventDefault();
          onStart({ type, eventsPerSecond, durationSeconds });
        }}
      >
        <label>
          Event type
          <select value={type} onChange={(e) => setType(e.target.value)} disabled={yourRunActive}>
            {limits.eventTypes.map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        </label>
        <label>
          Rate
          <select value={eventsPerSecond} onChange={(e) => setRate(Number(e.target.value))} disabled={yourRunActive}>
            {RATES.filter((r) => r <= limits.maxEventsPerSecond).map((r) => (
              <option key={r} value={r}>
                {count(r)} events/s{tenantA && r === tenantA.eventsPerSecond ? ' (plan maximum)' : ''}
              </option>
            ))}
          </select>
        </label>
        <label>
          Duration
          <select
            value={durationSeconds}
            onChange={(e) => setDuration(Number(e.target.value))}
            disabled={yourRunActive}
          >
            {DURATIONS.filter((d) => d <= limits.maxDurationSeconds).map((d) => (
              <option key={d} value={d}>
                {d} s
              </option>
            ))}
          </select>
        </label>
        <div className="actions">
          {yourRunActive && run.phase === 'SENDING' ? (
            <button type="button" className="secondary" onClick={onStop} disabled={pending || run.stopRequested}>
              Stop sending
            </button>
          ) : (
            <button type="submit" disabled={pending || yourRunActive || blocked != null}>
              Send {count(planned)} events
            </button>
          )}
        </div>
      </form>
      {tenantA && (
        <p className="muted small">
          Tenant A's plan allows {count(tenantA.eventsPerSecond)} events/s with a burst of {count(tenantA.burst)}.
        </p>
      )}
      {blocked && !yourRunActive && <p className="muted">{blocked}</p>}
      {run && <RunStatus run={run} tenant={tenantA} />}
    </Section>
  );
}

function RunStatus({ run, tenant }: { run: Run; tenant: Tenant | undefined }) {
  const phase = PHASES[run.phase];
  const x = run.timeline.map((p) => p.seconds);
  const lost = run.rejected + run.failed > 0;
  const sendingEnded =
    run.sendingEndedAt == null ? null : (Date.parse(run.sendingEndedAt) - Date.parse(run.startedAt)) / 1000;
  return (
    <div className="result" aria-live="polite">
      <p>
        <span className={`badge badge-${phase.tone}`}>{phase.label}</span> {phase.text}
        {run.message && run.phase !== 'COMPLETED' && <span className="muted"> {run.message}</span>}
      </p>
      <dl className="figures">
        <Figure label="Submitted" value={count(run.submitted)} hint={`of ${count(run.plannedEvents)} planned`} />
        <Figure label="Accepted by Kafka (202)" value={count(run.accepted)} hint="stored in Kafka" />
        <Figure label="Rejected" value={count(run.rejected)} hint="refused; never reached Kafka" />
        <Figure label="Aggregated in PostgreSQL" value={count(run.processed)} hint="per the statistics API" />
        <Figure label="Pending" value={count(run.pending)} hint="accepted, not yet aggregated" />
      </dl>
      {lost && <Rejections run={run} tenant={tenant} />}
      <label className="progress">
        Aggregated {count(run.processed)} of {count(run.accepted)} accepted
        <progress max={Math.max(1, run.accepted)} value={run.processed} />
      </label>
      <LineChart
        title="This run only: events over time"
        x={x}
        domainX={[0, Math.ceil(Math.max(x[x.length - 1] ?? 0, run.durationSeconds) / 4) * 4]}
        formatX={(s) => `${Math.round(s)} s`}
        formatY={(v) => count(v)}
        yLabel="Events"
        xLabel="Seconds since the run started"
        markers={sendingEnded == null ? [] : [{ at: sendingEnded, label: 'Sending ended' }]}
        series={[
          ...(lost
            ? [{ name: 'Submitted', tone: 'submitted' as const, values: run.timeline.map((p) => p.submitted) }]
            : []),
          { name: 'Accepted by Kafka', tone: 'accepted', values: run.timeline.map((p) => p.accepted) },
          { name: 'Aggregated in PostgreSQL', tone: 'processed', values: run.timeline.map((p) => p.processed) },
          {
            name: 'Pending (accepted − aggregated)',
            tone: 'pending',
            values: run.timeline.map((p) => Math.max(0, p.accepted - p.processed)),
          },
        ]}
      />
    </div>
  );
}

function Rejections({ run, tenant }: { run: Run; tenant: Tenant | undefined }) {
  const other = run.rejected - run.rejectedByRate - run.rejectedByQuota;
  return (
    <div className="notice">
      <p>
        {count(run.rejected + run.failed)} of {count(run.submitted)} submitted events were not accepted. They are not
        retried and do not count towards completion.
      </p>
      <ul>
        {run.rejectedByRate > 0 && (
          <li>
            {count(run.rejectedByRate)} refused with HTTP 429 because tenant A sent faster than its plan allows
            {tenant && ` (${count(tenant.eventsPerSecond)} events/s, burst ${count(tenant.burst)})`}. A batch is
            admitted or refused as a whole; refused events never reached Kafka.
          </li>
        )}
        {run.rejectedByQuota > 0 && (
          <li>
            {count(run.rejectedByQuota)} refused with HTTP 429 because they exceeded what is left of tenant A's daily
            quota{tenant && ` of ${count(tenant.dailyQuota)} events`}. They never reached Kafka.
          </li>
        )}
        {other > 0 && (
          <li>{count(other)} refused by the ingestion API for another reason; they never reached Kafka.</li>
        )}
        {run.failed > 0 && (
          <li>
            {count(run.failed)} got no acknowledgement: the ingestion API was unreachable or answered with a server
            error.
          </li>
        )}
      </ul>
      {run.lastRejection && <p className="small">Last refusal: {run.lastRejection}</p>}
    </div>
  );
}
