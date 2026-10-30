import { count } from '../format';
import type { DedupState, SessionView } from '../types';
import { Figure, Section } from './Section';

interface Props {
  session: SessionView;
  onSend: () => void;
  onReplay: () => void;
  pending: boolean;
  error: string | null;
}

export function DedupSection({ session, onSend, onReplay, pending, error }: Props) {
  const state = session.dedup;
  const running = session.busy.includes('dedup');
  const working = state?.phase === 'DELIVERING' || state?.phase === 'REPLAYING';
  const canReplay = state?.countAfterFirst != null && !working;

  return (
    <Section
      id="dedup"
      number={2}
      title="Deduplication"
      scope="session"
      error={error}
      intro={
        <>
          <p>
            Producers retry, so an event can arrive twice. Deliver 100 events for <strong>tenant A</strong>, then send
            exactly the same events again: the aggregated count must not change.
          </p>
          <details className="more">
            <summary>How it is verified</summary>
            <ul>
              <li>The replay is accepted (HTTP 202) like any request; Kafka does not deduplicate.</li>
              <li>
                The processor records each event id in PostgreSQL in the same transaction as the aggregate, and skips
                ids it has already seen.
              </li>
              <li>
                The counts come from the statistics API, read after a marker event sent behind the replay has been
                processed, so the replay has been handled by then.
              </li>
            </ul>
          </details>
        </>
      }
    >
      <div className="actions">
        <button type="button" onClick={onSend} disabled={pending || working}>
          Send 100 new events
        </button>
        <button type="button" onClick={onReplay} disabled={pending || !canReplay}>
          Replay the same 100 events
        </button>
      </div>
      {state && <DedupResult state={state} metricPending={running && state.phase === 'VERIFIED'} />}
    </Section>
  );
}

function verdict(state: DedupState): { text: string; tone: string } {
  if (state.phase === 'VERIFIED') return { text: 'Count unchanged', tone: 'ok' };
  if (state.phase === 'FAILED') {
    return state.countAfterReplay != null
      ? { text: 'Count changed', tone: 'bad' }
      : { text: 'Not verified', tone: 'bad' };
  }
  return { text: state.phase === 'DELIVERED' ? 'Replay not sent yet' : 'Waiting', tone: 'busy' };
}

function DedupResult({ state, metricPending }: { state: DedupState; metricPending: boolean }) {
  const replayed = state.replayStatus != null;
  const result = verdict(state);
  return (
    <div className="result" aria-live="polite">
      <p>
        {state.phase === 'VERIFIED' && <span className="badge badge-ok">Verified</span>}
        {state.phase === 'FAILED' && <span className="badge badge-bad">Not verified</span>}
        {(state.phase === 'DELIVERING' || state.phase === 'REPLAYING') && (
          <span className="badge badge-busy">Waiting</span>
        )}
        {state.phase === 'DELIVERED' && <span className="badge badge-ok">Delivered</span>} {state.message}
      </p>
      <dl className="figures">
        <Figure label="Count before replay" value={count(state.countAfterFirst)} hint="aggregated for tenant A" />
        <Figure
          label="Count after replay"
          value={count(state.countAfterReplay)}
          hint={state.countAfterReplay != null ? 'after the replay was processed' : 'not measured yet'}
        />
        <Figure
          label="Duplicates detected"
          value={count(state.duplicatesSkipped)}
          hint={
            state.duplicatesSkipped != null
              ? "rise of the processor's counter (whole system)"
              : metricPending
                ? 'waiting for Prometheus …'
                : 'processor counter, whole system'
          }
        />
        <Figure label="Result" value={<span className={`badge badge-${result.tone}`}>{result.text}</span>} />
      </dl>
      <details className="more">
        <summary>Step by step</summary>
        <ol className="steps">
          <li>
            First delivery: aggregated count {count(state.countBefore)} →{' '}
            <strong>{count(state.countAfterFirst)}</strong>
          </li>
          <li>
            Replay of the same event ids:{' '}
            {replayed ? `HTTP ${state.replayStatus}${state.replayStatus === 202 ? ' (accepted by Kafka)' : ''}` : '–'}
          </li>
          <li>
            After the replay was processed: aggregated count <strong>{count(state.countAfterReplay)}</strong>
          </li>
        </ol>
        <p className="muted">
          Event ids reused by the replay: <code>{state.sampleEventIds.join(', ')}</code> …
        </p>
      </details>
    </div>
  );
}
