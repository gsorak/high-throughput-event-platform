import { useEffect, useState } from 'react';
import { ApiError, api } from '../api';
import { count, rate } from '../format';
import type { Overview, OverviewWindow } from '../types';
import { LineChart } from './LineChart';
import { Figure, Section } from './Section';

const REFRESH_MS = 5000;

const RATE_NOTE = (
  <p>
    Prometheus samples the counters every 5 s and estimates a rate from the samples in the period, scaled to the full
    period. It is an average, not the instantaneous rate, and can be off by a few percent; a burst shorter than the
    period is spread out and looks lower.
  </p>
);

const TOTAL_NOTE = (
  <p>
    The exact number of increments of the services' counters, summed from the 5 s samples. Only the window edges are
    rounded to a sample, so an event in the first or last 5 s can fall into the neighbouring window. These are
    whole-system metrics; your own exact counts come from the statistics API in sections 1–4.
  </p>
);

const HTTP_NOTE = (
  <p>
    Prometheus' <code>increase()</code> extrapolates the change between the first and last sample in the window to the
    whole window, so the figure can be a few percent higher than the real number of responses (for example 590 shown
    for 580). It is rounded to a whole number. A counter that restarts and is created again in the same window can be
    undercounted.
  </p>
);

const BACKLOG_NOTE = (
  <p>
    The newest Kafka offset minus the processor's last committed offset, as reported by kafka-exporter at the last 5 s
    sample. The processor commits after each batch, so events it has already aggregated can still count for a moment,
    and the value can be up to one sample old.
  </p>
);

const WINDOWS: { key: OverviewWindow; label: string; seconds: number }[] = [
  { key: '5m', label: 'Last 5 min', seconds: 300 },
  { key: '15m', label: 'Last 15 min', seconds: 900 },
];

interface Props {
  onSessionLost: () => void;
}

/** Platform-wide metrics from Prometheus, read through the gateway. */
export function OverviewSection({ onSessionLost }: Props) {
  const [window, setWindow] = useState<OverviewWindow>('5m');
  const [overview, setOverview] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const next = await api.overview(window);
        if (active) {
          setOverview(next);
          setError(null);
        }
      } catch (e) {
        if (!active) return;
        if (e instanceof ApiError && e.status === 401) {
          onSessionLost();
        } else {
          setError(e instanceof Error ? e.message : 'The overview could not be loaded.');
        }
      }
    };
    void load();
    const timer = setInterval(load, REFRESH_MS);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [onSessionLost, window]);

  const shown = WINDOWS.find((w) => w.key === (overview?.window ?? window)) ?? WINDOWS[0];
  const end = overview ? Date.parse(overview.at) / 1000 : 0;
  const domain: [number, number] = [end - shown.seconds, end];
  const x = overview?.series.map((p) => p.epochSecond) ?? [];
  const clock = (epochSecond: number) =>
    new Date(epochSecond * 1000).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit',
      second: shown.seconds <= 300 ? '2-digit' : undefined,
    });

  return (
    <Section
      id="overview"
      number={5}
      title="System overview"
      scope="system"
      error={error}
      intro={
        <>
          <p>Live metrics of the whole platform from Prometheus, including other visitors' traffic.</p>
          <details className="more">
            <summary>How to read these numbers</summary>
            <ul>
              <li>
                The platform has no per-tenant metrics, so sections 1–4 use your tenants' statistics instead of these.
              </li>
              <li>
                <strong>Now</strong> is the average of the last 30 s, so the rates fall to 0/s soon after traffic stops;
                the backlog is the level now.
              </li>
              <li>
                <strong>The window buttons</strong> set the totals, peaks, counters and charts below them. A total counts
                events during the window; a peak is the highest 15 s average in it (Prometheus samples every 5 s, so a
                burst of a few seconds shows as a short, flattened peak).
              </li>
              <li>
                Events processed excludes duplicates (counted separately), so ingested minus processed is not the
                backlog.
              </li>
              <li>
                Figures marked <strong>≈</strong> and the chart lines are <strong>estimates</strong> from sampled
                metrics and can differ slightly from the real values; open <em>Estimate: why?</em> under a figure for
                the reason. The totals are exact counts of the metrics, apart from the 5 s window edges.
              </li>
            </ul>
          </details>
        </>
      }
    >
      {overview == null && !error && <p className="muted">Loading metrics …</p>}
      {overview != null && !overview.available && (
        <p className="muted">Metrics are unavailable right now (Prometheus did not answer).</p>
      )}
      {overview?.available && (
        <>
          <h3 className="subhead">Now, whole system</h3>
          <dl className="figures">
            <Figure
              label="Ingest rate"
              value={rate(overview.ingestRate)}
              hint="accepted by Kafka, last 30 s"
              estimate
              note={RATE_NOTE}
            />
            <Figure
              label="Processing rate"
              value={rate(overview.processRate)}
              hint="aggregated, last 30 s"
              estimate
              note={RATE_NOTE}
            />
            <Figure
              label="Kafka backlog"
              value={count(overview.backlog)}
              hint="events not yet processed"
              estimate
              note={BACKLOG_NOTE}
            />
          </dl>
          <div className="segmented" role="group" aria-label="Time window for the figures and charts below">
            {WINDOWS.map((w) => (
              <button
                key={w.key}
                type="button"
                className={w.key === window ? undefined : 'secondary'}
                aria-pressed={w.key === window}
                onClick={() => setWindow(w.key)}
              >
                {w.label}
              </button>
            ))}
          </div>
          <h3 className="subhead">{shown.label}, whole system</h3>
          <dl className="figures">
            <Figure
              label="Events ingested"
              value={count(overview.ingestedTotal)}
              hint="accepted by Kafka"
              note={TOTAL_NOTE}
            />
            <Figure
              label="Events processed"
              value={count(overview.processedTotal)}
              hint="new events aggregated"
              note={TOTAL_NOTE}
            />
            <Figure
              label="Peak ingest rate"
              value={rate(overview.ingestPeakRate)}
              hint="highest 15 s average"
              estimate
              note={RATE_NOTE}
            />
            <Figure
              label="Peak processing rate"
              value={rate(overview.processPeakRate)}
              hint="highest 15 s average"
              estimate
              note={RATE_NOTE}
            />
          </dl>
          <dl className="figures">
            <Figure label="Duplicates skipped" value={count(overview.duplicatesSkipped)} note={TOTAL_NOTE} />
            <Figure
              label="Requests rejected (429)"
              value={count(overview.rejectedRequests)}
              estimate
              note={HTTP_NOTE}
            />
            <Figure label="Server errors (5xx)" value={count(overview.serverErrors)} estimate note={HTTP_NOTE} />
            <Figure
              label="Dead-lettered / failed deliveries"
              value={`${count(overview.deadLettered)} / ${count(overview.deliveryFailures)}`}
              note={TOTAL_NOTE}
            />
          </dl>
          <div className="chart-pair">
            <LineChart
              title={`Whole system: ingest and processing rate, ${shown.label.toLowerCase()}`}
              x={x}
              domainX={domain}
              formatX={clock}
              formatY={(v) => count(v)}
              yLabel="Events/s"
              xLabel="Local time"
              series={[
                { name: 'Ingest rate (accepted)', tone: 'accepted', values: overview.series.map((p) => p.ingestRate) },
                {
                  name: 'Processing rate (aggregated)',
                  tone: 'processed',
                  values: overview.series.map((p) => p.processRate),
                },
              ]}
            />
            <LineChart
              title={`Whole system: Kafka backlog, ${shown.label.toLowerCase()}`}
              x={x}
              domainX={domain}
              formatX={clock}
              formatY={(v) => count(v)}
              yLabel="Events"
              xLabel="Local time"
              series={[{ name: 'Kafka backlog', tone: 'backlog', values: overview.series.map((p) => p.backlog) }]}
            />
          </div>
          {overview.grafanaUrl && (
            <p>
              <a href={overview.grafanaUrl} target="_blank" rel="noreferrer">
                Open the Grafana dashboard
              </a>{' '}
              <span className="muted">(local development only)</span>
            </p>
          )}
        </>
      )}
    </Section>
  );
}
