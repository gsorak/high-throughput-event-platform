import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { api } from '../api';
import type { Overview, OverviewWindow } from '../types';
import { OverviewSection } from './OverviewSection';

function overview(window: OverviewWindow, changes: Partial<Overview> = {}): Overview {
  const at = 1_900_000_000;
  return {
    at: new Date(at * 1000).toISOString(),
    available: true,
    ingestRate: 0,
    processRate: 0,
    backlog: 12000,
    ingestedTotal: window === '5m' ? 40000 : 90000,
    processedTotal: window === '5m' ? 28000 : 78000,
    ingestPeakRate: 950,
    processPeakRate: 420,
    duplicatesSkipped: 100,
    rejectedRequests: 3,
    serverErrors: 0,
    deadLettered: 0,
    deliveryFailures: 0,
    window,
    stepSeconds: 5,
    series: [
      { epochSecond: at - 10, ingestRate: 0, processRate: 0, backlog: 0 },
      { epochSecond: at - 5, ingestRate: 950, processRate: 400, backlog: 12000 },
      { epochSecond: at, ingestRate: 0, processRate: 600, backlog: 9000 },
    ],
    grafanaUrl: 'http://localhost:3000/d/platform',
    ...changes,
  };
}

function figure(label: string): HTMLElement {
  return screen.getByText(label, { selector: 'dt' }).nextSibling as HTMLElement;
}

describe('OverviewSection', () => {
  it('labels every figure and chart as system-wide and switches the chart window', async () => {
    const load = vi.spyOn(api, 'overview').mockImplementation(async (window) => overview(window));
    render(<OverviewSection onSessionLost={() => { }} />);

    expect(await screen.findByText('Now, whole system')).toBeInTheDocument();
    expect(load).toHaveBeenCalledWith('5m');
    expect(figure('Kafka backlog')).toHaveTextContent('12,000');
    expect(screen.getByText('Last 5 min, whole system')).toBeInTheDocument();
    expect(
      screen.getByRole('img', { name: /Whole system: ingest and processing rate, last 5 min/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole('img', { name: /Whole system: Kafka backlog/ })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open the Grafana dashboard' })).toBeInTheDocument();

    await userEvent.click(screen.getByRole('button', { name: 'Last 15 min' }));

    expect(load).toHaveBeenCalledWith('15m');
    expect(await screen.findByRole('img', { name: /rate, last 15 min/ })).toBeInTheDocument();
    expect(screen.getByText('Last 15 min, whole system')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Last 15 min' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('keeps the current rate, the window totals and the window peaks apart', async () => {
    vi.spyOn(api, 'overview').mockImplementation(async (window) => overview(window));
    render(<OverviewSection onSessionLost={() => { }} />);

    await screen.findByText('Now, whole system');
    expect(figure('Ingest rate')).toHaveTextContent('0/s');
    expect(figure('Events ingested')).toHaveTextContent('40,000');
    expect(figure('Events processed')).toHaveTextContent('28,000');
    expect(figure('Peak ingest rate')).toHaveTextContent('950/s');
    expect(figure('Peak processing rate')).toHaveTextContent('420/s');

    await userEvent.click(screen.getByRole('button', { name: 'Last 15 min' }));

    await screen.findByText('Last 15 min, whole system');
    expect(figure('Events ingested')).toHaveTextContent('90,000');
    expect(figure('Events processed')).toHaveTextContent('78,000');
    expect(figure('Ingest rate')).toHaveTextContent('0/s');
  });

  it('marks the estimated figures with ≈ and explains them, the exact totals without ≈', async () => {
    vi.spyOn(api, 'overview').mockImplementation(async (window) => overview(window));
    render(<OverviewSection onSessionLost={() => { }} />);

    await screen.findByText('Now, whole system');
    for (const label of ['Ingest rate', 'Processing rate', 'Kafka backlog', 'Peak ingest rate', 'Requests rejected (429)', 'Server errors (5xx)']) {
      expect(figure(label)).toHaveTextContent(/^≈/);
    }
    for (const label of ['Events ingested', 'Events processed', 'Duplicates skipped']) {
      expect(figure(label)).not.toHaveTextContent('≈');
    }
    expect(screen.getAllByText('Estimate: why?', { selector: 'summary' })).toHaveLength(7);
    expect(screen.getAllByText('How it is counted', { selector: 'summary' })).toHaveLength(4);

    const why = screen.getAllByText('Estimate: why?', { selector: 'summary' })[0];
    await userEvent.click(why);
    expect(why.closest('details')).toHaveAttribute('open');
  });

  it('shows a missing metric as a dash, not as zero', async () => {
    vi.spyOn(api, 'overview').mockImplementation(async (window) =>
      overview(window, { ingestedTotal: null, processedTotal: null, ingestPeakRate: null, duplicatesSkipped: null }),
    );
    render(<OverviewSection onSessionLost={() => { }} />);

    await screen.findByText('Now, whole system');
    expect(figure('Events ingested')).toHaveTextContent('–');
    expect(figure('Events processed')).toHaveTextContent('–');
    expect(figure('Peak ingest rate')).toHaveTextContent('–');
    expect(figure('Peak ingest rate')).not.toHaveTextContent('≈');
    expect(figure('Duplicates skipped')).toHaveTextContent('–');
    expect(figure('Server errors (5xx)')).toHaveTextContent('0');
  });
});
