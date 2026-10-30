import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { IngestSection } from './IngestSection';
import { makeRun, makeSession } from '../test/fixtures';

const noop = () => {};

describe('IngestSection', () => {
  it('starts a run with the chosen settings', async () => {
    const onStart = vi.fn();
    render(<IngestSection session={makeSession()} onStart={onStart} onStop={noop} pending={false} error={null} />);

    await userEvent.selectOptions(screen.getByLabelText('Event type'), 'order.placed');
    await userEvent.selectOptions(screen.getByLabelText('Rate'), '500');
    await userEvent.selectOptions(screen.getByLabelText('Duration'), '10');
    await userEvent.click(screen.getByRole('button', { name: 'Send 5,000 events' }));

    expect(onStart).toHaveBeenCalledWith({ type: 'order.placed', eventsPerSecond: 500, durationSeconds: 10 });
  });

  it('does not call a run completed while the backlog is still being processed', () => {
    const run = makeRun({ phase: 'PROCESSING', submitted: 20000, accepted: 20000, processed: 12000, pending: 8000 });
    render(<IngestSection session={makeSession({ run })} onStart={noop} onStop={noop} pending={false} error={null} />);

    expect(screen.getByText('Processing backlog')).toBeInTheDocument();
    expect(screen.queryByText('Completed')).not.toBeInTheDocument();
    expect(screen.getByText('Accepted by Kafka (202)').nextSibling).toHaveTextContent('20,000');
    expect(screen.getByText('Aggregated in PostgreSQL', { selector: 'dt' }).nextSibling).toHaveTextContent('12,000');
    expect(screen.getByText('Pending', { selector: 'dt' }).nextSibling).toHaveTextContent('8,000');
  });

  it('calls a run completed only when every accepted event is aggregated', () => {
    const run = makeRun({ phase: 'COMPLETED', submitted: 20000, accepted: 20000, processed: 20000, pending: 0 });
    render(<IngestSection session={makeSession({ run })} onStart={noop} onStop={noop} pending={false} error={null} />);

    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(screen.getByText(/Every accepted event is aggregated/)).toBeInTheDocument();
  });

  it('offers to stop a run while it is sending', async () => {
    const onStop = vi.fn();
    render(
      <IngestSection
        session={makeSession({ run: makeRun(), systemRun: { busy: true, yours: true, secondsRunning: 5 } })}
        onStart={noop}
        onStop={onStop}
        pending={false}
        error={null}
      />,
    );

    await userEvent.click(screen.getByRole('button', { name: 'Stop sending' }));

    expect(onStop).toHaveBeenCalled();
  });

  it('blocks a start while another visitor is running', () => {
    render(
      <IngestSection
        session={makeSession({ systemRun: { busy: true, yours: false, secondsRunning: 12 } })}
        onStart={noop}
        onStop={noop}
        pending={false}
        error={null}
      />,
    );

    expect(screen.getByRole('button', { name: /Send .* events/ })).toBeDisabled();
    expect(screen.getByText(/Another visitor's run is in progress \(12 s\)/)).toBeInTheDocument();
  });

  it('blocks a run larger than the remaining session budget', () => {
    render(
      <IngestSection
        session={makeSession({ budget: { used: 45000, limit: 50000 } })}
        onStart={noop}
        onStop={noop}
        pending={false}
        error={null}
      />,
    );

    expect(screen.getByRole('button', { name: 'Send 10,000 events' })).toBeDisabled();
    expect(screen.getByText('Your session has 5,000 events left.')).toBeInTheDocument();
  });

  it('defaults to a rate that stays within the plan and marks the plan maximum', () => {
    render(<IngestSection session={makeSession()} onStart={noop} onStop={noop} pending={false} error={null} />);

    expect(screen.getByLabelText('Rate')).toHaveValue('500');
    expect(screen.getByRole('option', { name: '1,000 events/s (plan maximum)' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Send 10,000 events' })).toBeEnabled();
  });

  it('charts this run only, with pending events and the end of sending', () => {
    const run = makeRun({
      phase: 'PROCESSING',
      sendingEndedAt: '2030-01-01T12:00:20Z',
      timeline: [
        { seconds: 0, submitted: 0, accepted: 0, processed: 0 },
        { seconds: 10, submitted: 10000, accepted: 10000, processed: 4000 },
        { seconds: 20, submitted: 20000, accepted: 20000, processed: 9000 },
      ],
    });
    const { container } = render(
      <IngestSection session={makeSession({ run })} onStart={noop} onStop={noop} pending={false} error={null} />,
    );

    expect(screen.getByRole('img', { name: /This run only/ })).toBeInTheDocument();
    const legend = container.querySelector('.legend');
    expect(legend).toHaveTextContent('Accepted by Kafka');
    expect(legend).toHaveTextContent('Aggregated in PostgreSQL');
    expect(legend).toHaveTextContent('Pending (accepted − aggregated)');
    expect(legend).toHaveTextContent('Sending ended');
    expect(legend).not.toHaveTextContent('Submitted');
    expect(legend).not.toHaveTextContent('backlog');
  });

  it('explains rejected events and still completes once every accepted event is aggregated', () => {
    const run = makeRun({
      phase: 'COMPLETED',
      submitted: 10000,
      accepted: 8500,
      rejected: 1500,
      rejectedByRate: 1500,
      processed: 8500,
      pending: 0,
      lastRejection: 'HTTP 429 (rate limit), Retry-After 1 s',
    });
    render(<IngestSection session={makeSession({ run })} onStart={noop} onStop={noop} pending={false} error={null} />);

    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(screen.getByText('Rejected', { selector: 'dt' }).nextSibling).toHaveTextContent('1,500');
    expect(screen.getByText(/1,500 of 10,000 submitted events were not accepted/)).toBeInTheDocument();
    expect(
      screen.getByText(/1,500 refused with HTTP 429 because tenant A sent faster than its plan allows/),
    ).toHaveTextContent('1,000 events/s, burst 2,000');
    expect(screen.getByText(/Last refusal: HTTP 429 \(rate limit\)/)).toBeInTheDocument();
  });
});
