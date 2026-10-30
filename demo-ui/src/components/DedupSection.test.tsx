import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { DedupSection } from './DedupSection';
import { makeSession } from '../test/fixtures';
import type { DedupState } from '../types';

const noop = () => {};

function dedup(overrides: Partial<DedupState>): DedupState {
  return {
    phase: 'DELIVERED',
    batchSize: 100,
    sampleEventIds: ['11111111-1111-4111-8111-111111111111'],
    countBefore: 0,
    countAfterFirst: 100,
    replayStatus: null,
    countAfterReplay: null,
    duplicatesSkipped: null,
    message: 'All 100 events are aggregated.',
    updatedAt: '2030-01-01T12:00:00Z',
    ...overrides,
  };
}

describe('DedupSection', () => {
  it('allows a replay only after the first delivery has been aggregated', async () => {
    const onSend = vi.fn();
    const { rerender } = render(
      <DedupSection session={makeSession()} onSend={onSend} onReplay={noop} pending={false} error={null} />,
    );
    expect(screen.getByRole('button', { name: 'Replay the same 100 events' })).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: 'Send 100 new events' }));
    expect(onSend).toHaveBeenCalled();

    rerender(
      <DedupSection
        session={makeSession({ dedup: dedup({ phase: 'DELIVERING', countAfterFirst: null }) })}
        onSend={onSend}
        onReplay={noop}
        pending={false}
        error={null}
      />,
    );
    expect(screen.getByRole('button', { name: 'Replay the same 100 events' })).toBeDisabled();

    rerender(
      <DedupSection
        session={makeSession({ dedup: dedup({}) })}
        onSend={onSend}
        onReplay={noop}
        pending={false}
        error={null}
      />,
    );
    expect(screen.getByRole('button', { name: 'Replay the same 100 events' })).toBeEnabled();
  });

  it('shows the verified counts from the statistics API', () => {
    const state = dedup({
      phase: 'VERIFIED',
      replayStatus: 202,
      countAfterReplay: 100,
      duplicatesSkipped: 100,
      message: 'The count did not change.',
    });
    render(
      <DedupSection
        session={makeSession({ dedup: state })}
        onSend={noop}
        onReplay={noop}
        pending={false}
        error={null}
      />,
    );

    expect(screen.getByText('Verified')).toBeInTheDocument();
    expect(screen.getByText('Count before replay').nextSibling).toHaveTextContent('100');
    expect(screen.getByText('Count after replay').nextSibling).toHaveTextContent('100');
    expect(screen.getByText('Duplicates detected').nextSibling).toHaveTextContent('100');
    expect(screen.getByText("rise of the processor's counter (whole system)")).toBeInTheDocument();
    expect(screen.getByText('Result').nextSibling).toHaveTextContent('Count unchanged');
    expect(screen.getByText(/HTTP 202 \(accepted by Kafka\)/)).toBeInTheDocument();
  });

  it('reports a changed count as not verified', () => {
    const state = dedup({
      phase: 'FAILED',
      replayStatus: 202,
      countAfterReplay: 200,
      message: 'The aggregate changed from 100 to 200 after the replay.',
    });
    render(
      <DedupSection
        session={makeSession({ dedup: state })}
        onSend={noop}
        onReplay={noop}
        pending={false}
        error={null}
      />,
    );

    expect(screen.getByText('Count after replay').nextSibling).toHaveTextContent('200');
    expect(screen.getByText('Result').nextSibling).toHaveTextContent('Count changed');
    expect(screen.getByText('Duplicates detected').nextSibling).toHaveTextContent('–');
  });
});
