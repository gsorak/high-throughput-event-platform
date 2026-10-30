import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { LimitsSection } from './LimitsSection';
import { makeSession } from '../test/fixtures';
import type { LimitAttempt } from '../types';

const noop = () => {};

function attempt(overrides: Partial<LimitAttempt>): LimitAttempt {
  return {
    at: '2030-01-01T12:00:00Z',
    step: 'within',
    events: 50,
    status: 202,
    accepted: 50,
    limit: null,
    retryAfterSeconds: null,
    detail: null,
    quotaRemainingBefore: 1000,
    ...overrides,
  };
}

describe('LimitsSection', () => {
  it('runs the chosen step', async () => {
    const onStep = vi.fn();
    render(<LimitsSection session={makeSession()} onStep={onStep} pending={null} error={null} />);

    await userEvent.click(screen.getByRole('button', { name: 'Send a burst (2 × 500)' }));

    expect(onStep).toHaveBeenCalledWith('burst');
  });

  it('disables all steps while one is running', () => {
    render(<LimitsSection session={makeSession()} onStep={noop} pending="quota" error={null} />);

    for (const button of screen.getAllByRole('button')) {
      expect(button).toBeDisabled();
    }
  });

  it('lists the real rate and quota rejections', () => {
    const session = makeSession({
      rateLimits: {
        attempts: [
          attempt({
            step: 'quota',
            events: 1,
            status: 429,
            accepted: 0,
            limit: 'quota',
            retryAfterSeconds: 40000,
            quotaRemainingBefore: 0,
          }),
          attempt({ step: 'burst', events: 500, status: 429, accepted: 0, limit: 'rate', retryAfterSeconds: 5 }),
          attempt({}),
        ],
        acceptedEvents: 1000,
        quotaRemaining: 0,
        quotaExhausted: true,
        quotaResetsAt: '2030-01-02T00:00:00Z',
      },
    });
    render(<LimitsSection session={session} onStep={noop} pending={null} error={null} />);

    const rows = within(screen.getByRole('table')).getAllByRole('row');
    expect(rows[1]).toHaveTextContent('429 daily quota used up');
    expect(rows[2]).toHaveTextContent('429 rate limit, retry after 5 s');
    expect(rows[3]).toHaveTextContent('202 accepted 50');
  });

  it('explains that an exhausted quota stays exhausted', () => {
    const session = makeSession({
      rateLimits: {
        attempts: [],
        acceptedEvents: 1000,
        quotaRemaining: 0,
        quotaExhausted: true,
        quotaResetsAt: '2030-01-02T00:00:00Z',
      },
    });
    render(<LimitsSection session={session} onStep={noop} pending={null} error={null} />);

    const status = screen.getByRole('status');
    expect(status).toHaveTextContent('daily quota is used up');
    expect(status).toHaveTextContent('00:00 UTC');
    expect(status).toHaveTextContent('Nothing resets it early; start a new session');
    expect(screen.getByText(/1,000 used, 0 remaining of 1,000/)).toBeInTheDocument();
  });

  it('tells a batch larger than the remaining quota apart from an exhausted quota', () => {
    const session = makeSession({
      rateLimits: {
        attempts: [
          attempt({
            step: 'burst',
            events: 500,
            status: 429,
            accepted: 0,
            limit: 'quota',
            retryAfterSeconds: 40000,
            quotaRemainingBefore: 450,
          }),
          attempt({ step: 'burst', events: 500, status: 202, accepted: 500, quotaRemainingBefore: 950 }),
        ],
        acceptedEvents: 550,
        quotaRemaining: 450,
        quotaExhausted: false,
        quotaResetsAt: '2030-01-02T00:00:00Z',
      },
    });
    render(<LimitsSection session={session} onStep={noop} pending={null} error={null} />);

    const status = screen.getByRole('status');
    expect(status).toHaveTextContent('although 450 events of quota remain');
    expect(status).toHaveTextContent('admitted as a whole');
    expect(status).not.toHaveTextContent('used up');
    expect(screen.getByText(/550 used, 450 remaining of 1,000/)).toBeInTheDocument();
    const rows = within(screen.getByRole('table')).getAllByRole('row');
    expect(rows[1]).toHaveTextContent('429 batch exceeds remaining quota (450 left)');
  });
});
