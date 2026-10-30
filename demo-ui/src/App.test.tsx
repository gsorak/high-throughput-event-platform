import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App } from './App';
import { jsonResponse, makeSession } from './test/fixtures';

type Handler = (method: string, path: string) => Response | undefined;

function mockGateway(handler: Handler) {
  return vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
    const method = init?.method ?? 'GET';
    const path = String(input);
    const answer = handler(method, path);
    if (answer) return answer;
    if (path.startsWith('/demo/overview')) {
      return jsonResponse(200, { available: false, window: '5m', stepSeconds: 5, series: [], grafanaUrl: null });
    }
    return jsonResponse(404, { detail: `unexpected ${method} ${path}` });
  });
}

const unauthorized = () => jsonResponse(401, { title: 'No session', detail: 'Start a demo session first.' });

describe('App', () => {
  it('offers to start a session when there is none, then shows all five sections', async () => {
    const fetchMock = mockGateway((method, path) => {
      if (path === '/demo/session' && method === 'GET') return unauthorized();
      if (path === '/demo/session' && method === 'POST') return jsonResponse(201, makeSession());
      return undefined;
    });
    render(<App />);

    await userEvent.click(await screen.findByRole('button', { name: 'Start demo session' }));

    expect(await screen.findByText('demo-0123456789abcdef-a')).toBeInTheDocument();
    for (const title of [
      'Ingest and process',
      'Deduplication',
      'Tenant isolation',
      'Rate limits and quotas',
      'System overview',
    ]) {
      expect(screen.getByRole('heading', { name: new RegExp(title) })).toBeInTheDocument();
    }
    expect(fetchMock).toHaveBeenCalledWith('/demo/session', expect.objectContaining({ method: 'POST' }));
  });

  it('restores an existing session after a reload without starting a new one', async () => {
    const fetchMock = mockGateway((method, path) =>
      path === '/demo/session' && method === 'GET' ? jsonResponse(200, makeSession()) : undefined,
    );
    render(<App />);

    expect(await screen.findByText('demo-fedcba9876543210-b')).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalledWith('/demo/session', expect.objectContaining({ method: 'POST' }));
  });

  it('returns to the start screen when the session has expired', async () => {
    mockGateway((method, path) => {
      if (path === '/demo/session' && method === 'GET') return jsonResponse(200, makeSession());
      if (path === '/demo/isolation/check') return unauthorized();
      return undefined;
    });
    render(<App />);

    await userEvent.click(await screen.findByRole('button', { name: 'Run isolation checks' }));

    expect(await screen.findByText(/ended or expired/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start demo session' })).toBeInTheDocument();
  });

  it('explains when the demo is at capacity', async () => {
    mockGateway((method, path) => {
      if (path === '/demo/session' && method === 'GET') return unauthorized();
      if (path === '/demo/session' && method === 'POST') {
        return jsonResponse(503, { title: 'Demo is full', detail: 'All 20 demo sessions are in use.' });
      }
      return undefined;
    });
    render(<App />);

    await userEvent.click(await screen.findByRole('button', { name: 'Start demo session' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('All 20 demo sessions are in use.');
  });

  it('shows an action error in its own section and keeps the session', async () => {
    mockGateway((method, path) => {
      if (path === '/demo/session' && method === 'GET') return jsonResponse(200, makeSession());
      if (path === '/demo/runs') {
        return jsonResponse(409, { title: 'Busy', detail: "Another visitor's run is in progress." });
      }
      return undefined;
    });
    render(<App />);

    await userEvent.click(await screen.findByRole('button', { name: /Send 10,000 events/ }));

    expect(await screen.findByRole('alert')).toHaveTextContent("Another visitor's run is in progress.");
    expect(screen.getByText('demo-0123456789abcdef-a')).toBeInTheDocument();
  });

  it('ends the session on request', async () => {
    mockGateway((method, path) => {
      if (path === '/demo/session' && method === 'GET') return jsonResponse(200, makeSession());
      if (path === '/demo/session' && method === 'DELETE') return new Response(null, { status: 204 });
      return undefined;
    });
    render(<App />);

    await userEvent.click(await screen.findByRole('button', { name: 'End session' }));

    expect(await screen.findByText(/Session ended/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Start demo session' })).toBeInTheDocument();
  });
});
