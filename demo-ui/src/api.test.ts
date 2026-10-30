import { ApiError, api } from './api';
import { jsonResponse, makeSession } from './test/fixtures';

describe('api', () => {
  it('sends the client header and the cookie, and posts JSON bodies', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(jsonResponse(202, makeSession()));

    await api.startRun({ type: 'page.view', eventsPerSecond: 100, durationSeconds: 5 });

    const [path, init] = fetchMock.mock.calls[0];
    expect(path).toBe('/demo/runs');
    expect(init?.method).toBe('POST');
    expect(init?.credentials).toBe('same-origin');
    expect(init?.headers).toMatchObject({ 'X-Demo-Client': '1', 'Content-Type': 'application/json' });
    expect(JSON.parse(String(init?.body))).toEqual({ type: 'page.view', eventsPerSecond: 100, durationSeconds: 5 });
  });

  it('turns a problem response into an ApiError carrying the detail', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      jsonResponse(409, { title: 'Busy', detail: 'Another run is in progress.' }),
    );

    const error = await api.startRun({ type: 'page.view', eventsPerSecond: 100, durationSeconds: 5 }).catch((e) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect(error.status).toBe(409);
    expect(error.message).toBe('Another run is in progress.');
  });

  it('reports an unreachable gateway', async () => {
    vi.spyOn(globalThis, 'fetch').mockRejectedValue(new TypeError('Failed to fetch'));

    const error = await api.getSession().catch((e) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect(error.status).toBe(0);
  });

  it('accepts an empty 204 answer', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(null, { status: 204 }));

    await expect(api.endSession()).resolves.toBeUndefined();
  });
});
