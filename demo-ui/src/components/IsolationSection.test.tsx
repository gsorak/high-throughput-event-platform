import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { IsolationSection } from './IsolationSection';
import { makeSession } from '../test/fixtures';
import type { IsolationReport } from '../types';

const report: IsolationReport = {
  checkedAt: '2030-01-01T12:00:00Z',
  allPassed: true,
  checks: [
    {
      title: 'Tenant A reads its own statistics',
      request: 'GET /v1/stats',
      claims: { tenant_id: 'demo-0123456789abcdef-a' },
      tokenNote: 'Signed by the platform issuer',
      expectedStatus: 200,
      actualStatus: 200,
      answeredForTenant: 'demo-0123456789abcdef-a',
      totalCount: 100,
      passed: true,
    },
    {
      title: "Tenant A's token altered to name tenant B",
      request: 'GET /v1/stats',
      claims: { tenant_id: 'demo-fedcba9876543210-b' },
      tokenNote: 'Claims changed after signing',
      expectedStatus: 401,
      actualStatus: 401,
      answeredForTenant: null,
      totalCount: null,
      passed: true,
    },
  ],
};

describe('IsolationSection', () => {
  it('runs the checks on request', async () => {
    const onCheck = vi.fn();
    render(<IsolationSection session={makeSession()} onCheck={onCheck} pending={false} error={null} />);

    await userEvent.click(screen.getByRole('button', { name: 'Run isolation checks' }));

    expect(onCheck).toHaveBeenCalled();
  });

  it('groups the checks by expected status and shows decoded claims, never a token', () => {
    const { container } = render(
      <IsolationSection session={makeSession({ isolation: report })} onCheck={() => {}} pending={false} error={null} />,
    );

    expect(screen.getByText('All checks passed')).toBeInTheDocument();
    const allowed = screen.getByRole('table', { name: 'Authorized requests — expected HTTP 200' });
    const refused = screen.getByRole('table', { name: 'Unauthorized or tampered requests — expected HTTP 401' });
    const allowedRows = within(allowed).getAllByRole('row');
    const refusedRows = within(refused).getAllByRole('row');
    expect(allowedRows).toHaveLength(2);
    expect(allowedRows[1]).toHaveTextContent('Tenant A reads its own statistics');
    expect(allowedRows[1]).toHaveTextContent('data of Tenant A (100 events)');
    expect(refusedRows).toHaveLength(2);
    expect(refusedRows[1]).toHaveTextContent('tenant_id: demo-fedcba9876543210-b');
    expect(refusedRows[1]).toHaveTextContent('Claims changed after signing');
    expect(refusedRows[1]).toHaveTextContent('401');
    expect(container.textContent).not.toMatch(/eyJ[\w-]+\.[\w-]+\./);
  });
});
