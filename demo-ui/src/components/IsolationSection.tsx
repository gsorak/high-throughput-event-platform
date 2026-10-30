import { count } from '../format';
import type { IsolationCheck, SessionView } from '../types';
import { Section } from './Section';

interface Props {
  session: SessionView;
  onCheck: () => void;
  pending: boolean;
  error: string | null;
}

export function IsolationSection({ session, onCheck, pending, error }: Props) {
  const report = session.isolation;
  const tenantName = (id: string | null) => session.tenants.find((t) => t.id === id)?.name ?? id ?? '–';
  const authorized = report?.checks.filter((c) => c.expectedStatus === 200) ?? [];
  const refused = report?.checks.filter((c) => c.expectedStatus !== 200) ?? [];

  return (
    <Section
      id="isolation"
      number={3}
      title="Tenant isolation"
      scope="session"
      error={error}
      intro={
        <>
          <p>
            The platform takes the tenant only from a signed JWT, never from the request. These checks send real
            requests with valid, altered and foreign-signed tokens.
          </p>
          <details className="more">
            <summary>About the tokens</summary>
            <ul>
              <li>
                The APIs check the signature against the platform issuer's key, then the issuer, audience and expiry. A
                token that fails any of these is refused with HTTP 401 before any data is read.
              </li>
              <li>
                Tokens are created and kept inside the demo gateway. Your browser holds only a session cookie, so you
                see the decoded claims, never a token.
              </li>
            </ul>
          </details>
        </>
      }
    >
      <div className="actions">
        <button type="button" onClick={onCheck} disabled={pending}>
          Run isolation checks
        </button>
      </div>
      {report && (
        <div className="result" aria-live="polite">
          <p>
            {report.allPassed ? (
              <span className="badge badge-ok">All checks passed</span>
            ) : (
              <span className="badge badge-bad">A check failed</span>
            )}
          </p>
          <CheckGroup title="Authorized requests — expected HTTP 200" checks={authorized} tenantName={tenantName} />
          <CheckGroup
            title="Unauthorized or tampered requests — expected HTTP 401"
            checks={refused}
            tenantName={tenantName}
          />
        </div>
      )}
    </Section>
  );
}

function CheckGroup({
  title,
  checks,
  tenantName,
}: {
  title: string;
  checks: IsolationCheck[];
  tenantName: (id: string | null) => string;
}) {
  if (checks.length === 0) return null;
  return (
    <div className="table-wrap group">
      <table>
        <caption>{title}</caption>
        <colgroup>
          <col className="col-attempt" />
          <col />
          <col className="col-status" />
          <col className="col-actual" />
          <col className="col-status" />
        </colgroup>
        <thead>
          <tr>
            <th scope="col">Attempt</th>
            <th scope="col">Token</th>
            <th scope="col">Expected</th>
            <th scope="col">Actual</th>
            <th scope="col">Result</th>
          </tr>
        </thead>
        <tbody>
          {checks.map((check) => (
            <tr key={check.title}>
              <td>
                {check.title}
                <div className="muted">
                  <code>{check.request}</code>
                </div>
              </td>
              <td>
                <code>tenant_id: {String(check.claims['tenant_id'] ?? '–')}</code>
                <div className="muted">{check.tokenNote}</div>
                <details className="claims">
                  <summary>All claims</summary>
                  <pre>{JSON.stringify(check.claims, null, 2)}</pre>
                </details>
              </td>
              <td>{check.expectedStatus}</td>
              <td>
                {check.actualStatus === 0 ? 'unreachable' : check.actualStatus}
                {check.answeredForTenant && (
                  <div className="muted">
                    data of {tenantName(check.answeredForTenant)} ({count(check.totalCount)} events)
                  </div>
                )}
              </td>
              <td>
                {check.passed ? (
                  <span className="badge badge-ok">Pass</span>
                ) : (
                  <span className="badge badge-bad">Fail</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
