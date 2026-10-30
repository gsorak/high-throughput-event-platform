import type { ReactNode } from 'react';

interface Props {
  id: string;
  number: number;
  title: string;
  scope: 'session' | 'system';
  intro: ReactNode;
  error?: string | null;
  children: ReactNode;
}

/** A numbered demo section with a label saying whose data it shows. */
export function Section({ id, number, title, scope, intro, error, children }: Props) {
  return (
    <section className="card" aria-labelledby={`${id}-title`}>
      <header className="card-header">
        <h2 id={`${id}-title`}>
          <span className="step">{number}</span>
          {title}
        </h2>
        <span className={`scope scope-${scope}`}>{scope === 'session' ? 'Your session' : 'Whole system'}</span>
      </header>
      <div className="intro">{intro}</div>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {children}
    </section>
  );
}

interface FigureProps {
  label: string;
  value: ReactNode;
  hint?: string;
  /** Marks the value with ≈; the note then explains why it may differ from the real number. */
  estimate?: boolean;
  note?: ReactNode;
}

export function Figure({ label, value, hint, estimate = false, note }: FigureProps) {
  const known = value !== '–';
  return (
    <div className="figure">
      <dt>{label}</dt>
      <dd>
        {estimate && known && (
          <abbr className="approx" title="Estimate">
            ≈{' '}
          </abbr>
        )}
        {value}
      </dd>
      {hint && <dd className="hint">{hint}</dd>}
      {note && (
        <dd className="note">
          <details className="more">
            <summary>{estimate ? 'Estimate: why?' : 'How it is counted'}</summary>
            {note}
          </details>
        </dd>
      )}
    </div>
  );
}
