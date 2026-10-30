export type Tone = 'accepted' | 'processed' | 'backlog' | 'pending' | 'submitted';

export interface Series {
  name: string;
  tone: Tone;
  values: (number | null)[];
}

/** A vertical line at an x value, e.g. the end of sending. */
export interface Marker {
  at: number;
  label: string;
}

interface Props {
  title: string;
  x: number[];
  series: Series[];
  formatX: (value: number) => string;
  formatY: (value: number) => string;
  /** Unit of the y axis, shown above it. */
  yLabel: string;
  /** Meaning of the x axis, shown below it. */
  xLabel: string;
  markers?: Marker[];
  /** Fixed x range; defaults to the first and last x. */
  domainX?: [number, number];
}

const WIDTH = 600;
const HEIGHT = 230;
const PAD = { left: 60, right: 14, top: 24, bottom: 40 };
const X_TICKS = 5;

/** The smallest of 1, 2, 2.5, 5 × 10^n at or above {@code value}, so axis labels are round numbers. */
export function niceCeiling(value: number): number {
  if (value <= 1) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const step of [1, 2, 2.5, 5, 10]) {
    if (step * magnitude >= value) return step * magnitude;
  }
  return 10 * magnitude;
}

/** A minimal SVG line chart; null values leave gaps, and an isolated value is drawn as a dot. */
export function LineChart({ title, x, series, formatX, formatY, yLabel, xLabel, markers = [], domainX }: Props) {
  const values = series.flatMap((s) => s.values).filter((v): v is number => v != null);
  if (x.length < 2 || values.length === 0) {
    return <p className="muted chart-empty">Not enough data for a chart yet.</p>;
  }
  const maxY = niceCeiling(Math.max(...values));
  const [minX, maxX] = domainX ?? [x[0], x[x.length - 1]];
  const spanX = Math.max(1e-9, maxX - minX);
  const plotW = WIDTH - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const px = (v: number) => PAD.left + ((v - minX) / spanX) * plotW;
  const py = (v: number) => PAD.top + plotH - (v / maxY) * plotH;

  const segments = (vals: (number | null)[]) => {
    const result: [number, number][][] = [];
    let current: [number, number][] = [];
    vals.forEach((v, i) => {
      if (v == null) {
        if (current.length > 0) result.push(current);
        current = [];
      } else {
        current.push([px(x[i]), py(v)]);
      }
    });
    if (current.length > 0) result.push(current);
    return result;
  };

  return (
    <figure className="chart">
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={`${title}. ${yLabel} over ${xLabel}.`}>
        <text className="axis-label" x={PAD.left - 6} y={PAD.top - 10} textAnchor="end">
          {yLabel}
        </text>
        {[0, 0.25, 0.5, 0.75, 1].map((f) => (
          <g key={f}>
            <line className="grid" x1={PAD.left} x2={WIDTH - PAD.right} y1={py(maxY * f)} y2={py(maxY * f)} />
            <text className="axis" x={PAD.left - 6} y={py(maxY * f) + 4} textAnchor="end">
              {formatY(maxY * f)}
            </text>
          </g>
        ))}
        {Array.from({ length: X_TICKS }, (_, i) => minX + (spanX * i) / (X_TICKS - 1)).map((v, i) => (
          <text
            key={i}
            className="axis"
            x={px(v)}
            y={HEIGHT - PAD.bottom + 16}
            textAnchor={i === 0 ? 'start' : i === X_TICKS - 1 ? 'end' : 'middle'}
          >
            {formatX(v)}
          </text>
        ))}
        <text className="axis-label" x={PAD.left + plotW / 2} y={HEIGHT - 6} textAnchor="middle">
          {xLabel}
        </text>
        {markers
          .filter((m) => m.at >= minX && m.at <= maxX)
          .map((m) => (
            <g key={m.label}>
              <line className="marker" x1={px(m.at)} x2={px(m.at)} y1={PAD.top} y2={PAD.top + plotH} />
              <text
                className="marker-label"
                x={px(m.at) + (px(m.at) > WIDTH - 140 ? -4 : 4)}
                y={PAD.top + 10}
                textAnchor={px(m.at) > WIDTH - 140 ? 'end' : 'start'}
              >
                {m.label}
              </text>
            </g>
          ))}
        {series.map((s) =>
          segments(s.values).map((points, i) =>
            points.length > 1 ? (
              <polyline
                key={`${s.name}-${i}`}
                className={`line tone-${s.tone}`}
                points={points.map(([a, b]) => `${a.toFixed(1)},${b.toFixed(1)}`).join(' ')}
              />
            ) : (
              <circle
                key={`${s.name}-${i}`}
                className={`dot tone-${s.tone}`}
                cx={points[0][0]}
                cy={points[0][1]}
                r={3}
              />
            ),
          ),
        )}
      </svg>
      <figcaption>
        <div className="chart-title">{title}</div>
        <ul className="legend">
          {series.map((s) => (
            <li key={s.name}>
              <span className={`swatch tone-${s.tone}`} aria-hidden="true" />
              {s.name}
            </li>
          ))}
          {markers.length > 0 && (
            <li>
              <span className="swatch swatch-marker" aria-hidden="true" />
              {markers.map((m) => m.label).join(', ')}
            </li>
          )}
        </ul>
      </figcaption>
    </figure>
  );
}
