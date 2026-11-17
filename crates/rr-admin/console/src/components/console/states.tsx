import { CircleAlert, Inbox } from "lucide-react";
import { formatNumber, sanitizeError } from "@/lib/format";

export function EmptyState({
  title,
  description,
  coverage,
}: {
  title: string;
  description?: string;
  coverage?: number | null;
}) {
  return (
    <div className="empty-state">
      <Inbox aria-hidden="true" size={24} />
      <h2>{title}</h2>
      {description ? <p>{description}</p> : null}
      {coverage != null ? <p>Observation coverage: {formatNumber(coverage)}%</p> : null}
    </div>
  );
}

export function ErrorState({
  title,
  message,
  correlationId,
  recovery,
}: {
  title: string;
  message: string;
  correlationId?: string | null;
  recovery?: string;
}) {
  return (
    <div className="error-state" role="alert">
      <CircleAlert aria-hidden="true" size={20} />
      <div>
        <h2>{title}</h2>
        <p>{sanitizeError(message)}</p>
        {recovery ? <p>{recovery}</p> : null}
        {correlationId ? <p>Correlation ID: <code>{correlationId}</code></p> : null}
      </div>
    </div>
  );
}
