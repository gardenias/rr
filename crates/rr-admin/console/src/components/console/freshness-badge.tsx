import { Clock3, TriangleAlert } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { ConsoleMeta } from "@/lib/contract";
import { formatAge } from "@/lib/format";

export function FreshnessBadge({ meta }: { meta: ConsoleMeta }) {
  const ages = meta.sources
    .map((source) => source.age_ms)
    .filter((age): age is number => age != null && Number.isFinite(age));
  const oldestAge = ages.length ? Math.max(...ages) : null;
  const stale = meta.sources.some((source) => source.status === "STALE") ||
    (oldestAge != null && oldestAge > 30_000);
  return (
    <span className="freshness-group">
      <Badge tone={stale ? "bad" : "neutral"}>
        {stale ? <TriangleAlert aria-hidden="true" size={12} /> : <Clock3 aria-hidden="true" size={12} />}
        {stale ? "Stale" : oldestAge == null ? "Freshness unknown" : "Fresh"} · {formatAge(oldestAge)}
      </Badge>
      {meta.partial ? (
        <Badge tone="accent">
          <TriangleAlert aria-hidden="true" size={12} />
          Partial
        </Badge>
      ) : null}
    </span>
  );
}
