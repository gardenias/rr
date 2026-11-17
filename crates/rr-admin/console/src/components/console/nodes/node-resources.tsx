import { Cpu, HardDrive, MemoryStick, Network } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { formatBytes, formatNumber, formatTimestamp } from "@/lib/format";
import { formatRatio, type FeatureNode, type NodeResourceSample, type ResourceScope } from "./model";

function Scope({ value }: { value?: ResourceScope | null }) {
  return <Badge tone="system">{value ? value.toUpperCase() : "SCOPE UNKNOWN"}</Badge>;
}

function ResourceCard({
  icon,
  title,
  value,
  detail,
  scope,
}: {
  icon: React.ReactNode;
  title: string;
  value: string;
  detail: string;
  scope?: ResourceScope | null;
}) {
  return (
    <article className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 text-(--tx-3)">{icon}<h3>{title}</h3></div>
        <Scope value={scope} />
      </div>
      <p className="mt-4 font-mono text-xl text-(--tx-1)">{value}</p>
      <p className="mt-1 text-xs text-(--tx-3)">{detail}</p>
    </article>
  );
}

export function NodeResources({ node }: { node: FeatureNode }) {
  const resources: NodeResourceSample | null | undefined = node.resources;
  if (!resources) {
    return <CapabilityNotice capability={{ feature: "Resource sampling unsupported", status: "UNSUPPORTED", reason: "This node has not provided a current resource sample. Missing values are not zero." }} />;
  }
  const memoryLimit = resources.memory_limit_bytes ?? resources.memory_total_bytes;
  return (
    <div className="grid gap-4" data-testid="node-resources">
      <p className="text-xs text-(--tx-3)">Last resource sample: {formatTimestamp(resources.sampled_at)}. Scopes describe the source of each measurement.</p>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <ResourceCard
          icon={<Cpu aria-hidden="true" size={16} />}
          title="CPU"
          value={formatRatio(resources.cpu_usage_ratio)}
          detail={`${formatNumber(resources.cpu_cores)} cores · ${formatNumber(resources.cpu_limit_cores)} core limit`}
          scope={resources.cpu_scope ?? resources.scope}
        />
        <ResourceCard
          icon={<MemoryStick aria-hidden="true" size={16} />}
          title="Memory"
          value={`${formatBytes(resources.memory_used_bytes)} / ${formatBytes(resources.memory_total_bytes)}`}
          detail={`Limit ${formatBytes(memoryLimit)}`}
          scope={resources.memory_scope ?? resources.scope}
        />
        <ResourceCard
          icon={<Network aria-hidden="true" size={16} />}
          title="Network"
          value={`↓ ${formatBytes(resources.network_rx_bytes_per_sec)}/s`}
          detail={`↑ ${formatBytes(resources.network_tx_bytes_per_sec)}/s transmit`}
          scope={resources.network_scope ?? resources.scope}
        />
        <ResourceCard
          icon={<HardDrive aria-hidden="true" size={16} />}
          title="Disk"
          value={`${formatBytes(resources.filesystem_used_bytes)} / ${formatBytes(resources.filesystem_total_bytes)}`}
          detail="Node filesystem capacity; no per-group usage is inferred"
          scope={resources.filesystem_scope ?? resources.scope}
        />
      </div>
    </div>
  );
}
