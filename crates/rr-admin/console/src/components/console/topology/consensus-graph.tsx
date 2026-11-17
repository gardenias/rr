"use client";

import { useId, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import Link from "next/link";
import { Button } from "@/components/ui/button";

export interface ConsensusGraphNode {
  id: string;
  label: string;
  sublabel: string;
  description: string;
  href?: string;
  x?: number;
  y?: number;
}

export interface ConsensusGraphEdge {
  id: string;
  source: string;
  target: string;
  label: string;
  description: string;
}

export function ConsensusGraph({
  title,
  description,
  nodes,
  edges,
  width = 960,
  height = 480,
}: {
  title: string;
  description: string;
  nodes: ConsensusGraphNode[];
  edges: ConsensusGraphEdge[];
  width?: number;
  height?: number;
}) {
  const markerId = `arrow-${useId().replaceAll(":", "")}`;
  const descriptionId = `${markerId}-description`;
  const [scale, setScale] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const drag = useRef<{ pointerId: number; x: number; y: number; panX: number; panY: number } | null>(null);
  const laidOutNodes = nodes.map((node, index) => {
    if (node.x != null && node.y != null) return node;
    const angle = nodes.length <= 1 ? 0 : index / (nodes.length - 1) * Math.PI;
    return { ...node, x: 140 + index * Math.min(180, (width - 280) / Math.max(1, nodes.length - 1)), y: height / 2 + Math.sin(angle) * 100 };
  });
  const positions = new Map(laidOutNodes.map((node) => [node.id, node]));
  const visibleEdges = edges.filter((edge) => positions.has(edge.source) && positions.has(edge.target));
  const selectedNode = laidOutNodes.find((node) => node.id === selectedNodeId);
  const selectedEdge = visibleEdges.find((edge) => edge.id === selectedEdgeId);
  const activate = (event: KeyboardEvent<SVGGElement>, action: () => void) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    action();
  };
  const onPointerDown = (event: PointerEvent<SVGSVGElement>) => {
    if (event.target !== event.currentTarget) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    drag.current = { pointerId: event.pointerId, x: event.clientX, y: event.clientY, panX: pan.x, panY: pan.y };
  };
  const onPointerMove = (event: PointerEvent<SVGSVGElement>) => {
    if (!drag.current || drag.current.pointerId !== event.pointerId) return;
    setPan({ x: drag.current.panX + event.clientX - drag.current.x, y: drag.current.panY + event.clientY - drag.current.y });
  };
  const stopDragging = () => { drag.current = null; };

  return (
    <div className="grid gap-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p id={descriptionId} className="text-xs text-zinc-400">{description}</p>
        <div className="flex gap-2" aria-label={`${title} controls`}>
          <Button variant="secondary" size="small" aria-label="Zoom out" onClick={() => setScale((value) => Math.max(0.5, value - 0.2))}>−</Button>
          <Button variant="secondary" size="small" aria-label="Fit topology" onClick={() => { setScale(1); setPan({ x: 0, y: 0 }); }}>Fit</Button>
          <Button variant="secondary" size="small" aria-label="Zoom in" onClick={() => setScale((value) => Math.min(2.5, value + 0.2))}>+</Button>
        </div>
      </div>
      <div className="max-w-full overflow-auto rounded border border-white/10 bg-black/10">
        <svg
          role="group"
          aria-label={title}
          aria-describedby={descriptionId}
          viewBox={`0 0 ${width} ${height}`}
          className="h-[440px] min-w-[720px] w-full touch-none"
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={stopDragging}
          onPointerCancel={stopDragging}
        >
          <defs>
            <marker id={markerId} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
              <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--tx-4)" />
            </marker>
          </defs>
          <g transform={`translate(${pan.x} ${pan.y}) scale(${scale})`}>
            {visibleEdges.map((edge) => {
              const source = positions.get(edge.source)!;
              const target = positions.get(edge.target)!;
              const selected = edge.id === selectedEdgeId;
              return (
                <g
                  key={edge.id}
                  role="button"
                  tabIndex={0}
                  aria-label={`Connection ${edge.label}`}
                  onClick={() => { setSelectedEdgeId(edge.id); setSelectedNodeId(null); }}
                  onKeyDown={(event) => activate(event, () => { setSelectedEdgeId(edge.id); setSelectedNodeId(null); })}
                  className="cursor-pointer focus-visible:outline-none"
                >
                  <title>{edge.description}</title>
                  <line x1={source.x} y1={source.y} x2={target.x} y2={target.y} stroke={selected ? "var(--acc)" : "var(--tx-4)"} strokeWidth="1.5" markerEnd={`url(#${markerId})`} />
                  <line x1={source.x} y1={source.y} x2={target.x} y2={target.y} stroke="transparent" strokeWidth="14" />
                </g>
              );
            })}
            {laidOutNodes.map((node) => {
              const selected = node.id === selectedNodeId;
              return (
                <g
                  key={node.id}
                  role="button"
                  tabIndex={0}
                  onClick={() => { setSelectedNodeId(node.id); setSelectedEdgeId(null); }}
                  onKeyDown={(event) => activate(event, () => { setSelectedNodeId(node.id); setSelectedEdgeId(null); })}
                  className="cursor-pointer focus-visible:outline-none"
                >
                  <title>{node.description}</title>
                  <rect x={node.x! - 78} y={node.y! - 30} width="156" height="64" rx="5" fill="var(--bg-2)" stroke={selected ? "var(--acc)" : "var(--line-2)"} strokeWidth={selected ? "2" : "1"} />
                  <text x={node.x} y={node.y! - 5} textAnchor="middle" fill="var(--tx-1)" fontSize="12">{node.label}</text>
                  <text x={node.x} y={node.y! + 15} textAnchor="middle" fill="var(--tx-3)" fontSize="10">{node.sublabel}</text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>
      <div aria-live="polite" className="min-h-10 text-xs text-zinc-300">
        {selectedNode ? (
          <div className="rounded border border-white/10 p-3">
            <p>{selectedNode.description}</p>
            {selectedNode.href ? <Link className="mt-2 inline-block text-orange-300" href={selectedNode.href}>Open {selectedNode.label} details</Link> : null}
          </div>
        ) : selectedEdge ? <p className="rounded border border-white/10 p-3">{selectedEdge.description}</p> : (
          <p className="text-zinc-500">Select a node or connection with Enter or Space to inspect its evidence. Drag the background to pan.</p>
        )}
      </div>
    </div>
  );
}
