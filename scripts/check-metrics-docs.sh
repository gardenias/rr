#!/usr/bin/env bash
#
# check-metrics-docs.sh — bidirectional drift check between the metrics the
# code emits and the metrics documented in the reference page
# (docs/docs/reference/metrics-reference.md).
#
# Direction 1: every metric name emitted by a metrics-macro call site in
#              crates/ has a `#### ` heading in the doc.
# Direction 2: every metric named by a `#### ` heading in the doc is still
#              emitted somewhere in crates/.
#
# Usage:
#   ./scripts/check-metrics-docs.sh        # run from the repository root
#
# Exit status: 0 when both directions are clean, 1 on any drift.
#
# Portability: BSD (macOS) and GNU grep/sed/awk only — no `grep -oP`, no
# GNU-only flags (same baseline as check-invariants.sh).

set -euo pipefail

DOC="docs/docs/reference/metrics-reference.md"
SEARCH_DIRS=(crates)

# --- Exemptions -------------------------------------------------------------
#
# EXEMPT: exact metric names that legitimately never round-trip through this
# check — i.e. series exported by dependencies or exporter internals rather
# than by a metrics:: macro call site in this repo (the extraction below can
# never see them), yet worth documenting in the reference page.
# Currently EMPTY on purpose: every rr_* metric is emitted via metrics::
# macros in crates/ and is expected to round-trip through the doc.
# (Verified 2026-11-04: rr_snapshot_installs_total is a plain
# metrics::counter! call in rr-raft/src/service_adapter.rs — NOT exempt.)
EXEMPT=(
  # none — add exact names here only with a comment proving the emission
  # mechanism is invisible to the macro scan below
)

failures=0

if [[ ! -f "$DOC" ]]; then
  echo "error: $DOC not found — run this script from the repository root" >&2
  exit 1
fi
for dir in "${SEARCH_DIRS[@]}"; do
  if [[ ! -d "$dir" ]]; then
    echo "error: directory '$dir' not found — run this script from the repository root" >&2
    exit 1
  fi
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# ---------------------------------------------------------------------------
# Extract metric names emitted in code.
#
# A metric name is the first string-literal argument of a counter!/gauge!/
# histogram! macro call (both the qualified `metrics::counter!` form and the
# `use metrics::counter; counter!` imported form — every bare use in this
# tree is a metrics-crate import, verified 2026-11-04), or of a describe_*
# registration (none exist today; kept so future registrations are covered).
# Call sites routinely span lines:
#     metrics::counter!(
#         "rr_server_requests_total",
#         ...
#     )
# and BSD grep has no multiline mode, so each file is flattened with tr
# before matching — the first string literal after the macro name is the
# metric name in the metrics-crate API.
# ---------------------------------------------------------------------------
: > "$WORK/code_metrics_raw"
for dir in "${SEARCH_DIRS[@]}"; do
  while IFS= read -r f; do
    tr '\n' ' ' < "$f" \
      | grep -oE '((metrics::)?(counter|gauge|histogram)|describe_(counter|gauge|histogram))!\([[:space:]]*"[^"]+"' \
      | sed -E 's/^((metrics::)?(counter|gauge|histogram)|describe_(counter|gauge|histogram))!\([[:space:]]*"([^"]+)"/\5/' \
      >> "$WORK/code_metrics_raw" || true
  done < <(grep -rlE '((metrics::)?(counter|gauge|histogram)|describe_(counter|gauge|histogram))!\(' "$dir" --include='*.rs' || true)
done
sort -u -o "$WORK/code_metrics" "$WORK/code_metrics_raw"

if [[ ! -s "$WORK/code_metrics" ]]; then
  echo "error: no metric emissions found under ${SEARCH_DIRS[*]} — extraction broken?" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Extract documented metric names.
#
# Doc contract: every documented metric has a `#### ` heading; grouped
# entries keep each name in its own backticks (`a` / `b`); some names carry a
# `{label}` suffix (`rr_config_etcd_watch_reconnects_total{domain}`) that is
# illustration, not part of the name. Only tokens that look like metric names
# (`rr_*` or legacy dot-style `node.started`) count — recording-rule names
# (`rr:...:...`) and prose tokens never appear in headings, and body/query
# mentions (`_bucket`/`_count` series) are not headings.
# ---------------------------------------------------------------------------
grep -E '^#### ' "$DOC" \
  | grep -oE '`[^`]+`' \
  | tr -d '`' \
  | sed -E 's/\{[^}]*\}$//' \
  | grep -E '^(rr_[a-z0-9_]*|[a-z0-9_]+(\.[a-z0-9_]+)+)$' \
  | sort -u > "$WORK/doc_metrics" || true

if [[ ! -s "$WORK/doc_metrics" ]]; then
  echo "error: no metric headings found in $DOC — extraction broken or doc format changed?" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Apply exemptions (exact names) to a drift list on stdin.
# ---------------------------------------------------------------------------
filter_exempt() {
  local out exempt_hit
  while IFS= read -r out; do
    exempt_hit=0
    for name in ${EXEMPT[@]+"${EXEMPT[@]}"}; do
      if [[ "$out" == "$name" ]]; then exempt_hit=1; break; fi
    done
    [[ "$exempt_hit" -eq 1 ]] || printf '%s\n' "$out"
  done
}

comm -23 "$WORK/code_metrics" "$WORK/doc_metrics" | filter_exempt > "$WORK/undocumented"
comm -13 "$WORK/code_metrics" "$WORK/doc_metrics" | filter_exempt > "$WORK/absent"

# --- direction 1: code → doc -------------------------------------------------
if [[ -s "$WORK/undocumented" ]]; then
  echo "error: metrics emitted in code but not documented in $DOC:" >&2
  while IFS= read -r m; do
    echo "  - $m" >&2
    grep -rn -F "\"$m\"" "${SEARCH_DIRS[@]}" --include='*.rs' | head -3 | sed 's/^/      /' >&2 || true
  done < "$WORK/undocumented"
  echo "  -> add a \`####\` section to $DOC (or an exemption in scripts/check-metrics-docs.sh)" >&2
  failures=$((failures + 1))
fi

# --- direction 2: doc → code -------------------------------------------------
if [[ -s "$WORK/absent" ]]; then
  echo "error: metrics documented in $DOC but no longer emitted in code:" >&2
  while IFS= read -r m; do
    echo "  - $m" >&2
  done < "$WORK/absent"
  echo "  -> remove/rename the stale \`####\` section in $DOC (or fix the emission)" >&2
  failures=$((failures + 1))
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
n_code="$(wc -l < "$WORK/code_metrics" | tr -d ' ')"
n_doc="$(wc -l < "$WORK/doc_metrics" | tr -d ' ')"

if (( failures > 0 )); then
  echo >&2
  echo "metrics-docs-lint FAILED: $n_code emitted, $n_doc documented — drift listed above" >&2
  exit 1
fi

echo "metrics-docs OK: $n_code emitted and $n_doc documented metric names match bidirectionally"
