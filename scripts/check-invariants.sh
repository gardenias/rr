#!/usr/bin/env bash
#
# check-invariants.sh — enforce the bidirectional link between the system
# invariant registry (docs/docs/reference/invariants.md, INV-1…INV-n) and the
# code that claims to cover it (R-1: 不变量目录执法机制).
#
# Checks (all failures are reported, each with its own message):
#   1. every INV-n declared in the registry is referenced from code
#      (at least one test carries an `INV-n` backlink);
#   2. every INV-n referenced from code is declared in the registry;
#   3. every INV entry lists at least one test name in its 现有测试 section;
#   4. every listed test still exists in the code (guards against test
#      renames drifting the registry);
#   5. every listed test carries the `INV-n` marker — in its name or in the
#      comment block directly above the test fn (per the R-1 convention:
#      existing tests are not renamed; they get `/// INV-n: <invariant>` as
#      the first doc-comment line, or a `// INV-n: …` line directly above
#      the test fn when they have no doc comment).
#
# Usage:
#   ./scripts/check-invariants.sh        # run from the repository root
#
# Exit status: 0 when every link is green, 1 otherwise.
#
# Portability: BSD (macOS) and GNU grep/sed/awk only — no `grep -oP`, no
# GNU-only flags. CI runs this on ubuntu-latest.

set -euo pipefail

REGISTRY="docs/docs/reference/invariants.md"
SEARCH_DIRS=(crates tests examples tools)

failures=0

fail() {
  echo "error: $1" >&2
  failures=$((failures + 1))
}

# test_carries_marker <test-fn-name> <INV-id>
# Succeeds when the contiguous comment/attribute block directly above the
# test's fn line mentions the INV id.
test_carries_marker() {
  local name="$1" id="$2"
  local files f
  files="$(grep -rlE --include='*.rs' "fn[[:space:]]+${name}[[:space:]]*\(" "${SEARCH_DIRS[@]}" || true)"
  [[ -n "$files" ]] || return 1
  for f in $files; do
    if awk -v name="$name" -v marker="$id" '
      { lines[NR] = $0 }
      END {
        pat = "fn[[:space:]]+" name "[[:space:]]*\\("
        for (i = 1; i <= NR; i++) {
          if (lines[i] ~ pat) {
            for (j = i - 1; j >= 1; j--) {
              l = lines[j]
              if (l ~ /^[[:space:]]*$/ || l ~ /^[[:space:]]*\/\// || l ~ /^[[:space:]]*#/) {
                if (index(l, marker) > 0) found = 1
                continue
              }
              break
            }
          }
        }
        exit(found ? 0 : 1)
      }
    ' "$f"; then
      return 0
    fi
  done
  return 1
}

if [[ ! -f "$REGISTRY" ]]; then
  echo "error: $REGISTRY not found — run this script from the repository root" >&2
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
# Extract the INV ids declared in the registry.
# ---------------------------------------------------------------------------
grep -oE 'INV-[0-9]+' "$REGISTRY" | sort -u -t- -k2 -n > "$WORK/registry_ids"

if [[ ! -s "$WORK/registry_ids" ]]; then
  echo "error: no INV-n ids found in $REGISTRY — is the registry file intact?" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Extract the test names declared in each INV entry's 现有测试 section.
#
# Registry formatting convention (parsed tolerantly):
#   - test names are backticked snake_case identifiers (`test_foo_bar`);
#   - `...`-prefixed backticked tokens are abbreviations sharing the entry's
#     common prefix (e.g. `..._mtls_rejects` for `validate_security_...`),
#     matched later as substrings;
#   - a backticked identifier only counts as a test name when it directly
#     follows a code-location token (`path.rs`, `path.rs:NNN`, `:NNN`) or is
#     directly followed by a `(:NNN)` line reference — this keeps descriptive
#     tokens such as `leader_info` / `last_log_index` out of the test list.
# ---------------------------------------------------------------------------
awk '
  function flush(   s, tok, rest, prev_loc) {
    s = buf
    prev_loc = 0
    while (match(s, /`[^`]+`/)) {
      tok  = substr(s, RSTART + 1, RLENGTH - 2)
      rest = substr(s, RSTART + RLENGTH)
      if (tok ~ /\.rs(:[0-9]+(-[0-9]+)?)?$/ || tok ~ /^:[0-9]+(-[0-9]+)?$/) {
        prev_loc = 1
      } else if (tok ~ /^[a-z_][a-z0-9_]*$/ || tok ~ /^\.\.\.[a-z0-9_]+$/) {
        if (prev_loc || rest ~ /^[[:space:]]*\(:[0-9]+(-[0-9]+)?\)/) {
          print inv "\t" tok
        }
        prev_loc = 0
      } else {
        prev_loc = 0
      }
      s = rest
    }
  }
  /^## INV-[0-9]+/ { if (in_tests) flush(); inv = $2; in_tests = 0; buf = ""; next }
  /^## /           { if (in_tests) flush(); inv = ""; in_tests = 0; buf = ""; next }
  /^### /          { if (in_tests) flush(); in_tests = (inv != "" && $0 ~ /^### 现有测试/); buf = ""; next }
  in_tests         { buf = buf " " $0 }
  END              { if (in_tests) flush() }
' "$REGISTRY" > "$WORK/listed_tests"

# ---------------------------------------------------------------------------
# Reverse direction: INV ids referenced from code.
# ---------------------------------------------------------------------------
grep -rhoE 'INV-[0-9]+' --include='*.rs' "${SEARCH_DIRS[@]}" \
  | sort -u -t- -k2 -n > "$WORK/code_ids" || true

# --- check 1: registry → code ----------------------------------------------
echo "==> [1/5] every declared INV is referenced from code"
while IFS= read -r id; do
  if ! grep -qxF "$id" "$WORK/code_ids"; then
    fail "$id is declared in $REGISTRY but never referenced from code — add an '/// $id: <invariant>' backlink to at least one covering test"
  fi
done < "$WORK/registry_ids"

# --- check 2: code → registry ----------------------------------------------
echo "==> [2/5] every code-side INV reference is declared in the registry"
while IFS= read -r id; do
  if ! grep -qxF "$id" "$WORK/registry_ids"; then
    fail "$id is referenced from code but not declared in $REGISTRY:"
    grep -rn -F "$id" --include='*.rs' "${SEARCH_DIRS[@]}" | sed 's/^/        /' >&2 || true
  fi
done < "$WORK/code_ids"

# --- check 3: every INV lists at least one test -----------------------------
echo "==> [3/5] every declared INV lists at least one test"
while IFS= read -r id; do
  if ! awk -F '\t' -v id="$id" '$1 == id { found = 1 } END { exit !found }' "$WORK/listed_tests"; then
    fail "$id declares no test names in its 现有测试 section (section missing or unparseable?)"
  fi
done < "$WORK/registry_ids"

# --- checks 4+5: listed tests exist in code and carry their marker ----------
echo "==> [4/5] every listed test still exists in code"
echo "==> [5/5] every listed test carries its INV backlink"
while IFS=$'\t' read -r id tok; do
  name="$tok"
  if [[ "$tok" == ...* ]]; then
    # Registry abbreviation (`..._suffix`) — resolve to the full identifier.
    suffix="${tok#...}"
    name="$(grep -rhoE "[A-Za-z0-9_]*${suffix}" --include='*.rs' "${SEARCH_DIRS[@]}" | sort -u | head -n 1 || true)"
    if [[ -z "$name" ]]; then
      fail "[$id] listed test abbreviation \`$tok\` matches nothing in code — registry drifted?"
      continue
    fi
  fi
  # check 4: the test fn must still exist
  if ! grep -rqE --include='*.rs' "fn[[:space:]]+${name}[[:space:]]*\(" "${SEARCH_DIRS[@]}"; then
    fail "[$id] lists test \`$name\` in 现有测试, but no 'fn ${name}(' exists under ${SEARCH_DIRS[*]} — test renamed? update $REGISTRY in the same PR"
    continue
  fi
  # check 5: the test must carry the marker (in its name or comment block)
  if [[ "$name" == *"$id"* || "$name" == *"inv${id#INV-}"* ]]; then
    continue
  fi
  if ! test_carries_marker "$name" "$id"; then
    fail "[$id] test \`$name\` carries no '$id' marker — add '/// $id: <invariant>' as the first doc-comment line (or '// $id: …' directly above the test fn)"
  fi
done < "$WORK/listed_tests"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
n_registry="$(wc -l < "$WORK/registry_ids" | tr -d ' ')"
n_code="$(wc -l < "$WORK/code_ids" | tr -d ' ')"
n_links="$(wc -l < "$WORK/listed_tests" | tr -d ' ')"

if (( failures > 0 )); then
  echo >&2
  echo "invariants-lint FAILED: $failures problem(s) — $n_registry declared INV ids, $n_code code-referenced ids, $n_links listed registry↔test links" >&2
  exit 1
fi

echo "invariants-lint OK: $n_registry INV ids declared and referenced, $n_links registry↔test links verified (declared→code, code→declared, tests exist, tests carry markers)"
