#!/usr/bin/env bash
# scripts/install-local-gates.sh — register or remove a launchd agent that runs
# the quality gates unattended (local replacement for the disabled nightly CI).
#
# Usage:
#   scripts/install-local-gates.sh install [--hour H] [--minute M] [--suite S]
#   scripts/install-local-gates.sh uninstall
#   scripts/install-local-gates.sh status
#
# Defaults: daily at 03:47 local time, suite "nightly". The agent runs
# scripts/quality-gate.sh, which archives per-step logs and SUMMARY.md under
# target/quality-runs/<timestamp>-<suite>/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
LABEL="com.rr.quality-gates"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
RUNNER="$ROOT/scripts/quality-gate.sh"

cmd="${1:-}"
hour=3
minute=47
suite="nightly"
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --hour)   hour="$2"; shift 2 ;;
    --minute) minute="$2"; shift 2 ;;
    --suite)  suite="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

case "$cmd" in
  install)
    command -v just >/dev/null || { echo "just is required (brew install just)" >&2; exit 1; }
    [[ -x "$RUNNER" ]] || { echo "$RUNNER missing or not executable" >&2; exit 1; }
    mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/target/quality-runs"
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$RUNNER</string>
    <string>$suite</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>$hour</integer>
    <key>Minute</key><integer>$minute</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>$ROOT/target/quality-runs/launchd.out.log</string>
  <key>StandardErrorPath</key>
  <string>$ROOT/target/quality-runs/launchd.err.log</string>
  <key>WorkingDirectory</key>
  <string>$ROOT</string>
</dict>
</plist>
EOF
    launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    echo "installed $LABEL: suite=$suite daily at $(printf '%02d:%02d' "$hour" "$minute"), repo=$ROOT"
    echo "results: $ROOT/target/quality-runs/<timestamp>-$suite/"
    echo "uninstall: scripts/install-local-gates.sh uninstall"
    ;;
  uninstall)
    launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
    rm -f "$PLIST"
    echo "uninstalled $LABEL"
    ;;
  status)
    if [[ ! -f "$PLIST" ]]; then
      echo "$LABEL is not installed"
      exit 1
    fi
    echo "plist: $PLIST"
    launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | grep -E "state|last exit|pid" || true
    echo "recent runs:"
    ls -dt "$ROOT"/target/quality-runs/*/ 2>/dev/null | head -5 || echo "  (none yet)"
    ;;
  *)
    echo "usage: install-local-gates.sh install [--hour H] [--minute M] [--suite S] | uninstall | status" >&2
    exit 2
    ;;
esac
