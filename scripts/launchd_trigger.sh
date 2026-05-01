#!/bin/bash
# Fired by ~/Library/LaunchAgents/com.leeslater.hotlist-daily.plist.
# Triggers the GitHub Actions workflow_dispatch for daily.yml so all the
# heavy work (Playwright scrape, perf, render, email, push) happens on
# GitHub's runner with state preserved in the repo. We just need a real
# clock locally — GitHub's schedule: cron is best-effort and unreliable.

set -u

LOG="/Users/leeslater/code/hotlist-tracker/launchd.log"
GH=/opt/homebrew/bin/gh
REPO=soylee22/hotlist-tracker
WORKFLOW=daily.yml

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

echo "$(stamp) trigger: invoked by launchd" >> "$LOG"

# Idempotency: skip if any run was created in the last 18 hours. This
# is window-based not date-based so the 07:35 morning backup correctly
# skips when the 22:05 evening primary fired ~9.5h earlier (different
# UTC date but same logical "today's run"). Window only re-opens if
# the evening primary genuinely missed (Mac off, network down).
CUTOFF=$(date -u -v-18H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '18 hours ago' +%Y-%m-%dT%H:%M:%SZ)
existing=$("$GH" api "/repos/$REPO/actions/workflows/$WORKFLOW/runs?per_page=10" \
  --jq ".workflow_runs[] | select(.created_at > \"$CUTOFF\") | \"\(.id) \(.created_at)\"" 2>>"$LOG" | head -1)

if [ -n "$existing" ]; then
  echo "$(stamp) trigger: recent run exists ($existing) within 18h window, skipping dispatch" >> "$LOG"
  exit 0
fi

"$GH" workflow run "$WORKFLOW" -R "$REPO" >>"$LOG" 2>&1
rc=$?
if [ "$rc" -eq 0 ]; then
  echo "$(stamp) trigger: dispatched ok" >> "$LOG"
  exit 0
fi
echo "$(stamp) trigger: dispatch FAILED (gh exit $rc)" >> "$LOG"
exit 1
