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

# Idempotency: skip if a run for today's UTC date already exists.
TODAY=$(date -u +%Y-%m-%d)
existing=$("$GH" api "/repos/$REPO/actions/workflows/$WORKFLOW/runs?per_page=10" \
  --jq ".workflow_runs[] | select(.created_at | startswith(\"$TODAY\")) | .id" 2>>"$LOG" | head -1)

if [ -n "$existing" ]; then
  echo "$(stamp) trigger: run $existing already exists for $TODAY, skipping dispatch" >> "$LOG"
  exit 0
fi

if "$GH" workflow run "$WORKFLOW" -R "$REPO" >>"$LOG" 2>&1; then
  echo "$(stamp) trigger: dispatched ok" >> "$LOG"
  exit 0
else
  echo "$(stamp) trigger: dispatch FAILED (exit $?)" >> "$LOG"
  exit 1
fi
