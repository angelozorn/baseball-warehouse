#!/bin/bash
# Daily scheduled run: ingest yesterday's games, then rebuild and test the
# dbt models. Invoked by launchd; see com.angelozorn.statcast-daily.plist.
#
# Two heartbeat files, because they answer different questions:
#   last_run.txt          — the scheduler fired and the run completed
#   last_data_loaded.txt  — data actually arrived
# In the offseason the first advances daily while the second goes stale, which
# is correct. In season, a gap between them is the alarm.

set -Eeuo pipefail

REPO="$HOME/baseball-warehouse"
LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/daily_$(date +%Y-%m-%d).log"

on_failure() {
  echo "FAILED $(date -Iseconds)" >> "$LOG"
  osascript -e 'display notification "statcast daily run failed — check logs" with title "baseball-warehouse"' 2>/dev/null || true
}
trap on_failure ERR

cd "$REPO"
source venv/bin/activate

# Yesterday: today's games aren't finished. BSD date — this line is macOS-only.
TARGET=$(date -v-1d +%Y-%m-%d)

echo "=== run $(date -Iseconds) for $TARGET ===" >> "$LOG"

# `|| RC=$?` rather than `; RC=$?` — under `set -e` the script would exit
# before the capture.
RC=0
python3 ingest_statcast.py "$TARGET" >> "$LOG" 2>&1 || RC=$?

if [ "$RC" -eq 3 ]; then
  # No games. Nothing changed, so there is nothing for dbt to rebuild.
  echo "NO GAMES $TARGET" >> "$LOG"
  echo "$(date -Iseconds) $TARGET no-games" > "$REPO/last_run.txt"
  exit 0
fi

if [ "$RC" -ne 0 ]; then
  on_failure
  exit "$RC"
fi

# `build` rather than `run` then `test`: build interleaves them per model, so a
# model whose tests fail is not used by anything downstream.
( cd transform && dbt build ) >> "$LOG" 2>&1

echo "$(date -Iseconds) $TARGET" > "$REPO/last_run.txt"
echo "$(date -Iseconds) $TARGET" > "$REPO/last_data_loaded.txt"
echo "OK $TARGET" >> "$LOG"