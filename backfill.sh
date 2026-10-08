#!/bin/bash
# Backfill a date range, one day per ingest run.
#
# Resumable: completed dates are appended to backfill_done.txt and skipped on a
# later run, so an interrupt costs at most the day in flight (partition
# overwrite makes redoing that day harmless).
#
# Failures are logged, not fatal: one transient blip should not cost the rest
# of the range.

set -uo pipefail

START="2026-03-25"
END="2026-09-27"

python3 -c "
import datetime as dt, sys
d   = dt.date.fromisoformat('$START')
end = dt.date.fromisoformat('$END')
while d <= end:
    print(d); d += dt.timedelta(days=1)
" > dates.txt

touch backfill_done.txt backfill_failed.txt

while read -r d; do
  if grep -qx "$d" backfill_done.txt; then
    echo "skip $d (already done)"
    continue
  fi

  echo "=== $d ==="

  RC=0
  python3 ingest_statcast.py "$d" || RC=$?

  # 0 = loaded, 3 = no games scheduled. Both mean "this date is handled."
  if [ "$RC" -eq 0 ] || [ "$RC" -eq 3 ]; then
    echo "$d" >> backfill_done.txt
  else
    echo "$d" >> backfill_failed.txt
    echo "FAILED $d rc=$RC (continuing)"
  fi
done < dates.txt

echo "done: $(wc -l < backfill_done.txt), failed: $(wc -l < backfill_failed.txt)"