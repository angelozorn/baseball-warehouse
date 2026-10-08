#!/bin/bash
# 2026 regular season: 2026-03-25 through 2026-09-27
python3 -c "
import datetime as dt
d, end = dt.date(2026,3,25), dt.date(2026,9,27)
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
  if python3 ingest_statcast.py "$d"; then
    echo "$d" >> backfill_done.txt
  else
    echo "$d" >> backfill_failed.txt
    echo "FAILED $d (continuing)"
  fi
done < dates.txt

echo "done: $(wc -l < backfill_done.txt), failed: $(wc -l < backfill_failed.txt)"
