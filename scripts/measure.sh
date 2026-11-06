#!/usr/bin/env bash
# Run the release collector for N minutes under a label, keep its stderr as
# runs/<UTC start>-<label>.log, send SIGINT at the end so the drain path is
# exercised, then print the summary row for that log.
#
#   scripts/measure.sh <label> <minutes> [collector flags...]
#
# stdout (the SBE stream, the default output) goes to runs/<start>-<label>.sbe
# and the journal to runs/<start>-<label>.journal/ unless the flags say
# otherwise. KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH must be set.
set -euo pipefail

label=${1:?usage: measure.sh <label> <minutes> [flags...]}
minutes=${2:?usage: measure.sh <label> <minutes> [flags...]}
shift 2

root=$(cd "$(dirname "$0")/.." && pwd)
bin="$root/target/release/engineering-challenge"
if [ ! -x "$bin" ]; then
  (cd "$root" && cargo build --release)
fi
mkdir -p "$root/runs"

start=$(date -u +%Y%m%dT%H%M%SZ)
log="$root/runs/$start-$label.log"
sbe="$root/runs/$start-$label.sbe"
journal="$root/runs/$start-$label.journal"

extra=()
case " $* " in
  *" --journal-dir "*) ;;
  *) extra+=(--journal-dir "$journal") ;;
esac

echo "start $start  log $log"
"$bin" "${extra[@]}" "$@" > "$sbe" 2> "$log" &
pid=$!
trap 'kill -INT "$pid" 2>/dev/null || true' INT TERM

sleep "$((minutes * 60))"
kill -INT "$pid"
# The writer pads and flushes its last chunk on the way out; give it time.
for _ in $(seq 1 120); do
  kill -0 "$pid" 2>/dev/null || break
  sleep 0.5
done
status=0
wait "$pid" || status=$?
echo "collector exit status $status"
# No SBE on stdout when --out <dir> was given; drop the empty file.
[ -s "$sbe" ] || rm -f "$sbe"

python3 "$root/scripts/summarize.py" "$log"
