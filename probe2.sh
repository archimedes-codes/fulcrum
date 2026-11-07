#!/usr/bin/env bash
# Probe 2: the two remaining unknowns from probe 1.
#   (a) does enumerate_unix's exact `ps` spelling produce rows on this platform?
#       (Case I failed with an EMPTY enumerated stream on macOS, which is a
#        different bug from the env-extraction one.)
#   (b) does a multi-match env extraction recover the marker from real
#       platform-shaped `ps` output, where the single-match BASH_REMATCH does not?
set -uo pipefail

MARKER_NAME=FULCRUM_CI_RUN_MARKER
MARKER_VALUE="probe2-$$-$(date +%s)"
export "$MARKER_NAME=$MARKER_VALUE"

echo "=== platform ==="
uname -s
ps --version 2>/dev/null | head -1 || echo "(BSD ps: no --version)"

sleep 8 &
CHILD=$!
sleep 1

echo
echo "=== (a) enumerate_unix's EXACT spelling ==="
PS_A="ps -eo pid=,ppid=,stat=,comm=,args="
echo "--- \$($PS_A) ---"
rows=$($PS_A 2>&1)
rc=$?
printf 'exit=%s rows=%s bytes=%s\n' "$rc" "$(printf '%s' "$rows" | grep -c '[^[:space:]]')" "$(printf '%s' "$rows" | wc -c | tr -d ' ')"
printf '%s\n' "$rows" | grep -E "(^|[[:space:]])$CHILD([[:space:]]|$)" | head -2 | sed 's/^/  child row: /'
echo "  child $CHILD present in enumeration? $(printf '%s\n' "$rows" | grep -qE "(^|[[:space:]])$CHILD([[:space:]]|$)" && echo YES || echo NO)"

echo
echo "--- the SAME with the e/eww modifiers the env path adds ---"
for spelling in "ps eww -eo pid=,args=" "ps -E -eo pid=,args=" "ps e -eo pid=,args=" "ps -eww -eo pid=,args="; do
  out=$($spelling 2>&1); rc=$?
  printf '  %-32s exit=%-3s bytes=%-8s marker=%s\n' "$spelling" "$rc" \
    "$(printf '%s' "$out" | wc -c | tr -d ' ')" \
    "$(printf '%s' "$out" | grep -q -- "$MARKER_NAME=$MARKER_VALUE" && echo FOUND || echo absent)"
done

echo
echo "=== (b) single-match (what the script does) vs multi-match, on REAL output ==="
# The real line for our child, exactly as the platform prints it.
realline="$(ps eww -o pid=,args= -p "$CHILD" 2>/dev/null | head -1)"
echo "  real ps line length: ${#realline} chars, $(printf '%s' "$realline" | wc -l | tr -d ' ') newline(s)"

single() {
  local raw="$1" lines=()
  while IFS= read -r line; do
    if [[ "$line" =~ [[:space:]]([A-Za-z_][A-Za-z0-9_]*)=([^[:space:]]*) ]]; then
      lines+=("${BASH_REMATCH[1]}=${BASH_REMATCH[2]}")
    fi
  done <<<"$raw"
  printf '%s' "${lines[*]}"
}
multi() {
  local raw="$1" rest lines=()
  while IFS= read -r line; do
    rest="$line"
    while [[ "$rest" =~ [[:space:]]([A-Za-z_][A-Za-z0-9_]*)=([^[:space:]]*) ]]; do
      lines+=("${BASH_REMATCH[1]}=${BASH_REMATCH[2]}")
      # `#*` — strip the shortest prefix UP TO AND INCLUDING the match. A plain
      # `#"$match"` only strips a PREFIX; the match is usually mid-line, so that
      # spelling leaves `rest` unchanged and the loop never terminates. (Found the
      # hard way: the first draft of this probe hung and had to be killed.)
      rest="${rest#*"${BASH_REMATCH[0]}"}"
      [[ -n "$rest" ]] || break
    done
  done <<<"$raw"
  printf '%s' "${lines[*]}"
}

s="$(single "$realline")"
m="$(multi "$realline")"
printf '  single-match extracted %d var(s); marker in output? %s\n' "$((${#s} > 0 ? 1 : 0))" "$(printf '%s' "$s" | grep -q -- "$MARKER_NAME" && echo YES || echo NO)"
printf '  first var single-match found: [%s]\n' "${s%% *}"
printf '  multi-match found marker?     %s\n' "$(printf '%s' "$m" | grep -q -- "$MARKER_NAME=$MARKER_VALUE" && echo YES || echo NO)"
printf '  multi-match var count: %s\n' "$(printf '%s' "$m" | wc -w | tr -d ' ')"

echo
echo "=== (c) argv contamination risk for multi-match ==="
# A worker's argv carries --flag=VALUE tokens. If multi-match treats those as env,
# the marker could be "found" in an argv token instead of the environment.
argvy="/tmp/build/fake_worker_core --worker --session=deadbeef --exe=/bin/true"
printf '  argv-only line, multi-match says: [%s]\n' "$(multi " 1234 $argvy")"
printf '  (expect EMPTY or argv-derived; a marker here would be a false attribution)\n'

echo
echo "=== (d) setsid / pgrep availability (live self-test cases need these) ==="
for t in setsid pgrep tasklist; do
  command -v "$t" >/dev/null 2>&1 && printf '  %-9s %s\n' "$t" "$(command -v "$t")" || printf '  %-9s MISSING\n' "$t"
done

kill "$CHILD" 2>/dev/null; wait "$CHILD" 2>/dev/null
echo "=== probe 2 complete ==="
