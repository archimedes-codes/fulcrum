#!/usr/bin/env bash
# Throwaway diagnostic: what can a macOS runner actually observe about process
# environments? The census's whole run-scoping answer depends on this, and the
# branch's own header admits it is UNVERIFIED from the machine that wrote it.
#
# Prints, per spelling: exit code, byte count, and whether the sentinel marker
# was found. Nothing here gates anything; it exists to turn "SIP probably blocks
# this" into a measurement.
set -uo pipefail

MARKER_NAME=FULCRUM_CI_RUN_MARKER
MARKER_VALUE="probe-$$-$(date +%s)"
export "$MARKER_NAME=$MARKER_VALUE"

SELF=$$

echo "=== platform ==="
uname -a
ps --version 2>/dev/null || echo "ps: no --version (BSD ps, as expected on macOS)"
echo "self pid=$SELF  marker=$MARKER_NAME=$MARKER_VALUE"

# A child that carries the marker and lives long enough to be probed: this is the
# shape the census actually needs — reading SOMEONE ELSE'S (same-user) env, not
# its own. Own-env success would prove nothing about that.
sleep 30 &
CHILD=$!

# Give it a moment to be scheduled, then confirm it is really there.
sleep 1
if ! ps -p "$CHILD" >/dev/null 2>&1; then
  echo "FATAL: the probe child $CHILD is not visible to ps; nothing below means anything"
  kill "$CHILD" 2>/dev/null
  exit 1
fi
echo "probe child pid=$CHILD (a same-user process carrying the marker)"
echo

probe() {
  local label="$1"; shift
  # NOTE: no pid argument here. The command line after the label IS the command;
  # an earlier draft consumed `ps` as the pid and ran `eww ...` as a program,
  # which reported 127 for every spelling and proved nothing.
  local out rc
  out="$("$@" 2>&1)"; rc=$?
  local bytes found
  bytes=$(printf '%s' "$out" | wc -c | tr -d ' ')
  if printf '%s' "$out" | grep -q -- "$MARKER_NAME=$MARKER_VALUE"; then
    found="MARKER FOUND"
  else
    found="no marker"
  fi
  printf '%-46s exit=%-3s bytes=%-7s %s\n' "$label" "$rc" "$bytes" "$found"
  if [[ $rc -eq 0 && $bytes -gt 0 && $found == "no marker" ]]; then
    # Show whether ANY env-looking text came back, to separate "ps refused"
    # (no NAME=VALUE at all) from "ps worked but the marker is absent".
    if printf '%s' "$out" | grep -Eq '[A-Za-z_][A-Za-z0-9_]*='; then
      echo "      -> returned NAME=VALUE-looking text, but NOT our marker"
    else
      echo "      -> returned NO env-shaped text at all (refusal-shaped)"
    fi
  elif [[ $rc -ne 0 ]]; then
    echo "      -> stderr/stdout: $(printf '%s' "$out" | head -1 | cut -c1-100)"
  fi
}

echo "=== spellings against the CENSUS'S OWN pid (what control_probe does) ==="
probe "ps eww -o pid=,args= -p \$SELF   (what the script uses)" ps eww -o pid=,args= -p "$SELF"
probe "ps -E -o pid=,args= -p \$SELF    (BSD env flag)" ps -E -o pid=,args= -p "$SELF"
probe "ps e -ww -o pid=,args= -p \$SELF" ps e -ww -o pid=,args= -p "$SELF"
probe "ps eww -p \$SELF                 (no -o)" ps eww -p "$SELF"
probe "ps -p \$SELF -wwwe               (reported working on 10.6)" ps -p "$SELF" -wwwe

echo
echo "=== the same spellings against ANOTHER process (what the census REALLY needs) ==="
probe "ps eww -o pid=,args= -p \$CHILD" ps eww -o pid=,args= -p "$CHILD"
probe "ps -E -o pid=,args= -p \$CHILD" ps -E -o pid=,args= -p "$CHILD"
probe "ps e -ww -o pid=,args= -p \$CHILD" ps e -ww -o pid=,args= -p "$CHILD"
probe "ps eww -p \$CHILD" ps eww -p "$CHILD"

echo
echo "=== full-table env read (does the census need per-pid calls at all?) ==="
tb=$(ps eww -eo pid=,args= 2>&1 | wc -c | tr -d ' ')
te=$(ps -E -eo pid=,args= 2>&1 | wc -c | tr -d ' ')
echo "ps eww -eo pid=,args=    bytes=$tb"
echo "ps -E  -eo pid=,args=    bytes=$te"
if ps eww -eo pid=,args= 2>/dev/null | grep -q -- "$MARKER_NAME=$MARKER_VALUE"; then
  echo "  -> full-table 'ps eww' DOES contain the marker"
else
  echo "  -> full-table 'ps eww' does NOT contain the marker"
fi
if ps -E -eo pid=,args= 2>/dev/null | grep -q -- "$MARKER_NAME=$MARKER_VALUE"; then
  echo "  -> full-table 'ps -E'   DOES contain the marker"
else
  echo "  -> full-table 'ps -E'   does NOT contain the marker"
fi

echo
echo "=== tooling the self-test's live cases assume ==="
for t in ps setsid python3 pgrep tasklist; do
  if command -v "$t" >/dev/null 2>&1; then
    printf '  %-10s %s\n' "$t" "$(command -v "$t")"
  else
    printf '  %-10s MISSING\n' "$t"
  fi
done
echo "pgrep -x truncation check (15-char limit):"
pgrep -x fake_worker_core >/dev/null 2>&1; echo "  pgrep -x fake_worker_core exit=$?"

echo
echo "=== lineage arm the census falls back to on macOS ==="
pp=$(ps -o ppid= -p "$CHILD" 2>&1); echo "  ps -o ppid= -p \$CHILD -> [$pp] exit=$?"
echo "  /proc/$CHILD/status readable? $([[ -r /proc/$CHILD/status ]] && echo yes || echo no)"

echo
echo "=== does a zombie show up, and shaped how? (Case M's premise) ==="
if command -v python3 >/dev/null 2>&1; then
  python3 -c "
import os,sys,time
pid=os.fork()
if pid==0: os._exit(0)
time.sleep(2)   # parent never wait()s -> real zombie
print('zombie child pid',pid,file=sys.stderr)
" 2>/tmp/probe-zombie-pid.txt
  zpid=$(grep -oE '[0-9]+' /tmp/probe-zombie-pid.txt | head -1)
  echo "  planted zombie pid=$zpid"
  ps -o pid=,stat=,comm=,args= -p "$zpid" 2>/dev/null | sed 's/^/  ps row: /'
else
  echo "  python3 absent; zombie premise not testable here"
fi

kill "$CHILD" 2>/dev/null
wait "$CHILD" 2>/dev/null
echo
echo "=== probe complete ==="
