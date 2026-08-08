#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Portable OM write-path benchmark for the HDDS-11898 finding:
# on classic-Ratis OM, spreading OBS create-key load across many buckets
# does not scale throughput (striped BUCKET_LOCK is uncontended with one
# writer per bucket); the single-threaded apply executor
# (OMStateMachineApplyTransactionThread) is the CPU-bound ceiling.
#
# Works against any already-running Ozone cluster. Compose defaults below;
# override FREON_RUN / OZONE_ADMIN / OM_TOP / OM_JSTACK for a real cluster.
#
# Usage (from compose/ozone): ./om-write-bench/om-write-bench.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

OZONE_ENV=${OZONE_ENV:-compose}
OM=${OM:-ozone-om-1}
VOL=${VOL:-vol1}
BUCKETS=${BUCKETS:-8}
THREADS_LIST=${THREADS_LIST:-"16 64 128"}
N=${N:-40000}
ITER=${ITER:-3}
REPL=${REPL:-"--type RATIS -r ONE"}
OUT=${OUT:-./om-write-bench-out}

# If FREON_RUN is unset: compose-managed persistent freon (one container, docker exec).
# If the caller exported FREON_RUN: use it verbatim; no freon lifecycle management.
FREON_CONTAINER_NAME=${FREON_CONTAINER_NAME:-ozone-bench-freon}
if [ -n "${FREON_RUN+x}" ]; then
  FREON_MODE=custom
else
  FREON_MODE=managed
  FREON_RUN="docker exec ${FREON_CONTAINER_NAME} ozone"
fi
OZONE_ADMIN=${OZONE_ADMIN:-"docker exec ${OM} ozone"}
OM_TOP=${OM_TOP:-"docker exec ${OM} top -Hbn1"}
OM_JSTACK=${OM_JSTACK:-"docker exec ${OM} jstack"}

if [ "${OZONE_ENV}" = "compose" ]; then
  cd "${COMPOSE_DIR}" || {
    printf 'ERROR: cannot cd to compose dir %s\n' "${COMPOSE_DIR}" >&2
    exit 2
  }
fi

cleanup_managed_freon() {
  docker rm -f "${FREON_CONTAINER_NAME}" >/dev/null 2>&1 || true
}

mkdir -p "${OUT}"
SUMMARY="${OUT}/summary.txt"
CSV="${OUT}/results.csv"
: > "${SUMMARY}"
printf 'phase,config,iter,keys_per_sec\n' > "${CSV}"

progress() {
  printf '%s\n' "$*" >&2
}

log_summary() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}" >&2
}

# Run a multi-word command prefix plus argv (word-split the prefix only).
invoke() {
  local prefix="$1"
  shift
  # shellcheck disable=SC2086
  $prefix "$@"
}

# APPLY-evidence = OMStateMachineApplyTransactionThread | OMExecutionFlow |
#   (StateMachine AND Apply) | OMDoubleBufferFlushThread | SegmentedRaftLogWorker
is_apply_evidence() {
  case "$1" in
    *OMStateMachineApplyTransactionThread*|*OMExecutionFlow*|*OMDoubleBufferFlushThread*|*SegmentedRaftLogWorker*)
      return 0
      ;;
    *StateMachine*)
      case "$1" in
        *Apply*|*apply*) return 0 ;;
      esac
      ;;
  esac
  return 1
}

# RPC intake (IPC handlers / pools) — expected busy; not apply-bottleneck evidence.
is_rpc_intake() {
  case "$1" in
    *IPC*)
      case "$1" in
        *handler*|*pool*) return 0 ;;
      esac
      ;;
  esac
  return 1
}

now_ms() {
  python3 -c 'import time; print(int(time.time()*1000))'
}

# Middle value of THREADS_LIST (0-based index n/2).
middle_threads() {
  # shellcheck disable=SC2086
  set -- ${THREADS_LIST}
  local n=$#
  if [ "$n" -lt 1 ]; then
    printf '64\n'
    return
  fi
  local idx=$((n / 2))
  local i=0
  for t in "$@"; do
    if [ "$i" -eq "$idx" ]; then
      printf '%s\n' "$t"
      return
    fi
    i=$((i + 1))
  done
}

# Parse freon "Successful executions: N" -> echo N (0 if absent).
successful_count() {
  local logfile="$1"
  local n
  n=$(grep -Eo 'Successful executions:[[:space:]]*[0-9]+' "${logfile}" 2>/dev/null \
    | head -n1 \
    | grep -Eo '[0-9]+' \
    || true)
  if [ -n "${n:-}" ]; then
    printf '%s\n' "$n"
  else
    printf '0\n'
  fi
}

# Throughput from host wall time + actual successful count.
keys_per_sec() {
  local logfile="$1"
  local n ms

  n=$(successful_count "${logfile}")
  ms=0
  if [ -f "${logfile}.ms" ]; then
    ms=$(cat "${logfile}.ms" 2>/dev/null || true)
  fi
  if [ -n "${n:-}" ] && [ -n "${ms:-}" ] && [ "$n" -gt 0 ] 2>/dev/null && [ "$ms" -gt 0 ] 2>/dev/null; then
    awk -v n="$n" -v ms="$ms" 'BEGIN { printf "%.2f\n", n * 1000 / ms }'
    return 0
  fi
  printf '0\n'
  return 0
}

# Sample mean and sample stddev (n-1) from space-separated numbers on stdin / args.
# Echoes: mean stddev
mean_stddev() {
  awk '
    BEGIN { n = 0; sum = 0; sumsq = 0 }
    {
      for (i = 1; i <= NF; i++) {
        x = $i + 0
        n++
        sum += x
        sumsq += x * x
      }
    }
    END {
      if (n == 0) { printf "0.00 0.00\n"; exit }
      mean = sum / n
      if (n > 1) {
        var = (sumsq - (sum * sum) / n) / (n - 1)
        if (var < 0) var = 0
        sd = sqrt(var)
      } else {
        sd = 0
      }
      printf "%.2f %.2f\n", mean, sd
    }
  '
}

# run_omkg <bucket> <threads> <nkeys> <prefix> <logfile>
# Host wall-clock elapsed ms written to <logfile>.ms sidecar.
run_omkg() {
  local bucket="$1" threads="$2" nkeys="$3" prefix="$4" logfile="$5"
  local start end rc
  progress "  freon omkg -b ${bucket} -t ${threads} -n ${nkeys} -p ${prefix}"
  start=$(now_ms)
  # shellcheck disable=SC2086
  invoke "${FREON_RUN}" freon omkg \
    -v "${VOL}" -b "${bucket}" \
    ${REPL} \
    -t "${threads}" -n "${nkeys}" -p "${prefix}" \
    >"${logfile}" 2>&1
  rc=$?
  end=$(now_ms)
  echo $((end - start)) >"${logfile}.ms"
  return "${rc}"
}

csv_row() {
  printf '%s,%s,%s,%s\n' "$1" "$2" "$3" "$4" >> "${CSV}"
}

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
progress "=== Preflight ==="
if ! command -v python3 >/dev/null 2>&1; then
  printf 'ERROR: python3 is required on the host running this script (wall-clock ms timer).\n' >&2
  exit 2
fi

if ! invoke "${OZONE_ADMIN}" sh volume list >/dev/null 2>&1; then
  printf 'ERROR: cluster not reachable via OZONE_ADMIN (%s sh volume list).\n' "${OZONE_ADMIN}" >&2
  printf 'Start a compose cluster (OZONE_REPLICATION_FACTOR=3 ./run.sh -d) or override OZONE_ADMIN / FREON_RUN.\n' >&2
  exit 2
fi

if [ "${FREON_MODE}" = "managed" ]; then
  progress "Starting persistent freon container (${FREON_CONTAINER_NAME})..."
  trap cleanup_managed_freon EXIT INT TERM
  docker rm -f "${FREON_CONTAINER_NAME}" >/dev/null 2>&1 || true
  if ! docker compose -f docker-compose.yaml -f om-write-bench/freon-omkg.yaml run -d --no-deps \
      --name "${FREON_CONTAINER_NAME}" freon sleep infinity >/dev/null; then
    printf 'ERROR: failed to start persistent freon container (%s).\n' "${FREON_CONTAINER_NAME}" >&2
    printf 'Check compose files docker-compose.yaml + om-write-bench/freon-omkg.yaml from %s.\n' "${COMPOSE_DIR}" >&2
    exit 2
  fi
  if ! docker inspect -f '{{.State.Running}}' "${FREON_CONTAINER_NAME}" 2>/dev/null | grep -qx true; then
    printf 'ERROR: freon container %s is not running after start.\n' "${FREON_CONTAINER_NAME}" >&2
    exit 2
  fi
  progress "Persistent freon ready (FREON_RUN=${FREON_RUN})"
else
  progress "Using caller FREON_RUN as-is (no freon lifecycle): ${FREON_RUN}"
fi
progress "Preflight OK (python3 + cluster reachable; FREON_MODE=${FREON_MODE})"

T_REF=$(middle_threads)
progress "Parameters: VOL=${VOL} BUCKETS=${BUCKETS} THREADS_LIST=${THREADS_LIST} T_ref=${T_REF} N=${N} ITER=${ITER} OUT=${OUT}"

# ---------------------------------------------------------------------------
# Phase 0 — setup (idempotent)
# ---------------------------------------------------------------------------
progress "=== Phase 0: setup volume/buckets ==="
invoke "${OZONE_ADMIN}" sh volume create "/${VOL}" >/dev/null 2>&1 || true
k=1
while [ "$k" -le "${BUCKETS}" ]; do
  invoke "${OZONE_ADMIN}" sh bucket create --layout OBJECT_STORE "/${VOL}/bucket${k}" \
    >/dev/null 2>&1 || true
  k=$((k + 1))
done
progress "Setup done: /${VOL}/bucket1..bucket${BUCKETS}"

# ---------------------------------------------------------------------------
# Phase A — single-bucket sweep (averaged)
# ---------------------------------------------------------------------------
progress "=== Phase A: single-bucket sweep (ITER=${ITER}) ==="
PHASE_A_SUMMARY_LINES=""
SINGLE_TREF_MEAN="0"

# shellcheck disable=SC2086
for T in ${THREADS_LIST}; do
  rates=""
  it=1
  while [ "$it" -le "${ITER}" ]; do
    logf="${OUT}/sweepA_t${T}_i${it}.log"
    progress "Phase A: t=${T} n=${N} iter=${it}/${ITER} -> ${logf}"
    if ! run_omkg bucket1 "${T}" "${N}" "sweepA_t${T}_i${it}" "${logf}"; then
      progress "  NOTE: iteration failed (recording 0 keys/s); continuing"
    fi
    rate=$(keys_per_sec "${logf}")
    if [ "${rate}" = "0" ] || [ "${rate}" = "0.00" ]; then
      progress "  NOTE: iteration produced 0 keys/s (see ${logf})"
    fi
    progress "  -> ${rate} keys/s"
    csv_row "A" "single-bucket t=${T}" "${it}" "${rate}"
    rates="${rates} ${rate}"
    it=$((it + 1))
  done
  read -r mean sd <<EOF
$(printf '%s\n' ${rates} | mean_stddev)
EOF
  line="A | single-bucket t=${T} | ${mean} +/- ${sd} keys/s (n=${ITER})"
  progress "${line}"
  PHASE_A_SUMMARY_LINES="${PHASE_A_SUMMARY_LINES}${line}"$'\n'
  if [ "${T}" = "${T_REF}" ]; then
    SINGLE_TREF_MEAN="${mean}"
  fi
done

# ---------------------------------------------------------------------------
# Phase B — multi-bucket spread at T_ref (averaged)
# ---------------------------------------------------------------------------
progress "=== Phase B: ${BUCKETS}-bucket spread at t=${T_REF} (ITER=${ITER}) ==="
per_threads=$((T_REF / BUCKETS))
if [ "${per_threads}" -lt 1 ]; then
  per_threads=1
fi
per_nkeys=$((N / BUCKETS))
if [ "${per_nkeys}" -lt 1 ]; then
  per_nkeys=1
fi

B_RATES=""
it=1
while [ "$it" -le "${ITER}" ]; do
  progress "Phase B iter=${it}/${ITER}: ${BUCKETS} concurrent freon (t=${per_threads}/bucket, n=${per_nkeys}/bucket)"
  B_PIDS=""
  B_START_MS=$(now_ms)
  k=1
  while [ "$k" -le "${BUCKETS}" ]; do
    blog="${OUT}/spreadB_i${it}_b${k}.log"
    run_omkg "bucket${k}" "${per_threads}" "${per_nkeys}" "spreadB_i${it}_b${k}" "${blog}" &
    B_PIDS="${B_PIDS} $!"
    k=$((k + 1))
  done
  batch_failed=0
  for p in ${B_PIDS}; do
    if ! wait "$p"; then
      batch_failed=1
    fi
  done
  B_END_MS=$(now_ms)
  B_WALL_MS=$((B_END_MS - B_START_MS))

  sum_keys=0
  k=1
  while [ "$k" -le "${BUCKETS}" ]; do
    blog="${OUT}/spreadB_i${it}_b${k}.log"
    actual=$(successful_count "${blog}")
    sum_keys=$((sum_keys + actual))
    k=$((k + 1))
  done

  if [ "${B_WALL_MS}" -gt 0 ] && [ "${sum_keys}" -gt 0 ]; then
    agg_wall=$(awk -v k="${sum_keys}" -v ms="${B_WALL_MS}" 'BEGIN { printf "%.2f", k * 1000 / ms }')
  else
    agg_wall=0
    progress "  NOTE: Phase B iter ${it} produced 0 aggregate keys/s (wall=${B_WALL_MS}ms sum_keys=${sum_keys})"
  fi
  if [ "${batch_failed}" -ne 0 ]; then
    progress "  NOTE: one or more freon processes failed in iter ${it}"
  fi
  progress "  wall=${B_WALL_MS}ms sum_keys=${sum_keys} -> ${agg_wall} keys/s"
  csv_row "B" "${BUCKETS}-bucket spread t=${T_REF}" "${it}" "${agg_wall}"
  B_RATES="${B_RATES} ${agg_wall}"
  it=$((it + 1))
done

read -r B_MEAN B_SD <<EOF
$(printf '%s\n' ${B_RATES} | mean_stddev)
EOF

scale_ratio=$(awk -v a="${B_MEAN}" -v s="${SINGLE_TREF_MEAN}" \
  'BEGIN { if (s + 0 > 0) printf "%.2f", a / s; else print "n/a" }')
verdict=$(awk -v r="${scale_ratio}" -v b="${BUCKETS}" 'BEGIN {
  if (r == "n/a") { print "inconclusive (missing single-bucket baseline)"; exit }
  if (r + 0 >= (b * 0.5)) {
    print "SCALED (~" r "x vs single-bucket; near " b "x expected if lock-bound)"
  } else {
    print "DID NOT SCALE (~" r "x vs single-bucket; expected ~" b "x if BUCKET_LOCK-bound)"
  }
}')
PHASE_B_LINE="B | ${BUCKETS}-bucket spread t=${T_REF} (${per_threads}/bucket) | ${B_MEAN} +/- ${B_SD} keys/s (n=${ITER})"
progress "${PHASE_B_LINE}"
progress "Phase B / Phase A@t=${T_REF}: ratio=${scale_ratio}x -> ${verdict}"

# ---------------------------------------------------------------------------
# Phase C — apply-thread profile (one representative run)
# ---------------------------------------------------------------------------
progress "=== Phase C: jstack/CPU under sustained load ==="
progress "NOTE: freon runs in its own container (managed or custom); top -H on OM will not see freon threads."
LOAD_LOG="${OUT}/loadC.log"
run_omkg bucket1 "${T_REF}" "$((N * 3))" loadC "${LOAD_LOG}" &
LOAD_PID=$!
progress "Background load PID=${LOAD_PID}; sleeping 8s to ramp..."
sleep 8

OM_JSTACK_PID=1
if ! invoke "${OM_JSTACK}" 1 >/dev/null 2>&1; then
  OM_JPS_CMD=$(printf '%s\n' "${OM_JSTACK}" | sed 's/[[:space:]]jstack$/ jps/; s/jstack$/jps/')
  OM_JSTACK_PID=$(invoke "${OM_JPS_CMD}" 2>/dev/null | awk '/OzoneManager|om/ { print $1; exit }')
  if [ -z "${OM_JSTACK_PID:-}" ]; then
    OM_JSTACK_PID=$(invoke "${OM_JPS_CMD}" 2>/dev/null | awk 'NR==1 { print $1 }')
  fi
  progress "jstack pid 1 failed; using pid ${OM_JSTACK_PID:-unknown}"
fi

i=1
while [ "$i" -le 5 ]; do
  progress "Sample ${i}/5: OM_TOP + OM_JSTACK"
  invoke "${OM_TOP}" >"${OUT}/top_H_${i}.txt" 2>&1 || true
  if [ -n "${OM_JSTACK_PID:-}" ]; then
    invoke "${OM_JSTACK}" "${OM_JSTACK_PID}" >"${OUT}/jstack_${i}.txt" 2>&1 \
      || invoke "${OM_JSTACK}" 1 >"${OUT}/jstack_${i}.txt" 2>&1 \
      || true
  else
    invoke "${OM_JSTACK}" 1 >"${OUT}/jstack_${i}.txt" 2>&1 || true
  fi
  i=$((i + 1))
  if [ "$i" -le 5 ]; then
    sleep 2
  fi
done

progress "Waiting for background load PID=${LOAD_PID} ..."
wait "${LOAD_PID}" || true
LOAD_RATE=$(keys_per_sec "${LOAD_LOG}")
csv_row "C" "sustained load t=${T_REF} n=$((N * 3))" "1" "${LOAD_RATE}"
progress "Phase C load finished -> ${LOAD_RATE} keys/s"

# Cross-ref hottest TIDs from top -H with OM jstack (decimal nid / [TID])
HOT_ANALYSIS="${OUT}/hot_threads.txt"
: > "${HOT_ANALYSIS}"
{
  echo "Hottest native TIDs by %CPU across top -H samples:"
  awk '
    $1 ~ /^[0-9]+$/ && NF >= 9 {
      tid = $1; cpu = $(NF - 3) + 0
      if (cpu > max[tid] + 0) max[tid] = cpu
    }
    END {
      for (t in max) printf "%s %.1f\n", t, max[t]
    }
  ' "${OUT}"/top_H_*.txt 2>/dev/null \
    | sort -k2 -nr \
    | head -n 12
} >> "${HOT_ANALYSIS}"

{
  echo ""
  echo "Cross-reference to OM jstack thread names (nid = decimal TID, or [TID] bracket):"
} >> "${HOT_ANALYSIS}"

HOT_NAMES=""
APPLY_NAMES=""
RPC_NAMES=""
OTHER_NAMES=""
HOTTEST_OM_NAME=""
HOTTEST_OM_CPU=""
APPLY_NEAR_TOP_NAME=""
APPLY_NEAR_TOP_CPU=""
APPLY_NEAR_TOP=0
SKIPPED_NON_OM=0
RESOLVED_COUNT=0
while read -r tid cpu; do
  [ -z "${tid:-}" ] && continue
  # This JDK's jstack: "name" #NN [TID] ... nid=DECIMAL ... (nid equals OS TID from top -H)
  name=$(grep -h -E "nid=${tid}[^0-9]|\\[${tid}\\]" "${OUT}"/jstack_*.txt 2>/dev/null \
    | head -n1 \
    | sed -E 's/^"([^"]+)".*/\1/' \
    || true)
  if [ -z "${name:-}" ]; then
    SKIPPED_NON_OM=$((SKIPPED_NON_OM + 1))
    echo "  TID=${tid} cpu=${cpu}% -> (skipped non-OM; no match in OM jstack)" >> "${HOT_ANALYSIS}"
    continue
  fi
  if [ "${RESOLVED_COUNT}" -ge 8 ]; then
    continue
  fi
  RESOLVED_COUNT=$((RESOLVED_COUNT + 1))
  line="  TID=${tid} cpu=${cpu}% nid=${tid} -> ${name}"
  echo "${line}" >> "${HOT_ANALYSIS}"
  HOT_NAMES="${HOT_NAMES}${name} (${cpu}%); "
  if [ "${RESOLVED_COUNT}" -eq 1 ]; then
    HOTTEST_OM_NAME="${name}"
    HOTTEST_OM_CPU="${cpu}"
  fi
  if is_apply_evidence "${name}"; then
    APPLY_NAMES="${APPLY_NAMES}${name} (${cpu}%); "
    if [ "${RESOLVED_COUNT}" -le 2 ]; then
      APPLY_NEAR_TOP=1
      if [ -z "${APPLY_NEAR_TOP_NAME}" ]; then
        APPLY_NEAR_TOP_NAME="${name}"
        APPLY_NEAR_TOP_CPU="${cpu}"
      fi
    fi
  elif is_rpc_intake "${name}"; then
    RPC_NAMES="${RPC_NAMES}${name} (${cpu}%); "
  else
    OTHER_NAMES="${OTHER_NAMES}${name} (${cpu}%); "
  fi
done < <(awk '
  $1 ~ /^[0-9]+$/ && NF >= 9 {
    tid = $1; cpu = $(NF - 3) + 0
    if (cpu > max[tid] + 0) max[tid] = cpu
  }
  END {
    for (t in max) printf "%s %.1f\n", t, max[t]
  }
' "${OUT}"/top_H_*.txt 2>/dev/null | sort -k2 -nr | head -n 24)

{
  echo ""
  echo "Apply/commit path (the bottleneck under test):"
  if [ -n "${APPLY_NAMES:-}" ]; then
    echo "  ${APPLY_NAMES}"
  else
    echo "  (none matched in top OM samples)"
  fi
  echo "RPC intake (expected busy, NOT the bottleneck):"
  if [ -n "${RPC_NAMES:-}" ]; then
    echo "  ${RPC_NAMES}"
  else
    echo "  (none matched in top OM samples)"
  fi
  echo "Other:"
  if [ -n "${OTHER_NAMES:-}" ]; then
    echo "  ${OTHER_NAMES}"
  else
    echo "  (none)"
  fi
  echo ""
  echo "Hottest OM thread overall: ${HOTTEST_OM_NAME:-none} (${HOTTEST_OM_CPU:-n/a}%)"
  if [ "${APPLY_NEAR_TOP}" -eq 1 ]; then
    echo "APPLY-evidence in top-2 OM threads: yes — ${APPLY_NEAR_TOP_NAME} (${APPLY_NEAR_TOP_CPU}%)"
  else
    echo "APPLY-evidence in top-2 OM threads: no"
  fi
  echo ""
  echo "Skipped ${SKIPPED_NON_OM} hot TID(s) as non-OM (no jstack match)."
  echo "NOTE: freon in its own container keeps top -H OM-only."
} >> "${HOT_ANALYSIS}"

progress "Hot thread analysis written to ${HOT_ANALYSIS}"
if [ -n "${APPLY_NAMES:-}" ]; then
  progress "Apply/commit path threads: ${APPLY_NAMES}"
else
  progress "No APPLY-evidence thread names matched in top OM samples"
fi
if [ -n "${RPC_NAMES:-}" ]; then
  progress "RPC intake threads (not bottleneck evidence): ${RPC_NAMES}"
fi

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
progress "=== Writing final summary ==="

conclusion="INCONCLUSIVE — inspect ${OUT} for details"
case "${verdict}" in
  DID\ NOT\ SCALE*)
    if [ "${APPLY_NEAR_TOP}" -eq 1 ]; then
      conclusion="REPRODUCED: multi-bucket OBS create-key did not scale (~${scale_ratio}x not ~${BUCKETS}x) and APPLY-evidence thread is #1/#2 hottest OM CPU (${APPLY_NEAR_TOP_NAME} at ${APPLY_NEAR_TOP_CPU}%) — bottleneck is single-threaded apply executor, not striped BUCKET_LOCK."
    else
      conclusion="PARTIAL: multi-bucket did not scale (ratio=${scale_ratio}x), but the apply thread was not saturated in the Phase C sample (hottest was ${HOTTEST_OM_NAME:-unknown} at ${HOTTEST_OM_CPU:-n/a}%); increase N or THREADS to saturate the apply executor."
    fi
    ;;
  SCALED*)
    conclusion="NOT REPRODUCED: ${BUCKETS}-bucket spread scaled (~${scale_ratio}x), which argues against a single shared apply-thread bottleneck under this config."
    ;;
esac

{
  echo "======== OM write bench summary (HDDS-11898) ========"
  echo "N=${N} THREADS_LIST=${THREADS_LIST} T_ref=${T_REF} BUCKETS=${BUCKETS} ITER=${ITER} VOL=${VOL} OM=${OM}"
  echo "Throughput is host-wall-clock based (python3 ms timer + Successful executions)."
  echo "FREON_MODE=${FREON_MODE} FREON_RUN=${FREON_RUN}"
  echo ""
  echo "phase | config | mean +/- stddev keys/s"
  echo "------+--------+------------------------"
  printf '%s' "${PHASE_A_SUMMARY_LINES}"
  echo "${PHASE_B_LINE}"
  echo "C | sustained load t=${T_REF} n=$((N * 3)) | ${LOAD_RATE} keys/s (one run)"
  echo ""
  echo "Single-bucket @ t=${T_REF}: ${SINGLE_TREF_MEAN} keys/s"
  echo "${BUCKETS}-bucket spread: ${B_MEAN} +/- ${B_SD} keys/s, ratio=${scale_ratio}x"
  echo "Verdict: ${verdict}"
  echo ""
  echo "Top OM threads by CPU (see ${HOT_ANALYSIS}):"
  echo "  Apply/commit path (the bottleneck under test): ${APPLY_NAMES:-none}"
  echo "  RPC intake (expected busy, NOT the bottleneck): ${RPC_NAMES:-none}"
  echo "  Other: ${OTHER_NAMES:-none}"
  echo "  ALL HOT: ${HOT_NAMES}"
  echo ""
  echo "CONCLUSION: ${conclusion}"
  echo "====================================================="
} | tee -a "${SUMMARY}" >&2

progress "Done. results.csv=${CSV} summary.txt=${SUMMARY}"
printf 'CONCLUSION: %s\n' "${conclusion}"
exit 0
