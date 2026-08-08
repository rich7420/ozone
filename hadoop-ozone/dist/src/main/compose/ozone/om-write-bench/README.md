<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# OM write-path benchmark (HDDS-11898)

This suite measures create-key throughput on the classic-Ratis Ozone Manager write
path. The HDDS-11898 hypothesis is that spreading OBS (`OBJECT_STORE`) create-key
load across many buckets does **not** scale throughput: with one writer the
striped `BUCKET_LOCK` is uncontended, and the CPU-bound ceiling is the
single-threaded apply executor (`OMStateMachineApplyTransactionThread`). Phase A
sweeps single-bucket concurrency; Phase B spreads the same total concurrency
across many buckets and reports the scale ratio; Phase C profiles OM thread CPU
under load to show which threads dominate.

## Prerequisites

- An already-running Ozone cluster (this script does not start or stop Docker).
- For the default compose path: this distribution's `compose/ozone` cluster is up
  (for example `OZONE_REPLICATION_FACTOR=3 ./run.sh -d` from `compose/ozone`).
- `python3` on the host that runs the script (used only for millisecond wall-clock
  timing).

## Quick start (compose)

From the compose project directory:

```bash
cd compose/ozone
./om-write-bench/om-write-bench.sh
```

Common knobs (environment variables):

```bash
N=40000 \
THREADS_LIST="16 64 128" \
ITER=3 \
BUCKETS=8 \
./om-write-bench/om-write-bench.sh
```

By default (no `FREON_RUN` set) the script runs in **managed** mode: it starts one
long-lived Freon container (`ozone-bench-freon`, from `om-write-bench/freon-omkg.yaml`)
in preflight and `docker exec`s every Freon call into it, then removes it on exit via
a trap. This keeps load generation off the OM container's memory and out of OM
`top -H` samples, and — because the container starts once — avoids charging per-run
container/JVM startup into the measured throughput. Control-plane commands (`ozone sh`)
and JVM sampling default to `docker exec` against `OM` (default `ozone-om-1`). Override
`FREON_CONTAINER_NAME` if `ozone-bench-freon` collides.

## Running against a real (non-compose) cluster

Setting `FREON_RUN` switches the script to **custom** mode: it does no container
lifecycle and uses your command verbatim. Also set `OZONE_ENV=custom` so it does not
`cd` into the compose directory. Override the injectable commands so Freon, admin CLI,
and OM sampling reach your cluster. Example against an edge node / SSH to the OM host:

```bash
export OZONE_ENV=custom
export FREON_RUN="ozone"   # or: ssh edge-node ozone
export OZONE_ADMIN="ozone" # or: ssh edge-node ozone
export OM_TOP="ssh om-host.example top -Hbn1"
export OM_JSTACK="ssh om-host.example jstack"
export OUT=./om-write-bench-out-prod

./om-write-bench/om-write-bench.sh
```

`OM_JSTACK` must accept a JVM pid as its final argument (the script appends it).
If `jstack 1` fails, the script derives a `jps` command from `OM_JSTACK` (replacing
the trailing `jstack` with `jps`) to discover the Ozone Manager pid.

## Reading the results

Output directory (`OUT`, default `./om-write-bench-out`):

| File | Contents |
|------|----------|
| `results.csv` | Per-iteration rows: `phase,config,iter,keys_per_sec` |
| `summary.txt` | Human-readable table with mean +/- sample stddev per config, scale verdict, and top OM threads |
| `hot_threads.txt` | Phase C TID/%CPU cross-referenced to jstack names (decimal `nid`) |

Throughput is **not** Freon's reported mean rate. Each run records host wall-clock
milliseconds (`python3`) and divides actual `Successful executions` by that
elapsed time.

**Did not scale** means Phase B mean throughput is far below ~`BUCKETS` times the
Phase A single-bucket mean at the middle `THREADS_LIST` value (`T_ref`). That argues
against a bucket-lock bottleneck.

Phase C groups the hottest OM threads into three buckets: the **apply/commit path**
(`OMStateMachineApplyTransactionThread`, `OMDoubleBufferFlushThread`,
`SegmentedRaftLogWorker`, `OMExecutionFlow`) — the bottleneck under test; **RPC intake**
(`IPC Server handler`, `pool-*`) — expected to be busy but *not* the bottleneck; and
**other**. The `CONCLUSION` is deliberately strict:

- `REPRODUCED` only when Phase B did not scale **and** an apply/commit thread is the #1
  or #2 hottest OM thread (its name and %CPU are printed).
- `PARTIAL` when Phase B did not scale but the apply thread was not saturated in the
  sample — typically because the sustained load was too light. Raise `N` / `THREADS_LIST`
  so the single apply thread becomes the ceiling. Note that IPC handlers being hot on
  their own is only RPC intake, not evidence of the apply bottleneck.

## Caveats

- There is a tension between the two phases on a small host: Phase C needs a large
  enough `N`/`THREADS_LIST` to saturate the single apply thread, but its sustained load
  (`n = N * 3`) can OOM-kill the OM on a small VM. On a laptop-sized Docker VM,
  `N` around 40000 saturates the apply thread while `n = N * 3` still fits; much larger
  and the OM may exit with code 137. On real cluster hardware, raise `N` freely.
- Keep Freon in its own container (the managed default). Running Freon inside the OM
  container pollutes `top -H` and shares OM heap.
- Absolute keys/s depends on hardware, heap, and disk. Compare the
  single-bucket vs multi-bucket **ratio** and the hot-thread breakdown across
  machines, not raw keys/s alone.
- Failed Freon iterations are recorded as `0` keys/s and noted on stderr; the
  suite continues so a single bad run does not abort the campaign.
