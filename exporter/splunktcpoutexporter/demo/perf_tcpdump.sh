#!/usr/bin/env bash
# perf_tcpdump.sh — tcpout throughput + memory benchmark using tcpdump for byte counting
#
# Metrics:
#   Wire throughput  : tcpdump pcap bytes ÷ active window (first→last packet to port 9997)
#   Peak RSS         : VmHWM from /proc/PID/status (high-water mark of resident pages)
#   Peak PSS         : sampled from /proc/PID/smaps_rollup at peak RSS instant
#   Active window    : first → last packet captured to dst port 9997
#
# Usage:
#   ./perf_tcpdump.sh [100k|1mb|both]   (default: both)
#
# Requires: sudo tcpdump, python3, bc
# Build the perf binary first:
#   FW_DIR=/home/chli/main/src/framework_cabi
#   SPLUNK_HOME=/home/chli/splunk_home
#   CGO_LDFLAGS="-L${FW_DIR} -lsplunk_cabi -Wl,-rpath,${FW_DIR} -Wl,-rpath,${SPLUNK_HOME}/lib -lstdc++ -ldl -lpthread" \
#   /home/chli/go/bin/builder --config builder-config-perf.yaml

set -euo pipefail

DEMO="$(cd "$(dirname "$0")" && pwd)"
BIN="${DEMO}/bin-perf/splunk-col-perf"
IFACE="ens5"
INDEXER_IP="10.236.40.125"
INDEXER_PORT="9997"
FW_DIR="/home/chli/main/src/framework_cabi"
SPLUNK_HOME_LIB="/home/chli/splunk_home/lib"
SPLUNK_HOME="/home/chli/splunk_home"

# ── pcap analysis: total bytes, first/last timestamps, computed throughput ──────
analyse_pcap() {
    local PCAP="$1"
    python3 - "$PCAP" <<'EOF'
import struct, sys

path = sys.argv[1]
with open(path, 'rb') as f:
    raw = f.read(4)
    magic = struct.unpack('<I', raw)[0]
    bo = '<' if magic in (0xa1b2c3d4, 0xa1b23c4d) else '>'
    # re-read full global header
    f.seek(0)
    gh = f.read(24)
    total_bytes = 0
    first_ts = last_ts = None
    while True:
        h = f.read(16)
        if len(h) < 16:
            break
        ts_s, ts_us, cl, ol = struct.unpack(bo + 'IIII', h)
        ts = ts_s + ts_us / 1e6
        total_bytes += cl
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        f.seek(cl, 1)

if first_ts is None or last_ts is None or last_ts <= first_ts:
    print("PCAP_MB=0 PCAP_DUR=0 PCAP_MBPS=0")
else:
    dur = last_ts - first_ts
    mbps = total_bytes / dur / 1024 / 1024
    print(f"PCAP_MB={total_bytes/1024/1024:.1f} PCAP_DUR={dur:.3f} PCAP_MBPS={mbps:.1f}")
EOF
}

# ── single benchmark run ─────────────────────────────────────────────────────────
run_bench() {
    local NAME="$1"
    local CONFIG="$2"
    local IDLE_MS="${3:-5000}"    # ms of TX silence → declare done
    local PCAP="/tmp/perf_${NAME}.pcap"

    echo ""
    echo "══════════════════════════════════════════════════"
    echo " $NAME"
    echo "══════════════════════════════════════════════════"

    # clear fishbucket so filelog starts from beginning
    rm -f "${SPLUNK_HOME}/var/lib/splunk/fishbucket/splunk_private_db/"* 2>/dev/null || true

    # start tcpdump (sudo)
    rm -f "$PCAP"
    sudo tcpdump -i "$IFACE" -w "$PCAP" \
        "tcp and host ${INDEXER_IP} and port ${INDEXER_PORT}" \
        >/dev/null 2>&1 &
    TCPDUMP_PID=$!
    sleep 0.3   # give tcpdump time to open the socket

    # start collector
    local T0_NS; T0_NS=$(date +%s%N)
    LD_LIBRARY_PATH="${FW_DIR}:${SPLUNK_HOME_LIB}" \
    SPLUNK_HOME="${SPLUNK_HOME}" \
        "${BIN}" --config "${DEMO}/${CONFIG}" \
        >/tmp/perf_col_"${NAME}".log 2>&1 &
    COL_PID=$!

    echo " Collector PID: ${COL_PID}  tcpdump PID: ${TCPDUMP_PID}"
    sleep 1  # let framework init complete

    # CPU jiffies at start (fields 14=utime 15=stime in /proc/PID/stat)
    local CPU0; CPU0=$(awk '{print $14+$15}' /proc/"${COL_PID}"/stat 2>/dev/null || echo 0)

    # ── monitoring loop ──────────────────────────────────────────────────────
    local MAX_RSS=0 PEAK_PSS=0 PSS_AT_PEAK=0
    local LAST_TX; LAST_TX=$(awk '/ens5:/{print $10}' /proc/net/dev)
    local FIRST_NS="" LAST_ACTIVE_NS="" SAMPLE=0 LAST_CPU_J=$CPU0

    while kill -0 "$COL_PID" 2>/dev/null; do
        sleep 0.5
        SAMPLE=$((SAMPLE+1))

        local TX; TX=$(awk '/ens5:/{print $10}' /proc/net/dev)
        local DELTA=$(( TX - LAST_TX )); LAST_TX=$TX
        local NOW_NS; NOW_NS=$(date +%s%N)

        # CPU jiffies (snapshot)
        LAST_CPU_J=$(awk '{print $14+$15}' /proc/"${COL_PID}"/stat 2>/dev/null || echo "$LAST_CPU_J")

        # RSS
        local RSS; RSS=$(awk '/VmRSS/{print $2}' /proc/"${COL_PID}"/status 2>/dev/null || echo 0)
        if [[ $RSS -gt $MAX_RSS ]]; then
            MAX_RSS=$RSS
            # sample PSS at peak RSS
            PSS_AT_PEAK=$(awk '/^Pss:/{sum+=$2} END{print sum}' \
                /proc/"${COL_PID}"/smaps_rollup 2>/dev/null || echo 0)
        fi

        # active-window tracking (>500 KB delta = actively sending)
        if [[ $DELTA -gt 512000 ]]; then
            [[ -z $FIRST_NS ]] && FIRST_NS=$NOW_NS
            LAST_ACTIVE_NS=$NOW_NS
            local ELAPSED_MS=$(( (NOW_NS - T0_NS) / 1000000 ))
            local DMIB; DMIB=$(echo "scale=1; $DELTA / 1048576" | bc)
            printf "  T+%5dms  Δ=%5s MB  RSS=%6d KB  PSS=%6d KB\n" \
                "$ELAPSED_MS" "$DMIB" "$RSS" "$PSS_AT_PEAK"
        fi

        # idle detection: N ms with no significant TX → done
        if [[ -n $LAST_ACTIVE_NS ]]; then
            local IDLE_NOW=$(( (NOW_NS - LAST_ACTIVE_NS) / 1000000 ))
            if [[ $IDLE_NOW -gt $IDLE_MS ]]; then
                echo "  (idle ${IDLE_NOW}ms — stopping collector)"
                kill -TERM "$COL_PID" 2>/dev/null || true
                sleep 1
                kill -KILL "$COL_PID" 2>/dev/null || true
                break
            fi
        fi
    done

    wait "$COL_PID" 2>/dev/null || true

    # stop tcpdump — give it a moment to flush the pcap
    sleep 0.5
    sudo kill -INT "$TCPDUMP_PID" 2>/dev/null || true
    wait "$TCPDUMP_PID" 2>/dev/null || true
    sleep 0.3

    # final CPU snapshot after process ends
    local CPU_S; CPU_S=$(echo "scale=1; ($LAST_CPU_J - $CPU0) / 100" | bc)

    # ── report ───────────────────────────────────────────────────────────────
    local WIN_MS=0
    if [[ -n $FIRST_NS && -n $LAST_ACTIVE_NS && $LAST_ACTIVE_NS -gt $FIRST_NS ]]; then
        WIN_MS=$(( (LAST_ACTIVE_NS - FIRST_NS + 500000) / 1000000 ))
    fi

    # VmHWM = kernel-tracked peak RSS (more reliable than our sampled max)
    local HWM_KB; HWM_KB=$(grep VmHWM /tmp/perf_col_"${NAME}".log 2>/dev/null | awk '{print $2}' || echo 0)
    # If not in log, use our sampled max
    [[ ${HWM_KB:-0} -eq 0 ]] && HWM_KB=$MAX_RSS

    # pcap analysis
    local PCAP_RESULT; PCAP_RESULT=$(analyse_pcap "$PCAP" 2>/dev/null || echo "PCAP_MB=0 PCAP_DUR=0 PCAP_MBPS=0")
    eval "$PCAP_RESULT"

    echo ""
    echo " ┌─────────────────────────────────────────────┐"
    printf " │  Wire throughput  : %6s MB/s (tcpdump)   │\n" "${PCAP_MBPS}"
    printf " │  Payload captured : %6s MB               │\n" "${PCAP_MB}"
    printf " │  Active window    : %6s ms               │\n" "${WIN_MS}"
    printf " │  CPU time         : %6s s                │\n" "${CPU_S}"
    printf " │  Peak RSS (sampled): %5d MB              │\n" "$(( MAX_RSS / 1024 ))"
    printf " │  Peak PSS (sampled): %5d MB              │\n" "$(( PSS_AT_PEAK / 1024 ))"
    echo " └─────────────────────────────────────────────┘"
    echo " pcap: ${PCAP}"
}

# ── main ────────────────────────────────────────────────────────────────────────
MODE="${1:-both}"

echo "Binary: ${BIN}"
echo "Interface: ${IFACE}   Indexer: ${INDEXER_IP}:${INDEXER_PORT}"
echo "Date: $(date -u)"

case "$MODE" in
    100k|100K)
        run_bench "100k_1kb" "config_perf_fw.yaml" 5000
        ;;
    1mb|1MB)
        run_bench "1000_1mb" "config_perf_1mb_fw.yaml" 8000
        ;;
    both|*)
        run_bench "100k_1kb" "config_perf_fw.yaml" 5000
        run_bench "1000_1mb" "config_perf_1mb_fw.yaml" 8000
        ;;
esac
