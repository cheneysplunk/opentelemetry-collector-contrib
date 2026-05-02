#!/usr/bin/env bash
# build.sh — build the custom collector binary using ocb.
#
# Steps:
#   1. Install ocb (OpenTelemetry Collector Builder) if absent.
#   2. Run ocb with builder-config.yaml → outputs bin/splunktcpout-demo-col
#
# The splunktcpoutexporter uses CGo, so we must export CGO_* env vars so
# that ocb passes them through to the inner `go build` invocation.

set -euo pipefail
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
TCPOUT_LIB=/home/chli/main/src/output/tcpout_lib
SPLUNK_HOME_LIB=/home/chli/splunk_home/lib
OCB_VERSION=v0.151.0

# ── 1. Install ocb if needed ──────────────────────────────────────────────────
if ! command -v builder &>/dev/null; then
  echo "Installing ocb ${OCB_VERSION}…"
  go install go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}
fi

# ── 2. Export CGo flags so the inner build can link libtcpout_cabi.so ─────────
export CGO_CFLAGS="-I${TCPOUT_LIB}"
export CGO_LDFLAGS="-L${TCPOUT_LIB} -L${SPLUNK_HOME_LIB} \
  -Wl,-rpath,${TCPOUT_LIB} \
  -Wl,-rpath,${SPLUNK_HOME_LIB}"

# ── 3. Build ──────────────────────────────────────────────────────────────────
echo "Building splunktcpout-demo-col…"
builder --config "${DEMO_DIR}/builder-config.yaml" --skip-compilation=false

echo ""
echo "Binary: ${DEMO_DIR}/bin/splunktcpout-demo-col"
echo ""
echo "Run with:"
echo "  LD_LIBRARY_PATH=${TCPOUT_LIB}:${SPLUNK_HOME_LIB} \\"
echo "  SPLUNK_HOME=/home/chli/splunk_home \\"
echo "  ${DEMO_DIR}/bin/splunktcpout-demo-col --config ${DEMO_DIR}/config.yaml"
