#!/usr/bin/env bash
# Parameter sweep for webhook claim/finalize load.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.webhook-load.yml)
RESULTS=deploy/webhook-load-results
mkdir -p "$RESULTS"

run_one() {
  local label="$1" agents="$2" batch="$3" poll="$4" deliveries="${5:-200}"
  echo "======== RUN $label agents=$agents batch=$batch poll=$poll per_tenant=$deliveries ========"
  # Scale webhook agents 1-8 via profiles is hard; we always have 4 services.
  # For higher agent counts we reuse the 4 services with higher batch.
  # Override env on the fly.
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null
  BATCH_LIMIT="$batch" POLL_INTERVAL="$poll" \
  LOAD_LABEL="$label" \
  DELIVERIES_PER_TENANT="$deliveries" \
  RESULTS_PATH="/results/${label}.json" \
  "${COMPOSE[@]}" up --build --abort-on-container-exit \
    --exit-code-from webhook-load-soak \
    -e webhook-1.BATCH_LIMIT="$batch" \
    webhook-load-soak 2>&1 | tee "/tmp/${label}.log" | tail -30
}

# docker compose doesn't support -e on service that way easily; use env file overlay
run_cfg() {
  local label="$1" batch="$2" poll="$3" per="${4:-200}"
  cat > /tmp/wh-load-override.yml <<YML
services:
  webhook-1:
    environment:
      BATCH_LIMIT: "$batch"
      POLL_INTERVAL: "$poll"
  webhook-2:
    environment:
      BATCH_LIMIT: "$batch"
      POLL_INTERVAL: "$poll"
  webhook-3:
    environment:
      BATCH_LIMIT: "$batch"
      POLL_INTERVAL: "$poll"
  webhook-4:
    environment:
      BATCH_LIMIT: "$batch"
      POLL_INTERVAL: "$poll"
  postgres:
    command:
      - postgres
      - -c
      - max_connections=300
      - -c
      - shared_buffers=256MB
      - -c
      - work_mem=8MB
      - -c
      - synchronous_commit=off
  webhook-load-soak:
    environment:
      LOAD_LABEL: "$label"
      DELIVERIES_PER_TENANT: "$per"
      RESULTS_PATH: "/results/${label}.json"
      MAX_WALL_SECONDS: "300"
YML
  echo "======== $label batch=$batch poll=$poll per=$per ========"
  docker compose -f docker-compose.yml -f docker-compose.webhook-load.yml \
    -f /tmp/wh-load-override.yml down -v --remove-orphans >/dev/null
  docker compose -f docker-compose.yml -f docker-compose.webhook-load.yml \
    -f /tmp/wh-load-override.yml up --build --abort-on-container-exit \
    --exit-code-from webhook-load-soak webhook-load-soak 2>&1 \
    | tee "/tmp/${label}.log" | grep -E "RESULT|WEBHOOK LOAD|ERROR|error|seed complete|drain t=" | tail -40
}

# Builds once via first run
run_cfg "sweep-b50-p05" 50 0.05 200
run_cfg "sweep-b100-p05" 100 0.05 200
run_cfg "sweep-b200-p02" 200 0.02 200
run_cfg "sweep-b200-p02-8k" 200 0.02 400
run_cfg "sweep-b50-p05-8k" 50 0.05 400

echo "======== SWEEP DONE ========"
ls -la "$RESULTS"
python3 - <<'PY'
import json, glob
rows=[]
for p in sorted(glob.glob("deploy/webhook-load-results/*.json")):
    try:
        d=json.load(open(p))
    except Exception:
        continue
    rows.append({
        "file": p.split("/")[-1],
        "label": d.get("label"),
        "ok": d.get("ok"),
        "total": d.get("total") or d.get("seeded"),
        "drain_s": d.get("drain_seconds"),
        "rate": d.get("drain_deliveries_per_sec"),
    })
rows.sort(key=lambda r: (r.get("rate") or 0), reverse=True)
print(json.dumps(rows, indent=2))
json.dump({"runs": rows, "peak": rows[0] if rows else None}, open("deploy/webhook-load-results/comparison.json","w"), indent=2)
PY
