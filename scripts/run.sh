#!/usr/bin/env bash
# scripts/run.sh
# Cron-friendly wrapper for one disaster-alerts pipeline execution (no locking).
# Usage:
#   ./scripts/run.sh --dry-run
#   ./scripts/run.sh --print-settings

set -euo pipefail

# --- Hardening / defaults ---
umask 027
export LANG=C.UTF-8 LC_ALL=C.UTF-8

# --- Resolve repo root & paths ---
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
export DISASTER_ALERTS_ROOT="${DISASTER_ALERTS_ROOT:-$ROOT}"
LOG_DIR="$ROOT/logs"
DATA_DIR="$ROOT/data"
mkdir -p "$LOG_DIR" "$DATA_DIR"

# --- Load .env if present (exports YAGMAIL_* etc.) ---
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ROOT/.env"
  set +a
fi

# --- Timestamped log (UTC) ---
TS_UTC="$(date -u +'%Y%m%dT%H%M%SZ')"
LOG_FILE="$LOG_DIR/run_${TS_UTC}.log"

# --- Runner helpers ---
ENV_NAME="disaster-alerts"

run_with_conda() {
  if command -v conda >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    eval "$(conda shell.bash hook)"
    if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
      conda run -n "$ENV_NAME" python -m disaster_alerts "$@"
      return $?
    fi
  fi
  return 1
}

run_with_system_python() {
  if command -v python >/dev/null 2>&1; then
    python -m disaster_alerts "$@"
    return $?
  fi
  return 1
}

# --- Execute with banners + tee to file ---
{
  echo "[$(date -u +'%F %TZ')] ===== disaster-alerts start ====="
  echo "root=$DISASTER_ALERTS_ROOT | args=$*"
  if [[ -n "${DISASTER_ALERTS_CONFIG_DIR:-}" ]]; then
    echo "config_dir=$DISASTER_ALERTS_CONFIG_DIR"
  fi

  set +e
  if run_with_conda "$@"; then
    EC=0
  elif run_with_system_python "$@"; then
    EC=0
  else
    echo "ERROR: No conda env '$ENV_NAME' or system 'python' found."
    echo "       Create the env with:  conda env create -f environment.yml"
    EC=1
  fi
  set -e

  echo "[$(date -u +'%F %TZ')] ===== disaster-alerts end (exit=$EC) ====="
  exit "$EC"
} 2>&1 | tee -a "$LOG_FILE"
