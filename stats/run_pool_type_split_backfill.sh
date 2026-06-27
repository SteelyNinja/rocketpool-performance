#!/bin/bash
# Backfill the per-pool-type split (minipool / megapool / combined) onto every
# existing snapshot in stats_history.json.
#
# Recent snapshots are recomputed from ClickHouse; older pre-megapool snapshots
# are synthesised from the existing legacy fields. Pass --force to recompute
# every snapshot regardless of whether it already carries a split.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PATH="$PROJECT_ROOT/venv"

cd "$SCRIPT_DIR"

if [ ! -d "$VENV_PATH" ]; then
    echo "ERROR: Virtual environment not found at $VENV_PATH"
    exit 1
fi

if [ ! -f "stats_history.json" ]; then
    echo "ERROR: stats_history.json not found in $SCRIPT_DIR"
    exit 1
fi

source "$VENV_PATH/bin/activate"

echo "Starting per-pool-type split backfill..."
python3 backfill_pool_type_split.py "$@" 2>&1 | tee pool_type_split_backfill.log

echo ""
echo "Backfill complete! Log saved to pool_type_split_backfill.log"
