#!/usr/bin/env python3
"""
Backfill the per-pool-type split (minipool / megapool / combined) onto every
existing snapshot in stats_history.json.

For snapshots whose epochs are still retained in ClickHouse the split is
recomputed directly from the validator data. For older snapshots (outside the
ClickHouse retention window) the split is synthesised from the existing legacy
fields: minipools carry the historical totals and megapools are zero, which is
correct because megapool validators only began activating in mid-2026.

Usage:
    python3 backfill_pool_type_split.py            # fill any missing splits
    python3 backfill_pool_type_split.py --force    # recompute every snapshot
"""

import json
import shutil
import sys
from datetime import datetime

from collect_daily_stats import (
    EPOCHS_PER_DAY,
    STATS_FILE,
    log,
    connect_to_clickhouse,
    get_latest_epoch,
    load_scan_results,
    load_megapool_scan_results,
    get_validator_list_from_scan,
    get_validator_ids,
    get_val_id_pool_type,
    get_node_assignments,
    get_validator_snapshot,
    query_7day_performance,
    calculate_pool_type_split,
)

# ClickHouse keeps roughly 120 days of validator_summary history; snapshots older
# than this cannot be recomputed and are synthesised from legacy fields instead.
RETENTION_DAYS = 120

# Megapool validators only began activating on the beacon chain in mid-June 2026.
# Any snapshot before that has zero active megapools, so synthesising it from the
# legacy (minipool-only) fields is exact and avoids a slow ClickHouse requery.
# --recompute-days N narrows the requery window; the rest is synthesised.
DEFAULT_RECOMPUTE_DAYS = RETENTION_DAYS

# Bands carried straight across when synthesising a minipool block from legacy data.
LEGACY_BANDS = (
    'perf_band_0', 'perf_band_0_50', 'perf_band_50_80', 'perf_band_80_90',
    'perf_band_90_95', 'perf_band_95_99_5', 'perf_band_99_5_100',
)


def synthesise_split_from_legacy(snapshot):
    """Build a by_pool_type block from legacy flat fields (minipool-only era)."""
    minipool = {
        'active': snapshot.get('active_minipools', 0),
        'exited': snapshot.get('exited_minipools', 0),
        'total': snapshot.get('total_minipools', 0),
        'underperforming': snapshot.get('underperforming_minipools', 0),
        'zero_performance': snapshot.get('zero_performance_minipools', 0),
        'total_earned_gwei': snapshot.get('total_earned_rewards_gwei', 0),
        'total_missed_gwei': snapshot.get('total_missed_rewards_gwei', 0),
        'total_penalties_gwei': snapshot.get('total_penalties_gwei', 0),
        'total_lost_gwei': snapshot.get('total_lost_gwei', 0),
        'avg_performance_score': snapshot.get('avg_performance_score', 0),
        'below_31_9_eth': snapshot.get('below_31_9_eth', 0),
    }
    for band in LEGACY_BANDS:
        minipool[band] = snapshot.get(band, 0)

    megapool = {key: 0 for key in minipool}
    combined = dict(minipool)

    return {'minipool': minipool, 'megapool': megapool, 'combined': combined}


def recompute_split(client, snapshot, validators, val_id_map, val_id_pool_type):
    """Recompute the split directly from ClickHouse for a retained snapshot."""
    end_epoch = snapshot['end_epoch']
    start_epoch = snapshot['start_epoch']

    performance_data = query_7day_performance(client, val_id_map, end_epoch, start_epoch)
    node_assignments = get_node_assignments(validators, val_id_map)
    validator_snapshot = get_validator_snapshot(client, val_id_map, end_epoch)

    return calculate_pool_type_split(
        performance_data, node_assignments, validator_snapshot, val_id_pool_type
    )


def save_stats_history(stats_history):
    """Atomic save with a temporary backup, mirroring the other backfill scripts."""
    temp_file = STATS_FILE.with_suffix('.tmp')
    backup_file = STATS_FILE.with_suffix('.backup')

    try:
        if STATS_FILE.exists():
            shutil.copy2(STATS_FILE, backup_file)
            log(f"Created backup: {backup_file}")

        with open(temp_file, 'w') as f:
            json.dump(stats_history, f, indent=2)
        temp_file.rename(STATS_FILE)
        log(f"Stats history saved to {STATS_FILE}")

        if backup_file.exists():
            backup_file.unlink()
    except Exception as e:
        log(f"ERROR: Failed to save stats history: {e}")
        if temp_file.exists():
            temp_file.unlink()
        if backup_file.exists():
            backup_file.rename(STATS_FILE)
            log("Restored from backup")
        sys.exit(1)


def parse_recompute_days(argv):
    """Read --recompute-days N (default DEFAULT_RECOMPUTE_DAYS)."""
    for i, arg in enumerate(argv):
        if arg == '--recompute-days' and i + 1 < len(argv):
            return int(argv[i + 1])
        if arg.startswith('--recompute-days='):
            return int(arg.split('=', 1)[1])
    return DEFAULT_RECOMPUTE_DAYS


def main():
    force = '--force' in sys.argv
    recompute_days = parse_recompute_days(sys.argv)

    log("=" * 60)
    log("Starting per-pool-type split backfill")
    log("=" * 60)

    with open(STATS_FILE, 'r') as f:
        stats_history = json.load(f)
    snapshots = stats_history['snapshots']
    log(f"Loaded {len(snapshots)} snapshots from history")

    scan_data = load_scan_results()
    megapool_scan_data = load_megapool_scan_results()
    validators = get_validator_list_from_scan(scan_data, megapool_scan_data)

    client = connect_to_clickhouse()
    val_id_map = get_validator_ids(client, validators)
    val_id_pool_type = get_val_id_pool_type(validators, val_id_map)

    latest_epoch = get_latest_epoch(client)
    recompute_start_epoch = latest_epoch - recompute_days * EPOCHS_PER_DAY
    log(f"Recomputing snapshots from epoch {recompute_start_epoch} "
        f"(last {recompute_days} days); older snapshots synthesised from legacy fields")

    recomputed = 0
    synthesised = 0
    skipped = 0

    for i, snapshot in enumerate(snapshots, 1):
        snap_date = snapshot['date']
        if 'by_pool_type' in snapshot and not force:
            skipped += 1
            continue

        end_epoch = snapshot.get('end_epoch')
        if end_epoch is not None and end_epoch >= recompute_start_epoch:
            log(f"[{i}/{len(snapshots)}] Recomputing {snap_date} from ClickHouse...")
            try:
                snapshot['by_pool_type'] = recompute_split(
                    client, snapshot, validators, val_id_map, val_id_pool_type
                )
                recomputed += 1
            except Exception as e:
                log(f"  ERROR recomputing {snap_date}: {e} - synthesising from legacy")
                snapshot['by_pool_type'] = synthesise_split_from_legacy(snapshot)
                synthesised += 1
        else:
            log(f"[{i}/{len(snapshots)}] Synthesising {snap_date} from legacy fields...")
            snapshot['by_pool_type'] = synthesise_split_from_legacy(snapshot)
            synthesised += 1

    if recomputed or synthesised:
        stats_history['metadata']['last_updated'] = datetime.now().isoformat()
        stats_history['metadata']['pool_type_split_note'] = (
            f"Per-pool-type split backfilled on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        save_stats_history(stats_history)

    log("=" * 60)
    log(f"Backfill complete: {recomputed} recomputed, {synthesised} synthesised, {skipped} skipped")
    log("=" * 60)


if __name__ == '__main__':
    main()
