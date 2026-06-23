#!/usr/bin/env python3
"""Recompute balance/status-derived stats fields for historical snapshots."""

from __future__ import annotations

import argparse
from datetime import date

import collect_daily_stats as cds


FIELDS = (
    'active_minipools',
    'exited_minipools',
    'total_minipools',
    'below_31_9_eth',
    'megapool_node_count',
    'megapool_validator_count',
    'start_epoch',
    'end_epoch',
    'epochs_analyzed',
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date-from', type=date.fromisoformat, default=None)
    parser.add_argument('--date-to', type=date.fromisoformat, default=None)
    parser.add_argument('--limit', type=int, default=0)
    return parser.parse_args()


def rebuild_balance_snapshot(client, target_date, val_id_map, megapool_scan_data):
    """Recompute balance/status fields without rerunning 7-day performance queries."""
    end_epoch = cds.date_to_end_epoch(target_date)
    start_epoch = end_epoch - cds.ANALYSIS_PERIOD_EPOCHS + 1

    latest_epoch = cds.get_latest_epoch(client)
    if end_epoch > latest_epoch:
        cds.log(f'  Skipped {target_date}: end epoch {end_epoch} not yet available (latest: {latest_epoch})')
        return None

    validator_snapshot = cds.get_validator_snapshot(client, val_id_map, end_epoch)

    active_minipools = sum(
        1 for row in validator_snapshot.values()
        if row['status'] in cds.ACTIVE_STATUSES
    )
    exited_minipools = sum(
        1 for row in validator_snapshot.values()
        if row['status'] in cds.EXITED_STATUSES
    )
    below_31_9_eth = sum(
        1 for row in validator_snapshot.values()
        if row['status'] in cds.ACTIVE_STATUSES and 0 < (row['balance'] / 1_000_000_000) < 31.9
    )

    return {
        'active_minipools': active_minipools,
        'exited_minipools': exited_minipools,
        'total_minipools': active_minipools + exited_minipools,
        'below_31_9_eth': below_31_9_eth,
        'megapool_node_count': len(megapool_scan_data),
        'megapool_validator_count': sum(n.get('megapool_validator_count', 0) for n in megapool_scan_data),
        'start_epoch': start_epoch,
        'end_epoch': end_epoch,
        'epochs_analyzed': cds.ANALYSIS_PERIOD_EPOCHS,
        'timestamp': cds.datetime.now().isoformat(),
        'collection_note': 'Historical balance/status backfill using active-validator snapshot logic',
    }


def main() -> None:
    args = parse_args()
    stats_history = cds.load_stats_history()
    megapool_scan_data = cds.load_megapool_scan_results()
    scan_data = cds.load_scan_results()
    validators = cds.get_validator_list_from_scan(scan_data, megapool_scan_data)
    client = cds.connect_to_clickhouse()
    val_id_map = cds.get_validator_ids(client, validators)

    snapshots = stats_history.get('snapshots', [])
    targets = []
    for snapshot in snapshots:
        snapshot_date = date.fromisoformat(snapshot['date'])
        if args.date_from and snapshot_date < args.date_from:
            continue
        if args.date_to and snapshot_date > args.date_to:
            continue
        targets.append(snapshot)

    if args.limit and args.limit > 0:
        targets = targets[:args.limit]

    cds.log(f'Recomputing balance metrics for {len(targets)} snapshots')
    updated = 0
    for snapshot in targets:
        target_date = date.fromisoformat(snapshot['date'])
        cds.log(f'[{updated + 1}/{len(targets)}] Recomputing {target_date}')
        rebuilt = rebuild_balance_snapshot(client, target_date, val_id_map, megapool_scan_data)
        if rebuilt is None:
            cds.log(f'  Skipped {target_date} (data unavailable)')
            continue
        for field in FIELDS:
            snapshot[field] = rebuilt[field]
        snapshot['timestamp'] = rebuilt['timestamp']
        snapshot['collection_note'] = rebuilt['collection_note']
        updated += 1

    if updated:
        stats_history['metadata']['last_updated'] = cds.datetime.now().isoformat()
        cds.save_stats_history(stats_history)
    cds.log(f'Updated {updated} snapshots')


if __name__ == '__main__':
    main()
