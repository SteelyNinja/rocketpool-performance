#!/usr/bin/env python3
"""
Independent stats collection - queries ClickHouse directly
Does NOT depend on existing reports or analysis scripts

Collects daily snapshot of Rocket Pool network statistics based on
7-day rolling window with 80% performance threshold.
"""

import clickhouse_connect
import json
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

# ClickHouse connection
CLICKHOUSE_HOST = '192.168.202.250'
CLICKHOUSE_PORT = 8123

# Analysis configuration
EPOCHS_PER_DAY = 225
ANALYSIS_PERIOD_EPOCHS = 1575  # 7 days
PERFORMANCE_THRESHOLD = 80.0  # 80% threshold

# File paths
SCRIPT_DIR = Path(__file__).parent
STATS_FILE = SCRIPT_DIR / 'stats_history.json'
FUSAKA_DEATHS_FILE = Path(__file__).parent.parent / 'fusaka_deaths.json'
SCAN_RESULTS_FILE = Path(__file__).parent.parent / 'reports' / 'rocketpool_scan_results.json'

# Fusaka hard fork epoch
FUSAKA_EPOCH = 411392


def log(message):
    """Print timestamped log message"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def connect_to_clickhouse():
    """Connect to ClickHouse database"""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT
        )
        log(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        return client
    except Exception as e:
        log(f"ERROR: Failed to connect to ClickHouse: {e}")
        sys.exit(1)


def get_latest_epoch(client):
    """Get latest epoch from validators_summary table"""
    try:
        result = client.query("SELECT MAX(epoch) FROM validators_summary")
        latest_epoch = result.result_rows[0][0]
        log(f"Latest epoch in database: {latest_epoch}")
        return latest_epoch
    except Exception as e:
        log(f"ERROR: Failed to get latest epoch: {e}")
        sys.exit(1)


def date_to_end_epoch(target_date):
    """
    Convert a date to the last epoch of that UTC day
    Returns the epoch number at 23:59:59 UTC of the given date
    """
    from datetime import datetime, time

    # Get timestamp for end of day (23:59:59 UTC)
    end_of_day = datetime.combine(target_date, time(23, 59, 59))
    target_timestamp = end_of_day.timestamp()

    # Genesis: Dec 1, 2020, 12:00:23 UTC
    genesis_timestamp = 1606824023

    # Calculate epoch number (each epoch = 32 slots × 12 seconds = 384 seconds)
    seconds_since_genesis = target_timestamp - genesis_timestamp
    epoch = int(seconds_since_genesis / 384)

    return epoch


def get_previous_complete_day():
    """
    Get the most recent complete UTC day
    If run on Jan 30, returns Jan 29
    """
    from datetime import date, timedelta

    # Yesterday is the most recent complete day
    yesterday = date.today() - timedelta(days=1)
    return yesterday


def load_scan_results():
    """Load Rocket Pool scan results to get validator list"""
    if not SCAN_RESULTS_FILE.exists():
        log(f"ERROR: Scan results file not found: {SCAN_RESULTS_FILE}")
        sys.exit(1)

    try:
        with open(SCAN_RESULTS_FILE, 'r') as f:
            data = json.load(f)
            # Scan results is a list of nodes
            if isinstance(data, list):
                log(f"Loaded scan results with {len(data)} nodes")
                return data
            else:
                log(f"ERROR: Unexpected scan results format")
                sys.exit(1)
    except Exception as e:
        log(f"ERROR: Failed to load scan results: {e}")
        sys.exit(1)


def get_validator_list_from_scan(scan_data):
    """Extract validator list from scan results"""
    validators = []

    for node in scan_data:
        node_address = node.get('node_address')
        minipools = node.get('minipool_addresses', [])
        pubkeys = node.get('minipool_pubkeys', [])  # Note: key is 'minipool_pubkeys'

        for i, pubkey in enumerate(pubkeys):
            validators.append({
                'node_address': node_address,
                'minipool_address': minipools[i] if i < len(minipools) else None,
                'pubkey': pubkey
            })

    log(f"Extracted {len(validators)} validators from scan results")
    return validators


def load_fusaka_deaths():
    """Load Fusaka deaths data for tracking stopped validators"""
    if not FUSAKA_DEATHS_FILE.exists():
        return []

    try:
        with open(FUSAKA_DEATHS_FILE, 'r') as f:
            data = json.load(f)
            return data.get('validators', [])
    except Exception as e:
        log(f"Warning: Could not load Fusaka deaths: {e}")
        return []


def get_validator_ids(client, validators):
    """Get validator IDs from ClickHouse for the pubkeys in scan results"""
    log("Getting validator IDs from ClickHouse...")

    # Process in batches
    batch_size = 1000
    val_id_map = {}  # pubkey -> val_id

    for i in range(0, len(validators), batch_size):
        batch = validators[i:i+batch_size]
        pubkey_list = "','".join([f"0x{v['pubkey']}" for v in batch])

        query = f"""
        SELECT val_id, val_pubkey
        FROM validators_index
        WHERE val_pubkey IN ('{pubkey_list}')
        """

        try:
            result = client.query(query)
            for row in result.result_rows:
                val_id, pubkey = row
                pubkey_clean = pubkey[2:] if pubkey.startswith('0x') else pubkey
                val_id_map[pubkey_clean] = val_id

        except Exception as e:
            log(f"Warning: Error getting validator IDs for batch {i//batch_size + 1}: {e}")

    log(f"Retrieved {len(val_id_map)} validator IDs")
    return val_id_map


def query_7day_performance(client, validator_ids, end_epoch, start_epoch):
    """
    Query ClickHouse for 7-day performance data
    Query all validators then filter to Rocket Pool in Python (faster than IN clause)
    """
    log(f"Querying aggregated performance data for epochs {start_epoch} to {end_epoch}...")

    rp_val_ids = set(validator_ids.values())
    log(f"Will filter to {len(rp_val_ids)} Rocket Pool validators")

    query = f"""
    SELECT
        val_id,
        SUM(att_earned_reward) as total_earned,
        SUM(att_missed_reward) as total_missed,
        SUM(att_penalty) as total_penalties,
        SUM(CASE WHEN att_happened = 1 THEN 1 ELSE 0 END) as successful_attestations,
        COUNT(*) as total_epochs,
        MAX(CASE WHEN att_happened = 1 THEN epoch ELSE NULL END) as last_attestation_epoch,
        MAX(val_balance) as latest_balance
    FROM validators_summary
    WHERE epoch >= {start_epoch} AND epoch <= {end_epoch}
    GROUP BY val_id
    """

    try:
        log("Executing query (this may take 1-2 minutes)...")
        result = client.query(query)
        log(f"Query complete, filtering results...")

        # Filter to only Rocket Pool validators
        performance_data = []
        for row in result.result_rows:
            val_id, earned, missed, penalties, successful, total, last_att, balance = row

            if val_id not in rp_val_ids:
                continue  # Skip non-Rocket Pool validators

            performance_data.append({
                'val_id': val_id,
                'total_earned': earned or 0,
                'total_missed': missed or 0,
                'total_penalties': penalties or 0,
                'successful_attestations': successful or 0,
                'total_epochs': total or 0,
                'last_attestation_epoch': last_att,
                'latest_balance': balance or 0
            })

        log(f"Retrieved performance data for {len(performance_data)} Rocket Pool validators")
        return performance_data

    except Exception as e:
        log(f"ERROR: Failed to query performance data: {e}")
        sys.exit(1)


def get_node_assignments(validators, val_id_map):
    """Get validator to node assignments from scan data"""
    log("Building node assignments from scan data...")

    assignments = {}

    for v in validators:
        pubkey = v['pubkey']
        node_address = v['node_address']

        if pubkey in val_id_map:
            val_id = val_id_map[pubkey]
            assignments[val_id] = node_address

    log(f"Built assignments for {len(assignments)} validators")
    return assignments


def get_validator_statuses(client, validator_ids, end_epoch):
    """Get current status for Rocket Pool validators"""
    log("Querying validator statuses...")

    rp_val_ids = set(validator_ids.values())

    query = f"""
    SELECT val_id, val_status
    FROM validators_summary
    WHERE epoch = {end_epoch}
    """

    try:
        result = client.query(query)
        statuses = {}

        for row in result.result_rows:
            val_id, status = row
            if val_id in rp_val_ids:  # Filter to Rocket Pool validators
                statuses[val_id] = status

        log(f"Retrieved statuses for {len(statuses)} Rocket Pool validators")
        return statuses

    except Exception as e:
        log(f"Warning: Could not get validator statuses: {e}")
        return {}


def calculate_snapshot_metrics(performance_data, node_assignments, validator_statuses, end_epoch):
    """Calculate all metrics for daily snapshot"""
    log("Calculating snapshot metrics...")

    # Load Fusaka deaths for tracking (count by node addresses)
    fusaka_deaths = load_fusaka_deaths()
    fusaka_death_count = len(fusaka_deaths)

    # Active validator statuses
    active_statuses = ('active_ongoing', 'active_exiting', 'active_slashed')

    # Aggregate by node - separate active-only performance tracking from financial totals
    node_performance = {}
    total_earned_gwei = 0
    total_missed_gwei = 0
    total_penalties_gwei = 0

    for perf in performance_data:
        val_id = perf['val_id']
        node_addr = node_assignments.get(val_id)
        status = validator_statuses.get(val_id)

        if not node_addr:
            continue  # Skip validators without node assignment

        # Accumulate network-wide financial metrics for ALL validators
        total_earned_gwei += perf['total_earned']
        total_missed_gwei += perf['total_missed']
        total_penalties_gwei += perf['total_penalties']

        # Only include active validators in performance scoring
        if status not in active_statuses:
            continue

        if node_addr not in node_performance:
            node_performance[node_addr] = {
                'validators': [],
                'successful_attestations': 0,
                'total_epochs': 0
            }

        node_performance[node_addr]['validators'].append(val_id)
        node_performance[node_addr]['successful_attestations'] += perf['successful_attestations']
        node_performance[node_addr]['total_epochs'] += perf['total_epochs']

    # Calculate node-level performance scores (active validators only)
    underperforming_nodes = 0
    zero_performance_nodes = 0
    node_performance_scores = []  # For median calculation (per node)

    # For network-wide average (across all validators)
    total_network_successful_attestations = 0
    total_network_possible_attestations = 0

    for node_addr, node_data in node_performance.items():
        if node_data['total_epochs'] > 0:
            # Node-level score (for counting underperforming nodes)
            performance_score = (node_data['successful_attestations'] / node_data['total_epochs']) * 100
            node_performance_scores.append(performance_score)

            if performance_score < PERFORMANCE_THRESHOLD:
                underperforming_nodes += 1

            if performance_score == 0:
                zero_performance_nodes += 1

            # Accumulate for network-wide average
            total_network_successful_attestations += node_data['successful_attestations']
            total_network_possible_attestations += node_data['total_epochs']

    # Calculate minipool-level performance scores (active validators only)
    underperforming_minipools = 0
    zero_performance_minipools = 0

    # Performance band counters
    perf_band_99_5_100 = 0    # 99.5-100% - Excellent (Emerald)
    perf_band_95_99_5 = 0     # 95-99.5% - Very Good (Light Green)
    perf_band_90_95 = 0       # 90-95% - Good (Amber)
    perf_band_80_90 = 0       # 80-90% - Acceptable (Orange)
    perf_band_50_80 = 0       # 50-80% - Underperforming (Red-Orange)
    perf_band_0_50 = 0        # 0-50% - Poor (Dark Red)
    perf_band_0 = 0           # 0% - Critical (Crimson)

    for perf in performance_data:
        val_id = perf['val_id']
        node_addr = node_assignments.get(val_id)
        status = validator_statuses.get(val_id)

        # Only count active validators
        if not node_addr or status not in active_statuses:
            continue

        if perf['total_epochs'] > 0:
            minipool_performance_score = (perf['successful_attestations'] / perf['total_epochs']) * 100

            # Count underperforming and zero performance (existing logic)
            if minipool_performance_score < PERFORMANCE_THRESHOLD:
                underperforming_minipools += 1

            if minipool_performance_score == 0:
                zero_performance_minipools += 1

            # Bucket into performance bands
            if minipool_performance_score == 0.0:
                perf_band_0 += 1
            elif minipool_performance_score < 50.0:
                perf_band_0_50 += 1
            elif minipool_performance_score < 80.0:
                perf_band_50_80 += 1
            elif minipool_performance_score < 90.0:
                perf_band_80_90 += 1
            elif minipool_performance_score < 95.0:
                perf_band_90_95 += 1
            elif minipool_performance_score < 99.5:
                perf_band_95_99_5 += 1
            else:  # minipool_performance_score >= 99.5
                perf_band_99_5_100 += 1

    # Count validator statuses
    active_minipools = 0
    exited_minipools = 0

    for val_id, status in validator_statuses.items():
        if status in ('active_ongoing', 'active_exiting', 'active_slashed'):
            active_minipools += 1
        elif status in ('exited_unslashed', 'exited_slashed', 'withdrawal_possible', 'withdrawal_done'):
            exited_minipools += 1

    # Count undercollateralised validators (below 31.9 ETH)
    below_31_9_eth = 0
    for perf in performance_data:
        balance_eth = perf['latest_balance'] / 1_000_000_000  # gwei to ETH
        if balance_eth < 31.9 and balance_eth > 0:  # Ignore zero balances
            below_31_9_eth += 1

    # Calculate performance statistics
    # Network-wide average across ALL validators (weighted by validator count)
    if total_network_possible_attestations > 0:
        avg_performance = (total_network_successful_attestations / total_network_possible_attestations) * 100
    else:
        avg_performance = 0

    # Total lost = missed + penalties
    total_lost_gwei = total_missed_gwei + total_penalties_gwei

    metrics = {
        'underperforming_nodes': underperforming_nodes,
        'zero_performance_nodes': zero_performance_nodes,
        'underperforming_minipools': underperforming_minipools,
        'zero_performance_minipools': zero_performance_minipools,
        'active_minipools': active_minipools,
        'fusaka_deaths': fusaka_death_count,
        'below_31_9_eth': below_31_9_eth,

        'total_earned_rewards_gwei': total_earned_gwei,
        'total_missed_rewards_gwei': total_missed_gwei,
        'total_penalties_gwei': total_penalties_gwei,
        'total_lost_gwei': total_lost_gwei,

        'total_nodes': len(node_performance),
        'total_minipools': active_minipools + exited_minipools,
        'exited_minipools': exited_minipools,

        'avg_performance_score': round(avg_performance, 2),

        # Performance band distribution
        'perf_band_99_5_100': perf_band_99_5_100,
        'perf_band_95_99_5': perf_band_95_99_5,
        'perf_band_90_95': perf_band_90_95,
        'perf_band_80_90': perf_band_80_90,
        'perf_band_50_80': perf_band_50_80,
        'perf_band_0_50': perf_band_0_50,
        'perf_band_0': perf_band_0,

        'epochs_analyzed': ANALYSIS_PERIOD_EPOCHS,
        'start_epoch': end_epoch - ANALYSIS_PERIOD_EPOCHS + 1,
        'end_epoch': end_epoch
    }

    log(f"Metrics calculated: {underperforming_nodes} underperforming nodes, "
        f"{underperforming_minipools} underperforming minipools, "
        f"{active_minipools} active minipools, {avg_performance:.2f}% avg performance")
    log(f"Performance bands: 99.5-100%={perf_band_99_5_100}, 95-99.5%={perf_band_95_99_5}, "
        f"90-95%={perf_band_90_95}, 80-90%={perf_band_80_90}, "
        f"50-80%={perf_band_50_80}, 0-50%={perf_band_0_50}, 0%={perf_band_0}")

    return metrics


def load_stats_history():
    """Load existing stats history JSON"""
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            log(f"Warning: Could not load existing stats file: {e}")

    # Create new structure
    return {
        "metadata": {
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_snapshots": 0,
            "description": "Rocket Pool 7-day/80% threshold statistics history"
        },
        "snapshots": []
    }


def append_snapshot(stats_history, snapshot):
    """Append new snapshot, handling deduplication"""
    snapshot_date = snapshot['date']

    # Remove existing snapshot for same date if exists
    stats_history['snapshots'] = [
        s for s in stats_history['snapshots']
        if s['date'] != snapshot_date
    ]

    # Append new snapshot
    stats_history['snapshots'].append(snapshot)

    # Sort chronologically
    stats_history['snapshots'].sort(key=lambda s: s['date'])

    # Update metadata
    stats_history['metadata']['last_updated'] = datetime.now().isoformat()
    stats_history['metadata']['total_snapshots'] = len(stats_history['snapshots'])

    return stats_history


def save_stats_history(stats_history):
    """Save stats history to JSON file with atomic write"""
    temp_file = STATS_FILE.with_suffix('.tmp')

    try:
        # Write to temp file first
        with open(temp_file, 'w') as f:
            json.dump(stats_history, f, indent=2)

        # Atomic rename
        temp_file.rename(STATS_FILE)
        log(f"Stats history saved to {STATS_FILE}")

    except Exception as e:
        log(f"ERROR: Failed to save stats history: {e}")
        if temp_file.exists():
            temp_file.unlink()
        sys.exit(1)


def get_missing_dates(stats_history, up_to_date):
    """
    Find all missing dates in stats history from last recorded date to up_to_date
    Returns list of dates that need to be backfilled
    """
    if not stats_history['snapshots']:
        # No existing data - return list starting from 120 days ago up to up_to_date
        # (limited by ClickHouse retention)
        oldest_date = up_to_date - timedelta(days=119)
        missing = []
        current = oldest_date
        while current <= up_to_date:
            missing.append(current)
            current += timedelta(days=1)
        return missing

    # Get most recent date in history
    existing_dates = {datetime.fromisoformat(s['date']).date() for s in stats_history['snapshots']}
    most_recent = max(existing_dates)

    # Find all missing dates from day after most_recent to up_to_date
    missing = []
    current = most_recent + timedelta(days=1)
    while current <= up_to_date:
        if current not in existing_dates:
            missing.append(current)
        current += timedelta(days=1)

    return sorted(missing)


def collect_snapshot_for_date(client, target_date, scan_data, validators, val_id_map):
    """
    Collect snapshot data for a specific date
    Returns snapshot dict or None if data not available
    """
    # Calculate epoch range for complete UTC day + 7 days before
    end_epoch = date_to_end_epoch(target_date)
    start_epoch = end_epoch - ANALYSIS_PERIOD_EPOCHS + 1

    # Verify data is available in ClickHouse
    latest_epoch = get_latest_epoch(client)
    if end_epoch > latest_epoch:
        log(f"  Skipping {target_date}: end epoch {end_epoch} not yet available (latest: {latest_epoch})")
        return None

    log(f"  Collecting {target_date} (epochs {start_epoch} to {end_epoch})...")

    # Query performance data
    performance_data = query_7day_performance(client, val_id_map, end_epoch, start_epoch)

    # Get node assignments and validator statuses
    node_assignments = get_node_assignments(validators, val_id_map)
    validator_statuses = get_validator_statuses(client, val_id_map, end_epoch)

    # Calculate metrics
    metrics = calculate_snapshot_metrics(
        performance_data,
        node_assignments,
        validator_statuses,
        end_epoch
    )

    # Create snapshot
    snapshot = {
        "date": target_date.isoformat(),
        "timestamp": datetime.now().isoformat(),
        "collection_note": f"Complete UTC day analysis collected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        **metrics
    }

    return snapshot


def backfill_missing_days(client, stats_history, scan_data, validators, val_id_map, up_to_date):
    """
    Automatically backfill any missing days in stats history
    Returns updated stats_history
    """
    missing_dates = get_missing_dates(stats_history, up_to_date)

    if not missing_dates:
        log("No missing dates to backfill")
        return stats_history

    log(f"Found {len(missing_dates)} missing dates to backfill")
    log(f"Date range: {missing_dates[0]} to {missing_dates[-1]}")

    successful = 0
    skipped = 0

    for target_date in missing_dates:
        snapshot = collect_snapshot_for_date(client, target_date, scan_data, validators, val_id_map)

        if snapshot:
            stats_history = append_snapshot(stats_history, snapshot)
            successful += 1
            log(f"  ✓ Collected snapshot for {target_date}")
        else:
            skipped += 1

    if successful > 0:
        save_stats_history(stats_history)
        log(f"Backfilled {successful} missing days ({skipped} skipped)")

    return stats_history


def main():
    """Main collection workflow with automatic backfill"""
    log("=" * 60)
    log("Starting daily stats collection with automatic backfill")
    log("=" * 60)

    # Get the most recent complete UTC day
    target_date = get_previous_complete_day()
    log(f"Target date for collection: {target_date}")

    # Load Rocket Pool scan results
    scan_data = load_scan_results()
    validators = get_validator_list_from_scan(scan_data)

    # Connect to ClickHouse
    client = connect_to_clickhouse()

    # Get validator IDs from ClickHouse
    val_id_map = get_validator_ids(client, validators)

    # Load existing stats history
    stats_history = load_stats_history()

    # First, backfill any missing days
    log("")
    log("=" * 60)
    log("Step 1: Checking for missing days to backfill")
    log("=" * 60)
    stats_history = backfill_missing_days(
        client,
        stats_history,
        scan_data,
        validators,
        val_id_map,
        target_date
    )

    # Then collect current day's data
    log("")
    log("=" * 60)
    log("Step 2: Collecting today's snapshot")
    log("=" * 60)

    snapshot = collect_snapshot_for_date(client, target_date, scan_data, validators, val_id_map)

    if snapshot:
        stats_history = append_snapshot(stats_history, snapshot)
        save_stats_history(stats_history)

        log("=" * 60)
        log(f"✓ Stats snapshot collected for {snapshot['date']} (complete UTC day)")
        log(f"  Total snapshots: {stats_history['metadata']['total_snapshots']}")
        log("=" * 60)
    else:
        log("=" * 60)
        log(f"⚠ Could not collect snapshot for {target_date}")
        log(f"  Data may not be available yet in ClickHouse")
        log("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
