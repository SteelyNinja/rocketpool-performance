#!/usr/bin/env python3
"""
Rocket Pool Performance Analysis Only Script

This script performs only the performance analysis using existing scan data,
making it much faster for frequent updates between full scans.
"""

import json
import clickhouse_connect
from datetime import datetime
from collections import defaultdict
import os
import argparse
import sys

# ClickHouse connection details
CLICKHOUSE_HOST = '192.168.202.250'
CLICKHOUSE_PORT = 8123

# Configuration constants
ANALYSIS_PERIODS = {
    '1day': 225,    # 225 epochs ≈ 24 hours
    '3day': 675,    # 675 epochs ≈ 72 hours  
    '7day': 1575,   # 1575 epochs ≈ 168 hours
    '30day': 6750,  # 6750 epochs ≈ 30 days
    '100day': 22500 # 22500 epochs ≈ 100 days
}

PERFORMANCE_THRESHOLDS = [80, 90, 95, "all"]

# Default analysis parameters (for backward compatibility)
EPOCHS_TO_ANALYZE = 900

# Fusaka hard fork constants
FUSAKA_EPOCH = 411392
FUSAKA_DATETIME = "2025-12-03T21:49:11"
FUSAKA_DEATHS_FILE = "fusaka_deaths.json"

# Nimbus fork constants (Merkle tree cache corruption bug, 2026-02-08)
NIMBUS_FORK_EPOCH = 426274
NIMBUS_FORK_DATETIME = "2026-02-08T01:13:59"
NIMBUS_FORK_DATETIME_EARLY = "2026-02-08T01:07:35"  # Epoch 426273 (3 nodes)
NIMBUS_FORK_DEATHS_FILE = "nimbus_fork_deaths.json"
VALIDATOR_ID_CACHE_FILE = "validator_id_cache.json"

ACTIVE_VALIDATOR_STATUSES = ['active_ongoing', 'active_exiting']


def configure_stdio():
    """Make progress output visible immediately in foreground runs."""
    for stream_name in ('stdout', 'stderr'):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, 'reconfigure'):
            stream.reconfigure(line_buffering=True)


configure_stdio()

def connect_to_clickhouse():
    """Connect to ClickHouse database."""
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT
        )
        return client
    except Exception as e:
        print(f"Failed to connect to ClickHouse: {e}")
        return None


def get_latest_epoch(client):
    """Get the latest epoch using table partition metadata first, then scan only the newest day."""
    try:
        partition_query = """
        SELECT MAX(toInt64(partition))
        FROM system.parts
        WHERE active
          AND database = currentDatabase()
          AND table = 'validators_summary'
        """
        partition_result = client.query(partition_query)
        latest_partition = partition_result.result_rows[0][0]
        if latest_partition is None:
            return None

        latest_epoch_query = f"""
        SELECT MAX(epoch)
        FROM validators_summary
        PREWHERE intDiv(epoch, 225) = {latest_partition}
        """
        latest_epoch_result = client.query(latest_epoch_query)
        return latest_epoch_result.result_rows[0][0]
    except Exception as e:
        print(f"Error getting latest epoch: {e}")
        return None

def load_fusaka_deaths():
    """Load persistent Fusaka deaths data."""
    if not os.path.exists(FUSAKA_DEATHS_FILE):
        return {'validators': []}

    try:
        with open(FUSAKA_DEATHS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load {FUSAKA_DEATHS_FILE}: {e}")
        return {'validators': []}

def save_fusaka_deaths(fusaka_data):
    """Save updated Fusaka deaths data."""
    try:
        with open(FUSAKA_DEATHS_FILE, 'w') as f:
            json.dump(fusaka_data, f, indent=2)
    except IOError as e:
        print(f"Warning: Could not save {FUSAKA_DEATHS_FILE}: {e}")

def load_nimbus_fork_deaths():
    """Load persistent Nimbus fork deaths data."""
    if not os.path.exists(NIMBUS_FORK_DEATHS_FILE):
        return {'validators': []}

    try:
        with open(NIMBUS_FORK_DEATHS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load {NIMBUS_FORK_DEATHS_FILE}: {e}")
        return {'validators': []}

def save_nimbus_fork_deaths(nimbus_data):
    """Save updated Nimbus fork deaths data."""
    try:
        with open(NIMBUS_FORK_DEATHS_FILE, 'w') as f:
            json.dump(nimbus_data, f, indent=2)
    except IOError as e:
        print(f"Warning: Could not save {NIMBUS_FORK_DEATHS_FILE}: {e}")


def batched(items, size):
    """Yield fixed-size batches from a list."""
    for index in range(0, len(items), size):
        yield items[index:index + size]


def build_attestation_info(epoch, newest_epoch, status):
    """Convert an attestation epoch into the report's attestation metadata shape."""
    if epoch is None:
        return None

    genesis_timestamp = 1606824023
    estimated_timestamp = genesis_timestamp + (epoch * 32 * 12)
    return {
        'epoch': epoch,
        'timestamp': estimated_timestamp,
        'datetime': datetime.fromtimestamp(estimated_timestamp).isoformat(),
        'age_epochs': newest_epoch - epoch,
        'status': status
    }


def get_period_windows(latest_epoch, periods=None):
    """Build start/end epoch metadata for the requested analysis periods."""
    selected_periods = periods or list(ANALYSIS_PERIODS.keys())
    period_windows = {}

    for period_name in selected_periods:
        epochs_to_analyze = ANALYSIS_PERIODS[period_name]
        period_windows[period_name] = {
            'epochs_analyzed': epochs_to_analyze,
            'start_epoch': latest_epoch - epochs_to_analyze + 1,
            'end_epoch': latest_epoch
        }

    return period_windows


def build_validator_balances_lookup(validators, validator_statuses):
    """Create the frontend balance lookup keyed by pubkey for active validators."""
    balances_lookup = {}

    for validator in validators:
        pubkey = validator['pubkey']
        status_info = validator_statuses.get(pubkey)
        if not status_info or status_info.get('val_id') is None:
            continue

        balances_lookup[pubkey] = {
            'val_id': status_info['val_id'],
            'balance_eth': (status_info.get('val_balance') or 0) / 1e9,
            'effective_balance_eth': (status_info.get('val_effective_balance') or 0) / 1e9
        }

    return balances_lookup


def dedupe_validators_by_val_id(validators):
    """Keep the first validator entry for each val_id and drop duplicate scan rows."""
    unique_validators = []
    seen_val_ids = set()
    duplicate_count = 0

    for validator in validators:
        val_id = validator.get('val_id')
        if val_id is None:
            unique_validators.append(validator)
            continue

        if val_id in seen_val_ids:
            duplicate_count += 1
            continue

        seen_val_ids.add(val_id)
        unique_validators.append(validator)

    return unique_validators, duplicate_count


def load_validator_id_cache(filename=VALIDATOR_ID_CACHE_FILE):
    """Load cached pubkey-to-validator-id mappings."""
    if not os.path.exists(filename):
        return {'pubkey_to_val_id': {}}

    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("validator id cache must be a JSON object")
        mapping = data.get('pubkey_to_val_id', {})
        if not isinstance(mapping, dict):
            raise ValueError("pubkey_to_val_id must be a JSON object")
        return {'pubkey_to_val_id': mapping}
    except (OSError, json.JSONDecodeError, ValueError) as e:
        print(f"Warning: Could not load {filename}: {e}")
        return {'pubkey_to_val_id': {}}


def save_validator_id_cache(pubkey_to_val_id, filename=VALIDATOR_ID_CACHE_FILE):
    """Persist cached pubkey-to-validator-id mappings with an atomic replace."""
    temp_file = f"{filename}.tmp"
    payload = {
        'updated_at': datetime.now().isoformat(),
        'total_pubkeys': len(pubkey_to_val_id),
        'pubkey_to_val_id': pubkey_to_val_id
    }

    try:
        with open(temp_file, 'w') as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(temp_file, filename)
        print(f"Saved validator id cache with {len(pubkey_to_val_id)} entries")
    except OSError as e:
        print(f"Warning: Could not save {filename}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)


def filter_node_performance_scores(node_scores, threshold_filter):
    """Apply the report threshold filter to an already-computed node score list."""
    if threshold_filter == "all":
        print(f"Returning all {len(node_scores)} nodes (no threshold filter)")
        return node_scores

    filtered_scores = []
    for node in node_scores:
        if node['performance_score'] != "N/A" and node['performance_score'] < threshold_filter:
            filtered_scores.append(node)

    print(f"Filtered to {len(filtered_scores)} nodes below {threshold_filter}% threshold")
    return filtered_scores

def get_all_validator_statuses(client, validators):
    """Get status information for all validators."""
    print(f"Getting validator statuses...")

    latest_epoch = get_latest_epoch(client)
    validator_statuses = {}
    cache_data = load_validator_id_cache()
    validator_ids = {
        pubkey: int(val_id)
        for pubkey, val_id in cache_data['pubkey_to_val_id'].items()
        if isinstance(pubkey, str)
    }
    print(f"Loaded {len(validator_ids)} validator ids from cache")

    unique_pubkeys = []
    seen_pubkeys = set()
    for validator in validators:
        pubkey = validator['pubkey']
        if pubkey and pubkey not in seen_pubkeys:
            unique_pubkeys.append(pubkey)
            seen_pubkeys.add(pubkey)

    missing_pubkeys = [pubkey for pubkey in unique_pubkeys if pubkey not in validator_ids]
    if missing_pubkeys:
        print(f"Resolving {len(missing_pubkeys)} uncached validator ids...")
    else:
        print("All validator ids resolved from cache")

    pubkey_batch_size = 2000
    cache_updated = False
    for batch in batched(missing_pubkeys, pubkey_batch_size):
        try:
            pubkey_list = "','".join([f"0x{pubkey}" for pubkey in batch])
            index_query = f"""
            SELECT val_id, val_pubkey 
            FROM validators_index 
            WHERE val_pubkey IN ('{pubkey_list}')
            """

            result = client.query(index_query)
            for row in result.result_rows:
                val_id, pubkey = row
                pubkey_clean = pubkey[2:] if pubkey.startswith('0x') else pubkey
                validator_ids[pubkey_clean] = val_id
                cache_updated = True
        except Exception as e:
            print(f"Error processing validator index batch: {e}")

    if cache_updated:
        save_validator_id_cache(validator_ids)

    latest_snapshot = {}
    val_id_batch_size = 5000
    all_val_ids = list(validator_ids.values())
    for val_id_batch in batched(all_val_ids, val_id_batch_size):
        try:
            val_id_list = ','.join([str(val_id) for val_id in val_id_batch])
            status_query = f"""
            SELECT val_id, val_status, val_balance, val_effective_balance
            FROM validators_summary
            PREWHERE epoch = {latest_epoch}
            WHERE val_id IN ({val_id_list})
            """

            status_result = client.query(status_query)
            for row in status_result.result_rows:
                val_id, status, val_balance, val_effective_balance = row
                latest_snapshot[val_id] = {
                    'status': status,
                    'val_balance': val_balance or 0,
                    'val_effective_balance': val_effective_balance or 0
                }
        except Exception as e:
            print(f"Error processing latest validator snapshot batch: {e}")

    for validator in validators:
        pubkey = validator['pubkey']
        if pubkey in validator_ids:
            val_id = validator_ids[pubkey]
            snapshot = latest_snapshot.get(val_id, {})
            validator_statuses[pubkey] = {
                'val_id': val_id,
                'status': snapshot.get('status', 'unknown'),
                'node_address': validator['node_address'],
                'minipool_address': validator.get('minipool_address'),
                'megapool_address': validator.get('megapool_address'),
                'pool_type': validator.get('pool_type', 'minipool'),
                'val_balance': snapshot.get('val_balance', 0),
                'val_effective_balance': snapshot.get('val_effective_balance', 0)
            }
        else:
            validator_statuses[pubkey] = {
                'val_id': None,
                'status': 'not_in_database',
                'node_address': validator['node_address'],
                'minipool_address': validator.get('minipool_address'),
                'megapool_address': validator.get('megapool_address'),
                'pool_type': validator.get('pool_type', 'minipool'),
                'val_balance': 0,
                'val_effective_balance': 0
            }

    print(f"Got status for {len(validator_statuses)} validators")
    return validator_statuses, latest_epoch

def get_active_validators(client, validators, validator_statuses=None):
    """Get only active validators (exclude withdrawn, exited, and pending ones)."""
    print(f"Filtering for active validators...")
    
    # Get all validator statuses first (if not provided)
    if validator_statuses is None:
        validator_statuses, _ = get_all_validator_statuses(client, validators)
    
    # Filter for active validators
    active_validators = []
    for validator in validators:
        pubkey = validator['pubkey']
        if pubkey in validator_statuses:
            status_info = validator_statuses[pubkey]
            if status_info['status'] in ACTIVE_VALIDATOR_STATUSES:
                validator['val_id'] = status_info['val_id']
                active_validators.append(validator)

    active_validators, duplicate_count = dedupe_validators_by_val_id(active_validators)
    if duplicate_count:
        print(f"Removed {duplicate_count} duplicate active validator entries by val_id")
    
    print(f"Found {len(active_validators)} active validators")
    return active_validators

def get_database_retention_info(client, newest_epoch=None):
    """Get the database retention information to determine max available data."""
    try:
        if newest_epoch is None:
            newest_epoch = client.query("SELECT MAX(epoch) FROM validators_summary").result_rows[0][0]

        retention_query = """
        SELECT MIN(toInt64(partition))
        FROM system.parts
        WHERE active
          AND database = currentDatabase()
          AND table = 'validators_summary'
        """
        result = client.query(retention_query)
        oldest_partition = result.result_rows[0][0]
        if oldest_partition is None:
            return {'oldest_epoch': None, 'newest_epoch': newest_epoch, 'total_days': None}

        epochs_per_day = 225
        min_epoch = oldest_partition * epochs_per_day
        total_epochs = newest_epoch - min_epoch
        total_days = int(total_epochs / epochs_per_day)
        
        return {
            'oldest_epoch': min_epoch,
            'newest_epoch': newest_epoch,
            'total_days': total_days
        }
    except Exception as e:
        print(f"Error getting database retention info: {e}")
        return {'oldest_epoch': None, 'newest_epoch': None, 'total_days': None}

def find_last_attestation_extended(client, val_id_list, oldest_epoch, search_end_epoch, newest_epoch):
    """Extended search to find the actual last attestation across the full database range."""
    try:
        val_ids = [int(v.strip()) for v in str(val_id_list).split(',') if v.strip()]
        if not val_ids:
            return {}

        epochs_per_day = 225
        total_epochs = newest_epoch - oldest_epoch
        total_days = int(total_epochs / epochs_per_day)

        extended_attestations = {
            val_id: {
                'epoch': None,
                'timestamp': None,
                'datetime': None,
                'age_epochs': None,
                'status': f'older_than_{total_days}_days'
            }
            for val_id in val_ids
        }

        remaining_ids = set(val_ids)
        chunk_days = 1
        max_chunk_days = 28
        chunk_end_epoch = search_end_epoch
        chunk_number = 0

        while remaining_ids and chunk_end_epoch >= oldest_epoch:
            chunk_number += 1
            chunk_epochs = chunk_days * epochs_per_day
            chunk_start_epoch = max(oldest_epoch, chunk_end_epoch - chunk_epochs + 1)
            print(
                f"Extended attestation search chunk {chunk_number}: "
                f"epochs {chunk_start_epoch}-{chunk_end_epoch}, "
                f"window_days={chunk_days}, remaining={len(remaining_ids)}"
            )
            val_id_csv = ','.join(str(val_id) for val_id in remaining_ids)
            extended_query = f"""
            SELECT
                val_id,
                MAX(epoch) as last_attestation_epoch
            FROM validators_summary
            PREWHERE epoch >= {chunk_start_epoch} AND epoch <= {chunk_end_epoch}
            WHERE val_id IN ({val_id_csv})
              AND att_happened = 1
            GROUP BY val_id
            """

            result = client.query(extended_query)
            found_in_chunk = 0
            for row in result.result_rows:
                val_id, last_attestation_epoch = row
                if last_attestation_epoch is None:
                    continue

                attestation_info = build_attestation_info(last_attestation_epoch, newest_epoch, 'found_extended')
                extended_attestations[val_id] = attestation_info
                if val_id in remaining_ids:
                    remaining_ids.remove(val_id)
                    found_in_chunk += 1

            if found_in_chunk > 0:
                print(f"  Found {found_in_chunk} validators in this chunk")
                chunk_days = 1
            else:
                print("  No matches in this chunk, expanding search window")
                chunk_days = min(chunk_days * 2, max_chunk_days)

            chunk_end_epoch = chunk_start_epoch - 1

        print(f"Extended attestation search complete, unresolved={len(remaining_ids)}")
        
        return extended_attestations
        
    except Exception as e:
        print(f"Error in extended attestation search: {e}")
        return {}

def query_attestation_performance_windows(client, validators, validator_statuses, latest_epoch, periods=None):
    """Query performance data for one or more periods using a single shared max-window scan."""
    validators, duplicate_count = dedupe_validators_by_val_id(validators)
    if duplicate_count:
        print(f"Dropped {duplicate_count} duplicate validator rows before shared aggregate fan-out")

    selected_periods = periods or list(ANALYSIS_PERIODS.keys())
    selected_periods = [period for period in ANALYSIS_PERIODS if period in selected_periods]
    max_period = max(selected_periods, key=lambda period: ANALYSIS_PERIODS[period])
    period_windows = get_period_windows(latest_epoch, selected_periods)
    max_window = period_windows[max_period]

    print(
        f"Querying shared attestation performance for {len(validators)} validators "
        f"over {max_window['epochs_analyzed']} epochs..."
    )
    print(f"Analyzing epochs {max_window['start_epoch']} to {max_window['end_epoch']}")

    retention_info = get_database_retention_info(client, latest_epoch)
    oldest_epoch = retention_info['oldest_epoch']
    total_days = retention_info['total_days']
    print(f"Database retention: {total_days} days (epochs {oldest_epoch} to {latest_epoch})")

    val_id_list = ','.join(str(validator['val_id']) for validator in validators)
    query_fields = ["val_id"]
    for period_name in selected_periods:
        start_epoch = period_windows[period_name]['start_epoch']
        query_fields.extend([
            f"SUM(ifNull(att_earned_reward, 0) * (epoch >= {start_epoch})) AS total_earned_{period_name}",
            f"SUM(ifNull(att_missed_reward, 0) * (epoch >= {start_epoch})) AS total_missed_{period_name}",
            f"SUM(ifNull(att_penalty, 0) * (epoch >= {start_epoch})) AS total_penalties_{period_name}",
            f"COUNTIf(epoch >= {start_epoch}) AS total_epochs_{period_name}",
            f"SUM((att_happened = 1) AND (epoch >= {start_epoch})) AS successful_attestations_{period_name}",
            f"NULLIF(MAXIf(epoch, att_happened = 1 AND epoch >= {start_epoch}), 0) AS last_attestation_epoch_{period_name}"
        ])

        if period_name != max_period:
            query_fields.append(
                f"NULLIF(MAXIf(epoch, att_happened = 1 AND epoch < {start_epoch}), 0) "
                f"AS last_attestation_epoch_before_{period_name}"
            )

    performance_query = f"""
    SELECT
        {', '.join(query_fields)}
    FROM validators_summary
    PREWHERE epoch >= {max_window['start_epoch']} AND epoch <= {max_window['end_epoch']}
    WHERE val_id IN ({val_id_list})
    GROUP BY val_id
    """

    result = client.query(performance_query)
    print(f"Processed {len(result.result_rows)} validators from the shared aggregate query")

    row_lookup = {}
    for row in result.named_results():
        row_lookup[row['val_id']] = row

    validators_needing_extended_search = [
        row['val_id']
        for row in row_lookup.values()
        if row.get(f'last_attestation_epoch_{max_period}') is None
    ]
    print(
        f"Validators needing extended last-attestation lookup for {max_period}: "
        f"{len(validators_needing_extended_search)}"
    )

    extended_attestations = {}
    if validators_needing_extended_search and oldest_epoch is not None:
        search_end_epoch = max_window['start_epoch'] - 1
        if search_end_epoch < oldest_epoch:
            search_end_epoch = oldest_epoch
        extended_attestations = find_last_attestation_extended(
            client,
            ','.join(str(val_id) for val_id in validators_needing_extended_search),
            oldest_epoch,
            search_end_epoch,
            latest_epoch
        )

    fallback_last_attestation = {
        'epoch': None,
        'timestamp': None,
        'datetime': None,
        'age_epochs': None,
        'status': f'older_than_{total_days or 10}_days' if total_days else 'older_than_10_days'
    }

    performance_by_period = {period_name: [] for period_name in selected_periods}
    for validator in validators:
        row = row_lookup.get(validator['val_id'])
        if row is None:
            continue

        status_info = validator_statuses.get(validator['pubkey'], {})
        val_balance = status_info.get('val_balance', 0) or 0
        val_effective_balance = status_info.get('val_effective_balance', 0) or 0

        for period_name in selected_periods:
            last_attestation_epoch = row.get(f'last_attestation_epoch_{period_name}')
            if last_attestation_epoch is not None:
                last_attestation_info = build_attestation_info(last_attestation_epoch, latest_epoch, 'found')
            else:
                prior_attestation_epoch = row.get(f'last_attestation_epoch_before_{period_name}')
                if prior_attestation_epoch is not None:
                    last_attestation_info = build_attestation_info(prior_attestation_epoch, latest_epoch, 'found_extended')
                else:
                    last_attestation_info = extended_attestations.get(validator['val_id'], fallback_last_attestation)

            total_earned = row.get(f'total_earned_{period_name}') or 0
            total_missed = row.get(f'total_missed_{period_name}') or 0
            total_penalties = row.get(f'total_penalties_{period_name}') or 0

            performance_by_period[period_name].append({
                'val_id': validator['val_id'],
                'node_address': validator['node_address'],
                'minipool_address': validator.get('minipool_address'),
                'megapool_address': validator.get('megapool_address'),
                'pool_type': validator.get('pool_type', 'minipool'),
                'pubkey': validator['pubkey'],
                'total_earned_rewards': total_earned,
                'total_missed_rewards': total_missed,
                'total_penalties': total_penalties,
                'total_possible_rewards': total_earned + total_missed,
                'total_lost': total_missed + total_penalties,
                'total_epochs': row.get(f'total_epochs_{period_name}') or 0,
                'successful_attestations': row.get(f'successful_attestations_{period_name}') or 0,
                'performance_score': 0.0 if last_attestation_epoch is None else None,
                'last_attestation': last_attestation_info,
                'val_balance': val_balance,
                'val_balance_eth': val_balance / 1e9,
                'val_effective_balance': val_effective_balance,
                'val_effective_balance_eth': val_effective_balance / 1e9
            })

    return performance_by_period, period_windows


def query_attestation_performance(client, validators, latest_epoch, epochs_to_analyze=EPOCHS_TO_ANALYZE, validator_statuses=None):
    """Backward-compatible single-period query wrapper."""
    period_name = None
    for name, period_epochs in ANALYSIS_PERIODS.items():
        if period_epochs == epochs_to_analyze:
            period_name = name
            break

    if period_name is None:
        raise ValueError(f"Unsupported analysis period: {epochs_to_analyze} epochs")
    if validator_statuses is None:
        raise ValueError("validator_statuses is required for query_attestation_performance")

    performance_by_period, period_windows = query_attestation_performance_windows(
        client,
        validators,
        validator_statuses,
        latest_epoch,
        periods=[period_name]
    )
    period_window = period_windows[period_name]
    return (
        performance_by_period[period_name],
        period_window['start_epoch'],
        period_window['end_epoch']
    )

def normalize_pubkey(pubkey):
    """Normalise pubkey values for stable matching across data sources."""
    if not isinstance(pubkey, str):
        return None
    value = pubkey.lower()
    if value.startswith('0x'):
        value = value[2:]
    return value


def calculate_uld_status(node_address, scan_data, active_pubkeys=None, megapool_scan_data=None):
    """Calculate Use Latest Delegate status for a node.
    Returns: dict with 'status' ('yes'/'no'/'partial'/'unknown') and 'count' (x/y format for partial).
    Only active validators are considered. Handles both minipools (per-validator flag) and
    megapools (single flag covers all validators in the megapool).
    """
    active_pubkey_set = set()
    if active_pubkeys:
        for pubkey in active_pubkeys:
            normalized = normalize_pubkey(pubkey)
            if normalized:
                active_pubkey_set.add(normalized)

    active_flags = []

    # Minipool ULD: per-minipool delegate flag
    if scan_data:
        node = next((n for n in scan_data if n['node_address'] == node_address), None)
        if node and node.get('minipool_use_latest_delegate') and active_pubkey_set:
            flags = node['minipool_use_latest_delegate']
            pubkeys = node.get('minipool_pubkeys', [])
            seen_active_pubkeys = set()
            for idx, flag in enumerate(flags):
                if flag is None or idx >= len(pubkeys):
                    continue
                normalized = normalize_pubkey(pubkeys[idx])
                if (
                    normalized
                    and normalized in active_pubkey_set
                    and normalized not in seen_active_pubkeys
                ):
                    active_flags.append(flag)
                    seen_active_pubkeys.add(normalized)

    # Megapool ULD: single flag per megapool, applies to all active megapool validators
    if megapool_scan_data and active_pubkey_set:
        mp_node = next((n for n in megapool_scan_data if n['node_address'] == node_address), None)
        if mp_node and mp_node.get('megapool_use_latest_delegate') is not None:
            mp_uld_flag = mp_node['megapool_use_latest_delegate']
            mp_pubkeys = mp_node.get('megapool_validator_pubkeys', [])
            for pubkey in mp_pubkeys:
                normalized = normalize_pubkey(pubkey)
                if normalized and normalized in active_pubkey_set:
                    active_flags.append(mp_uld_flag)

    total = len(active_flags)
    if total == 0:
        return {'status': 'unknown', 'count': None}

    true_count = sum(1 for f in active_flags if f is True)
    false_count = sum(1 for f in active_flags if f is False)

    if true_count == total:
        return {'status': 'yes', 'count': None}
    elif false_count == total:
        return {'status': 'no', 'count': None}
    else:
        return {'status': 'partial', 'count': f"{true_count}/{total}"}

def calculate_node_performance_scores(performance_data, all_rp_validators, threshold_filter=None, scan_data=None, fusaka_deaths=None, nimbus_fork_deaths=None, megapool_scan_data=None, validator_statuses=None):
    """Calculate performance scores grouped by node with optional threshold filtering."""
    print("Calculating node performance scores...")

    # Initialise Fusaka deaths tracking
    if fusaka_deaths is None:
        fusaka_deaths = {'validators': []}

    # Create a lookup dict for quick access
    fusaka_lookup = {v['node_address']: v for v in fusaka_deaths.get('validators', [])}
    validators_to_remove = []  # Track validators that came back online

    # Initialise Nimbus fork deaths tracking
    if nimbus_fork_deaths is None:
        nimbus_fork_deaths = {'validators': []}

    nimbus_lookup = {v['node_address']: v for v in nimbus_fork_deaths.get('validators', [])}
    nimbus_validators_to_remove = []  # Track Nimbus validators that came back online
    
    def _empty_pool_counts():
        return {'total': 0, 'active': 0, 'exited': 0}

    node_pool_counts = defaultdict(lambda: {
        'total': 0, 'active': 0, 'exited': 0,
        'minipool': _empty_pool_counts(),
        'megapool': _empty_pool_counts()
    })

    for validator in all_rp_validators:
        node_addr = validator['node_address']
        pool_type = validator.get('pool_type', 'minipool')
        node_pool_counts[node_addr]['total'] += 1
        node_pool_counts[node_addr][pool_type]['total'] += 1
    
    # Group aggregated performance data by node
    node_data = defaultdict(lambda: {
        'active_validators': set(),
        'active_pubkeys': set(),
        'total_earned_rewards': 0,
        'total_missed_rewards': 0,
        'total_penalties': 0,
        'total_possible_rewards': 0,
        'total_lost': 0,
        'has_zero_score_validator': False,
        'last_attestations': [],
        'validator_balances': [],
        'total_balance': 0,
        'validators_below_32_eth': 0
    })
    
    # Process each aggregated performance record (only active validators)
    for record in performance_data:
        node_addr = record['node_address']
        pool_type = record.get('pool_type', 'minipool')
        node_data[node_addr]['active_validators'].add(record['val_id'])
        node_pool_counts[node_addr][pool_type]['active'] += 1
        if record.get('pubkey'):
            node_data[node_addr]['active_pubkeys'].add(record['pubkey'])
        node_data[node_addr]['total_earned_rewards'] += record['total_earned_rewards']
        node_data[node_addr]['total_missed_rewards'] += record['total_missed_rewards']
        node_data[node_addr]['total_penalties'] += record['total_penalties']
        node_data[node_addr]['total_possible_rewards'] += record['total_possible_rewards']
        node_data[node_addr]['total_lost'] += record['total_lost']
        if record.get('performance_score') == 0.0:
            node_data[node_addr]['has_zero_score_validator'] = True
        
        # Collect last attestation data - always collect it, even for 0% performance
        if record.get('last_attestation'):
            node_data[node_addr]['last_attestations'].append(record['last_attestation'])

        # Aggregate balance data
        if 'val_balance_eth' in record:
            balance_eth = record['val_balance_eth']
            node_data[node_addr]['validator_balances'].append(balance_eth)
            node_data[node_addr]['total_balance'] += record['val_balance']

            # Track undercollateralised validators
            if balance_eth < 31.9:
                node_data[node_addr]['validators_below_32_eth'] += 1
    
    def get_node_last_attestation(attestations):
        """Get the most recent attestation from a list of attestations."""
        if not attestations:
            return {'status': 'no_data'}
        
        # Filter found attestations (both 'found' and 'found_extended')
        found_attestations = [att for att in attestations if att['status'] in ['found', 'found_extended']]
        
        if found_attestations:
            # Return the most recent found attestation
            # Only compare attestations that have actual epoch values
            valid_attestations = [att for att in found_attestations if att.get('epoch') is not None]
            if valid_attestations:
                most_recent = max(valid_attestations, key=lambda x: x['epoch'])
                return most_recent
            else:
                # All found attestations have null epochs, return the first one
                return found_attestations[0]
        else:
            # All attestations are older than database retention - find the most specific status
            # Look for any status that starts with 'older_than_' and get the highest number
            older_statuses = [att for att in attestations if att['status'].startswith('older_than_')]
            if older_statuses:
                # Return the status with the highest day count (e.g., 'older_than_45_days')
                try:
                    return max(older_statuses, key=lambda x: int(x['status'].split('_')[2]) if x['status'].count('_') >= 2 and x['status'].split('_')[2].isdigit() else 10)
                except (ValueError, IndexError):
                    return {'status': 'older_than_10_days'}
            else:
                # Default fallback - should rarely be reached
                return {'status': 'older_than_10_days'}
    
    # Calculate scores for each node that has validators (minipools or megapool)
    node_scores = []
    for node_addr, pool_counts in node_pool_counts.items():
        active_validators = len(node_data[node_addr]['active_validators'])

        exited_validators = pool_counts['total'] - active_validators

        pool_counts['active'] = active_validators
        pool_counts['exited'] = exited_validators

        # Minipool exited = total - active (pending_initialized is rare for minipools;
        # the vast majority of inactive minipools have genuinely withdrawn)
        pool_counts['minipool']['exited'] = (
            pool_counts['minipool']['total'] - pool_counts['minipool']['active']
        )

        # Megapool exited: only validators that have actually withdrawn/exited.
        # Pending validators (pending_initialized, not_in_database) must NOT count
        # as exited — they are queued for activation, not gone.
        if validator_statuses:
            mega_exited = sum(
                1 for v in all_rp_validators
                if v.get('pool_type') == 'megapool'
                and v['node_address'] == node_addr
                and validator_statuses.get(v['pubkey'], {}).get('status') in (
                    'withdrawal_done', 'exited_unslashed', 'exited_slashed', 'withdrawal_possible'
                )
            )
            pool_counts['megapool']['exited'] = mega_exited
        else:
            pool_counts['megapool']['exited'] = (
                pool_counts['megapool']['total'] - pool_counts['megapool']['active']
            )
        
        # Calculate performance score (only for nodes with active validators)
        if active_validators > 0:
            if node_data[node_addr]['has_zero_score_validator']:
                # If any validator has 0% score, the entire node gets 0%
                performance_score = 0.0
            else:
                # Normal calculation for nodes with attestations in reporting period
                performance_score = (node_data[node_addr]['total_earned_rewards'] / node_data[node_addr]['total_possible_rewards']) * 100 if node_data[node_addr]['total_possible_rewards'] > 0 else 0
                performance_score = round(performance_score, 2)
        else:
            performance_score = "N/A"  # No active validators
        
        # Get the most recent attestation for this node
        attestations_list = node_data[node_addr]['last_attestations']
        
        # Backend debug confirmed working - removed debug logging
        
        node_last_attestation = get_node_last_attestation(attestations_list)

        # Check for Fusaka deaths - validators that died at the hard fork
        if node_addr in fusaka_lookup:
            # This node was previously identified as a Fusaka death
            # Check if it has come back online (has recent attestations within 120-day window)
            if (node_last_attestation.get('status') in ['found', 'found_extended'] and
                node_last_attestation.get('epoch') is not None and
                node_last_attestation['epoch'] > FUSAKA_EPOCH):
                # Node is back online! Remove from Fusaka deaths list
                validators_to_remove.append(node_addr)
                print(f"  Node {node_addr} recovered from Fusaka death (last attestation: epoch {node_last_attestation['epoch']})")
            elif active_validators == 0 and pool_counts['total'] > 0:
                # Node has fully exited - no longer a Fusaka death, just exited
                validators_to_remove.append(node_addr)
                print(f"  Node {node_addr} exited - removing from Fusaka deaths tracking")
            else:
                # Still offline - override with Fusaka death data
                # This ensures we maintain the classification even after the 120-day window
                genesis_timestamp = 1606824023
                fusaka_timestamp = genesis_timestamp + (FUSAKA_EPOCH * 32 * 12)
                node_last_attestation = {
                    'epoch': FUSAKA_EPOCH,
                    'timestamp': fusaka_timestamp,
                    'datetime': FUSAKA_DATETIME,
                    'age_epochs': node_last_attestation.get('age_epochs'),  # Keep current age
                    'status': 'fusaka_death'
                }

        # Check for Nimbus fork deaths - validators that died at the Nimbus fork
        if node_addr in nimbus_lookup:
            nimbus_entry = nimbus_lookup[node_addr]
            entry_epoch = nimbus_entry.get('nimbus_fork_epoch', NIMBUS_FORK_EPOCH)
            if (node_last_attestation.get('status') in ['found', 'found_extended'] and
                node_last_attestation.get('epoch') is not None and
                node_last_attestation['epoch'] > entry_epoch):
                nimbus_validators_to_remove.append(node_addr)
                print(f"  Node {node_addr} recovered from Nimbus fork death (last attestation: epoch {node_last_attestation['epoch']})")
            elif active_validators == 0 and pool_counts['total'] > 0:
                nimbus_validators_to_remove.append(node_addr)
                print(f"  Node {node_addr} exited - removing from Nimbus fork deaths tracking")
            else:
                genesis_timestamp = 1606824023
                nimbus_timestamp = genesis_timestamp + (entry_epoch * 32 * 12)
                entry_datetime = nimbus_entry.get('nimbus_fork_datetime', NIMBUS_FORK_DATETIME)
                node_last_attestation = {
                    'epoch': entry_epoch,
                    'timestamp': nimbus_timestamp,
                    'datetime': entry_datetime,
                    'age_epochs': node_last_attestation.get('age_epochs'),
                    'status': 'nimbus_fork_death'
                }

        # Determine if validator is back up (has made attestations in the most recent 3 epochs)
        is_back_up = False
        if (isinstance(performance_score, (int, float)) and performance_score > 0 and 
            node_last_attestation.get('status') in ['found', 'found_extended'] and
            node_last_attestation.get('age_epochs') is not None):
            # Check if validator has made attestations in the most recent 3 epochs
            is_back_up = node_last_attestation['age_epochs'] <= 3

        # Calculate ULD (Use Latest Delegate) status
        uld_info = calculate_uld_status(node_addr, scan_data, node_data[node_addr]['active_pubkeys'], megapool_scan_data)

        # Calculate balance statistics
        if node_data[node_addr]['validator_balances']:
            balances = node_data[node_addr]['validator_balances']
            avg_balance = sum(balances) / len(balances)
            min_balance = min(balances)
            max_balance = max(balances)
        else:
            avg_balance = 0
            min_balance = None
            max_balance = None

        node_scores.append({
            'node_address': node_addr,
            'total_minipools': pool_counts['minipool']['total'],
            'active_minipools': pool_counts['minipool']['active'],
            'exited_minipools': pool_counts['minipool']['exited'],
            'total_megapool_validators': pool_counts['megapool']['total'],
            'active_megapool_validators': pool_counts['megapool']['active'],
            'exited_megapool_validators': pool_counts['megapool']['exited'],
            'pool_type_breakdown': {
                'minipool': dict(pool_counts['minipool']),
                'megapool': dict(pool_counts['megapool'])
            },
            'performance_score': performance_score,
            'total_earned_rewards': node_data[node_addr]['total_earned_rewards'],
            'total_missed_rewards': node_data[node_addr]['total_missed_rewards'],
            'total_penalties': node_data[node_addr]['total_penalties'],
            'total_lost': node_data[node_addr]['total_lost'],
            'total_possible_rewards': node_data[node_addr]['total_possible_rewards'],
            'last_attestation': node_last_attestation,
            'is_back_up': is_back_up,
            'uld_status': uld_info['status'],
            'uld_count': uld_info['count'],
            'total_balance_eth': node_data[node_addr]['total_balance'] / 1e9,
            'avg_balance_eth': avg_balance,
            'min_balance_eth': min_balance,
            'max_balance_eth': max_balance,
            'validators_below_32_eth': node_data[node_addr]['validators_below_32_eth']
        })
    
    # Update Fusaka deaths list - remove validators that came back online
    if validators_to_remove:
        fusaka_deaths['validators'] = [
            v for v in fusaka_deaths.get('validators', [])
            if v['node_address'] not in validators_to_remove
        ]
        fusaka_deaths['total_count'] = len(fusaka_deaths['validators'])
        fusaka_deaths['last_updated'] = datetime.now().isoformat() + 'Z'
        print(f"Removed {len(validators_to_remove)} recovered node(s) from Fusaka deaths tracking")

    # Update Nimbus fork deaths list - remove validators that came back online
    if nimbus_validators_to_remove:
        nimbus_fork_deaths['validators'] = [
            v for v in nimbus_fork_deaths.get('validators', [])
            if v['node_address'] not in nimbus_validators_to_remove
        ]
        nimbus_fork_deaths['total_count'] = len(nimbus_fork_deaths['validators'])
        nimbus_fork_deaths['last_updated'] = datetime.now().isoformat() + 'Z'
        print(f"Removed {len(nimbus_validators_to_remove)} recovered node(s) from Nimbus fork deaths tracking")

    # Sort by performance score (put N/A at the end)
    def sort_key(x):
        if x['performance_score'] == "N/A":
            return -1  # Put N/A at the end
        return x['performance_score']

    node_scores.sort(key=sort_key, reverse=True)

    # Apply threshold filter if specified (show underperforming nodes)
    if threshold_filter is not None:
        return filter_node_performance_scores(node_scores, threshold_filter), fusaka_deaths, nimbus_fork_deaths

    return node_scores, fusaka_deaths, nimbus_fork_deaths

def load_scan_data(filename='rocketpool_scan_results.json'):
    """Load existing scan data."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found. Please run the full scanner first.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return None


def load_megapool_scan_data(filename='rocketpool_megapool_scan_results.json'):
    """Load megapool scan data. Returns empty list if file does not exist yet."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        print(f"Warning: Invalid JSON in {filename}, skipping megapool data")
        return []


def extract_validators_from_scan(scan_data, pool_type='minipool'):
    """Extract a flat validator list from scan data for one pool type.

    Each entry has: node_index, node_address, minipool_address (or None),
    megapool_address (or None), pubkey, pool_type.
    """
    validators = []
    for node in scan_data:
        if pool_type == 'minipool':
            if node.get('minipool_count', 0) > 0:
                for i, pubkey in enumerate(node.get('minipool_pubkeys', [])):
                    if pubkey:
                        validators.append({
                            'node_index': node['node_index'],
                            'node_address': node['node_address'],
                            'minipool_address': node['minipool_addresses'][i],
                            'megapool_address': None,
                            'pubkey': pubkey,
                            'pool_type': 'minipool'
                        })
        elif pool_type == 'megapool':
            megapool_addr = node.get('megapool_address')
            for pubkey in node.get('megapool_validator_pubkeys', []):
                if pubkey:
                    validators.append({
                        'node_index': node['node_index'],
                        'node_address': node['node_address'],
                        'minipool_address': None,
                        'megapool_address': megapool_addr,
                        'pubkey': pubkey,
                        'pool_type': 'megapool'
                    })
    return validators


def run_analysis(period='7day', threshold=80, output_dir='reports'):
    """Run performance analysis for specified period and threshold."""
    # Get epochs for the specified period
    epochs_to_analyze = ANALYSIS_PERIODS.get(period, EPOCHS_TO_ANALYZE)
    
    print(f"=== ROCKET POOL PERFORMANCE ANALYSIS ({period.upper()}, {threshold}%) ===")
    print("Loading existing scan data...")
    
    # Load existing scan results
    scan_results = load_scan_data()
    if not scan_results:
        return 1

    print(f"Loaded scan data for {len(scan_results)} nodes")

    megapool_scan_results = load_megapool_scan_data()
    if megapool_scan_results:
        print(f"Loaded megapool scan data for {len(megapool_scan_results)} nodes with megapools")
    else:
        print("No megapool scan data found - tracking minipools only")

    # Load Fusaka deaths tracking
    fusaka_deaths = load_fusaka_deaths()
    if fusaka_deaths.get('validators'):
        print(f"Loaded {len(fusaka_deaths['validators'])} Fusaka death validators for tracking")

    # Load Nimbus fork deaths tracking
    nimbus_fork_deaths = load_nimbus_fork_deaths()
    if nimbus_fork_deaths.get('validators'):
        print(f"Loaded {len(nimbus_fork_deaths['validators'])} Nimbus fork death validators for tracking")

    # Extract validators from both minipool and megapool scan data
    minipool_validators = extract_validators_from_scan(scan_results, 'minipool')
    megapool_validators = extract_validators_from_scan(megapool_scan_results, 'megapool')
    validators = minipool_validators + megapool_validators

    print(f"Starting performance analysis for {len(validators)} validators "
          f"({len(minipool_validators)} minipool, {len(megapool_validators)} megapool)...")
    
    # Connect to ClickHouse for performance analysis
    client = connect_to_clickhouse()
    if not client:
        print("Error: Could not connect to ClickHouse.")
        return 1
    
    # Get all validator statuses first
    all_validator_statuses, latest_epoch = get_all_validator_statuses(client, validators)
    print(f"Latest epoch: {latest_epoch}")
    
    # Filter for active validators only
    active_validators = get_active_validators(client, validators, all_validator_statuses)
    
    # Query attestation performance
    performance_data, start_epoch, end_epoch = query_attestation_performance(
        client,
        active_validators,
        latest_epoch,
        epochs_to_analyze,
        validator_statuses=all_validator_statuses
    )
    
    # Calculate node performance scores with threshold filtering
    node_scores_all, fusaka_deaths, nimbus_fork_deaths = calculate_node_performance_scores(
        performance_data, validators, None, scan_data=scan_results, fusaka_deaths=fusaka_deaths,
        nimbus_fork_deaths=nimbus_fork_deaths, megapool_scan_data=megapool_scan_results,
        validator_statuses=all_validator_statuses
    )
    node_scores = filter_node_performance_scores(node_scores_all, threshold)

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    period_dir = os.path.join(output_dir, period)
    os.makedirs(period_dir, exist_ok=True)
    
    # Create validator balance lookup by pubkey for frontend
    validator_balances_lookup = build_validator_balances_lookup(active_validators, all_validator_statuses)

    print(f"Created validator_balances_lookup with {len(validator_balances_lookup)} entries")

    # Save performance analysis with new structure
    output_data = {
        'analysis_date': datetime.now().isoformat(),
        'period': period,
        'threshold': threshold,
        'epochs_analyzed': epochs_to_analyze,
        'start_epoch': start_epoch,
        'end_epoch': end_epoch,
        'total_nodes': len(node_scores),
        'node_performance_scores': node_scores,
        'validator_statuses': all_validator_statuses,
        'validator_balances': validator_balances_lookup
    }
    
    filename = os.path.join(period_dir, f'performance_{threshold}.json')
    with open(filename, 'w') as f:
        json.dump(output_data, f, indent=2, default=str)

    print(f"\nPerformance analysis saved to {filename}")

    # Save updated Fusaka deaths tracking
    save_fusaka_deaths(fusaka_deaths)

    # Save updated Nimbus fork deaths tracking
    save_nimbus_fork_deaths(nimbus_fork_deaths)

    # Print performance summary
    print(f"\n=== PERFORMANCE ANALYSIS SUMMARY ===")
    print(f"Period: {period} ({epochs_to_analyze} epochs, {start_epoch} to {end_epoch})")
    print(f"Threshold: <{threshold}% (underperforming)")
    print(f"Nodes below threshold: {len(node_scores)}")
    
    if node_scores:
        numeric_scores = [
            node['performance_score']
            for node in node_scores
            if isinstance(node['performance_score'], (int, float))
        ]
        if numeric_scores:
            avg_performance = sum(numeric_scores) / len(numeric_scores)
            print(f"Average performance score: {avg_performance:.2f}%")
    
    print(f"\nAnalysis completed successfully!")
    print(f"Output: {filename}")
    
    return filename

def main():
    """Main function with command line argument support."""
    parser = argparse.ArgumentParser(description='Rocket Pool Performance Analysis')
    parser.add_argument('--period', choices=['1day', '3day', '7day', '30day', '100day'], default='7day',
                       help='Analysis time period (default: 7day)')
    parser.add_argument('--threshold', type=int, choices=[80, 90, 95], default=80,
                       help='Performance threshold percentage (default: 80)')
    parser.add_argument('--output-dir', default='reports',
                       help='Output directory for reports (default: reports)')
    
    args = parser.parse_args()
    
    try:
        run_analysis(args.period, args.threshold, args.output_dir)
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
