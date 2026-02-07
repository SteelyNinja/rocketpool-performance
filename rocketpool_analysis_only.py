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

# ClickHouse connection details
CLICKHOUSE_HOST = '192.168.202.250'
CLICKHOUSE_PORT = 8123

# Configuration constants
ANALYSIS_PERIODS = {
    '1day': 225,    # 225 epochs ≈ 24 hours
    '3day': 675,    # 675 epochs ≈ 72 hours  
    '7day': 1575    # 1575 epochs ≈ 168 hours
}

PERFORMANCE_THRESHOLDS = [80, 90, 95, "all"]

# Default analysis parameters (for backward compatibility)
EPOCHS_TO_ANALYZE = 900

# Fusaka hard fork constants
FUSAKA_EPOCH = 411392
FUSAKA_DATETIME = "2025-12-03T21:49:11"
FUSAKA_DEATHS_FILE = "fusaka_deaths.json"

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

def get_all_validator_statuses(client, validators):
    """Get status information for all validators."""
    print(f"Getting validator statuses...")
    
    # Get latest epoch
    latest_epoch = client.query("SELECT MAX(epoch) FROM validators_summary").result_rows[0][0]
    
    # Process in batches
    batch_size = 1000
    validator_statuses = {}
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i+batch_size]
        
        try:
            # Get validator IDs for this batch
            pubkey_list = "','".join([f"0x{v['pubkey']}" for v in batch])
            
            index_query = f"""
            SELECT val_id, val_pubkey 
            FROM validators_index 
            WHERE val_pubkey IN ('{pubkey_list}')
            """
            
            result = client.query(index_query)
            validator_ids = {}
            
            for row in result.result_rows:
                val_id, pubkey = row
                pubkey_clean = pubkey[2:] if pubkey.startswith('0x') else pubkey
                validator_ids[pubkey_clean] = val_id
            
            # Get status for these validators
            if validator_ids:
                val_id_list = ','.join([str(v) for v in validator_ids.values()])
                
                status_query = f"""
                SELECT val_id, val_status
                FROM validators_summary 
                WHERE val_id IN ({val_id_list}) 
                AND epoch = {latest_epoch}
                """
                
                status_result = client.query(status_query)
                status_map = {row[0]: row[1] for row in status_result.result_rows}
                
                # Record status for all validators
                for validator in batch:
                    pubkey = validator['pubkey']
                    if pubkey in validator_ids:
                        val_id = validator_ids[pubkey]
                        status = status_map.get(val_id, 'unknown')
                        validator_statuses[pubkey] = {
                            'val_id': val_id,
                            'status': status,
                            'node_address': validator['node_address'],
                            'minipool_address': validator['minipool_address']
                        }
                    else:
                        # Validator not found in database - consider as exited
                        validator_statuses[pubkey] = {
                            'val_id': None,
                            'status': 'not_in_database',
                            'node_address': validator['node_address'],
                            'minipool_address': validator['minipool_address']
                        }
                            
        except Exception as e:
            print(f"Error processing batch: {e}")
            continue
    
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
    active_statuses = ['active_ongoing', 'active_exiting']
    
    for validator in validators:
        pubkey = validator['pubkey']
        if pubkey in validator_statuses:
            status_info = validator_statuses[pubkey]
            if status_info['status'] in active_statuses:
                validator['val_id'] = status_info['val_id']
                active_validators.append(validator)
    
    print(f"Found {len(active_validators)} active validators")
    return active_validators

def get_database_retention_info(client):
    """Get the database retention information to determine max available data."""
    try:
        retention_query = "SELECT MIN(epoch) as oldest_epoch, MAX(epoch) as newest_epoch FROM validators_summary"
        result = client.query(retention_query)
        min_epoch, max_epoch = result.result_rows[0]
        
        # Calculate total available days
        epochs_per_day = 225
        total_epochs = max_epoch - min_epoch
        total_days = int(total_epochs / epochs_per_day)
        
        return {
            'oldest_epoch': min_epoch,
            'newest_epoch': max_epoch,
            'total_days': total_days
        }
    except Exception as e:
        print(f"Error getting database retention info: {e}")
        return {'oldest_epoch': None, 'newest_epoch': None, 'total_days': None}

def find_last_attestation_extended(client, val_id_list, oldest_epoch, search_end_epoch, newest_epoch):
    """Extended search to find the actual last attestation across the full database range."""
    try:
        extended_query = f"""
        SELECT 
            val_id,
            MAX(CASE WHEN att_happened = 1 THEN epoch ELSE NULL END) as last_attestation_epoch,
            MIN(epoch) as first_epoch_in_db,
            MAX(epoch) as last_epoch_in_db
        FROM validators_summary 
        PREWHERE epoch >= {oldest_epoch} AND epoch <= {search_end_epoch}
        WHERE val_id IN ({val_id_list}) 
        AND epoch >= {oldest_epoch} 
        AND epoch <= {search_end_epoch}
        GROUP BY val_id
        """
        
        result = client.query(extended_query)
        extended_attestations = {}
        
        for row in result.result_rows:
            val_id, last_attestation_epoch, first_epoch_in_db, last_epoch_in_db = row
            
            if last_attestation_epoch is not None:
                # Genesis timestamp: Dec 1, 2020 12:00:23 UTC
                genesis_timestamp = 1606824023
                estimated_timestamp = genesis_timestamp + (last_attestation_epoch * 32 * 12)
                
                extended_attestations[val_id] = {
                    'epoch': last_attestation_epoch,
                    'timestamp': estimated_timestamp,
                    'datetime': datetime.fromtimestamp(estimated_timestamp).isoformat(),
                    'age_epochs': newest_epoch - last_attestation_epoch,
                    'status': 'found_extended'
                }
            else:
                # No attestation found in entire database
                epochs_per_day = 225
                total_epochs = newest_epoch - oldest_epoch
                total_days = int(total_epochs / epochs_per_day)
                
                extended_attestations[val_id] = {
                    'epoch': None,
                    'timestamp': None,
                    'datetime': None,
                    'age_epochs': None,
                    'status': f'older_than_{total_days}_days'
                }
        
        return extended_attestations
        
    except Exception as e:
        print(f"Error in extended attestation search: {e}")
        return {}

def query_attestation_performance(client, validators, latest_epoch, epochs_to_analyze=EPOCHS_TO_ANALYZE):
    """Query detailed attestation performance for the specified number of epochs."""
    print(f"Querying attestation performance for {len(validators)} validators over {epochs_to_analyze} epochs...")
    
    # Get database retention info
    retention_info = get_database_retention_info(client)
    oldest_epoch = retention_info['oldest_epoch']
    total_days = retention_info['total_days']
    
    # Calculate epoch range for performance analysis
    start_epoch = latest_epoch - epochs_to_analyze + 1
    end_epoch = latest_epoch
    
    print(f"Analyzing epochs {start_epoch} to {end_epoch}")
    print(f"Database retention: {total_days} days (epochs {oldest_epoch} to {latest_epoch})")
    
    # Process in batches and aggregate immediately to save memory
    batch_size = 1000  # Larger batches to reduce ClickHouse round-trips
    
    # Use aggregated data structure instead of storing all records
    validator_aggregates = {}
    
    for i in range(0, len(validators), batch_size):
        batch = validators[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(validators) + batch_size - 1)//batch_size}")
        
        try:
            val_id_list = ','.join([str(v['val_id']) for v in batch])
            
            # Query aggregated performance data for the reporting period (single query)
            performance_query = f"""
            SELECT
                val_id,
                SUM(ifNull(att_earned_reward, 0)) as total_earned,
                SUM(ifNull(att_missed_reward, 0)) as total_missed,
                SUM(ifNull(att_penalty, 0)) as total_penalties,
                COUNT(*) as total_epochs,
                SUM(att_happened = 1) as successful_attestations,
                MAXIf(epoch, att_happened = 1) as last_attestation_epoch,
                MAXIf(val_balance, epoch = {end_epoch}) as val_balance,
                MAXIf(val_effective_balance, epoch = {end_epoch}) as val_effective_balance
            FROM validators_summary
            PREWHERE epoch >= {start_epoch} AND epoch <= {end_epoch}
            WHERE val_id IN ({val_id_list})
            AND epoch >= {start_epoch}
            AND epoch <= {end_epoch}
            GROUP BY val_id
            """
            
            result = client.query(performance_query)

            # Get extended attestation data for validators with no recent attestations
            validators_needing_extended_search = []
            validators_with_recent_attestations = {}
            
            for row in result.result_rows:
                val_id, total_earned, total_missed, total_penalties, total_epochs, successful_attestations, last_attestation_epoch, val_balance, val_effective_balance = row
                
                if last_attestation_epoch is not None:
                    # Found attestation in reporting period
                    genesis_timestamp = 1606824023
                    estimated_timestamp = genesis_timestamp + (last_attestation_epoch * 32 * 12)
                    
                    validators_with_recent_attestations[val_id] = {
                        'epoch': last_attestation_epoch,
                        'timestamp': estimated_timestamp,
                        'datetime': datetime.fromtimestamp(estimated_timestamp).isoformat(),
                        'age_epochs': end_epoch - last_attestation_epoch,
                        'status': 'found'
                    }
                else:
                    # No attestation in reporting period - needs extended search
                    validators_needing_extended_search.append(val_id)
            
            # Perform extended search for validators with no recent attestations
            extended_attestations = {}
            if validators_needing_extended_search and oldest_epoch is not None:
                extended_val_id_list = ','.join([str(v) for v in validators_needing_extended_search])
                search_end_epoch = start_epoch - 1
                if search_end_epoch < oldest_epoch:
                    search_end_epoch = oldest_epoch
                extended_attestations = find_last_attestation_extended(
                    client, extended_val_id_list, oldest_epoch, search_end_epoch, latest_epoch
                )
            
            # Process aggregated results
            for row in result.result_rows:
                val_id, total_earned, total_missed, total_penalties, total_epochs, successful_attestations, last_attestation_epoch, val_balance, val_effective_balance = row
                
                # Determine last attestation info
                if val_id in validators_with_recent_attestations:
                    last_attestation_info = validators_with_recent_attestations[val_id]
                elif val_id in extended_attestations:
                    last_attestation_info = extended_attestations[val_id]
                else:
                    # Fallback if extended search failed
                    last_attestation_info = {
                        'epoch': None,
                        'timestamp': None,
                        'datetime': None,
                        'age_epochs': None,
                        'status': f'older_than_{total_days or 10}_days' if total_days else 'older_than_10_days'
                    }
                
                # Find the corresponding validator
                validator = next((v for v in batch if v['val_id'] == val_id), None)
                if validator:
                    # For validators with no attestations in reporting period, set score to 0%
                    performance_score = 0.0 if last_attestation_epoch is None else None

                    # Get balance data for this validator
                    balance_data = {
                        'val_balance': val_balance or 0,
                        'val_effective_balance': val_effective_balance or 0
                    }

                    validator_aggregates[val_id] = {
                        'val_id': val_id,
                        'node_address': validator['node_address'],
                        'minipool_address': validator['minipool_address'],
                        'pubkey': validator['pubkey'],
                        'total_earned_rewards': total_earned or 0,
                        'total_missed_rewards': total_missed or 0,
                        'total_penalties': total_penalties or 0,
                        'total_possible_rewards': (total_earned or 0) + (total_missed or 0),
                        'total_lost': (total_missed or 0) + (total_penalties or 0),
                        'total_epochs': total_epochs,
                        'successful_attestations': successful_attestations,
                        'performance_score': performance_score,
                        'last_attestation': last_attestation_info,
                        'val_balance': balance_data['val_balance'],
                        'val_balance_eth': balance_data['val_balance'] / 1e9,
                        'val_effective_balance': balance_data['val_effective_balance'],
                        'val_effective_balance_eth': balance_data['val_effective_balance'] / 1e9
                    }
            
            print(f"  Processed {len(result.result_rows)} validators")
            
        except Exception as e:
            print(f"Error querying batch {i//batch_size + 1}: {e}")
            continue
    
    print(f"Total validators processed: {len(validator_aggregates)}")
    return list(validator_aggregates.values()), start_epoch, end_epoch

def calculate_uld_status(node_address, scan_data):
    """Calculate Use Latest Delegate status for a node.
    Returns: dict with 'status' ('yes'/'no'/'partial'/'unknown') and 'count' (x/y format for partial)
    """
    if not scan_data:
        return {'status': 'unknown', 'count': None}

    # Find node in scan data
    node = next((n for n in scan_data if n['node_address'] == node_address), None)
    if not node or 'minipool_use_latest_delegate' not in node:
        return {'status': 'unknown', 'count': None}

    flags = node['minipool_use_latest_delegate']
    if not flags:
        return {'status': 'unknown', 'count': None}

    # Count True and False values (ignore None)
    total = len([f for f in flags if f is not None])
    if total == 0:
        return {'status': 'unknown', 'count': None}

    true_count = sum(1 for f in flags if f == True)
    false_count = sum(1 for f in flags if f == False)

    # Determine status
    if true_count == total:
        return {'status': 'yes', 'count': None}
    elif false_count == total:
        return {'status': 'no', 'count': None}
    else:
        return {'status': 'partial', 'count': f"{true_count}/{total}"}

def calculate_node_performance_scores(performance_data, all_rp_validators, threshold_filter=None, scan_data=None, fusaka_deaths=None):
    """Calculate performance scores grouped by node with optional threshold filtering."""
    print("Calculating node performance scores...")

    # Initialise Fusaka deaths tracking
    if fusaka_deaths is None:
        fusaka_deaths = {'validators': []}

    # Create a lookup dict for quick access
    fusaka_lookup = {v['node_address']: v for v in fusaka_deaths.get('validators', [])}
    validators_to_remove = []  # Track validators that came back online
    
    # First, get minipool counts per node from all Rocket Pool data
    node_minipool_counts = defaultdict(lambda: {'total': 0, 'active': 0, 'exited': 0})
    
    # Count total minipools per node
    for validator in all_rp_validators:
        node_addr = validator['node_address']
        node_minipool_counts[node_addr]['total'] += 1
    
    # Group aggregated performance data by node
    node_data = defaultdict(lambda: {
        'active_validators': set(),
        'total_earned_rewards': 0,
        'total_missed_rewards': 0,
        'total_penalties': 0,
        'total_possible_rewards': 0,
        'total_lost': 0,
        'last_attestations': [],
        'validator_balances': [],
        'total_balance': 0,
        'validators_below_32_eth': 0
    })
    
    # Process each aggregated performance record (only active validators)
    for record in performance_data:
        node_addr = record['node_address']
        node_data[node_addr]['active_validators'].add(record['val_id'])
        node_data[node_addr]['total_earned_rewards'] += record['total_earned_rewards']
        node_data[node_addr]['total_missed_rewards'] += record['total_missed_rewards']
        node_data[node_addr]['total_penalties'] += record['total_penalties']
        node_data[node_addr]['total_possible_rewards'] += record['total_possible_rewards']
        node_data[node_addr]['total_lost'] += record['total_lost']
        
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
    
    # Calculate scores for each node that has minipools
    node_scores = []
    for node_addr, minipool_counts in node_minipool_counts.items():
        # Only count validators that were actually found as active in the database
        # This includes validators with 'active_ongoing' or 'active_exiting' status
        active_validators = len(node_data[node_addr]['active_validators'])
        
        # All other validators are considered exited, including:
        # - Validators with 'withdrawal_done', 'exited_*' statuses
        # - Validators missing from ClickHouse database entirely
        exited_validators = minipool_counts['total'] - active_validators
        
        # Update counts
        minipool_counts['active'] = active_validators
        minipool_counts['exited'] = exited_validators
        
        # Calculate performance score (only for nodes with active validators)
        if active_validators > 0:
            # Check if any validator has a preset 0% score (no attestations in reporting period)
            zero_score_validators = [record for record in performance_data 
                                   if record['node_address'] == node_addr and record.get('performance_score') == 0.0]
            
            if zero_score_validators:
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

        # Determine if validator is back up (has made attestations in the most recent 3 epochs)
        is_back_up = False
        if (isinstance(performance_score, (int, float)) and performance_score > 0 and 
            node_last_attestation.get('status') in ['found', 'found_extended'] and
            node_last_attestation.get('age_epochs') is not None):
            # Check if validator has made attestations in the most recent 3 epochs
            is_back_up = node_last_attestation['age_epochs'] <= 3

        # Calculate ULD (Use Latest Delegate) status
        uld_info = calculate_uld_status(node_addr, scan_data)

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
            'total_minipools': minipool_counts['total'],
            'active_minipools': minipool_counts['active'],
            'exited_minipools': minipool_counts['exited'],
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

    # Sort by performance score (put N/A at the end)
    def sort_key(x):
        if x['performance_score'] == "N/A":
            return -1  # Put N/A at the end
        return x['performance_score']

    node_scores.sort(key=sort_key, reverse=True)

    # Apply threshold filter if specified (show underperforming nodes)
    if threshold_filter is not None:
        if threshold_filter == "all":
            print(f"Returning all {len(node_scores)} nodes (no threshold filter)")
            return node_scores, fusaka_deaths
        else:
            filtered_scores = []
            for node in node_scores:
                if node['performance_score'] != "N/A" and node['performance_score'] < threshold_filter:
                    filtered_scores.append(node)
            print(f"Filtered to {len(filtered_scores)} nodes below {threshold_filter}% threshold")
            return filtered_scores, fusaka_deaths

    return node_scores, fusaka_deaths

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

    # Load Fusaka deaths tracking
    fusaka_deaths = load_fusaka_deaths()
    if fusaka_deaths.get('validators'):
        print(f"Loaded {len(fusaka_deaths['validators'])} Fusaka death validators for tracking")

    # Extract validators from scan results
    validators = []
    for node in scan_results:
        if node['minipool_count'] > 0:
            for i, pubkey in enumerate(node['minipool_pubkeys']):
                if pubkey:  # Skip None values
                    validators.append({
                        'node_index': node['node_index'],
                        'node_address': node['node_address'],
                        'minipool_address': node['minipool_addresses'][i],
                        'pubkey': pubkey
                    })
    
    print(f"Starting performance analysis for {len(validators)} validators...")
    
    # Connect to ClickHouse for performance analysis
    client = connect_to_clickhouse()
    if not client:
        print("Error: Could not connect to ClickHouse.")
        return 1
    
    # Get latest epoch
    latest_epoch = client.query("SELECT MAX(epoch) FROM validators_summary").result_rows[0][0]
    print(f"Latest epoch: {latest_epoch}")
    
    # Get all validator statuses first
    all_validator_statuses, _ = get_all_validator_statuses(client, validators)
    
    # Filter for active validators only
    active_validators = get_active_validators(client, validators, all_validator_statuses)
    
    # Query attestation performance
    performance_data, start_epoch, end_epoch = query_attestation_performance(client, active_validators, latest_epoch, epochs_to_analyze)
    
    # Calculate node performance scores with threshold filtering
    node_scores, fusaka_deaths = calculate_node_performance_scores(
        performance_data, validators, threshold, scan_data=scan_results, fusaka_deaths=fusaka_deaths
    )

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    period_dir = os.path.join(output_dir, period)
    os.makedirs(period_dir, exist_ok=True)
    
    # Create validator balance lookup by pubkey for frontend
    validator_balances_lookup = {}
    for record in performance_data:
        pubkey = record.get('pubkey')
        if pubkey:
            validator_balances_lookup[pubkey] = {
                'val_id': record.get('val_id'),
                'balance_eth': record.get('val_balance_eth', 0),
                'effective_balance_eth': record.get('val_effective_balance_eth', 0)
            }

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
    parser.add_argument('--period', choices=['1day', '3day', '7day'], default='7day',
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
