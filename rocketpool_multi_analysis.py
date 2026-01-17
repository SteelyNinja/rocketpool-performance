#!/usr/bin/env python3
"""
Rocket Pool Multi-Period Multi-Threshold Analysis Script

This script generates all 9 report combinations efficiently:
- Time periods: 1day, 3day, 7day
- Performance thresholds: 80%, 90%, 95%

The script reuses scan data and shares ClickHouse queries for optimal performance.
"""

import json
import clickhouse_connect
from datetime import datetime
from collections import defaultdict
import os
import sys

# Import functions from the modified analysis script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from rocketpool_analysis_only import (
    ANALYSIS_PERIODS, PERFORMANCE_THRESHOLDS,
    connect_to_clickhouse, load_scan_data, get_all_validator_statuses,
    get_active_validators, query_attestation_performance,
    calculate_node_performance_scores
)

def run_multi_analysis(output_dir='reports'):
    """Generate all 9 report combinations efficiently."""
    print("=== ROCKET POOL MULTI-PERIOD MULTI-THRESHOLD ANALYSIS ===")
    print("Loading existing scan data...")
    
    # Load existing scan results (shared across all reports)
    scan_results = load_scan_data()
    if not scan_results:
        print("Error: Could not load scan data")
        return 1
    
    print(f"Loaded scan data for {len(scan_results)} nodes")
    
    # Extract validators from scan results (shared across all reports)
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
    
    # Connect to ClickHouse (shared connection)
    client = connect_to_clickhouse()
    if not client:
        print("Error: Could not connect to ClickHouse.")
        return 1
    
    # Get latest epoch and all validator statuses (shared across all reports)
    latest_epoch = client.query("SELECT MAX(epoch) FROM validators_summary").result_rows[0][0]
    print(f"Latest epoch: {latest_epoch}")
    
    all_validator_statuses, _ = get_all_validator_statuses(client, validators)
    active_validators = get_active_validators(client, validators)
    
    print(f"Found {len(active_validators)} active validators")

    # Create output directory structure
    os.makedirs(output_dir, exist_ok=True)
    
    # Prepare data structures for efficient processing
    performance_cache = {}  # Cache performance data by period
    reports_generated = []
    
    print(f"\nGenerating {len(ANALYSIS_PERIODS)} × {len(PERFORMANCE_THRESHOLDS)} = {len(ANALYSIS_PERIODS) * len(PERFORMANCE_THRESHOLDS)} reports...")
    
    # Process each time period
    for period_name, epochs_to_analyze in ANALYSIS_PERIODS.items():
        print(f"\n--- Processing {period_name.upper()} period ({epochs_to_analyze} epochs) ---")
        
        # Create period directory
        period_dir = os.path.join(output_dir, period_name)
        os.makedirs(period_dir, exist_ok=True)
        
        # Query performance data for this period (shared across all thresholds)
        if period_name not in performance_cache:
            print(f"Querying performance data for {period_name}...")
            performance_data, start_epoch, end_epoch = query_attestation_performance(
                client, active_validators, latest_epoch, epochs_to_analyze
            )
            performance_cache[period_name] = {
                'data': performance_data,
                'start_epoch': start_epoch,
                'end_epoch': end_epoch
            }
        else:
            performance_data = performance_cache[period_name]['data']
            start_epoch = performance_cache[period_name]['start_epoch']
            end_epoch = performance_cache[period_name]['end_epoch']
        
        # Process each threshold for this period
        for threshold in PERFORMANCE_THRESHOLDS:
            if threshold == "all":
                print(f"  Generating {period_name} @ all nodes (no threshold filter)...")
                threshold_desc = "all"
            else:
                print(f"  Generating {period_name} @ <{threshold}% (underperforming) threshold...")
                threshold_desc = f"under {threshold}%"
            
            # Calculate node performance scores with threshold filtering
            node_scores = calculate_node_performance_scores(
                performance_data, validators, threshold, scan_results
            )

            # Save performance analysis
            output_data = {
                'analysis_date': datetime.now().isoformat(),
                'period': period_name,
                'threshold': threshold,
                'epochs_analyzed': epochs_to_analyze,
                'start_epoch': start_epoch,
                'end_epoch': end_epoch,
                'total_nodes': len(node_scores),
                'node_performance_scores': node_scores,
                'validator_statuses': all_validator_statuses
            }
            
            filename = os.path.join(period_dir, f'performance_{threshold}.json')
            with open(filename, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
            
            reports_generated.append({
                'period': period_name,
                'threshold': threshold,
                'filename': filename,
                'nodes_count': len(node_scores)
            })

            if threshold == "all":
                print(f"    → {len(node_scores)} total nodes")
            else:
                print(f"    → {len(node_scores)} underperforming nodes")

    # Generate summary report
    generate_summary_report(reports_generated, output_dir)
    
    # Copy scan data to reports directory for website access
    copy_scan_data_to_reports(output_dir)
    
    # Print final summary
    print(f"\n=== MULTI-ANALYSIS COMPLETE ===")
    print(f"Reports generated: {len(reports_generated)}")
    print(f"Output directory: {output_dir}")
    print(f"Directory structure:")
    for report in reports_generated:
        print(f"  {report['period']}/performance_{report['threshold']}.json")
    print(f"  rocketpool_scan_results.json (ENS/withdrawal data)")
    print(f"  summary.json")
    
    return 0

def copy_scan_data_to_reports(output_dir):
    """Copy scan data to reports directory for website access."""
    import shutil
    
    scan_file = 'rocketpool_scan_results.json'
    if os.path.exists(scan_file):
        dest_file = os.path.join(output_dir, scan_file)
        shutil.copy2(scan_file, dest_file)
        print(f"Scan data copied to {dest_file}")
    else:
        print(f"Warning: {scan_file} not found - ENS names and withdrawal addresses will not be available")

def generate_summary_report(reports_generated, output_dir):
    """Generate a summary report of all generated analyses."""
    summary_data = {
        'generation_date': datetime.now().isoformat(),
        'total_reports': len(reports_generated),
        'reports': reports_generated,
        'periods': list(ANALYSIS_PERIODS.keys()),
        'thresholds': PERFORMANCE_THRESHOLDS
    }
    
    summary_file = os.path.join(output_dir, 'summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary_data, f, indent=2, default=str)
    
    print(f"Summary report saved to {summary_file}")

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate all Rocket Pool performance reports')
    parser.add_argument('--output-dir', default='reports',
                       help='Output directory for reports (default: reports)')
    
    args = parser.parse_args()
    
    try:
        return run_multi_analysis(args.output_dir)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())