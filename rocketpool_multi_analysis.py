#!/usr/bin/env python3
"""
Rocket Pool Multi-Period Multi-Threshold Analysis Script

This script generates all 20 report combinations efficiently:
- Time periods: 1day, 3day, 7day, 30day, 100day
- Performance thresholds: 80%, 90%, 95%, all

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
    connect_to_clickhouse, load_scan_data, load_megapool_scan_data,
    extract_validators_from_scan, get_all_validator_statuses,
    get_active_validators, query_attestation_performance_windows,
    calculate_node_performance_scores, load_fusaka_deaths, save_fusaka_deaths,
    load_nimbus_fork_deaths, save_nimbus_fork_deaths, build_validator_balances_lookup,
    filter_node_performance_scores
)

for stream_name in ('stdout', 'stderr'):
    stream = getattr(sys, stream_name, None)
    if stream and hasattr(stream, 'reconfigure'):
        stream.reconfigure(line_buffering=True)

def run_multi_analysis(output_dir='reports'):
    """Generate all report combinations efficiently."""
    print("=== ROCKET POOL MULTI-PERIOD MULTI-THRESHOLD ANALYSIS ===")
    print("Loading existing scan data...")
    
    # Load scan results (shared across all reports)
    scan_results = load_scan_data()
    if not scan_results:
        print("Error: Could not load scan data")
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

    # Extract validators from both scan files (shared across all reports)
    minipool_validators = extract_validators_from_scan(scan_results, 'minipool')
    megapool_validators = extract_validators_from_scan(megapool_scan_results, 'megapool')
    validators = minipool_validators + megapool_validators

    print(f"Starting performance analysis for {len(validators)} validators "
          f"({len(minipool_validators)} minipool, {len(megapool_validators)} megapool)...")
    
    # Connect to ClickHouse (shared connection)
    client = connect_to_clickhouse()
    if not client:
        print("Error: Could not connect to ClickHouse.")
        return 1
    
    # Get latest epoch and all validator statuses (shared across all reports)
    all_validator_statuses, latest_epoch = get_all_validator_statuses(client, validators)
    print(f"Latest epoch: {latest_epoch}")

    active_validators = get_active_validators(client, validators, all_validator_statuses)
    
    print(f"Found {len(active_validators)} active validators")

    # Create output directory structure
    os.makedirs(output_dir, exist_ok=True)
    
    # Query shared performance data once, then fan it out per period/threshold.
    reports_generated = []
    print(f"\nGenerating {len(ANALYSIS_PERIODS)} × {len(PERFORMANCE_THRESHOLDS)} = {len(ANALYSIS_PERIODS) * len(PERFORMANCE_THRESHOLDS)} reports...")

    print("\nQuerying shared performance data for all periods...")
    performance_by_period, period_windows = query_attestation_performance_windows(
        client,
        active_validators,
        all_validator_statuses,
        latest_epoch,
        periods=list(ANALYSIS_PERIODS.keys())
    )
    validator_balances_lookup = build_validator_balances_lookup(active_validators, all_validator_statuses)

    for period_name, epochs_to_analyze in ANALYSIS_PERIODS.items():
        print(f"\n--- Processing {period_name.upper()} period ({epochs_to_analyze} epochs) ---")

        # Create period directory
        period_dir = os.path.join(output_dir, period_name)
        os.makedirs(period_dir, exist_ok=True)

        performance_data = performance_by_period[period_name]
        period_window = period_windows[period_name]
        start_epoch = period_window['start_epoch']
        end_epoch = period_window['end_epoch']

        node_scores_all, fusaka_deaths, nimbus_fork_deaths = calculate_node_performance_scores(
            performance_data, validators, None, scan_results, fusaka_deaths, nimbus_fork_deaths,
            megapool_scan_data=megapool_scan_results,
            validator_statuses=all_validator_statuses
        )

        for threshold in PERFORMANCE_THRESHOLDS:
            if threshold == "all":
                print(f"  Generating {period_name} @ all nodes (no threshold filter)...")
                node_scores = node_scores_all
            else:
                print(f"  Generating {period_name} @ <{threshold}% (underperforming) threshold...")
                node_scores = filter_node_performance_scores(node_scores_all, threshold)

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
                'validator_statuses': all_validator_statuses,
                'validator_balances': validator_balances_lookup
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

    # Save updated Fusaka deaths tracking
    save_fusaka_deaths(fusaka_deaths)

    # Save updated Nimbus fork deaths tracking
    save_nimbus_fork_deaths(nimbus_fork_deaths)

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

    megapool_scan_file = 'rocketpool_megapool_scan_results.json'
    if os.path.exists(megapool_scan_file):
        dest_file = os.path.join(output_dir, megapool_scan_file)
        shutil.copy2(megapool_scan_file, dest_file)
        print(f"Megapool scan data copied to {dest_file}")
    else:
        print(f"Note: {megapool_scan_file} not found - megapool data will not be available in reports")

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
