#!/usr/bin/env python3
"""
Rocket Pool Node and Minipool Scanner

This script connects to an Ethereum client and scans all Rocket Pool nodes
and their associated minipools, collecting minipool addresses and public keys.
The scan data is saved to rocketpool_scan_results.json for use by analysis scripts.
"""

from web3 import Web3
from multicall import Call
from tqdm import tqdm
import json

# Rocket Pool contract addresses (mainnet)
NODE_MANAGER = '0x2b52479F6ea009907e46fc43e91064D1b92Fdc86'
MINIPOOL_MANAGER = '0xF82991Bd8976c243eB3b7CDDc52AB0Fc8dc1246C'

# Connect to local Ethereum client
ETH_RPC_URL = 'http://192.168.202.2:8545'

def connect_to_ethereum():
    """Connect to the Ethereum client and verify connection."""
    print(f"Connecting to Ethereum client at {ETH_RPC_URL}...")
    w3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
    
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum client at {ETH_RPC_URL}")
    
    print(f"Connected successfully! Latest block: {w3.eth.block_number}")
    return w3

def get_minipool_pubkey(w3, minipool_address):
    """Get the public key for a minipool."""
    try:
        # Call the getMinipoolPubkey function on the RocketMinipoolManager contract
        pubkey_call = Call(MINIPOOL_MANAGER, ['getMinipoolPubkey(address)(bytes)', minipool_address], _w3=w3)()
        return pubkey_call.hex() if pubkey_call else None
    except Exception as e:
        print(f"Warning: Could not get pubkey for minipool {minipool_address}: {e}")
        return None

def get_use_latest_delegate(w3, minipool_address):
    """Get the use latest delegate flag for a minipool."""
    try:
        # Call the getUseLatestDelegate function on the minipool contract
        use_latest = Call(minipool_address, 'getUseLatestDelegate()(bool)', _w3=w3)()
        return use_latest
    except Exception as e:
        print(f"Warning: Could not get use_latest_delegate for minipool {minipool_address}: {e}")
        return None

def get_ens_name(w3, address):
    """Get ENS name for an address if it exists."""
    try:
        # Convert address to ENS name using reverse resolution
        ens_name = w3.ens.name(address)
        return ens_name
    except Exception as e:
        # ENS lookup failed - address likely doesn't have an ENS record
        return None

def get_withdrawal_addresses(w3, node_address):
    """Get withdrawal addresses for a node."""
    try:
        # Primary withdrawal address
        primary_withdrawal = Call(NODE_MANAGER, ['getNodeWithdrawalAddress(address)(address)', node_address], _w3=w3)()
        
        # RPL withdrawal address
        rpl_withdrawal = Call(NODE_MANAGER, ['getNodeRPLWithdrawalAddress(address)(address)', node_address], _w3=w3)()
        
        return {
            'primary_withdrawal': primary_withdrawal,
            'rpl_withdrawal': rpl_withdrawal
        }
    except Exception as e:
        print(f"Warning: Could not get withdrawal addresses for node {node_address}: {e}")
        return {
            'primary_withdrawal': None,
            'rpl_withdrawal': None
        }

def scan_rocket_pool_nodes(w3):
    """Scan all Rocket Pool nodes and their minipools."""
    print("Starting Rocket Pool node scan...")
    
    # Get total node count
    node_count = Call(NODE_MANAGER, 'getNodeCount()(uint256)', _w3=w3)()
    print(f"Found {node_count} nodes in Rocket Pool network")
    
    all_nodes_data = []
    ens_found_count = 0
    
    # Iterate through all nodes
    for node_idx in tqdm(range(node_count), desc="Scanning nodes"):
        try:
            # Get node address
            node_addr = Call(NODE_MANAGER, ['getNodeAt(uint256)(address)', node_idx], _w3=w3)()
            
            # Get ENS name for this node address
            ens_name = get_ens_name(w3, node_addr)
            if ens_name:
                ens_found_count += 1
                
            # Get withdrawal addresses for this node
            withdrawal_data = get_withdrawal_addresses(w3, node_addr)
            
            # Get ENS names for withdrawal addresses if they exist and are different
            primary_withdrawal_ens = None
            rpl_withdrawal_ens = None
            
            if withdrawal_data['primary_withdrawal'] and withdrawal_data['primary_withdrawal'] != node_addr:
                primary_withdrawal_ens = get_ens_name(w3, withdrawal_data['primary_withdrawal'])
                
            if withdrawal_data['rpl_withdrawal'] and withdrawal_data['rpl_withdrawal'] != node_addr and withdrawal_data['rpl_withdrawal'] != withdrawal_data['primary_withdrawal']:
                rpl_withdrawal_ens = get_ens_name(w3, withdrawal_data['rpl_withdrawal'])
            
            # Get minipool count for this node
            mp_count = Call(MINIPOOL_MANAGER, ['getNodeMinipoolCount(address)(uint256)', node_addr], _w3=w3)()
            
            # Get all minipool addresses for this node
            mp_addresses = []
            mp_pubkeys = []
            mp_use_latest_delegate = []

            for mp_idx in range(mp_count):
                mp_addr = Call(MINIPOOL_MANAGER, ['getNodeMinipoolAt(address,uint256)(address)', node_addr, mp_idx], _w3=w3)()
                mp_addresses.append(mp_addr)

                # Get the public key for this minipool
                pubkey = get_minipool_pubkey(w3, mp_addr)
                mp_pubkeys.append(pubkey)

                # Get the use latest delegate flag for this minipool
                use_latest = get_use_latest_delegate(w3, mp_addr)
                mp_use_latest_delegate.append(use_latest)
            
            node_data = {
                'node_index': node_idx,
                'node_address': node_addr,
                'ens_name': ens_name,
                'primary_withdrawal_address': withdrawal_data['primary_withdrawal'],
                'primary_withdrawal_ens': primary_withdrawal_ens,
                'rpl_withdrawal_address': withdrawal_data['rpl_withdrawal'],
                'rpl_withdrawal_ens': rpl_withdrawal_ens,
                'minipool_count': mp_count,
                'minipool_addresses': mp_addresses,
                'minipool_pubkeys': mp_pubkeys,
                'minipool_use_latest_delegate': mp_use_latest_delegate
            }
            
            all_nodes_data.append(node_data)
            
            # Print progress for nodes with minipools
            if mp_count > 0:
                # Show first pubkey as sample
                first_pubkey = mp_pubkeys[0] if mp_pubkeys and mp_pubkeys[0] else "None"
                ens_display = f" ({ens_name})" if ens_name else ""
                print(f"Node {node_idx}: {node_addr}{ens_display} has {mp_count} minipools (sample pubkey: {first_pubkey[:20]}...)" if first_pubkey != "None" else f"Node {node_idx}: {node_addr}{ens_display} has {mp_count} minipools")
                
        except Exception as e:
            print(f"Error processing node {node_idx}: {e}")
            continue
    
    print(f"ENS records found: {ens_found_count}")
    return all_nodes_data, ens_found_count

def save_results(data, filename='rocketpool_scan_results.json'):
    """Save scan results to a JSON file."""
    print(f"Saving results to {filename}...")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Results saved to {filename}")

def print_summary(data, ens_count):
    """Print a summary of the scan results."""
    total_nodes = len(data)
    nodes_with_minipools = len([n for n in data if n['minipool_count'] > 0])
    total_minipools = sum(n['minipool_count'] for n in data)

    # Withdrawal address statistics
    nodes_with_different_primary = len([n for n in data if n['primary_withdrawal_address'] and n['primary_withdrawal_address'] != n['node_address']])
    nodes_with_different_rpl = len([n for n in data if n['rpl_withdrawal_address'] and n['rpl_withdrawal_address'] != n['node_address']])

    # ENS statistics for withdrawal addresses
    primary_withdrawal_ens_count = len([n for n in data if n['primary_withdrawal_ens']])
    rpl_withdrawal_ens_count = len([n for n in data if n['rpl_withdrawal_ens']])

    # Delegate flag statistics
    all_delegate_flags = [flag for n in data for flag in n.get('minipool_use_latest_delegate', [])]
    valid_delegate_flags = [flag for flag in all_delegate_flags if flag is not None]
    use_latest_count = sum(1 for flag in valid_delegate_flags if flag == True)
    use_pinned_count = sum(1 for flag in valid_delegate_flags if flag == False)

    print("\n" + "="*60)
    print("ROCKET POOL SCAN SUMMARY")
    print("="*60)
    print(f"Total nodes scanned: {total_nodes}")
    print(f"Nodes with minipools: {nodes_with_minipools}")
    print(f"Total minipools found: {total_minipools}")
    print(f"Average minipools per active node: {total_minipools/nodes_with_minipools:.2f}" if nodes_with_minipools > 0 else "No active nodes found")
    print()
    print("ENS RECORDS FOUND:")
    print(f"  Node addresses: {ens_count}")
    print(f"  Primary withdrawal addresses: {primary_withdrawal_ens_count}")
    print(f"  RPL withdrawal addresses: {rpl_withdrawal_ens_count}")
    print()
    print("WITHDRAWAL ADDRESS USAGE:")
    print(f"  Different primary withdrawal: {nodes_with_different_primary} ({nodes_with_different_primary/total_nodes*100:.1f}%)")
    print(f"  Different RPL withdrawal: {nodes_with_different_rpl} ({nodes_with_different_rpl/total_nodes*100:.1f}%)")
    print()
    print("DELEGATE CONFIGURATION:")
    if valid_delegate_flags:
        print(f"  Using latest delegate: {use_latest_count} ({use_latest_count/len(valid_delegate_flags)*100:.1f}%)")
        print(f"  Using pinned delegate: {use_pinned_count} ({use_pinned_count/len(valid_delegate_flags)*100:.1f}%)")
    else:
        print(f"  No delegate flag data available")

def main():
    """Main function to run the Rocket Pool scanner."""
    try:
        # Connect to Ethereum
        w3 = connect_to_ethereum()
        
        # Scan all nodes
        results, ens_count = scan_rocket_pool_nodes(w3)
        
        # Save scan results
        save_results(results)
        
        # Print summary
        print_summary(results, ens_count)
        
        print(f"\n=== SCAN COMPLETE ===")
        print(f"Scan data saved to: rocketpool_scan_results.json")
        print(f"Run performance analysis with:")
        print(f"  python rocketpool_analysis_only.py     # Single report")
        print(f"  python rocketpool_multi_analysis.py    # All 9 reports")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())