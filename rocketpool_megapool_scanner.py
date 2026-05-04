#!/usr/bin/env python3
"""
Rocket Pool Megapool Scanner (Saturn-1)

Scans all Rocket Pool nodes for deployed megapools and collects their
validator public keys. Writes rocketpool_megapool_scan_results.json,
which rocketpool_analysis_only.py and rocketpool_multi_analysis.py
consume alongside the existing minipool scan results.

Each node entry in the output represents one node that has deployed a
megapool. Nodes without megapools are omitted.
"""

from web3 import Web3
from multicall import Call
from tqdm import tqdm
import json

# rocketStorage is the permanent registry address - never changes across upgrades.
ROCKET_STORAGE = '0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46'

# The minipool scanner hardcodes NODE_MANAGER, but Saturn-1 deployed a new
# NodeManager that includes getMegapoolAddress(). We resolve it dynamically.
# MINIPOOL_MANAGER is still needed to enumerate nodes (getNodeCount / getNodeAt).
MINIPOOL_MANAGER_LEGACY = '0xF82991Bd8976c243eB3b7CDDc52AB0Fc8dc1246C'

ETH_RPC_URL = 'http://192.168.202.2:8545'

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'


def connect_to_ethereum():
    print(f"Connecting to Ethereum client at {ETH_RPC_URL}...")
    w3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum client at {ETH_RPC_URL}")
    print(f"Connected successfully! Latest block: {w3.eth.block_number}")
    return w3


def resolve_contract_address(w3, contract_name):
    """Resolve a Rocket Pool contract address from rocketStorage."""
    key = w3.keccak(text=f"contract.address{contract_name}")
    addr = Call(ROCKET_STORAGE, ['getAddress(bytes32)(address)', key], _w3=w3)()
    if not addr or addr.lower() == ZERO_ADDRESS:
        raise RuntimeError(f"Could not resolve contract address for '{contract_name}' from rocketStorage")
    return addr


def get_megapool_address(w3, node_manager, node_address):
    """Return the megapool address for a node, or None if not deployed."""
    try:
        addr = Call(node_manager, ['getMegapoolAddress(address)(address)', node_address], _w3=w3)()
        if not addr or addr.lower() == ZERO_ADDRESS:
            return None
        return addr
    except Exception as e:
        print(f"Warning: getMegapoolAddress failed for {node_address}: {e}")
        return None


def get_validator_count(w3, megapool_address):
    """Return total validator count for a megapool (all statuses)."""
    try:
        return Call(megapool_address, 'getValidatorCount()(uint32)', _w3=w3)()
    except Exception as e:
        print(f"Warning: getValidatorCount failed for {megapool_address}: {e}")
        return 0


def get_validator_pubkey(w3, megapool_address, validator_id):
    """Return the validator pubkey (hex string, no 0x prefix) for a given index."""
    try:
        pubkey_bytes = Call(
            megapool_address,
            ['getValidatorPubkey(uint32)(bytes)', validator_id],
            _w3=w3
        )()
        return pubkey_bytes.hex() if pubkey_bytes else None
    except Exception as e:
        print(f"Warning: getValidatorPubkey({validator_id}) failed for {megapool_address}: {e}")
        return None


def get_use_latest_delegate(w3, megapool_address):
    """Return the Use Latest Delegate flag for a megapool (single flag covers all validators)."""
    try:
        return Call(megapool_address, 'getUseLatestDelegate()(bool)', _w3=w3)()
    except Exception as e:
        print(f"Warning: getUseLatestDelegate failed for {megapool_address}: {e}")
        return None


def get_ens_name(w3, address):
    try:
        return w3.ens.name(address)
    except Exception:
        return None


def get_withdrawal_addresses(w3, node_manager, node_address):
    try:
        primary = Call(node_manager, ['getNodeWithdrawalAddress(address)(address)', node_address], _w3=w3)()
        rpl = Call(node_manager, ['getNodeRPLWithdrawalAddress(address)(address)', node_address], _w3=w3)()
        return {'primary_withdrawal': primary, 'rpl_withdrawal': rpl}
    except Exception as e:
        print(f"Warning: Could not get withdrawal addresses for {node_address}: {e}")
        return {'primary_withdrawal': None, 'rpl_withdrawal': None}


def scan_megapools(w3, node_manager):
    """Scan all nodes for megapools and collect their validator pubkeys."""
    print("Fetching node count...")
    node_count = Call(node_manager, 'getNodeCount()(uint256)', _w3=w3)()
    print(f"Found {node_count} nodes in Rocket Pool network")

    results = []
    nodes_with_megapool = 0

    for node_idx in tqdm(range(node_count), desc="Scanning nodes for megapools"):
        try:
            node_addr = Call(node_manager, ['getNodeAt(uint256)(address)', node_idx], _w3=w3)()

            megapool_addr = get_megapool_address(w3, node_manager, node_addr)
            if not megapool_addr:
                continue

            nodes_with_megapool += 1

            ens_name = get_ens_name(w3, node_addr)
            withdrawal = get_withdrawal_addresses(w3, node_manager, node_addr)

            primary_withdrawal_ens = None
            rpl_withdrawal_ens = None
            if withdrawal['primary_withdrawal'] and withdrawal['primary_withdrawal'] != node_addr:
                primary_withdrawal_ens = get_ens_name(w3, withdrawal['primary_withdrawal'])
            if (withdrawal['rpl_withdrawal']
                    and withdrawal['rpl_withdrawal'] != node_addr
                    and withdrawal['rpl_withdrawal'] != withdrawal['primary_withdrawal']):
                rpl_withdrawal_ens = get_ens_name(w3, withdrawal['rpl_withdrawal'])

            validator_count = get_validator_count(w3, megapool_addr)
            pubkeys = []
            for vid in range(validator_count):
                pubkey = get_validator_pubkey(w3, megapool_addr, vid)
                pubkeys.append(pubkey)

            uld = get_use_latest_delegate(w3, megapool_addr)

            ens_display = f" ({ens_name})" if ens_name else ""
            print(
                f"Node {node_idx}: {node_addr}{ens_display} → megapool {megapool_addr} "
                f"with {validator_count} validators"
            )

            results.append({
                'node_index': node_idx,
                'node_address': node_addr,
                'ens_name': ens_name,
                'primary_withdrawal_address': withdrawal['primary_withdrawal'],
                'primary_withdrawal_ens': primary_withdrawal_ens,
                'rpl_withdrawal_address': withdrawal['rpl_withdrawal'],
                'rpl_withdrawal_ens': rpl_withdrawal_ens,
                'megapool_address': megapool_addr,
                'megapool_validator_count': validator_count,
                'megapool_validator_pubkeys': pubkeys,
                'megapool_use_latest_delegate': uld
            })

        except Exception as e:
            print(f"Error processing node {node_idx}: {e}")
            continue

    print(f"\nNodes with megapools: {nodes_with_megapool} of {node_count}")
    return results


def save_results(data, filename='rocketpool_megapool_scan_results.json'):
    print(f"Saving results to {filename}...")
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Results saved to {filename}")


def print_summary(data):
    total_validators = sum(n['megapool_validator_count'] for n in data)
    pubkeys_found = sum(1 for n in data for p in n['megapool_validator_pubkeys'] if p)
    uld_yes = sum(1 for n in data if n['megapool_use_latest_delegate'] is True)
    uld_no = sum(1 for n in data if n['megapool_use_latest_delegate'] is False)

    print("\n" + "=" * 60)
    print("ROCKET POOL MEGAPOOL SCAN SUMMARY")
    print("=" * 60)
    print(f"Nodes with megapools: {len(data)}")
    print(f"Total megapool validators: {total_validators}")
    print(f"Validator pubkeys resolved: {pubkeys_found}")
    print(f"Use Latest Delegate: yes={uld_yes}, no={uld_no}")


def main():
    try:
        w3 = connect_to_ethereum()

        print("Resolving rocketNodeManager address from rocketStorage...")
        node_manager = resolve_contract_address(w3, 'rocketNodeManager')
        print(f"rocketNodeManager: {node_manager}")

        results = scan_megapools(w3, node_manager)
        save_results(results)
        print_summary(results)

        print("\n=== MEGAPOOL SCAN COMPLETE ===")
        print("Data saved to: rocketpool_megapool_scan_results.json")
        print("Run performance analysis with:")
        print("  python rocketpool_multi_analysis.py")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
