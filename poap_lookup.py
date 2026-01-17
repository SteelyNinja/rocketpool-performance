#!/usr/bin/env python3
"""
POAP Lookup Script for Rocket Pool Addresses
Queries Gnosis Chain to check if addresses have any POAPs
"""

import json
import time
import requests
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class POAPInfo:
    """Store POAP information for an address"""
    address: str
    poap_count: int
    has_poaps: bool
    last_checked: str
    error: Optional[str] = None

class POAPLookup:
    """Lookup POAP balances for addresses using Gnosis Chain RPC"""
    
    def __init__(self):
        # Multiple RPC endpoints for redundancy
        self.rpc_endpoints = [
            "https://gnosis-mainnet.public.blastapi.io",  # Blast API (primary)
            "https://rpc.ankr.com/gnosis",  # Ankr (fallback)
            "https://rpc.gnosischain.com",  # Official (backup)
        ]
        
        # POAP contract on Gnosis Chain
        self.poap_contract_address = "0x22c1f6050e56d2876009903609a2cc3fef83b415"
        
        # Rate limiting settings (optimized)
        self.requests_per_second = 8
        self.batch_size = 100
        self.delay_between_batches = 2  # seconds
        
        # Current endpoint index
        self.current_endpoint = 0
        
    def get_current_endpoint(self) -> str:
        """Get the current RPC endpoint"""
        return self.rpc_endpoints[self.current_endpoint]
    
    def switch_endpoint(self):
        """Switch to the next RPC endpoint"""
        self.current_endpoint = (self.current_endpoint + 1) % len(self.rpc_endpoints)
        logger.info(f"Switched to endpoint: {self.get_current_endpoint()}")
    
    def make_rpc_call(self, method: str, params: List) -> Optional[dict]:
        """Make an RPC call with error handling and endpoint switching"""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        
        max_retries = len(self.rpc_endpoints)
        
        for attempt in range(max_retries):
            try:
                endpoint = self.get_current_endpoint()
                response = requests.post(
                    endpoint, 
                    json=payload, 
                    timeout=30,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if "result" in data:
                        return data["result"]
                    elif "error" in data:
                        logger.error(f"RPC error from {endpoint}: {data['error']}")
                elif response.status_code == 429:
                    logger.warning(f"Rate limited by {endpoint}, switching...")
                else:
                    logger.error(f"HTTP {response.status_code} from {endpoint}")
                
            except Exception as e:
                logger.error(f"Error calling {self.get_current_endpoint()}: {e}")
            
            # Switch to next endpoint and try again
            self.switch_endpoint()
            time.sleep(2)  # Brief delay before retry
        
        logger.error(f"All endpoints failed for {method}")
        return None
    
    def get_poap_balance(self, address: str) -> Optional[int]:
        """Get POAP balance for a specific address"""
        # Prepare balanceOf call data
        # balanceOf(address) function signature: 0x70a08231
        # Pad address to 32 bytes
        call_data = "0x70a08231" + address[2:].zfill(64)
        
        result = self.make_rpc_call("eth_call", [
            {
                "to": self.poap_contract_address,
                "data": call_data
            },
            "latest"
        ])
        
        if result:
            try:
                # Convert hex result to integer
                balance = int(result, 16)
                return balance
            except ValueError as e:
                logger.error(f"Error parsing balance for {address}: {e}")
                return None
        
        return None
    
    def load_addresses_from_scan_data(self) -> Set[str]:
        """Load unique addresses from scan results"""
        try:
            with open('reports/rocketpool_scan_results.json', 'r') as f:
                scan_data = json.load(f)
            
            addresses = set()
            
            for node in scan_data:
                # Add node address
                addresses.add(node['node_address'].lower())
                
                # Add withdrawal addresses if different
                if node.get('primary_withdrawal_address'):
                    addr = node['primary_withdrawal_address'].lower()
                    if addr != node['node_address'].lower():
                        addresses.add(addr)
                
                if node.get('rpl_withdrawal_address'):
                    addr = node['rpl_withdrawal_address'].lower()
                    if addr != node['node_address'].lower():
                        addresses.add(addr)
            
            logger.info(f"Loaded {len(addresses)} unique addresses")
            return addresses
            
        except FileNotFoundError:
            logger.error("Scan results file not found")
            return set()
        except Exception as e:
            logger.error(f"Error loading addresses: {e}")
            return set()
    
    def load_existing_results(self) -> Dict[str, POAPInfo]:
        """Load existing POAP results if available"""
        try:
            with open('reports/poap_results.json', 'r') as f:
                data = json.load(f)
                results = {}
                for addr, info in data.items():
                    results[addr] = POAPInfo(
                        address=info['address'],
                        poap_count=info['poap_count'],
                        has_poaps=info['has_poaps'],
                        last_checked=info['last_checked'],
                        error=info.get('error')
                    )
                logger.info(f"Loaded {len(results)} existing POAP results")
                return results
        except FileNotFoundError:
            logger.info("No existing POAP results found")
            return {}
        except Exception as e:
            logger.error(f"Error loading existing results: {e}")
            return {}
    
    def save_results(self, results: Dict[str, POAPInfo]):
        """Save POAP results to JSON file"""
        try:
            data = {}
            for addr, info in results.items():
                data[addr] = {
                    'address': info.address,
                    'poap_count': info.poap_count,
                    'has_poaps': info.has_poaps,
                    'last_checked': info.last_checked,
                    'error': info.error
                }
            
            with open('reports/poap_results.json', 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Saved {len(results)} POAP results")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def run_lookup(self):
        """Main function to run POAP lookup"""
        logger.info("Starting POAP lookup...")
        
        # Load addresses and existing results
        addresses = self.load_addresses_from_scan_data()
        results = self.load_existing_results()
        
        if not addresses:
            logger.error("No addresses to check")
            return
        
        # Filter addresses that need checking (not already checked recently)
        addresses_to_check = []
        for addr in addresses:
            if addr not in results:
                addresses_to_check.append(addr)
        
        logger.info(f"Need to check {len(addresses_to_check)} new addresses")
        logger.info(f"Already have results for {len(results)} addresses")
        
        if not addresses_to_check:
            logger.info("All addresses already checked!")
            return
        
        # Process in batches
        total_checked = 0
        successful_checks = 0
        
        for i in range(0, len(addresses_to_check), self.batch_size):
            batch = addresses_to_check[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(addresses_to_check) + self.batch_size - 1)//self.batch_size}")
            
            for addr in batch:
                try:
                    # Rate limiting
                    time.sleep(1.0 / self.requests_per_second)
                    
                    balance = self.get_poap_balance(addr)
                    
                    if balance is not None:
                        results[addr] = POAPInfo(
                            address=addr,
                            poap_count=balance,
                            has_poaps=balance > 0,
                            last_checked=time.strftime('%Y-%m-%d %H:%M:%S')
                        )
                        successful_checks += 1
                        
                        if balance > 0:
                            logger.info(f"✨ {addr} has {balance} POAPs!")
                    else:
                        results[addr] = POAPInfo(
                            address=addr,
                            poap_count=0,
                            has_poaps=False,
                            last_checked=time.strftime('%Y-%m-%d %H:%M:%S'),
                            error="Failed to fetch balance"
                        )
                    
                    total_checked += 1
                    
                    # Progress update
                    if total_checked % 100 == 0:
                        logger.info(f"Progress: {total_checked}/{len(addresses_to_check)} addresses checked")
                
                except Exception as e:
                    logger.error(f"Error checking {addr}: {e}")
                    results[addr] = POAPInfo(
                        address=addr,
                        poap_count=0,
                        has_poaps=False,
                        last_checked=time.strftime('%Y-%m-%d %H:%M:%S'),
                        error=str(e)
                    )
                    total_checked += 1
            
            # Save progress after each batch
            self.save_results(results)
            
            # Delay between batches
            if i + self.batch_size < len(addresses_to_check):
                logger.info(f"Sleeping {self.delay_between_batches} seconds between batches...")
                time.sleep(self.delay_between_batches)
        
        # Final save and summary
        self.save_results(results)
        
        # Summary statistics
        total_with_poaps = sum(1 for r in results.values() if r.has_poaps)
        total_poap_count = sum(r.poap_count for r in results.values() if r.poap_count > 0)
        
        logger.info("=== POAP Lookup Complete ===")
        logger.info(f"Total addresses checked: {len(results)}")
        logger.info(f"Addresses with POAPs: {total_with_poaps}")
        logger.info(f"Total POAPs found: {total_poap_count}")
        logger.info(f"Success rate: {successful_checks}/{total_checked} ({successful_checks/total_checked*100:.1f}%)")

if __name__ == "__main__":
    lookup = POAPLookup()
    lookup.run_lookup()