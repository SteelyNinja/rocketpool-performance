# Rocket Pool Performance Dashboard

A comprehensive performance analysis and monitoring dashboard for Rocket Pool node operators and validators on Ethereum.

## Overview

This project provides real-time performance tracking, analysis, and visualization for Rocket Pool validators. It queries the Ethereum blockchain and beacon chain data to generate detailed performance reports across multiple time periods and performance thresholds.

## Features

### Analysis Engine
- **Multi-Period Analysis**: Track performance over 1 day, 3 days, or 7 days
- **Multi-Threshold Filtering**: View nodes below 80%, 90%, 95% performance, or all nodes
- **Automated Reporting**: Generates 12 comprehensive performance reports in a single run
- **Efficient Data Collection**: Uses multicall for batch blockchain queries
- **ClickHouse Integration**: Fast attestation data queries from beacon chain database

### Interactive Web Dashboard
- **Modern Glassmorphism Design**: Responsive UI with dark/light themes
- **Real-time Filtering**: Toggle zero performance nodes, backup validators, and Fusaka deaths
- **Search & Sort**: Find specific nodes, sort by performance, rewards, or minipool count
- **Pagination**: Handle large datasets with "Load More" functionality
- **Node Detail View**: Drill down into individual node performance metrics
- **Notes System**: Add and save notes for specific node addresses (via REST API)
- **POAP Integration**: Display POAP badges for node operators

### Security
- Content Security Policy (CSP) headers
- XSS protection
- Input sanitization
- No inline scripts
- Secure CORS configuration

## Architecture

### Smart Contract Integration
- **Node Manager**: `0x2b52479F6ea009907e46fc43e91064D1b92Fdc86`
- **Minipool Manager**: `0xF82991Bd8976c243eB3b7CDDc52AB0Fc8dc1246C`

### Data Flow

**Note**: This system requires a ClickHouse database populated with beacon chain attestation data. Setting up and populating the ClickHouse database is not covered in this documentation.

```
Ethereum Blockchain
        ↓
rocketpool_scanner.py (scan nodes & minipools)
        ↓
rocketpool_scan_results.json
        ↓
rocketpool_multi_analysis.py (generate reports)
        ↓
ClickHouse Database (query attestation data)
        ↓
12 Performance Reports (JSON)
        ↓
Web Dashboard (index.html)
```

### Components

1. **Scanner** (`rocketpool_scanner.py`)
   - Connects to Ethereum node via Web3
   - Scans all Rocket Pool nodes and minipools
   - Collects node addresses, minipool addresses, and validator public keys
   - Outputs: `rocketpool_scan_results.json`

2. **Analysis Library** (`rocketpool_analysis_only.py`)
   - Core analysis functions
   - ClickHouse database connection
   - Performance calculations
   - Can be run standalone for single reports

3. **Multi-Report Generator** (`rocketpool_multi_analysis.py`)
   - Orchestrates generation of all 12 reports
   - Efficient data reuse across reports
   - Creates summary metadata

4. **POAP Lookup** (`poap_lookup.py`)
   - Queries Gnosis Chain for POAP ownership
   - Generates badge data for dashboard

5. **Notes API** (`notes_api.py`)
   - FastAPI REST backend
   - Stores user notes for node addresses
   - Automatic backup on every save
   - CORS enabled for web dashboard

6. **Web Dashboard** (`reports/index.html` + assets)
   - Single-page application
   - Loads and displays performance reports
   - Interactive filtering, searching, sorting
   - Theme management
   - Notes integration

## Installation

### Prerequisites
- Python 3.8+
- Ethereum node with RPC access (for scanner)
- ClickHouse database with beacon chain data (for analysis)
- Web server (for dashboard hosting)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/Steelyninja/rocketpool-dashboard.git
cd rocketpool-dashboard
```

2. **Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure connections**

Edit the configuration in each script:

**rocketpool_scanner.py:**
```python
ETH_RPC_URL = 'http://your-ethereum-node:8545'
```

**rocketpool_analysis_only.py:**
```python
CLICKHOUSE_HOST = 'your-clickhouse-host'
CLICKHOUSE_PORT = 8123
```

**notes_api.py:**
```python
NOTES_FILE = '/path/to/reports/notes.json'
BACKUP_DIR = '/path/to/reports/backup'
```

## Usage

### 1. Scan Blockchain Data

```bash
python rocketpool_scanner.py
```

This creates `rocketpool_scan_results.json` with all node and minipool data.

### 2. Generate Performance Reports

**All 12 reports (recommended):**
```bash
python rocketpool_multi_analysis.py
```

**Single report:**
```bash
python rocketpool_analysis_only.py --period 7day --threshold 80
```

**Output structure:**
```
reports/
├── 1day/
│   ├── performance_80.json
│   ├── performance_90.json
│   ├── performance_95.json
│   └── performance_all.json
├── 3day/
│   └── ...
├── 7day/
│   └── ...
└── summary.json
```

### 3. (Optional) Generate POAP Data

```bash
python poap_lookup.py
```

Creates `reports/poap_results.json` for badge display.

### 4. Start Notes API

```bash
python notes_api.py
```

Runs on `http://localhost:8001` by default.

### 5. Deploy Dashboard

Copy the `reports/` folder to your web server:
```bash
rsync -avz reports/ user@server:/var/www/rocketpool/
```

Access at `http://your-server/rocketpool/`

## Dashboard Features

### Period Selection
- **1 day**: Last ~225 epochs (~24 hours)
- **3 day**: Last ~675 epochs (~72 hours)
- **7 day**: Last ~1575 epochs (~168 hours)

### Threshold Filters
- **<80%**: Broadly underperforming nodes
- **<90%**: Moderately underperforming nodes
- **<95%**: Missing elite performance tier
- **All**: Every node regardless of performance

### Display Controls
- **Zero Performance Toggle**: Show/hide nodes with 0% performance
- **Exclude Backup Validators**: Filter out backup validators
- **Fusaka Deaths Toggle**: Show only validators affected by Fusaka hard fork
- **Theme**: System, Light, or Dark mode

### Node Information
Each node displays:
- Node address with ENS name (if available)
- Withdrawal address
- Total/Active/Exited minipool count
- Performance score percentage
- Total earned vs possible rewards
- Individual minipool performance
- POAP badges (if applicable)
- Custom notes (editable)

## Notes API

### Endpoints

**Get all notes:**
```bash
GET /api/rp-notes
```

**Save notes:**
```bash
POST /api/rp-notes
Content-Type: application/json

{
  "notes": {
    "0x1234...": "Node operator contact: alice@example.com",
    "0x5678...": "Investigating performance issues"
  }
}
```

**List backups:**
```bash
GET /api/rp-notes/backups
```

**Health check:**
```bash
GET /health
```

### Backup System
- Automatic backup on every save
- Timestamped backup files: `notes_backup_YYYYMMDD_HHMMSS.json`
- Stored in `reports/backup/`

## Report Structure

Each performance report JSON contains:

```json
{
  "analysis_date": "2026-01-17T12:36:29.137765",
  "period": "7day",
  "threshold": 80,
  "epochs_analyzed": 1575,
  "start_epoch": 410000,
  "end_epoch": 411575,
  "total_nodes": 74,
  "node_performance_scores": [
    {
      "node_address": "0x...",
      "ens_name": "example.eth",
      "withdrawal_address": "0x...",
      "total_minipools": 5,
      "active_minipools": 4,
      "exited_minipools": 1,
      "performance_score": 78.5,
      "total_earned_rewards": 1234567890,
      "total_possible_rewards": 1500000000,
      "minipools": [...]
    }
  ],
  "validator_statuses": {...}
}
```

## Development

### Project Structure

```
.
├── rocketpool_scanner.py           # Blockchain scanner
├── rocketpool_analysis_only.py     # Analysis library
├── rocketpool_multi_analysis.py    # Multi-report generator
├── poap_lookup.py                  # POAP badge lookup
├── notes_api.py                    # Notes REST API
├── requirements.txt                # Python dependencies
└── reports/
    ├── index.html                  # Dashboard HTML
    └── assets/
        ├── css/
        │   └── styles.css          # Dashboard styles
        └── js/
            └── app.js              # Dashboard application
```

### Dependencies

- **web3** - Ethereum blockchain interaction
- **multicall** - Efficient batch contract calls
- **tqdm** - Progress bars
- **clickhouse-connect** - ClickHouse database queries
- **fastapi** - REST API framework
- **uvicorn** - ASGI server

## Configuration

### ClickHouse Database

The analysis scripts require a ClickHouse database with beacon chain data. The database should contain:

- `validators_summary` table with epoch-level validator data
- Validator public keys, attestation performance, and status information

### Ethereum Node

The scanner requires access to an Ethereum node with:
- Web3 RPC endpoint
- Archive node recommended for historical data
- Mainnet connection

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Rocket Pool community
- Ethereum Foundation
- ClickHouse team

## Support

For issues and questions:
- Open an issue on GitHub
- Join the Rocket Pool Discord #support channel

---

**Disclaimer**: This is a community tool for monitoring Rocket Pool validator performance. Always verify critical information through official Rocket Pool interfaces.
