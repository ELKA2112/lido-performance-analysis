"""
Lido Performance Analysis Script - Standalone Version

This script performs a comprehensive analysis of a reference node operator's performance
vs other Lido node operators.

Process:
1. Fetch all Lido operators and their validator keys from TheGraph
2. Fetch daily rewards data for each operator (with checkpointing for resume capability)
3. Fetch accurate stake balances using the /stakes endpoint (tracks activations and exits)
4. Analyze reference operator's performance vs other operators (configurable date range)
5. Compare analysis with and without outliers
6. Generate visualizations and save results

Key Features:
- Configurable reference operator: Set any operator as the benchmark in CONFIG
- Accurate balance tracking: Uses /eth/stakes endpoint to calculate precise active balances
  by tracking validator activations (activated_at) and exits (exited_at). This is more
  accurate than /rewards endpoint which includes exited but not yet withdrawn validators.
- Checkpointing: Resume capability for long-running jobs
- Outlier detection: Statistical outlier removal using standard deviations
- Comprehensive metrics: APR, rankings, percentiles, z-scores, extra rewards
- Visualizations: Dashboard plots and operator heatmaps

Requirements:
- Python 3.8+
- pandas, numpy, requests, aiohttp, matplotlib, seaborn

Configuration:
- Set environment variables or edit the CONFIG section below:
  - KILN_API_KEY: Your Kiln API key for fetching rewards data
  - THEGRAPH_API_KEY: Your TheGraph API key for subgraph queries
  - KILN_API_URL: Kiln API base URL (default: https://api.kiln.fi/v1)
  - REFERENCE_OPERATOR: The operator name to analyze (default: 'Kiln')

Author: Finance Team
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
import aiohttp
import time
import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime, timedelta
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Configuration - Edit these or set as environment variables
CONFIG = {
    # Kiln API for rewards data
    'KILN_API_KEY': os.environ.get('KILN_API_KEY', ''),
    'KILN_API_URL': os.environ.get('KILN_API_URL', 'https://api.kiln.fi/v1'),

    # TheGraph API for Lido subgraph
    'THEGRAPH_API_KEY': os.environ.get('THEGRAPH_API_KEY', ''),
    'LIDO_SUBGRAPH_URL': 'https://gateway.thegraph.com/api/subgraphs/id/Sxx812XgeKyzQPaBpR5YZWmGV5fZuBaPdh7DFhzSwiQ',

    # Reference operator configuration
    'REFERENCE_OPERATOR': 'Kiln',  # The operator name to analyze (must match name in Lido subgraph)

    # Analysis configuration
    'MIN_DAYS_PER_MONTH': 20,  # Minimum days of data required to include a month in analysis
    'OUTLIER_STD_THRESHOLD': 2,  # Standard deviations above mean to consider as outlier
    'START_DATE': '2023-01-01',  # Start date for analysis
    'END_DATE': '2025-08-31',  # End date for analysis

    # Processing configuration
    'MAX_CONCURRENT_REQUESTS': 10,  # Max concurrent API calls
    'BATCH_SIZE': 81,  # Number of validator keys per API call
    'MAX_RETRIES': 10,  # Max retry attempts for failed API calls
}

# Output directory configuration
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, 'lido_checkpoint.json')
RESULTS_FILE = os.path.join(OUTPUT_DIR, 'lido_results.json')
LOG_FILE = os.path.join(OUTPUT_DIR, 'lido_processing.log')


# ============================================================================
# UTILITY FUNCTIONS - LOGGING AND CHECKPOINTING
# ============================================================================

def log_message(message):
    """Log message to both console and file"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_line = f"[{timestamp}] {message}"
    print(log_line)
    with open(LOG_FILE, 'a') as f:
        f.write(log_line + '\n')


def load_checkpoint():
    """Load checkpoint file if it exists"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {'completed_operators': [], 'last_updated': None, 'total_operators': 0}


def save_checkpoint(checkpoint_data):
    """Save checkpoint file"""
    checkpoint_data['last_updated'] = datetime.now().isoformat()
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint_data, f, indent=2)
    log_message(f"Checkpoint saved: {len(checkpoint_data['completed_operators'])}/{checkpoint_data['total_operators']} operators completed")


def load_results():
    """Load existing results if they exist"""
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_results(results):
    """Save results incrementally"""
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)


def validate_config():
    """Validate that required configuration is set"""
    errors = []

    if not CONFIG['KILN_API_KEY']:
        errors.append("KILN_API_KEY is not set. Please set it in the CONFIG dictionary or as an environment variable.")

    if not CONFIG['THEGRAPH_API_KEY']:
        errors.append("THEGRAPH_API_KEY is not set. Please set it in the CONFIG dictionary or as an environment variable.")

    if errors:
        log_message("Configuration errors:")
        for error in errors:
            log_message(f"  - {error}")
        log_message("\nPlease set the required API keys and try again.")
        sys.exit(1)


# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def fetch_lido_operators():
    """Fetch all Lido node operators from TheGraph subgraph"""
    log_message("Fetching operators from Lido subgraph...")

    operators_query = """{
      nodeOperators{
        id
        name
      }
    }"""

    header = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {CONFIG['THEGRAPH_API_KEY']}"
    }

    try:
        response = requests.post(
            CONFIG['LIDO_SUBGRAPH_URL'],
            json={'query': operators_query},
            headers=header,
            timeout=30
        )
        response.raise_for_status()
        operators_res = response.json()

        if 'errors' in operators_res:
            raise Exception(f"GraphQL errors: {operators_res['errors']}")

        operators_list = operators_res['data']['nodeOperators']
        log_message(f"Retrieved {len(operators_list)} operators")

        operator_keys = {}
        for operator in operators_list:
            operator_id = operator['id']
            operator_name = operator['name']
            operator_keys[operator_id] = {
                'name': operator_name,
                'keys': []
            }

        return operator_keys

    except Exception as e:
        log_message(f"ERROR: Failed to fetch operators: {str(e)}")
        raise


def fetch_operator_keys(operator_keys, checkpoint):
    """Fetch validator keys for each operator"""
    log_message("Fetching validator keys for each operator...")

    header = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {CONFIG['THEGRAPH_API_KEY']}"
    }

    for operator_id in operator_keys:
        # Skip if already processed
        if operator_id in checkpoint['completed_operators']:
            log_message(f"Skipping operator {operator_id} - {operator_keys[operator_id]['name']} (already processed)")
            continue

        log_message(f"Processing operator {operator_id} - {operator_keys[operator_id]['name']}")
        offset = 0

        try:
            while True:
                keys_query = f"""
                {{
                nodeOperatorSigningKeys(where: {{ operatorId: {operator_id} }}, first:1000, skip:{offset}) {{
                    pubkey
                }}
                }}"""

                response = requests.post(
                    CONFIG['LIDO_SUBGRAPH_URL'],
                    json={'query': keys_query},
                    headers=header,
                    timeout=30
                )
                response.raise_for_status()
                keys_res = response.json()

                if 'errors' in keys_res:
                    raise Exception(f"GraphQL errors: {keys_res['errors']}")

                keys = keys_res['data']['nodeOperatorSigningKeys']

                if len(keys) == 0:
                    break

                log_message(f"  Retrieved {len(keys)} keys at offset {offset}")
                for key in keys:
                    operator_keys[operator_id]['keys'].append(key['pubkey'])

                offset += 1000

            log_message(f"  Total keys for operator {operator_id}: {len(operator_keys[operator_id]['keys'])}")

        except Exception as e:
            log_message(f"ERROR: Failed to fetch keys for operator {operator_id}: {str(e)}")
            raise

    log_message("All operators and their keys retrieved")
    return operator_keys


async def fetch_api_async(endpoint, key, session, semaphore):
    """Async version of API fetch with retry logic and concurrency control"""
    async with semaphore:
        for i in range(CONFIG['MAX_RETRIES']):
            t1 = time.time()
            try:
                async with session.get(
                    endpoint,
                    headers={"Authorization": f"Bearer {key}"}
                ) as response:
                    t2 = time.time()
                    print(f"Fetched {endpoint[:100]} in {round(t2-t1, 2)} seconds")

                    if response.status == 200:
                        return await response.json()
                    else:
                        print(f"Error with {endpoint} - {response.status} - try: {i+1}")
                        await asyncio.sleep(10 * (i + 1))
            except Exception as e:
                t2 = time.time()
                print(f"Exception fetching {endpoint[:100]} in {round(t2-t1, 2)} seconds: {e}")
                await asyncio.sleep(10 * (i + 1))

        raise Exception(f"Failed to fetch {endpoint} after {CONFIG['MAX_RETRIES']} retries")


async def get_eth_stakes_async(keys_list, api_key, session, semaphore):
    """
    Fetch stake activation/exit data for validators to calculate accurate active balances.
    This is more precise than the balance from /rewards endpoint which includes exited validators.
    """
    batch_size = CONFIG['BATCH_SIZE']
    n = len(keys_list) // batch_size
    all_stakes_data = []

    async def fetch_stakes_batch(batch_index):
        start_idx = batch_index * batch_size
        end_idx = (batch_index + 1) * batch_size
        keys_batch = keys_list[start_idx:end_idx]

        if not keys_batch:
            return []

        endpoint = f"{CONFIG['KILN_API_URL']}/eth/stakes?validators={','.join(keys_batch)}"
        res = await fetch_api_async(endpoint, api_key, session, semaphore)
        print(f"Stakes batch {batch_index}/{n} processed")

        return res.get('data', [])

    # Fetch all stake batches concurrently
    batch_results = await asyncio.gather(
        *[fetch_stakes_batch(i) for i in range(n + 1)],
        return_exceptions=True
    )

    # Merge all batch results
    for batch_data in batch_results:
        if isinstance(batch_data, Exception):
            print(f"Stakes batch failed with error: {batch_data}")
            continue
        all_stakes_data.extend(batch_data)

    # Calculate active balance per day based on activations and exits
    balance_changes = {}

    for stake in all_stakes_data:
        # Each validator is 32 ETH
        # Add balance when activated
        if 'activated_at' in stake and stake['activated_at']:
            activation_date = stake['activated_at'][:10]
            if activation_date not in balance_changes:
                balance_changes[activation_date] = 0
            balance_changes[activation_date] += 32

        # Subtract balance when exited
        if 'exited_at' in stake and stake['exited_at']:
            exit_date = stake['exited_at'][:10]
            if exit_date not in balance_changes:
                balance_changes[exit_date] = 0
            balance_changes[exit_date] -= 32

    # Create cumulative balance for each day
    if not balance_changes:
        return {}

    dates = sorted(balance_changes.keys())
    cumulative_balances = {}

    # Calculate cumulative balance for first date
    cumulative_balances[dates[0]] = balance_changes[dates[0]]

    # Fill in all dates and calculate cumulative
    for i in range(1, len(dates)):
        cumulative_balances[dates[i]] = cumulative_balances[dates[i-1]] + balance_changes[dates[i]]

    # Fill in missing dates between first and last date
    if dates:
        start_date = datetime.strptime(dates[0], '%Y-%m-%d')
        end_date = datetime.strptime(dates[-1], '%Y-%m-%d')

        current_date = start_date
        current_balance = 0
        filled_balances = {}

        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str in cumulative_balances:
                current_balance = cumulative_balances[date_str]
            filled_balances[date_str] = current_balance
            current_date += timedelta(days=1)

        return filled_balances

    return cumulative_balances


async def get_eth_rewards_daily_async(keys_list, api_key, session, semaphore):
    """Async fetch of daily ETH rewards for a list of validator keys"""
    batch_size = CONFIG['BATCH_SIZE']
    n = len(keys_list) // batch_size
    output = {}

    async def fetch_batch(batch_index):
        start_idx = batch_index * batch_size
        end_idx = (batch_index + 1) * batch_size
        keys_batch = keys_list[start_idx:end_idx]

        if not keys_batch:
            return {}

        endpoint = f"{CONFIG['KILN_API_URL']}/eth/rewards?validators={','.join(keys_batch)}"
        res = await fetch_api_async(endpoint, api_key, session, semaphore)
        print(f"Rewards batch {batch_index}/{n} processed")

        batch_output = {}
        for entry in res['data']:
            date = entry['date']
            rewards = int(entry['rewards']) / 1E18  # Convert from wei to ETH

            if date not in batch_output:
                batch_output[date] = {
                    'rewards': 0
                }
            batch_output[date]['rewards'] += rewards

        return batch_output

    # Fetch all batches concurrently (but limited by semaphore)
    batch_results = await asyncio.gather(
        *[fetch_batch(i) for i in range(n + 1)],
        return_exceptions=True
    )

    # Merge all batch results
    for batch_output in batch_results:
        if isinstance(batch_output, Exception):
            print(f"Batch failed with error: {batch_output}")
            continue

        for date, values in batch_output.items():
            if date not in output:
                output[date] = {'rewards': 0}
            output[date]['rewards'] += values['rewards']

    return output


async def process_operator_async(operator_id, operator_data, api_key, session, semaphore):
    """Process a single operator's rewards and accurate balances asynchronously"""
    try:
        log_message(f"Fetching data for operator {operator_id} - {operator_data['name']}")

        # Fetch rewards and accurate balances in parallel
        rewards_task = get_eth_rewards_daily_async(
            operator_data['keys'],
            api_key,
            session,
            semaphore
        )

        balances_task = get_eth_stakes_async(
            operator_data['keys'],
            api_key,
            session,
            semaphore
        )

        rewards_data, balances_data = await asyncio.gather(rewards_task, balances_task)

        # Merge rewards and balances
        combined_data = {}

        # Add rewards
        for date, data in rewards_data.items():
            combined_data[date] = {
                'rewards': data['rewards'],
                'balance': 0
            }

        # Add accurate balances from stakes endpoint
        for date, balance in balances_data.items():
            if date in combined_data:
                combined_data[date]['balance'] = balance
            else:
                combined_data[date] = {
                    'rewards': 0,
                    'balance': balance
                }

        log_message(f"Successfully fetched data for operator {operator_id}")
        return combined_data

    except Exception as e:
        log_message(f"ERROR: Failed to process operator {operator_id} - {operator_data['name']}: {str(e)}")
        import traceback
        log_message(f"Traceback:\n{traceback.format_exc()}")
        return None


def process_all_operators(operator_keys, api_key, checkpoint, existing_results):
    """Process operators sequentially with async batch fetching for each operator"""
    daily_rewards_operator = existing_results.copy()

    operators_to_process = [op_id for op_id in operator_keys.keys()
                           if op_id not in checkpoint['completed_operators']]

    log_message(f"Processing {len(operators_to_process)} operators sequentially (out of {len(operator_keys)} total)")
    log_message(f"Each operator will fetch batches with max {CONFIG['MAX_CONCURRENT_REQUESTS']} concurrent API calls")
    log_message("Using accurate balance tracking from /eth/stakes endpoint (tracks activations and exits)")

    for idx, operator_id in enumerate(operators_to_process, 1):
        try:
            log_message(f"===== Operator {idx}/{len(operators_to_process)}: {operator_id} - {operator_keys[operator_id]['name']} =====")
            log_message(f"  Validator keys: {len(operator_keys[operator_id]['keys'])}")

            async def run_operator():
                semaphore = asyncio.Semaphore(CONFIG['MAX_CONCURRENT_REQUESTS'])
                async with aiohttp.ClientSession() as session:
                    return await process_operator_async(
                        operator_id,
                        operator_keys[operator_id],
                        api_key,
                        session,
                        semaphore
                    )

            rewards = asyncio.run(run_operator())

            if rewards is not None:
                daily_rewards_operator[operator_id] = {
                    'name': operator_keys[operator_id]['name'],
                    'keys': operator_keys[operator_id]['keys'],
                    'rewards': rewards
                }

                checkpoint['completed_operators'].append(operator_id)
                save_checkpoint(checkpoint)
                save_results(daily_rewards_operator)

                log_message(f"✓ Progress: {len(checkpoint['completed_operators'])}/{len(operator_keys)} operators completed")
            else:
                log_message(f"✗ WARNING: Operator {operator_id} returned no data, will retry on next run")

        except Exception as e:
            log_message(f"✗ CRITICAL ERROR processing operator {operator_id}: {str(e)}")
            import traceback
            log_message(f"Traceback:\n{traceback.format_exc()}")
            continue

    return daily_rewards_operator


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyse_operator_performance():
    """
    Analyze reference operator's performance vs other Lido node operators.

    Returns:
        dict: Analysis results including APR comparisons, rankings, and extra rewards with DataFrame
    """
    ref_operator = CONFIG['REFERENCE_OPERATOR']
    log_message(f"Starting analysis for {ref_operator} (with all data)")

    with open(RESULTS_FILE, 'r') as f:
        lido_results_raw = json.load(f)

    # Filter to only include dates within the configured date range
    lido_results = {}
    for operator_id, operator_data in lido_results_raw.items():
        lido_results[operator_id] = {
            'name': operator_data['name'],
            'keys': operator_data.get('keys', []),
            'rewards': {
                date: rewards_data
                for date, rewards_data in operator_data['rewards'].items()
                if CONFIG['START_DATE'] <= date <= CONFIG['END_DATE']
            }
        }

    # Calculate monthly metrics for each operator
    monthly_metrics = {}

    for operator_id in lido_results:
        operator_name = lido_results[operator_id]['name']
        monthly_metrics[operator_name] = {}

        for date in lido_results[operator_id]['rewards']:
            rewards = lido_results[operator_id]['rewards'][date]['rewards']
            balance = lido_results[operator_id]['rewards'][date]['balance']
            month = date[:7]

            if month not in monthly_metrics[operator_name]:
                monthly_metrics[operator_name][month] = {
                    'total_rewards': 0,
                    'total_balance': 0,
                    'days': 0
                }

            if balance > 0 and rewards >= 0:
                monthly_metrics[operator_name][month]['total_rewards'] += rewards
                monthly_metrics[operator_name][month]['total_balance'] += balance
                monthly_metrics[operator_name][month]['days'] += 1

    # Calculate APRs
    monthly_aprs = {}
    for operator_name, months in monthly_metrics.items():
        monthly_aprs[operator_name] = {}
        for month, metrics in months.items():
            if metrics['days'] >= CONFIG['MIN_DAYS_PER_MONTH']:
                avg_balance = metrics['total_balance'] / metrics['days']
                monthly_apr = (metrics['total_rewards'] / avg_balance) * (365 / metrics['days'])
                monthly_aprs[operator_name][month] = {
                    'apr': monthly_apr,
                    'avg_balance': avg_balance,
                    'days': metrics['days'],
                    'total_rewards': metrics['total_rewards']
                }

    # Calculate network-wide metrics (excluding reference operator) with stake-weighting
    network_metrics = {}
    all_operator_aprs = {}

    for month in set(m for ops in monthly_aprs.values() for m in ops.keys()):
        operators_data = []

        for operator_name, months_data in monthly_aprs.items():
            if operator_name == ref_operator or month not in months_data:
                continue
            operators_data.append(months_data[month])

        if len(operators_data) >= 3:
            total_stake = sum(op['avg_balance'] for op in operators_data)
            weighted_avg_apr = sum(
                op['apr'] * op['avg_balance'] for op in operators_data
            ) / total_stake if total_stake > 0 else 0

            aprs = [op['apr'] for op in operators_data]

            # Calculate top 10 operators metrics
            top_10_count = min(10, len(aprs))
            top_10_aprs = sorted(aprs, reverse=True)[:top_10_count]

            network_metrics[month] = {
                'weighted_avg_apr': weighted_avg_apr,
                'median_apr': np.median(aprs),
                'top_10_mean_apr': np.mean(top_10_aprs),
                'top_10_median_apr': np.median(top_10_aprs),
                'std_apr': np.std(aprs),
                'min_apr': min(aprs),
                'max_apr': max(aprs),
                'operator_count': len(aprs),
                'total_network_stake': total_stake
            }
            all_operator_aprs[month] = aprs

    # Reference operator performance analysis
    ref_analysis = {}
    total_extra_rewards_vs_mean = 0
    total_extra_rewards_vs_median = 0
    total_extra_rewards_vs_top10_mean = 0
    total_extra_rewards_vs_top10_median = 0

    ref_operator_id = None
    for operator_id, data in lido_results.items():
        if data['name'] == ref_operator:
            ref_operator_id = operator_id
            break

    if ref_operator in monthly_aprs and ref_operator_id:
        for month, ref_data in monthly_aprs[ref_operator].items():
            if month in network_metrics:
                ref_apr = ref_data['apr']
                network_weighted_apr = network_metrics[month]['weighted_avg_apr']
                network_median_apr = network_metrics[month]['median_apr']
                top_10_mean_apr = network_metrics[month]['top_10_mean_apr']
                top_10_median_apr = network_metrics[month]['top_10_median_apr']

                # Calculate all APRs for ranking
                all_aprs = all_operator_aprs[month] + [ref_apr]
                all_aprs_sorted = sorted(all_aprs, reverse=True)
                ref_rank = all_aprs_sorted.index(ref_apr) + 1

                # Calculate percentile rank
                percentile = (sum(1 for apr in all_aprs if apr <= ref_apr) / len(all_aprs)) * 100

                # Calculate extra rewards vs different benchmarks
                month_extra_rewards_vs_mean = 0
                month_extra_rewards_vs_median = 0
                month_extra_rewards_vs_top10_mean = 0
                month_extra_rewards_vs_top10_median = 0

                for date in lido_results[ref_operator_id]['rewards']:
                    if date.startswith(month):
                        daily_balance = lido_results[ref_operator_id]['rewards'][date]['balance']
                        month_extra_rewards_vs_mean += daily_balance * ((ref_apr - network_weighted_apr) / 365)
                        month_extra_rewards_vs_median += daily_balance * ((ref_apr - network_median_apr) / 365)
                        month_extra_rewards_vs_top10_mean += daily_balance * ((ref_apr - top_10_mean_apr) / 365)
                        month_extra_rewards_vs_top10_median += daily_balance * ((ref_apr - top_10_median_apr) / 365)

                total_extra_rewards_vs_mean += month_extra_rewards_vs_mean
                total_extra_rewards_vs_median += month_extra_rewards_vs_median
                total_extra_rewards_vs_top10_mean += month_extra_rewards_vs_top10_mean
                total_extra_rewards_vs_top10_median += month_extra_rewards_vs_top10_median

                # Z-score
                std = network_metrics[month]['std_apr']
                z_score = ((ref_apr - network_weighted_apr) / std) if std > 0 else 0

                ref_analysis[month] = {
                    'month': month,
                    'ref_apr': ref_apr,
                    'ref_rank': ref_rank,
                    'total_operators': len(all_aprs),
                    'network_weighted_mean_apr': network_weighted_apr,
                    'network_median_apr': network_median_apr,
                    'top_10_mean_apr': top_10_mean_apr,
                    'top_10_median_apr': top_10_median_apr,
                    'apr_diff_vs_mean': ref_apr - network_weighted_apr,
                    'apr_diff_vs_median': ref_apr - network_median_apr,
                    'apr_diff_vs_top10_mean': ref_apr - top_10_mean_apr,
                    'apr_diff_vs_top10_median': ref_apr - top_10_median_apr,
                    'apr_diff_vs_mean_bps': (ref_apr - network_weighted_apr) * 10000,
                    'apr_diff_vs_median_bps': (ref_apr - network_median_apr) * 10000,
                    'percentile_rank': percentile,
                    'z_score': z_score,
                    'extra_rewards_vs_mean': month_extra_rewards_vs_mean,
                    'extra_rewards_vs_median': month_extra_rewards_vs_median,
                    'extra_rewards_vs_top10_mean': month_extra_rewards_vs_top10_mean,
                    'extra_rewards_vs_top10_median': month_extra_rewards_vs_top10_median,
                    'network_std': std,
                    'days_in_month': ref_data['days']
                }

    # Sort by month
    ref_analysis_sorted = dict(sorted(ref_analysis.items()))

    # Get monthly operator rankings
    monthly_operator_rankings = {}
    for month in sorted(ref_analysis.keys()):
        if month in all_operator_aprs:
            operators_with_aprs = []
            for op_name, months_data in monthly_aprs.items():
                if month in months_data:
                    operators_with_aprs.append({
                        'operator': op_name,
                        'apr': months_data[month]['apr'],
                        'avg_balance': months_data[month]['avg_balance']
                    })

            operators_with_aprs_sorted = sorted(operators_with_aprs, key=lambda x: x['apr'], reverse=True)

            monthly_operator_rankings[month] = []
            for rank, op_data in enumerate(operators_with_aprs_sorted, 1):
                monthly_operator_rankings[month].append({
                    'rank': rank,
                    'operator': op_data['operator'],
                    'apr': op_data['apr'],
                    'avg_balance': op_data['avg_balance']
                })

    # Get top 3 operators' raw data for each month
    top_3_operators_data = {}
    for month in sorted(ref_analysis.keys()):
        if month in all_operator_aprs:
            top_3_names = [op['operator'] for op in monthly_operator_rankings[month][:3]]

            top_3_operators_data[month] = {}
            for op_name in top_3_names:
                op_id = None
                for operator_id, data in lido_results.items():
                    if data['name'] == op_name:
                        op_id = operator_id
                        break

                if op_id:
                    top_3_operators_data[month][op_name] = {}
                    for date in lido_results[op_id]['rewards']:
                        if date.startswith(month):
                            top_3_operators_data[month][op_name][date] = {
                                'rewards': lido_results[op_id]['rewards'][date]['rewards'],
                                'balance': lido_results[op_id]['rewards'][date]['balance']
                            }

    # Create DataFrame
    df_data = []
    for month in sorted(ref_analysis.keys()):
        data = ref_analysis[month]
        df_data.append({
            'Month': month,
            f'{ref_operator} APR (%)': data['ref_apr'] * 100,
            f'{ref_operator} Rank': data['ref_rank'],
            'Total Operators': data['total_operators'],
            'Percentile': data['percentile_rank'],
            'Network Mean APR (%)': data['network_weighted_mean_apr'] * 100,
            'Network Median APR (%)': data['network_median_apr'] * 100,
            'Top 10 Operators Mean APR (%)': data['top_10_mean_apr'] * 100,
            'Top 10 Operators Median APR (%)': data['top_10_median_apr'] * 100,
            'Diff vs Mean (bps)': data['apr_diff_vs_mean_bps'],
            'Diff vs Median (bps)': data['apr_diff_vs_median_bps'],
            'Extra Rewards vs Mean (ETH)': data['extra_rewards_vs_mean'],
            'Extra Rewards vs Median (ETH)': data['extra_rewards_vs_median'],
            'Extra Rewards vs Top 10 Mean (ETH)': data['extra_rewards_vs_top10_mean'],
            'Extra Rewards vs Top 10 Median (ETH)': data['extra_rewards_vs_top10_median'],
            'Z-Score': data['z_score'],
            'Days': data['days_in_month']
        })

    df_monthly = pd.DataFrame(df_data)

    log_message(f"Analysis complete for {ref_operator}: {len(ref_analysis)} months analyzed")

    return {
        'ref_analysis': ref_analysis_sorted,
        'network_metrics': network_metrics,
        'monthly_dataframe': df_monthly,
        'monthly_operator_rankings': monthly_operator_rankings,
        'top_3_operators_raw_data': top_3_operators_data,
        'summary': {
            'total_months': len(ref_analysis),
            'avg_rank': np.mean([m['ref_rank'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'avg_percentile': np.mean([m['percentile_rank'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'months_above_mean': sum(1 for m in ref_analysis.values() if m['apr_diff_vs_mean'] > 0),
            'months_above_median': sum(1 for m in ref_analysis.values() if m['apr_diff_vs_median'] > 0),
            'avg_apr_diff_vs_mean_bps': np.mean([m['apr_diff_vs_mean_bps'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'avg_apr_diff_vs_median_bps': np.mean([m['apr_diff_vs_median_bps'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'total_extra_rewards_vs_mean': total_extra_rewards_vs_mean,
            'total_extra_rewards_vs_median': total_extra_rewards_vs_median,
            'total_extra_rewards_vs_top10_mean': total_extra_rewards_vs_top10_mean,
            'total_extra_rewards_vs_top10_median': total_extra_rewards_vs_top10_median,
            'start_date': CONFIG['START_DATE'],
            'end_date': CONFIG['END_DATE']
        }
    }


def analyse_operator_performance_no_outliers():
    """
    Analyze reference operator's performance vs other Lido node operators with outlier removal.
    Removes days where rewards > N standard deviations above mean for each operator per month.

    Returns:
        dict: Analysis results including APR comparisons, rankings, and extra rewards with DataFrame
    """
    ref_operator = CONFIG['REFERENCE_OPERATOR']
    log_message(f"Starting analysis for {ref_operator} (outliers removed - > {CONFIG['OUTLIER_STD_THRESHOLD']} std dev)")

    with open(RESULTS_FILE, 'r') as f:
        lido_results_raw = json.load(f)

    # Filter to only include dates within the configured date range
    lido_results_filtered = {}
    for operator_id, operator_data in lido_results_raw.items():
        lido_results_filtered[operator_id] = {
            'name': operator_data['name'],
            'keys': operator_data.get('keys', []),
            'rewards': {
                date: rewards_data
                for date, rewards_data in operator_data['rewards'].items()
                if CONFIG['START_DATE'] <= date <= CONFIG['END_DATE']
            }
        }

    # Remove outliers for each operator per month
    lido_results = {}
    for operator_id, operator_data in lido_results_filtered.items():
        lido_results[operator_id] = {
            'name': operator_data['name'],
            'keys': operator_data.get('keys', []),
            'rewards': {}
        }

        # Group by month
        monthly_rewards = {}
        for date, rewards_data in operator_data['rewards'].items():
            month = date[:7]
            if month not in monthly_rewards:
                monthly_rewards[month] = []
            monthly_rewards[month].append((date, rewards_data['rewards']))

        # Calculate mean/std and filter outliers for each month
        for month, date_reward_list in monthly_rewards.items():
            rewards_values = [r for d, r in date_reward_list if r > 0]

            if len(rewards_values) > 0:
                mean_reward = np.mean(rewards_values)
                std_reward = np.std(rewards_values)
                threshold = mean_reward + (CONFIG['OUTLIER_STD_THRESHOLD'] * std_reward)

                for date, reward in date_reward_list:
                    if reward <= threshold:
                        lido_results[operator_id]['rewards'][date] = operator_data['rewards'][date]

    # Rest of the analysis is identical to the non-outlier version
    # Calculate monthly metrics for each operator
    monthly_metrics = {}

    for operator_id in lido_results:
        operator_name = lido_results[operator_id]['name']
        monthly_metrics[operator_name] = {}

        for date in lido_results[operator_id]['rewards']:
            rewards = lido_results[operator_id]['rewards'][date]['rewards']
            balance = lido_results[operator_id]['rewards'][date]['balance']
            month = date[:7]

            if month not in monthly_metrics[operator_name]:
                monthly_metrics[operator_name][month] = {
                    'total_rewards': 0,
                    'total_balance': 0,
                    'days': 0
                }

            if balance > 0 and rewards >= 0:
                monthly_metrics[operator_name][month]['total_rewards'] += rewards
                monthly_metrics[operator_name][month]['total_balance'] += balance
                monthly_metrics[operator_name][month]['days'] += 1

    # Calculate APRs
    monthly_aprs = {}
    for operator_name, months in monthly_metrics.items():
        monthly_aprs[operator_name] = {}
        for month, metrics in months.items():
            if metrics['days'] >= CONFIG['MIN_DAYS_PER_MONTH']:
                avg_balance = metrics['total_balance'] / metrics['days']
                monthly_apr = (metrics['total_rewards'] / avg_balance) * (365 / metrics['days'])
                monthly_aprs[operator_name][month] = {
                    'apr': monthly_apr,
                    'avg_balance': avg_balance,
                    'days': metrics['days'],
                    'total_rewards': metrics['total_rewards']
                }

    # Calculate network-wide metrics (excluding reference operator) with stake-weighting
    network_metrics = {}
    all_operator_aprs = {}

    for month in set(m for ops in monthly_aprs.values() for m in ops.keys()):
        operators_data = []

        for operator_name, months_data in monthly_aprs.items():
            if operator_name == ref_operator or month not in months_data:
                continue
            operators_data.append(months_data[month])

        if len(operators_data) >= 3:
            total_stake = sum(op['avg_balance'] for op in operators_data)
            weighted_avg_apr = sum(
                op['apr'] * op['avg_balance'] for op in operators_data
            ) / total_stake if total_stake > 0 else 0

            aprs = [op['apr'] for op in operators_data]

            # Calculate top 10 operators metrics
            top_10_count = min(10, len(aprs))
            top_10_aprs = sorted(aprs, reverse=True)[:top_10_count]

            network_metrics[month] = {
                'weighted_avg_apr': weighted_avg_apr,
                'median_apr': np.median(aprs),
                'top_10_mean_apr': np.mean(top_10_aprs),
                'top_10_median_apr': np.median(top_10_aprs),
                'std_apr': np.std(aprs),
                'min_apr': min(aprs),
                'max_apr': max(aprs),
                'operator_count': len(aprs),
                'total_network_stake': total_stake
            }
            all_operator_aprs[month] = aprs

    # Reference operator performance analysis
    ref_analysis = {}
    total_extra_rewards_vs_mean = 0
    total_extra_rewards_vs_median = 0
    total_extra_rewards_vs_top10_mean = 0
    total_extra_rewards_vs_top10_median = 0

    ref_operator_id = None
    for operator_id, data in lido_results.items():
        if data['name'] == ref_operator:
            ref_operator_id = operator_id
            break

    if ref_operator in monthly_aprs and ref_operator_id:
        for month, ref_data in monthly_aprs[ref_operator].items():
            if month in network_metrics:
                ref_apr = ref_data['apr']
                network_weighted_apr = network_metrics[month]['weighted_avg_apr']
                network_median_apr = network_metrics[month]['median_apr']
                top_10_mean_apr = network_metrics[month]['top_10_mean_apr']
                top_10_median_apr = network_metrics[month]['top_10_median_apr']

                # Calculate all APRs for ranking
                all_aprs = all_operator_aprs[month] + [ref_apr]
                all_aprs_sorted = sorted(all_aprs, reverse=True)
                ref_rank = all_aprs_sorted.index(ref_apr) + 1

                # Calculate percentile rank
                percentile = (sum(1 for apr in all_aprs if apr <= ref_apr) / len(all_aprs)) * 100

                # Calculate extra rewards vs different benchmarks
                month_extra_rewards_vs_mean = 0
                month_extra_rewards_vs_median = 0
                month_extra_rewards_vs_top10_mean = 0
                month_extra_rewards_vs_top10_median = 0

                for date in lido_results[ref_operator_id]['rewards']:
                    if date.startswith(month):
                        daily_balance = lido_results[ref_operator_id]['rewards'][date]['balance']
                        month_extra_rewards_vs_mean += daily_balance * ((ref_apr - network_weighted_apr) / 365)
                        month_extra_rewards_vs_median += daily_balance * ((ref_apr - network_median_apr) / 365)
                        month_extra_rewards_vs_top10_mean += daily_balance * ((ref_apr - top_10_mean_apr) / 365)
                        month_extra_rewards_vs_top10_median += daily_balance * ((ref_apr - top_10_median_apr) / 365)

                total_extra_rewards_vs_mean += month_extra_rewards_vs_mean
                total_extra_rewards_vs_median += month_extra_rewards_vs_median
                total_extra_rewards_vs_top10_mean += month_extra_rewards_vs_top10_mean
                total_extra_rewards_vs_top10_median += month_extra_rewards_vs_top10_median

                # Z-score
                std = network_metrics[month]['std_apr']
                z_score = ((ref_apr - network_weighted_apr) / std) if std > 0 else 0

                ref_analysis[month] = {
                    'month': month,
                    'ref_apr': ref_apr,
                    'ref_rank': ref_rank,
                    'total_operators': len(all_aprs),
                    'network_weighted_mean_apr': network_weighted_apr,
                    'network_median_apr': network_median_apr,
                    'top_10_mean_apr': top_10_mean_apr,
                    'top_10_median_apr': top_10_median_apr,
                    'apr_diff_vs_mean': ref_apr - network_weighted_apr,
                    'apr_diff_vs_median': ref_apr - network_median_apr,
                    'apr_diff_vs_top10_mean': ref_apr - top_10_mean_apr,
                    'apr_diff_vs_top10_median': ref_apr - top_10_median_apr,
                    'apr_diff_vs_mean_bps': (ref_apr - network_weighted_apr) * 10000,
                    'apr_diff_vs_median_bps': (ref_apr - network_median_apr) * 10000,
                    'percentile_rank': percentile,
                    'z_score': z_score,
                    'extra_rewards_vs_mean': month_extra_rewards_vs_mean,
                    'extra_rewards_vs_median': month_extra_rewards_vs_median,
                    'extra_rewards_vs_top10_mean': month_extra_rewards_vs_top10_mean,
                    'extra_rewards_vs_top10_median': month_extra_rewards_vs_top10_median,
                    'network_std': std,
                    'days_in_month': ref_data['days']
                }

    # Sort by month
    ref_analysis_sorted = dict(sorted(ref_analysis.items()))

    # Get monthly operator rankings
    monthly_operator_rankings = {}
    for month in sorted(ref_analysis.keys()):
        if month in all_operator_aprs:
            operators_with_aprs = []
            for op_name, months_data in monthly_aprs.items():
                if month in months_data:
                    operators_with_aprs.append({
                        'operator': op_name,
                        'apr': months_data[month]['apr'],
                        'avg_balance': months_data[month]['avg_balance']
                    })

            operators_with_aprs_sorted = sorted(operators_with_aprs, key=lambda x: x['apr'], reverse=True)

            monthly_operator_rankings[month] = []
            for rank, op_data in enumerate(operators_with_aprs_sorted, 1):
                monthly_operator_rankings[month].append({
                    'rank': rank,
                    'operator': op_data['operator'],
                    'apr': op_data['apr'],
                    'avg_balance': op_data['avg_balance']
                })

    # Get top 3 operators' raw data for each month
    top_3_operators_data = {}
    for month in sorted(ref_analysis.keys()):
        if month in all_operator_aprs:
            top_3_names = [op['operator'] for op in monthly_operator_rankings[month][:3]]

            top_3_operators_data[month] = {}
            for op_name in top_3_names:
                op_id = None
                for operator_id, data in lido_results.items():
                    if data['name'] == op_name:
                        op_id = operator_id
                        break

                if op_id:
                    top_3_operators_data[month][op_name] = {}
                    for date in lido_results[op_id]['rewards']:
                        if date.startswith(month):
                            top_3_operators_data[month][op_name][date] = {
                                'rewards': lido_results[op_id]['rewards'][date]['rewards'],
                                'balance': lido_results[op_id]['rewards'][date]['balance']
                            }

    # Create DataFrame
    df_data = []
    for month in sorted(ref_analysis.keys()):
        data = ref_analysis[month]
        df_data.append({
            'Month': month,
            f'{ref_operator} APR (%)': data['ref_apr'] * 100,
            f'{ref_operator} Rank': data['ref_rank'],
            'Total Operators': data['total_operators'],
            'Percentile': data['percentile_rank'],
            'Network Mean APR (%)': data['network_weighted_mean_apr'] * 100,
            'Network Median APR (%)': data['network_median_apr'] * 100,
            'Top 10 Operators Mean APR (%)': data['top_10_mean_apr'] * 100,
            'Top 10 Operators Median APR (%)': data['top_10_median_apr'] * 100,
            'Diff vs Mean (bps)': data['apr_diff_vs_mean_bps'],
            'Diff vs Median (bps)': data['apr_diff_vs_median_bps'],
            'Extra Rewards vs Mean (ETH)': data['extra_rewards_vs_mean'],
            'Extra Rewards vs Median (ETH)': data['extra_rewards_vs_median'],
            'Extra Rewards vs Top 10 Mean (ETH)': data['extra_rewards_vs_top10_mean'],
            'Extra Rewards vs Top 10 Median (ETH)': data['extra_rewards_vs_top10_median'],
            'Z-Score': data['z_score'],
            'Days': data['days_in_month']
        })

    df_monthly = pd.DataFrame(df_data)

    log_message(f"Analysis complete: {len(ref_analysis)} months analyzed (outliers removed)")

    return {
        'ref_analysis': ref_analysis_sorted,
        'network_metrics': network_metrics,
        'monthly_dataframe': df_monthly,
        'monthly_operator_rankings': monthly_operator_rankings,
        'top_3_operators_raw_data': top_3_operators_data,
        'summary': {
            'total_months': len(ref_analysis),
            'avg_rank': np.mean([m['ref_rank'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'avg_percentile': np.mean([m['percentile_rank'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'months_above_mean': sum(1 for m in ref_analysis.values() if m['apr_diff_vs_mean'] > 0),
            'months_above_median': sum(1 for m in ref_analysis.values() if m['apr_diff_vs_median'] > 0),
            'avg_apr_diff_vs_mean_bps': np.mean([m['apr_diff_vs_mean_bps'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'avg_apr_diff_vs_median_bps': np.mean([m['apr_diff_vs_median_bps'] for m in ref_analysis.values()]) if ref_analysis else 0,
            'total_extra_rewards_vs_mean': total_extra_rewards_vs_mean,
            'total_extra_rewards_vs_median': total_extra_rewards_vs_median,
            'total_extra_rewards_vs_top10_mean': total_extra_rewards_vs_top10_mean,
            'total_extra_rewards_vs_top10_median': total_extra_rewards_vs_top10_median,
            'start_date': CONFIG['START_DATE'],
            'end_date': CONFIG['END_DATE'],
            'outliers_removed': True,
            'outlier_threshold': f'> {CONFIG["OUTLIER_STD_THRESHOLD"]} standard deviations per operator per month'
        }
    }


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def plot_operator_performance_analysis(df, title_suffix="", save_path=None):
    """
    Create visualizations for operator performance analysis.

    Args:
        df: DataFrame from analyse_operator_performance() functions
        title_suffix: Optional suffix to add to plot titles
        save_path: Optional path to save the figure (e.g., 'operator_analysis.png')

    Returns:
        matplotlib figure object
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    # Get the reference operator name from the dataframe columns
    ref_operator = CONFIG['REFERENCE_OPERATOR']

    # Convert Month to datetime for better plotting
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Month'] + '-01')

    # Create figure with subplots
    fig = plt.figure(figsize=(24, 20))
    gs = fig.add_gridspec(4, 3, hspace=0.45, wspace=0.3)

    # 1. APR Comparison Over Time (large plot, top)
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(df['Date'], df[f'{ref_operator} APR (%)'], marker='o', linewidth=2, label=ref_operator, color='#2E86AB', markersize=6)
    ax1.plot(df['Date'], df['Network Mean APR (%)'], marker='s', linewidth=1.5, label='Network Mean', color='#A23B72', linestyle='--', markersize=4)
    ax1.plot(df['Date'], df['Network Median APR (%)'], marker='^', linewidth=1.5, label='Network Median', color='#F18F01', linestyle='--', markersize=4)
    ax1.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APR (%)', fontsize=12, fontweight='bold')
    ax1.set_title(f'{ref_operator} APR vs Network Benchmarks Over Time{title_suffix}', fontsize=14, fontweight='bold')
    ax1.legend(loc='best', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 2. Ranking Over Time
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.plot(df['Date'], df[f'{ref_operator} Rank'], marker='o', linewidth=2, color='#2E86AB', markersize=6)
    ax2.invert_yaxis()
    ax2.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax2.set_ylabel('Rank (lower is better)', fontsize=10, fontweight='bold')
    ax2.set_title(f'{ref_operator} Ranking Over Time', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 3. Percentile Over Time
    ax3 = fig.add_subplot(gs[1, 1])
    ax3.plot(df['Date'], df['Percentile'], marker='o', linewidth=2, color='#6A994E', markersize=6)
    ax3.axhline(y=50, color='gray', linestyle='--', alpha=0.5, label='Median (50th)')
    ax3.axhline(y=75, color='orange', linestyle='--', alpha=0.5, label='75th Percentile')
    ax3.axhline(y=90, color='red', linestyle='--', alpha=0.5, label='90th Percentile')
    ax3.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax3.set_ylabel('Percentile', fontsize=10, fontweight='bold')
    ax3.set_title(f'{ref_operator} Percentile Ranking', fontsize=12, fontweight='bold')
    ax3.set_ylim(0, 100)
    ax3.legend(fontsize=8)
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 4. Z-Score Over Time
    ax4 = fig.add_subplot(gs[1, 2])
    colors = ['green' if z > 0 else 'red' for z in df['Z-Score']]
    ax4.bar(df['Date'], df['Z-Score'], color=colors, alpha=0.7, width=20)
    ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax4.axhline(y=1, color='green', linestyle='--', alpha=0.5, label='+1σ (good)')
    ax4.axhline(y=-1, color='red', linestyle='--', alpha=0.5, label='-1σ (poor)')
    ax4.axhline(y=2, color='darkgreen', linestyle='--', alpha=0.5, label='+2σ (excellent)')
    ax4.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax4.set_ylabel('Z-Score (std deviations)', fontsize=10, fontweight='bold')
    ax4.set_title('Z-Score: Statistical Significance', fontsize=12, fontweight='bold')
    ax4.legend(fontsize=8)
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 5. APR Difference vs Mean (bps)
    ax5 = fig.add_subplot(gs[2, 0])
    colors = ['green' if d > 0 else 'red' for d in df['Diff vs Mean (bps)']]
    ax5.bar(df['Date'], df['Diff vs Mean (bps)'], color=colors, alpha=0.7, width=20)
    ax5.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax5.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax5.set_ylabel('Basis Points', fontsize=10, fontweight='bold')
    ax5.set_title('APR Difference vs Network Mean', fontsize=12, fontweight='bold')
    ax5.grid(True, alpha=0.3, axis='y')
    ax5.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 6. Cumulative Extra Rewards vs Mean
    ax6 = fig.add_subplot(gs[2, 1])
    cumulative_rewards = df['Extra Rewards vs Mean (ETH)'].cumsum()
    ax6.fill_between(df['Date'], 0, cumulative_rewards, alpha=0.3, color='#2E86AB')
    ax6.plot(df['Date'], cumulative_rewards, marker='o', linewidth=2, color='#2E86AB', markersize=4)
    ax6.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax6.set_ylabel('Cumulative ETH', fontsize=10, fontweight='bold')
    ax6.set_title('Cumulative Extra Rewards vs Mean', fontsize=12, fontweight='bold')
    ax6.grid(True, alpha=0.3)
    ax6.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax6.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 7. Monthly Extra Rewards vs Mean and Median
    ax7 = fig.add_subplot(gs[2, 2])
    width = 10
    ax7.bar(df['Date'] - pd.Timedelta(days=width), df['Extra Rewards vs Mean (ETH)'],
            width=width, label='vs Mean', alpha=0.7, color='#2E86AB')
    ax7.bar(df['Date'] + pd.Timedelta(days=width), df['Extra Rewards vs Median (ETH)'],
            width=width, label='vs Median', alpha=0.7, color='#F18F01')
    ax7.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax7.set_ylabel('Extra Rewards (ETH)', fontsize=10, fontweight='bold')
    ax7.set_title('Monthly Extra Rewards: Mean & Median', fontsize=12, fontweight='bold')
    ax7.legend(fontsize=8)
    ax7.grid(True, alpha=0.3, axis='y')
    ax7.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax7.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 8. APR Difference vs Median (bps)
    ax8 = fig.add_subplot(gs[3, 0])
    colors = ['green' if d > 0 else 'red' for d in df['Diff vs Median (bps)']]
    ax8.bar(df['Date'], df['Diff vs Median (bps)'], color=colors, alpha=0.7, width=20)
    ax8.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax8.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax8.set_ylabel('Basis Points', fontsize=10, fontweight='bold')
    ax8.set_title('APR Difference vs Network Median', fontsize=12, fontweight='bold')
    ax8.grid(True, alpha=0.3, axis='y')
    ax8.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax8.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 9. Cumulative Extra Rewards vs Median
    ax9 = fig.add_subplot(gs[3, 1])
    cumulative_rewards_median = df['Extra Rewards vs Median (ETH)'].cumsum()
    ax9.fill_between(df['Date'], 0, cumulative_rewards_median, alpha=0.3, color='#F18F01')
    ax9.plot(df['Date'], cumulative_rewards_median, marker='o', linewidth=2, color='#F18F01', markersize=4)
    ax9.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax9.set_ylabel('Cumulative ETH', fontsize=10, fontweight='bold')
    ax9.set_title('Cumulative Extra Rewards vs Median', fontsize=12, fontweight='bold')
    ax9.grid(True, alpha=0.3)
    ax9.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax9.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # 10. All Cumulative Rewards Comparison
    ax10 = fig.add_subplot(gs[3, 2])
    cumulative_rewards_mean = df['Extra Rewards vs Mean (ETH)'].cumsum()
    ax10.plot(df['Date'], cumulative_rewards_mean, marker='o', linewidth=2,
             label='vs Mean', color='#2E86AB', markersize=4)
    ax10.plot(df['Date'], cumulative_rewards_median, marker='s', linewidth=2,
             label='vs Median', color='#F18F01', markersize=4)
    ax10.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax10.set_ylabel('Cumulative ETH', fontsize=10, fontweight='bold')
    ax10.set_title('Cumulative Extra Rewards: All Benchmarks', fontsize=12, fontweight='bold')
    ax10.legend(fontsize=8, loc='best')
    ax10.grid(True, alpha=0.3)
    ax10.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax10.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.suptitle(f'{ref_operator} Performance Analysis Dashboard{title_suffix}',
                 fontsize=16, fontweight='bold', y=0.998)

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        log_message(f"Plot saved to: {save_path}")

    return fig


def plot_operator_rankings_heatmap(monthly_operator_rankings, save_path=None):
    """
    Create a heatmap showing ALL operator rankings over time, ordered by average rank.

    Args:
        monthly_operator_rankings: Dict from analyse_operator_performance() functions
        save_path: Optional path to save the figure

    Returns:
        matplotlib figure object
    """
    import matplotlib.pyplot as plt
    import seaborn as sns

    # Get all unique operators and calculate their average rank
    operator_avg_ranks = {}
    operator_all_ranks = {}

    for month, rankings in monthly_operator_rankings.items():
        for op_data in rankings:
            op_name = op_data['operator']
            if op_name not in operator_all_ranks:
                operator_all_ranks[op_name] = []
            operator_all_ranks[op_name].append(op_data['rank'])

    # Calculate average rank for each operator
    for op_name, ranks in operator_all_ranks.items():
        operator_avg_ranks[op_name] = np.mean(ranks)

    # Sort operators by their average rank (best to worst)
    sorted_operators = sorted(operator_avg_ranks.keys(), key=lambda x: operator_avg_ranks[x])

    # Create matrix: operators x months
    months = sorted(monthly_operator_rankings.keys())
    data = []

    for operator in sorted_operators:
        row = []
        for month in months:
            # Find this operator's rank in this month
            op_data = next((op for op in monthly_operator_rankings[month] if op['operator'] == operator), None)
            if op_data:
                row.append(op_data['rank'])
            else:
                row.append(None)  # Operator didn't exist this month
        data.append(row)

    # Create DataFrame
    df_heatmap = pd.DataFrame(data, index=sorted_operators, columns=months)

    # Create figure - size dynamically based on number of operators and months
    num_operators = len(sorted_operators)
    fig, ax = plt.subplots(figsize=(max(12, len(months) * 0.8), max(10, num_operators * 0.4)))

    # Create heatmap
    sns.heatmap(df_heatmap, annot=True, fmt='.0f', cmap='RdYlGn_r',
                cbar_kws={'label': 'Rank (lower is better)'},
                linewidths=0.5, linecolor='gray', ax=ax,
                vmin=1, vmax=num_operators)

    # Highlight reference operator row if present
    ref_operator = CONFIG['REFERENCE_OPERATOR']
    if ref_operator in sorted_operators:
        ref_idx = sorted_operators.index(ref_operator)
        ax.add_patch(plt.Rectangle((0, ref_idx), len(months), 1,
                                   fill=False, edgecolor='blue', linewidth=3))

    ax.set_title(f'All Operator Rankings Over Time ({num_operators} operators, ordered by avg rank)',
                fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax.set_ylabel(f'Operator ({ref_operator} highlighted in blue)', fontsize=12, fontweight='bold')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        log_message(f"Heatmap saved to: {save_path}")

    return fig


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    log_message("="*80)
    log_message("STARTING LIDO PERFORMANCE ANALYSIS")
    log_message("="*80)

    # Validate configuration
    validate_config()

    # Step 1: Fetch and process Lido data (if not already complete)
    checkpoint = load_checkpoint()
    existing_results = load_results()

    # Check if data fetching is needed
    data_fetching_needed = (
        len(checkpoint.get('completed_operators', [])) < checkpoint.get('total_operators', 1) or
        not os.path.exists(RESULTS_FILE)
    )

    if data_fetching_needed:
        log_message("\n" + "="*80)
        log_message("STEP 1: FETCHING LIDO DATA")
        log_message("="*80)

        # Fetch operators and keys
        operator_keys = fetch_lido_operators()
        checkpoint['total_operators'] = len(operator_keys)
        save_checkpoint(checkpoint)

        operator_keys = fetch_operator_keys(operator_keys, checkpoint)

        # Fetch rewards data
        log_message("\n" + "="*80)
        log_message("STEP 2: PROCESSING OPERATOR REWARDS")
        log_message("="*80)

        daily_rewards_operator = process_all_operators(
            operator_keys,
            CONFIG['KILN_API_KEY'],
            checkpoint,
            existing_results
        )

        log_message(f"Data processing complete! Total operators: {len(checkpoint['completed_operators'])}/{checkpoint['total_operators']}")
    else:
        log_message("Data already fetched and processed. Skipping to analysis.")

    # Verify results file exists before proceeding to analysis
    if not os.path.exists(RESULTS_FILE):
        log_message(f"ERROR: Results file not found at {RESULTS_FILE}")
        log_message("Please complete the data fetching step first.")
        sys.exit(1)

    # Step 2: Run analysis with all data
    log_message("\n" + "="*80)
    log_message("STEP 3: RUNNING ANALYSIS (WITH ALL DATA)")
    log_message("="*80)

    analysis_results = analyse_operator_performance()

    # Save analysis results with dynamic filename
    ref_operator_clean = CONFIG['REFERENCE_OPERATOR'].lower().replace(' ', '_')
    analysis_csv_path = os.path.join(OUTPUT_DIR, f'{ref_operator_clean}_analysis.csv')
    analysis_results['monthly_dataframe'].to_csv(analysis_csv_path, index=False)
    log_message(f"Analysis DataFrame saved to: {analysis_csv_path}")

    # Print summary
    log_message(f"\nSummary for {CONFIG['REFERENCE_OPERATOR']} (with all data):")
    for key, value in analysis_results['summary'].items():
        log_message(f"  {key}: {value}")

    # Generate plots
    plot_path = os.path.join(OUTPUT_DIR, f'{ref_operator_clean}_performance_dashboard.png')
    plot_operator_performance_analysis(
        analysis_results['monthly_dataframe'],
        title_suffix="",
        save_path=plot_path
    )

    heatmap_path = os.path.join(OUTPUT_DIR, f'operator_rankings_heatmap.png')
    plot_operator_rankings_heatmap(
        analysis_results['monthly_operator_rankings'],
        save_path=heatmap_path
    )

    # Step 3: Run analysis without outliers
    log_message("\n" + "="*80)
    log_message("STEP 4: RUNNING ANALYSIS (NO OUTLIERS)")
    log_message("="*80)

    analysis_no_outliers = analyse_operator_performance_no_outliers()

    # Save analysis results
    analysis_no_outliers_csv_path = os.path.join(OUTPUT_DIR, f'{ref_operator_clean}_analysis_no_outliers.csv')
    analysis_no_outliers['monthly_dataframe'].to_csv(analysis_no_outliers_csv_path, index=False)
    log_message(f"Analysis DataFrame (no outliers) saved to: {analysis_no_outliers_csv_path}")

    # Print summary
    log_message(f"\nSummary for {CONFIG['REFERENCE_OPERATOR']} (without outliers):")
    for key, value in analysis_no_outliers['summary'].items():
        log_message(f"  {key}: {value}")

    # Generate plots
    plot_no_outliers_path = os.path.join(OUTPUT_DIR, f'{ref_operator_clean}_performance_dashboard_no_outliers.png')
    plot_operator_performance_analysis(
        analysis_no_outliers['monthly_dataframe'],
        title_suffix=" (No Outliers)",
        save_path=plot_no_outliers_path
    )

    heatmap_no_outliers_path = os.path.join(OUTPUT_DIR, f'operator_rankings_heatmap_no_outliers.png')
    plot_operator_rankings_heatmap(
        analysis_no_outliers['monthly_operator_rankings'],
        save_path=heatmap_no_outliers_path
    )

    # Step 4: Create comparison summary
    log_message("\n" + "="*80)
    log_message("STEP 5: COMPARISON SUMMARY")
    log_message("="*80)

    log_message("\nComparison: With vs Without Outliers")
    log_message("-" * 50)
    log_message(f"Average Rank - All Data: {analysis_results['summary']['avg_rank']:.2f}")
    log_message(f"Average Rank - No Outliers: {analysis_no_outliers['summary']['avg_rank']:.2f}")
    log_message(f"Average Percentile - All Data: {analysis_results['summary']['avg_percentile']:.2f}")
    log_message(f"Average Percentile - No Outliers: {analysis_no_outliers['summary']['avg_percentile']:.2f}")
    log_message(f"Avg APR Diff vs Mean (bps) - All Data: {analysis_results['summary']['avg_apr_diff_vs_mean_bps']:.2f}")
    log_message(f"Avg APR Diff vs Mean (bps) - No Outliers: {analysis_no_outliers['summary']['avg_apr_diff_vs_mean_bps']:.2f}")
    log_message(f"Total Extra Rewards vs Mean - All Data: {analysis_results['summary']['total_extra_rewards_vs_mean']:.2f} ETH")
    log_message(f"Total Extra Rewards vs Mean - No Outliers: {analysis_no_outliers['summary']['total_extra_rewards_vs_mean']:.2f} ETH")

    log_message("\n" + "="*80)
    log_message("ANALYSIS COMPLETE!")
    log_message("="*80)
    log_message(f"\nAll results saved to: {OUTPUT_DIR}")
    log_message("Generated files:")
    log_message(f"  - {analysis_csv_path}")
    log_message(f"  - {analysis_no_outliers_csv_path}")
    log_message(f"  - {plot_path}")
    log_message(f"  - {plot_no_outliers_path}")
    log_message(f"  - {heatmap_path}")
    log_message(f"  - {heatmap_no_outliers_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"FATAL ERROR: Script crashed with error: {str(e)}")
        import traceback
        log_message(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)
