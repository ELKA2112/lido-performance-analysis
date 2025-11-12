"""
Lido Performance Analysis Script - Detailed Version with CL/EL Split

This script performs a detailed analysis with separate consensus layer (CL) and execution layer (EL) metrics.

Process:
1. Fetch all Lido operators and their validator keys from TheGraph
2. Fetch daily rewards data for each operator (CL + EL separately)
3. Fetch accurate stake balances using the /stakes endpoint
4. Analyze reference operator's performance vs other operators
5. Generate three separate analyses: CL only, EL only, and Total (CL+EL)
6. Compare analysis with and without outliers for each layer
7. Generate visualizations and export heatmap rankings

Key Features:
- **CL/EL Split**: Separate analysis for consensus rewards, execution rewards, and total
- **Operator Filtering**: Optionally filter analysis to specific operator IDs
- **Heatmap Export**: CSV file with monthly rankings and average ranks
- Accurate balance tracking via /eth/stakes endpoint
- Checkpointing for resume capability
- Outlier detection using standard deviations

Requirements:
- Python 3.8+
- pandas, numpy, requests, aiohttp, matplotlib, seaborn

Configuration:
- Set environment variables or edit the CONFIG section below:
  - KILN_API_KEY: Your Kiln API key
  - THEGRAPH_API_KEY: Your TheGraph API key
  - REFERENCE_OPERATOR: The operator name to analyze (default: 'Kiln')
  - FILTER_OPERATOR_IDS: List of operator IDs to include in analysis (optional)

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
    'REFERENCE_OPERATOR': 'Kiln',  # The operator name to analyze

    # Operator filtering (optional)
    # Set to list of operator IDs to filter analysis to specific operators
    # Example: ['0', '1', '2', '3'] - only analyze these operator IDs
    # Set to None or empty list to include all operators
    'FILTER_OPERATOR_IDS': ['38','37','3','2','5','29','35','0'],  # None = analyze all operators

    # Analysis configuration
    'MIN_DAYS_PER_MONTH': 20,  # Minimum days of data required to include a month
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

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, 'lido_checkpoint_detailed.json')
RESULTS_FILE = os.path.join(OUTPUT_DIR, 'lido_results_detailed.json')
LOG_FILE = os.path.join(OUTPUT_DIR, 'lido_processing_detailed.log')


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
        log_message(f"Retrieved {len(operators_list)} operators from subgraph")

        # Build full operator list
        operator_keys = {}
        for operator in operators_list:
            operator_id = operator['id']
            operator_name = operator['name']
            operator_keys[operator_id] = {
                'name': operator_name,
                'keys': []
            }

        # Apply filtering if configured
        filter_ids = CONFIG.get('FILTER_OPERATOR_IDS')
        if filter_ids:
            # Find reference operator ID
            ref_operator_name = CONFIG['REFERENCE_OPERATOR']
            ref_operator_id = None
            for op_id, op_data in operator_keys.items():
                if op_data['name'] == ref_operator_name:
                    ref_operator_id = op_id
                    break

            # Auto-include reference operator if not in filter list
            filter_ids_set = set(filter_ids)
            if ref_operator_id and ref_operator_id not in filter_ids_set:
                log_message(f"Auto-including reference operator '{ref_operator_name}' (ID: {ref_operator_id}) in filter list")
                filter_ids_set.add(ref_operator_id)

            # Filter operators
            operator_keys = {op_id: data for op_id, data in operator_keys.items() if op_id in filter_ids_set}
            log_message(f"Filtered to {len(operator_keys)} operators (including reference operator)")
        else:
            log_message(f"No filtering applied - will fetch all {len(operator_keys)} operators")

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
        # Add balance when delegated
        if 'delegated_at' in stake and stake['delegated_at']:
            activation_date = stake['delegated_at'][:10]
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


async def get_eth_rewards_daily_async_split(keys_list, api_key, session, semaphore):
    """
    Async fetch of daily ETH rewards with CL/EL split.
    Returns separate consensus_rewards and execution_rewards.
    """
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
            # Split rewards into CL and EL
            cl_rewards = int(entry.get('consensus_rewards', 0)) / 1E18
            el_rewards = int(entry.get('execution_rewards', 0)) / 1E18
            total_rewards = int(entry.get('rewards', 0)) / 1E18

            if date not in batch_output:
                batch_output[date] = {
                    'cl_rewards': 0,
                    'el_rewards': 0,
                    'total_rewards': 0
                }
            batch_output[date]['cl_rewards'] += cl_rewards
            batch_output[date]['el_rewards'] += el_rewards
            batch_output[date]['total_rewards'] += total_rewards

        return batch_output

    # Fetch all batches concurrently
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
                output[date] = {'cl_rewards': 0, 'el_rewards': 0, 'total_rewards': 0}
            output[date]['cl_rewards'] += values['cl_rewards']
            output[date]['el_rewards'] += values['el_rewards']
            output[date]['total_rewards'] += values['total_rewards']

    return output


async def process_operator_async(operator_id, operator_data, api_key, session, semaphore):
    """Process a single operator's rewards (CL/EL split) and accurate balances"""
    try:
        log_message(f"Fetching data for operator {operator_id} - {operator_data['name']}")

        # Fetch rewards (with CL/EL split) and accurate balances in parallel
        rewards_task = get_eth_rewards_daily_async_split(
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

        # Add rewards (with CL/EL split)
        for date, data in rewards_data.items():
            combined_data[date] = {
                'cl_rewards': data['cl_rewards'],
                'el_rewards': data['el_rewards'],
                'total_rewards': data['total_rewards'],
                'balance': 0
            }

        # Add accurate balances from stakes endpoint
        for date, balance in balances_data.items():
            if date in combined_data:
                combined_data[date]['balance'] = balance
            else:
                combined_data[date] = {
                    'cl_rewards': 0,
                    'el_rewards': 0,
                    'total_rewards': 0,
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
    log_message("Fetching CL/EL split rewards and accurate balance tracking")

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

def analyse_operator_performance(reward_type='total', remove_outliers=False):
    """
    Analyze reference operator's performance vs other Lido node operators.

    Args:
        reward_type: 'cl', 'el', or 'total'
        remove_outliers: If True, remove outliers > N std deviations

    Returns:
        dict: Analysis results including APR comparisons, rankings, and DataFrame
    """
    ref_operator = CONFIG['REFERENCE_OPERATOR']
    outlier_suffix = " (no outliers)" if remove_outliers else ""
    log_message(f"Starting {reward_type.upper()} analysis for {ref_operator}{outlier_suffix}")

    with open(RESULTS_FILE, 'r') as f:
        lido_results_raw = json.load(f)

    # Apply operator filtering if configured
    filter_ids = CONFIG.get('FILTER_OPERATOR_IDS')
    if filter_ids:
        log_message(f"Filtering to {len(filter_ids)} specified operator IDs: {filter_ids}")
        lido_results_raw = {op_id: data for op_id, data in lido_results_raw.items() if op_id in filter_ids}
        log_message(f"Analyzing {len(lido_results_raw)} operators after filtering")

    # Filter to date range
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

    # Remove outliers if requested
    if remove_outliers:
        lido_results = {}
        for operator_id, operator_data in lido_results_filtered.items():
            lido_results[operator_id] = {
                'name': operator_data['name'],
                'keys': operator_data.get('keys', []),
                'rewards': {}
            }

            # Group by month
            monthly_rewards = {}
            reward_field = f'{reward_type}_rewards'
            for date, rewards_data in operator_data['rewards'].items():
                month = date[:7]
                if month not in monthly_rewards:
                    monthly_rewards[month] = []
                monthly_rewards[month].append((date, rewards_data.get(reward_field, 0)))

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
    else:
        lido_results = lido_results_filtered

    # Determine which reward field to use
    reward_field = f'{reward_type}_rewards'

    # Calculate monthly metrics for each operator
    monthly_metrics = {}

    for operator_id in lido_results:
        operator_name = lido_results[operator_id]['name']
        monthly_metrics[operator_name] = {}

        for date in lido_results[operator_id]['rewards']:
            rewards = lido_results[operator_id]['rewards'][date].get(reward_field, 0)
            balance = lido_results[operator_id]['rewards'][date].get('balance', 0)
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

    # Calculate network-wide metrics (excluding reference operator)
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

                # Calculate ranking
                all_aprs = all_operator_aprs[month] + [ref_apr]
                all_aprs_sorted = sorted(all_aprs, reverse=True)
                ref_rank = all_aprs_sorted.index(ref_apr) + 1

                # Calculate percentile
                percentile = (sum(1 for apr in all_aprs if apr <= ref_apr) / len(all_aprs)) * 100

                # Calculate extra rewards
                month_extra_rewards_vs_mean = 0
                month_extra_rewards_vs_median = 0

                for date in lido_results[ref_operator_id]['rewards']:
                    if date.startswith(month):
                        daily_balance = lido_results[ref_operator_id]['rewards'][date].get('balance', 0)
                        month_extra_rewards_vs_mean += daily_balance * ((ref_apr - network_weighted_apr) / 365)
                        month_extra_rewards_vs_median += daily_balance * ((ref_apr - network_median_apr) / 365)

                total_extra_rewards_vs_mean += month_extra_rewards_vs_mean
                total_extra_rewards_vs_median += month_extra_rewards_vs_median

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
                    'apr_diff_vs_mean': ref_apr - network_weighted_apr,
                    'apr_diff_vs_median': ref_apr - network_median_apr,
                    'apr_diff_vs_mean_bps': (ref_apr - network_weighted_apr) * 10000,
                    'apr_diff_vs_median_bps': (ref_apr - network_median_apr) * 10000,
                    'percentile_rank': percentile,
                    'z_score': z_score,
                    'extra_rewards_vs_mean': month_extra_rewards_vs_mean,
                    'extra_rewards_vs_median': month_extra_rewards_vs_median,
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
            'Diff vs Mean (bps)': data['apr_diff_vs_mean_bps'],
            'Diff vs Median (bps)': data['apr_diff_vs_median_bps'],
            'Extra Rewards vs Mean (ETH)': data['extra_rewards_vs_mean'],
            'Extra Rewards vs Median (ETH)': data['extra_rewards_vs_median'],
            'Z-Score': data['z_score'],
            'Days': data['days_in_month']
        })

    df_monthly = pd.DataFrame(df_data)

    log_message(f"Analysis complete: {len(ref_analysis)} months analyzed")

    return {
        'reward_type': reward_type,
        'ref_analysis': ref_analysis_sorted,
        'network_metrics': network_metrics,
        'monthly_dataframe': df_monthly,
        'monthly_operator_rankings': monthly_operator_rankings,
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
            'start_date': CONFIG['START_DATE'],
            'end_date': CONFIG['END_DATE'],
            'outliers_removed': remove_outliers
        }
    }


def export_heatmap_rankings(monthly_operator_rankings, reward_type='total', remove_outliers=False):
    """
    Export heatmap rankings to CSV with monthly ranks and average rank.

    Args:
        monthly_operator_rankings: Dict from analyse_operator_performance()
        reward_type: 'cl', 'el', or 'total'
        remove_outliers: If True, adds suffix to filename
    """
    # Calculate average rank for each operator
    operator_all_ranks = {}

    for month, rankings in monthly_operator_rankings.items():
        for op_data in rankings:
            op_name = op_data['operator']
            if op_name not in operator_all_ranks:
                operator_all_ranks[op_name] = []
            operator_all_ranks[op_name].append(op_data['rank'])

    # Build data for CSV
    csv_data = []
    for operator, ranks in operator_all_ranks.items():
        row = {
            'Operator': operator,
            'Average Rank': round(np.mean(ranks), 2),
            'Best Rank': min(ranks),
            'Worst Rank': max(ranks),
            'Months Active': len(ranks)
        }

        # Add monthly ranks
        for month in sorted(monthly_operator_rankings.keys()):
            op_data = next((op for op in monthly_operator_rankings[month] if op['operator'] == operator), None)
            if op_data:
                row[f'Rank_{month}'] = op_data['rank']
            else:
                row[f'Rank_{month}'] = None

        csv_data.append(row)

    # Sort by average rank
    csv_data = sorted(csv_data, key=lambda x: x['Average Rank'])

    # Create DataFrame
    df = pd.DataFrame(csv_data)

    # Generate filename
    ref_operator_clean = CONFIG['REFERENCE_OPERATOR'].lower().replace(' ', '_')
    outlier_suffix = '_no_outliers' if remove_outliers else ''
    filename = f'operator_rankings_{reward_type}{outlier_suffix}.csv'
    filepath = os.path.join(OUTPUT_DIR, filename)

    # Save to CSV
    df.to_csv(filepath, index=False)
    log_message(f"Heatmap rankings exported to: {filepath}")

    return filepath


# ============================================================================
# VISUALIZATION FUNCTIONS (simplified for now - can be expanded)
# ============================================================================

def plot_operator_performance(df, reward_type='total', remove_outliers=False, save_path=None):
    """
    Create basic performance visualization.
    """
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    ref_operator = CONFIG['REFERENCE_OPERATOR']
    outlier_suffix = " (No Outliers)" if remove_outliers else ""

    df = df.copy()
    df['Date'] = pd.to_datetime(df['Month'] + '-01')

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'{ref_operator} {reward_type.upper()} Performance{outlier_suffix}',
                 fontsize=16, fontweight='bold')

    # APR comparison
    ax = axes[0, 0]
    ax.plot(df['Date'], df[f'{ref_operator} APR (%)'], marker='o', label=ref_operator, linewidth=2)
    ax.plot(df['Date'], df['Network Mean APR (%)'], marker='s', label='Network Mean', linestyle='--')
    ax.plot(df['Date'], df['Network Median APR (%)'], marker='^', label='Network Median', linestyle='--')
    ax.set_ylabel('APR (%)', fontweight='bold')
    ax.set_title('APR Comparison')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Ranking
    ax = axes[0, 1]
    ax.plot(df['Date'], df[f'{ref_operator} Rank'], marker='o', linewidth=2, color='#2E86AB')
    ax.invert_yaxis()
    ax.set_ylabel('Rank (lower is better)', fontweight='bold')
    ax.set_title('Ranking Over Time')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # APR Difference
    ax = axes[1, 0]
    colors = ['green' if d > 0 else 'red' for d in df['Diff vs Mean (bps)']]
    ax.bar(df['Date'], df['Diff vs Mean (bps)'], color=colors, alpha=0.7, width=20)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.set_ylabel('Basis Points', fontweight='bold')
    ax.set_title('APR Difference vs Network Mean')
    ax.grid(True, alpha=0.3, axis='y')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Cumulative Extra Rewards
    ax = axes[1, 1]
    cumulative = df['Extra Rewards vs Mean (ETH)'].cumsum()
    ax.fill_between(df['Date'], 0, cumulative, alpha=0.3)
    ax.plot(df['Date'], cumulative, marker='o', linewidth=2)
    ax.set_ylabel('Cumulative ETH', fontweight='bold')
    ax.set_title('Cumulative Extra Rewards vs Mean')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        log_message(f"Plot saved to: {save_path}")

    return fig


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    log_message("="*80)
    log_message("STARTING LIDO DETAILED PERFORMANCE ANALYSIS (CL/EL SPLIT)")
    log_message("="*80)

    # Validate configuration
    validate_config()

    # Display operator filtering status
    if CONFIG.get('FILTER_OPERATOR_IDS'):
        log_message(f"Operator filtering ENABLED: {len(CONFIG['FILTER_OPERATOR_IDS'])} operators")
    else:
        log_message("Operator filtering DISABLED: Analyzing all operators")

    # Step 1: Fetch and process Lido data
    checkpoint = load_checkpoint()
    existing_results = load_results()

    data_fetching_needed = (
        len(checkpoint.get('completed_operators', [])) < checkpoint.get('total_operators', 1) or
        not os.path.exists(RESULTS_FILE)
    )

    if data_fetching_needed:
        log_message("\n" + "="*80)
        log_message("STEP 1: FETCHING LIDO DATA")
        log_message("="*80)

        operator_keys = fetch_lido_operators()
        checkpoint['total_operators'] = len(operator_keys)
        save_checkpoint(checkpoint)

        operator_keys = fetch_operator_keys(operator_keys, checkpoint)

        log_message("\n" + "="*80)
        log_message("STEP 2: PROCESSING OPERATOR REWARDS (CL/EL SPLIT)")
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

    # Verify results file exists
    if not os.path.exists(RESULTS_FILE):
        log_message(f"ERROR: Results file not found at {RESULTS_FILE}")
        log_message("Please complete the data fetching step first.")
        sys.exit(1)

    # Step 2: Run analyses for CL, EL, and Total (with and without outliers)
    ref_operator_clean = CONFIG['REFERENCE_OPERATOR'].lower().replace(' ', '_')

    reward_types = ['cl', 'el', 'total']

    for reward_type in reward_types:
        for remove_outliers in [False, True]:
            outlier_suffix = '_no_outliers' if remove_outliers else ''

            log_message("\n" + "="*80)
            log_message(f"ANALYZING {reward_type.upper()} REWARDS{' (NO OUTLIERS)' if remove_outliers else ''}")
            log_message("="*80)

            # Run analysis
            analysis = analyse_operator_performance(reward_type=reward_type, remove_outliers=remove_outliers)

            # Save CSV
            csv_filename = f'{ref_operator_clean}_{reward_type}_analysis{outlier_suffix}.csv'
            csv_path = os.path.join(OUTPUT_DIR, csv_filename)
            analysis['monthly_dataframe'].to_csv(csv_path, index=False)
            log_message(f"Analysis saved to: {csv_path}")

            # Export heatmap rankings
            export_heatmap_rankings(
                analysis['monthly_operator_rankings'],
                reward_type=reward_type,
                remove_outliers=remove_outliers
            )

            # Generate plot
            plot_filename = f'{ref_operator_clean}_{reward_type}_performance{outlier_suffix}.png'
            plot_path = os.path.join(OUTPUT_DIR, plot_filename)
            plot_operator_performance(
                analysis['monthly_dataframe'],
                reward_type=reward_type,
                remove_outliers=remove_outliers,
                save_path=plot_path
            )

            # Print summary
            log_message(f"\nSummary for {CONFIG['REFERENCE_OPERATOR']} ({reward_type.upper()}{' - No Outliers' if remove_outliers else ''}):")
            for key, value in analysis['summary'].items():
                log_message(f"  {key}: {value}")

    log_message("\n" + "="*80)
    log_message("DETAILED ANALYSIS COMPLETE!")
    log_message("="*80)
    log_message(f"\nAll results saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"FATAL ERROR: Script crashed with error: {str(e)}")
        import traceback
        log_message(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)
