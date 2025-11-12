# Lido Performance Analysis

Comprehensive analysis tool for comparing any Lido node operator's performance against other operators, with detailed consensus layer (CL) and execution layer (EL) reward analysis.

## Features

- **CL/EL Split Analysis**: Separate analysis for consensus rewards (attestations, proposals), execution rewards (MEV, tips), and total rewards
- **Configurable Reference Operator**: Easily change which operator to analyze by setting `REFERENCE_OPERATOR` in the CONFIG
- **Operator Filtering**: Optionally analyze only a subset of operators by specifying operator IDs
- **Accurate Balance Tracking**: Uses the Kiln API service's `/eth/stakes` endpoint to track validator activations and exits for precise active balance calculations
- **Comprehensive Metrics**: APR comparisons, rankings, percentiles, z-scores, and extra rewards vs multiple benchmarks
- **Flexible Outlier Detection**: Choose between absolute threshold (filter extreme MEV days) or statistical methods
- **Organized Outputs**: Results organized by filtering method (absolute, std, no_filtering) for easy comparison
- **Outlier Tracking**: Comprehensive CSV export of all filtered data points with statistics and context
- **Checkpointing**: Resume capability for long-running data fetching jobs
- **Visualizations**: Automated performance plots and operator ranking heatmaps for all reward types
- **GitHub Actions**: Automated scheduled analysis with result artifacts

## Installation

### Prerequisites

- Python 3.8 or higher
- Kiln API key (from [Kiln API service](https://api.kiln.fi) - used to fetch validator rewards data)
- TheGraph API key (for querying Lido subgraph data)

### Setup

1. Clone this repository:
```bash
git clone https://github.com/YOUR_USERNAME/lido-performance-analysis.git
cd lido-performance-analysis
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up API keys (choose one method):

**Option A: Environment variables**
```bash
export KILN_API_KEY="your_kiln_api_key"
export THEGRAPH_API_KEY="your_thegraph_api_key"
```

**Option B: Edit the script directly**

Edit `scripts/lido_perf_analysis_detailed.py` and update the `CONFIG` dictionary:
```python
CONFIG = {
    'KILN_API_KEY': 'your_kiln_api_key',
    'THEGRAPH_API_KEY': 'your_thegraph_api_key',
    # ...
}
```

## Usage

### Run Locally

```bash
python scripts/lido_perf_analysis_detailed.py
```

The script will:
1. Fetch all Lido operators and their validator keys (with optional operator filtering)
2. Fetch daily rewards with CL/EL split and accurate balance data
3. Perform 6 analyses: CL-only, EL-only, and Total rewards, each with and without outlier filtering
4. Generate CSV files, heatmaps, and performance visualizations
5. Save results to `output/` directory, organized by filtering method (absolute, std, no_filtering)

### Run via GitHub Actions

1. Add secrets to your GitHub repository:
   - `KILN_API_KEY`
   - `THEGRAPH_API_KEY`

2. Go to Actions tab → "Lido Detailed Performance Analysis (CL/EL Split)" → "Run workflow"

3. Optional: Specify comma-separated operator IDs to filter (e.g., `0,1,2,3` to analyze only operators 0-3)

4. Results will be:
   - Committed to the repo in `output/absolute/`, `output/std/`, and `output/no_filtering/` directories
   - Available as downloadable artifacts (30-day retention)



## Output Files

The script generates files organized by filtering method in subdirectories:

### Root Directory Files
- `lido_checkpoint_detailed.json` - Resume checkpoint for data fetching
- `lido_results_detailed.json` - Raw daily rewards (CL/EL split) and balance data
- `lido_processing_detailed.log` - Execution logs

### Organized by Filtering Method

Results are organized into three subdirectories based on the filtering method applied:

#### `output/absolute/` - Absolute Threshold Filtering
Days with EL rewards > 100 ETH are filtered from EL and Total analysis (CL never filtered):
- `{operator}_cl_analysis_no_outliers.csv` - CL analysis (no filtering applied)
- `{operator}_el_analysis_no_outliers.csv` - EL analysis (extreme MEV days removed)
- `{operator}_total_analysis_no_outliers.csv` - Total analysis (same days as EL filtered)
- `{operator}_cl_performance_no_outliers.png` - CL performance plots
- `{operator}_el_performance_no_outliers.png` - EL performance plots
- `{operator}_total_performance_no_outliers.png` - Total performance plots
- `operator_rankings_cl_no_outliers.csv` - CL rankings CSV
- `operator_rankings_el_no_outliers.csv` - EL rankings CSV
- `operator_rankings_total_no_outliers.csv` - Total rankings CSV
- `operator_rankings_cl_heatmap_no_outliers.png` - CL rankings heatmap
- `operator_rankings_el_heatmap_no_outliers.png` - EL rankings heatmap
- `operator_rankings_total_heatmap_no_outliers.png` - Total rankings heatmap
- `removed_outliers_el.csv` - List of filtered EL data points with statistics
- `removed_outliers_total.csv` - List of filtered Total data points with statistics

#### `output/std/` - Statistical Method Filtering
Outliers > 2 standard deviations above mean are filtered:
- Similar file structure as `absolute/` directory
- Uses statistical outlier detection instead of absolute threshold

#### `output/no_filtering/` - Unfiltered Baseline
Complete analysis with no outlier filtering applied:
- `{operator}_cl_analysis.csv` - Complete CL analysis
- `{operator}_el_analysis.csv` - Complete EL analysis
- `{operator}_total_analysis.csv` - Complete Total analysis
- `{operator}_cl_performance.png` - CL performance plots
- `{operator}_el_performance.png` - EL performance plots
- `{operator}_total_performance.png` - Total performance plots
- `operator_rankings_cl.csv` - CL rankings CSV
- `operator_rankings_el.csv` - EL rankings CSV
- `operator_rankings_total.csv` - Total rankings CSV
- `operator_rankings_cl_heatmap.png` - CL rankings heatmap
- `operator_rankings_el_heatmap.png` - EL rankings heatmap
- `operator_rankings_total_heatmap.png` - Total rankings heatmap

**Note**: `{operator}` is replaced with the lowercase, underscore-separated name of your reference operator (e.g., `kiln`, `chorus_one`, `p2p.org`).

## Configuration

The script is fully parametric - you can analyze any Lido node operator by simply changing the `REFERENCE_OPERATOR` setting.

Edit the `CONFIG` dictionary in `scripts/lido_perf_analysis_detailed.py`:

```python
CONFIG = {
    # Reference operator - The operator to analyze
    # Must match the operator name exactly as it appears in the Lido subgraph
    'REFERENCE_OPERATOR': 'Kiln',

    # Operator filtering (optional)
    # Set to list of operator IDs to filter analysis to specific operators
    # Example: ['0', '1', '2', '3'] - only analyze these operator IDs
    # Set to None or empty list to include all operators
    'FILTER_OPERATOR_IDS': None,

    # Date range for analysis
    'START_DATE': '2023-01-01',
    'END_DATE': '2025-08-31',

    # Minimum days of data required per month
    'MIN_DAYS_PER_MONTH': 20,

    # Outlier detection configuration
    # Filter based on EL rewards only - extreme MEV is the main outlier source
    # Days with EL > threshold are removed from both EL and Total analysis
    # CL analysis is never filtered (stable, predictable rewards)
    'OUTLIER_METHOD': 'absolute',  # 'std' (mean + N*std) or 'absolute' (fixed ETH threshold)
    'OUTLIER_STD_THRESHOLD': 2,  # For 'std' method: standard deviations above mean
    'OUTLIER_ABSOLUTE_THRESHOLD_EL': 100,  # Remove days where EL rewards > 100 ETH

    # API rate limiting
    'MAX_CONCURRENT_REQUESTS': 10,
    'BATCH_SIZE': 81,
    'MAX_RETRIES': 10,
}
```

### Analyzing Different Operators

To analyze a different operator, simply change the `REFERENCE_OPERATOR` value to match the operator name exactly as it appears in the Lido subgraph:

**Examples:**
- `'REFERENCE_OPERATOR': 'Kiln'` → Generates `kiln_cl_analysis.csv`, `kiln_el_analysis.csv`, `kiln_total_analysis.csv`, etc.
- `'REFERENCE_OPERATOR': 'Figment'` → Generates `figment_cl_analysis.csv`, `figment_el_analysis.csv`, etc.
- `'REFERENCE_OPERATOR': 'Chorus One'` → Generates `chorus_one_cl_analysis.csv`, `chorus_one_el_analysis.csv`, etc.
- `'REFERENCE_OPERATOR': 'P2P.org'` → Generates `p2p.org_cl_analysis.csv`, `p2p.org_el_analysis.csv`, etc.

All plots, CSV files, and analysis will automatically reference your chosen operator. The operator name must match exactly as it appears in the Lido protocol.

### Filtering to Specific Operators

To analyze only a subset of operators (e.g., for faster testing or focused analysis):

```python
'FILTER_OPERATOR_IDS': ['0', '1', '2', '3', '5', '29', '35', '37', '38']
```

The reference operator is automatically included even if not in the filter list, ensuring comparisons always work correctly.

## Analysis Methodology

### CL/EL Reward Split

The script analyzes three reward components separately:

1. **Consensus Layer (CL)**: Attestations, sync committee rewards, and block proposals (stable, predictable)
2. **Execution Layer (EL)**: MEV and priority fees from transaction ordering (volatile, can have extreme outliers)
3. **Total**: Sum of CL + EL rewards

This separation allows for more accurate performance analysis since CL and EL have fundamentally different characteristics.

### Balance Calculation

The script uses two Kiln API endpoints for maximum accuracy:

1. **`/eth/rewards`**: Provides daily rewards data with `consensus_rewards` and `execution_rewards` split
2. **`/eth/stakes`**: Provides validator activation (`delegated_at`) and exit (`exited_at`) dates

By tracking activations and exits, the script calculates the **true active balance** at any point in time (32 ETH per active validator), excluding validators that have exited but not yet withdrawn.

### APR Calculation

```
Monthly APR = (Total Monthly Rewards / Average Daily Balance) × (365 / Days in Month)
```

### Benchmarks

The reference operator's performance is compared against:
- **Network Mean**: Stake-weighted average APR (excluding the reference operator)
- **Network Median**: Median APR across all operators (excluding the reference operator)
- **Top 10 Mean**: Average APR of top 10 performing operators
- **Top 10 Median**: Median APR of top 10 performing operators

### Outlier Filtering

The script offers two outlier detection methods:

#### Absolute Threshold Method (Recommended)
- **Simple and transparent**: Removes days where EL rewards exceed a fixed threshold (default: 100 ETH)
- **EL-based filtering**:
  - **CL analysis**: Never filtered (stable, predictable consensus rewards)
  - **EL analysis**: Days with EL > 100 ETH removed
  - **Total analysis**: Same days as EL filtered (based on EL component only, not total value)
- **Rationale**: Extreme MEV days (100+ ETH) are rare outliers that skew performance comparisons

#### Statistical Method
- **Mean + N*STD**: Removes daily rewards more than N standard deviations above the monthly mean
- Configurable threshold (default: 2 standard deviations)
- Applied independently to each reward type

Both methods export a comprehensive CSV (`removed_outliers_*.csv`) showing:
- Which data points were filtered
- Statistics: mean, median, std, percentile, rank
- Context: EL reward value that triggered the filter, threshold values

**Why filter on EL only?** EL rewards are highly volatile due to MEV, while CL rewards are stable and predictable. Filtering extreme MEV days provides a fairer baseline for comparing operator performance.

## Metrics Explained

- **APR**: Annualized percentage return based on monthly rewards
- **Rank**: The reference operator's ranking among all operators (1 = best)
- **Percentile**: Percentile ranking (100 = top 1%)
- **Z-Score**: Standard deviations above/below mean (>1 = good, >2 = excellent)
- **Diff vs Benchmark (bps)**: APR difference in basis points (100 bps = 1%)
- **Extra Rewards**: Additional ETH earned vs benchmark APR

## Troubleshooting

### Data Fetching Issues

If data fetching is interrupted:
1. The checkpoint file (`lido_checkpoint_detailed.json`) tracks progress
2. Re-run the script - it will resume from the last completed operator
3. Check `output/lido_processing_detailed.log` for detailed error messages

### API Rate Limiting

If you hit rate limits:
1. Reduce `MAX_CONCURRENT_REQUESTS` in CONFIG
2. Increase retry delays by adjusting the sleep time in `fetch_api_async()`

### Missing Data

If certain months are missing:
- Ensure the operator had at least `MIN_DAYS_PER_MONTH` days of data
- Check the date range in CONFIG matches your needs

## Support

For issues or questions, please open an issue on GitHub.
