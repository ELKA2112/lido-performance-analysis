# Lido Performance Analysis

Comprehensive analysis tool for comparing Kiln's performance against other Lido node operators.

## Features

- **Accurate Balance Tracking**: Uses Kiln API's `/eth/stakes` endpoint to track validator activations and exits for precise active balance calculations
- **Comprehensive Metrics**: APR comparisons, rankings, percentiles, z-scores, and extra rewards vs multiple benchmarks
- **Statistical Analysis**: Includes outlier detection and removal using standard deviations
- **Checkpointing**: Resume capability for long-running data fetching jobs
- **Visualizations**: Automated dashboard plots and operator ranking heatmaps
- **GitHub Actions**: Automated scheduled analysis with result artifacts

## Installation

### Prerequisites

- Python 3.8 or higher
- Kiln API key
- TheGraph API key

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

Edit `scripts/lido_perf_analysis.py` and update the `CONFIG` dictionary:
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
python scripts/lido_perf_analysis.py
```

The script will:
1. Fetch all Lido operators and their validator keys
2. Fetch daily rewards and accurate balance data
3. Perform analysis with and without outliers
4. Generate CSV files and visualizations
5. Save results to `output/` directory

### Run via GitHub Actions

1. Add secrets to your GitHub repository:
   - `KILN_API_KEY`
   - `THEGRAPH_API_KEY`

2. Go to Actions tab → "Lido Performance Analysis" → "Run workflow"

3. Results will be:
   - Committed to the repo in `output/` directory
   - Available as downloadable artifacts (30-day retention)



## Output Files

The script generates the following files in `output/`:

### Data Files
- `lido_checkpoint.json` - Resume checkpoint for data fetching
- `lido_results.json` - Raw daily rewards and balance data
- `lido_processing.log` - Execution logs

### Analysis Results
- `kiln_analysis_2023_onwards.csv` - Full analysis with all data
- `kiln_analysis_2023_onwards_no_outliers.csv` - Analysis with outliers removed

### Visualizations
- `kiln_performance_dashboard_2023_onwards.png` - 10-plot dashboard (all data)
- `kiln_performance_dashboard_2023_onwards_no_outliers.png` - Dashboard (no outliers)
- `operator_rankings_heatmap_2023_onwards.png` - All operators heatmap (all data)
- `operator_rankings_heatmap_2023_onwards_no_outliers.png` - Heatmap (no outliers)

## Configuration

Edit the `CONFIG` dictionary in `scripts/lido_perf_analysis.py`:

```python
CONFIG = {
    # Date range for analysis
    'START_DATE': '2023-01-01',
    'END_DATE': '2025-08-31',

    # Minimum days of data required per month
    'MIN_DAYS_PER_MONTH': 20,

    # Outlier detection threshold (standard deviations)
    'OUTLIER_STD_THRESHOLD': 2,

    # API rate limiting
    'MAX_CONCURRENT_REQUESTS': 10,
    'BATCH_SIZE': 81,
    'MAX_RETRIES': 10,
}
```

## Analysis Methodology

### Balance Calculation

The script uses two endpoints for maximum accuracy:

1. **`/eth/rewards`**: Provides daily rewards data
2. **`/eth/stakes`**: Provides validator activation (`delegated_at`) and exit (`exited_at`) dates

By tracking activations and exits, the script calculates the **true active balance** at any point in time, excluding validators that have exited but not yet withdrawn.

### APR Calculation

```
Monthly APR = (Total Monthly Rewards / Average Daily Balance) × (365 / Days in Month)
```

### Benchmarks

Kiln's performance is compared against:
- **Network Mean**: Stake-weighted average APR (excluding Kiln)
- **Network Median**: Median APR across all operators
- **Top 10 Mean**: Average APR of top 10 performing operators
- **Top 10 Median**: Median APR of top 10 performing operators

### Outlier Removal

The no-outlier analysis removes daily rewards that are more than 2 standard deviations above the mean for each operator per month. This provides a cleaner comparison by removing anomalous high-reward days.

## Metrics Explained

- **APR**: Annualized percentage return based on monthly rewards
- **Rank**: Kiln's ranking among all operators (1 = best)
- **Percentile**: Kiln's percentile ranking (100 = top 1%)
- **Z-Score**: Standard deviations above/below mean (>1 = good, >2 = excellent)
- **Diff vs Benchmark (bps)**: APR difference in basis points (100 bps = 1%)
- **Extra Rewards**: Additional ETH earned vs benchmark APR

## Troubleshooting

### Data Fetching Issues

If data fetching is interrupted:
1. The checkpoint file tracks progress
2. Re-run the script - it will resume from the last completed operator
3. Check `output/lido_processing.log` for detailed error messages

### API Rate Limiting

If you hit rate limits:
1. Reduce `MAX_CONCURRENT_REQUESTS` in CONFIG
2. Increase retry delays by adjusting the sleep time in `fetch_api_async()`

### Missing Data

If certain months are missing:
- Ensure the operator had at least `MIN_DAYS_PER_MONTH` days of data
- Check the date range in CONFIG matches your needs

## License

MIT License - See LICENSE file for details

## Support

For issues or questions, please open an issue on GitHub.
