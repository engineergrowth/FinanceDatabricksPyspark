# Financial Data Pipeline with PySpark and Databricks

This project demonstrates a financial data pipeline built with PySpark and Databricks. The pipeline is designed to fetch, process, and transform historical stock data for multiple tickers, storing the results in Delta tables for further analysis.

## Overview

The pipeline fetches 5 years of stock data from a public API for multiple tickers (e.g., AAPL, MSFT, TSLA, etc.). The data is then processed using PySpark in Databricks, where it undergoes a series of transformations to make it suitable for analysis.

### Key Features:
- **Daily Metrics**: Calculate daily returns based on open and close prices.
- **Rolling Metrics**: Compute 30-day and 90-day rolling averages and volatility.
- **Cumulative Metrics**: Calculate cumulative returns over the entire dataset for each ticker.
- **Yearly Aggregations**: Aggregate average close prices and total traded volume by year.
- **Efficient Storage**: Save both raw and transformed datasets in Delta tables.

## Data Workflow

1. **Extract**: Fetch historical stock data from the API.
2. **Transform**:
   - Clean and prepare the data.
   - Perform calculations and generate new metrics.
   - Aggregate data by year for high-level analysis.
3. **Load**: Save processed datasets into Delta tables in staging and analytics schemas for further exploration.
  
## How to Run

1. Clone this repository and set up a Databricks workspace.
2. Configure API access by providing your API key in the relevant notebook cell.
3. Run the pipeline notebook in Databricks to fetch, process, and store the data.
4. Use the saved Delta tables for analysis and visualization.



