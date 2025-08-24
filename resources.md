# **Resources**

A comprehensive list of resources used in this project.

## Polygon

The primary source for stock & option data

### Options

* [All Contracts | Options REST API - Polygon](https://polygon.io/docs/rest/options/contracts/all-contracts)  - Used to generate a distinct list of contracts with expiration dates in a specified range. Data use: Lookup/resource.
* [Daily Ticker Summary (OHLC) | Options REST API - Polygon](https://polygon.io/docs/rest/options/aggregates/daily-ticker-summary) - The daily ticker summary for a given options contract. Used to populate daily aggregates on options contracts. Data use: Populate historical.
* [Option Chain Snapshot | Options REST API - Polygon](https://polygon.io/docs/rest/options/snapshots/option-chain-snapshot) - Used to get a comprehensive snapshot for all options on a given ticker, including greeks, pricing details, etc. Data use: Intraday / capturing of the current snapshot.
* [Flat Files Quickstart | Polygon](https://polygon.io/docs/flat-files/quickstart) - Flat files for daily snapshot

### Stocks

* [All Tickers | Stocks REST API - Polygon](https://polygon.io/docs/rest/stocks/tickers/all-tickers) - Used to generate a distinct list of tickers. Data use: Lookup/resource.
* [Daily Market Summary (OHLC) | Stocks REST API - Polygon](https://polygon.io/docs/rest/stocks/aggregates/daily-market-summary) - The daily ticker summary for all US stocks. Used to poopulate daily snapshots on stock prices.  Data use: Populate historical.
* [Full Market Snapshot | Stocks REST API - Polygon](https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot) - The full market snapshot - gets the full US stock market in a single response. Data use: Intraday / capturing of the current snapshot.

### Resources

* [Market Holidays | Options REST API - Polygon](https://polygon.io/docs/rest/options/market-operations/market-holidays) - List of market holidays with corresponding open/close times. Forward looking only. Data use: Lookup/resource.
* [Market Status | Options REST API - Polygon](https://polygon.io/docs/rest/options/market-operations/market-status) - Market status. Used to check if certain exchanges are open/closed. Data use: Lookup/resource
