# Cryptocurrency Arbitrage Project

## Overview

This project aims to explore and exploit arbitrage opportunities across various cryptocurrency exchanges. It encompasses two pivotal phases: the continuous collection of order book data and the backtesting of arbitrage trading strategies. The insights gained from these activities are intended to assess the potential profitability and operational feasibility of arbitrage trading in the dynamic cryptocurrency market.

---

## Order Book Data Collection for Arbitrage Trading

### Initial Setup

#### Account Verification and Preparation
Before commencing data collection, it was necessary to establish and verify trading accounts on the targeted exchanges. Verified accounts benefit from higher daily withdrawal limits, essential for executing significant arbitrage trades efficiently.

#### Selection of Trading Symbols
A script, `get_trading_symbols.py`, was developed to:
- Identify common trading pairs across at least two exchanges for inter-exchange arbitrage.
- Exclude fiat pairings to streamline the withdrawal process.
- Initialize market data using the `loadMarkets()` method from ccxt, preparing the system for data retrieval.

#### Distribution of Workload Across Servers
The `initialize_exchanges.py` script facilitates efficient data collection by distributing trading pairs across servers. This approach minimizes data gaps and optimizes retrieval speed by considering the geographical proximity of servers to exchanges.

### Data Collection and Management

#### Multithreaded Data Collection
The `orderbook.py` program collects live order book data using multithreading for concurrent API calls, storing the data in HDF5 files optimized for space efficiency.

#### Program Execution
The data collection program operates across various servers, each configured with a unique `server_number` to ensure comprehensive coverage of the market.

#### Maintenance and Updates
Routine restarts are incorporated to ensure the CCXT library remains updated, preserving the integrity of the data collection process.

---

## Backtesting Framework for Cryptocurrency Trading

### Features

#### Profitability Calculator
The core of the framework, it processes trade data from two exchanges to calculate potential profits, accounting for transactional nuances like fees, precision, and limits.

#### Time Alignment in Data
Ensures synchronization of trade data by aligning timestamps and adjusting data pointers based on relative time steps between exchanges.

#### Handling Multiple CSV Files
Processes trade data organized by cryptocurrency symbols, employing multithreading to enhance efficiency across multiple exchange combinations.

### Consideration of Withdrawal Fees
Accurately accounts for both fixed and percentage-based withdrawal fees, ensuring a realistic representation of trading costs.

### Implementation Notes
- **Multithreading**: Utilized for processing efficiency.
- **Future Enhancements**: Plans include refining the handling of percentage-based withdrawal fees for more accurate cost analysis.

---

## Note
The detailed artifacts and scripts mentioned in both components of the project form the foundation of this exploratory study into cryptocurrency arbitrage trading. The methodologies and tools developed are crucial for understanding the nuances of arbitrage opportunities in the cryptocurrency domain.
