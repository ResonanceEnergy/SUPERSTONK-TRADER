# ARCHITECTURE — SUPERSTONK TRADER

## Purpose
Autonomous/semi-autonomous trading platform. Market intelligence, strategy execution, and portfolio management.

## System Overview
```
[Market Data]   --> [Strategy Engine] --> [Signal Generator]
[Risk Manager]  --> [Execution Layer] --> [Portfolio Tracker]
```

## Components
- **Market Data**: Real-time quotes, options chain, sentiment feeds
- **Strategy Engine**: Rule-based + ML trading strategies
- **Risk Manager**: Position sizing, drawdown limits, circuit breakers
- **Execution Layer**: Broker API integration (TD/Schwab/IBKR)
- **Portfolio Tracker**: P&L, exposure, performance attribution

## Data Flow
Market Data → Strategy → Signal → Risk Check → Execute → Track → Report

## Integration Points
- VORTEX-HUNTER (opportunity feed)
- TESLACALLS2026 (options sub-strategy)
- TESLA-TECH (fundamental research)

## Key Decisions
- Risk manager is non-bypassable — hard limits enforced
- Paper trading mode always available for strategy testing
