"""
回测模块 - 用于策略回测和性能评估
"""

from backtest.backtest_engine import BacktestEngine
from backtest.performance_metrics import calculate_metrics
from backtest.data_provider import HistoricalDataProvider
from backtest.visualization import create_performance_charts, create_trades_visualization, plot_equity_curve

__all__ = [
    'BacktestEngine',
    'calculate_metrics',
    'HistoricalDataProvider',
    'create_performance_charts',
    'create_trades_visualization',
    'plot_equity_curve'
]
