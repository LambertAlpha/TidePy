"""
性能指标计算 - 计算回测结果的各种性能指标
"""

import numpy as np
import pandas as pd
from datetime import datetime
import logging

from utils.logger import setup_logger

logger = setup_logger('backtest')

def calculate_metrics(portfolio_values, initial_capital, trades):
    """
    计算回测性能指标
    
    Args:
        portfolio_values (DataFrame): 每日资产组合价值
        initial_capital (float): 初始资金
        trades (list): 交易记录列表
        
    Returns:
        dict: 性能指标
    """
    # 确保数据按日期排序
    portfolio_values = portfolio_values.sort_index()
    
    # 提取资产组合价值数据
    values = portfolio_values['portfolio_value']
    
    # 计算每日收益率
    portfolio_values['daily_return'] = values.pct_change()
    
    # 计算累计收益率
    total_return = (values.iloc[-1] / initial_capital) - 1
    
    # 计算年化收益率
    days = (portfolio_values.index[-1] - portfolio_values.index[0]).days
    if days > 0:
        annualized_return = (1 + total_return) ** (365 / days) - 1
    else:
        annualized_return = 0
    
    # 计算每月收益率
    monthly_returns = values.resample('M').last().pct_change().dropna()
    
    # 计算股夏普比率 (假设无风险利率为0.02)
    risk_free_rate = 0.02
    daily_excess_returns = portfolio_values['daily_return'] - risk_free_rate / 365
    sharpe_ratio = np.sqrt(365) * (daily_excess_returns.mean() / daily_excess_returns.std())
    
    # 计算最大回撤
    max_drawdown, drawdown_start, drawdown_end = calculate_max_drawdown(values)
    
    # 计算卡玛比率 (年化收益率除以最大回撤)
    calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else np.nan
    
    # 计算索提诺比率
    downside_returns = portfolio_values['daily_return'][portfolio_values['daily_return'] < 0]
    sortino_ratio = np.sqrt(365) * (portfolio_values['daily_return'].mean() / downside_returns.std()) if len(downside_returns) > 0 else np.nan
    
    # 计算波动率
    volatility = portfolio_values['daily_return'].std() * np.sqrt(365)
    
    # 计算盈利因子
    win_rate, profit_factor, avg_profit, avg_loss = calculate_trade_metrics(trades)
    
    # 综合指标
    metrics = {
        'total_return': total_return,
        'annualized_return': annualized_return,
        'sharpe_ratio': sharpe_ratio,
        'sortino_ratio': sortino_ratio,
        'calmar_ratio': calmar_ratio,
        'max_drawdown': max_drawdown,
        'drawdown_start_date': drawdown_start,
        'drawdown_end_date': drawdown_end,
        'volatility': volatility,
        'win_rate': win_rate,
        'profit_factor': profit_factor,
        'avg_profit': avg_profit,
        'avg_loss': avg_loss,
        'trading_days': days,
        'final_portfolio_value': values.iloc[-1],
        'monthly_returns': monthly_returns.to_dict()
    }
    
    return metrics

def calculate_max_drawdown(series):
    """
    计算最大回撤
    
    Args:
        series (Series): 资产价值序列
        
    Returns:
        tuple: (最大回撤值, 回撤开始日期, 回撤结束日期)
    """
    # 计算最大回撤和回撤区间
    roll_max = series.cummax()
    drawdown = (series / roll_max - 1)
    
    # 最大回撤
    max_drawdown = drawdown.min()
    
    # 回撤开始和结束日期
    end_idx = drawdown.idxmin()
    
    # 找到开始日期（回撤开始的点是在最大值点）
    start_idx = series[:end_idx].idxmax()
    
    return max_drawdown, start_idx, end_idx

def calculate_trade_metrics(trades):
    """
    计算交易相关指标
    
    Args:
        trades (list): 交易记录列表
        
    Returns:
        tuple: (胜率, 盈利因子, 平均盈利, 平均亏损)
    """
    # 如果没有交易记录，返回默认值
    if not trades:
        return 0, 0, 0, 0
    
    # 将交易记录转换为DataFrame
    trades_df = pd.DataFrame(trades) if not isinstance(trades, pd.DataFrame) else trades
    
    # 计算交易盈亏
    total_trades = len(trades_df)
    
    # 标记交易类型
    # 假设买入(buy/long)是开仓，卖出(sell/short)是平仓
    # 这个逻辑需要根据实际交易策略调整
    
    # 初始化利润列
    profit_dict = {}
    
    # 按交易对和时间排序
    trades_df = trades_df.sort_values(['symbol', 'timestamp'])
    
    # 按照交易对分组处理
    for symbol, group in trades_df.groupby('symbol'):
        # 初始化变量
        position = 0
        direction = None
        avg_price = 0
        
        for idx, trade in group.iterrows():
            trade_type = trade['type']
            price = trade['price']
            quantity = trade['quantity']
            
            if trade_type in ['buy', 'long']:
                if position == 0:
                    # 开仓
                    position = quantity
                    avg_price = price
                    direction = 'long'
                elif direction == 'long':
                    # 加仓
                    avg_price = (avg_price * position + price * quantity) / (position + quantity)
                    position += quantity
                elif direction == 'short':
                    # 减仓或平仓
                    if quantity >= position:
                        # 平仓并开多
                        profit = position * (avg_price - price)  # 空头平仓盈亏
                        profit_dict[idx] = profit
                        
                        # 剩余部分开多
                        new_qty = quantity - position
                        if new_qty > 0:
                            position = new_qty
                            avg_price = price
                            direction = 'long'
                        else:
                            position = 0
                    else:
                        # 部分减仓
                        profit = quantity * (avg_price - price)  # 空头部分平仓盈亏
                        profit_dict[idx] = profit
                        position -= quantity
            
            elif trade_type in ['sell', 'short']:
                if position == 0:
                    # 开空
                    position = quantity
                    avg_price = price
                    direction = 'short'
                elif direction == 'short':
                    # 加空
                    avg_price = (avg_price * position + price * quantity) / (position + quantity)
                    position += quantity
                elif direction == 'long':
                    # 减仓或平仓
                    if quantity >= position:
                        # 平仓并开空
                        profit = position * (price - avg_price)  # 多头平仓盈亏
                        profit_dict[idx] = profit
                        
                        # 剩余部分开空
                        new_qty = quantity - position
                        if new_qty > 0:
                            position = new_qty
                            avg_price = price
                            direction = 'short'
                        else:
                            position = 0
                    else:
                        # 部分减仓
                        profit = quantity * (price - avg_price)  # 多头部分平仓盈亏
                        profit_dict[idx] = profit
                        position -= quantity
    
    # 将利润值添加到DataFrame中
    trades_df['profit'] = trades_df.index.map(lambda x: profit_dict.get(x, 0))
    
    # 计算盈利交易和亏损交易
    profitable_trades = trades_df[trades_df['profit'] > 0]
    losing_trades = trades_df[trades_df['profit'] < 0]
    
    win_count = len(profitable_trades)
    loss_count = len(losing_trades)
    
    # 计算胜率
    if total_trades > 0:
        win_rate = win_count / total_trades
    else:
        win_rate = 0
    
    # 计算盈利因子 (总盈利 / 总亏损)
    total_profit = profitable_trades['profit'].sum() if len(profitable_trades) > 0 else 0
    total_loss = abs(losing_trades['profit'].sum()) if len(losing_trades) > 0 else 0
    
    if total_loss > 0:
        profit_factor = total_profit / total_loss
    else:
        profit_factor = float('inf') if total_profit > 0 else 0
    
    # 计算平均盈利和平均亏损
    avg_profit = total_profit / win_count if win_count > 0 else 0
    avg_loss = total_loss / loss_count if loss_count > 0 else 0
    
    return win_rate, profit_factor, avg_profit, avg_loss
