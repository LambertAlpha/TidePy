"""
回测结果可视化 - 生成各种图表展示回测结果
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio
import logging

from utils.logger import setup_logger

logger = setup_logger('backtest')

def create_performance_charts(portfolio_values, trades, metrics, output_path):
    """
    创建回测性能图表并保存为HTML报告
    
    Args:
        portfolio_values (DataFrame): 每日资产组合价值数据
        trades (DataFrame): 交易记录数据
        metrics (dict): 计算得到的性能指标
        output_path (str): 输出报告路径
    """
    logger.info("开始生成回测性能图表...")
    
    # 创建一个Plotly Dashboard
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            '投资组合价值', '每日收益率', 
            '回撤分析', '月度收益率热图',
            '交易分析', '资产配置'
        ),
        specs=[
            [{"colspan": 2}, None],
            [{"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"}, {"type": "domain"}]
        ],
        vertical_spacing=0.1
    )
    
    # 格式化日期
    portfolio_values['date'] = pd.to_datetime(portfolio_values['date'])
    
    # 1. 投资组合价值曲线
    fig.add_trace(
        go.Scatter(
            x=portfolio_values['date'],
            y=portfolio_values['value'],
            mode='lines',
            name='投资组合价值',
            line=dict(color='royalblue', width=2)
        ),
        row=1, col=1
    )
    
    # 添加起始资金线
    fig.add_trace(
        go.Scatter(
            x=[portfolio_values['date'].min(), portfolio_values['date'].max()],
            y=[metrics['final_portfolio_value'] / (1 + metrics['total_return'])] * 2,
            mode='lines',
            name='初始资金',
            line=dict(color='gray', width=1, dash='dash')
        ),
        row=1, col=1
    )
    
    # 2. 每日收益率
    # 计算每日收益率
    daily_returns = portfolio_values['value'].pct_change().fillna(0)
    
    fig.add_trace(
        go.Bar(
            x=portfolio_values['date'],
            y=daily_returns,
            name='每日收益率',
            marker=dict(
                color=np.where(daily_returns >= 0, 'green', 'red'),
                line=dict(color='black', width=1)
            )
        ),
        row=2, col=1
    )
    
    # 3. 回撤分析
    # 计算回撤
    portfolio_values['roll_max'] = portfolio_values['value'].cummax()
    portfolio_values['drawdown'] = portfolio_values['value'] / portfolio_values['roll_max'] - 1
    
    fig.add_trace(
        go.Scatter(
            x=portfolio_values['date'],
            y=portfolio_values['drawdown'] * 100,  # 转为百分比
            mode='lines',
            name='回撤(%)',
            line=dict(color='red', width=2)
        ),
        row=2, col=2
    )
    
    # 添加最大回撤标记
    max_dd_date = portfolio_values.loc[portfolio_values['drawdown'].idxmin(), 'date']
    max_dd = portfolio_values['drawdown'].min() * 100
    
    fig.add_trace(
        go.Scatter(
            x=[max_dd_date],
            y=[max_dd],
            mode='markers+text',
            marker=dict(symbol='circle', size=10, color='darkred'),
            text=[f"{max_dd:.2f}%"],
            textposition="bottom center",
            name='最大回撤'
        ),
        row=2, col=2
    )
    
    # 4. 月度收益率热图
    if 'date' in portfolio_values.columns:
        monthly_returns = portfolio_values.set_index('date')
        monthly_returns = monthly_returns['value'].resample('M').last().pct_change().dropna()
        monthly_returns = pd.DataFrame(monthly_returns)
        monthly_returns['year'] = monthly_returns.index.year
        monthly_returns['month'] = monthly_returns.index.month_name()
        
        # 透视表
        if not monthly_returns.empty and len(monthly_returns) > 1:
            try:
                pivot = monthly_returns.pivot_table(
                    index='year', 
                    columns='month', 
                    values='value',
                    aggfunc='sum'
                )
                
                month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                            'July', 'August', 'September', 'October', 'November', 'December']
                pivot = pivot[pivot.columns.intersection(month_order)]
                
                # 创建热图数据
                z_data = pivot.values
                
                fig.add_trace(
                    go.Heatmap(
                        z=z_data,
                        x=pivot.columns,
                        y=pivot.index,
                        colorscale='RdYlGn',
                        text=np.round(z_data * 100, 2),
                        texttemplate='%{text}%',
                        name='月度收益率'
                    ),
                    row=3, col=1
                )
            except Exception as e:
                logger.warning(f"创建月度收益率热图失败: {str(e)}")
                # 添加一个空白图
                fig.add_trace(
                    go.Scatter(
                        x=[],
                        y=[],
                        mode='text',
                        text=['数据不足，无法生成月度收益率热图'],
                        name='月度收益率'
                    ),
                    row=3, col=1
                )
        else:
            # 添加一个空白图
            fig.add_trace(
                go.Scatter(
                    x=[],
                    y=[],
                    mode='text',
                    text=['数据不足，无法生成月度收益率热图'],
                    name='月度收益率'
                ),
                row=3, col=1
            )
    
    # 5. 资产配置饼图
    if 'positions_value' in portfolio_values.columns and 'cash' in portfolio_values.columns:
        # 使用最后一个时间点的数据
        last_day = portfolio_values.iloc[-1]
        positions_value = last_day['positions_value']
        cash_value = last_day['cash']
        total_value = last_day['value']
        
        labels = ['持仓', '现金']
        values = [positions_value, cash_value]
        
        fig.add_trace(
            go.Pie(
                labels=labels,
                values=values,
                name='资产配置',
                marker=dict(colors=['royalblue', 'lightgray']),
                textinfo='percent+label'
            ),
            row=3, col=2
        )
    
    # 添加性能指标表
    # 格式化指标数据
    metrics_formatted = {
        '总收益率': f"{metrics['total_return'] * 100:.2f}%",
        '年化收益率': f"{metrics['annualized_return'] * 100:.2f}%",
        '夏普比率': f"{metrics['sharpe_ratio']:.2f}",
        '索提诺比率': f"{metrics['sortino_ratio']:.2f}" if pd.notna(metrics['sortino_ratio']) else 'N/A',
        '卡玛比率': f"{metrics['calmar_ratio']:.2f}" if pd.notna(metrics['calmar_ratio']) else 'N/A',
        '最大回撤': f"{metrics['max_drawdown'] * 100:.2f}%",
        '年化波动率': f"{metrics['volatility'] * 100:.2f}%",
        '胜率': f"{metrics['win_rate'] * 100:.2f}%",
        '盈亏比': f"{metrics['profit_factor']:.2f}" if pd.notna(metrics['profit_factor']) else 'N/A',
        '交易天数': f"{metrics['trading_days']}",
        '最终资产': f"{metrics['final_portfolio_value']:.2f}"
    }
    
    # 创建表格数据
    table_data = [
        list(metrics_formatted.keys()),
        list(metrics_formatted.values())
    ]
    
    # 添加表格
    fig.add_trace(
        go.Table(
            header=dict(
                values=['指标', '数值'],
                fill_color='paleturquoise',
                align='center',
                font=dict(size=14)
            ),
            cells=dict(
                values=table_data,
                fill_color='lavender',
                align='center',
                font=dict(size=12)
            )
        ),
        row=1, col=1
    )
    
    # 更新布局
    fig.update_layout(
        title_text=f'策略回测报告 ({portfolio_values["date"].min().strftime("%Y-%m-%d")} 至 {portfolio_values["date"].max().strftime("%Y-%m-%d")})',
        height=1200,
        showlegend=True
    )
    
    # 保存为HTML
    pio.write_html(fig, file=output_path, auto_open=False)
    
    logger.info(f"回测性能图表已保存至: {output_path}")
    
    return output_path

def create_trades_visualization(trades, output_path=None):
    """
    创建交易可视化图表
    
    Args:
        trades (DataFrame): 交易记录数据
        output_path (str, optional): 输出文件路径
        
    Returns:
        str: 输出文件路径
    """
    if output_path is None:
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'./trades_visualization_{now}.html'
    
    # 如果trades是一个列表，转换为DataFrame
    if isinstance(trades, list):
        trades = pd.DataFrame(trades)
    
    # 确保timestamp是日期时间类型
    trades['timestamp'] = pd.to_datetime(trades['timestamp'])
    
    # 创建图表
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            '交易分布', '交易量分析', 
            '交易盈亏分布', '累计盈亏'
        ),
        specs=[
            [{"type": "xy"}, {"type": "domain"}],
            [{"type": "xy"}, {"type": "xy"}]
        ],
        vertical_spacing=0.1
    )
    
    # 1. 交易分布图
    # 按日期统计交易次数
    trade_counts = trades.groupby(trades['timestamp'].dt.date).size()
    
    fig.add_trace(
        go.Bar(
            x=trade_counts.index,
            y=trade_counts.values,
            name='每日交易次数'
        ),
        row=1, col=1
    )
    
    # 2. 交易类型占比
    type_counts = trades['type'].value_counts()
    
    fig.add_trace(
        go.Pie(
            labels=type_counts.index,
            values=type_counts.values,
            name='交易类型占比',
            textinfo='percent+label'
        ),
        row=1, col=2
    )
    
    # 3. 交易盈亏分布
    if 'profit' in trades.columns:
        fig.add_trace(
            go.Histogram(
                x=trades['profit'],
                name='盈亏分布',
                marker=dict(color='rgba(0, 123, 255, 0.7)')
            ),
            row=2, col=1
        )
        
        # 4. 累计盈亏
        cumulative_profit = trades.sort_values('timestamp')['profit'].cumsum()
        
        fig.add_trace(
            go.Scatter(
                x=trades['timestamp'],
                y=cumulative_profit,
                mode='lines',
                name='累计盈亏',
                line=dict(color='green', width=2)
            ),
            row=2, col=2
        )
    else:
        # 如果没有盈亏数据，显示空白图
        fig.add_trace(
            go.Scatter(
                x=[],
                y=[],
                mode='text',
                text=['交易记录中没有盈亏数据'],
                name='盈亏分布'
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=[],
                y=[],
                mode='text',
                text=['交易记录中没有盈亏数据'],
                name='累计盈亏'
            ),
            row=2, col=2
        )
    
    # 更新布局
    fig.update_layout(
        title_text='交易分析报告',
        height=800,
        showlegend=True
    )
    
    # 保存为HTML
    pio.write_html(fig, file=output_path, auto_open=False)
    
    logger.info(f"交易可视化图表已保存至: {output_path}")
    
    return output_path

def plot_equity_curve(portfolio_values, benchmark=None, output_path=None):
    """
    绘制资金曲线图
    
    Args:
        portfolio_values (DataFrame): 包含日期和投资组合价值的DataFrame
        benchmark (DataFrame, optional): 包含日期和基准价值的DataFrame
        output_path (str, optional): 输出文件路径
        
    Returns:
        str: 输出文件路径
    """
    if output_path is None:
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'./equity_curve_{now}.png'
    
    # 创建图表
    plt.figure(figsize=(12, 6))
    
    # 确保数据格式正确
    if 'date' in portfolio_values.columns:
        x = portfolio_values['date']
    else:
        x = portfolio_values.index
    
    if 'value' in portfolio_values.columns:
        y = portfolio_values['value']
    else:
        # 假设第一列是价值
        y = portfolio_values.iloc[:, 0]
    
    # 绘制资金曲线
    plt.plot(x, y, label='策略', linewidth=2)
    
    # 如果有基准数据，也绘制
    if benchmark is not None:
        if 'date' in benchmark.columns:
            bm_x = benchmark['date']
        else:
            bm_x = benchmark.index
        
        if 'value' in benchmark.columns:
            bm_y = benchmark['value']
        else:
            # 假设第一列是价值
            bm_y = benchmark.iloc[:, 0]
        
        plt.plot(bm_x, bm_y, label='基准', linewidth=2, linestyle='--')
    
    # 设置图表格式
    plt.title('策略资金曲线')
    plt.xlabel('日期')
    plt.ylabel('资金')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    # 设置x轴日期格式
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gcf().autofmt_xdate()
    
    # 保存图表
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    
    logger.info(f"资金曲线图已保存至: {output_path}")
    
    return output_path
