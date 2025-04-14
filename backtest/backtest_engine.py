"""
回测引擎 - 负责执行策略回测并生成结果
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
from tqdm import tqdm
import matplotlib.pyplot as plt

from backtest.data_provider import HistoricalDataProvider
from backtest.performance_metrics import calculate_metrics
from strategy.factor_analyzer import FactorAnalyzer
from strategy.signal_generator import SignalGenerator
from risk_manager.risk_manager import RiskManager
from utils.logger import setup_logger

logger = setup_logger('backtest')

class BacktestEngine:
    """回测引擎，模拟策略在历史数据上的表现"""
    
    def __init__(self, start_date, end_date, initial_capital, symbols=None):
        """
        初始化回测引擎
        
        Args:
            start_date (str): 回测开始日期，格式 'YYYY-MM-DD'
            end_date (str): 回测结束日期，格式 'YYYY-MM-DD'
            initial_capital (float): 初始资金
            symbols (list, optional): 要回测的交易对列表
        """
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.symbols = symbols
        
        # 初始化组件
        self.data_provider = HistoricalDataProvider()
        self.factor_analyzer = FactorAnalyzer()
        self.signal_generator = SignalGenerator()
        self.risk_manager = RiskManager()
        
        # 回测数据
        self.positions = {}  # 当前持仓 {symbol: {'direction': 'long'/'short', 'size': size, 'entry_price': price}}
        self.trades = []  # 交易记录
        self.portfolio_values = []  # 组合价值变化
        self.cash = initial_capital  # 可用现金
        
        logger.info(f"初始化回测引擎: {start_date} 至 {end_date}, 初始资金: {initial_capital}")
    
    def prepare_data(self):
        """准备回测所需的历史数据"""
        logger.info("开始准备历史数据...")
        
        # 获取历史市场数据
        self.market_data = self.data_provider.get_market_data(
            self.start_date, self.end_date, self.symbols
        )
        
        # 获取历史资金费率数据
        self.funding_data = self.data_provider.get_funding_data(
            self.start_date, self.end_date, self.symbols
        )
        
        # 获取代币信息
        self.token_info = self.data_provider.get_token_info(self.symbols)
        
        logger.info("历史数据准备完成")
    
    def run(self):
        """运行回测"""
        logger.info("开始运行回测...")
        
        # 准备历史数据
        self.prepare_data()
        
        # 获取回测日期列表
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
        
        # 初始化回测结果数据
        self.portfolio_values = []
        self.trades = []
        
        # 记录初始资产价值
        self.portfolio_values.append({
            'date': self.start_date,
            'value': self.initial_capital,
            'cash': self.initial_capital,
            'positions_value': 0
        })
        
        # 遍历每一个交易日
        for current_date in tqdm(date_range, desc="回测进度"):
            # 获取当天的数据
            current_data = self._get_current_data(current_date)
            
            if current_data['market_data'].empty:
                logger.debug(f"日期 {current_date.strftime('%Y-%m-%d')} 没有市场数据，跳过")
                continue
                
            # 计算因子
            factor_scores = self.factor_analyzer.calculate_factors(
                current_data['market_data'], 
                current_data['funding_data'], 
                current_data['token_info']
            )
            
            # 生成信号
            signals = self.signal_generator.generate_signals(factor_scores)
            
            # 风险评估
            filtered_signals = self.risk_manager.filter_signals(signals)
            
            # 更新现有持仓的市场价值
            self._update_positions_value(current_data['market_data'])
            
            # 执行交易信号
            self._execute_signals(filtered_signals, current_date)
            
            # 计算当日资产组合价值
            portfolio_value = self._calculate_portfolio_value(current_data['market_data'])
            
            # 记录每日资产组合价值
            self.portfolio_values.append({
                'date': current_date,
                'value': portfolio_value,
                'cash': self.cash,
                'positions_value': portfolio_value - self.cash
            })
            
            # 监控和调整现有持仓
            self._monitor_and_adjust_positions(current_data, current_date)
        
        # 计算回测性能指标
        self.metrics = self._calculate_metrics()
        
        logger.info("回测完成")
        return self.metrics
    
    def _get_current_data(self, current_date):
        """获取指定日期的数据"""
        # 过滤特定日期的市场数据
        market_data = self.market_data[self.market_data['timestamp'].dt.date == current_date.date()]
        
        # 过滤特定日期的资金费率数据
        funding_data = self.funding_data[self.funding_data['timestamp'].dt.date == current_date.date()]
        
        return {
            'market_data': market_data,
            'funding_data': funding_data,
            'token_info': self.token_info
        }
    
    def _execute_signals(self, signals, current_date):
        """执行交易信号"""
        for _, signal in signals.iterrows():
            symbol = signal['symbol']
            signal_type = signal['signal_type']
            quantity = signal['quantity']
            price = signal['price']
            
            # 记录交易
            trade = {
                'timestamp': current_date,
                'symbol': symbol,
                'type': signal_type,
                'quantity': quantity,
                'price': price,
                'value': quantity * price,
                'reason': signal.get('reason', 'signal')
            }
            
            # 更新持仓
            if signal_type in ['buy', 'long']:
                # 做多或买入
                cost = quantity * price
                if cost > self.cash:
                    logger.warning(f"资金不足，无法执行买入信号: {symbol}, 需要 {cost}, 可用 {self.cash}")
                    continue
                
                self.cash -= cost
                
                if symbol in self.positions:
                    # 已有持仓，更新
                    pos = self.positions[symbol]
                    if pos['direction'] == 'long':
                        # 增加多头持仓
                        new_size = pos['size'] + quantity
                        new_entry_price = (pos['entry_price'] * pos['size'] + price * quantity) / new_size
                        pos['size'] = new_size
                        pos['entry_price'] = new_entry_price
                    else:
                        # 减少空头持仓
                        if quantity > pos['size']:
                            # 平仓后还有剩余，转为多头
                            remain = quantity - pos['size']
                            self.positions[symbol] = {
                                'direction': 'long',
                                'size': remain,
                                'entry_price': price
                            }
                        else:
                            # 部分平仓
                            pos['size'] -= quantity
                            if pos['size'] == 0:
                                del self.positions[symbol]
                else:
                    # 新建多头持仓
                    self.positions[symbol] = {
                        'direction': 'long',
                        'size': quantity,
                        'entry_price': price
                    }
                
            elif signal_type in ['sell', 'short']:
                # 做空或卖出
                if symbol in self.positions:
                    pos = self.positions[symbol]
                    if pos['direction'] == 'long':
                        # 减少多头持仓
                        if quantity > pos['size']:
                            # 平仓后还有剩余，转为空头
                            remain = quantity - pos['size']
                            proceeds = pos['size'] * price  # 卖出获得的资金
                            self.cash += proceeds
                            
                            cost = remain * price * 0.01  # 做空保证金，假设为1%
                            if cost > self.cash:
                                logger.warning(f"资金不足，无法执行额外的做空: {symbol}, 需要 {cost}, 可用 {self.cash}")
                                self.positions[symbol] = {
                                    'direction': 'short',
                                    'size': 0,
                                    'entry_price': price
                                }
                                continue
                            
                            self.cash -= cost
                            self.positions[symbol] = {
                                'direction': 'short',
                                'size': remain,
                                'entry_price': price
                            }
                        else:
                            # 部分平仓
                            proceeds = quantity * price
                            self.cash += proceeds
                            pos['size'] -= quantity
                            if pos['size'] == 0:
                                del self.positions[symbol]
                    else:
                        # 增加空头持仓
                        cost = quantity * price * 0.01  # 做空保证金，假设为1%
                        if cost > self.cash:
                            logger.warning(f"资金不足，无法执行做空信号: {symbol}, 需要 {cost}, 可用 {self.cash}")
                            continue
                        
                        self.cash -= cost
                        new_size = pos['size'] + quantity
                        new_entry_price = (pos['entry_price'] * pos['size'] + price * quantity) / new_size
                        pos['size'] = new_size
                        pos['entry_price'] = new_entry_price
                else:
                    # 新建空头持仓
                    cost = quantity * price * 0.01  # 做空保证金，假设为1%
                    if cost > self.cash:
                        logger.warning(f"资金不足，无法执行做空信号: {symbol}, 需要 {cost}, 可用 {self.cash}")
                        continue
                    
                    self.cash -= cost
                    self.positions[symbol] = {
                        'direction': 'short',
                        'size': quantity,
                        'entry_price': price
                    }
            
            # 记录交易
            self.trades.append(trade)
            logger.debug(f"执行交易: {trade}")
    
    def _update_positions_value(self, market_data):
        """更新持仓的市场价值"""
        for symbol, pos in list(self.positions.items()):
            # 获取最新价格
            symbol_data = market_data[market_data['symbol'] == symbol]
            if symbol_data.empty:
                logger.warning(f"无法找到 {symbol} 的市场数据，使用上一次价格")
                continue
            
            latest_price = symbol_data.iloc[-1]['close']
            
            # 更新持仓市场价值
            pos['current_price'] = latest_price
            
            if pos['direction'] == 'long':
                pos['market_value'] = pos['size'] * latest_price
                pos['unrealized_pnl'] = pos['size'] * (latest_price - pos['entry_price'])
            else:  # short
                pos['market_value'] = pos['size'] * latest_price * 0.01  # 保证金价值
                pos['unrealized_pnl'] = pos['size'] * (pos['entry_price'] - latest_price)
    
    def _calculate_portfolio_value(self, market_data):
        """计算当前资产组合的总价值"""
        positions_value = 0
        
        for symbol, pos in self.positions.items():
            # 获取最新价格
            symbol_data = market_data[market_data['symbol'] == symbol]
            if symbol_data.empty:
                logger.warning(f"无法找到 {symbol} 的市场数据，使用上一次价格计算")
                # 使用持仓中记录的当前价格
                latest_price = pos.get('current_price', pos['entry_price'])
            else:
                latest_price = symbol_data.iloc[-1]['close']
            
            # 计算持仓价值
            if pos['direction'] == 'long':
                value = pos['size'] * latest_price
            else:  # short
                value = pos['size'] * pos['entry_price'] * 0.01  # 保证金价值
                # 加上浮动盈亏
                value += pos['size'] * (pos['entry_price'] - latest_price)
            
            positions_value += value
        
        # 总资产 = 现金 + 持仓价值
        return self.cash + positions_value
    
    def _monitor_and_adjust_positions(self, current_data, current_date):
        """监控和调整持仓"""
        # 这里可以实现风险管理中的持仓调整逻辑
        # 例如止盈止损、动态调整仓位等
        
        # 获取持仓调整建议
        market_data = current_data['market_data']
        
        for symbol, pos in list(self.positions.items()):
            # 获取最新价格
            symbol_data = market_data[market_data['symbol'] == symbol]
            if symbol_data.empty:
                continue
                
            latest_price = symbol_data.iloc[-1]['close']
            
            # 止损检查 (示例: -20%)
            if pos['direction'] == 'long' and latest_price < pos['entry_price'] * 0.8:
                # 触发止损，平掉多头持仓
                trade = {
                    'timestamp': current_date,
                    'symbol': symbol,
                    'type': 'sell',
                    'quantity': pos['size'],
                    'price': latest_price,
                    'value': pos['size'] * latest_price,
                    'reason': 'stop_loss'
                }
                
                # 更新资金
                self.cash += pos['size'] * latest_price
                
                # 移除持仓
                del self.positions[symbol]
                
                # 记录交易
                self.trades.append(trade)
                logger.debug(f"执行止损: {trade}")
                
            elif pos['direction'] == 'short' and latest_price > pos['entry_price'] * 1.2:
                # 触发止损，平掉空头持仓
                trade = {
                    'timestamp': current_date,
                    'symbol': symbol,
                    'type': 'buy',
                    'quantity': pos['size'],
                    'price': latest_price,
                    'value': pos['size'] * latest_price,
                    'reason': 'stop_loss'
                }
                
                # 更新资金 (返还保证金 + 盈亏)
                self.cash += pos['size'] * pos['entry_price'] * 0.01  # 返还保证金
                self.cash += pos['size'] * (pos['entry_price'] - latest_price)  # 盈亏
                
                # 移除持仓
                del self.positions[symbol]
                
                # 记录交易
                self.trades.append(trade)
                logger.debug(f"执行止损: {trade}")
                
            # 可以添加更多的持仓调整逻辑，如移动止损、部分止盈等
    
    def _calculate_metrics(self):
        """计算回测性能指标"""
        # 准备数据
        dates = [item['date'] for item in self.portfolio_values]
        values = [item['value'] for item in self.portfolio_values]
        
        # 转换为DataFrame
        daily_values = pd.DataFrame({
            'date': dates,
            'portfolio_value': values
        })
        
        daily_values = daily_values.set_index('date')
        
        # 计算指标
        metrics = calculate_metrics(daily_values, self.initial_capital, self.trades)
        
        logger.info(f"回测性能指标: {metrics}")
        return metrics
    
    def generate_report(self, output_path=None):
        """生成回测报告"""
        from backtest.visualization import create_performance_charts
        
        if output_path is None:
            # 使用当前日期作为报告文件名
            now = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f'./backtest_report_{now}.html'
        
        # 准备数据
        df_values = pd.DataFrame(self.portfolio_values)
        df_trades = pd.DataFrame(self.trades)
        
        # 创建图表
        create_performance_charts(df_values, df_trades, self.metrics, output_path)
        
        logger.info(f"回测报告已生成: {output_path}")
        return output_path
