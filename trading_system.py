"""
系統整合層 - 整合各個模組，實現系統的完整功能
"""

import logging
import time
from datetime import datetime
import os
from dotenv import load_dotenv

from data_collector.collector import DataCollector
from strategy.factor_analyzer import FactorAnalyzer
from strategy.signal_generator import SignalGenerator
from risk_manager.risk_manager import RiskManager
from trade_executor.executor import TradeExecutor
from utils.logger import setup_logger
import config

# 載入環境變數
load_dotenv()

# 設置日誌
logger = setup_logger()


class TradingSystem:
    """交易系統，整合各個模組，實現系統的完整功能"""
    
    def __init__(self):
        """初始化交易系統"""
        logger.info("初始化交易系統...")
        self.data_collector = DataCollector()
        self.factor_analyzer = FactorAnalyzer()
        self.signal_generator = SignalGenerator()
        self.risk_manager = RiskManager()
        self.trade_executor = TradeExecutor()
        
        # 從配置中獲取系統循環間隔時間
        self.system_interval = config.SYSTEM_INTERVAL
        
        logger.info("交易系統初始化完成")
        
    def run(self):
        """
        系統主循環
        """
        logger.info("開始運行交易系統主循環")
        
        while True:
            try:
                logger.info(f"=== 開始新的循環 {datetime.now()} ===")
                
                # 1. 採集數據
                logger.info("開始採集數據...")
                market_data = self.data_collector.collect_market_data()
                funding_data = self.data_collector.fetch_funding_rate()
                token_info = self.data_collector.get_token_info()
                
                # 2. 計算因子和信號
                logger.info("開始計算因子...")
                factor_scores = self.factor_analyzer.calculate_factors(
                    market_data, funding_data, token_info
                )
                
                logger.info("開始生成交易信號...")
                signals = self.signal_generator.generate_signals(factor_scores)
                
                # 3. 風險評估
                logger.info("開始風險評估...")
                filtered_signals = self.risk_manager.filter_signals(signals)
                
                # 4. 執行交易
                if not filtered_signals.empty:
                    logger.info(f"開始執行 {len(filtered_signals)} 個交易信號...")
                    for _, signal in filtered_signals.iterrows():
                        self.trade_executor.execute_order(signal)
                else:
                    logger.info("沒有可執行的交易信號")
                
                # 5. 監控並調整現有倉位
                logger.info("開始監控和調整倉位...")
                position_adjustments = self.risk_manager.monitor_and_adjust_positions()
                
                # 6. 執行倉位調整
                if not position_adjustments.empty:
                    logger.info(f"開始執行 {len(position_adjustments)} 個倉位調整...")
                    for _, adjustment in position_adjustments.iterrows():
                        # 根據調整建議生成交易信號
                        if adjustment['action'] == 'increase':
                            # 加倉信號
                            signal = {
                                'symbol': adjustment['symbol'],
                                'signal_type': 'sell' if adjustment['direction'] == 'short' else 'buy',
                                'quantity': adjustment['adjustment_quantity'],
                                'timestamp': datetime.now(),
                                'reason': adjustment['reason']
                            }
                            self.trade_executor.execute_order(signal)
                            
                        elif adjustment['action'] == 'reduce':
                            # 減倉信號
                            signal = {
                                'symbol': adjustment['symbol'],
                                'signal_type': 'buy' if adjustment['direction'] == 'short' else 'sell',
                                'quantity': adjustment['adjustment_quantity'],
                                'timestamp': datetime.now(),
                                'reason': adjustment['reason']
                            }
                            self.trade_executor.execute_order(signal)
                else:
                    logger.info("沒有需要執行的倉位調整")
                
                # 7. 監控訂單狀態
                logger.info("開始監控訂單狀態...")
                self.trade_executor.monitor_order_status()
                
                logger.info(f"=== 循環完成，休息 {self.system_interval} 秒 ===")
                time.sleep(self.system_interval)
                
            except KeyboardInterrupt:
                logger.info("收到退出信號，系統關閉")
                break
                
            except Exception as e:
                logger.error(f"系統運行出現錯誤: {str(e)}")
                logger.info(f"系統將在 {self.system_interval} 秒後重試")
                time.sleep(self.system_interval)
        
        logger.info("交易系統已停止運行")
        
    def run_backtest(self, start_date, end_date, initial_capital):
        """
        運行回測
        
        Args:
            start_date: 回測開始日期
            end_date: 回測結束日期
            initial_capital: 初始資金
            
        Returns:
            dict: 回測結果
        """
        logger.info(f"開始回測: {start_date} 至 {end_date}, 初始資金: {initial_capital}")
        
        # 回測功能待實現
        logger.info("回測功能待實現")
        
        return {'status': 'not_implemented'}
