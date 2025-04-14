"""
交易執行層 - 負責執行下單指令、監控訂單狀態和報告執行結果
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
import ccxt
from database.db_manager import DBManager
import config

logger = logging.getLogger(__name__)


class TradeExecutor:
    """交易執行器，負責執行下單指令、監控訂單狀態和報告執行結果"""
    
    def __init__(self):
        """初始化交易執行器"""
        self.exchange_config = config.EXCHANGE_CONFIG
        self.exchange = self._initialize_exchange()
        self.db_manager = DBManager()
        
    def _initialize_exchange(self):
        """初始化交易所連接"""
        try:
            exchange_class = getattr(ccxt, self.exchange_config['name'])
            exchange = exchange_class({
                'apiKey': self.exchange_config['api_key'],
                'secret': self.exchange_config['secret'],
                'timeout': self.exchange_config['timeout'],
                'enableRateLimit': True,
            })
            logger.info(f"成功初始化交易所: {self.exchange_config['name']}")
            return exchange
        except Exception as e:
            logger.error(f"初始化交易所失敗: {str(e)}")
            raise
        
    def execute_order(self, signal):
        """
        執行下單指令
        
        Args:
            signal: 交易信號，包含symbol、signal_type、price、quantity等信息
            
        Returns:
            dict: 訂單執行結果
        """
        try:
            symbol = signal['symbol']
            signal_type = signal['signal_type']
            price = signal.get('price', None)  # 如果未提供價格，則使用市價單
            quantity = signal['quantity']
            
            # 轉換信號類型為交易所支持的訂單類型
            order_side = 'sell' if signal_type == 'sell' else 'buy'
            
            # 訂單類型：如果提供了價格，則使用限價單；否則使用市價單
            order_type = 'limit' if price is not None else 'market'
            
            # 執行下單
            if order_type == 'limit':
                order = self.exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=quantity,
                    price=price
                )
            else:
                order = self.exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=quantity
                )
            
            logger.info(f"成功執行 {symbol} {order_side} 訂單，數量: {quantity}, 類型: {order_type}")
            
            # 將訂單信息保存到資料庫
            self._save_order(order, signal)
            
            # 如果是市價單，訂單可能已經完成，創建倉位記錄
            if order_type == 'market' or order.get('status') == 'closed':
                self._create_position(order, signal)
            
            return order
            
        except Exception as e:
            logger.error(f"執行訂單失敗: {str(e)}")
            
            # 記錄失敗的訂單
            failed_order = {
                'symbol': signal.get('symbol', ''),
                'order_id': None,
                'status': 'failed',
                'error': str(e)
            }
            self._save_order(failed_order, signal)
            
            return {'status': 'failed', 'error': str(e)}
        
    def monitor_order_status(self, order_id=None):
        """
        監控訂單狀態
        
        Args:
            order_id: 訂單ID，如果為None則監控所有未完成的訂單
            
        Returns:
            DataFrame: 包含訂單狀態的DataFrame
        """
        try:
            # 待實現：從數據庫獲取未完成的訂單
            # 此處需要另外實現訂單存儲和查詢的功能
            
            # 示例實現
            if order_id:
                order = self.exchange.fetch_order(order_id)
                orders = [order]
            else:
                open_orders = self.exchange.fetch_open_orders()
                orders = open_orders
            
            # 更新訂單狀態到數據庫
            for order in orders:
                self._update_order_status(order)
                
                # 如果訂單完成，創建倉位記錄
                if order.get('status') == 'closed':
                    # 獲取原始信號信息（需要從數據庫中查詢）
                    # 此處示例假設有獲取信號的方法
                    signal = self._get_signal_by_order_id(order.get('id'))
                    if signal:
                        self._create_position(order, signal)
            
            logger.info(f"成功更新了 {len(orders)} 個訂單的狀態")
            return pd.DataFrame(orders)
            
        except Exception as e:
            logger.error(f"監控訂單狀態失敗: {str(e)}")
            return pd.DataFrame()
        
    def report_execution_results(self, start_time=None, end_time=None):
        """
        報告執行結果
        
        Args:
            start_time: 開始時間
            end_time: 結束時間
            
        Returns:
            DataFrame: 包含執行結果的DataFrame
        """
        try:
            # 待實現：從數據庫獲取訂單執行結果
            # 此處需要另外實現訂單存儲和查詢的功能
            
            # 示例實現
            logger.info("報告執行結果功能待實現")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"報告執行結果失敗: {str(e)}")
            return pd.DataFrame()
            
    def _save_order(self, order, signal):
        """
        將訂單信息保存到資料庫
        
        Args:
            order: 訂單信息
            signal: 原始交易信號
        """
        # 待實現：保存訂單信息到數據庫
        # 此處需要另外實現訂單存儲功能
        logger.info(f"保存訂單信息功能待實現: {order.get('id', 'unknown')}")
        
    def _update_order_status(self, order):
        """
        更新訂單狀態到數據庫
        
        Args:
            order: 訂單信息
        """
        # 待實現：更新訂單狀態到數據庫
        # 此處需要另外實現訂單狀態更新功能
        logger.info(f"更新訂單狀態功能待實現: {order.get('id', 'unknown')}")
        
    def _create_position(self, order, signal):
        """
        創建倉位記錄
        
        Args:
            order: 訂單信息
            signal: 原始交易信號
        """
        try:
            # 確認訂單已成交
            if order.get('status') != 'closed':
                return
                
            symbol = order.get('symbol')
            side = order.get('side')
            
            # 交易方向轉換為倉位方向（buy→long, sell→short）
            direction = 'long' if side == 'buy' else 'short'
            
            # 獲取成交價格和數量
            fill_price = order.get('price') or order.get('average')
            fill_quantity = order.get('filled', 0)
            
            if fill_quantity <= 0:
                logger.warning(f"訂單 {order.get('id')} 沒有成交量")
                return
            
            # 創建倉位記錄
            position = {
                'symbol': symbol,
                'direction': direction,
                'open_time': datetime.fromtimestamp(order.get('timestamp') / 1000) if order.get('timestamp') else datetime.utcnow(),
                'open_price': fill_price,
                'current_price': fill_price,  # 初始時當前價格等於開倉價格
                'quantity': fill_quantity,
                'pnl': 0,
                'pnl_percentage': 0,
                'status': 'open',
                'updated_at': datetime.utcnow()
            }
            
            # 保存倉位到數據庫
            position_df = pd.DataFrame([position])
            self.db_manager.save_position(position_df)
            
            logger.info(f"成功創建 {symbol} {direction} 倉位，數量: {fill_quantity}, 價格: {fill_price}")
            
        except Exception as e:
            logger.error(f"創建倉位記錄失敗: {str(e)}")
            
    def _get_signal_by_order_id(self, order_id):
        """
        根據訂單ID獲取原始信號信息
        
        Args:
            order_id: 訂單ID
            
        Returns:
            dict: 原始信號信息，如果未找到則返回None
        """
        # 待實現：從數據庫獲取與訂單關聯的信號信息
        # 此處需要另外實現信號存儲和查詢的功能
        logger.info(f"獲取信號信息功能待實現: {order_id}")
        return None
