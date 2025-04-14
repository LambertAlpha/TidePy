"""
風控管理層 - 負責管理倉位風險和調整倉位
根據策略要求實現以下風控措施：
1. 單幣空單持倉佔比控制5%以內
2. 單幣空單初始倉位佔比控制在2.5%以內
3. 加倉條件：虧損達到30%可加倉，盈利達到15%可加倉
4. 減倉條件：持倉達限制上限時虧損20%可減倉一半，持倉達限制上限時盈利20%可減倉一半
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from database.db_manager import DBManager
import config

logger = logging.getLogger(__name__)


class RiskManager:
    """風險管理器，負責倉位限制、盈虧監控和倉位調整"""
    
    def __init__(self):
        """初始化風險管理器"""
        self.db_manager = DBManager()
        self.position_config = config.POSITION_CONFIG
        
    def check_position_limit(self, symbol, position_size):
        """
        檢查單幣空單持倉是否超過限制（5%）
        
        Args:
            symbol: 交易對名稱
            position_size: 擬增加的倉位大小
            
        Returns:
            bool: 是否符合倉位限制
            float: 允許的最大倉位大小
        """
        try:
            # 獲取當前該幣種的倉位
            current_positions = self.db_manager.get_open_positions()
            if current_positions.empty:
                # 如果沒有現有倉位，直接檢查新倉位是否符合初始倉位限制
                max_position_percentage = self.position_config['max_position_percentage']
                initial_position_percentage = self.position_config['initial_position_percentage']
                
                if position_size <= initial_position_percentage:
                    return True, position_size
                else:
                    logger.warning(f"{symbol} 初始倉位大小 {position_size} 超過限制 {initial_position_percentage}")
                    return False, initial_position_percentage
            
            # 篩選出該幣種的持倉
            symbol_positions = current_positions[current_positions['symbol'] == symbol]
            
            if symbol_positions.empty:
                # 如果該幣種沒有現有倉位，使用初始倉位限制
                initial_position_percentage = self.position_config['initial_position_percentage']
                if position_size <= initial_position_percentage:
                    return True, position_size
                else:
                    logger.warning(f"{symbol} 初始倉位大小 {position_size} 超過限制 {initial_position_percentage}")
                    return False, initial_position_percentage
            
            # 計算當前該幣種的總倉位
            current_position_sum = symbol_positions['quantity'].sum()
            
            # 計算新的總倉位
            new_position_sum = current_position_sum + position_size
            
            # 檢查是否超過最大倉位限制
            max_position_percentage = self.position_config['max_position_percentage']
            if new_position_sum <= max_position_percentage:
                return True, position_size
            else:
                # 計算允許的最大增加倉位
                allowed_increase = max(0, max_position_percentage - current_position_sum)
                logger.warning(f"{symbol} 增加倉位 {position_size} 會導致總倉位 {new_position_sum} 超過限制 {max_position_percentage}，調整為 {allowed_increase}")
                return False, allowed_increase
                
        except Exception as e:
            logger.error(f"檢查倉位限制失敗: {str(e)}")
            return False, 0
    
    def filter_signals(self, signals):
        """
        根據風控規則過濾交易信號
        
        Args:
            signals: 原始交易信號DataFrame
            
        Returns:
            DataFrame: 過濾後的交易信號
        """
        try:
            if signals.empty:
                return pd.DataFrame()
                
            filtered_signals = []
            
            for _, signal in signals.iterrows():
                symbol = signal['symbol']
                position_size = signal['quantity']
                
                # 檢查倉位限制
                is_allowed, adjusted_size = self.check_position_limit(symbol, position_size)
                
                if adjusted_size > 0:
                    # 更新倉位大小
                    signal_copy = signal.copy()
                    signal_copy['quantity'] = adjusted_size
                    signal_copy['is_adjusted'] = not is_allowed
                    
                    filtered_signals.append(signal_copy)
            
            filtered_df = pd.DataFrame(filtered_signals) if filtered_signals else pd.DataFrame()
            
            if not filtered_df.empty:
                logger.info(f"風控過濾後保留 {len(filtered_df)} 個交易信號，調整了 {filtered_df['is_adjusted'].sum()} 個信號的倉位大小")
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"過濾交易信號失敗: {str(e)}")
            return pd.DataFrame()
    
    def monitor_pnl(self):
        """
        監控所有倉位的盈虧狀態
        
        Returns:
            DataFrame: 包含盈虧信息的倉位DataFrame
        """
        try:
            # 獲取當前所有開倉的倉位
            positions = self.db_manager.get_open_positions()
            
            if positions.empty:
                logger.info("當前沒有開倉的倉位")
                return pd.DataFrame()
            
            # 獲取最新市場數據以更新當前價格
            for i, position in positions.iterrows():
                symbol = position['symbol']
                market_data = self.db_manager.get_market_data(symbol=symbol, limit=1)
                
                if not market_data.empty:
                    current_price = market_data.iloc[0]['last_price']
                    
                    # 更新當前價格
                    positions.at[i, 'current_price'] = current_price
                    
                    # 計算盈虧
                    if position['direction'] == 'long':
                        # 多倉盈虧 = (當前價格 - 開倉價格) / 開倉價格
                        pnl_percentage = (current_price - position['open_price']) / position['open_price']
                    else:
                        # 空倉盈虧 = (開倉價格 - 當前價格) / 開倉價格
                        pnl_percentage = (position['open_price'] - current_price) / position['open_price']
                    
                    positions.at[i, 'pnl_percentage'] = pnl_percentage
                    positions.at[i, 'pnl'] = pnl_percentage * position['quantity'] * position['open_price']
                    
                    # 更新數據庫中的倉位信息
                    self.db_manager.update_position(position['id'], {
                        'current_price': current_price,
                        'pnl': positions.at[i, 'pnl'],
                        'pnl_percentage': pnl_percentage
                    })
            
            logger.info(f"成功更新了 {len(positions)} 個倉位的盈虧信息")
            return positions
            
        except Exception as e:
            logger.error(f"監控倉位盈虧失敗: {str(e)}")
            return pd.DataFrame()
    
    def monitor_and_adjust_positions(self):
        """
        監控並調整現有倉位
        
        根據以下規則調整：
        1. 虧損達到30%可加倉
        2. 盈利達到15%可加倉
        3. 持倉達限制上限時虧損20%可減倉一半
        4. 持倉達限制上限時盈利20%可減倉一半
        
        Returns:
            DataFrame: 調整建議
        """
        try:
            # 監控盈虧
            positions = self.monitor_pnl()
            
            if positions.empty:
                return pd.DataFrame()
            
            # 獲取配置參數
            add_position_loss_threshold = self.position_config['add_position_loss_threshold']
            add_position_profit_threshold = self.position_config['add_position_profit_threshold']
            reduce_position_loss_threshold = self.position_config['reduce_position_loss_threshold']
            reduce_position_profit_threshold = self.position_config['reduce_position_profit_threshold']
            reduce_position_ratio = self.position_config['reduce_position_ratio']
            max_position_percentage = self.position_config['max_position_percentage']
            
            # 初始化調整建議列表
            adjustments = []
            
            # 分析每個倉位並生成調整建議
            for _, position in positions.iterrows():
                symbol = position['symbol']
                direction = position['direction']
                pnl_percentage = position['pnl_percentage']
                quantity = position['quantity']
                
                # 檢查該幣種的總倉位是否接近上限
                close_to_limit = quantity >= (max_position_percentage * 0.9)
                
                # 根據盈虧生成調整建議
                if close_to_limit:
                    # 持倉接近上限時的調整規則
                    if pnl_percentage <= -reduce_position_loss_threshold:
                        # 虧損20%以上，減倉一半
                        adjustment = {
                            'symbol': symbol,
                            'direction': direction,
                            'action': 'reduce',
                            'current_quantity': quantity,
                            'adjustment_quantity': quantity * reduce_position_ratio,
                            'reason': f"持倉接近上限且虧損達到 {pnl_percentage:.2%}，減倉 {reduce_position_ratio * 100:.0f}%"
                        }
                        adjustments.append(adjustment)
                        
                    elif pnl_percentage >= reduce_position_profit_threshold:
                        # 盈利20%以上，減倉一半
                        adjustment = {
                            'symbol': symbol,
                            'direction': direction,
                            'action': 'reduce',
                            'current_quantity': quantity,
                            'adjustment_quantity': quantity * reduce_position_ratio,
                            'reason': f"持倉接近上限且盈利達到 {pnl_percentage:.2%}，減倉 {reduce_position_ratio * 100:.0f}%"
                        }
                        adjustments.append(adjustment)
                else:
                    # 持倉未接近上限時的調整規則
                    if pnl_percentage <= -add_position_loss_threshold:
                        # 虧損30%以上，可加倉
                        # 計算可增加的倉位大小（不超過最大倉位限制）
                        _, allowed_increase = self.check_position_limit(symbol, quantity * 0.5)
                        
                        if allowed_increase > 0:
                            adjustment = {
                                'symbol': symbol,
                                'direction': direction,
                                'action': 'increase',
                                'current_quantity': quantity,
                                'adjustment_quantity': allowed_increase,
                                'reason': f"虧損達到 {pnl_percentage:.2%}，加倉 {allowed_increase:.4f}"
                            }
                            adjustments.append(adjustment)
                            
                    elif pnl_percentage >= add_position_profit_threshold:
                        # 盈利15%以上，可加倉
                        # 計算可增加的倉位大小（不超過最大倉位限制）
                        _, allowed_increase = self.check_position_limit(symbol, quantity * 0.5)
                        
                        if allowed_increase > 0:
                            adjustment = {
                                'symbol': symbol,
                                'direction': direction,
                                'action': 'increase',
                                'current_quantity': quantity,
                                'adjustment_quantity': allowed_increase,
                                'reason': f"盈利達到 {pnl_percentage:.2%}，加倉 {allowed_increase:.4f}"
                            }
                            adjustments.append(adjustment)
            
            # 轉換為DataFrame
            adjustments_df = pd.DataFrame(adjustments) if adjustments else pd.DataFrame()
            
            if not adjustments_df.empty:
                logger.info(f"生成了 {len(adjustments_df)} 條倉位調整建議")
                
                # 保存調整建議到數據庫（這裡可以實現一個新的數據表來存儲）
                
            return adjustments_df
            
        except Exception as e:
            logger.error(f"監控並調整倉位失敗: {str(e)}")
            return pd.DataFrame()
