"""
策略核心層 - 信號生成模組
負責根據因子評分生成交易信號和計算倉位大小
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from database.db_manager import DBManager
import config

logger = logging.getLogger(__name__)


class SignalGenerator:
    """信號生成器，負責生成交易信號和計算倉位大小"""
    
    def __init__(self):
        """初始化信號生成器"""
        self.db_manager = DBManager()
        self.position_config = config.POSITION_CONFIG
        
    def generate_signals(self, factor_scores):
        """
        根據因子評分生成交易信號
        
        Args:
            factor_scores: 因子評分DataFrame
            
        Returns:
            DataFrame: 包含交易信號的DataFrame
        """
        try:
            if factor_scores.empty:
                logger.warning("因子評分為空，無法生成信號")
                return pd.DataFrame()
            
            # 篩選符合條件的交易對
            eligible_tokens = factor_scores[factor_scores['is_eligible']]
            
            if eligible_tokens.empty:
                logger.warning("沒有符合條件的交易對，無法生成信號")
                return pd.DataFrame()
            
            # 獲取當前倉位情況
            current_positions = self.db_manager.get_open_positions()
            
            # 初始化信號列表
            signals = []
            
            # 根據評分排名生成信號
            for _, row in eligible_tokens.iterrows():
                symbol = row['symbol']
                
                # 檢查是否已有該幣種的倉位
                has_position = False
                if not current_positions.empty:
                    has_position = symbol in current_positions['symbol'].values
                
                # 如果已有倉位，則不生成新的開倉信號
                if has_position:
                    continue
                
                # 只對評分足夠高的交易對生成信號（閾值可以設置為0.5或其他值）
                if row['total_score'] >= 0.5:
                    # 計算建議的倉位大小
                    position_size = self.calculate_position_size(row, current_positions)
                    
                    # 如果倉位大小為0，則不生成信號
                    if position_size <= 0:
                        continue
                    
                    # 獲取當前市場價格
                    market_data = self.db_manager.get_market_data(symbol=symbol, limit=1)
                    if market_data.empty:
                        logger.warning(f"無法獲取 {symbol} 的市場數據，跳過信號生成")
                        continue
                        
                    price = market_data.iloc[0]['last_price']
                    
                    # 生成信號
                    signal = {
                        'symbol': symbol,
                        'timestamp': datetime.utcnow(),
                        'signal_type': 'sell',  # 對於統計套利策略，這裡生成做空信號
                        'price': price,
                        'quantity': position_size,
                        'reason': self._generate_signal_reason(row),
                        'score': row['total_score']
                    }
                    
                    signals.append(signal)
            
            # 轉換為DataFrame
            signals_df = pd.DataFrame(signals) if signals else pd.DataFrame()
            
            if not signals_df.empty:
                # 按評分排序
                signals_df = signals_df.sort_values('score', ascending=False)
                
                # 保存信號到數據庫
                self.db_manager.save_trade_signal(signals_df)
                
                logger.info(f"成功生成了 {len(signals_df)} 個交易信號")
            else:
                logger.info("沒有生成新的交易信號")
                
            return signals_df
            
        except Exception as e:
            logger.error(f"生成交易信號失敗: {str(e)}")
            return pd.DataFrame()
    
    def calculate_position_size(self, factor_row, current_positions):
        """
        計算初始倉位大小（控制在2.5%以內）
        
        Args:
            factor_row: 因子評分行
            current_positions: 當前所有倉位
            
        Returns:
            float: 建議的倉位大小
        """
        try:
            # 獲取配置參數
            initial_position_percentage = self.position_config['initial_position_percentage']
            
            # 簡單實現：使用固定的初始倉位比例
            # 實際應用中，可以根據總資金和風險因素動態計算
            position_size = initial_position_percentage
            
            # 根據因子評分調整倉位大小
            # 評分越高，倉位越接近最大值；評分越低，倉位越小
            if 'total_score' in factor_row:
                score = factor_row['total_score']
                # 調整係數，評分為1時使用100%的初始倉位，評分為0.5時使用70%
                adjustment_factor = 0.4 + 0.6 * score
                position_size = position_size * adjustment_factor
            
            # 確保倉位大小不超過配置的最大值
            position_size = min(position_size, initial_position_percentage)
            
            return position_size
            
        except Exception as e:
            logger.error(f"計算倉位大小失敗: {str(e)}")
            return 0
    
    def _generate_signal_reason(self, factor_row):
        """
        生成信號原因說明
        
        Args:
            factor_row: 因子評分行
            
        Returns:
            str: 信號生成原因
        """
        reasons = []
        
        if factor_row.get('funding_rate', 0) > 0:
            reasons.append(f"資金費率為正({factor_row.get('funding_rate', 0):.4f})")
            
        if factor_row.get('pump_pattern_score', 0) > 0.5:
            reasons.append("識別到拉盤模式")
            
        if factor_row.get('sector_score', 0) > 0.5:
            reasons.append("屬於目標賽道")
            
        if factor_row.get('liquidity_score', 0) > 0.5:
            reasons.append("流動性良好")
            
        return ", ".join(reasons) if reasons else "符合綜合因子評分要求"
