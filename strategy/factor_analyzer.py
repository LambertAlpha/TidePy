"""
策略核心層 - 因子分析模組
負責計算和分析各項交易因子，包括資金費率、流動性、拉盤模式識別、解鎖進度評估和賽道分類
"""

import logging
import pandas as pd
import numpy as np
from database.db_manager import DBManager
import config

logger = logging.getLogger(__name__)


class FactorAnalyzer:
    """因子分析器，負責計算和分析各項交易因子"""
    
    def __init__(self):
        """初始化因子分析器"""
        self.db_manager = DBManager()
        self.strategy_config = config.STRATEGY_CONFIG
        
    def calculate_factors(self, market_data=None, funding_data=None, token_info=None):
        """
        計算各項因子
        
        Args:
            market_data: 市場數據DataFrame，如果為None則從數據庫獲取
            funding_data: 資金費率DataFrame，如果為None則從數據庫獲取
            token_info: 代幣信息DataFrame，如果為None則從數據庫獲取
            
        Returns:
            DataFrame: 包含所有因子評分的DataFrame
        """
        try:
            # 如果未提供數據，則從數據庫獲取
            if market_data is None:
                market_data = self.db_manager.get_market_data(limit=1000)
                
            if funding_data is None:
                funding_data = self.db_manager.get_funding_rate(limit=1000)
                
            if token_info is None:
                token_info = self.db_manager.get_token_info()
                
            # 如果數據為空，返回空DataFrame
            if market_data.empty:
                logger.warning("無法獲取市場數據，無法計算因子")
                return pd.DataFrame()
                
            # 初始化因子DataFrame
            symbols = market_data['symbol'].unique()
            factor_scores = pd.DataFrame({
                'symbol': symbols,
                'timestamp': pd.Timestamp.utcnow()
            })
            
            # 1. 資金費率篩選
            factor_scores = self._analyze_funding_rate(factor_scores, funding_data)
            
            # 2. 流動性分析
            factor_scores = self._analyze_liquidity(factor_scores, market_data)
            
            # 3. 拉盤模式識別
            factor_scores = self._analyze_pump_patterns(factor_scores, market_data)
            
            # 4. 解鎖進度評估
            if not token_info.empty:
                factor_scores = self._analyze_unlock_progress(factor_scores, token_info)
            
            # 5. 賽道分類
            if not token_info.empty:
                factor_scores = self._analyze_sector(factor_scores, token_info)
            
            # 計算綜合因子評分
            factor_scores = self._calculate_total_score(factor_scores)
            
            logger.info(f"成功計算了 {len(factor_scores)} 個交易對的因子評分")
            return factor_scores
            
        except Exception as e:
            logger.error(f"計算因子失敗: {str(e)}")
            return pd.DataFrame()
    
    def _analyze_funding_rate(self, factor_scores, funding_data):
        """
        分析資金費率因子
        
        根據策略要求，資金費率不能為負
        
        Args:
            factor_scores: 因子評分DataFrame
            funding_data: 資金費率數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if funding_data.empty:
                logger.warning("無資金費率數據，跳過資金費率分析")
                factor_scores['funding_rate_score'] = 0
                factor_scores['funding_rate'] = 0
                return factor_scores
            
            # 獲取每個交易對的最新資金費率
            latest_funding = funding_data.sort_values('timestamp', ascending=False)
            latest_funding = latest_funding.drop_duplicates('symbol', keep='first')
            
            # 合併資金費率數據到因子評分表
            factor_scores = pd.merge(
                factor_scores, 
                latest_funding[['symbol', 'funding_rate']], 
                on='symbol', 
                how='left'
            )
            
            # 填充缺失值
            factor_scores['funding_rate'] = factor_scores['funding_rate'].fillna(0)
            
            # 計算資金費率評分：必須為非負，評分為0或1
            min_funding_rate = self.strategy_config['min_funding_rate']
            factor_scores['funding_rate_score'] = factor_scores['funding_rate'].apply(
                lambda x: 1 if x >= min_funding_rate else 0
            )
            
            logger.info(f"完成資金費率分析，有 {factor_scores['funding_rate_score'].sum()} 個交易對的資金費率符合要求")
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析資金費率因子失敗: {str(e)}")
            factor_scores['funding_rate_score'] = 0
            factor_scores['funding_rate'] = 0
            return factor_scores
    
    def _analyze_liquidity(self, factor_scores, market_data):
        """
        分析流動性因子
        
        Args:
            factor_scores: 因子評分DataFrame
            market_data: 市場數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if market_data.empty:
                logger.warning("無市場數據，跳過流動性分析")
                factor_scores['liquidity_score'] = 0
                factor_scores['market_cap_score'] = 0
                return factor_scores
            
            # 獲取每個交易對的最新市場數據
            latest_market = market_data.sort_values('timestamp', ascending=False)
            latest_market = latest_market.drop_duplicates('symbol', keep='first')
            
            # 計算美元成交量
            latest_market['usd_volume'] = latest_market['last_price'] * latest_market['volume_24h']
            
            # 合併流動性數據到因子評分表
            factor_scores = pd.merge(
                factor_scores, 
                latest_market[['symbol', 'usd_volume', 'last_price', 'volume_24h']], 
                on='symbol', 
                how='left'
            )
            
            # 填充缺失值
            factor_scores['usd_volume'] = factor_scores['usd_volume'].fillna(0)
            
            # 計算流動性評分：標準化後的美元成交量
            min_liquidity = self.strategy_config['min_liquidity']
            factor_scores['liquidity_score'] = factor_scores['usd_volume'].apply(
                lambda x: 0 if x < min_liquidity else min(1, x / (10 * min_liquidity))
            )
            
            # 市值評分（假設市值為價格乘以流通量，這裡使用成交量作為近似）
            min_market_cap = self.strategy_config['min_market_cap']
            factor_scores['market_cap_score'] = factor_scores['usd_volume'].apply(
                lambda x: 0 if x < min_market_cap else min(1, x / (10 * min_market_cap))
            )
            
            logger.info(f"完成流動性分析，有 {(factor_scores['liquidity_score'] > 0).sum()} 個交易對的流動性符合要求")
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析流動性因子失敗: {str(e)}")
            factor_scores['liquidity_score'] = 0
            factor_scores['market_cap_score'] = 0
            return factor_scores
    
    def _analyze_pump_patterns(self, factor_scores, market_data):
        """
        分析拉盤模式
        
        策略要求：考慮是否拉過盤（拉過盤做空成功率會大一些）
        
        Args:
            factor_scores: 因子評分DataFrame
            market_data: 市場數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if market_data.empty:
                logger.warning("無市場數據，跳過拉盤模式分析")
                factor_scores['pump_pattern_score'] = 0
                return factor_scores
            
            # 按交易對和時間排序
            market_data_sorted = market_data.sort_values(['symbol', 'timestamp'])
            
            # 計算價格變化率
            market_data_sorted['price_change'] = market_data_sorted.groupby('symbol')['last_price'].pct_change()
            
            # 識別拉盤模式（簡單示例：過去24小時價格上漲超過20%）
            pump_patterns = []
            for symbol in factor_scores['symbol']:
                symbol_data = market_data_sorted[market_data_sorted['symbol'] == symbol]
                
                if len(symbol_data) < 2:
                    pump_patterns.append({'symbol': symbol, 'pump_pattern_score': 0})
                    continue
                
                # 拉盤判斷指標1：短期內價格快速上漲
                max_price_change = symbol_data['price_change'].max()
                
                # 拉盤判斷指標2：成交量突然放大
                max_volume = symbol_data['volume_24h'].max()
                avg_volume = symbol_data['volume_24h'].mean()
                volume_ratio = max_volume / avg_volume if avg_volume > 0 else 1
                
                # 綜合評分
                pump_score = 0
                if max_price_change > 0.2:  # 單日漲幅超過20%
                    pump_score += 0.5
                if volume_ratio > 3:  # 成交量放大3倍以上
                    pump_score += 0.5
                
                pump_patterns.append({
                    'symbol': symbol, 
                    'pump_pattern_score': min(1, pump_score)
                })
            
            # 合併拉盤模式評分到因子評分表
            pump_patterns_df = pd.DataFrame(pump_patterns)
            factor_scores = pd.merge(factor_scores, pump_patterns_df, on='symbol', how='left')
            
            # 填充缺失值
            factor_scores['pump_pattern_score'] = factor_scores['pump_pattern_score'].fillna(0)
            
            logger.info(f"完成拉盤模式分析，有 {(factor_scores['pump_pattern_score'] > 0.5).sum()} 個交易對符合拉盤模式")
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析拉盤模式因子失敗: {str(e)}")
            factor_scores['pump_pattern_score'] = 0
            return factor_scores
    
    def _analyze_unlock_progress(self, factor_scores, token_info):
        """
        分析代幣解鎖進度
        
        策略要求：考慮解鎖進度和流通的籌碼
        
        Args:
            factor_scores: 因子評分DataFrame
            token_info: 代幣信息數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if token_info.empty:
                logger.warning("無代幣信息數據，跳過解鎖進度分析")
                factor_scores['unlock_progress_score'] = 0
                return factor_scores
            
            # 獲取最新的代幣信息
            latest_token_info = token_info.sort_values('updated_at', ascending=False)
            latest_token_info = latest_token_info.drop_duplicates('symbol', keep='first')
            
            # 計算解鎖進度評分
            latest_token_info['unlock_progress_score'] = latest_token_info.apply(
                lambda row: min(1, row.get('unlock_progress', 0) / 100) 
                if 'unlock_progress' in latest_token_info.columns else 0, 
                axis=1
            )
            
            # 合併解鎖進度評分到因子評分表
            factor_scores = pd.merge(
                factor_scores, 
                latest_token_info[['symbol', 'unlock_progress_score']], 
                on='symbol', 
                how='left'
            )
            
            # 填充缺失值
            factor_scores['unlock_progress_score'] = factor_scores['unlock_progress_score'].fillna(0)
            
            logger.info("完成解鎖進度分析")
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析解鎖進度因子失敗: {str(e)}")
            factor_scores['unlock_progress_score'] = 0
            return factor_scores
    
    def _analyze_sector(self, factor_scores, token_info):
        """
        分析代幣所屬賽道
        
        策略要求：賽道篩選，不空DeFi
        
        Args:
            factor_scores: 因子評分DataFrame
            token_info: 代幣信息數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if token_info.empty:
                logger.warning("無代幣信息數據，跳過賽道分析")
                factor_scores['sector_score'] = 0
                return factor_scores
            
            # 獲取最新的代幣信息
            latest_token_info = token_info.sort_values('updated_at', ascending=False)
            latest_token_info = latest_token_info.drop_duplicates('symbol', keep='first')
            
            # 計算賽道評分
            excluded_sectors = self.strategy_config['excluded_sectors']
            target_sectors = self.strategy_config['target_sectors']
            
            def calculate_sector_score(row):
                sector = row.get('sector', '')
                if not sector:
                    return 0.5  # 如果無賽道信息，給予中等評分
                
                if sector in excluded_sectors:
                    return 0  # 排除的賽道
                    
                if sector in target_sectors:
                    return 1  # 目標賽道
                    
                return 0.5  # 其他賽道
            
            latest_token_info['sector_score'] = latest_token_info.apply(calculate_sector_score, axis=1)
            
            # 合併賽道評分到因子評分表
            factor_scores = pd.merge(
                factor_scores, 
                latest_token_info[['symbol', 'sector_score']], 
                on='symbol', 
                how='left'
            )
            
            # 填充缺失值
            factor_scores['sector_score'] = factor_scores['sector_score'].fillna(0.5)
            
            logger.info(f"完成賽道分析，有 {(factor_scores['sector_score'] == 0).sum()} 個交易對屬於排除賽道")
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析賽道因子失敗: {str(e)}")
            factor_scores['sector_score'] = 0.5
            return factor_scores
    
    def _calculate_total_score(self, factor_scores):
        """
        計算綜合因子評分
        
        Args:
            factor_scores: 因子評分DataFrame
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            # 綜合評分 = 各項因子的加權平均
            # 資金費率因子是強制要求，必須符合要求才能有總分
            # 這裡我們給資金費率一個較高的權重
            
            # 確保所有需要的列都存在
            required_columns = ['funding_rate_score', 'liquidity_score', 'market_cap_score', 
                               'pump_pattern_score', 'unlock_progress_score', 'sector_score']
            
            for col in required_columns:
                if col not in factor_scores.columns:
                    factor_scores[col] = 0
            
            # 計算總分
            factor_scores['total_score'] = (
                factor_scores['funding_rate_score'] * 0.4 +
                factor_scores['liquidity_score'] * 0.3 +
                factor_scores['market_cap_score'] * 0.1 +
                factor_scores['pump_pattern_score'] * 0.1 +
                factor_scores['unlock_progress_score'] * 0.05 +
                factor_scores['sector_score'] * 0.05
            )
            
            # 如果資金費率不符合要求，總分為0
            factor_scores.loc[factor_scores['funding_rate_score'] == 0, 'total_score'] = 0
            
            # 過濾出符合最低資金費率要求的交易對
            qualified_symbols = factor_scores[factor_scores['funding_rate_score'] > 0]['symbol'].tolist()
            
            # 添加详细日志输出
            for idx, row in factor_scores.iterrows():
                logger.info(f"因子评分详情 - 交易对: {row['symbol']} - "
                           f"总分: {row['total_score']:.2f}, "
                           f"资金费率分数: {row['funding_rate_score']:.2f}, "
                           f"资金费率: {row.get('funding_rate', 0):.6f}, "
                           f"流动性分数: {row['liquidity_score']:.2f}, "
                           f"市值分数: {row['market_cap_score']:.2f}, "
                           f"拉盘模式分数: {row['pump_pattern_score']:.2f}")
            
            logger.info(f"完成綜合因子評分計算，有 {len(qualified_symbols)} 個交易對符合資金費率要求")
            return factor_scores
            
        except Exception as e:
            logger.error(f"計算綜合因子評分失敗: {str(e)}")
            factor_scores['total_score'] = 0
            return factor_scores
