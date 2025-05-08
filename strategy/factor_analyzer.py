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
            
            # 4. 市值分析
            factor_scores = self._analyze_market_cap(factor_scores, market_data)
            
            # 5. 解鎖進度評估
            if not token_info.empty:
                factor_scores = self._analyze_unlock_progress(factor_scores, token_info)
            
            # 6. 賽道分類
            if not token_info.empty:
                factor_scores = self._analyze_sector(factor_scores, token_info)
            
            # 計算綜合因子評分
            factor_scores = self._calculate_total_score(factor_scores)
            
            # 將因子評分存儲到數據庫中和本地文件
            self._save_factor_scores(factor_scores)
            
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
            
            # 处理币安永续合约符号格式
            # 在资金费率数据中，币安的永续合约符号格式是'BTC/USDT:USDT'
            # 在因子评分数据中，我们使用的是'BTC/USDT'
            # 需要转换以确保正确匹配
            logger.info(f"處理資金費率數據: {funding_data.shape[0]} 條記錄")
            
            # 输出资金费率数据示例，便于调试
            if not funding_data.empty:
                logger.info(f"资金费率数据示例: {funding_data.iloc[0].to_dict()}")
                logger.info(f"资金费率数据列: {funding_data.columns.tolist()}")
            
            funding_data_processed = funding_data.copy()
            funding_data_processed['base_symbol'] = funding_data_processed['symbol'].apply(
                lambda x: x.split(':')[0] if ':' in x else x
            )
            
            # 输出转换后的示例
            if not funding_data_processed.empty:
                sample = funding_data_processed.iloc[0]
                logger.info(f"转换后资金费率数据示例: 原符号={sample['symbol']}, 基础符号={sample['base_symbol']}")
            
            # 检查是否有非NA的资金费率数据
            valid_funding_rates = funding_data_processed[~funding_data_processed['funding_rate'].isna()]
            if valid_funding_rates.empty:
                logger.warning("所有資金費率數據都為NA，跳過資金費率分析")
                factor_scores['funding_rate_score'] = 0
                factor_scores['funding_rate'] = 0
                return factor_scores
            
            logger.info(f"有 {len(valid_funding_rates)} 个交易对有有效的资金费率数据")
            
            # 输出因子评分数据示例
            if not factor_scores.empty:
                logger.info(f"因子评分数据示例: {factor_scores.iloc[0].to_dict()}")
                logger.info(f"因子评分数据列: {factor_scores.columns.tolist()}")
                logger.info(f"因子评分数据符号示例: {factor_scores['symbol'].iloc[0]}")
            
            # 获取每个交易对的最新资金费率
            latest_funding = funding_data_processed.sort_values('timestamp', ascending=False)
            latest_funding = latest_funding.drop_duplicates('base_symbol', keep='first')
            
            # 输出有效的资金费率数据，便于检查
            '''for idx, row in latest_funding.iterrows():
                if not np.isnan(row['funding_rate']):
                    logger.info(f"交易对 {row['symbol']} (基础符号: {row['base_symbol']}) 的资金费率为 {row['funding_rate']}")'''
            
            # 合并资金费率数据到因子评分表
            logger.info(f"开始合并资金费率数据，因子评分行数: {len(factor_scores)}, 资金费率行数: {len(latest_funding)}")
            
            # 保存合并前的数据副本，用于比较
            factor_scores_before = factor_scores.copy()
            
            # 合并数据，使用left_on和right_on确保正确匹配
            merged_df = pd.merge(
                factor_scores,
                latest_funding[['base_symbol', 'funding_rate']],
                left_on='symbol',
                right_on='base_symbol',
                how='left'
            )
            
            # 检查合并结果
            logger.info(f"合并后数据行数: {len(merged_df)}, 列: {merged_df.columns.tolist()}")
            logger.info(f"合并前后行数差异: {len(merged_df) - len(factor_scores)}")
            
            # 如果合并导致数据丢失，使用交叉匹配
            if len(merged_df) < len(factor_scores):
                logger.warning("合并导致数据丢失，尝试手动匹配...")
                # 创建映射字典
                funding_map = {row['base_symbol']: row['funding_rate'] for _, row in latest_funding.iterrows()}
                
                # 手动匹配
                factor_scores['funding_rate'] = factor_scores['symbol'].apply(
                    lambda x: funding_map.get(x, 0)
                )
                logger.info(f"手动匹配后资金费率非零项: {(factor_scores['funding_rate'] != 0).sum()}")
            else:
                # 使用合并结果
                factor_scores = merged_df
                # 确保funding_rate存在
                if 'funding_rate' not in factor_scores.columns and 'funding_rate_y' in factor_scores.columns:
                    factor_scores['funding_rate'] = factor_scores['funding_rate_y']
                    factor_scores = factor_scores.drop('funding_rate_y', axis=1)
                if 'funding_rate_x' in factor_scores.columns:
                    factor_scores = factor_scores.drop('funding_rate_x', axis=1)
            
            # 检查数据是否成功合并
            non_zero_count = (factor_scores['funding_rate'] != 0).sum()
            logger.info(f"资金费率非零值数量: {non_zero_count}")
            
            # 填充可能的NA值
            factor_scores['funding_rate'] = factor_scores['funding_rate'].fillna(0)
            
            # 计算资金费率评分
            min_funding_rate = self.strategy_config['min_funding_rate']
            factor_scores['funding_rate_score'] = factor_scores['funding_rate'].apply(
                lambda x: 1 if x >= min_funding_rate else 0
            )
            
            # 标记资金费率为0的交易对
            zero_rates = (factor_scores['funding_rate'] == 0).sum()
            if zero_rates > 0:
                logger.info(f"有 {zero_rates} 个交易对的资金费率为0，将不参与交易")
                factor_scores.loc[factor_scores['funding_rate'] == 0, 'funding_rate_score'] = -1
            
            # 对得分不为0的标的分数进行正则化
            valid_scores = factor_scores['funding_rate_score'] > 0
            if valid_scores.sum() > 0:
                max_funding = factor_scores.loc[valid_scores, 'funding_rate'].max()
                min_funding = factor_scores.loc[valid_scores, 'funding_rate'].min()
                if max_funding > min_funding:
                    factor_scores.loc[valid_scores, 'funding_rate_score'] = (
                        (factor_scores.loc[valid_scores, 'funding_rate'] - min_funding) / 
                        (max_funding - min_funding)
                    )
            
            qualified_count = (factor_scores['funding_rate_score'] > 0).sum()
            na_count = (factor_scores['funding_rate_score'] == -1).sum()
            logger.info(f"完成資金費率分析，有 {qualified_count} 個交易對的資金費率符合要求，{na_count} 個交易對的資金費率為NA或0")
            # 删除临时列
            if 'base_symbol' in factor_scores.columns:
                factor_scores = factor_scores.drop('base_symbol', axis=1)
            
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析資金費率因子失敗: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
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
                return factor_scores
            
            # 檢查市場數據列名
            logger.info(f"流動性分析 - 市場數據列名: {market_data.columns.tolist()}")
            
            # 确保必要的列存在
            price_col = None
            if 'last_price' in market_data.columns:
                price_col = 'last_price'
            elif 'last' in market_data.columns:
                price_col = 'last'
            elif 'close' in market_data.columns:
                price_col = 'close'
            
            volume_col = None
            if 'volume_24h' in market_data.columns:
                volume_col = 'volume_24h'
            elif 'quoteVolume' in market_data.columns:
                volume_col = 'quoteVolume'
            elif 'baseVolume' in market_data.columns:
                volume_col = 'baseVolume'
            
            if not price_col or not volume_col:
                logger.error(f"流動性分析 - 無法找到必要的市場數據列: 價格列={price_col}, 成交量列={volume_col}")
                factor_scores['liquidity_score'] = 0
                return factor_scores
            
            # 獲取最新的市場數據
            latest_market = market_data.sort_values('timestamp', ascending=False)
            latest_market = latest_market.drop_duplicates('symbol', keep='first')
            
            # 添加日成交額 (turnover)列
            latest_market['turnover'] = latest_market[price_col] * latest_market[volume_col]
            
            # 將流動性數據合並到因子評分表
            market_liquidity = latest_market[['symbol', 'turnover', price_col, volume_col]]
            factor_scores = pd.merge(factor_scores, market_liquidity, on='symbol', how='left')
            
            # 列印每個交易對的流動性數據，以便調試
            for idx, row in factor_scores.iterrows():
                logger.info(f"流動性數據 - 交易對: {row['symbol']}, "
                          f"價格: {row.get(price_col, 0)}, "
                          f"成交量: {row.get(volume_col, 0)}, "
                          f"成交額: {row.get('turnover', 0)}")
            
            # 計算流動性分數 - 基於日成交額的歸一化評分
            min_turnover = 1000000  # 最低日成交額：100萬美元
            
            # 先標記不符合最低要求的交易對
            factor_scores['liquidity_score'] = 0
            
            # 對於符合最低要求的交易對，進行歸一化評分
            qualified_symbols = factor_scores[factor_scores['turnover'] >= min_turnover].index
            if len(qualified_symbols) > 0:
                max_turnover = factor_scores.loc[qualified_symbols, 'turnover'].max()
                if max_turnover > 0:  # 防止除以零
                    factor_scores.loc[qualified_symbols, 'liquidity_score'] = factor_scores.loc[qualified_symbols, 'turnover'] / max_turnover
            
            # 檢查是否成功計算了流動性分數
            if 'liquidity_score' not in factor_scores.columns:
                logger.error("流動性分數計算失敗，列不存在")
                factor_scores['liquidity_score'] = 0
            
            # 輸出分析結果
            liquid_count = (factor_scores['liquidity_score'] > 0).sum()
            logger.info(f"完成流動性分析，有 {liquid_count} 個交易對流動性足夠")
            
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析流動性因子失敗: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            factor_scores['liquidity_score'] = 0
            return factor_scores
    
    def _analyze_pump_patterns(self, factor_scores, market_data):
        """
        分析拉盤模式因子
        是否拉過盤？ 問題是，如何定量判斷是否拉過盤？
        輸出是0或0.5（拉過一次）1（拉過兩次以上？）

        能不能將現在價格回溯到這個幣幣安合約後的第二天收盤價或者一個月前的價格對比？
        
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
            
            # 檢查市場數據列名
            logger.info(f"拉盤模式分析 - 市場數據列名: {market_data.columns.tolist()}")
            
            # 确保必要的列存在
            price_col = None
            if 'last_price' in market_data.columns:
                price_col = 'last_price'
            elif 'last' in market_data.columns:
                price_col = 'last'
            elif 'close' in market_data.columns:
                price_col = 'close'
            
            volume_col = None
            if 'volume_24h' in market_data.columns:
                volume_col = 'volume_24h'
            elif 'quoteVolume' in market_data.columns:
                volume_col = 'quoteVolume'
            elif 'baseVolume' in market_data.columns:
                volume_col = 'baseVolume'
            
            if not price_col or not volume_col:
                logger.error(f"拉盤模式分析 - 無法找到必要的市場數據列: 價格列={price_col}, 成交量列={volume_col}")
                factor_scores['pump_pattern_score'] = 0
                return factor_scores
            
            # 获取最新市场数据
            all_symbols = factor_scores['symbol'].unique().tolist()
            logger.info(f"需要分析拉盘模式的交易对数量: {len(all_symbols)}")
            
            # 创建结果列表
            pump_results = []
            
            for symbol in all_symbols:
                try:
                    # 获取该交易对的市场数据
                    symbol_data = market_data[market_data['symbol'] == symbol].sort_values('timestamp')
                    
                    if len(symbol_data) < 2:
                        logger.warning(f"交易对 {symbol} 的市场数据不足，无法分析拉盘模式")
                        pump_results.append({
                            'symbol': symbol,
                            'pump_pattern_score': 0
                        })
                        continue
                    
                    # 获取最新和前一个时间点的数据
                    latest = symbol_data.iloc[-1]
                    previous = symbol_data.iloc[-2]
                    
                    # 计算价格变化率和成交量变化率
                    price_change = 0
                    volume_change = 0
                    
                    if previous[price_col] > 0:
                        price_change = (latest[price_col] - previous[price_col]) / previous[price_col]
                    
                    if previous[volume_col] > 0:
                        volume_change = (latest[volume_col] - previous[volume_col]) / previous[volume_col]
                    
                    # 判断是否是拉盘模式：价格上涨超过10%且成交量上涨超过100%
                    is_pump = price_change > 0.1 and volume_change > 1.0
                    
                    logger.info(f"交易对 {symbol} 拉盘分析结果: "
                               f"价格变化率={price_change:.2f}, "
                               f"成交量变化率={volume_change:.2f}, "
                               f"是否拉盘={is_pump}")
                    
                    pump_results.append({
                        'symbol': symbol,
                        'pump_pattern_score': 1 if is_pump else 0
                    })
                    
                except Exception as e:
                    logger.warning(f"分析交易对 {symbol} 拉盘模式时发生错误: {str(e)}")
                    pump_results.append({
                        'symbol': symbol,
                        'pump_pattern_score': 0
                    })
            
            # 创建拉盘模式数据框
            pump_df = pd.DataFrame(pump_results)
            
            # 合并到因子评分表
            factor_scores = pd.merge(
                factor_scores,
                pump_df,
                on='symbol',
                how='left'
            )
            
            # 填充缺失值
            factor_scores['pump_pattern_score'] = factor_scores['pump_pattern_score'].fillna(0)
            
            # 检查是否成功计算了拉盘模式分数
            if 'pump_pattern_score' not in factor_scores.columns:
                logger.error("拉盘模式分数计算失败，列不存在")
                factor_scores['pump_pattern_score'] = 0
            
            # 输出分析结果
            pump_count = (factor_scores['pump_pattern_score'] > 0).sum()
            logger.info(f"完成拉盘模式分析，有 {pump_count} 个交易对符合拉盘模式")
            
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析拉盘模式因子失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            factor_scores['pump_pattern_score'] = 0
            return factor_scores
    
    def _analyze_market_cap(self, factor_scores, market_data):
        """
        分析市值因子
        
        Args:
            factor_scores: 因子評分DataFrame
            market_data: 市場數據
            
        Returns:
            DataFrame: 更新後的因子評分
        """
        try:
            if market_data.empty:
                logger.warning("無市場數據，跳過市值分析")
                factor_scores['market_cap_score'] = 0
                return factor_scores
            
            # 檢查市場數據列名
            logger.info(f"市值分析 - 市場數據列名: {market_data.columns.tolist()}")
            
            # 使用CoinGecko API獲取准確市值
            import requests
            import time
            
            # 獲取所有交易對的符號
            symbols = factor_scores['symbol'].unique().tolist()
            market_caps = {}
            
            # CoinGecko API 的币种ID映射
            # 有些币的ID和币名不一致，需要特殊处理
            coingecko_id_map = {
                'btc': 'bitcoin',
                'eth': 'ethereum',
                'bnb': 'binancecoin',
                'ada': 'cardano',
                'xrp': 'ripple',
                'doge': 'dogecoin',
                'sol': 'solana',
                'om': 'mantra-dao',  # 注意，这需要根据实际情况调整
                # 添加更多映射...
            }
            
            for symbol in symbols:
                try:
                    # 提取幣種名稱（去掉/USDT等後綴）
                    base_symbol = symbol.split('/')[0].lower()
                    
                    # 使用映射表获取正确的CoinGecko ID
                    coin_id = coingecko_id_map.get(base_symbol, base_symbol)
                    
                    # 使用CoinGecko API獲取市值數據
                    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
                    logger.info(f"請求CoinGecko API獲取{base_symbol}的市值數據 (ID: {coin_id}): {url}")
                    
                    response = requests.get(url)
                    if response.status_code == 200:
                        coin_data = response.json()
                        if 'market_data' in coin_data and 'market_cap' in coin_data['market_data']:
                            market_cap = coin_data['market_data']['market_cap'].get('usd', 0)
                            market_caps[symbol] = market_cap
                            logger.info(f"獲取到{symbol}的市值: ${market_cap:,.2f}")
                        else:
                            logger.warning(f"無法獲取{symbol}的市值數據: API返回數據結構不符")
                            # 使用交易額作為備用
                            if 'turnover' in factor_scores.columns:
                                symbol_data = factor_scores[factor_scores['symbol'] == symbol]
                                if not symbol_data.empty and pd.notna(symbol_data['turnover'].values[0]):
                                    estimated_cap = symbol_data['turnover'].values[0] * 10
                                    market_caps[symbol] = estimated_cap
                                    logger.info(f"使用估算的{symbol}市值: ${estimated_cap:,.2f}")
                    elif response.status_code == 429:  # Too Many Requests
                        logger.warning(f"CoinGecko API限流，等待1秒后重試")
                        time.sleep(1)
                        # 使用交易額作為備用
                        if 'turnover' in factor_scores.columns:
                            symbol_data = factor_scores[factor_scores['symbol'] == symbol]
                            if not symbol_data.empty and pd.notna(symbol_data['turnover'].values[0]):
                                estimated_cap = symbol_data['turnover'].values[0] * 10
                                market_caps[symbol] = estimated_cap
                                logger.info(f"由於API限流，使用估算的{symbol}市值: ${estimated_cap:,.2f}")
                    else:
                        logger.warning(f"無法獲取{symbol}的市值數據: HTTP狀態碼 {response.status_code}")
                        # 使用交易額作為備用
                        if 'turnover' in factor_scores.columns:
                            symbol_data = factor_scores[factor_scores['symbol'] == symbol]
                            if not symbol_data.empty and pd.notna(symbol_data['turnover'].values[0]):
                                estimated_cap = symbol_data['turnover'].values[0] * 10
                                market_caps[symbol] = estimated_cap
                                logger.info(f"由於API錯誤，使用估算的{symbol}市值: ${estimated_cap:,.2f}")
                    
                    # 避免API限流，適當延遲
                    time.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"獲取{symbol}市值時發生錯誤: {str(e)}")
                    # 使用交易額作為備用
                    if 'turnover' in factor_scores.columns:
                        symbol_data = factor_scores[factor_scores['symbol'] == symbol]
                        if not symbol_data.empty and pd.notna(symbol_data['turnover'].values[0]):
                            estimated_cap = symbol_data['turnover'].values[0] * 10
                            market_caps[symbol] = estimated_cap
                            logger.info(f"由於異常，使用估算的{symbol}市值: ${estimated_cap:,.2f}")
            
            # 創建市值DataFrame
            market_cap_df = pd.DataFrame([
                {'symbol': symbol, 'market_cap': cap}
                for symbol, cap in market_caps.items()
            ])
            
            # 合併市值數據到因子評分表
            if not market_cap_df.empty:
                factor_scores = pd.merge(
                    factor_scores,
                    market_cap_df,
                    on='symbol',
                    how='left'
                )
            else:
                logger.warning("無法從CoinGecko獲取市值數據，將使用備用方法")
                if 'turnover' in factor_scores.columns:
                    factor_scores['market_cap'] = factor_scores['turnover'] * 10
                else:
                    logger.error("無法估算市值，因為turnover列不存在")
                    factor_scores['market_cap'] = 0
            
            # 填充缺失值
            factor_scores['market_cap'] = factor_scores['market_cap'].fillna(0)
            
            # 計算市值分數：優先選擇小市值幣種
            max_market_cap = 1000000000  # 10億美元
            factor_scores['market_cap_score'] = factor_scores['market_cap'].apply(
                lambda x: 1 if pd.notna(x) and x > 0 and x < max_market_cap else 0.2  # 小市值得高分，大市值得低分
            )
            
            # 檢查是否成功計算了市值分數
            if 'market_cap_score' not in factor_scores.columns:
                logger.error("市值分數計算失敗，列不存在")
                factor_scores['market_cap_score'] = 0
            
            # 輸出分析結果
            small_cap_count = (factor_scores['market_cap_score'] >= 1).sum()
            large_cap_count = (factor_scores['market_cap_score'] < 1).sum()
            logger.info(f"完成市值分析，有 {small_cap_count} 個小市值交易對，{large_cap_count} 個大市值交易對")
            
            return factor_scores
            
        except Exception as e:
            logger.error(f"分析市值因子失敗: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            factor_scores['market_cap_score'] = 0
            return factor_scores
    
    def _analyze_unlock_progress(self, factor_scores, token_info):
        """
        分析代幣解鎖進度
        
        策略要求：考慮解鎖進度和流通的籌碼

        解鎖進度如何量化？
        
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

        哪裏可以獲取賽道數據？
        
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
                factor_scores['liquidity_score'] * 0.2 +
                factor_scores['market_cap_score'] * 0.2 +
                factor_scores['pump_pattern_score'] * 0.2 +  # 拉盤模式的權重
                factor_scores['unlock_progress_score'] * 0.4  # 解鎖進度的權重
                #factor_scores['sector_score'] * 0.0  # 賽道的權重
            )
            
            # 資金費率是NA（-1）的交易對總分設為0，不參與交易
            na_count = (factor_scores['funding_rate_score'] == -1).sum()
            if na_count > 0:
                factor_scores.loc[factor_scores['funding_rate_score'] == -1, 'total_score'] = 0
                logger.info(f"有 {na_count} 個交易對的資金費率為NA，已設置總分為0，將不參與交易")
            
            # 如果資金費率不符合要求，總分為0
            no_funding_count = (factor_scores['funding_rate_score'] == 0).sum()
            if no_funding_count > 0:
                factor_scores.loc[factor_scores['funding_rate_score'] == 0, 'total_score'] = 0
                logger.info(f"有 {no_funding_count} 個交易對的資金費率不符合要求，已設置總分為0")
            
            # 過濾出符合條件的交易對（資金費率不是NA且符合要求）
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
    
    def _save_factor_scores(self, factor_scores):
        """
        將因子評分存儲到數據庫中和本地文件
        
        Args:
            factor_scores: 因子評分DataFrame
        """
        try:
            # 保存到數據庫
            self.db_manager.save_factor_scores(factor_scores)
            logger.info("成功將因子評分存儲到數據庫中")
            
            # 保存到本地文件
            import os
            import pytz
            from datetime import datetime
            
            # 創建data目錄（如果不存在）
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # 獲取中國時區的當前時間
            china_tz = pytz.timezone('Asia/Shanghai')
            current_time = datetime.now(china_tz)
            timestamp = current_time.strftime('%Y%m%d_%H%M%S')
            
            # 構建文件名
            filename = os.path.join(data_dir, f'factor_scores_{timestamp}.csv')
            
            # 保存到CSV
            factor_scores.to_csv(filename, index=False)
            logger.info(f"成功將因子評分保存到本地文件: {filename}")
            
        except Exception as e:
            logger.error(f"存儲因子評分失敗: {str(e)}")
