"""
數據採集層 - 負責收集市場數據、資金費率、代幣信息和流動性分析
"""

import logging
import ccxt
import pandas as pd
import numpy as np
import time
from database.db_manager import DBManager
import config

logger = logging.getLogger(__name__)


class DataCollector:
    """數據收集器，負責從交易所和其他數據源收集交易數據"""
    
    def __init__(self):
        """初始化數據收集器"""
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
                'options': {
                    'defaultType': 'future',  # 设置默认市场类型为期货
                }
            })
            logger.info(f"成功初始化交易所: {self.exchange_config['name']}")
            return exchange
        except Exception as e:
            logger.error(f"初始化交易所失敗: {str(e)}")
            raise
    
    def get_available_symbols(self, quote_currency='USDT', limit=200):
        """
        獲取可用的交易對
        
        Args:
            quote_currency: 計價幣種，例如 'USDT'
            limit: 返回的最大交易對數量
            
        Returns:
            list: 交易對符號列表
        """
        try:
            markets = self.exchange.fetch_markets()
            symbols = []
            
            for market in markets:
                # 只獲取現貨交易對且計價幣種匹配的
                if (not market.get('future', False) and 
                    not market.get('swap', False) and 
                    market['symbol'].endswith(f'/{quote_currency}')):
                    symbols.append(market['symbol'])
                    
                    # 限制返回的數量
                    if len(symbols) >= limit:
                        break
            
            logger.info(f"成功獲取 {len(symbols)} 個 {quote_currency} 交易對")
            return symbols
            
        except Exception as e:
            logger.error(f"獲取交易對列表失敗: {str(e)}")
            return []
    
    def get_available_futures(self, quote_currency='USDT', limit=200):
        """
        獲取可用的永續合約交易對
        
        Args:
            quote_currency: 計價幣種，例如 'USDT'
            limit: 返回的最大交易對數量
            
        Returns:
            list: 交易對符號列表
        """
        try:
            # 设置市场类型为期货
            self.exchange.options['defaultType'] = 'future'
            
            markets = self.exchange.fetch_markets()
            symbols = []
            
            for market in markets:
                # 只獲取永續合約交易對(linear 类型,即USDT本位合约)
                if (market.get('linear', False) and 
                    market.get('quote', '') == quote_currency and
                    not market.get('spot', False) and
                    market.get('active', False)):  # 确保合约是活跃的
                    
                    symbols.append(market['symbol'])
                    
                    # 限制返回的數量
                    if len(symbols) >= limit:
                        break
            
            logger.info(f"成功獲取 {len(symbols)} 個 {quote_currency} 永續合約交易對")
            return symbols
            
        except Exception as e:
            logger.error(f"獲取永續合約交易對列表失敗: {str(e)}")
            return []
    
    def collect_market_data(self, symbols=None):
        """
        收集市場數據
        
        Args:
            symbols: 需要收集數據的交易對列表，如果為None則收集所有交易對
            
        Returns:
            DataFrame: 包含市場數據的DataFrame
        """
        try:
            # 确保设置市场类型为spot
            self.exchange.options['defaultType'] = 'spot'
            
            if symbols is None:
                # 如果未指定交易對，則獲取所有可交易的USDT交易對
                symbols = self.get_available_symbols(quote_currency='USDT')
                
            market_data = []
            
            # 记录不可用的交易对
            unavailable_symbols = []
            
            for symbol in symbols:
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1d', limit=1)
                    orderbook = self.exchange.fetch_order_book(symbol, limit=20)
                    
                    if ohlcv and len(ohlcv) > 0:
                        data = {
                            'symbol': symbol,
                            'timestamp': ticker['timestamp'],
                            'last_price': ticker['last'],
                            'bid': ticker['bid'] if 'bid' in ticker else None,
                            'ask': ticker['ask'] if 'ask' in ticker else None,
                            'volume_24h': ticker['quoteVolume'] if 'quoteVolume' in ticker else None,
                            'price_change_24h': ticker['percentage'] if 'percentage' in ticker else None,
                            'best_bid_size': orderbook['bids'][0][1] if len(orderbook['bids']) > 0 else 0,
                            'best_ask_size': orderbook['asks'][0][1] if len(orderbook['asks']) > 0 else 0,
                            'orderbook_depth': len(orderbook['bids']) + len(orderbook['asks'])
                        }
                        market_data.append(data)
                except Exception as e:
                    unavailable_symbols.append(symbol)
                    logger.warning(f"获取交易对 {symbol} 的市场数据失败: {str(e)}")
                    continue
            
            if unavailable_symbols:
                logger.warning(f"以下 {len(unavailable_symbols)} 个交易对不可用或无法获取数据: {', '.join(unavailable_symbols[:10])}{' 等...' if len(unavailable_symbols) > 10 else ''}")
            
            if not market_data:
                logger.error("所有交易对都无法获取市场数据")
                return pd.DataFrame()
                
            df = pd.DataFrame(market_data)
            
            # 保存到數據庫
            if not df.empty:
                self.db_manager.save_market_data(df)
                logger.info(f"成功收集了 {len(df)} 個交易對的市場數據")
            
            return df
        
        except Exception as e:
            logger.error(f"收集市場數據失敗: {str(e)}")
            return pd.DataFrame()
    
    def fetch_funding_rate(self, symbols=None):
        """
        獲取資金費率
        
        Args:
            symbols: 需要獲取資金費率的交易對列表，如果為None則獲取所有交易對
            
        Returns:
            DataFrame: 包含資金費率的DataFrame
        """
        try:
            # 确保设置市场类型为期货
            self.exchange.options['defaultType'] = 'future'
            
            funding_data = []
            
            if symbols is None:
                # 如果未指定交易對，則獲取所有可交易的期貨交易對
                symbols = self.get_available_futures(quote_currency='USDT')
            
            logger.info(f"开始获取 {len(symbols)} 个交易对的资金费率")
            
            # 首先為所有交易對創建一個基本記錄，資金費率設為NA
            for symbol in symbols:
                data = {
                    'symbol': symbol,
                    'timestamp': int(time.time() * 1000),  # 當前時間戳
                    'funding_rate': np.nan,  # 使用 NaN 表示 NA
                    'next_funding_time': None,
                }
                funding_data.append(data)
            
            # 然後嘗試從交易所獲取實際的資金費率
            fetched_symbols = set()
            for symbol in symbols:
                try:
                    # 确保使用期货市场
                    funding_info = self.exchange.fetch_funding_rate(symbol)
                    
                    # 更新已有的記錄
                    for item in funding_data:
                        if item['symbol'] == symbol:
                            item.update({
                                'timestamp': funding_info['timestamp'],
                                'funding_rate': funding_info['fundingRate'],
                                'next_funding_time': funding_info.get('nextFundingTime', None),
                            })
                            fetched_symbols.add(symbol)
                            logger.info(f"成功获取 {symbol} 的资金费率: {funding_info['fundingRate']}")
                            break
                except Exception as e:
                    logger.warning(f"獲取 {symbol} 的資金費率失敗: {str(e)}")
            
            logger.info(f"成功獲取 {len(fetched_symbols)} 個交易對的資金費率，{len(symbols) - len(fetched_symbols)} 個交易對的資金費率為 NA")
            
            df = pd.DataFrame(funding_data)
            
            # 保存到數據庫
            if not df.empty:
                self.db_manager.save_funding_data(df)
                logger.info(f"成功收集了 {len(df)} 個交易對的資金費率數據")
            
            return df
            
        except Exception as e:
            logger.error(f"獲取資金費率失敗: {str(e)}")
            return pd.DataFrame()
    
    def get_token_info(self, symbols=None):
        """
        獲取代幣基本信息（流通量、解鎖進度等）
        使用CoinMarketCap API获取代币信息，包括解锁进度、供应量和赛道分类
        
        Args:
            symbols: 需要獲取信息的交易對列表
            
        Returns:
            DataFrame: 包含代幣信息的DataFrame，包括：
                - symbol: 代币符号
                - name: 代币名称
                - total_supply: 总供应量
                - circulating_supply: 流通量
                - unlock_progress: 解锁进度 (0-100%)
                - next_unlock_date: 下次解锁日期
                - next_unlock_amount: 下次解锁数量
                - sector: 代币所属赛道/类别
                - updated_at: 数据更新时间戳
        """
        try:
            import os
            import requests
            from datetime import datetime
            import time
            import json
            from requests.adapters import HTTPAdapter
            from requests.packages.urllib3.util.retry import Retry
            
            # 获取CMC API密钥
            api_key = os.environ.get('CMC_API_KEY')
            if not api_key:
                logger.error("缺少CoinMarketCap API密钥，请在.env文件中设置CMC_API_KEY")
                return pd.DataFrame()
            
            # 如果没有指定symbols，则返回空DataFrame
            if not symbols:
                logger.warning("未指定交易對列表，無法獲取代幣信息")
                return pd.DataFrame()
            
            # 创建具有重试功能的session
            session = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                respect_retry_after_header=True
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # 准备结果列表和API请求头
            token_info_list = []
            headers = {
                'X-CMC_PRO_API_KEY': api_key,
                'Accept': 'application/json',
                'Accept-Encoding': 'deflate, gzip'
            }
            
            # 提取基础货币符号（去除/USDT等后缀）
            base_symbols = [symbol.split('/')[0] for symbol in symbols]
            
            # 批量请求ID映射 - 最多100个符号一次
            all_ids = {}
            all_names = {}
            all_slugs = {}
            
            # 分批处理符号，每批最多100个
            batch_size = 100
            for i in range(0, len(base_symbols), batch_size):
                batch_symbols = base_symbols[i:i+batch_size]
                
                # 步骤1：批量获取货币ID
                logger.info(f"批量请求CoinMarketCap获取{len(batch_symbols)}个符号的ID映射")
                map_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
                map_params = {
                    'symbol': ','.join(batch_symbols)
                }
                
                try:
                    map_response = session.get(map_url, params=map_params, headers=headers)
                    map_data = map_response.json()
                    
                    if map_response.status_code != 200:
                        error_code = map_data.get('status', {}).get('error_code', 0)
                        error_message = map_data.get('status', {}).get('error_message', 'Unknown error')
                        logger.warning(f"批量获取符号ID失败，状态码: {map_response.status_code}, 错误码: {error_code}, 错误信息: {error_message}")
                        continue
                    
                    if 'data' not in map_data:
                        logger.warning(f"批量获取符号ID返回数据格式异常")
                        continue
                    
                    # 处理返回的数据，按符号分组
                    for item in map_data['data']:
                        symbol = item['symbol']
                        # 使用市值最高的币种（默认排序）
                        if symbol not in all_ids:
                            all_ids[symbol] = item['id']
                            all_names[symbol] = item['name']
                            all_slugs[symbol] = item['slug']
                    
                    # 避免API限流
                    time.sleep(1.0)
                    
                except Exception as e:
                    logger.error(f"批量获取符号ID时发生错误: {str(e)}")
                    continue
            
            # 分批获取代币详细信息
            batch_ids = []
            id_to_symbols = {}
            
            # 准备ID批次
            for symbol, cmc_id in all_ids.items():
                batch_ids.append(str(cmc_id))
                id_to_symbols[str(cmc_id)] = symbol
            
            # 每批处理最多100个ID
            info_results = {}
            quotes_results = {}
            
            for i in range(0, len(batch_ids), batch_size):
                batch_chunk = batch_ids[i:i+batch_size]
                
                # 步骤2：批量获取代币详细信息
                logger.info(f"批量请求CoinMarketCap获取{len(batch_chunk)}个代币的详细信息")
                info_url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
                info_params = {
                    'id': ','.join(batch_chunk),
                    'aux': 'urls,logo,description,tags,platform,date_added,notice,status'
                }
                
                try:
                    info_response = session.get(info_url, params=info_params, headers=headers)
                    info_data = info_response.json()
                    
                    if info_response.status_code != 200:
                        error_code = info_data.get('status', {}).get('error_code', 0)
                        error_message = info_data.get('status', {}).get('error_message', 'Unknown error')
                        logger.warning(f"批量获取代币详细信息失败，状态码: {info_response.status_code}, 错误码: {error_code}, 错误信息: {error_message}")
                    else:
                        if 'data' in info_data:
                            info_results.update(info_data['data'])
                    
                    # 避免API限流
                    time.sleep(1.0)
                    
                except Exception as e:
                    logger.error(f"批量获取代币详细信息时发生错误: {str(e)}")
                
                # 步骤3：批量获取代币报价信息（包含供应量数据）
                logger.info(f"批量请求CoinMarketCap获取{len(batch_chunk)}个代币的报价信息")
                quotes_url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"
                quotes_params = {
                    'id': ','.join(batch_chunk),
                    'aux': 'circulating_supply,total_supply,max_supply'
                }
                
                try:
                    quotes_response = session.get(quotes_url, params=quotes_params, headers=headers)
                    quotes_data = quotes_response.json()
                    
                    if quotes_response.status_code != 200:
                        error_code = quotes_data.get('status', {}).get('error_code', 0)
                        error_message = quotes_data.get('status', {}).get('error_message', 'Unknown error')
                        logger.warning(f"批量获取代币报价信息失败，状态码: {quotes_response.status_code}, 错误码: {error_code}, 错误信息: {error_message}")
                    else:
                        if 'data' in quotes_data:
                            quotes_results.update(quotes_data['data'])
                    
                    # 避免API限流
                    time.sleep(1.0)
                    
                except Exception as e:
                    logger.error(f"批量获取代币报价信息时发生错误: {str(e)}")
                    
            # 逐个获取解锁进度信息（目前API不支持批量获取解锁信息）
            for symbol in base_symbols:
                try:
                    if symbol not in all_ids:
                        logger.warning(f"无法为{symbol}找到对应的CoinMarketCap ID")
                        continue
                    
                    cmc_id = all_ids[symbol]
                    name = all_names.get(symbol, symbol)
                    
                    # 获取解锁信息
                    unlock_progress = 0.0
                    next_unlock_date = None
                    next_unlock_amount = 0.0
                    
                    # 获取详细信息
                    total_supply = 0.0
                    circulating_supply = 0.0
                    sector = ""
                    market_cap = 0.0
                    
                    # 从详细信息中获取数据
                    str_id = str(cmc_id)
                    
                    # 从info_results获取代币基本信息
                    if str_id in info_results:
                        token_data = info_results[str_id]
                        # 获取代币类别/赛道
                        if 'category' in token_data:
                            sector = token_data['category']
                        elif 'tags' in token_data and token_data['tags']:
                            sector = token_data['tags'][0]  # 使用第一个标签作为赛道
                    
                    # 从quotes_results获取准确的供应量数据
                    if str_id in quotes_results:
                        quotes_data = quotes_results[str_id]
                        total_supply = quotes_data.get('total_supply', 0.0) or 0.0
                        circulating_supply = quotes_data.get('circulating_supply', 0.0) or 0.0
                        max_supply = quotes_data.get('max_supply', 0.0) or 0.0
                        market_cap = quotes_data.get('quote', {}).get('USD', {}).get('market_cap', 0.0) or 0.0
                    
                    # 尝试获取解锁进度信息
                    try:
                        unlocks_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/unlock-calendar"
                        unlocks_params = {'id': cmc_id}
                        
                        logger.info(f"请求CoinMarketCap获取{symbol}的解锁信息 (ID: {cmc_id})")
                        unlocks_response = session.get(unlocks_url, params=unlocks_params, headers=headers)
                        
                        # 检查响应状态码
                        if unlocks_response.status_code == 200:
                            unlocks_data = unlocks_response.json()
                            if 'data' in unlocks_data and 'tokenUnlockEvents' in unlocks_data['data']:
                                # 排序解锁事件
                                events = unlocks_data['data']['tokenUnlockEvents']
                                now = datetime.now()
                                
                                # 计算已解锁比例
                                total_unlocked = sum(event['percentage'] for event in events if datetime.fromtimestamp(event['date']/1000) < now)
                                unlock_progress = min(100.0, total_unlocked)
                                
                                # 查找下一个解锁事件
                                future_events = [event for event in events if datetime.fromtimestamp(event['date']/1000) > now]
                                future_events.sort(key=lambda x: x['date'])
                                
                                if future_events:
                                    next_event = future_events[0]
                                    next_unlock_date = datetime.fromtimestamp(next_event['date']/1000).isoformat()
                                    next_unlock_amount = next_event['tokenAmount']
                        elif unlocks_response.status_code == 404:
                            # 没有解锁日历是正常情况，对一些币种来说
                            logger.info(f"{symbol}没有解锁日历信息 (404 Not Found)")
                        else:
                            # 处理其他错误
                            unlocks_data = unlocks_response.json()
                            error_code = unlocks_data.get('status', {}).get('error_code', 0)
                            error_message = unlocks_data.get('status', {}).get('error_message', 'Unknown error')
                            logger.warning(f"获取{symbol}的解锁信息失败，状态码: {unlocks_response.status_code}, 错误码: {error_code}, 错误信息: {error_message}")
                    
                    except Exception as e:
                        logger.warning(f"获取{symbol}的解锁信息时发生错误: {str(e)}")
                    
                    # 避免API限流
                    time.sleep(0.5)
                    
                    # 获取原始交易对符号，以匹配返回结果
                    original_symbol = next((s for s in symbols if s.split('/')[0] == symbol), f"{symbol}/USDT")
                    
                    # 添加到结果列表
                    token_info_list.append({
                        'symbol': original_symbol,
                        'name': name,
                        'total_supply': total_supply,
                        'circulating_supply': circulating_supply,
                        'max_supply': max_supply if 'max_supply' in locals() else 0.0,
                        'market_cap': market_cap if 'market_cap' in locals() else 0.0,
                        'unlock_progress': unlock_progress,
                        'next_unlock_date': next_unlock_date,
                        'next_unlock_amount': next_unlock_amount,
                        'sector': sector,
                        'updated_at': pd.Timestamp.utcnow()
                    })
                    
                except Exception as e:
                    logger.error(f"处理{symbol}的代币信息时发生错误: {str(e)}")
            
            # 创建DataFrame
            result_df = pd.DataFrame(token_info_list)
            
            # 确保DataFrame不为空并包含所有必要的列
            if not result_df.empty:
                required_columns = ['symbol', 'name', 'total_supply', 'circulating_supply', 
                                    'unlock_progress', 'next_unlock_date', 'next_unlock_amount', 
                                    'sector', 'updated_at']
                for col in required_columns:
                    if col not in result_df.columns:
                        result_df[col] = None
            
            logger.info(f"成功获取{len(result_df)}个代币的信息")
            return result_df
            
        except Exception as e:
            logger.error(f"获取代币信息失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def analyze_liquidity(self, market_data=None):
        """
        分析幣種流動性
        
        Args:
            market_data: 市場數據DataFrame，如果為None則重新獲取
            
        Returns:
            DataFrame: 包含流動性分析結果的DataFrame
        """
        try:
            if market_data is None or market_data.empty:
                market_data = self.collect_market_data()
                
            if market_data.empty:
                return pd.DataFrame()
            
            # 計算流動性指標
            liquidity_data = market_data.copy()
            
            # 計算買賣盤深度比率
            liquidity_data['bid_ask_ratio'] = liquidity_data['best_bid_size'] / liquidity_data['best_ask_size']
            
            # 計算美元成交量（假設最後價格為美元單位）
            liquidity_data['usd_volume'] = liquidity_data['last_price'] * liquidity_data['volume_24h']
            
            # 計算相對流動性分數（簡單示例）
            max_volume = liquidity_data['usd_volume'].max()
            if max_volume > 0:
                liquidity_data['liquidity_score'] = liquidity_data['usd_volume'] / max_volume
            else:
                liquidity_data['liquidity_score'] = 0
                
            # 篩選符合最低流動性要求的幣種
            min_liquidity = config.STRATEGY_CONFIG['min_liquidity']
            filtered_data = liquidity_data[liquidity_data['usd_volume'] >= min_liquidity]
            
            logger.info(f"完成流動性分析，有 {len(filtered_data)} 個交易對符合最低流動性要求")
            return filtered_data
            
        except Exception as e:
            logger.error(f"分析流動性失敗: {str(e)}")
            return pd.DataFrame()
