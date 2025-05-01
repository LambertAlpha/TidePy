"""
數據採集層 - 負責收集市場數據、資金費率、代幣信息和流動性分析
"""

import logging
import ccxt
import pandas as pd
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
            })
            logger.info(f"成功初始化交易所: {self.exchange_config['name']}")
            return exchange
        except Exception as e:
            logger.error(f"初始化交易所失敗: {str(e)}")
            raise
    
    def get_available_markets(self, quote_currency='USDT', limit=200):
        """
        獲取可用的交易對
        
        Args:
            quote_currency: 計價幣種，例如 'USDT'
            limit: 返回的最大交易對數量
            
        Returns:
            list: 交易對符號列表
        """
        try:
            # 獲取所有市場
            markets = self.exchange.fetch_markets()
            
            # 篩選出符合計價幣種的現貨交易對
            spot_markets = [market['symbol'] for market in markets 
                          if (not market.get('future', False) 
                              and not market.get('swap', False)
                              and market['symbol'].endswith(f'/{quote_currency}'))]
            
            # 限制返回數量並日誌記錄
            result = spot_markets[:limit]
            logger.info(f"獲取到 {len(result)} 個 {quote_currency} 計價的交易對")
            return result
            
        except Exception as e:
            logger.error(f"獲取可用交易對失敗: {str(e)}")
            return []
    
    def collect_market_data(self, symbols=None):
        """
        採集市場價格、深度數據
        
        Args:
            symbols: 需要收集數據的交易對列表，如果為None則收集所有交易對
            
        Returns:
            DataFrame: 包含市場數據的DataFrame
        """
        try:
            if symbols is None:
                # 如果未指定交易對，則獲取所有可交易的期貨交易對
                markets = self.exchange.fetch_markets()
                symbols = [market['symbol'] for market in markets if 
                           market.get('future', False) or market.get('swap', False)]
            
            market_data = []
            for symbol in symbols:
                ticker = self.exchange.fetch_ticker(symbol)
                orderbook = self.exchange.fetch_order_book(symbol, limit=20)
                
                data = {
                    'symbol': symbol,
                    'timestamp': ticker['timestamp'],
                    'last_price': ticker['last'],
                    'bid': ticker['bid'],
                    'ask': ticker['ask'],
                    'volume_24h': ticker['quoteVolume'],
                    'price_change_24h': ticker['percentage'],
                    'best_bid_size': orderbook['bids'][0][1] if len(orderbook['bids']) > 0 else 0,
                    'best_ask_size': orderbook['asks'][0][1] if len(orderbook['asks']) > 0 else 0,
                    'orderbook_depth': len(orderbook['bids']) + len(orderbook['asks'])
                }
                market_data.append(data)
                
            df = pd.DataFrame(market_data)
            
            # 保存到數據庫
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
            funding_data = []
            
            if symbols is None:
                # 如果未指定交易對，則獲取所有可交易的期貨交易對
                markets = self.exchange.fetch_markets()
                symbols = [market['symbol'] for market in markets if 
                          market.get('future', False) or market.get('swap', False)]
            
            for symbol in symbols:
                try:
                    funding_info = self.exchange.fetch_funding_rate(symbol)
                    data = {
                        'symbol': symbol,
                        'timestamp': funding_info['timestamp'],
                        'funding_rate': funding_info['fundingRate'],
                        'next_funding_time': funding_info.get('nextFundingTime', None),
                    }
                    funding_data.append(data)
                except Exception as e:
                    logger.warning(f"獲取 {symbol} 的資金費率失敗: {str(e)}")
            
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
        
        这个功能可能需要从其他API获取，如CoinMarketCap, CoinGecko等
        
        Args:
            symbols: 需要獲取信息的交易對列表
            
        Returns:
            DataFrame: 包含代幣信息的DataFrame
        """
        # 这里需要实现通过第三方API获取代币信息的功能
        # 由于CCXT不提供这些信息，此处为示例结构
        logger.info("獲取代幣信息功能需從第三方API實現")
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
