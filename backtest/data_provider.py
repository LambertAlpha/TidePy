"""
历史数据提供者 - 负责获取和准备回测所需的历史数据
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import sqlite3
from tqdm import tqdm

from database.db_manager import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger('backtest')

class HistoricalDataProvider:
    """提供历史数据用于回测"""
    
    def __init__(self, data_source='database'):
        """
        初始化历史数据提供者
        
        Args:
            data_source (str): 数据来源，可选 'database'(数据库), 'csv'(CSV文件), 'api'(API)
        """
        self.data_source = data_source
        self.db_manager = DatabaseManager()
        
        # 缓存数据
        self.cached_market_data = None
        self.cached_funding_data = None
        self.cached_token_info = None
        
        logger.info(f"初始化历史数据提供者，数据来源: {data_source}")
    
    def get_market_data(self, start_date, end_date, symbols=None):
        """
        获取历史市场数据
        
        Args:
            start_date (datetime): 开始日期
            end_date (datetime): 结束日期
            symbols (list, optional): 交易对列表，如果为None则获取所有交易对
            
        Returns:
            DataFrame: 历史市场数据
        """
        logger.info(f"获取历史市场数据: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        
        if self.data_source == 'database':
            # 从数据库获取数据
            market_data = self._get_market_data_from_db(start_date, end_date, symbols)
        elif self.data_source == 'csv':
            # 从CSV文件获取数据
            market_data = self._get_market_data_from_csv(start_date, end_date, symbols)
        elif self.data_source == 'api':
            # 从API获取数据
            market_data = self._get_market_data_from_api(start_date, end_date, symbols)
        else:
            logger.error(f"不支持的数据来源: {self.data_source}")
            market_data = pd.DataFrame()
        
        # 缓存数据
        self.cached_market_data = market_data
        
        logger.info(f"获取到 {len(market_data)} 条历史市场数据记录")
        return market_data
    
    def get_funding_data(self, start_date, end_date, symbols=None):
        """
        获取历史资金费率数据
        
        Args:
            start_date (datetime): 开始日期
            end_date (datetime): 结束日期
            symbols (list, optional): 交易对列表，如果为None则获取所有交易对
            
        Returns:
            DataFrame: 历史资金费率数据
        """
        logger.info(f"获取历史资金费率数据: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        
        if self.data_source == 'database':
            # 从数据库获取数据
            funding_data = self._get_funding_data_from_db(start_date, end_date, symbols)
        elif self.data_source == 'csv':
            # 从CSV文件获取数据
            funding_data = self._get_funding_data_from_csv(start_date, end_date, symbols)
        elif self.data_source == 'api':
            # 从API获取数据
            funding_data = self._get_funding_data_from_api(start_date, end_date, symbols)
        else:
            logger.error(f"不支持的数据来源: {self.data_source}")
            funding_data = pd.DataFrame()
        
        # 缓存数据
        self.cached_funding_data = funding_data
        
        logger.info(f"获取到 {len(funding_data)} 条历史资金费率数据记录")
        return funding_data
    
    def get_token_info(self, symbols=None):
        """
        获取代币信息
        
        Args:
            symbols (list, optional): 交易对列表，如果为None则获取所有交易对
            
        Returns:
            DataFrame: 代币信息数据
        """
        logger.info("获取代币信息")
        
        if self.data_source == 'database':
            # 从数据库获取数据
            token_info = self._get_token_info_from_db(symbols)
        elif self.data_source == 'csv':
            # 从CSV文件获取数据
            token_info = self._get_token_info_from_csv(symbols)
        elif self.data_source == 'api':
            # 从API获取数据
            token_info = self._get_token_info_from_api(symbols)
        else:
            logger.error(f"不支持的数据来源: {self.data_source}")
            token_info = pd.DataFrame()
        
        # 缓存数据
        self.cached_token_info = token_info
        
        logger.info(f"获取到 {len(token_info)} 条代币信息记录")
        return token_info
    
    def _get_market_data_from_db(self, start_date, end_date, symbols=None):
        """从数据库获取市场数据"""
        try:
            # 构建SQL查询
            query = """
            SELECT * FROM market_data
            WHERE timestamp BETWEEN %s AND %s
            """
            params = [start_date, end_date]
            
            if symbols and len(symbols) > 0:
                placeholders = ', '.join(['%s'] * len(symbols))
                query += f" AND symbol IN ({placeholders})"
                params.extend(symbols)
            
            # 执行查询
            conn = self.db_manager.get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            
            # 确保时间戳是datetime类型
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
        except Exception as e:
            logger.error(f"从数据库获取市场数据失败: {str(e)}")
            # 如果无法从数据库获取，则生成模拟数据用于测试
            return self._generate_mock_market_data(start_date, end_date, symbols)
    
    def _get_funding_data_from_db(self, start_date, end_date, symbols=None):
        """从数据库获取资金费率数据"""
        try:
            # 构建SQL查询
            query = """
            SELECT * FROM funding_rate
            WHERE timestamp BETWEEN %s AND %s
            """
            params = [start_date, end_date]
            
            if symbols and len(symbols) > 0:
                placeholders = ', '.join(['%s'] * len(symbols))
                query += f" AND symbol IN ({placeholders})"
                params.extend(symbols)
            
            # 执行查询
            conn = self.db_manager.get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            
            # 确保时间戳是datetime类型
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            return df
            
        except Exception as e:
            logger.error(f"从数据库获取资金费率数据失败: {str(e)}")
            # 如果无法从数据库获取，则生成模拟数据用于测试
            return self._generate_mock_funding_data(start_date, end_date, symbols)
    
    def _get_token_info_from_db(self, symbols=None):
        """从数据库获取代币信息"""
        try:
            # 构建SQL查询
            query = "SELECT * FROM token_info"
            params = []
            
            if symbols and len(symbols) > 0:
                placeholders = ', '.join(['%s'] * len(symbols))
                query += f" WHERE symbol IN ({placeholders})"
                params.extend(symbols)
            
            # 执行查询
            conn = self.db_manager.get_connection()
            df = pd.read_sql_query(query, conn, params=params)
            
            return df
            
        except Exception as e:
            logger.error(f"从数据库获取代币信息失败: {str(e)}")
            # 如果无法从数据库获取，则生成模拟数据用于测试
            return self._generate_mock_token_info(symbols)
    
    def _get_market_data_from_csv(self, start_date, end_date, symbols=None):
        """从CSV文件获取市场数据"""
        # TODO: 实现从CSV文件读取数据的逻辑
        logger.warning("从CSV文件获取市场数据功能尚未实现，返回模拟数据")
        return self._generate_mock_market_data(start_date, end_date, symbols)
    
    def _get_funding_data_from_csv(self, start_date, end_date, symbols=None):
        """从CSV文件获取资金费率数据"""
        # TODO: 实现从CSV文件读取数据的逻辑
        logger.warning("从CSV文件获取资金费率数据功能尚未实现，返回模拟数据")
        return self._generate_mock_funding_data(start_date, end_date, symbols)
    
    def _get_token_info_from_csv(self, symbols=None):
        """从CSV文件获取代币信息"""
        # TODO: 实现从CSV文件读取数据的逻辑
        logger.warning("从CSV文件获取代币信息功能尚未实现，返回模拟数据")
        return self._generate_mock_token_info(symbols)
    
    def _get_market_data_from_api(self, start_date, end_date, symbols=None):
        """从API获取市场数据"""
        # TODO: 实现从API获取数据的逻辑
        logger.warning("从API获取市场数据功能尚未实现，返回模拟数据")
        return self._generate_mock_market_data(start_date, end_date, symbols)
    
    def _get_funding_data_from_api(self, start_date, end_date, symbols=None):
        """从API获取资金费率数据"""
        # TODO: 实现从API获取数据的逻辑
        logger.warning("从API获取资金费率数据功能尚未实现，返回模拟数据")
        return self._generate_mock_funding_data(start_date, end_date, symbols)
    
    def _get_token_info_from_api(self, symbols=None):
        """从API获取代币信息"""
        # TODO: 实现从API获取数据的逻辑
        logger.warning("从API获取代币信息功能尚未实现，返回模拟数据")
        return self._generate_mock_token_info(symbols)
    
    def _generate_mock_market_data(self, start_date, end_date, symbols=None):
        """生成模拟市场数据用于测试"""
        if not symbols:
            symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'DOGE/USDT']
        
        # 生成日期范围
        date_range = pd.date_range(start=start_date, end=end_date, freq='1H')
        
        # 生成模拟数据
        data = []
        for symbol in symbols:
            # 为每个交易对设置一个基础价格
            if symbol == 'BTC/USDT':
                base_price = 50000
            elif symbol == 'ETH/USDT':
                base_price = 3000
            elif symbol == 'SOL/USDT':
                base_price = 150
            elif symbol == 'BNB/USDT':
                base_price = 400
            elif symbol == 'DOGE/USDT':
                base_price = 0.1
            else:
                base_price = 100
            
            # 为每个交易对生成模拟价格数据
            for date in date_range:
                # 添加一些随机波动
                random_factor = np.random.normal(1, 0.01)
                price = base_price * random_factor
                
                # 生成OHLCV数据
                row = {
                    'timestamp': date,
                    'symbol': symbol,
                    'open': price * np.random.normal(1, 0.005),
                    'high': price * np.random.normal(1.01, 0.005),
                    'low': price * np.random.normal(0.99, 0.005),
                    'close': price,
                    'volume': np.random.normal(1000000, 500000)
                }
                data.append(row)
                
                # 更新基础价格，模拟价格变动趋势
                base_price = price * np.random.normal(1, 0.005)
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        return df
    
    def _generate_mock_funding_data(self, start_date, end_date, symbols=None):
        """生成模拟资金费率数据用于测试"""
        if not symbols:
            symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'DOGE/USDT']
        
        # 生成日期范围 (每8小时一次资金费率)
        date_range = pd.date_range(start=start_date, end=end_date, freq='8H')
        
        # 生成模拟数据
        data = []
        for symbol in symbols:
            for date in date_range:
                # 生成随机资金费率 (-0.1% 到 0.1%)
                rate = np.random.uniform(-0.001, 0.001)
                
                row = {
                    'timestamp': date,
                    'symbol': symbol,
                    'funding_rate': rate
                }
                data.append(row)
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        return df
    
    def _generate_mock_token_info(self, symbols=None):
        """生成模拟代币信息用于测试"""
        if not symbols:
            symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'DOGE/USDT']
        
        # 预设一些模拟数据
        token_data = {
            'BTC/USDT': {
                'market_cap': 1000000000000,
                'circulating_supply': 19000000,
                'total_supply': 21000000,
                'category': 'Coin',
                'liquidity': 'high'
            },
            'ETH/USDT': {
                'market_cap': 500000000000,
                'circulating_supply': 120000000,
                'total_supply': 120000000,
                'category': 'Platform',
                'liquidity': 'high'
            },
            'SOL/USDT': {
                'market_cap': 50000000000,
                'circulating_supply': 300000000,
                'total_supply': 500000000,
                'category': 'Platform',
                'liquidity': 'high'
            },
            'BNB/USDT': {
                'market_cap': 70000000000,
                'circulating_supply': 150000000,
                'total_supply': 200000000,
                'category': 'Exchange',
                'liquidity': 'high'
            },
            'DOGE/USDT': {
                'market_cap': 20000000000,
                'circulating_supply': 130000000000,
                'total_supply': 130000000000,
                'category': 'Meme',
                'liquidity': 'high'
            }
        }
        
        # 生成数据
        data = []
        for symbol in symbols:
            if symbol in token_data:
                info = token_data[symbol]
            else:
                # 对于未预设的交易对，生成随机数据
                info = {
                    'market_cap': np.random.uniform(1000000, 10000000000),
                    'circulating_supply': np.random.uniform(1000000, 1000000000),
                    'total_supply': np.random.uniform(1000000, 1000000000),
                    'category': np.random.choice(['Coin', 'Platform', 'Meme', 'DeFi', 'NFT', 'GameFi']),
                    'liquidity': np.random.choice(['low', 'medium', 'high'])
                }
            
            # 补充通用字段
            info['symbol'] = symbol
            data.append(info)
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        return df
