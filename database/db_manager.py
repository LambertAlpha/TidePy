"""
數據庫管理模組 - 負責數據庫連接和數據存儲
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, text
from sqlalchemy.exc import SQLAlchemyError
import redis
import config
from datetime import datetime

logger = logging.getLogger(__name__)


class DBManager:
    """數據庫管理器，負責處理數據庫連接和數據存儲"""
    
    def __init__(self):
        """初始化數據庫連接"""
        self.db_config = config.DB_CONFIG
        self.redis_config = config.REDIS_CONFIG
        self._initialize_connections()
        self._initialize_tables()
        
    def _initialize_connections(self):
        """初始化資料庫連接"""
        try:
            # PostgreSQL連接
            conn_str = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(conn_str)
            self.metadata = MetaData()
            
            # Redis連接
            self.redis_client = redis.Redis(
                host=self.redis_config['host'],
                port=self.redis_config['port'],
                db=self.redis_config['db']
            )
            logger.info("數據庫連接初始化成功")
            
        except Exception as e:
            logger.error(f"初始化數據庫連接失敗: {str(e)}")
            raise
            
    def _initialize_tables(self):
        """初始化資料庫表結構"""
        try:
            # 市場數據表
            self.market_data_table = Table(
                'market_data', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('symbol', String(50), nullable=False),
                Column('timestamp', DateTime, nullable=False),
                Column('last_price', Float),
                Column('bid', Float),
                Column('ask', Float),
                Column('volume_24h', Float),
                Column('price_change_24h', Float),
                Column('best_bid_size', Float),
                Column('best_ask_size', Float),
                Column('orderbook_depth', Integer),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            
            # 資金費率表
            self.funding_rate_table = Table(
                'funding_rate', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('symbol', String(50), nullable=False),
                Column('timestamp', DateTime, nullable=False),
                Column('funding_rate', Float),
                Column('next_funding_time', DateTime),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            
            # 代幣信息表
            self.token_info_table = Table(
                'token_info', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('symbol', String(50), nullable=False),
                Column('name', String(100)),
                Column('market_cap', Float),
                Column('circulating_supply', Float),
                Column('total_supply', Float),
                Column('sector', String(50)),
                Column('unlock_progress', Float),
                Column('updated_at', DateTime, nullable=False),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            
            # 交易信號表
            self.trade_signal_table = Table(
                'trade_signal', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('symbol', String(50), nullable=False),
                Column('timestamp', DateTime, nullable=False),
                Column('signal_type', String(20)),  # buy, sell, close
                Column('price', Float),
                Column('quantity', Float),
                Column('reason', String(200)),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            
            # 倉位表
            self.position_table = Table(
                'position', self.metadata,
                Column('id', Integer, primary_key=True),
                Column('symbol', String(50), nullable=False),
                Column('direction', String(10)),  # long, short
                Column('open_time', DateTime),
                Column('open_price', Float),
                Column('current_price', Float),
                Column('quantity', Float),
                Column('pnl', Float),
                Column('pnl_percentage', Float),
                Column('status', String(20)),  # open, closed
                Column('updated_at', DateTime),
                Column('created_at', DateTime, default=datetime.utcnow)
            )
            
            # 創建表（如果不存在）
            self.metadata.create_all(self.engine)
            logger.info("數據庫表結構初始化成功")
            
        except SQLAlchemyError as e:
            logger.error(f"初始化數據庫表結構失敗: {str(e)}")
            raise
    
    def save_market_data(self, market_data_df):
        """
        保存市場數據到數據庫
        
        Args:
            market_data_df: 包含市場數據的DataFrame
        """
        try:
            if market_data_df.empty:
                return
                
            # 轉換時間戳為datetime對象
            market_data_df['timestamp'] = pd.to_datetime(market_data_df['timestamp'], unit='ms')
            
            # 使用to_sql方法將DataFrame寫入數據庫
            market_data_df.to_sql(
                'market_data', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"成功保存了 {len(market_data_df)} 條市場數據")
            
            # 將最新數據緩存到Redis（以便快速訪問）
            for _, row in market_data_df.iterrows():
                key = f"market:{row['symbol']}"
                self.redis_client.hmset(key, {
                    'last_price': row['last_price'],
                    'bid': row['bid'],
                    'ask': row['ask'],
                    'volume_24h': row['volume_24h'],
                    'timestamp': int(row['timestamp'].timestamp())
                })
                # 設置過期時間（1小時）
                self.redis_client.expire(key, 3600)
                
        except Exception as e:
            logger.error(f"保存市場數據失敗: {str(e)}")
    
    def save_funding_data(self, funding_df):
        """
        保存資金費率數據到數據庫
        
        Args:
            funding_df: 包含資金費率的DataFrame
        """
        try:
            if funding_df.empty:
                return
                
            # 轉換時間戳為datetime對象
            funding_df['timestamp'] = pd.to_datetime(funding_df['timestamp'], unit='ms')
            if 'next_funding_time' in funding_df.columns:
                funding_df['next_funding_time'] = pd.to_datetime(funding_df['next_funding_time'], unit='ms')
            
            # 使用to_sql方法將DataFrame寫入數據庫
            funding_df.to_sql(
                'funding_rate', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"成功保存了 {len(funding_df)} 條資金費率數據")
            
            # 將最新數據緩存到Redis（以便快速訪問）
            for _, row in funding_df.iterrows():
                key = f"funding:{row['symbol']}"
                self.redis_client.hmset(key, {
                    'funding_rate': row['funding_rate'],
                    'timestamp': int(row['timestamp'].timestamp())
                })
                # 設置過期時間（8小時）
                self.redis_client.expire(key, 8 * 3600)
                
        except Exception as e:
            logger.error(f"保存資金費率數據失敗: {str(e)}")
    
    def save_token_info(self, token_info_df):
        """
        保存代幣信息到數據庫
        
        Args:
            token_info_df: 包含代幣信息的DataFrame
        """
        try:
            if token_info_df.empty:
                return
                
            # 確保有updated_at列
            if 'updated_at' not in token_info_df.columns:
                token_info_df['updated_at'] = datetime.utcnow()
            
            # 使用to_sql方法將DataFrame寫入數據庫
            token_info_df.to_sql(
                'token_info', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"成功保存了 {len(token_info_df)} 條代幣信息")
                
        except Exception as e:
            logger.error(f"保存代幣信息失敗: {str(e)}")
    
    def get_market_data(self, symbol=None, limit=100):
        """
        從數據庫獲取市場數據
        
        Args:
            symbol: 交易對名稱，如果為None則獲取所有交易對
            limit: 返回的記錄數限制
            
        Returns:
            DataFrame: 包含市場數據的DataFrame
        """
        try:
            # 首先嘗試從Redis獲取最新數據
            if symbol is not None:
                key = f"market:{symbol}"
                cached_data = self.redis_client.hgetall(key)
                if cached_data:
                    # 如果緩存存在，直接返回
                    return pd.DataFrame([{
                        'symbol': symbol,
                        'last_price': float(cached_data[b'last_price']),
                        'bid': float(cached_data[b'bid']),
                        'ask': float(cached_data[b'ask']),
                        'volume_24h': float(cached_data[b'volume_24h']),
                        'timestamp': datetime.fromtimestamp(int(cached_data[b'timestamp']))
                    }])
            
            # 構建SQL查詢
            query = "SELECT * FROM market_data"
            if symbol is not None:
                query += f" WHERE symbol = '{symbol}'"
            query += f" ORDER BY timestamp DESC LIMIT {limit}"
            
            # 執行查詢並返回DataFrame
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            return df
            
        except Exception as e:
            logger.error(f"獲取市場數據失敗: {str(e)}")
            return pd.DataFrame()
    
    def get_funding_rate(self, symbol=None, limit=100):
        """
        從數據庫獲取資金費率數據
        
        Args:
            symbol: 交易對名稱，如果為None則獲取所有交易對
            limit: 返回的記錄數限制
            
        Returns:
            DataFrame: 包含資金費率的DataFrame
        """
        try:
            # 首先嘗試從Redis獲取最新數據
            if symbol is not None:
                key = f"funding:{symbol}"
                cached_data = self.redis_client.hgetall(key)
                if cached_data:
                    # 如果緩存存在，直接返回
                    return pd.DataFrame([{
                        'symbol': symbol,
                        'funding_rate': float(cached_data[b'funding_rate']),
                        'timestamp': datetime.fromtimestamp(int(cached_data[b'timestamp']))
                    }])
            
            # 構建SQL查詢
            query = "SELECT * FROM funding_rate"
            if symbol is not None:
                query += f" WHERE symbol = '{symbol}'"
            query += f" ORDER BY timestamp DESC LIMIT {limit}"
            
            # 執行查詢並返回DataFrame
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            return df
            
        except Exception as e:
            logger.error(f"獲取資金費率數據失敗: {str(e)}")
            return pd.DataFrame()
    
    def get_token_info(self, symbol=None):
        """
        從數據庫獲取代幣信息
        
        Args:
            symbol: 交易對名稱，如果為None則獲取所有代幣
            
        Returns:
            DataFrame: 包含代幣信息的DataFrame
        """
        try:
            # 構建SQL查詢
            query = "SELECT * FROM token_info"
            if symbol is not None:
                query += f" WHERE symbol = '{symbol}'"
            query += " ORDER BY updated_at DESC"
            
            # 執行查詢並返回DataFrame
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            return df
            
        except Exception as e:
            logger.error(f"獲取代幣信息失敗: {str(e)}")
            return pd.DataFrame()
            
    def save_trade_signal(self, signal_df):
        """
        保存交易信號到數據庫
        
        Args:
            signal_df: 包含交易信號的DataFrame
        """
        try:
            if signal_df.empty:
                return
                
            # 確保timestamp為datetime格式
            if 'timestamp' in signal_df.columns and not pd.api.types.is_datetime64_any_dtype(signal_df['timestamp']):
                signal_df['timestamp'] = pd.to_datetime(signal_df['timestamp'])
            
            # 使用to_sql方法將DataFrame寫入數據庫
            signal_df.to_sql(
                'trade_signal', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"成功保存了 {len(signal_df)} 條交易信號")
                
        except Exception as e:
            logger.error(f"保存交易信號失敗: {str(e)}")
            
    def save_position(self, position_df):
        """
        保存倉位數據到數據庫
        
        Args:
            position_df: 包含倉位數據的DataFrame
        """
        try:
            if position_df.empty:
                return
                
            # 確保時間列為datetime格式
            time_columns = ['open_time', 'updated_at']
            for col in time_columns:
                if col in position_df.columns and not pd.api.types.is_datetime64_any_dtype(position_df[col]):
                    position_df[col] = pd.to_datetime(position_df[col])
            
            # 添加更新時間
            if 'updated_at' not in position_df.columns:
                position_df['updated_at'] = datetime.utcnow()
            
            # 使用to_sql方法將DataFrame寫入數據庫
            position_df.to_sql(
                'position', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info(f"成功保存了 {len(position_df)} 條倉位數據")
                
        except Exception as e:
            logger.error(f"保存倉位數據失敗: {str(e)}")
            
    def update_position(self, position_id, update_data):
        """
        更新倉位數據
        
        Args:
            position_id: 倉位ID
            update_data: 需要更新的數據字典
        """
        try:
            # 添加更新時間
            update_data['updated_at'] = datetime.utcnow()
            
            # 構建更新SQL
            update_items = [f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" for k, v in update_data.items()]
            update_sql = f"UPDATE position SET {', '.join(update_items)} WHERE id = {position_id}"
            
            # 執行更新
            with self.engine.connect() as connection:
                connection.execute(text(update_sql))
                
            logger.info(f"成功更新了倉位 {position_id} 的數據")
                
        except Exception as e:
            logger.error(f"更新倉位數據失敗: {str(e)}")
            
    def get_open_positions(self):
        """
        獲取所有開倉的倉位
        
        Returns:
            DataFrame: 包含開倉倉位的DataFrame
        """
        try:
            # 構建SQL查詢
            query = "SELECT * FROM position WHERE status = 'open' ORDER BY open_time DESC"
            
            # 執行查詢並返回DataFrame
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            
            return df
            
        except Exception as e:
            logger.error(f"獲取開倉倉位失敗: {str(e)}")
            return pd.DataFrame()
