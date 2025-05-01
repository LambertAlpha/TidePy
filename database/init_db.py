"""
數據庫初始化腳本 - 創建必要的數據庫表結構
"""

import logging
import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# 添加項目根目錄到Python路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 載入環境變數
load_dotenv()

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def create_database():
    """
    創建數據庫（如果不存在）
    """
    try:
        # 從環境變數獲取數據庫配置
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'password')
        db_name = os.getenv('DB_NAME', 'tidepy')
        
        # 連接到PostgreSQL服務器
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # 檢查數據庫是否存在
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cursor.fetchone()
            
            if not exists:
                logger.info(f"創建數據庫 {db_name}")
                cursor.execute(f"CREATE DATABASE {db_name}")
                logger.info(f"數據庫 {db_name} 創建成功")
            else:
                logger.info(f"數據庫 {db_name} 已存在")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"創建數據庫失敗: {str(e)}")
        return False


def create_tables():
    """
    創建必要的數據表
    """
    try:
        # 從環境變數獲取數據庫配置
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'password')
        db_name = os.getenv('DB_NAME', 'tidepy')
        
        # 連接到數據庫
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            dbname=db_name
        )
        
        # 創建表
        with conn.cursor() as cursor:
            # 市場數據表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    last_price FLOAT,
                    bid FLOAT,
                    ask FLOAT,
                    volume_24h FLOAT,
                    price_change_24h FLOAT,
                    best_bid_size FLOAT,
                    best_ask_size FLOAT,
                    orderbook_depth INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 資金費率表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS funding_rate (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    funding_rate FLOAT,
                    next_funding_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 代幣信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS token_info (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    name VARCHAR(100),
                    market_cap FLOAT,
                    circulating_supply FLOAT,
                    total_supply FLOAT,
                    sector VARCHAR(50),
                    unlock_progress FLOAT,
                    updated_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 交易信號表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trade_signal (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    signal_type VARCHAR(20),
                    price FLOAT,
                    quantity FLOAT,
                    reason VARCHAR(200),
                    score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 倉位表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS position (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    direction VARCHAR(10),
                    open_time TIMESTAMP,
                    open_price FLOAT,
                    current_price FLOAT,
                    quantity FLOAT,
                    pnl FLOAT,
                    pnl_percentage FLOAT,
                    status VARCHAR(20),
                    updated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 訂單表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_record (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    order_type VARCHAR(20),
                    side VARCHAR(10),
                    price FLOAT,
                    amount FLOAT,
                    filled FLOAT,
                    status VARCHAR(20),
                    signal_id INTEGER,
                    error TEXT,
                    order_time TIMESTAMP,
                    updated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 指數表（用於性能評估）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    total_pnl FLOAT,
                    pnl_change FLOAT,
                    open_positions INTEGER,
                    win_count INTEGER,
                    loss_count INTEGER,
                    win_rate FLOAT,
                    max_drawdown FLOAT,
                    sharpe_ratio FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        # 提交事務
        conn.commit()
        conn.close()
        
        logger.info("數據表創建成功")
        return True
        
    except Exception as e:
        logger.error(f"創建數據表失敗: {str(e)}")
        return False


def main():
    """
    主函數
    """
    logger.info("開始初始化數據庫")
    
    # 創建數據庫
    if not create_database():
        logger.error("數據庫創建失敗，初始化中斷")
        return
    
    # 創建表
    if not create_tables():
        logger.error("數據表創建失敗，初始化中斷")
        return
    
    logger.info("數據庫初始化完成")


if __name__ == "__main__":
    main()
