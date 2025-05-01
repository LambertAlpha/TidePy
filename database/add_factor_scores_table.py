"""
數據庫遷移腳本 - 添加 factor_scores 表來存儲因子評分結果
"""

import logging
import os
import sys
import psycopg2
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


def add_factor_scores_table():
    """
    創建因子評分表，用於存儲策略因子計算結果
    """
    try:
        # 從環境變數獲取數據庫配置
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        # 直接使用正確的用戶名，不從環境變數讀取
        db_user = 'lambertlin'  # 強制使用正確的用戶名
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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS factor_scores (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    total_score FLOAT,
                    funding_rate_score FLOAT,
                    funding_rate FLOAT,
                    liquidity_score FLOAT,
                    market_cap_score FLOAT,
                    pump_pattern_score FLOAT,
                    unlock_progress_score FLOAT,
                    sector_score FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
        conn.commit()
        logger.info("成功創建因子評分表 factor_scores")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"創建因子評分表失敗: {str(e)}")
        return False


if __name__ == "__main__":
    add_factor_scores_table()
