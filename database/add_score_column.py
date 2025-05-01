"""
數據庫遷移腳本 - 添加 score 列到 trade_signal 表
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


def add_score_column():
    """
    向 trade_signal 表添加 score 列
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
        
        # 添加列
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE trade_signal 
                ADD COLUMN IF NOT EXISTS score FLOAT;
            """)
            
        conn.commit()
        logger.info("成功添加 score 列到 trade_signal 表")
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"添加列失敗: {str(e)}")
        return False


if __name__ == "__main__":
    add_score_column()
