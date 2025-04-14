"""
日誌模組 - 設置系統日誌
"""

import logging
import os
from datetime import datetime
import config

def setup_logger():
    """
    設置日誌配置
    
    Returns:
        logging.Logger: 配置好的日誌器
    """
    # 創建日誌目錄
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 設置日誌文件名，包含日期時間戳
    log_file = os.path.join(log_dir, f'tidepy_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # 設置日誌級別
    log_level = getattr(logging, config.LOG_LEVEL)
    
    # 設置根日誌器
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # 返回根日誌器
    return logging.getLogger()
