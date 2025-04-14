"""
配置文件 - 存儲系統全局配置參數
"""

# 系統配置
SYSTEM_INTERVAL = 60  # 系統主循環間隔時間（秒）
LOG_LEVEL = "INFO"    # 日誌級別：DEBUG, INFO, WARNING, ERROR, CRITICAL

# 數據庫配置
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "lambertlin",  # 您的 macOS 用户名
    "password": "",        # 留空，因为通常不需要密码
    "database": "tidepy"
}

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

# 交易所配置
EXCHANGE_CONFIG = {
    "name": "binance",
    "api_key": "",  # 從環境變量獲取
    "secret": "",   # 從環境變量獲取
    "timeout": 30000,  # API超時時間（毫秒）
}

# 策略配置
STRATEGY_CONFIG = {
    "min_funding_rate": 0.0,  # 最低資金費率（不能為負）
    "min_liquidity": 1000000,  # 最低流動性閾值（USD）
    "min_market_cap": 10000000,  # 最低市值閾值（USD）
    "excluded_sectors": ["DeFi"],  # 排除的賽道
    "target_sectors": ["Meme"],  # 目標賽道
}

# 倉位和風控配置
POSITION_CONFIG = {
    "max_position_percentage": 0.05,  # 單幣空單最大持倉比例 (5%)
    "initial_position_percentage": 0.025,  # 單幣空單初始倉位比例 (2.5%)
    "add_position_loss_threshold": 0.30,  # 加倉虧損閾值 (30%)
    "add_position_profit_threshold": 0.15,  # 加倉盈利閾值 (15%)
    "reduce_position_loss_threshold": 0.20,  # 減倉虧損閾值 (20%)
    "reduce_position_profit_threshold": 0.20,  # 減倉盈利閾值 (20%)
    "reduce_position_ratio": 0.5,  # 減倉比例 (50%)
}
