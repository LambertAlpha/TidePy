#!/usr/bin/env python
"""
TidePy - 量化交易統計套利策略系統
主入口腳本
"""

import sys
import os
import argparse
from trading_system import TradingSystem
from utils.logger import setup_logger

# 設置日誌
logger = setup_logger()


def parse_arguments():
    """
    解析命令行參數
    
    Returns:
        argparse.Namespace: 解析後的參數
    """
    parser = argparse.ArgumentParser(description='TidePy - 量化交易統計套利策略系統')
    
    # 添加運行模式參數
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['live', 'backtest', 'simulate'],
        default='live',
        help='運行模式：live (實盤交易)，backtest (歷史回測)，simulate (模擬交易)'
    )
    
    # 添加回測參數
    parser.add_argument('--start-date', type=str, help='回測開始日期，格式：YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, help='回測結束日期，格式：YYYY-MM-DD')
    parser.add_argument('--initial-capital', type=float, default=10000.0, help='初始資金')
    
    # 添加是否使用測試環境參數
    parser.add_argument('--test', action='store_true', help='使用測試環境')
    
    return parser.parse_args()


def main():
    """
    主函數
    """
    try:
        # 解析命令行參數
        args = parse_arguments()
        
        # 創建並運行交易系統
        trading_system = TradingSystem()
        
        if args.mode == 'live':
            logger.info("啟動實盤交易模式")
            trading_system.run()
            
        elif args.mode == 'backtest':
            if not args.start_date or not args.end_date:
                logger.error("回測模式需要指定開始和結束日期")
                sys.exit(1)
                
            logger.info(f"啟動回測模式：{args.start_date} 至 {args.end_date}")
            results = trading_system.run_backtest(
                args.start_date, 
                args.end_date, 
                args.initial_capital
            )
            
            # 處理回測結果（可以輸出到文件或顯示圖表等）
            logger.info("回測完成")
            
        elif args.mode == 'simulate':
            logger.info("啟動模擬交易模式")
            # 模擬交易功能可以基於實盤交易模式，但不實際執行訂單
            # 這個功能可以在未來實現
            logger.error("模擬交易模式尚未實現")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("收到中斷信號，系統關閉")
        
    except Exception as e:
        logger.error(f"系統運行出錯: {str(e)}")
        sys.exit(1)
        
    logger.info("系統正常退出")
    sys.exit(0)


if __name__ == "__main__":
    # 確保日誌目錄存在
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 運行主函數
    main()
