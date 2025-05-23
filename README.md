# TidePy - 量化交易統計套利策略系統

TidePy是一個基於Python的量化交易系統，專門用於加密貨幣市場的統計套利策略執行。

## 系統架構

採用單體架構設計，包含以下主要模組：
1. **數據採集層** (data_collector) - 負責收集市場數據、資金費率、代幣信息和流動性分析
2. **策略核心層** (strategy) - 負責因子計算、信號生成和倉位大小確定
3. **風控管理層** (risk_manager) - 處理倉位限制、盈虧監控和倉位調整
4. **交易執行層** (trade_executor) - 執行訂單、監控訂單狀態和報告執行結果
5. **系統整合層** (trading_system) - 協調各層的運作，實現系統的完整功能
6. **儀表板層** (dashboard) - 提供數據可視化界面，監控系統運行狀態和倉位情況

## Quick Start

1. 安裝依賴:
```
pip install -r requirements.txt
```

2. 設置環境變數:
- 複製`.env.example`到`.env`
- 填寫交易所API密鑰和其他配置信息

3. 啟動系統:
```
python main.py
```

## 策略說明

本系統實現了一套針對加密貨幣市場的統計套利策略，主要篩選因子包括：

- 資金費率(不能為負)
- 幣種流動性和市值大小
- 拉盤情況識別(優先選擇拉過盤的meme幣進行做空)
- 賽道篩選(不做空DeFi)
- 代幣解鎖進度和流通量

風控措施包括：
- 單幣空單持倉佔比控制在5%以內
- 單幣空單初始倉位佔比控制在2.5%以內
- 加減倉條件設置

## 技術棧

- Python 3.10+
- PostgreSQL和Redis用於數據存儲和快取
- Pandas和NumPy用於數據分析和處理
- Matplotlib和Dash/Plotly用於數據可視化和監控儀表板
- CCXT庫用於多交易所API整合
- AsyncIO和aiohttp用於非阻塞並發操作
- SQLAlchemy用於數據庫ORM
- Backtrader用於策略回測

## 关于PostgerSQL数据库查询
因子计算结果不会储存在数据库，会在终端打印出来；

-- 查看市场数据
SELECT * FROM market_data ORDER BY timestamp DESC LIMIT 10;

-- 查看资金费率数据
SELECT * FROM funding_rate ORDER BY timestamp DESC LIMIT 10;

-- 查看交易信号
SELECT * FROM trade_signal ORDER BY timestamp DESC LIMIT 10;

## 架構師在量化系統開發中的核心決策
1. 分析需求 - 交易頻率、資金規模
2. 評估技術可行性 - 技術成熟度、可用資源
3. 設計備選方案 - 框架選型（微服務 vs 單體）
4. 方案評估比較 - 延遲測試 風險評估
5. 最終決策 - 選定框架和技術棧