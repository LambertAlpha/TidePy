"""
交易系統儀表板 - 使用Dash/Plotly實現的Web監控界面
提供對交易系統運行狀態、倉位情況和性能指標的可視化監控
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px
from dotenv import load_dotenv

# 添加項目根目錄到Python路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 載入環境變數
load_dotenv()

# 導入數據庫管理器
from database.db_manager import DBManager

# 初始化數據庫管理器
db_manager = DBManager()

# 初始化Dash應用
app = dash.Dash(
    __name__,
    title='TidePy 交易系統儀表板',
    update_title='更新中...',
    suppress_callback_exceptions=True
)

# 定義應用布局
app.layout = html.Div([
    # 頁眉
    html.Div([
        html.H1('TidePy 量化交易系統儀表板'),
        html.Div([
            html.Span('最後更新: '),
            html.Span(id='last-update-time')
        ], style={'float': 'right'})
    ], className='header'),
    
    # 主體內容
    html.Div([
        # 左側面板
        html.Div([
            # 系統狀態卡片
            html.Div([
                html.H3('系統狀態'),
                html.Div([
                    html.Div([
                        html.Span('狀態: '),
                        html.Span('運行中', id='system-status', className='status-running')
                    ]),
                    html.Div([
                        html.Span('運行時間: '),
                        html.Span('0天 0小時 0分鐘', id='uptime')
                    ]),
                    html.Div([
                        html.Span('開倉數量: '),
                        html.Span('0', id='open-positions-count')
                    ]),
                    html.Div([
                        html.Span('今日信號數: '),
                        html.Span('0', id='today-signals-count')
                    ])
                ], className='status-details')
            ], className='card'),
            
            # 性能指標卡片
            html.Div([
                html.H3('性能指標'),
                html.Div([
                    html.Div([
                        html.Span('總盈虧: '),
                        html.Span('0.00%', id='total-pnl', className='neutral')
                    ]),
                    html.Div([
                        html.Span('勝率: '),
                        html.Span('0.00%', id='win-rate')
                    ]),
                    html.Div([
                        html.Span('最大回撤: '),
                        html.Span('0.00%', id='max-drawdown')
                    ]),
                    html.Div([
                        html.Span('夏普比率: '),
                        html.Span('0.00', id='sharpe-ratio')
                    ])
                ], className='performance-details')
            ], className='card'),
            
            # 過濾控制
            html.Div([
                html.H3('數據過濾'),
                html.Div([
                    html.Label('時間範圍:'),
                    dcc.Dropdown(
                        id='time-range',
                        options=[
                            {'label': '今日', 'value': 'today'},
                            {'label': '過去7天', 'value': '7d'},
                            {'label': '過去30天', 'value': '30d'},
                            {'label': '所有時間', 'value': 'all'}
                        ],
                        value='7d'
                    ),
                    html.Label('交易對:'),
                    dcc.Dropdown(
                        id='symbol-filter',
                        options=[],
                        multi=True,
                        placeholder='選擇交易對...'
                    )
                ], className='filter-controls')
            ], className='card')
        ], className='left-panel'),
        
        # 主內容區
        html.Div([
            # 標籤頁
            dcc.Tabs([
                # 倉位標籤頁
                dcc.Tab(label='倉位', children=[
                    html.Div([
                        # 倉位表格
                        html.Div([
                            html.H3('當前倉位'),
                            html.Div(id='positions-table', className='table-container')
                        ], className='card full-width'),
                        
                        # 倉位分佈圖
                        html.Div([
                            html.H3('倉位分佈'),
                            dcc.Graph(id='positions-distribution')
                        ], className='card full-width')
                    ])
                ]),
                
                # 盈虧標籤頁
                dcc.Tab(label='盈虧分析', children=[
                    html.Div([
                        # 盈虧走勢圖
                        html.Div([
                            html.H3('盈虧走勢'),
                            dcc.Graph(id='pnl-chart')
                        ], className='card full-width'),
                        
                        # 每個交易對的盈虧貢獻
                        html.Div([
                            html.H3('交易對盈虧貢獻'),
                            dcc.Graph(id='symbol-pnl-contribution')
                        ], className='card full-width')
                    ])
                ]),
                
                # 交易信號標籤頁
                dcc.Tab(label='交易信號', children=[
                    html.Div([
                        # 信號列表
                        html.Div([
                            html.H3('最近交易信號'),
                            html.Div(id='signals-table', className='table-container')
                        ], className='card full-width'),
                        
                        # 信號分布圖
                        html.Div([
                            html.H3('信號分布'),
                            dcc.Graph(id='signals-distribution')
                        ], className='card full-width')
                    ])
                ]),
                
                # 因子分析標籤頁
                dcc.Tab(label='因子分析', children=[
                    html.Div([
                        # 因子相關性熱圖
                        html.Div([
                            html.H3('因子相關性'),
                            dcc.Graph(id='factor-correlation')
                        ], className='card full-width'),
                        
                        # 因子貢獻度
                        html.Div([
                            html.H3('因子貢獻度'),
                            dcc.Graph(id='factor-contribution')
                        ], className='card full-width')
                    ])
                ])
            ])
        ], className='main-content')
    ], className='content'),
    
    # 數據更新間隔
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # 每30秒更新一次
        n_intervals=0
    )
])


# 更新時間回調
@app.callback(
    Output('last-update-time', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_time(n):
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# 更新系統狀態回調
@app.callback(
    [
        Output('system-status', 'children'),
        Output('system-status', 'className'),
        Output('uptime', 'children'),
        Output('open-positions-count', 'children'),
        Output('today-signals-count', 'children')
    ],
    Input('interval-component', 'n_intervals')
)
def update_system_status(n):
    try:
        # 獲取開倉倉位數量
        positions = db_manager.get_open_positions()
        positions_count = len(positions) if not positions.empty else 0
        
        # 獲取今日信號數量
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # 這裡需要實現一個方法來獲取今日信號數量
        # 示例實現，實際項目中需要替換
        today_signals_count = 0
        
        # 模擬運行時間（實際應用中可以從數據庫或日誌文件中獲取啟動時間）
        uptime_seconds = 3600 * 5  # 示例：5小時
        days, remainder = divmod(uptime_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)
        uptime_str = f"{days}天 {hours}小時 {minutes}分鐘"
        
        return '運行中', 'status-running', uptime_str, str(positions_count), str(today_signals_count)
    except Exception as e:
        print(f"更新系統狀態出錯: {str(e)}")
        return '錯誤', 'status-error', '無法獲取', '無法獲取', '無法獲取'


# 更新性能指標回調
@app.callback(
    [
        Output('total-pnl', 'children'),
        Output('total-pnl', 'className'),
        Output('win-rate', 'children'),
        Output('max-drawdown', 'children'),
        Output('sharpe-ratio', 'children')
    ],
    Input('interval-component', 'n_intervals')
)
def update_performance_metrics(n):
    try:
        # 獲取倉位盈虧數據
        positions = db_manager.get_open_positions()
        
        if positions.empty:
            return '0.00%', 'neutral', '0.00%', '0.00%', '0.00'
        
        # 計算總盈虧
        total_pnl_percentage = positions['pnl_percentage'].mean() * 100 if 'pnl_percentage' in positions.columns else 0
        
        # 設置盈虧顏色類別
        pnl_class = 'positive' if total_pnl_percentage > 0 else 'negative' if total_pnl_percentage < 0 else 'neutral'
        
        # 計算勝率（假設有盈利的倉位佔比）
        win_count = (positions['pnl_percentage'] > 0).sum() if 'pnl_percentage' in positions.columns else 0
        win_rate = (win_count / len(positions)) * 100 if len(positions) > 0 else 0
        
        # 模擬其他指標（實際應用中應從數據庫獲取）
        max_drawdown = 5.23  # 示例值
        sharpe_ratio = 1.45  # 示例值
        
        return f"{total_pnl_percentage:.2f}%", pnl_class, f"{win_rate:.2f}%", f"{max_drawdown:.2f}%", f"{sharpe_ratio:.2f}"
    except Exception as e:
        print(f"更新性能指標出錯: {str(e)}")
        return '無法獲取', 'neutral', '無法獲取', '無法獲取', '無法獲取'


# 更新交易對過濾器選項
@app.callback(
    Output('symbol-filter', 'options'),
    Input('interval-component', 'n_intervals')
)
def update_symbol_options(n):
    try:
        # 獲取所有倉位的交易對
        positions = db_manager.get_open_positions()
        symbols = positions['symbol'].unique() if not positions.empty else []
        
        return [{'label': symbol, 'value': symbol} for symbol in symbols]
    except Exception as e:
        print(f"更新交易對選項出錯: {str(e)}")
        return []


# 更新倉位表格
@app.callback(
    Output('positions-table', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('symbol-filter', 'value')]
)
def update_positions_table(n, selected_symbols):
    try:
        # 獲取開倉倉位
        positions = db_manager.get_open_positions()
        
        if positions.empty:
            return html.Div("暫無倉位數據", className='no-data')
        
        # 應用交易對過濾
        if selected_symbols and len(selected_symbols) > 0:
            positions = positions[positions['symbol'].isin(selected_symbols)]
        
        # 整理表格數據
        positions['pnl_formatted'] = positions['pnl_percentage'].apply(lambda x: f"{x*100:.2f}%") if 'pnl_percentage' in positions.columns else "N/A"
        positions['open_time_formatted'] = positions['open_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S')) if 'open_time' in positions.columns else "N/A"
        
        # 創建表格
        table_header = [
            html.Thead(html.Tr([
                html.Th("交易對"),
                html.Th("方向"),
                html.Th("開倉時間"),
                html.Th("開倉價格"),
                html.Th("當前價格"),
                html.Th("數量"),
                html.Th("盈虧%")
            ]))
        ]
        
        rows = []
        for i, row in positions.iterrows():
            pnl_class = 'positive' if row.get('pnl_percentage', 0) > 0 else 'negative' if row.get('pnl_percentage', 0) < 0 else 'neutral'
            rows.append(html.Tr([
                html.Td(row['symbol']),
                html.Td("多頭" if row.get('direction') == 'long' else "空頭"),
                html.Td(row.get('open_time_formatted', 'N/A')),
                html.Td(f"{row.get('open_price', 0):.4f}"),
                html.Td(f"{row.get('current_price', 0):.4f}"),
                html.Td(f"{row.get('quantity', 0):.4f}"),
                html.Td(row.get('pnl_formatted', 'N/A'), className=pnl_class)
            ]))
        
        table_body = [html.Tbody(rows)]
        
        return html.Table(table_header + table_body, className='data-table')
    except Exception as e:
        print(f"更新倉位表格出錯: {str(e)}")
        return html.Div(f"獲取數據出錯: {str(e)}", className='error-message')


# 更新倉位分佈圖
@app.callback(
    Output('positions-distribution', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('symbol-filter', 'value')]
)
def update_positions_distribution(n, selected_symbols):
    try:
        # 獲取開倉倉位
        positions = db_manager.get_open_positions()
        
        if positions.empty:
            return create_empty_figure("暫無倉位數據")
        
        # 應用交易對過濾
        if selected_symbols and len(selected_symbols) > 0:
            positions = positions[positions['symbol'].isin(selected_symbols)]
            
        if positions.empty:
            return create_empty_figure("無符合過濾條件的倉位")
        
        # 計算每個交易對的倉位大小
        position_sizes = positions.groupby('symbol')['quantity'].sum().reset_index()
        
        # 創建餅圖
        fig = px.pie(
            position_sizes, 
            values='quantity', 
            names='symbol', 
            title='倉位分佈 (按數量)'
        )
        
        fig.update_layout(
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
    except Exception as e:
        print(f"更新倉位分佈圖出錯: {str(e)}")
        return create_empty_figure(f"獲取數據出錯: {str(e)}")


# 更新盈虧走勢圖
@app.callback(
    Output('pnl-chart', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('time-range', 'value')]
)
def update_pnl_chart(n, time_range):
    try:
        # 根據時間範圍設置起始時間
        now = datetime.now()
        if time_range == 'today':
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif time_range == '7d':
            start_time = now - timedelta(days=7)
        elif time_range == '30d':
            start_time = now - timedelta(days=30)
        else:  # 'all'
            start_time = now - timedelta(days=365)  # 假設最多顯示一年數據
        
        # 這裡需要實現一個方法來獲取歷史盈虧數據
        # 由於我們尚未實現該功能，這裡使用模擬數據
        
        # 生成模擬數據
        dates = pd.date_range(start=start_time, end=now, freq='D')
        pnl_values = np.cumsum(np.random.normal(0.005, 0.02, size=len(dates)))  # 隨機漫步
        
        # 創建折線圖
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=dates,
            y=pnl_values,
            mode='lines',
            name='累計盈虧',
            line=dict(width=2, color='rgba(0, 150, 136, 0.8)')
        ))
        
        # 添加零線
        fig.add_shape(
            type="line",
            x0=dates.min(),
            y0=0,
            x1=dates.max(),
            y1=0,
            line=dict(
                color="rgba(150, 150, 150, 0.5)",
                width=1,
                dash="dash",
            )
        )
        
        fig.update_layout(
            title='累計盈虧走勢',
            xaxis_title='日期',
            yaxis_title='累計盈虧 (%)',
            hovermode='x unified',
            margin=dict(l=20, r=20, t=50, b=20),
        )
        
        return fig
    except Exception as e:
        print(f"更新盈虧走勢圖出錯: {str(e)}")
        return create_empty_figure(f"獲取數據出錯: {str(e)}")


# 更新交易對盈虧貢獻圖
@app.callback(
    Output('symbol-pnl-contribution', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('symbol-filter', 'value')]
)
def update_symbol_pnl_contribution(n, selected_symbols):
    try:
        # 獲取開倉倉位
        positions = db_manager.get_open_positions()
        
        if positions.empty:
            return create_empty_figure("暫無倉位數據")
        
        # 應用交易對過濾
        if selected_symbols and len(selected_symbols) > 0:
            positions = positions[positions['symbol'].isin(selected_symbols)]
            
        if positions.empty:
            return create_empty_figure("無符合過濾條件的倉位")
        
        # 計算每個交易對的盈虧貢獻
        pnl_contribution = positions.groupby('symbol')['pnl'].sum().reset_index()
        
        # 排序
        pnl_contribution = pnl_contribution.sort_values('pnl', ascending=True)
        
        # 設置顏色
        colors = ['green' if x > 0 else 'red' for x in pnl_contribution['pnl']]
        
        # 創建水平條形圖
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=pnl_contribution['symbol'],
            x=pnl_contribution['pnl'],
            orientation='h',
            marker_color=colors
        ))
        
        fig.update_layout(
            title='各交易對盈虧貢獻',
            xaxis_title='盈虧金額',
            yaxis_title='交易對',
            margin=dict(l=20, r=20, t=50, b=20),
        )
        
        return fig
    except Exception as e:
        print(f"更新交易對盈虧貢獻圖出錯: {str(e)}")
        return create_empty_figure(f"獲取數據出錯: {str(e)}")


# 輔助函數：創建空圖表
def create_empty_figure(message):
    fig = go.Figure()
    
    fig.update_layout(
        annotations=[
            dict(
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                text=message,
                showarrow=False,
                font=dict(size=14)
            )
        ]
    )
    
    return fig


if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
