
import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
from dash import dash_table

import util.draw_util as draw_util

# 繪製變動%圖表
file_folder = 'prob'
# 檢查 file_folder 是否存在於 ./data/picture/
if not os.path.exists(f'./data/picture/{file_folder}'):
    # 建立新的資料夾
    os.makedirs(f'./data/picture/{file_folder}', exist_ok=True)


# 繪圖
def draw_prop_change(df_info,base_column,comparison_column,options_type_week_month,option_type, year_filter=None):
    # Apply year filter if provided

    df_title = f'{options_type_week_month}_{option_type}'

    

    if year_filter:
        df_info = df_info[df_info['underlayed'].str.contains(year_filter)]

    if options_type_week_month == 'week':
        df_info = df_info[df_info['underlayed'].str.contains('W1|W2|W4|W5')]
    elif options_type_week_month == 'month':
        df_info = df_info[~df_info['underlayed'].str.contains('W1|W2|W4|W5')]

    
    df_info['change_value'] = ( df_info[comparison_column]- df_info[base_column])/df_info[base_column]*100
    df_info = df_info.replace([np.inf, -np.inf],np.nan)
    df_info.fillna(0,inplace=True)
    
    df_info = df_info[df_info[base_column]>0]
    

    if option_type == 'C':
        # Create a list to store all the charts
        charts = []

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[(df_info['kind'] == option_type)],
                    column='change_value',
                    title=f'{df_title}_prob',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] < 0.95) 
                        & (df_info['moneyness'] < 0.97) 
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_deep_ITM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 0.97)
                        & (df_info['moneyness'] < 0.99)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_ITM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 0.99)
                        & (df_info['moneyness'] < 1.01)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_ATM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 1.01)
                        & (df_info['moneyness'] < 1.03)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_OTM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] < 1.05)
                        & (df_info['moneyness'] >= 1.03) 
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_deep_OTM',
                )
            )
        )
    elif option_type == 'P':
        charts = []

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[(df_info['kind'] == option_type)],
                    column='change_value',
                    title=f'{df_title}_prob',
                )
            )
        )


        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] < 1.05)
                        & (df_info['moneyness'] >= 1.03) 
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_deep_ITM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 1.01)
                        & (df_info['moneyness'] < 1.03)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_ITM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 0.99)
                        & (df_info['moneyness'] < 1.01)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_ATM',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 0.97)
                        & (df_info['moneyness'] < 0.99)
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_OTM',
                )
            )
        )


        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 0.95)
                        & (df_info['moneyness'] < 0.97) 
                        & (df_info['kind'] == option_type)
                    ],
                    column='change_value',
                    title=f'{df_title}_deep_OTM',
                )
            )
        )
    
    table = dash_table.DataTable(
        df_info.to_dict('records'),
        [{"name": i, "id": i} for i in df_info.columns],
        fixed_rows={'headers': True},

    )

    return charts,table


# # 月選
# month_options_df = pd.read_csv(
#     './data/info/month_options_last_day_info.csv', encoding='utf-8'
# )

# month_charts_call = draw_prop_change(df_info=month_options_df, df_title='month', option_type='C')
# month_charts_put = draw_prop_change(df_info=month_options_df, df_title='month', option_type='P')

# # 周選
# week_options_df = pd.read_csv(
#     './data/info/week_options_last_day_info.csv', encoding='utf-8'
# )

# week_charts_call = draw_prop_change(df_info=week_options_df, df_title='week', option_type='C')
# week_charts_put = draw_prop_change(df_info=week_options_df, df_title='week', option_type='P')

options_df = pd.read_csv(
    './data/info/all_options_last_day_info.csv', encoding='utf-8'
)


# Create the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div(children=[
    html.H1(children='Options Day Data, 2020-2023'),

    html.Div(children=[
    html.H2(children='Select Base Value:'),
        dcc.Dropdown(
            id='base-column',
            options=[
                {'label': '結算日選擇權開盤價', 'value': '結算日選擇權開盤價'},
                {'label': '結算日選擇權收盤價', 'value': '結算日選擇權收盤價'},
                {'label': '結算日選擇權結算價', 'value': '結算日選擇權結算價'},
                {'label': '前天選擇權開盤價', 'value': '前天選擇權開盤價'},
                {'label': '前天選擇權收盤價', 'value': '前天選擇權收盤價'},
                {'label': '前天選擇權結算價', 'value': '前天選擇權結算價'}
            ],
            value='前天選擇權收盤價'
        )
    ]),

    html.Div(children=[
        html.H2(children='Select Comparison Value:'),
        dcc.Dropdown(
            id='comparison-column',
            options=[
                {'label': '結算日選擇權開盤價', 'value': '結算日選擇權開盤價'},
                {'label': '結算日選擇權收盤價', 'value': '結算日選擇權收盤價'},
                {'label': '結算日選擇權結算價', 'value': '結算日選擇權結算價'},
                {'label': '前天選擇權開盤價', 'value': '前天選擇權開盤價'},
                {'label': '前天選擇權收盤價', 'value': '前天選擇權收盤價'},
                {'label': '前天選擇權結算價', 'value': '前天選擇權結算價'}
            ],
            value='結算日選擇權收盤價'
        )
    ]),

    html.Div(children=[
        html.H2(children='Select Options Type:'),
        dcc.Dropdown(
            id='options-type_week_month',
            options=[
                {'label': 'Week Options', 'value': 'week'},
                {'label': 'Month Options', 'value': 'month'}
            ],
            value='week'
        )
    ]),
    
    html.Div(children=[
        html.H2(children='Select Option Type:'),
        dcc.Dropdown(
            id='option-type',
            options=[
                {'label': 'Call', 'value': 'C'},
                {'label': 'Put', 'value': 'P'}
            ],
            value='C'
        )
    ]),

    html.Div(children=[
        html.H2(children='Select Year Filter:'),
        dcc.Dropdown(
            id='year-filter',
            options=[
                {'label': 'All Years', 'value': None},
                {'label': '2020', 'value': '2020'},
                {'label': '2021', 'value': '2021'},
                {'label': '2022', 'value': '2022'},
                {'label': '2023', 'value': '2023'}
            ],
            value=None
        )
    ]),

    html.H2(id='options-title'),

    html.Div(id='options-charts'),

    html.Div(id='options-table')
])


@app.callback(
    dash.dependencies.Output('options-title', 'children'),
    dash.dependencies.Output('options-charts', 'children'),
    dash.dependencies.Output('options-table', 'children'),
    [
        dash.dependencies.Input('base-column', 'value'),
        dash.dependencies.Input('comparison-column', 'value'),
        dash.dependencies.Input('options-type_week_month', 'value'),
        dash.dependencies.Input('option-type', 'value'),
        dash.dependencies.Input('year-filter', 'value')
    ]
)
def update_options(base_column,comparison_column,options_type_week_month, option_type, year_filter):
    charts , table  = draw_prop_change(df_info=options_df,base_column=base_column,comparison_column=comparison_column,options_type_week_month=options_type_week_month, option_type=option_type, year_filter=year_filter)
    return f'{options_type_week_month}_{option_type}',charts,table



app.run_server(debug=False, use_reloader=False, port=8050,host='0.0.0.0')


    
