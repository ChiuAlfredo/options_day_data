
import dash
import dash_core_components as dcc
import dash_html_components as html
import os

import pandas as pd

import util.draw_util as draw_util

# 繪製變動%圖表
file_folder = 'prob'
# 檢查 file_folder 是否存在於 ./data/picture/
if not os.path.exists(f'./data/picture/{file_folder}'):
    # 建立新的資料夾
    os.makedirs(f'./data/picture/{file_folder}', exist_ok=True)


# 繪圖
def draw_prop_change(df_info, df_title, option_type, year_filter=None):
    # Apply year filter if provided
    if year_filter:
        df_info = df_info[df_info['underlayed'].str.contains(year_filter)]

    if option_type == 'C':
        # Create a list to store all the charts
        charts = []

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[(df_info['kind'] == option_type)],
                    column='previous_last_day_change',
                    title=f'{df_title}_{option_type.lower()}_prob_previous_last',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] < 0.97) & (df_info['kind'] == option_type)
                    ],
                    column='previous_last_day_change',
                    title=f'{df_title}_deep_ITM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_ITM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_ATM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_OTM_{option_type.lower()}_previous_last_change',
                )
            )
        )

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 1.03) & (df_info['kind'] == option_type)
                    ],
                    column='previous_last_day_change',
                    title=f'{df_title}_deep_OTM_{option_type.lower()}_previous_last_change',
                )
            )
        )
    elif option_type == 'P':
        charts = []

        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[(df_info['kind'] == option_type)],
                    column='previous_last_day_change',
                    title=f'{df_title}_{option_type.lower()}_prob_previous_last',
                )
            )
        )


        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] >= 1.03) & (df_info['kind'] == option_type)
                    ],
                    column='previous_last_day_change',
                    title=f'{df_title}_deep_ITM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_ITM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_ATM_{option_type.lower()}_previous_last_change',
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
                    column='previous_last_day_change',
                    title=f'{df_title}_OTM_{option_type.lower()}_previous_last_change',
                )
            )
        )


        charts.append(
            dcc.Graph(
                figure=draw_util.draw_prop_change(
                    df_info=df_info[
                        (df_info['moneyness'] < 0.97) & (df_info['kind'] == option_type)
                    ],
                    column='previous_last_day_change',
                    title=f'{df_title}_deep_OTM_{option_type.lower()}_previous_last_change',
                )
            )
        )


    return charts


# 月選
month_options_df = pd.read_csv(
    './data/info/month_options_last_day_info.csv', encoding='utf-8'
)

month_charts_call = draw_prop_change(df_info=month_options_df, df_title='month', option_type='C')
month_charts_put = draw_prop_change(df_info=month_options_df, df_title='month', option_type='P')

# 周選
week_options_df = pd.read_csv(
    './data/info/week_options_last_day_info.csv', encoding='utf-8'
)

week_charts_call = draw_prop_change(df_info=week_options_df, df_title='week', option_type='C')
week_charts_put = draw_prop_change(df_info=week_options_df, df_title='week', option_type='P')

# Create the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div(children=[
    html.H1(children='Options Day Data, 2020-2023'),

    html.Div(children=[
        html.H2(children='Select Options Type:'),
        dcc.Dropdown(
            id='options-type',
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

    html.Div(id='options-charts')
])


@app.callback(
    dash.dependencies.Output('options-title', 'children'),
    dash.dependencies.Output('options-charts', 'children'),
    [dash.dependencies.Input('options-type', 'value'),
     dash.dependencies.Input('option-type', 'value'),
     dash.dependencies.Input('year-filter', 'value')]
)
def update_options(options_type, option_type, year_filter):
    if options_type == 'week':
        if option_type == 'C':
            return 'Week Options - Call', draw_prop_change(week_options_df, 'week', 'C', year_filter)
        elif option_type == 'P':
            return 'Week Options - Put', draw_prop_change(week_options_df, 'week', 'P', year_filter)
    elif options_type == 'month':
        if option_type == 'C':
            return 'Month Options - Call', draw_prop_change(month_options_df, 'month', 'C', year_filter)
        elif option_type == 'P':
            return 'Month Options - Put', draw_prop_change(month_options_df, 'month', 'P', year_filter)


app.run_server(debug=False, use_reloader=False, port=8050,host="0.0.0.0")


    
