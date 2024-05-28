import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
from path_lib import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
import yaml
from yaml.loader import SafeLoader

warnings.simplefilter("ignore", category=FutureWarning)
st.set_page_config(layout="wide")


df_mp, df_api = read_and_prepare_df(path)

with st.sidebar:
    
    managers = st.multiselect('Выбор специалиста',
                                                df_mp['manager'].unique(),
                                                placeholder='специалист',
                                                )
    filtered_mp_df = df_mp[df_mp['manager'].isin(managers)]
    
    clients = st.multiselect('Выбор клиента',
                             filtered_mp_df['client'].unique(),
                             default=filtered_mp_df['client'].unique(),
                             placeholder='клиент')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['client'].isin(clients)]
    
    channels = st.multiselect('Выбор источника',
                              filtered_mp_df['channel'].unique(),
                              default=filtered_mp_df['channel'].unique(),
                              placeholder='источник')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['channel'].isin(channels)]
    
    first_date = st.date_input('Начало периода', value=df_mp['start_date'].min())
    end_date = st.date_input('Конец периода', value=datetime.date.today())

    currency_state = st.radio('Валюта', ['USD', 'KZT'])
    cur_kzt_usd = st.number_input('KZT/USD', value=475)
    
    authenticator.logout(location='sidebar')

uniq_ids = filtered_mp_df['campaign_id_uniq'].unique()
filtered_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
filtered_api_df = filtered_api_df[filtered_api_df['date'] >= first_date]
filtered_api_df = filtered_api_df[filtered_api_df['date'] <= end_date]
filtered_api_df = filtered_api_df.groupby(['client', 'channel']).aggregate(aggregate_dict).reset_index().replace(0, np.NaN)


filtered_api_df = format_table(filtered_api_df, currency_state, cur_kzt_usd, table_type='table')

filtered_api_df = filtered_api_df.rename(columns=main_col_dict)

if currency_state == 'USD':
    money_style = '$ {:,.2f}'
elif currency_state == 'KZT':
    money_style = '₸ {:,.1f}'

st.dataframe(
    filtered_api_df.style
    .format(precision=0, thousands=' ', subset=num_format_main)
    .format(precision=2, thousands=' ', subset=num_format_float_main)
    .format('{:.2f}%', thousands=' ', subset=num_format_perc_main)
    .format(money_style, thousands=' ', subset=num_format_money_main)
    ,
    width=2000, hide_index=True)

if len(clients) == 1:
    chart_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
    chart_api_df = chart_api_df[chart_api_df['date'] >= first_date]
    chart_api_df = chart_api_df[chart_api_df['date'] <= end_date]
    date_range = pd.date_range(first_date, end_date, freq='d')
    chart_api_df['date'] = pd.to_datetime(chart_api_df['date'])
    t_df = pd.DataFrame({'date': date_range})
    chart_api_df = t_df.merge(chart_api_df, how='left')
    chart_api_df = chart_api_df.groupby(['date']).aggregate(aggregate_dict).reset_index().replace(0, np.NaN)
    chart_api_df = format_table(chart_api_df, currency_state, cur_kzt_usd, table_type='chart')
    chart_api_df = chart_api_df.rename(columns=main_col_dict)
    
    
    metric1 = st.multiselect('Выбор метрики', metrics,
                             default=['Показы факт', 'Клики факт'],
                             max_selections=2)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if len(metric1) > 0:
        fig.add_trace(go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df[metric1[0]], mode='lines+markers', name=metric1[0]), secondary_y=False)
        fig.update_layout(yaxis=dict(title=dict(text=metric1[0]), side="left"))
    if len(metric1) > 1:
        fig.add_trace(go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df[metric1[1]], mode='lines+markers', name=metric1[1]), secondary_y=True)
        fig.update_layout(yaxis2=dict(title=dict(text=metric1[1]), side="right", overlaying="y", tickmode="sync"))
    fig.update_layout(legend=dict(orientation="h"))
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
    
    
    metric2 = st.multiselect('Выбор метрики', metrics,
                             default=['Расходы факт', 'CPA факт'],
                             max_selections=2)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if len(metric2) > 0:
        fig.add_trace(go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df[metric2[0]], mode='lines+markers', name=metric2[0]), secondary_y=False)
        fig.update_layout(yaxis=dict(title=dict(text=metric2[0]), side="left"))
    if len(metric2) > 1:
        fig.add_trace(go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df[metric2[1]], mode='lines+markers', name=metric2[1]), secondary_y=True)
        fig.update_layout(yaxis2=dict(title=dict(text=metric2[1]), side="right", overlaying="y", tickmode="sync"))
    fig.update_layout(legend=dict(orientation="h"))
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)


