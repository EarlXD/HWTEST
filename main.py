import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
import plotly.graph_objects as go
import datetime
# from st_aggrid import AgGrid

st.set_page_config(layout="wide")

df_mp, df_api = read_and_prepare_df('E:/work/strlit/')

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_mp['manager'].unique(),
                              placeholder='специалист')
    filtered_mp_df = df_mp[df_mp['manager'].isin(managers)]
    
    clients = st.multiselect('Выбор клиента',
                             filtered_mp_df['clients'].unique(),
                             default=filtered_mp_df['clients'].unique(),
                             placeholder='клиент')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['clients'].isin(clients)]
    
    channels = st.multiselect('Выбор источника',
                              filtered_mp_df['channel'].unique(),
                              default=filtered_mp_df['channel'].unique(),
                              placeholder='источник')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['channel'].isin(channels)]
    

    first_date = st.date_input('Начало периода', value=df_mp['start_date'].min())
    end_date = st.date_input('Конец периода', value=datetime.date.today())

    currency_state = st.radio('Валюта', ['USD', 'KZT'])
    cur_kzt_usd = st.number_input('KZT/USD', value=475)
    

uniq_ids = filtered_mp_df['campaign_id_uniq'].unique()
filtered_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
filtered_api_df = filtered_api_df[filtered_api_df['ДАТА'] >= first_date]
filtered_api_df = filtered_api_df[filtered_api_df['ДАТА'] <= end_date]
filtered_api_df = filtered_api_df.groupby(['Клиент', 'Платформа']).aggregate({'Показы': 'sum',
                                                   'Клики': 'sum',
                                                   'Расходы': 'sum',
                                                   'CTR': 'sum',
                                                   'Конверсии': 'sum',
                                                   'CPC': 'sum',
                                                   'unit': 'sum',
                                                   'Частота': 'mean',
                                                   'Охват': 'sum'}).reset_index().replace(0, np.NaN)
filtered_api_df = format_table(filtered_api_df, currency_state, cur_kzt_usd)

chart_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
chart_api_df = chart_api_df[chart_api_df['ДАТА'] >= first_date]
chart_api_df = chart_api_df[chart_api_df['ДАТА'] <= end_date]
date_range = pd.date_range(df_api['ДАТА'].min(), df_api['ДАТА'].max(), freq='d')
chart_api_df['ДАТА'] = pd.to_datetime(chart_api_df['ДАТА'])
t_df = pd.DataFrame({'ДАТА': date_range})
chart_api_df = t_df.merge(chart_api_df, how='left')
chart_api_df = chart_api_df.groupby(['ДАТА']).aggregate({'Показы': 'sum',
                                                   'Клики': 'sum',
                                                   'Расходы': 'sum',
                                                   'CTR': 'sum',
                                                   'Конверсии': 'sum',
                                                   'CPC': 'sum',
                                                   'unit': 'sum',
                                                   'Частота': 'mean',
                                                   'Охват': 'sum'}).reset_index().replace(0, np.NaN)
# with st.container():
st.dataframe(filtered_api_df, width=2000, hide_index=True)
fig = go.Figure(data=go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df['Клики'], mode='lines+markers'))
st.plotly_chart(fig, use_container_width=True)
fig = go.Figure(data=go.Scatter(x=chart_api_df['ДАТА'], y=chart_api_df['Показы'], mode='lines+markers'))
st.plotly_chart(fig, use_container_width=True)
# AgGrid(filtered_api_df)

