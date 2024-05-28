import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
from path_lib import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader

st.set_page_config(layout="wide")

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized'])

if st.session_state["authentication_status"]:
    pass
elif st.session_state["authentication_status"] is False:
    authenticator.login(location='sidebar')
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    authenticator.login(location='sidebar')
    

if 'authentication_status' not in st.session_state:
    st.stop()
if st.session_state['authentication_status'] == None or st.session_state['authentication_status'] == False:
    st.stop()
    
    
df_mp, df_api = read_and_prepare_df(path)

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_mp['manager'].unique(),
                              placeholder='специалист',
                            #   default=st.session_state.managers
                              )
    filtered_mp_df = df_mp[df_mp['manager'].isin(managers)]
    
    clients = st.multiselect('Выбор клиента',
                             filtered_mp_df['client'].unique(),
                             default=filtered_mp_df['client'].unique(),
                             placeholder='клиент')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['client'].isin(clients)]
    
    mplans = st.multiselect('Выбор медиаплана',
                              filtered_mp_df['mp_name'].unique(),
                              default=filtered_mp_df['mp_name'].unique(),
                              placeholder='медиаплан')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['mp_name'].isin(mplans)]    
    
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
filtered_api_df = filtered_api_df.groupby('campaign_id_uniq').aggregate(aggregate_dict).reset_index()
filtered_api_df = filtered_api_df.merge(filtered_mp_df, how='right')

filtered_api_df = format_plan_fact_table(filtered_api_df, currency_state, cur_kzt_usd)

filtered_api_df = filtered_api_df.rename(columns=rename_plan_fact)

filtered_api_df = filtered_api_df.fillna(0)

if currency_state == 'USD':
    money_style = '$ {:,.2f}'
elif currency_state == 'KZT':
    money_style = '₸ {:,.1f}'

st.dataframe(
             filtered_api_df.style
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'Показы, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'Показы, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'Показы, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'Клики, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'Клики, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'Клики, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'Конверсии, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'Конверсии, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'Конверсии, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'Охват, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'Охват, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'Охват, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'CPC, %' and value > 100 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'CPC, %' and value <= 100.1 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'CPC, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'CPM, %' and value > 100 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'CPM, %' and value <= 100.1 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'CPM, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #5eae76' if x.name == 'Расходы, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: #de796e' if x.name == 'Расходы, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
             .apply(lambda x: ['background-color: ' if x.name == 'Расходы, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
             .format(precision=0, thousands=' ', subset=num_format)
             .format('{:.1f}%', thousands=' ', subset=num_format_perc)
             .format(money_style, thousands=' ', subset=num_format_money),
             width=2000,
             hide_index=True
             )