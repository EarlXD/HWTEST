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

df_mp['start_date'] = pd.to_datetime(df_mp['start_date'])
df_mp['end_date'] = pd.to_datetime(df_mp['end_date'])
df_mp['uniq_mp_num'] = range(df_mp.shape[0])

df_api['date'] = pd.to_datetime(df_api['date'])

df_for_view = pd.DataFrame()
camp_un_ids = df_mp['campaign_id_uniq'].to_list()
camp_f_dates = df_mp['start_date'].to_list()
camp_l_dates = df_mp['end_date'].to_list()
mp_names = df_mp['mp_name'].to_list()
mp_nums = df_mp['uniq_mp_num'].to_list()
managers = df_mp['manager'].to_list()

for i in range(len(camp_un_ids)):
    camps = camp_un_ids[i].split(', ')
    f_date = camp_f_dates[i]
    l_date = camp_l_dates[i]
    mp_name = mp_names[i]
    mp_num = mp_nums[i]
    manager = managers[i]
    for j in camps:
        df_api_temp = df_api[df_api['campaign_id'] == j].copy()
        df_api_temp = df_api_temp[df_api_temp['date'] >= f_date]
        df_api_temp = df_api_temp[df_api_temp['date'] <= l_date]
        df_api_temp['start_date'] = f_date
        df_api_temp['end_date'] = l_date
        df_api_temp['uniq_mp_num'] = mp_num
        df_api_temp['manager'] = manager
        df_api_temp['mp_name'] = mp_name
        df_for_view = pd.concat([df_for_view, df_api_temp])
        
df_api = df_for_view

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_api['manager'].unique(),
                              placeholder='специалист',
                            #   default=st.session_state.managers
                              )
    filtered_api_df = df_api[df_api['manager'].isin(managers)]
    
    clients = st.multiselect('Выбор клиента',
                             filtered_api_df['client'].unique(),
                             default=filtered_api_df['client'].unique(),
                             placeholder='клиент')
    filtered_api_df = filtered_api_df[filtered_api_df['client'].isin(clients)]
    
    mplans = st.multiselect('Выбор медиаплана',
                              filtered_api_df['mp_name'].unique(),
                              default=filtered_api_df['mp_name'].unique(),
                              placeholder='медиаплан')
    filtered_api_df = filtered_api_df[filtered_api_df['mp_name'].isin(mplans)]    
    
    channels = st.multiselect('Выбор источника',
                              filtered_api_df['channel'].unique(),
                              default=filtered_api_df['channel'].unique(),
                              placeholder='источник')
    filtered_api_df = filtered_api_df[filtered_api_df['channel'].isin(channels)]

    campaigns = st.multiselect('Выбор кампании',
                              filtered_api_df['campaign_name'].unique(),
                              default=filtered_api_df['campaign_name'].unique(),
                              placeholder='кампания')
    filtered_api_df = filtered_api_df[filtered_api_df['campaign_name'].isin(campaigns)]

    first_date = st.date_input('Начало периода', value=df_mp['start_date'].min())
    end_date = st.date_input('Конец периода', value=datetime.date.today())

    currency_state = st.radio('Валюта', ['USD', 'KZT'])
    cur_kzt_usd = st.number_input('KZT/USD', value=475)
    
    authenticator.logout(location='sidebar')

if len(managers) == 0:
    st.stop()

filtered_api_df = filtered_api_df[filtered_api_df['date'].dt.date >= first_date]
filtered_api_df = filtered_api_df[filtered_api_df['date'].dt.date <= end_date]

filtered_api_df = filtered_api_df.groupby(['uniq_mp_num', 'campaign_id', 'start_date']).aggregate(aggregate_dict_campaign).reset_index()

filtered_api_df = filtered_api_df.merge(df_mp[['uniq_mp_num', 'client', 'mp_name', 'channel', 'source', 'unit_type', 'end_date']], on='uniq_mp_num')

filtered_api_df['start_date'] = pd.to_datetime(filtered_api_df['start_date'])
filtered_api_df['end_date'] = pd.to_datetime(filtered_api_df['end_date'])

filtered_api_df = format_table(filtered_api_df, currency_state, cur_kzt_usd, table_type='campaign')

filtered_api_df = filtered_api_df.rename(columns=main_col_dict)
filtered_api_df = filtered_api_df.replace(np.inf, np.NaN)

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
    width=2000,
    # height=600,
    hide_index=True)