import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
from path_lib import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(layout="wide")

df_mp, df_api = read_and_prepare_df(path)

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_mp['manager'].unique(),
                              placeholder='специалист')
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

    campaigns = st.multiselect('Выбор кампании',
                              filtered_mp_df['campaign_id_uniq'].unique(),
                              default=filtered_mp_df['campaign_id_uniq'].unique(),
                              placeholder='кампания')
    filtered_mp_df = filtered_mp_df[filtered_mp_df['campaign_id_uniq'].isin(campaigns)]

    first_date = st.date_input('Начало периода', value=df_mp['start_date'].min())
    end_date = st.date_input('Конец периода', value=datetime.date.today())

    currency_state = st.radio('Валюта', ['USD', 'KZT'])
    cur_kzt_usd = st.number_input('KZT/USD', value=475)

uniq_ids = filtered_mp_df['campaign_id_uniq'].unique()
filtered_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
filtered_api_df = filtered_api_df[filtered_api_df['date'] >= first_date]
filtered_api_df = filtered_api_df[filtered_api_df['date'] <= end_date]
filtered_api_df = filtered_api_df.groupby('campaign_id').aggregate(aggregate_dict_campaign).reset_index()
filtered_api_df = filtered_api_df.merge(filtered_mp_df, how='right')

filtered_api_df['impressions_perc'] = (filtered_api_df['impressions'] / filtered_api_df['impressions_plan']*100).round(1)

filtered_api_df['clicks_perc'] = (filtered_api_df['clicks'] / filtered_api_df['clicks_plan']*100).round(1)

filtered_api_df['CPM'] = filtered_api_df['cost'] / filtered_api_df['impressions'] * 1000 
filtered_api_df['CPM_perc'] = (filtered_api_df['CPM'] / filtered_api_df['CPM_plan_USD']*100).round(1)

filtered_api_df['conversions_perc'] = (filtered_api_df['conversions'] / filtered_api_df['conversions_ga_plan']*100).round(1)

filtered_api_df['reach_perc'] = (filtered_api_df['reach'] / filtered_api_df['reach_plan']*100).round(1)

filtered_api_df['ctr'] = filtered_api_df['clicks'] / filtered_api_df['impressions'] * 100
filtered_api_df['ctr_plan'] = filtered_api_df['ctr_plan'] * 100 
filtered_api_df['today'] = datetime.date.today()
filtered_api_df['today'] = pd.to_datetime(filtered_api_df['today'])
filtered_api_df['days'] = (filtered_api_df['today'] - filtered_api_df['start_date']).dt.days
filtered_api_df['percent'] = filtered_api_df['days']/filtered_api_df['planning_period'] * 100
filtered_api_df.loc[filtered_api_df['percent'] > 100, 'percent'] = 100
filtered_api_df['campaign_id'] = filtered_api_df['campaign_id'].astype('str')

filtered_api_df = filtered_api_df[['client', 'mp_name',
                                   'start_date', 'end_date',
                                   'channel', 'source',
                                   'campaign_name',
                                   'campaign_id',
                                   'impressions',
                                   'clicks',
                                   'ctr',
                                   'cost',
                                   'conversions',
                                   'reach',
                                   'CPM',
                                   ]]
# filtered_api_df = format_plan_fact_table(filtered_api_df, currency_state, cur_kzt_usd)
filtered_api_df['end_date'] = filtered_api_df['end_date'].dt.strftime('%d/%m/%Y')
filtered_api_df['start_date'] = filtered_api_df['start_date'].dt.strftime('%d/%m/%Y')
filtered_api_df = filtered_api_df.rename(columns=rename_camp)
# filtered_api_df = filtered_api_df.round(2)

# df.apply(lambda x: ['0' if col_i == 2 else '1' for col_i, value in enumerate(x)], axis = 1)
# filtered_api_df = filtered_api_df.rename(columns=rename_dict)
filtered_api_df = filtered_api_df.fillna(0)
st.dataframe(
# st.table(
                # filtered_api_df,
                filtered_api_df.style
                .format(precision=0, thousands=' ', subset=num_format_camp)
                .format('{:.1f}%', thousands=' ', subset=num_format_perc_camp)
                .format('$ {:.1f}', thousands=' ', subset=num_format_money_camp),
             width=2000,
             hide_index=True
             )