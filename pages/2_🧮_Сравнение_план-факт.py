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

    first_date = st.date_input('Начало периода', value=df_mp['start_date'].min())
    end_date = st.date_input('Конец периода', value=datetime.date.today())

    currency_state = st.radio('Валюта', ['USD', 'KZT'])
    cur_kzt_usd = st.number_input('KZT/USD', value=475)

uniq_ids = filtered_mp_df['campaign_id_uniq'].unique()
filtered_api_df = df_api[df_api['campaign_id_uniq'].isin(uniq_ids)]
filtered_api_df = filtered_api_df[filtered_api_df['date'] >= first_date]
filtered_api_df = filtered_api_df[filtered_api_df['date'] <= end_date]
filtered_api_df = filtered_api_df.groupby('campaign_id_uniq').aggregate(aggregate_dict).reset_index()
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

filtered_api_df = filtered_api_df[['client', 'mp_name',
                                   'start_date', 'end_date', 'percent',
                                   'channel', 'source',
                                   'impressions', 'impressions_plan', 'impressions_perc',
                                   'clicks', 'clicks_plan', 'clicks_perc',
                                   'ctr', 'ctr_plan',
                                   'cost', 'cost_exVAT_USD',
                                   'conversions', 'conversions_ga_plan', 'conversions_perc',
                                   'reach', 'reach_plan', 'reach_perc',
                                   'CPM', 'CPM_plan_USD', 'CPM_perc',
                                   ]]
# filtered_api_df = format_plan_fact_table(filtered_api_df, currency_state, cur_kzt_usd)
filtered_api_df['end_date'] = filtered_api_df['end_date'].dt.strftime('%d/%m/%Y')
filtered_api_df['start_date'] = filtered_api_df['start_date'].dt.strftime('%d/%m/%Y')
filtered_api_df = filtered_api_df.rename(columns=rename_plan_fact)
# filtered_api_df = filtered_api_df.round(2)

# df.apply(lambda x: ['0' if col_i == 2 else '1' for col_i, value in enumerate(x)], axis = 1)
# filtered_api_df = filtered_api_df.rename(columns=rename_dict)
filtered_api_df = filtered_api_df.fillna(0)
#st.dataframe(
st.table(
                # filtered_api_df,
                filtered_api_df.style
                .apply(lambda x: ['background-color: #5eae76; opacity: 1' if x.name == 'Показы, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #de796e; opacity: 1' if x.name == 'Показы, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: white; opacity: 1' if x.name == 'Показы, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #5eae76; opacity: 1' if x.name == 'Клики, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #de796e; opacity: 1' if x.name == 'Клики, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: white; opacity: 1' if x.name == 'Клики, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #5eae76; opacity: 1' if x.name == 'Конверсии, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #de796e; opacity: 1' if x.name == 'Конверсии, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: white; opacity: 1' if x.name == 'Конверсии, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #5eae76; opacity: 1' if x.name == 'Охват, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #de796e; opacity: 1' if x.name == 'Охват, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: white; opacity: 1' if x.name == 'Охват, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #5eae76; opacity: 1' if x.name == 'CPM, %' and value > filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: #de796e; opacity: 1' if x.name == 'CPM, %' and value < filtered_api_df.iloc[col_i]['Общий процент'] else '' for col_i, value in enumerate(x)], axis = 0)
                .apply(lambda x: ['background-color: white; opacity: 1' if x.name == 'CPM, %' and value == 0 else '' for col_i, value in enumerate(x)], axis = 0)
                .format(precision=0, thousands=' ', subset=num_format)
                .format('{:.1f}%', thousands=' ', subset=num_format_perc)
                .format('$ {:.1f}', thousands=' ', subset=num_format_money),

             #width=2000,
             #hide_index=True
             )
