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
from ad_functions_nova import * 
from itertools import chain

st.set_page_config(layout="wide") # отображение сайта в расширенном режиме

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader) # загрузка логинов и паролей

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized'])

# проверка аутентификации
if st.session_state["authentication_status"]:
    pass
elif st.session_state["authentication_status"] is False:
    authenticator.login(location='sidebar')
    st.error('Username/password is incorrect')
if st.session_state['authentication_status'] == None or st.session_state['authentication_status'] == False:
    authenticator.login(location='sidebar')
    
if 'authentication_status' not in st.session_state:
    st.stop()
if st.session_state['authentication_status'] == None:
    st.stop()

#чтение и подготовка уже существующих данных
df_mp = mp_df_read(path)
df_mp = clean_column_values(df_mp, 'campaign_id_uniq')
df_api = api_df_read(path, df_mp)

# подгрузка данных яндекса для аутентификации запросов
oath_data = pd.read_csv('data/yand_oath.csv')
oath_data = pd.Series(oath_data['токен'].values,index=oath_data['аккаунт']).to_dict()

# добавление виджетов в боковой панели
with st.sidebar:
    start_date_w = st.date_input('Начало выгрузки', datetime.date.today() - datetime.timedelta(days=7))
    end_date_w = st.date_input('Конец выгрузки', datetime.date.today())
    load_type = st.radio(
                         "Период выгрузки",
                         ['Обновить последние сутки', "Согласно выбранному периоду", "Последний год"])
    
authenticator.logout(location='sidebar')

# запуск коннектора
if st.button('Запустить сбор данных'):
    if load_type == 'Обновить последние сутки':
        start_date_w = datetime.date.today() - datetime.timedelta(days=2)
    elif load_type == "Последний год":
        start_date_w = datetime.date.today() - datetime.timedelta(days=365)
    start_date = start_date_w.strftime("%Y-%m-%d")
    end_date = end_date_w.strftime("%Y-%m-%d")
    start_date_t = start_date_w
    end_date_t = end_date_w
    # ---------------------------------------------------------------------------------------------------------
    # выгрузка данных из google ADS
    # аутентификация запросов осуществляется через файл google-ads.yaml
    st.text('Выгрузка Google ADS')
    google_mp_df = df_mp[df_mp['channel'] == 'Google']

    campaign_id_list = google_mp_df['campaign_id_uniq'].to_list()
    for i in range(len(campaign_id_list)):
        campaign_id_list[i] = campaign_id_list[i].split(', ')
    campaign_id_list = list(chain.from_iterable(campaign_id_list))

    google_data = pd.DataFrame()
    for id in google_mp_df['customer_account_id'].unique():
        st.toast('Выгрузка данных по аккауту с id ' + id)
        redact_yaml(id)
        seed_customer_ids = [id]
        df_google = get_google_ads_data(seed_customer_ids, start_date, end_date)
        df_google['campaign_id'] = df_google['campaign_id'].astype(str)
        google_data = pd.concat([google_data, df_google])
    google_data['channel'] = 'Google'
    google_data = google_data[google_data['campaign_id'].isin(campaign_id_list)]
    google_data['campaign_id_uniq'] = get_uniq_id(df_mp, google_data)
    google_data['campaign_type'] = google_data['campaign_type'].apply(lambda x: camp_type_dict[x])
    # ---------------------------------------------------------------------------------------------------------
    # выгрузка данных из Meta
    # токены и прочие ключи указаны и при необходимости обновляются в файле ad_functions_nova
    st.text('Выгрузка Meta Business')
    meta_mp_df = df_mp[df_mp['channel'] == 'Facebook ADS']

    campaign_id_list = meta_mp_df['campaign_id_uniq'].to_list()
    for i in range(len(campaign_id_list)):
        campaign_id_list[i] = campaign_id_list[i].split(', ')
    campaign_id_list = list(chain.from_iterable(campaign_id_list))

    meta_data = pd.DataFrame()
    for id in meta_mp_df['customer_account_id'].unique():
        st.toast('Выгрузка данных по аккауту с id ' + id)
        account_id = 'act_' + id
        df_meta = get_facebook_ads_data(account_id, start_date, end_date, campaign_id_list)
        df_meta['campaign_id'] = df_meta['campaign_id'].astype(str)
        meta_data = pd.concat([meta_data, df_meta])
    meta_data['channel'] = 'Facebook ADS'
    meta_data['campaign_id_uniq'] = get_uniq_id(meta_mp_df, meta_data)
    meta_data = meta_data.rename(columns={'campaign': 'campaign_name'})
    # ---------------------------------------------------------------------------------------------------------
    # выгрузка данных из ТикТока
    # сейчас все токены завязаны на мою почту, чтобы получить доступ к кабинету, нужно дать доступ к этому кабинету на мою почту: earlxdd@gmail.com
    st.text('Выгрузка TikTok Ads')
    tiktok_mp_df = df_mp[df_mp['channel'] == 'TikTok Ads']

    campaign_id_list = tiktok_mp_df['campaign_id_uniq'].to_list()
    for i in range(len(campaign_id_list)):
        campaign_id_list[i] = campaign_id_list[i].split(', ')
    campaign_id_list = list(chain.from_iterable(campaign_id_list))

    td = (end_date_t - start_date_t).days

    tiktok_data = pd.DataFrame()

    for id in tiktok_mp_df['customer_account_id'].unique():
        st.toast('Выгрузка данных по аккауту с id ' + id)
        for i in range(td//30 + 1):
            end_date_t = start_date_t + datetime.timedelta(days=30)
            df_tiktok = get_tiktok_ads_data(id, start_date_t.strftime("%Y-%m-%d"), end_date_t.strftime("%Y-%m-%d"))
            tiktok_data = pd.concat([tiktok_data, df_tiktok])
            start_date_t = end_date_t

    tiktok_data = tiktok_data[tiktok_data['campaign_id'].isin(campaign_id_list)]
    tiktok_data['date'] = pd.to_datetime(tiktok_data['date'])
    tiktok_data = tiktok_data.sort_values(by='date')
    tiktok_data['channel'] = 'TikTok Ads'
    tiktok_data = tiktok_data.rename(columns={'spend': 'cost'})
    tiktok_data['campaign_id_uniq'] = get_uniq_id(df_mp, tiktok_data)
    # ---------------------------------------------------------------------------------------------------------
    # Для яндекс директа токены доступа запрашиваются один раз для каждого аккаунта и потом сохраняются в файл по пути "data/yand_oath.csv", где первый столбец - имя аккаунта, второй столбец - токен
    
    st.text('Выгрузка Яндекс Директ')
    yandex_mp_df = df_mp[df_mp['channel'] == 'Яндекс Директ']
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'
    CampaignsURL = 'https://api.direct.yandex.com/json/v5/campaigns'

    campaign_id_list = yandex_mp_df['campaign_id_uniq'].to_list()
    for i in range(len(campaign_id_list)):
        campaign_id_list[i] = campaign_id_list[i].split(', ')
    campaign_id_list = list(chain.from_iterable(campaign_id_list))

    yandex_data = pd.DataFrame()
    yandex_data_reach = pd.DataFrame()
    for id in yandex_mp_df['customer_account_id'].unique():
        st.toast('Выгрузка данных по аккауту с id ' + id)
        df_yandex = req_data(oath_data[id], id, ReportsURL, start_date, end_date)
        df_yandex_reach = req_data_reach(oath_data[id], id, ReportsURL, start_date, end_date)
        yandex_data = pd.concat([yandex_data, df_yandex])
        if df_yandex_reach.shape[0] > 0:
            yandex_data_reach = pd.concat([yandex_data_reach, df_yandex_reach])

    yandex_data['Date'] = pd.to_datetime(yandex_data['Date'])
    yandex_data['CampaignId'] = yandex_data['CampaignId'].astype(str)
    yandex_data.columns = ['date', 'campaign_name', 'campaign_id', 'impressions', 'clicks', 'cost']

    yandex_data_reach['Date'] = pd.to_datetime(yandex_data_reach['Date'])
    yandex_data_reach['CampaignId'] = yandex_data_reach['CampaignId'].astype(str)
    yandex_data_reach.columns = ['date', 'campaign_id', 'reach', 'frequency']

    yandex_data['channel'] = 'Яндекс Директ'
    yandex_data = yandex_data[yandex_data['campaign_id'].isin(campaign_id_list)]
    yandex_data['cost'] = yandex_data['cost'] / 1000000
    yandex_data['cost'] = yandex_data['cost'] /475
    yandex_data = yandex_data.merge(yandex_data_reach, how='left')
    yandex_data['campaign_id_uniq'] = get_uniq_id(df_mp, yandex_data)
    # ---------------------------------------------------------------------------------------------------------
    # выгрузка данных с гугл аналитики. Кабинеты гугл аналитики указываются в файле "data/google_streams.csv"
    # так же используется файл "Quickstart-3c9a44cbc5e0.json".
    # для того, чтобы у скрипта был доступ к кабинету, нужно в кабинете гугл аналитики добавить пользователя по следующей электронной почте:
    # почта - starting-account-oflyp8n8dsxt@quickstart-1691598272290.iam.gserviceaccount.com
    # достаточно уровня доступа "читатель"
    google_analytics_ids = pd.read_csv('data/google_streams.csv', header=None)
    google_analytics_ids = google_analytics_ids[0].to_list()
    google_analytics_ids = prepare_camps(google_analytics_ids)
    google_analytics_ids = list(chain.from_iterable(google_analytics_ids))
    ga4_data = pd.DataFrame()
    for i in google_analytics_ids:
        st.toast('stream ' + i)
        ga4_d = get_google_analytics_data(property_id=i,
                                                start_date=start_date,
                                                end_date=end_date)
        ga4_data = pd.concat([ga4_data, ga4_d])
    
    ga4_data['date'] = str_to_date(ga4_data['date'])
    ga4_data['date'] = pd.to_datetime(ga4_data['date'])
    ga4_data['conversions'] = ga4_data['conversions'].astype('int64')
    # ---------------------------------------------------------------------------------------------------------
    # объединение всех выгруженных данных
    # итого все данные сохраняются в файле api_data.pkl и файле df_mp.pkl
    
    api_data = pd.concat([google_data, meta_data, tiktok_data, yandex_data])
    api_data['date'] = pd.to_datetime(api_data['date'])
    api_data['clicks'] = api_data['clicks'].astype('int')
    api_data['impressions'] = api_data['impressions'].astype('int')
    api_data['reach'] = api_data['reach'].fillna(0)
    api_data['frequency'] = api_data['frequency'].fillna(0)
    api_data['reach'] = api_data['reach'].astype('int')
    api_data['frequency'] = api_data['frequency'].astype('float')
    api_data['cost'] = api_data['cost'].astype('float')

    api_data = api_data.merge(ga4_data.groupby(['date', 'campaign_id']).aggregate({'conversions': 'sum'}).reset_index(), how='left')
    api_data.loc[(api_data['objective'].isin(['LEAD_GENERATION', 'OUTCOME_LEADS'])), 'conversions'] = api_data['conversions_fb']

    mp_df_t = df_mp[['campaign_id_uniq', 'unit_cost_exVAT_USD']].copy()
    mp_df_t['mp_id'] = mp_df_t['campaign_id_uniq'].astype('str')
    api_data['mp_id'] = api_data['campaign_id_uniq'].astype('str')
    mp_df_t = mp_df_t.drop(columns='campaign_id_uniq')
    mp_df_t.columns = ['unit_cost_plan_usd', 'mp_id']
    api_data = api_data.merge(mp_df_t)
    api_data = api_data.drop(columns='mp_id')
    api_data = pd.concat([df_api, api_data])
    api_data = api_data.drop_duplicates(subset=['date', 'campaign_id'])
    api_data.to_pickle(path + 'data/api_data.pkl')
