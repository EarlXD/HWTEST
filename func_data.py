import pandas as pd
import numpy as np
import datetime
import streamlit as st

# @st.cache_data
def read_and_prepare_df(path):
    df_mp = mp_df_read(path)
    df_api = api_df_read(path, df_mp)
    return df_mp, df_api

def mp_df_read(path):
    df_mp = pd.read_pickle(path + 'data/df_mp.pkl')
    df_mp = df_mp.rename(columns=rename_mp_df_dict)
    # df_mp['start_date'] = pd.to_datetime(df_mp['start_date'])
    # df_mp['end_date'] = pd.to_datetime(df_mp['end_date'])
    return df_mp

def api_df_read(path, df_mp):
    df_api = pd.read_pickle(path + 'data/api_data.pkl')
    df_api['date'] = pd.to_datetime(df_api['date']).dt.date
    df_api = df_api.merge(df_mp[['campaign_id_uniq', 'client', 'unit_type']])
    df_api.loc[df_api['unit_type'] == 'click', 'unit'] = df_api['clicks']
    df_api.loc[df_api['unit_type'] == '1000 Impressions', 'unit'] = df_api['impressions'] / 1000
    return df_api

def clean_column_values(df, column_name):
    df[column_name] = df[column_name].apply(lambda x: str(x).replace("'", "").replace("[", "").replace("]", ""))
    return df


def format_table(table, currency_state, cur_kzt_usd, table_type):
    
    table['cost_nds'] = table['cost'] * 1.12
    table['CPA'] = table['cost'] / table['conversions']
    table['CPC'] = table['cost'] / table['clicks']
    table['CPM'] = table['cost'] / table['impressions'] * 1000
    table['CPV'] = table['cost'] / table['video_views']  
    
    table['conv_coeff'] = table['conversions'] / table['clicks'] * 100
    table['CTR'] = table['clicks'] / table['impressions'] * 100
    if table_type == 'campaign':
        table['campaign_id'] = table['campaign_id'].astype('str')
    
    if currency_state != 'USD':
        table['cost'] = table['cost'] * cur_kzt_usd
        table['cost_nds'] = table['cost_nds'] * cur_kzt_usd
        table['CPA'] = table['CPA'] * cur_kzt_usd
        table['CPC'] = table['CPC'] * cur_kzt_usd
        table['CPM'] = table['CPM'] * cur_kzt_usd
    if table_type == 'table':
        table = table[['client', 'channel', 'impressions', 'clicks', 'video_views', 'CTR',
                       'cost', 'cost_nds', 'conversions', 'conv_coeff', 'CPC', 'CPM', 'CPA', 'CPV',
                       'frequency', 'reach']]
    elif table_type == 'chart':
        table = table[['date', 'impressions', 'clicks', 'CTR', 'video_views',
                       'cost', 'cost_nds', 'conversions', 'conv_coeff', 'CPC',
                       'CPM', 'CPA', 'frequency', 'reach', 'CPV']]
    elif table_type == 'campaign':
        table['start_date'] = table['start_date'].dt.strftime('%Y.%m.%d')
        table['end_date'] = table['end_date'].dt.strftime('%Y.%m.%d')
        table = table[['client', 'mp_name', 'channel', 'campaign_name', 'campaign_id', 'source', 'unit_type', 'start_date',
                       'end_date', 'impressions', 'clicks', 'CTR', 'video_views', 'cost', 'cost_nds',
                       'conversions', 'conv_coeff', 'CPC', 'CPM', 'CPA', 'CPV', 'frequency', 'reach']]
    return table


def format_plan_fact_table(table, currency_state, cur_kzt_usd):
    
    table['cost_nds'] = table['cost'] * 1.12
    table['CPA'] = table['cost'] / table['conversions']
    table['CPC'] = table['cost'] / table['clicks']
    table['CPM'] = table['cost'] / table['impressions'] * 1000
    table['CPV'] = table['cost'] / table['video_views']  
    
    table['conv_coeff'] = table['conversions'] / table['clicks'] * 100
    table['CTR'] = table['clicks'] / table['impressions'] * 100

    if currency_state != 'USD':
        table['cost'] = table['cost'] * cur_kzt_usd
        table['cost_nds'] = table['cost_nds'] * cur_kzt_usd
        table['cost_plan'] = table['cost_exVAT_KZT']
        
        table['CPA'] = table['CPA'] * cur_kzt_usd
        table['CPA_plan'] = table['CPA_plan_KZT']
        
        table['CPC'] = table['CPC'] * cur_kzt_usd
        table['CPC_plan'] = table['CPC_plan_KZT']
        
        table['CPM'] = table['CPM'] * cur_kzt_usd
        table['CPM_plan'] = table['CPM_plan_KZT']
    
    else:
        table['cost_plan'] = table['cost_exVAT_USD']
        table['CPA_plan'] = table['CPA_plan_USD']
        table['CPC_plan'] = table['CPC_plan_USD']
        table['CPM_plan'] = table['CPM_plan_USD']
        
    table['impressions_perc'] = (table['impressions'] / table['impressions_plan'] * 100)
    table['clicks_perc'] = table['clicks'] / table['clicks_plan'] * 100
    table['CPM_perc'] = table['CPM'] / table['CPM_plan'] * 100
    table['CPC_perc'] = table['CPC'] / table['CPC_plan'] * 100
    table['conversions_perc'] = table['conversions'] / table['conversions_ga_plan'] * 100
    table['reach_perc'] = table['reach'] / table['reach_plan'] * 100
    table['cost_perc'] = table['cost'] / table['cost_plan'] * 100
    
    # table['impressions'] = table['impressions'].astype('int', errors='ignore')
    table['ctr_plan'] = table['ctr_plan'] * 100 
    table['today'] = datetime.date.today()
    table['today'] = pd.to_datetime(table['today'])
    table['days'] = (table['today'] - table['start_date']).dt.days
    table['percent'] = table['days']/table['planning_period'] * 100
    table.loc[table['percent'] > 100, 'percent'] = 100
    table['end_date'] = table['end_date'].dt.strftime('%Y.%m.%d')
    table['start_date'] = table['start_date'].dt.strftime('%Y.%m.%d')
    table = table[['client', 'mp_name', 'channel', 'source', 'unit_type', 'percent', 'start_date', 'end_date', 
                   'impressions', 'impressions_plan', 'impressions_perc', 'clicks', 
                   'clicks_plan', 'clicks_perc', 'CTR', 'ctr_plan', 'cost', 'cost_plan', 'cost_perc',
                   'conversions', 'conversions_ga_plan', 'conversions_perc', 'reach', 'reach_plan', 
                   'reach_perc', 'CPC', 'CPC_plan', 'CPC_perc','CPM', 'CPM_plan', 'CPM_perc']].copy()
# filtered_api_df = format_plan_fact_tab
    
    
    return table

def format_plan_fact_table_kcell(table, currency_state, cur_kzt_usd):
    table.loc[table['unit_type'] == 'click', 'unit_fact'] = table['clicks']
    table.loc[table['unit_type'] == '1000 Impressions', 'unit_fact'] = table['impressions'] / 1000
    table['cost'] = table['unit_cost_exVAT_USD'] * table['unit_fact']
    table['cost_nds'] = table['cost'] * 1.12
    table['CPA'] = table['cost'] / table['conversions']
    table['CPC'] = table['cost'] / table['clicks']
    table['CPM'] = table['cost'] / table['impressions'] * 1000
    table['CPV'] = table['cost'] / table['video_views']  
    
    table['conv_coeff'] = table['conversions'] / table['clicks'] * 100
    table['CTR'] = table['clicks'] / table['impressions'] * 100

    if currency_state != 'USD':
        table['cost'] = table['cost'] * cur_kzt_usd
        table['cost_nds'] = table['cost_nds'] * cur_kzt_usd
        table['cost_plan'] = table['cost_exVAT_KZT']
        
        table['CPA'] = table['CPA'] * cur_kzt_usd
        table['CPA_plan'] = table['CPA_plan_KZT']
        
        table['CPC'] = table['CPC'] * cur_kzt_usd
        table['CPC_plan'] = table['CPC_plan_KZT']
        
        table['CPM'] = table['CPM'] * cur_kzt_usd
        table['CPM_plan'] = table['CPM_plan_KZT']
    
    else:
        table['cost_plan'] = table['cost_exVAT_USD']
        table['CPA_plan'] = table['CPA_plan_USD']
        table['CPC_plan'] = table['CPC_plan_USD']
        table['CPM_plan'] = table['CPM_plan_USD']
        
    table['impressions_perc'] = (table['impressions'] / table['impressions_plan'] * 100)
    table['clicks_perc'] = table['clicks'] / table['clicks_plan'] * 100
    table['CPM_perc'] = table['CPM'] / table['CPM_plan'] * 100
    table['CPC_perc'] = table['CPC'] / table['CPC_plan'] * 100
    table['conversions_perc'] = table['conversions'] / table['conversions_ga_plan'] * 100
    table['reach_perc'] = table['reach'] / table['reach_plan'] * 100
    table['cost_perc'] = table['cost'] / table['cost_plan'] * 100
    
    # table['impressions'] = table['impressions'].astype('int', errors='ignore')
    table['ctr_plan'] = table['ctr_plan'] * 100 
    table['today'] = datetime.date.today()
    table['today'] = pd.to_datetime(table['today'])
    table['days'] = (table['today'] - table['start_date']).dt.days
    table['percent'] = table['days']/table['planning_period'] * 100
    table.loc[table['percent'] > 100, 'percent'] = 100
    table['end_date'] = table['end_date'].dt.strftime('%Y.%m.%d')
    table['start_date'] = table['start_date'].dt.strftime('%Y.%m.%d')
    table = table[['client', 'mp_name', 'channel', 'source', 'unit_type', 'percent', 'start_date', 'end_date', 
                   'impressions', 'impressions_plan', 'impressions_perc', 'clicks', 
                   'clicks_plan', 'clicks_perc', 'CTR', 'ctr_plan', 'cost', 'cost_plan', 'cost_perc',
                   'conversions', 'conversions_ga_plan', 'conversions_perc', 'reach', 'reach_plan', 
                   'reach_perc', 'CPC', 'CPC_plan', 'CPC_perc','CPM', 'CPM_plan', 'CPM_perc']].copy()
# filtered_api_df = format_plan_fact_tab
    
    
    return table

main_col_dict = {
               'date': 'ДАТА',
               'channel':'Источник',
               'impressions': 'Показы факт',
               'clicks': 'Клики факт',
               'CTR': 'CTR факт, %',
               'cost': 'Расходы факт',
               'cost_nds': 'Расходы с НДС',
               'conversions': 'Конверсии факт',
               'conv_coeff': 'Коэфф.конверсии',
               'CPC': 'CPC факт',
               'CPM': 'CPM факт',
               'CPA': 'CPA факт',
               'client': 'Клиент',
               'frequency': 'Частота факт',
               'reach': 'Охват факт',
               'video_views': 'Просмотры факт',
               'CPV': 'CPV факт',
               'source': 'Тип кампании',
               'mp_name': 'Проект                ',
               'unit_type': 'Ед. оплаты',
               'start_date': 'Начало      ',
               'end_date': 'Окончание',
               'campaign_name': 'Название рекл. кампании',
               'campaign_id': 'id рекл. кампании'
               }

rename_mp_df_dict = {'campaign_id': 'campaign_id_uniq',
                     'Unit cost, excluding VAT, ₸/PLAN': 'unit_cost_exVAT_KZT',
                     'Unit cost, excluding VAT, $/PLAN': 'unit_cost_exVAT_USD',
                     'Cost total, excluding VAT, ₸/PLAN': 'cost_exVAT_KZT',
                     'Cost total, excluding VAT, $ / PLAN': 'cost_exVAT_USD',
                     'Click PLAN': 'clicks_plan',
                     'Impressions PLAN': 'impressions_plan',
                     'Coverage PLAN': 'reach_plan',
                     'Frequency plan': 'frequency_plan',
                     'CTR/PLAN': 'ctr_plan',
                     'Conversions GA / PLAN': 'conversions_ga_plan',
                     'Conversions_Meta_Leads': 'conversions_metaleads_plan',
                     'Conversions_Meta_Messages': 'conversoions_metamessages_plan',
                     'CR / PLAN': 'CR_plan',
                     'CPM/PLAN, тг': 'CPM_plan_KZT',
                     'CPM/PLAN, $': 'CPM_plan_USD',
                     'CPC/PLAN,тг': 'CPC_plan_KZT',
                     'CPC/PLAN, $': 'CPC_plan_USD',
                     'CPA/PLAN, ₸': 'CPA_plan_KZT',
                     'CPA/PLAN, $': 'CPA_plan_USD'}

rename_plan_fact = {'client': 'Клиент     ',
                    'mp_name': 'Проект                ',
                    'channel': 'Источник',
                    'source': 'Тип кампании',
                    'impressions': 'Показы факт',
                    'impressions_plan': 'Показы план',
                    'impressions_perc': 'Показы, %',
                    'clicks': 'Клики факт',
                    'clicks_plan': 'Клики план',
                    'clicks_perc': 'Клики, %',
                    'CTR': 'CTR факт %',
                    'ctr_plan': 'CTR план %',
                    'conversions': 'Конверсии факт',
                    'conversions_ga_plan': 'Конверсии план',
                    'conversions_perc': 'Конверсии, %',
                    'reach': 'Охват факт',
                    'reach_plan': 'Охват план',
                    'reach_perc': 'Охват, %',
                    'CPC': 'CPC факт',
                    'CPC_plan': 'CPC план',
                    'CPC_perc': 'CPC, %',
                    'CPM': 'CPM факт',
                    'CPM_plan': 'CPM план',
                    'CPM_perc': 'CPM, %',
                    'start_date': 'Начало      ',
                    'end_date': 'Окончание',
                    'cost': 'Расходы факт',
                    'cost_plan': 'Расходы план', 
                    'percent': 'Общий процент',
                    'unit_type': 'Ед. оплаты',
                    'cost_perc': 'Расходы, %',
                    }

aggregate_dict = {'impressions': 'sum',
                  'clicks': 'sum',
                  'cost': 'sum',
                  'conversions': 'sum',
                  'unit': 'sum',
                  'frequency': 'mean',
                  'reach': 'sum',
                  'video_views': 'sum'}

aggregate_dict_campaign = {'impressions': 'sum',
                           'clicks': 'sum',
                           'cost': 'sum',
                           'conversions': 'sum',
                           'unit': 'sum',
                           'frequency': 'mean',
                           'reach': 'sum',
                           'campaign_id_uniq': 'first',
                           'campaign_name': 'first',
                           'video_views': 'sum',
                           'campaign_type': 'first'}

metrics = ['Клики факт', 'Показы факт', 'Расходы факт',
           'CTR факт', 'CPC факт', 'CPM факт', 'CPA факт', 'CPV факт',
           'Конверсии факт', 'Охват факт', 'Частота факт']

num_format_main = ['Показы факт', 'Клики факт', 'Конверсии факт', 'Охват факт', 'Просмотры факт']
num_format_perc_main = ['CTR факт, %', 'Коэфф.конверсии']
num_format_float_main = ['Частота факт']
num_format_money_main = ['Расходы факт', 'CPM факт', 'Расходы с НДС', 'CPC факт', 'CPA факт', 'CPV факт']

num_format = ['Показы факт', 'Показы план', 'Клики факт', 'Клики план', 
              'Конверсии факт', 'Конверсии план', 'Охват факт', 'Охват план']
num_format_perc = ['Показы, %', 'Клики, %', 'Конверсии, %', 'Охват, %', 
                   'Общий процент', 'CTR факт %', 'CTR план %',
                   'CPM, %', 'Расходы, %', 'CPC, %']
num_format_money = ['Расходы факт', 'Расходы план', 'CPM план', 
                    'CPM факт', 'CPC факт', 'CPC план']

num_format_camp = ['Показы факт', 'Клики факт', 'Конверсии факт', 'Охват факт']
num_format_perc_camp = ['CTR факт, %']
num_format_money_camp = ['Расходы факт', 'CPM факт']