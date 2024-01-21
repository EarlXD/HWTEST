import pandas as pd
import numpy as np

def read_and_prepare_df(path):
    df_mp = pd.read_csv(path + 'data/mp_data.csv')
    df_mp = df_mp.rename(columns=rename_mp_df_dict)
    
    df_mp['start_date'] = pd.to_datetime(df_mp['start_date'])
    df_mp['end_date'] = pd.to_datetime(df_mp['end_date'])
    
    df_api = pd.read_csv(path + 'data/api_data.csv')
    df_api['date'] = pd.to_datetime(df_api['date']).dt.date
    df_api = df_api.merge(df_mp[['campaign_id_uniq', 'client', 'unit_type']])
    df_api.loc[df_api['unit_type'] == 'click', 'unit'] = df_api['clicks']
    df_api.loc[df_api['unit_type'] == '1000 Impressions', 'unit'] = df_api['impressions'] / 1000
    return df_mp, df_api

def format_table(table, currency_state, cur_kzt_usd):
    table['cost'] = table['cost'].round(2)
    table['cost_nds'] = table['cost'] * 1.12
    table['CPC'] = table['cost'] / table['conversions']
    table['CTR'] = table['clicks'] / table['impressions'] * 100
    table['avr_cost_per_click'] = table['cost']/table['clicks']
    table['CR'] = table['conversions'] / table['unit'] * 100
    
    if currency_state == 'USD':
        table['cost'] = table['cost'].apply(lambda x: "${:,.1f}".format((x)))
        table['CPC'] = table['CPC'].apply(lambda x: "${:,.1f}".format((x)))
        table['avr_cost_per_click'] = table['avr_cost_per_click'].apply(lambda x: "${:,.2f}".format((x)))
        table['cost_nds'] = table['cost_nds'].apply(lambda x: "${:,.2f}".format((x)))
    else:
        table['cost'] = table['cost'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['CPC'] = table['CPC'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['avr_cost_per_click'] = table['avr_cost_per_click'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['cost_nds'] = table['cost_nds'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
    
    table['impressions'] = table['impressions'].apply(lambda x: "{:,.0f}".format((x)))
    table['clicks'] = table['clicks'].apply(lambda x: "{:,.0f}".format((x)))
    table['CTR'] = table['CTR'].apply(lambda x: "{:,.2f}%".format((x)))
    table['CR'] = table['CR'].apply(lambda x: "{:,.2f}%".format((x)))
    
    table = table.replace('$nan', np.NaN)
    table = table.replace('₸nan', np.NaN)
    table = table.replace('nan%', np.NaN)
    # table = table[['Клиент', 'Платформа', 'Показы', 'Клики', 'CTR',
    #            'Сред. цена за клик', 'Расходы', 'Расходы с НДС',
    #            'Охват', 'Частота', 'Конверсии', 'CR', 'CPC']]
    
    return table


def format_plan_fact_table(table, currency_state, cur_kzt_usd):
    table['impressions'] = table['impressions'].astype('int', errors='ignore')
    return table


rename_dict = {'channel':'Платформа',
               'impressions': 'Показы факт',
               'clicks': 'Клики факт',
               'cost': 'Расходы факт',
               'ctr': 'CTR факт',
               'conversions': 'Конверсии факт',
               'cost_per_conversion': 'CPC факт',
               'date': 'ДАТА',
               'client': 'Клиент',
               'reach': 'Охват факт',
               'frequency': 'Частота факт'}

aggregate_dict = {'impressions': 'sum',
                  'clicks': 'sum',
                  'cost': 'sum',
                  'ctr': 'sum',
                  'conversions': 'sum',
                  'cost_per_conversion': 'sum',
                  'unit': 'sum',
                  'frequency': 'mean',
                  'reach': 'sum'}

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

rename_plan_fact = {'client': 'Клиент',
                    'mp_name': 'Медиаплан',
                    'channel': 'Платформа',
                    'source': 'Тип кампании',
                    'impressions': 'Показы факт',
                    'impressions_plan': 'Показы план',
                    'impressions_perc': 'Показы, %',
                    'clicks': 'Клики факт',
                    'clicks_plan': 'Клики план',
                    'clicks_perc': 'Клики, %',
                    'ctr': 'CTR факт',
                    'ctr_plan': 'CTR план',
                    'conversions': 'Конверсии факт',
                    'conversions_ga_plan': 'Конверсии план',
                    'conversions_perc': 'Конверсии, %',
                    'reach': 'Охват факт',
                    'reach_plan': 'Охват план',
                    'reach_perc': 'Охват, %',
                    'CPM': 'CPM факт',
                    'CPM_plan_USD': 'CPM план',
                    'CPM_perc': 'CPM, %',
                    'start_date': 'Начало',
                    'end_date': 'Окончание'}