import pandas as pd
import numpy as np

def read_and_prepare_df(path):
    df_mp = pd.read_csv(path + 'data/mp_data.csv')
    df_mp = df_mp.rename(columns={'campaign_id': 'campaign_id_uniq'})
    
    df_mp['start_date'] = pd.to_datetime(df_mp['start_date'])
    df_mp['end_date'] = pd.to_datetime(df_mp['end_date'])
    
    df_api = pd.read_csv(path + 'data/api_data.csv')
    df_api['date'] = pd.to_datetime(df_api['date']).dt.date
    df_api = df_api.merge(df_mp[['campaign_id_uniq', 'clients', 'unit_type']])
    df_api = df_api.rename(columns={'channel':'Платформа',
                                    'impressions': 'Показы',
                                    'clicks': 'Клики',
                                    'cost': 'Расходы',
                                    'ctr': 'CTR',
                                    'conversions': 'Конверсии',
                                    'cost_per_conversion': 'CPC',
                                    'date': 'ДАТА',
                                    'clients': 'Клиент',
                                    'reach': 'Охват',
                                    'frequency': 'Частота'})
    df_api.loc[df_api['unit_type'] == 'click', 'unit'] = df_api['Клики']
    df_api.loc[df_api['unit_type'] == '1000 Impressions', 'unit'] = df_api['Показы'] / 1000
    return df_mp, df_api

def format_table(table, currency_state, cur_kzt_usd):
    table['Расходы'] = table['Расходы'].round(2)
    table['Расходы с НДС'] = table['Расходы'] * 1.12
    table['CPC'] = table['Расходы'] / table['Конверсии']
    table['CTR'] = table['Клики'] / table['Показы'] * 100
    table['Сред. цена за клик'] = table['Расходы']/table['Клики']
    table['CR'] = table['Конверсии'] / table['unit'] * 100
    
    if currency_state == 'USD':
        table['Расходы'] = table['Расходы'].apply(lambda x: "${:,.1f}".format((x)))
        table['CPC'] = table['CPC'].apply(lambda x: "${:,.1f}".format((x)))
        table['Сред. цена за клик'] = table['Сред. цена за клик'].apply(lambda x: "${:,.2f}".format((x)))
        table['Расходы с НДС'] = table['Расходы с НДС'].apply(lambda x: "${:,.2f}".format((x)))
    else:
        table['Расходы'] = table['Расходы'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['CPC'] = table['CPC'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['Сред. цена за клик'] = table['Сред. цена за клик'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
        table['Расходы с НДС'] = table['Расходы с НДС'].apply(lambda x: "₸{:,.1f}".format((x*cur_kzt_usd)))
    
    table['Показы'] = table['Показы'].apply(lambda x: "{:,.0f}".format((x)))
    table['Клики'] = table['Клики'].apply(lambda x: "{:,.0f}".format((x)))
    table['CTR'] = table['CTR'].apply(lambda x: "{:,.2f}%".format((x)))
    table['CR'] = table['CR'].apply(lambda x: "{:,.2f}%".format((x)))
    
    table = table.replace('$nan', np.NaN)
    table = table.replace('₸nan', np.NaN)
    table = table.replace('nan%', np.NaN)
    table = table[['Клиент', 'Платформа', 'Показы', 'Клики', 'CTR',
               'Сред. цена за клик', 'Расходы', 'Расходы с НДС',
               'Охват', 'Частота', 'Конверсии', 'CR', 'CPC']]
    
    return table