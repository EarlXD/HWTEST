import re
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
                                                DateRange,
                                                Dimension,
                                                Metric,
                                                RunReportRequest)
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adsinsights import AdsInsights
import json
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa
import requests
import random
from io import StringIO
from time import sleep
import datetime



def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str, PATH, ACCESS_TOKEN):
    # type: (str, str, str) -> dict
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()

def prepare_ids(x, y):
    if type(x) == list:
        for i in range(len(x)):
            if y[i] == 'Яндекс Директ':
                continue
            x[i] = str(re.sub("[^0-9]", "", x[i]))
        return x
    else:
        if y == 'Яндекс Директ':
            return x
        x = str(re.sub("[^0-9]", "", x))
        return x

def prepare_camps(x):
    for i in range(len(x)):
        if ',' in x[i]:
            y = x[i].split(',')
            for j in range(len(y)):
                y[j] = y[j].strip()
            x[i] = y
        else:
            x[i] = [str(re.sub("[^0-9]", "", x[i]))]
    return x

def redact_yaml(id):
    with open('google-ads.yaml', 'r') as f:
        lines = f.readlines()
        for i in range(len(lines)):
            if 'login_customer_id:' in lines[i]:
                lines[i] = 'login_customer_id: ' + id + '\n'
    with open('google-ads.yaml', 'w') as f:
        f.writelines(lines)

def get_google_ads_data(seed_customer_ids, start_date, end_date):
    query = """
          SELECT
            customer_client.client_customer,
            customer_client.level,
            customer_client.manager,
            customer_client.descriptive_name,
            customer_client.currency_code,
            customer_client.time_zone,
            customer_client.id,
            customer_client.status
          FROM customer_client"""
          
    client = GoogleAdsClient.load_from_storage(path='google-ads.yaml', version="v15")
    ga_service = client.get_service("GoogleAdsService")

    customer_ids = []
    customer_levels = []
    manager_bool = []
    customer_status = []
    customer_descriptive_name = []
    customer_df = pd.DataFrame()
    for seed_customer_id in seed_customer_ids:
          unprocessed_customer_ids = [seed_customer_id]
          while unprocessed_customer_ids:
                  customer_id = int(unprocessed_customer_ids.pop(0))
                  response = ga_service.search(customer_id=str(customer_id), query=query)
                  for googleads_row in response:
                        customer_client = googleads_row.customer_client
                        customer_ids.append(customer_client.id)
                        customer_levels.append(customer_client.level)
                        manager_bool.append(customer_client.manager)
                        customer_status.append(customer_client.status)
                        customer_descriptive_name.append(customer_client.descriptive_name)
    customer_df['id'] = customer_ids
    customer_df['level'] = customer_levels
    customer_df['manager_bool'] = manager_bool
    customer_df['status'] = customer_status
    customer_df['descriptive_name'] = customer_descriptive_name
    customer_df['id'] = customer_df['id'].astype(str)
    customer_child_df = customer_df[(~customer_df['manager_bool'])&(customer_df['status'] != 3)]

    query = f"""
            SELECT
              campaign.id,
              campaign.name,
              campaign.advertising_channel_type,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.unique_users,
              metrics.average_impression_frequency_per_user,
              metrics.video_views,
              segments.date
            FROM campaign WHERE segments.date BETWEEN '{start_date}' AND  '{end_date}'
            ORDER BY metrics.impressions DESC"""
    campaigns = []
    campaigns_id = []
    ad_groups = []
    campaign_type = []
    impressions = []
    clicks = []
    cost_micros = []
    dates = []
    cust_names = []
    unique_users = []
    freq_impr = []
    video_views = []

    customer_child_id = customer_child_df['id'].to_list()
    customer_child_descriptive = customer_child_df['descriptive_name'].to_list()

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    for elem, id in enumerate(customer_child_id):
        name_cust = customer_child_descriptive[elem]
        search_request.customer_id = id
        search_request.query = query
        stream = ga_service.search_stream(search_request)
        for batch in stream:
            for row in batch.results:
                campaign = row.campaign
                ad_group = row.ad_group
                metrics = row.metrics
                date = row.segments
                campaigns.append(campaign.name)
                campaigns_id.append(campaign.id)
                campaign_type.append(campaign.advertising_channel_type)
                ad_groups.append(ad_group.name)
                dates.append(date.date)
                impressions.append(metrics.impressions)
                clicks.append(metrics.clicks)
                cost_micros.append(metrics.cost_micros)
                unique_users.append(metrics.unique_users)
                freq_impr.append(metrics.average_impression_frequency_per_user)
                video_views.append(metrics.video_views)
                cust_names.append(name_cust)

    df_parse_data = pd.DataFrame({'campaign_name':campaigns,
                                  'campaign_id': campaigns_id,
                                  'campaign_type': campaign_type,
                                  'impressions': impressions,
                                  'clicks': clicks,
                                  'video_views': video_views,
                                  'cost': cost_micros,
                                  'date': dates,
                                  'reach': unique_users,
                                  'frequency': freq_impr,
                                  'customer_name': cust_names})

    df2 = df_parse_data.copy()

    df2['cost'] = df2['cost'] / 1000000
    df2['date'] = pd.to_datetime(df2['date'])
    return df2

def get_facebook_ads_data(ad_account_id, start_date, end_date, campaigns_list):
    """
        чтобы получить access token, нужно нажать на кнопку здесь: https://developers.facebook.com/tools/explorer
        чтобы продлить токен на два месяца, нужно пройти по этой ссылке:
        https://graph.facebook.com/v17.0/oauth/access_token?grant_type=fb_exchange_token&client_id=255357527450811&client_secret=2c4c6038cb8555686828113b33e6ba00&fb_exchange_token=EAADoPw625LsBOyFoxgomgZCduL3uqOecwmyVClnc86FyTH1FWZAZBvSghSZCTzIAWIAOZCbwp6EXg0kwXqkAn2ZBCRkcMwQwZCv8Q3g5VFgRMpjGn22Q6vwCBzIdfobwgtEI6KZABzCd7qOCTOzIKOMtQfYs5BOTEm2jtYUg4MZBeD7ZCTsJ1mJWjbBAobdSOrFZBNUIr0E8VcB4zarfG5p8opESW2qZA0vQGz1m4e1A3OpmXw52cJnJQ5ADwjhFKpQZD
        где XXX - это короткий токен
    """
    
    
    obj_dict = {'LINK_CLICKS': 'link_click',
                'OUTCOME_LEADS': 'lead',
                'LEAD_GENERATION': 'lead'}
    
    my_app_id = 255357527450811
    my_app_secret = '2c4c6038cb8555686828113b33e6ba00'
    my_access_token = 'EAADoPw625LsBOyFlXTAFvl73cRZBPeDPLYhoSmcovVr91vZCsHoWsgOeiaa2zlxX3RrlMnLuNZAtcbwpPNa3XPLZB3C5hStWBXUYclB0oZC40jZB7G4gqHOVOs340w3dYVLYOABYZAnufq28dnGESlpQZA92JIZA1Ixat1PGDP9SC5LlZAunSZBYS5gXQZDZD'
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    account = AdAccount(ad_account_id)
    
    # Получаем все кампании аккаунта
    campaigns = account.get_campaigns(
      fields=[
        Campaign.Field.id,
        Campaign.Field.name,
        Campaign.Field.objective,
        Campaign.Field.status,
      ],
    )
    
    cpn_list = []
    cpn_id = []
    impr_list = []
    clicks_list = []
    spend_list = []
    frequency_list = []
    reach_list = []
    date = []
    objective = []
    actions = []
    for campaign in campaigns:
        if campaign['id'] in campaigns_list:
            # print("Кампания:", campaign['name'], campaign['id'])
            insights = campaign.get_insights(params={
                                                     'time_increment': 1,
                                                     'fields': [
                                                         AdsInsights.Field.impressions,
                                                         AdsInsights.Field.clicks,
                                                         AdsInsights.Field.spend,
                                                         AdsInsights.Field.frequency,
                                                         AdsInsights.Field.reach,
                                                         AdsInsights.Field.objective,
                                                         AdsInsights.Field.actions,
                                                     ],
                                                     'time_range': {
                                                         'since': start_date,
                                                         'until': end_date
                                                     },
                                                     'dimensions': 'days_1'
                                                    })
            for insight in insights:
                if int(insight[AdsInsights.Field.impressions]) > 0:
                    cpn_list.append(campaign['name'])
                    cpn_id.append(campaign['id'])
                    impr_list.append(insight[AdsInsights.Field.impressions])
                    clicks_list.append(insight[AdsInsights.Field.clicks])
                    spend_list.append(insight[AdsInsights.Field.spend])
                    frequency_list.append(insight[AdsInsights.Field.frequency])
                    reach_list.append(insight[AdsInsights.Field.reach])
                    date.append(insight[AdsInsights.Field.date_start])
                    objective.append(insight[AdsInsights.Field.objective])
                    try:
                        actions.append(insight[AdsInsights.Field.actions])
                    except:
                        actions.append(0)
                        
            # insights = campaign.get_insights(params={
            #                                 #  'time_increment': 1,
            #                                  'fields': [
            #                                      AdsInsights.Field.frequency,
            #                                      AdsInsights.Field.reach,
            #                                  ],
            #                                  'time_range': {
            #                                      'since': start_date,
            #                                      'until': end_date
            #                                  },
            #                                 #  'dimensions': 'days_1'
            #                                 })
    df = pd.DataFrame({'campaign_name': cpn_list,
                       'campaign_id': cpn_id,
                       'impressions': impr_list,
                       'clicks': clicks_list,
                       'cost': spend_list,
                       'frequency': frequency_list,
                       'reach': reach_list,
                       'date': date,
                       'objective': objective,
                       'actions': actions
                       })
    # df['reach_for_per'] = insights[0][AdsInsights.Field.reach]
    obj_type_list = df['objective'].to_list()
    act_val_list = df['actions'].to_list()
    conversions = []
    for j in range(len(act_val_list)):
        x = obj_type_list[j]
        flag=0
        if act_val_list[j] == 0:
            conversions.append(0)
            continue
        for i in act_val_list[j]:
            try:
                if i['action_type'] == obj_dict[x]:
                    conversions.append(i['value'])
                    flag = 1
            except:
                pass
        if flag == 0:
            conversions.append(0)

    df['conversions_fb'] = conversions
    df = df.drop(columns='actions')
    df['conversions_fb'] = df['conversions_fb'].astype(int)
    return df

def get_tiktok_ads_data(advertiser_id, start_date, end_date):
    ACCESS_TOKEN = "9117ac4ef4a30d24bb67e8a50656b082bbac503e"
    PATH = "/open_api/v1.3/report/integrated/get/"
    dimensions_list = [
                       "campaign_id",
                       "stat_time_day"
                       ]
    dimensions = json.dumps(dimensions_list)
    # met_list = ['spend', 'impressions', 'clicks', 'ctr', 'conversion', 'cost_per_conversion', 'campaign_name']
    met_list = ['spend', 'impressions', 'clicks', 'campaign_name', 'reach', 'frequency']
    metrics = json.dumps(met_list)
    
    page_size = 1000
    report_type = 'BASIC'
    page = 1

    data_level = "AUCTION_AD"
    data_level = "AUCTION_CAMPAIGN"
    
    my_args = {
      "dimensions": dimensions,
      "metrics": metrics,
      "start_date": start_date,
      "end_date": end_date,
      "page_size": page_size,
      "advertiser_id": advertiser_id,
      "report_type": report_type,
      "page": page,
      "data_level": data_level,
    }
    my_args = json.dumps(my_args)
    x = (get(my_args, PATH, ACCESS_TOKEN))
    # print(x)
    tiktok_data_df = pd.DataFrame()
    campaign_id_list = []
    date_list = []
    for i in x['data']['list']:
        tiktok_data_df = pd.concat([tiktok_data_df, pd.DataFrame(i['metrics'], index=[0])])
        campaign_id_list.append(i['dimensions']['campaign_id'])
        date_list.append(i['dimensions']['stat_time_day'])
    tiktok_data_df['campaign_id'] = campaign_id_list
    tiktok_data_df['date'] = date_list
    return tiktok_data_df

def u(x):
    if type(x) == type(b''):
        return x.decode('utf8')
    else:
        return x

def normalize_Data(df):
        """
        Нормализация и очистка данных отчета.

        Аргументы:
        df (DataFrame): DataFrame pandas с данными отчета.

        Возвращает:
        DataFrame: Очищенный и нормализированный DataFrame pandas.
        """

        #получаем список колонок с конверсиями
        conversionsCols = []

        #меняем -- на нули и переводим в инт
        for col in df.columns:
            if 'Conversions' in col:
                df[col] = df[col].replace('--','0').astype(int)
                conversionsCols.append(col)

        df.fillna('--',inplace=True)

        # создаем столбец, который будет суммировать все столбцы с conversions в названии
        df['Conversions'] = df[conversionsCols].sum(axis=1)

        # удаляем все столбцы с conversions в названии, кроме 'Conversions'
        for col in conversionsCols:
            if col != 'Conversions':
                df = df.drop(columns=[col])
        return df
    
def req_camp(token, clientLogin, CampaignsURL):
    headers = {"Authorization": "Bearer " + token,  # OAuth-токен. Использование слова Bearer обязательно
               "Client-Login": clientLogin,  # Логин клиента рекламного агентства
               "Accept-Language": "ru",  # Язык ответных сообщений
               }
    body = {"method": "get",  # Используемый метод.
            "params": {"SelectionCriteria": {},  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                       "FieldNames": ["Id", "Name"]  # Имена параметров, которые требуется получить.
                        }}
    jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
    try:
        result = requests.post(CampaignsURL, jsonBody, headers=headers)
        if result.status_code != 200 or result.json().get("error", False):
            print("Произошла ошибка при обращении к серверу API Директа.")
            print("Код ошибки: {}".format(result.json()["error"]["error_code"]))
            print("Описание ошибки: {}".format(u(result.json()["error"]["error_detail"])))
            print("RequestId: {}".format(result.headers.get("RequestId", False)))
        else:
            # print("RequestId: {}".format(result.headers.get("RequestId", False)))
            # print("Информация о баллах: {}".format(result.headers.get("Units", False)))
            # # Вывод списка кампаний
            # for campaign in result.json()["result"]["Campaigns"]:
            #     print("Рекламная кампания: {} №{}".format(u(campaign['Name']), campaign['Id']))
            

            if result.json()['result'].get('LimitedBy', False):
                # Если ответ содержит параметр LimitedBy, значит,  были получены не все доступные объекты.
                # В этом случае следует выполнить дополнительные запросы для получения всех объектов.
                # Подробное описание постраничной выборки - https://tech.yandex.ru/direct/doc/dg/best-practice/get-docpage/#page
                print("Получены не все доступные объекты.")
            yand_camp_df = pd.DataFrame()
            for camp in result.json()['result']['Campaigns']:
                yand_camp_df = pd.concat([yand_camp_df, pd.DataFrame(camp, index=[0])])
            return yand_camp_df
    except ConnectionError:
        # В данном случае мы рекомендуем повторить запрос позднее
        print("Произошла ошибка соединения с сервером API.")

    # Если возникла какая-либо другая ошибка
    # except:
    #     # В данном случае мы рекомендуем проанализировать действия приложения
    #     print("Произошла непредвиденная ошибка.")

def req_data(token, clientLogin, ReportsURL, start_date, end_date):
    headers = {
               # OAuth-токен. Использование слова Bearer обязательно
               "Authorization": "Bearer " + token,
               # Логин клиента рекламного агентства
               "Client-Login": clientLogin,
               # Язык ответных сообщений
               "Accept-Language": "ru",
               # Режим формирования отчета
               "processingMode": "auto",
               # Формат денежных значений в отчете
               # "returnMoneyInMicros": "false",
               # Не выводить в отчете строку с названием отчета и диапазоном дат
               "skipReportHeader": "true",
               # Не выводить в отчете строку с названиями полей
               # "skipColumnHeader": "true",
               # Не выводить в отчете строку с количеством строк статистики
               "skipReportSummary": "true"
               }
    reportNumber = random.randrange(1, 200000)
    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": start_date,
                "DateTo": end_date
            },
            "FieldNames": [
                "Date",
                "CampaignName",
                "CampaignId",
                "Impressions",
                "Clicks",
                "Cost",
            ],
            "ReportName": f'Отчет №{reportNumber}',
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            # "ReportType": "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }
    body = json.dumps(body, indent=4)
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            
            elif req.status_code == 200:
                # print("Отчет создан успешно")
                # print("RequestId: {}".format(req.headers.get("RequestId", False)))
                # print("Содержание отчета: \n{}".format(u(req.text)))
                df = pd.read_csv(StringIO(req.text), sep='\t')
                return df

            elif req.status_code == 201:
                # print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                # print("Повторная отправка запроса через {} секунд".format(retryIn))
                # print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            
            elif req.status_code == 202:
                # print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                # print("Повторная отправка запроса через {} секунд".format(retryIn))
                # print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break
      
def req_data_reach(token, clientLogin, ReportsURL, start_date, end_date):
    headers = {
               # OAuth-токен. Использование слова Bearer обязательно
               "Authorization": "Bearer " + token,
               # Логин клиента рекламного агентства
               "Client-Login": clientLogin,
               # Язык ответных сообщений
               "Accept-Language": "ru",
               # Режим формирования отчета
               "processingMode": "auto",
               # Формат денежных значений в отчете
               # "returnMoneyInMicros": "false",
               # Не выводить в отчете строку с названием отчета и диапазоном дат
               "skipReportHeader": "true",
               # Не выводить в отчете строку с названиями полей
               # "skipColumnHeader": "true",
               # Не выводить в отчете строку с количеством строк статистики
               "skipReportSummary": "true"
               }
    reportNumber = random.randrange(1, 200000)
    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": start_date,
                "DateTo": end_date
            },
            "FieldNames": [
                "Date",
                "CampaignId",
                'ImpressionReach',
                'AvgImpressionFrequency'
            ],
            "ReportName": f'Отчет №{reportNumber}',
            "ReportType": "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }
    body = json.dumps(body, indent=4)
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            
            elif req.status_code == 200:
                # print("Отчет создан успешно")
                # print("RequestId: {}".format(req.headers.get("RequestId", False)))
                # print("Содержание отчета: \n{}".format(u(req.text)))
                df = pd.read_csv(StringIO(req.text), sep='\t')
                return df

            elif req.status_code == 201:
                # print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                # print("Повторная отправка запроса через {} секунд".format(retryIn))
                # print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            
            elif req.status_code == 202:
                # print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                # print("Повторная отправка запроса через {} секунд".format(retryIn))
                # print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break
  
def read_oath(main_book):
    oath_sheet = main_book.sheets('OATH')
    clientLogin_list = oath_sheet.range('A2').expand('down').value
    token_list = oath_sheet.range('B2').expand('down').value
    oath_data = dict(zip(clientLogin_list, token_list))
    return oath_data

def get_uniq_id(mp_df, api_data):
    list_camp_id_mp = mp_df['campaign_id_uniq'].to_list()
    list_camp_id_api = api_data['campaign_id'].to_list()
    new_list = []
    for i in range(len(list_camp_id_api)):
        flag = None
        x = list_camp_id_api[i]
        for j in range(len(list_camp_id_mp)):
            y = list_camp_id_mp[j]
            if x in y:
                new_list.append(y)
                flag = 1
                break
        if flag == None:
            new_list.append(None)
    return new_list

def str_to_date(x):
    new_x = []
    for i in x:
        date = datetime.date(int(i[:4]), int(i[4:6]), int(i[6:]))
        new_x.append(date)
    return new_x

def get_google_analytics_data(property_id, start_date, end_date):
    credentials = service_account.Credentials.from_service_account_file('Quickstart-3c9a44cbc5e0.json')
    client = BetaAnalyticsDataClient(credentials=credentials)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        limit = 400000,
        dimensions=[
                    Dimension(name="date"), 
                    Dimension(name='firstUserCampaignId'),
                    Dimension(name='firstUserCampaignName'),
                    Dimension(name='eventName'),
                    ],
        metrics=[Metric(name="conversions"),
                 Metric(name='eventCountPerUser')
                 ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                               )
    response = client.run_report(request)

    df = pd.DataFrame()
    dates_list = []
    firstUserCampaignId_list = []
    firstUserCampaignName_list = []
    conversions_list = []
    test_list = []
    for row in response.rows:
        date = row.dimension_values[0].value
        firstUserCampaignId = row.dimension_values[1].value
        firstUserCampaignName = row.dimension_values[2].value
        test = row.dimension_values[3].value
        
        conversions = row.metric_values[0].value
        
        dates_list.append(date)
        firstUserCampaignId_list.append(firstUserCampaignId)
        firstUserCampaignName_list.append(firstUserCampaignName)
        conversions_list.append(conversions)
        test_list.append(test)
        
    df['date'] = dates_list
    df['campaign_id'] = firstUserCampaignId_list
    df['campaign_name'] = firstUserCampaignName_list
    df['conversions'] = conversions_list
    df['test'] = test_list
    return df
    # df['analytics_name'] = 
    
    
camp_type_dict = {2: 'Search',
                  4: 'Shopping',
                  9: 'Smart',
                  3: 'Display',
                  6: 'Video'}