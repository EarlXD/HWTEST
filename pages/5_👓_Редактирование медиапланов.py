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
import ad_functions_nova
from itertools import chain

st.set_page_config(layout="wide")

def save_new_mp(df_mp):
    df_mp = pd.concat([df_mp, st.session_state.filtered_df_mp])
    df_mp = df_mp.drop_duplicates(subset=['campaign_id_uniq', 'start_date', 'end_date'], keep='last')
    df_mp.to_pickle(path + 'data/df_mp.pkl', index=False, encoding='utf-8-sig')

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
if st.session_state['authentication_status'] == None or st.session_state['authentication_status'] == False:
    authenticator.login(location='sidebar')
    

if 'authentication_status' not in st.session_state:
    st.stop()
if st.session_state['authentication_status'] == None:
    st.stop()

df_mp = mp_df_read(path)
df_mp = clean_column_values(df_mp, 'campaign_id_uniq')

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_mp['manager'].unique(),
                              placeholder='специалист',
                            #   default=st.session_state.managers
                              )
    if st.button('Сохранить медиаплан'):
        save_new_mp(df_mp)

    authenticator.logout(location='sidebar')
    
filtered_df_mp = df_mp[df_mp['manager'].isin(managers)]

st.session_state.filtered_df_mp = st.data_editor(filtered_df_mp)
