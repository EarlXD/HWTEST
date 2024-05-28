import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
from path_lib import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from itertools import chain

st.set_page_config(layout="wide")

def save_new_mp(df_mp):
    df_mp = pd.concat([df_mp, st.session_state.filtered_df_mp])
    df_mp = df_mp.drop_duplicates(subset=['campaign_id_uniq', 'start_date', 'end_date'], keep='last')
    df_mp.to_pickle(path + 'data/df_mp.pkl', index=False, encoding='utf-8-sig')

df_mp = mp_df_read('')
df_mp = clean_column_values(df_mp, 'campaign_id_uniq')

with st.sidebar:
    managers = st.multiselect('Выбор специалиста',
                              df_mp['manager'].unique(),
                              placeholder='специалист',
                            #   default=st.session_state.managers
                              )
    if st.button('Сохранить медиаплан'):
        save_new_mp(df_mp)
    
filtered_df_mp = df_mp[df_mp['manager'].isin(managers)]

st.session_state.filtered_df_mp = st.data_editor(filtered_df_mp)
