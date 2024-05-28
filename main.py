import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader


st.set_page_config(
        page_title="Главная",
        page_icon="📈", layout='wide'
        )

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized'])

if st.session_state["authentication_status"]:
    authenticator.logout(location='sidebar')
    st.title(f'Welcome, {st.session_state["name"]}')
elif st.session_state["authentication_status"] is False:
    authenticator.login(location='sidebar')
    st.error('Username/password is incorrect')
elif st.session_state["authentication_status"] is None:
    authenticator.login(location='sidebar')
    
    