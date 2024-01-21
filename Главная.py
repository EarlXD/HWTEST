import streamlit as st
import numpy as np
import pandas as pd
from func_data import *
import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots


st.set_page_config(
    page_title="Главная",
    page_icon="📈", layout='wide'
    )