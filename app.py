import streamlit as st
import pandas as pd
import json
import re
import sqlite3
import datetime
from io import BytesIO
import os

st.set_page_config(page_title="Amazon JP 광고 최적화", layout="wide", page_icon="🎯")
st.title("🎯 Amazon JP 광고 최적화 — 제외 키워드")
st.write("앱 로딩 중...")
