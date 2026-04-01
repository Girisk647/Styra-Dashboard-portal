import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os
import csv
import requests
import base64

# --- 1. SECURITY & CONFIG ---
PASSWORD = "Styra@2026"  # Change this to your preferred password
GITHUB_TOKEN = "YOUR_GITHUB_TOKEN"
REPO = "girisk647/styra-dashboard-portal"
CSV_FILE = "daily_master_report.csv"

PROJECTS = [
    {"name": "PVVNL", "host": "172.21.33.3", "db": "styraiipl_pvvnl_prod", "user": "styra_pvvnl3", "pass": "Vyw2a5M99Gaq", "project_id": 8},
    {"name": "MVVVNL", "host": "172.31.1.119", "db": "styraiipl_mvvnl_prod", "user": "styra_mvvnl3", "pass": "nirKWXnznNg8", "project_id": 19},
    # ... add other projects here
]

st.set_page_config(layout="wide", page_title="Styra Portal")

# --- 2. LOGIN LOGIC ---
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

def login_page():
    st.title("🔒 Styra Secure Access")
    pwd = st.text_input("Enter Password", type="password")
    if st.button("Login"):
        if pwd == PASSWORD:
            st.session_state['authenticated'] = True
            st.rerun()
        else:
            st.error("Incorrect Password")

if not st.session_state['authenticated']:
    login_page()
    st.stop() # Stop the app here if not logged in

# --- 3. GITHUB SYNC LOGIC ---
def sync_to_github():
    try:
        url = f"https://api.github.com/repos/{REPO}/contents/{CSV_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r = requests.get(url, headers=headers)
        sha = r.json()['sha'] if r.status_code == 200 else None
        with open(CSV_FILE, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        data = {"message": f"Update {datetime.datetime.now()}", "content": content}
        if sha: data["sha"] = sha
        requests.put(url, json=data, headers=headers)
    except Exception as e:
        st.error(f"Sync failed: {e}")

# --- 4. FETCHING LOGIC ---
def run_collection(project):
    try:
        # Use the complex CTE query for PVVNL provided earlier
        engine = create_engine(f"postgresql+psycopg2://{project['user']}:{project['pass']}@{project['host']}:5432/{project['db']}")
        today_str = datetime.date.today().strftime('%d/%m/%Y')
        
        # [Insert the SQL Queries from previous turn here]
        
        # Trigger Sync after saving
        sync_to_github()
        return True
    except Exception as e:
        st.error(f"Network Error: Cloud cannot reach {project['host']}. Use Local/Ngrok link.")
        return False

# --- 5. MAIN UI ---
st.sidebar.title("Navigation")
choice = st.sidebar.radio("Go to:", ["📊 Dashboard", "⚙️ Data Collector"])
if st.sidebar.button("Logout"):
    st.session_state['authenticated'] = False
    st.rerun()

if choice == "📊 Dashboard":
    st.title("Operational Reports")
    # [Insert Dashboard UI logic here]

elif choice == "⚙️ Data Collector":
    st.title("System Control Panel")
    st.warning("⚠️ Fetching only works on your Local PC with VPN active.")
    for p in PROJECTS:
        if st.button(f"Fetch {p['name']}"):
            with st.spinner(f"Connecting to {p['name']}..."):
                run_collection(p)
