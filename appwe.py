import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os
import csv
import requests
import base64

# --- CONFIGURATION (UPDATE THESE) ---
GITHUB_TOKEN = "your_github_personal_access_token"
REPO = "girisk647/styra-dashboard-portal"
CSV_FILE = "daily_master_report.csv"

PROJECTS = [
    {"name": "PVVNL", "host": "172.21.33.3", "db": "styraiipl_pvvnl_prod", "user": "styra_pvvnl3", "pass": "Vyw2a5M99Gaq", "project_id": 8},
    {"name": "MVVVNL", "host": "172.31.1.119", "db": "styraiipl_mvvnl_prod", "user": "styra_mvvnl3", "pass": "nirKWXnznNg8", "project_id": 19},
    {"name": "MGVCL", "host": "172.31.32.2", "db": "styraiipl_mgvcl_prod", "user": "styra_mgvcl3", "pass": "CV1qmZNjrAor", "project_id": 9},
    {"name": "DGVCL", "host": "172.28.66.15", "db": "styraiipl_dgvcl_prod", "user": "styra_dgvcl3", "pass": "Yb4RqFR5BG5x", "project_id": 13},
    {"name": "APDCL7", "host": "172.29.18.46", "db": "styraiipl_apdcl_pack7", "user": "styra_package7_3", "pass": "Nx6W9cJZrzAd", "project_id": 15},
    {"name": "Polaris", "host": "10.26.10.34", "db": "styra_polaris_saryuprod", "user": "styra_polaris3", "pass": "d1MkW9zTppFV", "project_id": 17},
    {"name": "GVPR", "host": "192.168.37.29", "db": "styra_gvpr_prod", "user": "read_only", "pass": "g2Oo/JJiZhG<35>", "project_id": 21}
]

st.set_page_config(layout="wide", page_title="Styra Operations Hub")

# --- GITHUB SYNC LOGIC ---
def sync_to_github():
    try:
        url = f"https://api.github.com/repos/{REPO}/contents/{CSV_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        
        # Get SHA to allow overwriting
        resp = requests.get(url, headers=headers)
        sha = resp.json()['sha'] if resp.status_code == 200 else None

        with open(CSV_FILE, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        data = {
            "message": f"Data Update: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content
        }
        if sha:
            data["sha"] = sha
        
        requests.put(url, json=data, headers=headers)
        return True
    except Exception as e:
        st.sidebar.error(f"GitHub Sync Error: {e}")
        return False

# --- DATA FETCHING LOGIC ---
def run_collection(project):
    try:
        engine = create_engine(f"postgresql+psycopg2://{project['user']}:{project['pass']}@{project['host']}:5432/{project['db']}")
        today_str = datetime.date.today().strftime('%d/%m/%Y')
        
        # PVVNL Special Queries
        if project['name'] == "PVVNL":
            q_survey = f"SELECT 'CMI through MI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=72 AND surveydate='{today_str}' AND uniqueid ILIKE 'misurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CMI through CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid NOT ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL AND b.description NOT ILIKE '%%CMI%%' AND b.description NOT ILIKE '%%Consumer Indexing%%' AND b.description NOT ILIKE '%%Meter Installation%%' GROUP BY b.description"
            
            # Using your provided CTE query for downloads
            q_download = f"""
                WITH cte AS (
                    SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','' ) as user_clean, downloadedtimestamp FROM tblpreparedsqllitefiles a
                    WHERE downloadedtimestamp::DATE=CURRENT_DATE AND downloaded=1 AND unique_id NOT ILIKE '%%merge%%'
                    GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
                    UNION
                    SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','') as user_clean, downloadedtimestamp FROM tblpreparedsqllitefiles 
                    WHERE unique_id ILIKE '%%merge%%' AND preparedtimestamp::DATE=CURRENT_DATE AND userid<>0 
                    AND preparedtimestamp::TIMESTAMP > (CURRENT_DATE::text || ' 07:59:43')::TIMESTAMP
                    GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
                )
                SELECT count(distinct user_clean) as total FROM cte a
                INNER JOIN tblusers b ON b.userid=a.user_clean::int
            """
        else:
            q_survey = f"SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL GROUP BY b.description"
            q_download = "SELECT COUNT(DISTINCT userid) AS total FROM tblpreparedsqllitefiles WHERE downloadedtimestamp::DATE = CURRENT_DATE AND downloaded = 1"

        q_manpower = f"SELECT COUNT(DISTINCT serveyorid) AS manpower FROM tblresponselogs WHERE surveydate='{today_str}' AND responsestatusid>=0"

        with engine.connect() as conn:
            df_s = pd.read_sql(text(q_survey), conn)
            df_m = pd.read_sql(text(q_manpower), conn)
            df_d = pd.read_sql(text(q_download), conn)

        run_time = datetime.datetime.now().strftime('%I:%M %p')
        m_count = int(df_m['manpower'][0]) if not df_m.empty else 0
        d_count = int(df_d['total'][0]) if not df_d.empty and df_d['total'][0] else 0

        # Save locally
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Date', 'Time', 'Project', 'Activity_Description', 'Count', 'Manpower', 'Total_Downloads'])
            for _, row in df_s.iterrows():
                writer.writerow([datetime.date.today().strftime('%d/%m/%Y'), run_time, project['name'], row['description'], row['total'], m_count, d_count])
        
        # Automatic Sync
        sync_to_github()
        return True
    except Exception as e:
        st.error(f"Fetch Error: {e}")
        return False

# --- NAVIGATION ---
choice = st.sidebar.selectbox("Navigation", ["📊 View Dashboard", "⚙️ Data Collector"])

# --- TAB 1: VIEW DASHBOARD ---
if choice == "📊 View Dashboard":
    st.title("Styra Operational Dashboard")
    if os.path.exists(CSV_FILE):
        try:
            df = pd.read_csv(CSV_FILE)
            if not df.empty:
                selected_date = st.sidebar.selectbox("Date", df['Date'].unique()[::-1])
                date_df = df[df['Date'] == selected_date]
                
                tabs = st.tabs(sorted(date_df['Project'].unique()))
                for i, proj in enumerate(sorted(date_df['Project'].unique())):
                    with tabs[i]:
                        p_data = date_df[date_df['Project'] == proj]
                        st.markdown(f"### 🚀 {proj}")
                        for rt in p_data['Time'].unique()[::-1]:
                            run_df = p_data[p_data['Time'] == rt].copy()
                            with st.expander(f"🕒 Snapshot at {rt}", expanded=True):
                                c1, c2 = st.columns(2)
                                c1.metric("Manpower", int(run_df['Manpower'].iloc[0]))
                                c2.metric("Downloads", int(run_df['Total_Downloads'].iloc[0]))
                                run_df.insert(0, 'S.No', range(1, len(run_df) + 1))
                                st.dataframe(run_df[['S.No', 'Activity_Description', 'Count']], hide_index=True, use_container_width=True)
            else:
                st.info("The report file is currently empty.")
        except Exception as e:
            st.error(f"Error reading dashboard: {e}")
    else:
        st.warning("⚠️ No data file found. Please use the 'Data Collector' on your local PC with VPN to fetch initial data.")

# --- TAB 2: DATA COLLECTOR ---
elif choice == "⚙️ Data Collector":
    st.title("System Control Panel")
    st.info("💡 Reminder: Use your Local Ngrok link for fetching. The GitHub link is for viewing only.")
    for p in PROJECTS:
        col1, col2 = st.columns([3, 1])
        col1.write(f"**Project:** {p['name']}")
        if col2.button(f"Fetch {p['name']}", key=f"fetch_{p['name']}"):
            with st.spinner("Connecting and Syncing..."):
                if run_collection(p):
                    st.success(f"{p['name']} Updated!")
