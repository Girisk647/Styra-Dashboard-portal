import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os
import csv
import requests
import base64

# --- 1. SECURITY & CONFIGURATION ---
# I have updated the password to match what you tried in your screenshot.
ADMIN_PASSWORD = "styra123" 
GITHUB_TOKEN = "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" # Put your GitHub Token here
REPO = "Girisk647/Styra-Dashboard-portal"
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

st.set_page_config(layout="wide", page_title="Styra Operations Portal")

# --- 2. LOGIN SYSTEM ---
if 'auth' not in st.session_state:
    st.session_state['auth'] = False

def check_login():
    if not st.session_state['auth']:
        st.markdown("## 🔒 Styra Secure Access")
        pwd = st.text_input("Enter Password", type="password")
        if st.button("Login"):
            if pwd == ADMIN_PASSWORD:
                st.session_state['auth'] = True
                st.rerun()
            else:
                st.error("Invalid Password. Please try again.")
        st.stop()

check_login()

# --- 3. GITHUB SYNC UTILITY ---
def push_to_github():
    try:
        url = f"https://api.github.com/repos/{REPO}/contents/{CSV_FILE}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        
        # Check if file exists to get SHA
        get_res = requests.get(url, headers=headers)
        sha = get_res.json()['sha'] if get_res.status_code == 200 else None

        with open(CSV_FILE, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        payload = {
            "message": f"Auto-Sync: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": content,
            "branch": "main"
        }
        if sha:
            payload["sha"] = sha
        
        put_res = requests.put(url, json=payload, headers=headers)
        return put_res.status_code in [200, 201]
    except Exception as e:
        st.sidebar.error(f"Sync failed: {e}")
        return False

# --- 4. CORE DATABASE ENGINE ---
def fetch_data(project):
    try:
        engine = create_engine(f"postgresql+psycopg2://{project['user']}:{project['pass']}@{project['host']}:5432/{project['db']}")
        today_str = datetime.date.today().strftime('%d/%m/%Y')
        
        # 1. Survey Data
        if project['name'] == "PVVNL":
            q_survey = f"SELECT 'CMI through MI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=72 AND surveydate='{today_str}' AND uniqueid ILIKE 'misurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CMI through CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid NOT ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL AND b.description NOT ILIKE '%%CMI%%' AND b.description NOT ILIKE '%%Consumer Indexing%%' AND b.description NOT ILIKE '%%Meter Installation%%' GROUP BY b.description"
            
            # 2. Your specific CTE Download Query for PVVNL
            q_download = f"""
                WITH cte AS (
                    SELECT MAX(sqllitefileid), REPLACE(userid::text,'-',''), downloadedtimestamp FROM tblpreparedsqllitefiles a
                    WHERE downloadedtimestamp::DATE=CURRENT_DATE AND downloaded=1 AND unique_id NOT ILIKE '%%merge%%'
                    GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
                    UNION
                    SELECT MAX(sqllitefileid), REPLACE(userid::text,'-',''), downloadedtimestamp FROM tblpreparedsqllitefiles 
                    WHERE unique_id ILIKE '%%merge%%' AND preparedtimestamp::DATE=CURRENT_DATE AND userid<>0 
                    AND preparedtimestamp::TIMESTAMP > (CURRENT_DATE::text || ' 07:59:43')::TIMESTAMP
                    GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
                )
                SELECT 'download' as status, COUNT(DISTINCT userid) as total FROM cte a
                INNER JOIN tblusers b ON b.userid=a.replace::int
            """
        else:
            q_survey = f"SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL GROUP BY b.description"
            q_download = "SELECT COUNT(DISTINCT userid) AS total FROM tblpreparedsqllitefiles WHERE downloadedtimestamp::DATE = CURRENT_DATE AND downloaded = 1"

        # 3. Manpower Query
        q_manpower = f"SELECT COUNT(DISTINCT serveyorid) AS manpower FROM tblresponselogs WHERE surveydate='{today_str}' AND responsestatusid>=0"

        with engine.connect() as conn:
            df_s = pd.read_sql(text(q_survey), conn)
            df_m = pd.read_sql(text(q_manpower), conn)
            df_d = pd.read_sql(text(q_download), conn)

        # Process results
        run_time = datetime.datetime.now().strftime('%I:%M %p')
        m_count = int(df_m['manpower'][0]) if not df_m.empty else 0
        d_count = int(df_d['total'][0]) if not df_d.empty and df_d['total'][0] else 0

        # Save to local CSV
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Date', 'Time', 'Project', 'Activity_Description', 'Count', 'Manpower', 'Total_Downloads'])
            for _, row in df_s.iterrows():
                writer.writerow([datetime.date.today().strftime('%d/%m/%Y'), run_time, project['name'], row['description'], row['total'], m_count, d_count])
        
        # Trigger GitHub Sync
        push_to_github()
        return True
    except Exception as e:
        st.error(f"Failed to fetch {project['name']}: {e}")
        return False

# --- 5. USER INTERFACE ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select View", ["📊 Dashboard", "⚙️ Local Collector"])

if st.sidebar.button("Logout"):
    st.session_state['auth'] = False
    st.rerun()

if page == "📊 Dashboard":
    st.title("Styra Live Dashboard")
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        if not df.empty:
            dates = df['Date'].unique()[::-1]
            sel_date = st.sidebar.selectbox("Filter Date", dates)
            filtered_df = df[df['Date'] == sel_date]
            
            tabs = st.tabs(sorted(filtered_df['Project'].unique()))
            for i, p_name in enumerate(sorted(filtered_df['Project'].unique())):
                with tabs[i]:
                    p_rows = filtered_df[filtered_df['Project'] == p_name]
                    for t in p_rows['Time'].unique()[::-1]:
                        snap = p_rows[p_rows['Time'] == t].copy()
                        with st.expander(f"🕒 Snapshot: {t}", expanded=True):
                            c1, c2 = st.columns(2)
                            c1.metric("Manpower", int(snap['Manpower'].iloc[0]))
                            c2.metric("Downloads", int(snap['Total_Downloads'].iloc[0]))
                            snap.insert(0, 'S.No', range(1, len(snap) + 1))
                            st.dataframe(snap[['S.No', 'Activity_Description', 'Count']], hide_index=True, use_container_width=True)
    else:
        st.info("No data available yet. Use the Collector on your local PC.")

elif page == "⚙️ Local Collector":
    st.title("Data Management")
    st.warning("⚠️ Only use this on your computer with VPN active.")
    for p in PROJECTS:
        col1, col2 = st.columns([3, 1])
        col1.write(f"📡 **Project:** {p['name']}")
        if col2.button(f"Update {p['name']}", key=p['name']):
            with st.spinner(f"Connecting to {p['name']}..."):
                if fetch_data(p):
                    st.success(f"{p['name']} Updated & Synced!")
                    st.toast("Sync to Cloud Complete ✅")
