import streamlit as st
import pandas as pd
import psycopg2  # FIXED: Added missing import
import datetime
import os
import csv
import requests
import base64

# --- 1. SECURITY & CONFIGURATION ---
ADMIN_PASSWORD = "styra123" 
# Use Streamlit Secrets for the token to prevent GitHub from auto-deleting it
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"] 
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

# --- 3. CSV SAVING UTILITY ---
# FIXED: Added this function which was missing from your appwe.py
def save_to_history(project_name, survey_df, manpower, downloads):
    file_exists = os.path.isfile(CSV_FILE)
    today_date = datetime.date.today().strftime('%d/%m/%Y')
    run_time = datetime.datetime.now().strftime('%I:%M %p')
    
    with open(CSV_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Date', 'Time', 'Project', 'Activity_Description', 'Count', 'Manpower', 'Total_Downloads'])
        
        for _, row in survey_df.iterrows():
            writer.writerow([
                today_date, 
                run_time, 
                project_name, 
                row['description'], 
                row['total'], 
                manpower, 
                downloads
            ])

# --- 4. GITHUB SYNC UTILITY ---
def push_to_github(filename): # FIXED: Function now accepts the filename
    try:
        url = f"https://api.github.com/repos/{REPO}/contents/{filename}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        get_res = requests.get(url, headers=headers)
        sha = None
        if get_res.status_code == 200:
            sha = get_res.json().get('sha')

        with open(filename, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        payload = {
            "message": f"Manual Fetch: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}",
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

# --- 5. CORE DATABASE ENGINE ---
def fetch_data(project):
    conn = None
    try:
        conn = psycopg2.connect(
            host=project['host'],
            database=project['db'],
            user=project['user'],
            password=project['pass'],
            port=5432,
            connect_timeout=15
        )
        
        today_str = datetime.date.today().strftime('%d/%m/%Y')
        current_date_sql = datetime.date.today().strftime('%Y-%m-%d')
        
        # --- SURVEY QUERY LOGIC ---
        if project['name'] in ["PVVNL", "MVVVNL"]:
            q_survey = f"""
            SELECT 'CMI through MI' AS description, COUNT(*) AS total FROM tblresponselogs
            WHERE activityid=72 AND surveydate='{today_str}' AND uniqueid ILIKE 'misurvey%%' AND responsestatusid>=0 AND projectid<>999
            UNION ALL
            SELECT 'CMI through CI' AS description, COUNT(*) AS total FROM tblresponselogs
            WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999
            UNION ALL
            SELECT 'CI' AS description, COUNT(*) AS total FROM tblresponselogs
            WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid NOT ILIKE 'cisurvey%%' AND responsestatusid>=0 AND projectid<>999
            UNION ALL
            SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a
            INNER JOIN tblactivitynew b ON a.activityid=b.activityid
            WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 
            AND a.serveyorid IS NOT NULL 
            AND b.description NOT ILIKE 'Consumer Indexing' 
            AND b.description NOT ILIKE 'Meter Installation'
            GROUP BY b.description
            """
        else:
            q_survey = f"""
            SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a
            INNER JOIN tblactivitynew b ON a.activityid=b.activityid
            WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL
            GROUP BY b.description
            """

        # --- DOWNLOAD QUERY LOGIC ---
        if project['name'] in ["PVVNL", "MVVVNL"]:
            q_download = f"""
            WITH cte AS (
                SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','')::int AS uid, downloadedtimestamp 
                FROM tblpreparedsqllitefiles
                WHERE downloadedtimestamp::DATE = CURRENT_DATE AND downloaded = 1 AND unique_id NOT ILIKE '%%merge%%'
                GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
                UNION
                SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','')::int AS uid, downloadedtimestamp 
                FROM tblpreparedsqllitefiles 
                WHERE unique_id ILIKE '%%merge%%' AND preparedtimestamp::DATE = CURRENT_DATE AND userid <> 0 
                AND preparedtimestamp::timestamp > '{current_date_sql} 07:59:43'
                GROUP BY REPLACE(userid::text,'-',''), downloadedtimestamp
            )
            SELECT COUNT(DISTINCT uid) AS total_downloads FROM cte a INNER JOIN tblusers b ON b.userid = a.uid;
            """
        else:
            q_download = """
            SELECT SUM(daily_count) AS total_downloads FROM (
                SELECT COUNT(DISTINCT a.userid) AS daily_count FROM (
                    SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','')::int AS userid FROM tblpreparedsqllitefiles
                    WHERE downloadedtimestamp::DATE = CURRENT_DATE AND downloaded = 1 GROUP BY userid
                ) a INNER JOIN tblusers b ON b.userid = a.userid
                UNION ALL
                SELECT COUNT(DISTINCT userid) AS daily_count FROM tblpreparedsqllitefiles 
                WHERE unique_id LIKE '%%merged%%' AND userid <> 0 AND downloadedtimestamp::DATE = CURRENT_DATE
            ) AS combined_data
            """

        q_manpower = f"SELECT COUNT(DISTINCT serveyorid) AS manpower FROM tblresponselogs WHERE surveydate='{today_str}' AND responsestatusid>=0"

        # --- EXECUTION & CLEANING ---
        df_s = pd.read_sql(q_survey, conn)
        df_m = pd.read_sql(q_manpower, conn)
        df_d = pd.read_sql(q_download, conn)

        if not df_s.empty:
            df_s['description'] = df_s['description'].str.strip()
            m_count = int(df_m.iloc[0,0]) if not df_m.empty else 0
            total_val = df_d.iloc[0,0] if not df_d.empty and df_d.iloc[0,0] is not None else 0
            total_d = int(total_val)

            save_to_history(project['name'], df_s, m_count, total_d)
            push_to_github(CSV_FILE)
            return True
        return False

    except Exception as e:
        st.error(f"Error: {e}")
        return False
    finally:
        if conn: conn.close()

# --- 6. USER INTERFACE ---
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
