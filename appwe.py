import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text  # <--- This line fixes the 'name text is not defined' error
import datetime
import os
import csv

# --- CONFIGURATION (Project List) ---
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

# --- AUTHENTICATION ---
if 'user_role' not in st.session_state:
    st.session_state['user_role'] = None

if st.session_state['user_role'] is None:
    st.title("🔐 Styra Portal Login")
    pwd = st.text_input("Enter Password", type="password")
    
    if st.button("Login"):
        if pwd == "styra123":
            st.session_state['user_role'] = "manager"
            st.rerun()
        elif pwd == "admin789":
            st.session_state['user_role'] = "operator"
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

# --- FETCHING LOGIC ---
def run_collection(project):
    try:
        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql+psycopg2://{project['user']}:{project['pass']}@{project['host']}:5432/{project['db']}")
        
        today_str = datetime.date.today().strftime('%d/%m/%Y')
        
        # Queries wrapped in text() below
        if project['name'] == "PVVNL":
            q_survey = f"SELECT 'CMI through MI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=72 AND surveydate='{today_str}' AND uniqueid ILIKE 'misurvey%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CMI through CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid ILIKE 'cisurvey%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT 'CI' AS description, COUNT(*) AS total FROM tblresponselogs WHERE activityid=71 AND surveydate='{today_str}' AND uniqueid NOT ILIKE 'cisurvey%' AND responsestatusid>=0 AND projectid<>999 UNION ALL SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL AND b.description NOT ILIKE '%CMI%' AND b.description NOT ILIKE '%Consumer Indexing%' AND b.description NOT ILIKE '%Meter Installation%' GROUP BY b.description"
        else:
            q_survey = f"SELECT b.description, COUNT(responselogid) AS total FROM tblresponselogs a INNER JOIN tblactivitynew b ON a.activityid=b.activityid WHERE surveydate = '{today_str}' AND a.projectid = {project['project_id']} AND responsestatusid >= 0 AND a.serveyorid IS NOT NULL GROUP BY b.description"
        
        q_download = "SELECT SUM(daily_count) AS total_downloads FROM (SELECT COUNT(DISTINCT a.userid) AS daily_count FROM (SELECT MAX(sqllitefileid), REPLACE(userid::text,'-','')::int AS userid FROM tblpreparedsqllitefiles WHERE downloadedtimestamp::DATE = CURRENT_DATE AND downloaded = 1 GROUP BY userid) a INNER JOIN tblusers b ON b.userid = a.userid UNION ALL SELECT COUNT(DISTINCT userid) AS daily_count FROM tblpreparedsqllitefiles WHERE unique_id LIKE '%merged%' AND userid <> 0 AND downloadedtimestamp::DATE = CURRENT_DATE) AS combined_data"
        q_manpower = f"SELECT COUNT(DISTINCT serveyorid) AS manpower FROM tblresponselogs WHERE surveydate='{today_str}' AND responsestatusid>=0"

        with engine.connect() as conn:
            # Using text() here ensures compatibility with SQLAlchemy 2.0
            df_s = pd.read_sql(text(q_survey), conn)
            df_m = pd.read_sql(text(q_manpower), conn)
            df_d = pd.read_sql(text(q_download), conn)

        file_path = 'daily_master_report.csv'
        run_time = datetime.datetime.now().strftime('%I:%M %p')
        m_count = int(df_m['manpower'][0]) if not df_m.empty else 0
        d_count = int(df_d['total_downloads'][0]) if not df_d.empty and df_d['total_downloads'][0] else 0

        file_exists = os.path.isfile(file_path)
        with open(file_path, mode='a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Date', 'Time', 'Project', 'Activity_Description', 'Count', 'Manpower', 'Total_Downloads'])
            for _, row in df_s.iterrows():
                writer.writerow([datetime.date.today().strftime('%d/%m/%Y'), run_time, project['name'], row['description'], row['total'], m_count, d_count])
        return True
    except Exception as e:
        st.error(f"Error: {e}")
        return False

# --- NAVIGATION ---
menu = ["📊 View Dashboard"]
if st.session_state['user_role'] == "operator":
    menu.append("⚙️ Data Collector")
choice = st.sidebar.selectbox("Navigation", menu)

# --- VIEW DASHBOARD ---
if choice == "📊 View Dashboard":
    st.title("Operational Reports")
    try:
        df = pd.read_csv('daily_master_report.csv')
        selected_date = st.sidebar.selectbox("Date", df['Date'].unique()[::-1])
        date_df = df[df['Date'] == selected_date]
        
        tabs = st.tabs(sorted(date_df['Project'].unique()))
        for i, proj in enumerate(sorted(date_df['Project'].unique())):
            with tabs[i]:
                proj_data = date_df[date_df['Project'] == proj]
                st.markdown(f"<h2 style='color: #FF4B4B;'>🚀 {proj}</h2>", unsafe_allow_html=True)
                
                # Decreasing order for Time
                all_times = proj_data['Time'].unique()[::-1]
                for run_time in all_times:
                    run_df = proj_data[proj_data['Time'] == run_time].copy()
                    with st.expander(f"🕒 Snapshot at {run_time}", expanded=True):
                        c1, c2 = st.columns(2)
                        c1.metric("Manpower", int(run_df['Manpower'].iloc[0]))
                        c2.metric("Downloads", int(run_df['Total_Downloads'].iloc[0]))
                        
                        run_df.insert(0, 'S.No', range(1, len(run_df) + 1))
                        st.dataframe(
                            run_df[['S.No', 'Activity_Description', 'Count']].rename(columns={'Activity_Description': 'Description'}), 
                            width='stretch', 
                            hide_index=True
                        )
    except:
        st.info("No data yet. Operators need to fetch data first.")

# --- DATA COLLECTOR ---
elif choice == "⚙️ Data Collector":
    st.title("Data Collection Control")
    st.warning("Ensure VPN is active before fetching.")
    for p in PROJECTS:
        col1, col2 = st.columns([3, 1])
        col1.write(f"**Project:** {p['name']}")
        if col2.button(f"Fetch {p['name']}", key=p['name']):
            with st.spinner("Fetching..."):
                if run_collection(p):
                    st.success(f"Success! {p['name']} data updated.")