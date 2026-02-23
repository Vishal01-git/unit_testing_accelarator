import streamlit as st
import os
import json
import glob
import uuid
import pandas as pd
from datetime import datetime
import streamlit.components.v1 as components

# Import your existing validation modules
from unit_test_validator import Validator
from metadata_fetcher import MetadataFetcher

# Setup directories
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(PROJECT_DIR, 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)

st.set_page_config(page_title="Unit Test Validator", layout="wide")

# ==========================================
# --- URL ROUTING FOR REPORT VIEWER ---
# ==========================================
if hasattr(st, "query_params"):
    view_report = st.query_params.get("report")
else:
    params = st.experimental_get_query_params()
    view_report = params.get("report", [None])[0]

if view_report:
    file_path = os.path.join(REPORTS_DIR, view_report)
    if os.path.exists(file_path):
        st.markdown(f"### 📄 Viewing Report: `{view_report}`")
        st.markdown("---")
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        # Render the HTML natively
        components.html(html_content, height=1200, scrolling=True)
    else:
        st.error(f"Report file not found: {view_report}")
    
    # STOP execution here so the main app UI doesn't load in this tab!
    st.stop()
# ==========================================


# --- STATE MANAGEMENT ---
if 'mappings_df' not in st.session_state:
    st.session_state.mappings_df = pd.DataFrame(columns=["Athena Table", "SQL Table", "Primary Keys"])
if 'athena_tables' not in st.session_state:
    st.session_state.athena_tables = []
if 'sql_tables' not in st.session_state:
    st.session_state.sql_tables = []

# --- SIDEBAR: RUN HISTORY & CONFIG LOAD/SAVE ---
with st.sidebar:
    st.header("💾 Configuration")
    uploaded_file = st.file_uploader("⬆️ Load Config (.json)", type=['json'])
    loaded_config = {}
    if uploaded_file is not None:
        try:
            loaded_config = json.load(uploaded_file)
            st.success("Config loaded! Settings applied.")
        except Exception:
            st.error("Invalid JSON file")

    st.markdown("---")
    st.header("🕒 Run History")
    report_files = glob.glob(os.path.join(REPORTS_DIR, "*.html"))
    history = []
    for f in report_files:
        stats = os.stat(f)
        history.append({
            'filename': os.path.basename(f), 
            'path': f,
            'created_at': datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
            'timestamp': stats.st_mtime
        })
    history.sort(key=lambda x: x['timestamp'], reverse=True)
    
    if not history:
        st.info("No runs found.")
    else:
        for item in history:
            # Generate a link pointing to the same app but with the query parameter appended
            st.markdown(
                f"""
                <a href="?report={item['filename']}" target="_blank" style="display: block; padding: 10px; background-color: #ffffff; color: #1E1E1E; border: 1px solid #ccc; border-radius: 5px; text-decoration: none; margin-bottom: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                    📄 <b>{item['filename'][:15]}...</b><br>
                    <span style="font-size: 0.8em; color: #666;">{item['created_at']}</span>
                </a>
                """,
                unsafe_allow_html=True
            )

# --- MAIN UI ---
st.title("Unit Testing Validator")
st.markdown("A unified interface to configure and run your data validation suite.")

# --- 1. CONNECTION DETAILS ---
st.header("1. Connection Details")
col1, col2, col3, col4 = st.columns(4)
with col1: env = st.text_input("Environment", value=loaded_config.get("env", ""), placeholder="dev")
with col2: initiative = st.text_input("Initiative ID", value=loaded_config.get("initiative", ""), placeholder="6051")
with col3: name = st.text_input("Name", value=loaded_config.get("name", ""), placeholder="pgl")
with col4: layer = st.text_input("Layer", value=loaded_config.get("layer", ""), placeholder="int")

col5, col6, col7 = st.columns(3)
with col5:
    auth_idx = 0 if loaded_config.get("authMethod", "mfa") == "mfa" else 1
    auth_method = st.selectbox("Auth Method", options=["mfa", "password"], index=auth_idx, format_func=lambda x: "MFA (Active Directory)" if x == "mfa" else "SQL Login (Password)")
with col6:
    mssql_user = st.text_input("SQL User", value=loaded_config.get("mssqlUser", ""), placeholder="user@domain.com")
with col7:
    mssql_password = st.text_input("SQL Password", type="password", disabled=(auth_method == "mfa"))

# Dynamic Paths
sql_env = "uat" if env.lower() == "pprd" else env.lower()
default_s3 = loaded_config.get("s3") or f"s3://al-gdo-{env.lower()}-ap-dl-{initiative}-formatted-p/"
default_ath_db = loaded_config.get("athDb") or f"{env.lower()}_bi_ap_{initiative}_{layer}_{name}"
default_wg = loaded_config.get("wg") or f"{initiative}-workgroup"
default_sql_srv = loaded_config.get("sqlSrv") or f"al-sas-big-{sql_env}-sql-{name}-01.database.windows.net"
default_sql_db = loaded_config.get("sqlDb") or f"al-sas-big-{sql_env}-sql-{name}-01-db-01"

with st.expander("Advanced: Connection Paths", expanded=False):
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        s3_staging = st.text_input("S3 Staging", value=default_s3)
        athena_db = st.text_input("Athena DB", value=default_ath_db)
        athena_wg = st.text_input("Workgroup", value=default_wg)
    with p_col2:
        sql_srv = st.text_input("SQL Server", value=default_sql_srv)
        sql_db = st.text_input("SQL DB", value=default_sql_db)

# --- 2. TABLE MAPPINGS ---
st.header("2. Table Mappings")
map_mode = st.radio("Mapping Mode", ["Manual Entry", "Metadata Search"], horizontal=True)

mappings = {}

if map_mode == "Manual Entry":
    m_col1, m_col2, m_col3 = st.columns(3)
    manual_defaults = loaded_config.get("manual", {})
    with m_col1: ath_text = st.text_area("Athena Tables (comma separated)", value=manual_defaults.get("ath", ""))
    with m_col2: sql_text = st.text_area("SQL Tables (comma separated)", value=manual_defaults.get("sql", ""))
    with m_col3: pk_text = st.text_area("Primary Keys (semicolon separated per table, comma per key)", value=manual_defaults.get("pk", ""))
    
    if ath_text and sql_text:
        ath_list = [t.strip() for t in ath_text.split(',')]
        sql_list = [t.strip() for t in sql_text.split(',')]
        pk_list = [t.strip() for t in pk_text.split(';')]
        
        if len(ath_list) == len(sql_list):
            for i, ath in enumerate(ath_list):
                if ath and sql_list[i]:
                    pks = [k.strip() for k in pk_list[i].split(',')] if i < len(pk_list) and pk_list[i] else []
                    mappings[ath] = {'sql_table': sql_list[i], 'primary_keys': [k for k in pks if k]}

else:
# Metadata Search
    if st.button("🔄 Fetch Tables", type="secondary"):
        with st.spinner("Connecting and fetching metadata..."):
            args = {
                'aws_region': "ap-southeast-1",
                's3_staging': s3_staging,
                'athena_db': athena_db,
                'athena_workgroup': athena_wg,
                'mssql_server': sql_srv,
                'mssql_db': sql_db,
                'mssql_driver': 'ODBC Driver 17 for SQL Server',
                'auth_method': auth_method,
                'mssql_user': mssql_user,
                'mssql_password': mssql_password
            }
            try:
                fetcher = MetadataFetcher(args)
                st.session_state.athena_tables = fetcher.get_athena_tables()
                st.session_state.sql_tables = fetcher.get_sql_tables()
                
                # Fetch the primary keys dictionary (e.g., {'dbo.GPSales': 'id, date'})
                sql_pks = fetcher.get_sql_primary_keys()
                
                # Normalize SQL tables
                norm_sql = {
                    t.replace(".", "").replace("_", "").lower(): t 
                    for t in st.session_state.sql_tables
                }
                
                auto_mapped_rows = []
                for ath_tbl in st.session_state.athena_tables:
                    # Normalize Athena tables
                    norm_ath = ath_tbl.replace("_", "").replace(".", "").lower()
                    
                    if norm_ath in norm_sql:
                        matched_sql_table = norm_sql[norm_ath]
                        # Look up the primary keys for the matched SQL table
                        matched_pks = sql_pks.get(matched_sql_table, "")
                        
                        auto_mapped_rows.append({
                            "Athena Table": ath_tbl,
                            "SQL Table": matched_sql_table,
                            "Primary Keys": matched_pks
                        })
                
                if auto_mapped_rows:
                    st.session_state.mappings_df = pd.DataFrame(auto_mapped_rows)
                    st.success(f"Fetched {len(st.session_state.athena_tables)} Athena and {len(st.session_state.sql_tables)} SQL tables. Auto-mapped {len(auto_mapped_rows)} tables with Primary Keys!")
                else:
                    st.session_state.mappings_df = pd.DataFrame(columns=["Athena Table", "SQL Table", "Primary Keys"])
                    st.success(f"Fetched tables, but couldn't find any automatic matches.")
                    
            except Exception as e:
                st.error(f"Error fetching metadata: {e}")

    # Compact, Clean Grid
    column_config = {
        "Athena Table": st.column_config.SelectboxColumn("Athena Table", options=st.session_state.athena_tables, required=True),
        "SQL Table": st.column_config.SelectboxColumn("SQL Table", options=st.session_state.sql_tables, required=True),
        "Primary Keys": st.column_config.TextColumn("Primary Keys (comma separated)")
    }
    
    st.caption("Use the grid below to seamlessly map tables. It automatically adds rows as you type.")
    
    edited_df = st.data_editor(
        st.session_state.mappings_df, 
        num_rows="dynamic", 
        column_config=column_config, 
        use_container_width=True,
        hide_index=True 
    )
    st.session_state.mappings_df = edited_df
    
    for _, row in edited_df.iterrows():
        if pd.notna(row["Athena Table"]) and pd.notna(row["SQL Table"]):
            pks = [k.strip() for k in str(row.get("Primary Keys", "")).split(',')] if pd.notna(row.get("Primary Keys")) else []
            mappings[row["Athena Table"]] = {'sql_table': row["SQL Table"], 'primary_keys': [k for k in pks if k]}

# --- 3. EXECUTION ---
st.header("3. Execution")
e_col1, e_col2, e_col3 = st.columns([1,1,2])
with e_col1: aws_region = st.text_input("AWS Region", value="ap-southeast-1")
with e_col2: sample_size = st.number_input("Sample Size", value=100, min_value=1)
with e_col3: tests = st.text_input("Tests", value="schema,count,duplicates,nulls,data")

current_config = {
    "env": env, "initiative": initiative, "name": name, "layer": layer,
    "authMethod": auth_method, "mssqlUser": mssql_user,
    "s3": s3_staging, "athDb": athena_db, "wg": athena_wg, "sqlSrv": sql_srv, "sqlDb": sql_db,
    "manual": {"ath": ath_text if map_mode == "Manual Entry" else "", "sql": sql_text if map_mode == "Manual Entry" else "", "pk": pk_text if map_mode == "Manual Entry" else ""}
}
st.download_button("⬇️ Export Current Configuration", data=json.dumps(current_config, indent=2), file_name=f"config_{name}.json", mime="application/json")

st.markdown("---")

if st.button("🚀 Run Validation", use_container_width=True, type="primary"):
    if not mappings:
        st.error("Please configure at least one table mapping before running.")
    else:
        log_container = st.empty()
        log_text = ["Validation Started..."]
        log_container.code("\n".join(log_text), language="bash")

        def progress_callback(msg):
            log_text.append(f"> {msg}")
            log_container.code("\n".join(log_text[-20:]), language="bash") 

        run_id = str(uuid.uuid4())
        report_filename = f"report_{run_id}.html"
        report_file_path = os.path.join(REPORTS_DIR, report_filename)

        val_config = {
            "aws-region": aws_region, "s3-staging": s3_staging, "athena-db": athena_db,
            "athena-workgroup": athena_wg, "mssql-server": sql_srv, "mssql-db": sql_db,
            "mssql-driver": "ODBC Driver 17 for SQL Server", "auth-method": auth_method,
            "mssql-user": mssql_user, "mssql-password": mssql_password, "mappings": mappings
        }

        validator = Validator(
            config=val_config, output_path=report_file_path,
            tests=tests, sample_size=sample_size, verbose=True
        )

        with st.spinner("Validation running..."):
            success, message = validator.run(progress_callback=progress_callback)

        if success:
            st.success(f"SUCCESS: {message}")
            
            # Button that opens the same app but appends ?report=filename
            st.markdown(
                f"""
                <br>
                <a href="?report={report_filename}" target="_blank" style="display: inline-block; padding: 12px 24px; background-color: #367BF5; color: white; text-align: center; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px;">
                    👁️ View Report
                </a>
                """, 
                unsafe_allow_html=True
            )
        else:
            st.error(f"FAILURE: {message}")