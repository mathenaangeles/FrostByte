import json
import re
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="❄️ FrostByte", layout="wide")

st.markdown(
    """
<style>
div.block-container {
  padding-top: 1.1rem;
  padding-bottom: 2.2rem;
  max-width: 1600px;
  padding-left: 1.25rem;
  padding-right: 1.25rem;
}

section[data-testid="stSidebar"][aria-expanded="true"] {
  width: 20rem !important;
  min-width: 20rem !important;
  max-width: 24rem !important;
}
section[data-testid="stSidebar"][aria-expanded="false"] {
  width: 0rem !important;
  min-width: 0rem !important;
  max-width: 0rem !important;
  overflow-x: hidden !important;
}
i
section[data-testid="stSidebar"] > div {
  background: rgba(255,255,255,0.02);
  padding: 1.5rem 1rem;
}

section[data-testid="stSidebar"] h2 {
  font-size: 1.1rem;
  font-weight: 700;
  margin-bottom: 1rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid rgba(255,255,255,0.1);
}

section[data-testid="stSidebar"] .stSelectbox label {
  font-weight: 600;
  font-size: 0.9rem;
  opacity: 0.9;
}

section[data-testid="stSidebar"] hr {
  margin: 1.5rem 0;
  opacity: 0.2;
}

div[data-testid="column"] { overflow: visible !important; }
div[data-testid="stHorizontalBlock"] { gap: 0.9rem; }

h1, h2, h3 { letter-spacing: -0.01em; }
hr { opacity: 0.35; }

.fb-kpi { margin-bottom: 14px; }

.fb-kpi {
  box-sizing: border-box;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  padding: 16px 18px;
  border-radius: 16px;
  min-height: 120px;
  height: auto;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}
.fb-kpi-label { 
  opacity: 0.82; 
  font-weight: 650; 
  font-size: 0.88rem; 
  margin-bottom: 10px;
  line-height: 1.2;
}
.fb-kpi-value { 
  font-weight: 850; 
  font-size: 1.75rem; 
  line-height: 1.2; 
  word-wrap: break-word;
  overflow-wrap: break-word;
  margin-bottom: 6px;
  max-width: 100%;
}
.fb-kpi-sub { 
  opacity: 0.70; 
  font-size: 0.80rem; 
  margin-top: 6px;
  line-height: 1.3;
}

.fb-brief-card {
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.08), rgba(139, 92, 246, 0.08));
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: 16px;
  padding: 20px 24px;
  margin-bottom: 1.2rem;
}

.fb-brief-header {
  font-weight: 750;
  font-size: 1.15rem;
  opacity: 0.95;
  margin-bottom: 16px;
  padding-bottom: 12px;
  border-bottom: 1px solid rgba(255,255,255,0.15);
}

.fb-brief-section {
  margin-bottom: 18px;
}

.fb-brief-section-title {
  font-weight: 700;
  font-size: 0.95rem;
  opacity: 0.88;
  margin-bottom: 8px;
  color: rgba(147, 197, 253, 1);
}

.fb-brief-content {
  font-size: 0.92rem;
  line-height: 1.6;
  opacity: 0.85;
  margin-left: 12px;
}

.fb-brief-content ul {
  margin: 6px 0;
  padding-left: 20px;
}

.fb-brief-content li {
  margin: 4px 0;
}

.fb-anomaly-highlight {
  background: rgba(239, 68, 68, 0.12);
  border-left: 3px solid rgba(239, 68, 68, 0.6);
  padding: 10px 14px;
  border-radius: 8px;
  margin: 8px 0;
  font-size: 0.90rem;
  line-height: 1.5;
}

.fb-anomaly-item {
  margin: 6px 0;
  padding: 8px 0;
  border-bottom: 1px solid rgba(255,255,255,0.06);
}

.fb-anomaly-item:last-child {
  border-bottom: none;
}

.fb-anomaly-meta {
  font-size: 0.82rem;
  opacity: 0.7;
  margin-top: 4px;
}

div[data-testid="stVerticalBlockBorderWrapper"] {
  background: rgba(255,255,255,0.03) !important;
  border: 1px solid rgba(255,255,255,0.08) !important;
  border-radius: 16px !important;
  padding: 14px 14px 12px 14px !important;
}

div[data-testid="stDataFrame"] { 
  border-radius: 14px; 
  overflow: hidden; 
}

.fb-chat-title { 
  font-weight: 750; 
  opacity: 0.92; 
  margin-bottom: 0.8rem;
  font-size: 1.05rem;
}

.fb-chat-prompt-btn {
  width: 100%;
  padding: 12px 16px;
  margin: 8px 0;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 12px;
  color: inherit;
  font-size: 0.90rem;
  line-height: 1.4;
  text-align: left;
  cursor: pointer;
  transition: all 0.2s ease;
}

.fb-chat-prompt-btn:hover {
  background: rgba(255,255,255,0.08);
  border-color: rgba(255,255,255,0.20);
}

div.stButton > button {
  width: 100%;
  border-radius: 12px;
  padding: 10px 16px;
  font-weight: 600;
  transition: all 0.2s ease;
}


.stMetric {
  background: rgba(255,255,255,0.03);
  padding: 12px 16px;
  border-radius: 12px;
  border: 1px solid rgba(255,255,255,0.08);
}

.stMetric label {
  font-size: 0.88rem !important;
  font-weight: 600 !important;
  opacity: 0.85;
}

.stMetric [data-testid="stMetricValue"] {
  font-size: 1.6rem !important;
  font-weight: 800 !important;
}

.fb-status-card {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 14px;
  padding: 16px 18px;
  margin: 12px 0;
  line-height: 1.8;
  font-size: 0.90rem;
}

.fb-status-card b {
  opacity: 0.88;
}

footer { visibility: hidden; }

</style>
""",
    unsafe_allow_html=True,
)

session = get_active_session()

@st.cache_resource
def resolve_app_location():
    row = session.sql(
        "select current_database() as DB, current_schema() as SCHEMA"
    ).collect()[0]
    return row["DB"], row["SCHEMA"]

DB, SCHEMA = resolve_app_location()


def sql_escape(s: Any) -> str:
    return ("" if s is None else str(s)).replace("'", "''")


def df_sql(q: str) -> pd.DataFrame:
    return session.sql(q).to_pandas()


def run_sql(q: str):
    return session.sql(q).collect()


def strip_fences(t: str) -> str:
    x = (t or "").strip()
    x = re.sub(r"^```(sql)?", "", x, flags=re.IGNORECASE).strip()
    x = re.sub(r"```$", "", x).strip()
    return x


def is_read_query(sql_text: str) -> bool:
    s = (sql_text or "").strip().lower()
    s = re.sub(r"^\s*--.*$", "", s, flags=re.MULTILINE).strip()
    return s.startswith(("select", "with", "show", "describe", "desc"))


def is_dangerous(sql_text: str) -> bool:
    s = (sql_text or "").lower()
    tokens = set(re.findall(r"[a-z_]+", s))
    dangerous = {"drop", "truncate", "undrop", "alter", "grant", "revoke"}
    return len(tokens.intersection(dangerous)) > 0


def series_id_from_row(org: str, metric: str, category: str, d1: str, d2: str, d3: str) -> str:
    raw = "||".join([org or "", metric or "", category or "", d1 or "", d2 or "", d3 or ""])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def ensure_series_id(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "SERIES_ID" in df.columns:
        return df
    cols = ["ORG_UNIT", "METRIC_NAME", "CATEGORY", "DIM1", "DIM2", "DIM3"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df.copy()
    df["SERIES_ID"] = df.apply(
        lambda r: series_id_from_row(
            str(r.get("ORG_UNIT", "")),
            str(r.get("METRIC_NAME", "")),
            str(r.get("CATEGORY", "")),
            str(r.get("DIM1", "")),
            str(r.get("DIM2", "")),
            str(r.get("DIM3", "")),
        ),
        axis=1,
    )
    return df


VIEWS = {
    "RUN_CONTEXT": f"{DB}.{SCHEMA}.VW_RUN_CONTEXT",
    "DASHBOARD_KPIS": f"{DB}.{SCHEMA}.VW_DASHBOARD_KPIS",
    "TOP_MOVERS": f"{DB}.{SCHEMA}.VW_TOP_MOVERS",
    "TOP_ANOMALIES": f"{DB}.{SCHEMA}.VW_TOP_ANOMALIES",
    "SERIES_TIMELINE": f"{DB}.{SCHEMA}.VW_SERIES_TIMELINE",
    "DQ_RUN": f"{DB}.{SCHEMA}.VW_DQ_RUN",
    "DQ_SERIES": f"{DB}.{SCHEMA}.VW_DQ_SERIES",
    "FORECAST": f"{DB}.{SCHEMA}.VW_FORECAST",
    "FORECAST_JOINED": f"{DB}.{SCHEMA}.VW_FORECAST_JOINED",
    "FEATURES": f"{DB}.{SCHEMA}.VW_FEATURES",
    "CLUSTERS": f"{DB}.{SCHEMA}.VW_CLUSTERS",
    "ACTIONS": f"{DB}.{SCHEMA}.VW_ACTIONS",
    "WEEKLY_BRIEF": f"{DB}.{SCHEMA}.VW_WEEKLY_BRIEF",
    "AI_AUDIT": f"{DB}.{SCHEMA}.VW_AI_AUDIT",
}

BASE = {
    "CONFIG": f"{DB}.{SCHEMA}.FROSTBYTE_CONFIG",
    "RUNS": f"{DB}.{SCHEMA}.FROSTBYTE_RUNS",
    "DQ_RUN": f"{DB}.{SCHEMA}.FROSTBYTE_DQ_RUN",
    "DQ_SERIES": f"{DB}.{SCHEMA}.FROSTBYTE_DQ_SERIES",
    "SERIES_WEEKLY": f"{DB}.{SCHEMA}.FROSTBYTE_SERIES_WEEKLY",
    "ANOMALIES_WEEKLY": f"{DB}.{SCHEMA}.FROSTBYTE_ANOMALIES_WEEKLY",
    "FORECAST_4W": f"{DB}.{SCHEMA}.FROSTBYTE_FORECAST_4W",
    "FEATURES": f"{DB}.{SCHEMA}.FROSTBYTE_SERIES_FEATURES",
    "CLUSTERS": f"{DB}.{SCHEMA}.FROSTBYTE_SERIES_CLUSTERS",
    "ACTIONS": f"{DB}.{SCHEMA}.FROSTBYTE_ACTIONS",
    "WEEKLY_BRIEF": f"{DB}.{SCHEMA}.FROSTBYTE_WEEKLY_BRIEF",
    "AI_AUDIT": f"{DB}.{SCHEMA}.FROSTBYTE_AI_AUDIT",
}


@st.cache_data(ttl=60)
def view_exists(fqn: str) -> bool:
    try:
        parts = fqn.split(".")
        if len(parts) != 3:
            return False
        db, schema, name = parts
        q = f"""
        select count(*) as N
        from {db}.information_schema.views
        where table_schema = '{sql_escape(schema)}'
          and table_name = '{sql_escape(name)}'
        """
        d = df_sql(q)
        return (not d.empty) and int(d.iloc[0]["N"]) > 0
    except Exception:
        return False


def q_frostbyte(preferred_view: str, fallback_sql: str, warn_key: str) -> pd.DataFrame:
    if preferred_view and view_exists(preferred_view):
        return df_sql(f"select * from {preferred_view}")
    st.session_state.setdefault("_fb_warnings", set()).add(warn_key)
    return df_sql(fallback_sql)


def render_fallback_warning():
    warns = st.session_state.get("_fb_warnings", set())
    if warns:
        st.warning(
            "Using base FrostByte tables for some screens because one or more secure views (VW_*) were not found. "
            "For strict Native App governance, create the VW_* views from the setup script."
        )

def cortex_complete(cortex_fn: str, model_name: str, prompt: str) -> str:
    fn = (cortex_fn or "SNOWFLAKE.CORTEX.COMPLETE").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.]+", fn):
        fn = "SNOWFLAKE.CORTEX.COMPLETE"
    model = (model_name or "llama3.1-8b").strip()
    p = (prompt or "").replace("'", "''")
    q = f"select {fn}('{model}', '{p}') as TEXT"
    out = df_sql(q)
    if out.empty:
        return ""
    return str(out.iloc[0]["TEXT"] if "TEXT" in out.columns else out.iloc[0, 0])


def copilot_plan(cortex_fn: str, model_name: str, user_msg: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""
You are FrostByte Copilot inside a Snowflake Streamlit Native App.

Return ONLY valid JSON with this shape:
{{
  "intent": "explain" | "sql" | "summary" | "action",
  "title": "...",
  "answer": "...",
  "sql": "..." ,
  "notes": "..."
}}

Rules:
- Always produce SQL if the user asks for data, verification, drilldowns, or investigation steps.
- SQL may be ANY statement, but execution is governed by the app (safe-by-default).
- Prefer FrostByte secure views (VW_*) and always filter by RUN_ID and WEEK_START when relevant.
- If safe_mode = true, restrict SQL to FrostByte VW_* objects only.
- If safe_mode = false, you may query the input object read-only; still prefer VW_*.

Context (JSON):
{json.dumps(ctx, default=str)}

User message:
{user_msg}
""".strip()

    raw = strip_fences(cortex_complete(cortex_fn, model_name, prompt).strip())
    try:
        return json.loads(raw)
    except Exception:
        return {"intent": "explain", "title": "Response", "answer": raw, "sql": "", "notes": "Model output was not valid JSON."}


def audit_ai(run_id: str, config_name: str, purpose: str, model_name: str, prompt_text: str, response_text: str):
    try:
        rid = sql_escape(run_id)
        cfg = sql_escape(config_name)
        pur = sql_escape(purpose)
        mod = sql_escape(model_name)
        pr = sql_escape(prompt_text)
        resp = sql_escape(response_text)
        q = f"""
        insert into {BASE["AI_AUDIT"]} (RUN_ID, CONFIG_NAME, COMPUTED_AT, PURPOSE, MODEL_NAME, PROMPT_HASH, RESPONSE)
        select
          '{rid}', '{cfg}', current_timestamp(), '{pur}', '{mod}',
          sha2('{pr}', 256),
          '{resp}'
        """
        run_sql(q)
    except Exception:
        pass


st.title("❄️ FrostByte")
st.caption("This is your weekly metrics command center for automated diagnostics, explainable insights, and action-ready outputs across any dataset.")

with st.sidebar:
    st.subheader("Context")

    st.markdown("### Connect Data")
    st.caption("Bind your table/view to the INPUT_DATA reference, then run the app.")
    bind_sql = (
        f"call {DB}.CONFIG.REGISTER_REFERENCE("
        f"'INPUT_DATA','ADD','<db>.<schema>.<table_or_view>');"
    )
    st.code(bind_sql, language="sql")
    if st.button("Check Reference", key="sb_check_ref"):
        try:
            ref_row = df_sql("select system$get_reference('INPUT_DATA') as REF")
            ref_val = ref_row.iloc[0]["REF"] if not ref_row.empty else None
            if ref_val:
                st.success(f"INPUT_DATA bound to: {ref_val}")
            else:
                st.warning("INPUT_DATA is not bound yet.")
        except Exception as e:
            st.error(f"Reference check failed: {e}")
    st.markdown("---")

    if st.button("Clear Cache", key="sb_clear_cache"):
        st.cache_data.clear()
        st.session_state.pop("_fb_warnings", None)
        st.rerun()

    cfg_df = df_sql(f"select * from {BASE['CONFIG']} order by CREATED_AT desc")
    if cfg_df.empty:
        st.error("No configurations found in FROSTBYTE_CONFIG.")
        st.stop()

    cfg_names = cfg_df["CONFIG_NAME"].astype(str).tolist()
    sel_cfg = st.selectbox("Configuration", cfg_names, key="sb_cfg_sel")

    cfg_row = cfg_df[cfg_df["CONFIG_NAME"].astype(str) == str(sel_cfg)].iloc[0].to_dict()

    runs_df = df_sql(
        f"""
        select *
        from {BASE['RUNS']}
        where CONFIG_NAME = '{sql_escape(sel_cfg)}'
        order by STARTED_AT desc
        """
    )
    if runs_df.empty:
        st.warning("No runs found for this configuration.")
        if st.button("Run Now", key="sb_run_now_empty"):
            try:
                run_sql(f"call {DB}.{SCHEMA}.RUN_FROSTBYTE('{sql_escape(sel_cfg)}')")
                st.success("Run triggered successfully.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Run failed: {e}")
        st.stop()

    runs_df = runs_df.copy()
    runs_df["LABEL"] = (
        runs_df["RUN_ID"].astype(str)
        + " • "
        + runs_df["STATUS"].astype(str)
        + " • "
        + runs_df["STARTED_AT"].astype(str)
    )
    sel_run_label = st.selectbox("Run (Latest First)", runs_df["LABEL"].tolist(), index=0, key="sb_run_sel")
    run_id = str(runs_df[runs_df["LABEL"] == sel_run_label].iloc[0]["RUN_ID"])

    weeks_df = df_sql(
        f"""
        select distinct WEEK_START
        from {BASE['SERIES_WEEKLY']}
        where RUN_ID = '{sql_escape(run_id)}'
        order by WEEK_START desc
        """
    )
    week_list = weeks_df["WEEK_START"].astype(str).tolist() if not weeks_df.empty else []
    default_week = (
        str(runs_df[runs_df["LABEL"] == sel_run_label].iloc[0].get("LATEST_WEEK_START"))
        if "LATEST_WEEK_START" in runs_df.columns
        else (week_list[0] if week_list else "")
    )
    if week_list and default_week in week_list:
        week_index = week_list.index(default_week)
    else:
        week_index = 0

    sel_week = st.selectbox("Week (Latest First)", week_list, index=week_index, key="sb_week_sel") if week_list else ""

    st.markdown("---")
    st.subheader("Run Controls")

    c_run1, c_run2 = st.columns([1, 1])
    with c_run1:
        if st.button("Run Now", key="sb_run_now"):
            try:
                run_sql(f"call {DB}.{SCHEMA}.RUN_FROSTBYTE('{sql_escape(sel_cfg)}')")
                st.success("Run triggered successfully.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Run failed: {e}")

    with c_run2:
        st.write("")

    run_row = runs_df[runs_df["RUN_ID"].astype(str) == str(run_id)].iloc[0].to_dict()
    st.markdown(
        f"""
<div class="fb-status-card">
<b>Status:</b> {run_row.get("STATUS","—")}<br/>
<b>Window:</b> {run_row.get("WINDOW_START","—")} → {run_row.get("WINDOW_END","—")}<br/>
<b>Latest Week:</b> {run_row.get("LATEST_WEEK_START","—")}<br/>
<b>Series:</b> {run_row.get("SERIES_COUNT","—")} | <b>Rows Scanned:</b> {run_row.get("ROWS_SCANNED","—")}<br/>
<b>Version:</b> {run_row.get("APP_VERSION","—")}
</div>
""",
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.subheader("Configuration Thresholds")
    st.caption("View current configuration parameters used by the anomaly detection and forecasting engine.")

    tcols = st.columns(2)
    with tcols[0]:
        st.write(f"**Z Threshold:** {cfg_row.get('Z_THRESHOLD', '—')}")
        st.write(f"**Min Weeks History:** {cfg_row.get('MIN_WEEKS_HISTORY', '—')}")
        st.write(f"**WoW Change %:** {cfg_row.get('WOW_CHANGE_PCT', '—')}")
    with tcols[1]:
        st.write(f"**Forecast Weeks:** {cfg_row.get('FORECAST_WEEKS', '—')}")
        st.write(f"**Max Series:** {cfg_row.get('MAX_SERIES', '—')}")
        st.write(f"**Max Rows Scan:** {cfg_row.get('MAX_ROWS_SCAN', '—')}")

    st.markdown("---")
    st.subheader("AI Settings")

    enable_cortex = bool(cfg_row.get("ENABLE_CORTEX", False))
    model_name = str(cfg_row.get("MODEL_NAME") or "llama3.1-8b")
    cortex_fn = str(cfg_row.get("CORTEX_FN") or "SNOWFLAKE.CORTEX.COMPLETE")

    st.write(f"**Cortex Enabled:** {'Yes' if enable_cortex else 'No'}")
    safe_mode = st.toggle("Safe Mode (VW_* Only)", value=True, key="sb_safe_mode", help="Restricts SQL execution to secure views only")
    allow_execute = st.toggle("Allow Execution", value=False, key="sb_allow_execute", help="Permits SQL execution when enabled")
    power_mode = st.toggle("Power Mode (Allow Writes)", value=False, key="sb_power_mode", help="Enables write operations (use with caution)")
    st.caption("Safe-by-default: SQL generation is always available; execution requires appropriate toggles.")

run_short = run_id[:8] + "..." + run_id[-8:] if len(run_id) > 16 else run_id
st.code(f"Run ID: {run_id}", language="text")

rid = sql_escape(run_id)
wk = sql_escape(sel_week)

dq_run = q_frostbyte(
    VIEWS["DQ_RUN"],
    f"select * from {BASE['DQ_RUN']} where RUN_ID='{rid}'",
    "DQ_RUN",
)
dq_series = q_frostbyte(
    VIEWS["DQ_SERIES"],
    f"select * from {BASE['DQ_SERIES']} where RUN_ID='{rid}'",
    "DQ_SERIES",
)
weekly = df_sql(f"select * from {BASE['SERIES_WEEKLY']} where RUN_ID='{rid}'")
anoms = df_sql(f"select * from {BASE['ANOMALIES_WEEKLY']} where RUN_ID='{rid}'")
forecast = df_sql(f"select * from {BASE['FORECAST_4W']} where RUN_ID='{rid}'")
features = df_sql(f"select * from {BASE['FEATURES']} where RUN_ID='{rid}'")
clusters = df_sql(f"select * from {BASE['CLUSTERS']} where RUN_ID='{rid}'")
actions = df_sql(f"select * from {BASE['ACTIONS']} where RUN_ID='{rid}'")
brief = df_sql(f"select * from {BASE['WEEKLY_BRIEF']} where RUN_ID='{rid}' order by COMPUTED_AT desc limit 1")

weekly = ensure_series_id(weekly)
anoms = ensure_series_id(anoms)
forecast = ensure_series_id(forecast)
dq_series = ensure_series_id(dq_series)
features = ensure_series_id(features)
clusters = ensure_series_id(clusters)

dims_present = any(
    (weekly is not None and (not weekly.empty) and c in weekly.columns and weekly[c].dropna().astype(str).nunique() > 0)
    for c in ["DIM1", "DIM2", "DIM3"]
)

render_fallback_warning()


def filter_bar(key_prefix: str, source_df: pd.DataFrame) -> Dict[str, Any]:
    if source_df is None or source_df.empty:
        return {"ORG_UNIT": None, "METRIC_NAME": None, "CATEGORY": None, "DIM1": None, "DIM2": None, "DIM3": None}

    def uniq(col: str) -> List[str]:
        if col not in source_df.columns:
            return []
        return sorted(source_df[col].dropna().astype(str).unique().tolist())

    orgs = uniq("ORG_UNIT")
    mets = uniq("METRIC_NAME")
    cats = uniq("CATEGORY")
    d1s = uniq("DIM1") if dims_present else []
    d2s = uniq("DIM2") if dims_present else []
    d3s = uniq("DIM3") if dims_present else []

    c1, c2, c3, c4, c5, c6 = st.columns([1.15, 1.15, 1.0, 0.9, 0.9, 0.9])
    with c1:
        o = st.selectbox("Org Unit", ["(All)"] + orgs, key=f"{key_prefix}_org")
    with c2:
        m = st.selectbox("Metric", ["(All)"] + mets, key=f"{key_prefix}_met")
    with c3:
        c = st.selectbox("Category", ["(All)"] + cats, key=f"{key_prefix}_cat")
    with c4:
        d1 = st.selectbox("Dim1", ["(All)"] + d1s, key=f"{key_prefix}_d1") if dims_present else "(All)"
    with c5:
        d2 = st.selectbox("Dim2", ["(All)"] + d2s, key=f"{key_prefix}_d2") if dims_present else "(All)"
    with c6:
        d3 = st.selectbox("Dim3", ["(All)"] + d3s, key=f"{key_prefix}_d3") if dims_present else "(All)"

    def norm(x: str) -> Optional[str]:
        return None if x == "(All)" else x

    return {
        "ORG_UNIT": norm(o),
        "METRIC_NAME": norm(m),
        "CATEGORY": norm(c),
        "DIM1": norm(d1),
        "DIM2": norm(d2),
        "DIM3": norm(d3),
    }


def apply_series_filters(df: pd.DataFrame, f: Dict[str, Any]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in ["ORG_UNIT", "METRIC_NAME", "CATEGORY", "DIM1", "DIM2", "DIM3"]:
        v = f.get(col)
        if v is not None and col in out.columns:
            out = out[out[col].astype(str) == str(v)]
    return out


def _kpi(label: str, value: Any, sub: str = "") -> str:
    if value is None:
        v = "—"
    else:
        v_str = str(value)
        if len(v_str) > 15:
            try:
                num_val = float(value)
                if num_val >= 1_000_000:
                    v = f"{num_val/1_000_000:.2f}M"
                elif num_val >= 1_000:
                    v = f"{num_val/1_000:.1f}K"
                else:
                    v = f"{num_val:.2f}" if isinstance(num_val, float) and num_val % 1 != 0 else str(int(num_val))
            except (ValueError, TypeError):
                v = v_str[:12] + "..." if len(v_str) > 12 else v_str
        else:
            v = v_str
    
    s = f"<div class='fb-kpi-sub'>{sub}</div>" if sub else ""
    return f"""
<div class="fb-kpi">
  <div class="fb-kpi-label">{label}</div>
  <div class="fb-kpi-value">{v}</div>
  {s}
</div>
""".strip()


def render_kpi_tiles():
    status = str(run_row.get("STATUS", "—"))
    latest_week = str(run_row.get("LATEST_WEEK_START", "—"))
    series_count = run_row.get("SERIES_COUNT", "—")
    rows_scanned = run_row.get("ROWS_SCANNED", "—")

    health = missing = dup = negative = None
    if dq_run is not None and not dq_run.empty:
        r = dq_run.iloc[0]
        health = r.get("HEALTH_SCORE", None)
        missing = r.get("MISSING_DAYS", None)
        dup = r.get("DUP_ROWS", None)
        negative = r.get("NEGATIVE_VALUES", None)

    run_short = run_id[:8] + "…" + run_id[-6:] if run_id else "—"
    health_fmt = f"{float(health):.3f}" if health is not None else "—"

    row1 = st.columns(5)
    # with row1[0]: st.markdown(_kpi("Run ID", run_short), unsafe_allow_html=True)
    with row1[0]: st.markdown(_kpi("Status", status), unsafe_allow_html=True)
    with row1[1]: st.markdown(_kpi("Latest Week", latest_week), unsafe_allow_html=True)
    with row1[2]: st.markdown(_kpi("Health Score", health_fmt), unsafe_allow_html=True)
    with row1[3]: st.markdown(_kpi("Series Count", series_count), unsafe_allow_html=True)
    with row1[4]: st.markdown(_kpi("Rows Scanned", rows_scanned), unsafe_allow_html=True)

    row2 = st.columns(3)
    with row2[0]: st.markdown(_kpi("Missing Days", int(missing) if missing is not None else 0), unsafe_allow_html=True)
    with row2[1]: st.markdown(_kpi("Duplicate Rows", int(dup) if dup is not None else 0), unsafe_allow_html=True)
    with row2[2]: st.markdown(_kpi("Negative Values", int(negative) if negative is not None else 0), unsafe_allow_html=True)


def parse_brief_text(brief_text: str) -> Dict[str, Any]:
    sections = {
        "data_quality": [],
        "anomalies": [],
        "forecast": [],
        "next_steps": []
    }
    
    current_section = None
    lines = brief_text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        line_lower = line.lower()
        if 'data quality' in line_lower or 'dq' in line_lower:
            current_section = "data_quality"
        elif 'anomal' in line_lower:
            current_section = "anomalies"
        elif 'forecast' in line_lower or 'confidence' in line_lower:
            current_section = "forecast"
        elif 'next step' in line_lower or 'action' in line_lower or 'recommendation' in line_lower:
            current_section = "next_steps"
        elif current_section and (line.startswith('-') or line.startswith('*') or line.startswith('•')):
            sections[current_section].append(line.lstrip('-*• '))
        elif current_section:
            sections[current_section].append(line)
    
    return sections


def render_weekly_brief(brief_df: pd.DataFrame, anomalies_df: pd.DataFrame):
    if brief_df is None or brief_df.empty:
        st.info("No weekly brief available for this run. Enable Cortex in the configuration to generate AI-powered summaries.")
        return

    b = str(brief_df.iloc[0].get("BRIEF", "") or "").strip()

    cL, cR = st.columns([1.2, 0.8], gap="large")

    with cL:
        with st.container(border=True):
            st.markdown("### Executive Summary")
            if not b:
                st.markdown("_No brief text was generated._")
            else:
                parsed = parse_brief_text(b)
                
                if parsed["data_quality"]:
                    st.markdown('<div class="fb-brief-section-title">📊 Data Quality</div>', unsafe_allow_html=True)
                    for item in parsed["data_quality"]:
                        st.markdown(f"• {item}")
                    st.markdown("")
                
                if parsed["anomalies"]:
                    st.markdown('<div class="fb-brief-section-title">⚠️ Anomalies Detected</div>', unsafe_allow_html=True)
                    for item in parsed["anomalies"]:
                        st.markdown(f"• {item}")
                    st.markdown("")
                
                if parsed["forecast"]:
                    st.markdown('<div class="fb-brief-section-title">📈 Forecast Confidence</div>', unsafe_allow_html=True)
                    for item in parsed["forecast"]:
                        st.markdown(f"• {item}")
                    st.markdown("")
                
                if parsed["next_steps"]:
                    st.markdown('<div class="fb-brief-section-title">✅ Next Steps</div>', unsafe_allow_html=True)
                    for item in parsed["next_steps"]:
                        st.markdown(f"• {item}")

    with cR:
        with st.container(border=True):
            st.markdown("### Top Anomalies")
            an = anomalies_df.copy() if anomalies_df is not None else pd.DataFrame()

            if not an.empty:
                if "WEEK_START" in an.columns and sel_week:
                    an = an[an["WEEK_START"].astype(str) == str(sel_week)]
                if "Z_SCORE" in an.columns:
                    an = an.sort_values("Z_SCORE", ascending=False, key=lambda x: x.abs())

                show = an.head(8)
                for _, r in show.iterrows():
                    org = str(r.get("ORG_UNIT", ""))
                    met = str(r.get("METRIC_NAME", ""))
                    cat = str(r.get("CATEGORY", ""))
                    z = r.get("Z_SCORE", None)
                    val = r.get("VALUE", None)
                    z_txt = f"{float(z):.2f}" if z is not None else "—"
                    val_txt = f"{float(val):.1f}" if val is not None else "—"
                    st.markdown(f"**{org} · {met}**")
                    st.caption(f"{cat} | z={z_txt} | value={val_txt}")
                    st.markdown("")
            else:
                st.markdown("_No anomalies detected for this week._")


def chat_panel(
    panel_id: str,
    title: str,
    ctx: Dict[str, Any],
    visual_payload: Optional[pd.DataFrame],
    suggested_prompts: List[str],
):
    st.markdown(f"<div class='fb-chat-title'>{title}</div>", unsafe_allow_html=True)
    if not enable_cortex:
        st.info("Cortex is disabled for this configuration. Enable ENABLE_CORTEX in the configuration to use the copilot.")
        return
    
    key_msgs = f"chat_msgs_{panel_id}"
    st.session_state.setdefault(key_msgs, [])
    payload = None
    if visual_payload is not None and not visual_payload.empty:
        payload = visual_payload.head(50).to_dict(orient="records")
    
    with st.container():
        for i, p in enumerate(suggested_prompts):
            if st.button(p, key=f"{panel_id}_suggest_{i}", use_container_width=True):
                st.session_state[f"{panel_id}_prefill"] = p
    
    for m in st.session_state[key_msgs]:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if m.get("sql"):
                st.code(m["sql"], language="sql")
            if isinstance(m.get("df"), pd.DataFrame):
                st.dataframe(m["df"], use_container_width=True, hide_index=True)
    
    prefill = st.session_state.pop(f"{panel_id}_prefill", None)
    user_msg = st.chat_input("Ask about this view (SQL, actions, summaries)", key=f"chat_in_{panel_id}")
    if prefill and not user_msg:
        user_msg = prefill
    if not user_msg:
        return
    
    st.session_state[key_msgs].append({"role": "user", "content": user_msg})
    ctx_full = dict(ctx)
    ctx_full["safe_mode"] = bool(safe_mode)
    ctx_full["allow_execute"] = bool(allow_execute)
    ctx_full["power_mode"] = bool(power_mode)
    ctx_full["visual_payload_sample"] = payload
    plan = copilot_plan(cortex_fn, model_name, user_msg, ctx_full)
    intent = str(plan.get("intent", "explain")).lower()
    answer = str(plan.get("answer", "") or "")
    sql_stmt = strip_fences(str(plan.get("sql", "") or "")).strip()
    assistant = {"role": "assistant", "content": answer or plan.get("title", "Result")}
    if sql_stmt:
        assistant["sql"] = sql_stmt
    executed_df = None
    exec_error = None
    if sql_stmt:
        can_execute = bool(allow_execute)
        if safe_mode:
            allowed_prefixes = [
                f"{SCHEMA}.VW_",
                f"{SCHEMA}.FROSTBYTE_"
            ]
            if not any(p in sql_stmt.upper() for p in [a.upper() for a in allowed_prefixes]):
                can_execute = False
                answer = (answer + "\n\nExecution blocked by Safe Mode (VW_* / FROSTBYTE_* only).").strip()
        if can_execute:
            if is_read_query(sql_stmt):
                try:
                    executed_df = df_sql(sql_stmt)
                except Exception as e:
                    exec_error = str(e)
            else:
                if not power_mode:
                    exec_error = "Write execution requires Power Mode to be enabled."
                elif is_dangerous(sql_stmt):
                    exec_error = "Potentially destructive statement detected. Please refine or confirm explicitly."
                else:
                    try:
                        run_sql(sql_stmt)
                    except Exception as e:
                        exec_error = str(e)
        else:
            if not answer:
                answer = "SQL generated. Execution is disabled by current policy settings."
    if executed_df is not None:
        assistant["df"] = executed_df
        if not assistant["content"]:
            assistant["content"] = "Query executed successfully."
    if exec_error:
        assistant["content"] = (assistant["content"] + f"\n\n**Execution Error:** {exec_error}").strip()
    st.session_state[key_msgs].append(assistant)
    audit_ai(run_id, sel_cfg, f"copilot:{panel_id}", model_name, user_msg, json.dumps(plan, default=str))
    st.rerun()

tab_names = ["Dashboard", "Anomalies", "Forecasts", "Clustering", "Actions"]
tabs = st.tabs(tab_names)

base_ctx = {
    "CONFIG_NAME": sel_cfg,
    "RUN_ID": run_id,
    "WEEK_START": sel_week,
    "dims_present": dims_present,
}

with tabs[0]:
    render_kpi_tiles()

    f = filter_bar("dash", weekly)
    weekly_f = apply_series_filters(weekly, f)
    anoms_f = apply_series_filters(anoms, f)
    forecast_f = apply_series_filters(forecast, f)

    left, right = st.columns([1.35, 0.65])

    with left:
        st.markdown("### Top Movers (Week Over Week)")
        movers_payload = None

        if weekly is None or weekly.empty or not sel_week:
            st.info("No weekly data available for this run and week combination.")
        else:
            w = weekly.copy()
            w["WEEK_START"] = w["WEEK_START"].astype(str)
            latest = str(sel_week)
            weeks_sorted = sorted(w["WEEK_START"].dropna().unique().tolist())
            prev = None
            if latest in weeks_sorted:
                idx = weeks_sorted.index(latest)
                if idx > 0:
                    prev = weeks_sorted[idx - 1]

            if prev is None:
                st.info("Not enough historical data to compute week-over-week movers.")
            else:
                a = w[w["WEEK_START"].astype(str) == latest]
                b = w[w["WEEK_START"].astype(str) == prev]
                keys = ["SERIES_ID", "ORG_UNIT", "METRIC_NAME", "CATEGORY", "DIM1", "DIM2", "DIM3"]
                a = a[keys + ["VALUE"]].rename(columns={"VALUE": "VALUE_LATEST"})
                b = b[keys + ["VALUE"]].rename(columns={"VALUE": "VALUE_PREV"})
                m = a.merge(b, on=keys, how="left")
                m["WOW_ABS"] = m["VALUE_LATEST"] - m["VALUE_PREV"]
                m["WOW_PCT"] = m["WOW_ABS"] / m["VALUE_PREV"].replace({0: pd.NA})
                m = apply_series_filters(m, f)
                movers_payload = m.sort_values("WOW_ABS", ascending=False, key=lambda x: x.abs()).head(30)

                display_cols = ["ORG_UNIT", "METRIC_NAME", "CATEGORY"]
                if dims_present:
                    display_cols.extend(["DIM1", "DIM2", "DIM3"])
                display_cols.extend(["VALUE_LATEST", "VALUE_PREV", "WOW_ABS", "WOW_PCT"])
                
                st.dataframe(
                    movers_payload[[c for c in display_cols if c in movers_payload.columns]],
                    use_container_width=True,
                    hide_index=True,
                )

        st.markdown("### Top Anomalies (Latest Week)")
        if anoms_f is None or anoms_f.empty:
            st.info("No anomalies detected for the selected filters.")
        else:
            an = anoms_f.copy()
            if "WEEK_START" in an.columns and sel_week:
                an = an[an["WEEK_START"].astype(str) == str(sel_week)]
            an = an.sort_values("Z_SCORE", ascending=False, key=lambda x: x.abs()) if "Z_SCORE" in an.columns else an
            top_an = an.head(25)
            
            display_cols = ["WEEK_START", "ORG_UNIT", "METRIC_NAME", "CATEGORY"]
            if dims_present:
                display_cols.extend(["DIM1", "DIM2", "DIM3"])
            display_cols.extend(["VALUE", "HIST_MEAN", "Z_SCORE", "STATUS"])
            
            st.dataframe(
                top_an[[c for c in display_cols if c in top_an.columns]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("### Weekly Brief")
        render_weekly_brief(brief, anoms_f)

    with right:
        ctx = dict(base_ctx)
        ctx["filters"] = f
        ctx["screen"] = "Dashboard"
        payload = movers_payload if movers_payload is not None else anoms_f

        chat_panel(
            panel_id="dashboard",
            title="Copilot (Dashboard Context)",
            ctx=ctx,
            visual_payload=payload,
            suggested_prompts=[
                "What changed this week? Summarize top drivers.",
                "Explain the top movers and likely causes.",
                "Write SQL to list anomalies by org and severity.",
                "Draft stakeholder summary for the weekly ops brief.",
            ],
        )

with tabs[1]:
    f = filter_bar("anom", weekly)
    an = apply_series_filters(anoms, f)

    cA, cB, cC = st.columns([1.0, 1.0, 1.0])
    with cA:
        status_vals = ["(All)"] + (sorted(an["STATUS"].dropna().astype(str).unique().tolist()) if (an is not None and not an.empty and "STATUS" in an.columns) else [])
        pick_status = st.selectbox("Status", status_vals, key="anom_status")
    with cB:
        if an is not None and not an.empty and "Z_SCORE" in an.columns:
            zmin, zmax = float(an["Z_SCORE"].abs().min()), float(an["Z_SCORE"].abs().max())
        else:
            zmin, zmax = 0.0, 10.0
        z_band = st.slider("Minimum Z-Score (Absolute)", min_value=0.0, max_value=max(10.0, zmax), value=min(3.0, zmax), step=0.5, key="anom_zmin")
    with cC:
        only_week = st.toggle("Show Selected Week Only", value=True, key="anom_only_week")

    if an is None or an.empty:
        st.info("No anomalies detected for the current filters.")
        left, right = st.columns([1.35, 0.65])
        with right:
            chat_panel(
                panel_id="anomalies_empty",
                title="Copilot (Anomalies Context)",
                ctx=dict(base_ctx, filters=f, screen="Anomalies"),
                visual_payload=None,
                suggested_prompts=["Write SQL to find anomalies for this run.", "Explain how anomalies are computed."],
            )
    else:
        if pick_status != "(All)" and "STATUS" in an.columns:
            an = an[an["STATUS"].astype(str) == str(pick_status)]
        if "Z_SCORE" in an.columns:
            an = an[an["Z_SCORE"].abs() >= float(z_band)]
        if only_week and sel_week and "WEEK_START" in an.columns:
            an = an[an["WEEK_START"].astype(str) == str(sel_week)]

        an = an.sort_values("Z_SCORE", ascending=False, key=lambda x: x.abs()) if "Z_SCORE" in an.columns else an

        left, right = st.columns([1.35, 0.65])

        with left:
            st.markdown("### Anomaly Inbox")
            
            display_cols = ["WEEK_START", "ORG_UNIT", "METRIC_NAME", "CATEGORY"]
            if dims_present:
                display_cols.extend(["DIM1", "DIM2", "DIM3"])
            display_cols.extend(["VALUE", "HIST_MEAN", "HIST_STD", "Z_SCORE", "STATUS", "SERIES_ID"])
            
            st.dataframe(
                an[[c for c in display_cols if c in an.columns]],
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("### Drilldown (Selected Series)")
            series_ids = an["SERIES_ID"].dropna().astype(str).unique().tolist() if "SERIES_ID" in an.columns else []
            pick_sid = st.selectbox("Select Anomaly Series", series_ids, key="anom_sid") if series_ids else None

            if pick_sid:
                w = weekly[weekly["SERIES_ID"].astype(str) == str(pick_sid)].copy() if (weekly is not None and not weekly.empty) else pd.DataFrame()
                w = w.sort_values("WEEK_START") if (not w.empty and "WEEK_START" in w.columns) else w
                if not w.empty:
                    w["WEEK_START"] = pd.to_datetime(w["WEEK_START"])
                    st.line_chart(w.set_index("WEEK_START")["VALUE"], use_container_width=True)

                sel_row = an[an["SERIES_ID"].astype(str) == str(pick_sid)].head(1)
                if not sel_row.empty:
                    r = sel_row.iloc[0].to_dict()
                    c1, c2, c3 = st.columns(3)
                    val = r.get('VALUE', '—')
                    hist = r.get('HIST_MEAN', '—')
                    z = r.get('Z_SCORE', '—')
                    
                    try:
                        val_fmt = f"{float(val):.2f}" if val != '—' else '—'
                    except:
                        val_fmt = str(val)
                    
                    try:
                        hist_fmt = f"{float(hist):.2f}" if hist != '—' else '—'
                    except:
                        hist_fmt = str(hist)
                    
                    try:
                        z_fmt = f"{float(z):.2f}" if z != '—' else '—'
                    except:
                        z_fmt = str(z)
                    
                    c1.metric("Current Value", val_fmt)
                    c2.metric("Historical Mean", hist_fmt)
                    c3.metric("Z-Score", z_fmt)

        with right:
            ctx = dict(base_ctx)
            ctx["filters"] = f
            ctx["screen"] = "Anomalies"
            ctx["selected_series_id"] = pick_sid if 'pick_sid' in locals() else None
            payload = an.head(50)

            chat_panel(
                panel_id="anomalies",
                title="Copilot (Anomaly Investigation)",
                ctx=ctx,
                visual_payload=payload,
                suggested_prompts=[
                    "Explain the selected anomaly and likely root causes.",
                    "Write SQL to pull raw daily data for this series.",
                    "Draft an investigation plan and next steps.",
                    "Create an action item for this anomaly.",
                ],
            )


with tabs[2]:
    f = filter_bar("fc", weekly)
    fc = apply_series_filters(forecast, f)
    w = apply_series_filters(weekly, f)

    left, right = st.columns([1.35, 0.65])
    with left:
        st.markdown("### Forecast (Selected Series)")
        if fc is None or fc.empty:
            st.info("No forecast data available for the selected filters.")
        else:
            series_ids = fc["SERIES_ID"].dropna().astype(str).unique().tolist() if "SERIES_ID" in fc.columns else []
            pick_sid = st.selectbox("Series", series_ids, key="fc_sid") if series_ids else None

            if pick_sid:
                w1 = weekly[weekly["SERIES_ID"].astype(str) == str(pick_sid)].copy() if (weekly is not None and not weekly.empty) else pd.DataFrame()
                f1 = fc[fc["SERIES_ID"].astype(str) == str(pick_sid)].copy()
                if not w1.empty:
                    w1 = w1.sort_values("WEEK_START")
                    w1["WEEK_START"] = pd.to_datetime(w1["WEEK_START"])
                if not f1.empty and "FORECAST_WEEK_START" in f1.columns:
                    f1 = f1.sort_values("FORECAST_WEEK_START")
                    f1["FORECAST_WEEK_START"] = pd.to_datetime(f1["FORECAST_WEEK_START"])

                cA, cB = st.columns([1, 1])
                with cA:
                    st.caption("Historical Actuals")
                    if not w1.empty:
                        st.line_chart(w1.set_index("WEEK_START")["VALUE"], use_container_width=True)
                with cB:
                    st.caption("Forecast Values")
                    if not f1.empty and "FORECAST_VALUE" in f1.columns:
                        st.line_chart(f1.set_index("FORECAST_WEEK_START")["FORECAST_VALUE"], use_container_width=True)

                st.markdown("### Forecast Table (Filtered)")
                display_cols = ["FORECAST_WEEK_START", "ORG_UNIT", "METRIC_NAME", "CATEGORY"]
                if dims_present:
                    display_cols.extend(["DIM1", "DIM2", "DIM3"])
                display_cols.extend(["FORECAST_VALUE", "METHOD", "SERIES_ID"])
                
                cols = [c for c in display_cols if c in fc.columns]
                st.dataframe(fc[cols].sort_values("FORECAST_WEEK_START") if "FORECAST_WEEK_START" in fc.columns else fc[cols], use_container_width=True, hide_index=True)

        st.markdown("### Confidence Signals (Derived)")
        if features is None or features.empty:
            st.info("No series feature data available. Features are required to derive confidence signals.")
        else:
            feat = apply_series_filters(features, f)
            display_cols = ["ORG_UNIT", "METRIC_NAME", "CATEGORY"]
            if dims_present:
                display_cols.extend(["DIM1", "DIM2", "DIM3"])
            display_cols.extend(["VOLATILITY", "ANOMALY_RATE", "MISSING_PCT", "SERIES_ID"])
            
            cols = [c for c in display_cols if c in feat.columns]
            st.dataframe(feat[cols].head(50), use_container_width=True, hide_index=True)

    with right:
        ctx = dict(base_ctx)
        ctx["filters"] = f
        ctx["screen"] = "Forecasts"
        payload = fc.head(50) if (fc is not None and not fc.empty) else w.head(50)

        chat_panel(
            panel_id="forecasts",
            title="Copilot (Forecast Reasoning)",
            ctx=ctx,
            visual_payload=payload,
            suggested_prompts=[
                "Explain this forecast and key uncertainty drivers.",
                "Write SQL to validate forecast vs last 8 weeks baseline.",
                "Draft stakeholder summary for forecast outlook.",
                "Generate what-if SQL scenario analysis.",
            ],
        )

with tabs[3]:
    clustering_available = (clusters is not None and not clusters.empty)
    if not clustering_available:
        st.info("Clustering is not available for this run. Enable clustering in the configuration or ensure the run completed successfully with clustering enabled.")
        left, right = st.columns([1.35, 0.65])
        with right:
            chat_panel(
                panel_id="clustering_empty",
                title="Copilot (Clustering)",
                ctx=dict(base_ctx, screen="Clustering"),
                visual_payload=None,
                suggested_prompts=["Explain how clustering works and what features are used.", "Recommend cluster-based monitoring strategy."],
            )
    else:
        f = filter_bar("clu", weekly)
        cl = apply_series_filters(clusters, f)
        feat = apply_series_filters(features, f)

        left, right = st.columns([1.35, 0.65])
        with left:
            st.markdown("### Cluster Sizes")
            if cl is None or cl.empty or "CLUSTER_ID" not in cl.columns:
                st.info("Cluster data is present but CLUSTER_ID column is missing.")
            else:
                sizes = cl.groupby("CLUSTER_ID").size().reset_index(name="N").sort_values("N", ascending=False)
                st.bar_chart(sizes.set_index("CLUSTER_ID")["N"], use_container_width=True)
                pick_cluster = st.selectbox("Select Cluster", sizes["CLUSTER_ID"].astype(int).tolist(), key="clu_pick")

                st.markdown("### Cluster Profile (Average Features)")
                if feat is not None and not feat.empty and "SERIES_ID" in feat.columns:
                    members = cl[cl["CLUSTER_ID"] == pick_cluster]["SERIES_ID"].astype(str).unique().tolist()
                    prof = feat[feat["SERIES_ID"].astype(str).isin([str(x) for x in members])]
                    cols = [c for c in ["VOLATILITY","SLOPE","RECENT_WOW_ABS","ANOMALY_RATE","MISSING_PCT"] if c in prof.columns]
                    if cols and not prof.empty:
                        agg = prof[cols].astype(float).mean().to_frame("Average").reset_index().rename(columns={"index": "Feature"})
                        st.dataframe(agg, use_container_width=True, hide_index=True)

                st.markdown("### Representative Series")
                rep = cl[cl["CLUSTER_ID"] == pick_cluster].head(50)
                display_cols = ["ORG_UNIT", "METRIC_NAME", "CATEGORY"]
                if dims_present:
                    display_cols.extend(["DIM1", "DIM2", "DIM3"])
                display_cols.append("SERIES_ID")
                
                st.dataframe(rep[[c for c in display_cols if c in rep.columns]], use_container_width=True, hide_index=True)

        with right:
            ctx = dict(base_ctx)
            ctx["filters"] = f
            ctx["screen"] = "Clustering"
            payload = cl.head(50) if cl is not None else None

            chat_panel(
                panel_id="clustering",
                title="Copilot (Cluster Interpretation)",
                ctx=ctx,
                visual_payload=payload,
                suggested_prompts=[
                    "Describe what this cluster behavior suggests.",
                    "Compare clusters and recommend monitoring focus.",
                    "Recommend threshold adjustments based on cluster analysis.",
                ],
            )

with tabs[4]:
    f = filter_bar("act", weekly)
    act = actions.copy() if actions is not None else pd.DataFrame()
    act = ensure_series_id(act)

    left, right = st.columns([1.35, 0.65])

    with left:
        st.markdown("### Action Queue")
        if act is None or act.empty:
            st.info("No actions generated for this run.")
        else:
            c1, c2, c3 = st.columns([1.0, 1.0, 1.2])
            with c1:
                types = ["(All)"] + (sorted(act["ACTION_TYPE"].dropna().astype(str).unique().tolist()) if "ACTION_TYPE" in act.columns else [])
                pick_type = st.selectbox("Action Type", types, key="act_type")
            with c2:
                sevs = ["(All)"] + (sorted(act["SEVERITY"].dropna().astype(str).unique().tolist()) if "SEVERITY" in act.columns else [])
                pick_sev = st.selectbox("Severity", sevs, key="act_sev")
            with c3:
                qtxt = st.text_input("Search Actions", value="", key="act_search", placeholder="Search by keyword...")

            a = act.copy()
            if pick_type != "(All)" and "ACTION_TYPE" in a.columns:
                a = a[a["ACTION_TYPE"].astype(str) == str(pick_type)]
            if pick_sev != "(All)" and "SEVERITY" in a.columns:
                a = a[a["SEVERITY"].astype(str) == str(pick_sev)]
            if qtxt:
                hay = a.astype(str).agg(" ".join, axis=1)
                a = a[hay.str.contains(qtxt, case=False, na=False)]

            ex1, ex2 = st.columns([1, 1])
            with ex1:
                st.download_button(
                    "Export CSV",
                    data=a.to_csv(index=False).encode("utf-8"),
                    file_name=f"frostbyte_actions_{run_id}.csv",
                    key="act_export_csv",
                )
            with ex2:
                def df_to_md_table_no_tabulate(df: pd.DataFrame, max_rows: int = 200) -> str:
                    if df is None or df.empty:
                        return ""
                    d = df.head(max_rows).copy()
                    cols = [str(c) for c in d.columns]
                    header = "| " + " | ".join(cols) + " |"
                    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
                    rows = []
                    for _, r in d.iterrows():
                        vals = [str(r[c]).replace("\n", " ").replace("|", "\\|") for c in d.columns]
                        rows.append("| " + " | ".join(vals) + " |")
                    return "\n".join([header, sep] + rows)
                
                md = df_to_md_table_no_tabulate(a[[c for c in ["COMPUTED_AT","ACTION_TYPE","SEVERITY","MESSAGE","SERIES_ID"] if c in a.columns]].copy())
                st.download_button(
                    "Export Markdown",
                    data=md.encode("utf-8"),
                    file_name=f"frostbyte_actions_{run_id}.md",
                    key="act_export_md",
                )
            
            display_cols = ["COMPUTED_AT", "ACTION_TYPE", "SEVERITY", "MESSAGE", "SERIES_ID"]
            st.dataframe(
                a[[c for c in display_cols if c in a.columns]].head(200),
                use_container_width=True,
                hide_index=True,
            )
            
            st.markdown("### Action Details (Selected)")
            if "MESSAGE" in a.columns and not a.empty:
                idx = st.number_input("Select Row Index", min_value=0, max_value=max(0, len(a) - 1), value=0, step=1, key="act_row_idx")
                row = a.iloc[int(idx)].to_dict()
                st.markdown(
                    f"""
<div class="fb-status-card">
<b>Type:</b> {row.get("ACTION_TYPE","—")} | <b>Severity:</b> {row.get("SEVERITY","—")}<br/>
<b>Computed:</b> {row.get("COMPUTED_AT","—")}<br/><br/>
<b>Message:</b><br/>{row.get("MESSAGE","")}
</div>
""",
                    unsafe_allow_html=True,
                )

    with right:
        ctx = dict(base_ctx)
        ctx["filters"] = f
        ctx["screen"] = "Actions"
        payload = act.head(50) if act is not None else None

        chat_panel(
            panel_id="actions",
            title="Copilot (Triage & Runbooks)",
            ctx=ctx,
            visual_payload=payload,
            suggested_prompts=[
                "Group actions by likely root cause and prioritize.",
                "Draft runbook steps for high-severity items.",
                "Write SQL to summarize actions by org/metric/severity.",
                "Draft communications message for stakeholders.",
            ],
        )
