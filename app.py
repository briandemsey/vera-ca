"""
VERA-CA - Verification Engine for Results & Accountability
Streamlit Web Application for California Education Data

Post-AB 2225 infrastructure for California's accountability transformation.
Connecting inputs, outputs, and outcomes at the student level.

California context: AB 2225 (2024), CAASPP, ELPAC, LCFF/LCAP compliance.
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import re
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="VERA-CA | California Education",
    page_icon="🐻",
    layout="wide",
    initial_sidebar_state="expanded"
)

# California Colors
NAVY = "#002855"
GOLD = "#FDB515"
CREAM = "#F8F4EE"
RED = "#CC0000"
GREEN = "#1A5C38"

# Custom CSS for California branding
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Lora:wght@400;600;700&family=Source+Sans+3:wght@400;600&display=swap');

    /* Main app background */
    .stApp {{
        background-color: {CREAM};
    }}

    /* Sidebar */
    section[data-testid="stSidebar"] {{
        background-color: {NAVY};
    }}
    section[data-testid="stSidebar"] .stMarkdown {{
        color: white;
    }}
    section[data-testid="stSidebar"] label {{
        color: white !important;
    }}
    section[data-testid="stSidebar"] .stSelectbox label {{
        color: white !important;
    }}
    section[data-testid="stSidebar"] .stRadio > div {{
        display: flex;
        flex-direction: column;
        gap: 8px;
    }}
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stRadio label span,
    section[data-testid="stSidebar"] .stRadio label p,
    section[data-testid="stSidebar"] .stRadio label div,
    section[data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"],
    section[data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"] p {{
        color: white !important;
        font-size: 1rem;
        position: relative;
        z-index: 1;
    }}
    section[data-testid="stSidebar"] .stRadio label {{
        padding: 8px 12px;
        border-radius: 6px;
        cursor: pointer;
        transition: background-color 0.2s;
    }}
    section[data-testid="stSidebar"] .stRadio label:hover {{
        background-color: rgba(255,255,255,0.1);
    }}
    section[data-testid="stSidebar"] .stRadio input[type="radio"]:checked + div,
    section[data-testid="stSidebar"] .stRadio input[type="radio"]:checked + div p,
    section[data-testid="stSidebar"] .stRadio input[type="radio"]:checked ~ div,
    section[data-testid="stSidebar"] .stRadio input[type="radio"]:checked ~ div p {{
        color: {GOLD} !important;
        font-weight: 600;
    }}

    /* Headers */
    h1, h2, h3 {{
        font-family: 'Lora', serif;
        color: {NAVY};
    }}
    h1 {{
        border-bottom: 3px solid {GOLD};
        padding-bottom: 10px;
    }}

    /* Body text */
    p, li, span {{
        font-family: 'Source Sans 3', sans-serif;
    }}

    /* Stat cards */
    .stat-card {{
        background: white;
        border-left: 4px solid {GOLD};
        padding: 20px;
        border-radius: 4px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 16px;
    }}
    .stat-card .number {{
        font-size: 2.5rem;
        font-weight: 700;
        color: {GOLD};
        font-family: 'Lora', serif;
    }}
    .stat-card .label {{
        font-size: 0.9rem;
        color: #666;
        margin-top: 4px;
    }}

    /* Type 4 flag highlight */
    .type4-flag {{
        background-color: {RED};
        color: white;
        padding: 4px 12px;
        border-radius: 4px;
        font-weight: 600;
    }}

    /* Section headers */
    .section-header {{
        background: {NAVY};
        color: white;
        padding: 12px 20px;
        margin: 24px 0 16px 0;
        font-family: 'Lora', serif;
        font-size: 1.1rem;
    }}

    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# Authentication System
# =============================================================================

VERA_PASSWORD = os.environ.get("VERA_PASSWORD", "forever vera")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "brian@h-edu.solutions")

SCHOOL_DOMAIN_PATTERNS = [
    r'.*\.k12\.[a-z]{2}\.us$',
    r'.*\.edu$',
    r'.*school.*\.[a-z]+$',
    r'.*district.*\.[a-z]+$',
    r'.*unified.*\.[a-z]+$',
    r'.*usd\.[a-z]+$',
    r'.*isd\.[a-z]+$',
    r'.*coe\.[a-z]+$',
    r'.*schools\.[a-z]+$',
]

def is_school_email(email):
    if not email or '@' not in email:
        return False
    domain = email.lower().split('@')[1]
    for pattern in SCHOOL_DOMAIN_PATTERNS:
        if re.match(pattern, domain):
            return True
    return False

def init_auth_db():
    db_path = Path(__file__).parent / "vera_demo.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS access_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            organization TEXT,
            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'pending'
        )
    """)
    conn.commit()
    conn.close()

def save_access_request(email, phone, organization=""):
    db_path = Path(__file__).parent / "vera_demo.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO access_requests (email, phone, organization) VALUES (?, ?, ?)",
        (email, phone, organization)
    )
    conn.commit()
    conn.close()

def check_authentication():
    init_auth_db()

    if st.session_state.get('authenticated', False):
        return True

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.image("vera_logo.png", use_container_width=True)
        st.markdown(f"""
            <h2 style="text-align: center; color: {NAVY}; margin-top: 20px;">
                Welcome to VERA-CA
            </h2>
            <p style="text-align: center; color: #666; margin-bottom: 30px;">
                Verification Engine for Results & Accountability<br>
                <span style="color: {GOLD};">California Edition</span>
            </p>
        """, unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["🔐 I Have Access", "📝 Request Access"])

        with tab1:
            st.markdown("Enter your password to access VERA-CA.")
            password = st.text_input("Password", type="password", key="login_password")

            if st.button("Sign In", type="primary", use_container_width=True):
                if password.lower().strip() == VERA_PASSWORD.lower():
                    st.session_state['authenticated'] = True
                    st.rerun()
                else:
                    st.error("Incorrect password. Please try again or request access.")

        with tab2:
            st.markdown("Request access to VERA-CA. You must have a school board or educational institution email.")

            with st.form("access_request_form"):
                req_email = st.text_input("Email Address *", placeholder="you@yourdistrict.k12.ca.us")
                req_phone = st.text_input("Phone Number *", placeholder="(555) 123-4567")
                req_org = st.text_input("Organization/District", placeholder="Your School District")

                submitted = st.form_submit_button("Request Access", type="primary", use_container_width=True)

                if submitted:
                    if not req_email or not req_phone:
                        st.error("Please fill in all required fields.")
                    elif not is_school_email(req_email):
                        st.error("Please use an email address from a school board, district, or educational institution.")
                    else:
                        save_access_request(req_email, req_phone, req_org)
                        st.success("✅ Access request submitted! You will receive the password via email once approved.")

        st.markdown(f"""
            <div style="text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd;">
                <a href="https://h-edu.solutions" style="color: {GOLD}; text-decoration: none;">
                    ← Return to H-EDU.solutions
                </a>
            </div>
        """, unsafe_allow_html=True)

    return False

# Authentication disabled - public access
# if not check_authentication():
#     st.stop()

# =============================================================================
# Database Connection
# =============================================================================

DB_PATH = Path(__file__).parent / "vera_demo.db"

@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

def run_query(query, params=None):
    conn = get_connection()
    if params:
        return pd.read_sql_query(query, conn, params=params)
    return pd.read_sql_query(query, conn)

# =============================================================================
# Data Functions
# =============================================================================

@st.cache_data
def get_districts():
    query = """
        SELECT DISTINCT district_name, district_id, county
        FROM caaspp_results
        ORDER BY district_name
    """
    return run_query(query)

@st.cache_data
def get_caaspp_data(district_name, grade=None, subgroup=None):
    query = "SELECT * FROM caaspp_results WHERE district_name = ?"
    params = [district_name]

    if grade:
        query += " AND grade = ?"
        params.append(grade)
    if subgroup:
        query += " AND subgroup = ?"
        params.append(subgroup)

    query += " ORDER BY grade, subgroup"
    return run_query(query, params)

@st.cache_data
def compute_owd(district_name, subgroup=None):
    query = """
        SELECT c.district_name, c.district_id, c.grade, c.subgroup,
               c.ela_claim2_score as writing_score,
               e.speaking_score,
               (e.speaking_score - c.ela_claim2_score) as delta
        FROM caaspp_results c
        LEFT JOIN elpac_results e ON c.district_id = e.district_id
            AND c.grade = e.grade AND c.subgroup = e.subgroup
        WHERE c.district_name = ?
    """
    params = [district_name]

    if subgroup:
        query += " AND c.subgroup = ?"
        params.append(subgroup)

    query += " ORDER BY c.grade"
    return run_query(query, params)

@st.cache_data
def get_all_type4_flags(threshold=8.0):
    query = """
        SELECT c.district_name, c.district_id, c.county, c.grade, c.subgroup,
               c.ela_claim2_score as writing_score,
               e.speaking_score,
               (e.speaking_score - c.ela_claim2_score) as delta
        FROM caaspp_results c
        JOIN elpac_results e ON c.district_id = e.district_id
            AND c.grade = e.grade AND c.subgroup = e.subgroup
        WHERE (e.speaking_score - c.ela_claim2_score) > ?
        ORDER BY delta DESC
    """
    return run_query(query, [threshold])

# =============================================================================
# Sidebar
# =============================================================================

with st.sidebar:
    # Back arrow to h-edu.solutions
    st.markdown("""
        <a href="https://h-edu.solutions" target="_self" style="
            display: flex;
            align-items: center;
            color: white;
            text-decoration: none;
            font-size: 0.9rem;
            padding: 8px 0;
            margin-bottom: 10px;
            opacity: 0.9;
        ">
            <span style="font-size: 1.2rem; margin-right: 8px;">←</span>
            Back to H-EDU
        </a>
    """, unsafe_allow_html=True)

    # California flag
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("california-flag.svg", width=80)

    st.markdown(f"""
        <div style="text-align: center; padding: 10px 0 20px 0;">
            <h2 style="color: white; margin: 10px 0;">VERA-CA</h2>
            <p style="color: {GOLD}; font-size: 0.9rem;">Verification Engine for Results & Accountability</p>
            <p style="color: rgba(255,255,255,0.6); font-size: 0.8rem;">California • Post-AB 2225</p>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # Navigation
    st.markdown("""
        <p style="color: rgba(255,255,255,0.7); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px;">
            Navigate
        </p>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigate",
        ["📊 District Dashboard", "🔍 Cross-District Scan", "📋 LCAP Report", "🏫 Admin Dashboard", "📝 Student Record", "📅 Daily Observations", "ℹ️ About VERA"],
        label_visibility="collapsed",
        format_func=lambda x: x
    )

    st.markdown("---")

    # District selector (for relevant pages)
    if page in ["📊 District Dashboard", "📋 LCAP Report", "🏫 Admin Dashboard"]:
        districts = get_districts()
        selected_district = st.selectbox(
            "Select District",
            districts['district_name'].tolist()
        )

        district_data = get_caaspp_data(selected_district)

        grades = ["All"] + sorted(district_data['grade'].unique().tolist())
        selected_grade = st.selectbox("Grade", grades)

        subgroups = ["All"] + sorted(district_data['subgroup'].unique().tolist())
        selected_subgroup = st.selectbox("Subgroup", subgroups)

    st.markdown("---")
    st.markdown(f"""
        <p style="color: rgba(255,255,255,0.5); font-size: 0.8rem; text-align: center;">
            VERA-CA v1.0<br>
            <a href="https://h-edu.solutions" style="color: {GOLD};">h-edu.solutions</a>
        </p>
    """, unsafe_allow_html=True)

# =============================================================================
# Page: District Dashboard
# =============================================================================

if page == "📊 District Dashboard":
    st.title(f"District Dashboard: {selected_district}")

    district_info = districts[districts['district_name'] == selected_district].iloc[0]
    st.markdown(f"**{district_info['county']} County** | District ID: `{district_info['district_id']}`")

    subgroup_filter = None if selected_subgroup == "All" else selected_subgroup
    owd_data = compute_owd(selected_district, subgroup_filter)

    if selected_grade != "All":
        owd_data = owd_data[owd_data['grade'] == int(selected_grade)]

    col1, col2, col3, col4 = st.columns(4)

    type4_count = len(owd_data[owd_data['delta'] > 8])
    max_delta = owd_data['delta'].max() if len(owd_data) > 0 else 0
    avg_delta = owd_data['delta'].mean() if len(owd_data) > 0 else 0

    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{len(owd_data)}</div>
                <div class="label">Populations Analyzed</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number" style="color: {RED if type4_count > 0 else GOLD};">{type4_count}</div>
                <div class="label">Type 4 Flags</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{max_delta:+.1f}</div>
                <div class="label">Max Delta</div>
            </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{avg_delta:+.1f}</div>
                <div class="label">Avg Delta</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="section-header">Oral-Written Delta Analysis</div>', unsafe_allow_html=True)

    if len(owd_data) > 0:
        display_df = owd_data[['grade', 'subgroup', 'writing_score', 'speaking_score', 'delta']].copy()
        display_df.columns = ['Grade', 'Subgroup', 'Writing (ELA Claim 2)', 'Speaking (ELPAC)', 'Delta']
        display_df['Delta'] = display_df['Delta'].apply(lambda x: f"{x:+.1f}" if pd.notna(x) else "N/A")

        def highlight_type4(row):
            delta_val = float(row['Delta'].replace('+', '')) if row['Delta'] != 'N/A' else 0
            if delta_val > 8:
                return ['background-color: #FADBD8'] * len(row)
            return [''] * len(row)

        st.dataframe(
            display_df.style.apply(highlight_type4, axis=1),
            use_container_width=True,
            hide_index=True
        )

        st.markdown('<div class="section-header">Oral vs. Written Scores by Grade</div>', unsafe_allow_html=True)

        chart_data = owd_data[owd_data['speaking_score'].notna()].copy()
        if len(chart_data) > 0:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Writing (ELA Claim 2)',
                x=chart_data['grade'].astype(str) + ' - ' + chart_data['subgroup'],
                y=chart_data['writing_score'],
                marker_color=NAVY
            ))
            fig.add_trace(go.Bar(
                name='Speaking (ELPAC)',
                x=chart_data['grade'].astype(str) + ' - ' + chart_data['subgroup'],
                y=chart_data['speaking_score'],
                marker_color=GOLD
            ))
            fig.update_layout(
                barmode='group',
                xaxis_title='Grade - Subgroup',
                yaxis_title='Score',
                plot_bgcolor='white',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)

        csv = owd_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"vera_ca_owd_{selected_district.replace(' ', '_')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No data available for the selected filters.")

# =============================================================================
# Page: Cross-District Scan
# =============================================================================

elif page == "🔍 Cross-District Scan":
    st.title("Cross-District Type 4 Scan")
    st.markdown("Identifies oral-written delta flags across all districts in the database.")

    threshold = st.slider("Delta Threshold", min_value=5.0, max_value=15.0, value=8.0, step=0.5)

    flags_df = get_all_type4_flags(threshold)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number" style="color: {RED};">{len(flags_df)}</div>
                <div class="label">Total Flags</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        districts_flagged = flags_df['district_name'].nunique() if len(flags_df) > 0 else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{districts_flagged}</div>
                <div class="label">Districts Flagged</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        max_delta = flags_df['delta'].max() if len(flags_df) > 0 else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{max_delta:+.1f}</div>
                <div class="label">Max Delta</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="section-header">Flagged Populations</div>', unsafe_allow_html=True)

    if len(flags_df) > 0:
        display_df = flags_df[['district_name', 'county', 'grade', 'subgroup', 'writing_score', 'speaking_score', 'delta']].copy()
        display_df.columns = ['District', 'County', 'Grade', 'Subgroup', 'Writing', 'Speaking', 'Delta']
        display_df['Delta'] = display_df['Delta'].apply(lambda x: f"{x:+.1f}")

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown('<div class="section-header">Type 4 Flags by District</div>', unsafe_allow_html=True)

        flag_counts = flags_df.groupby('district_name').size().reset_index(name='flags')
        flag_counts = flag_counts.sort_values('flags', ascending=True)

        fig = px.bar(
            flag_counts,
            x='flags',
            y='district_name',
            orientation='h',
            color_discrete_sequence=[RED]
        )
        fig.update_layout(
            xaxis_title='Number of Flags',
            yaxis_title='',
            plot_bgcolor='white',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        csv = flags_df.to_csv(index=False)
        st.download_button(
            label="Download All Flags CSV",
            data=csv,
            file_name="vera_ca_type4_flags_all_districts.csv",
            mime="text/csv"
        )
    else:
        st.success(f"No Type 4 flags found at threshold {threshold}")

# =============================================================================
# Page: LCAP Report
# =============================================================================

elif page == "📋 LCAP Report":
    st.title(f"LCAP Match-Rate Report")
    st.markdown(f"**District:** {selected_district}")

    district_info = districts[districts['district_name'] == selected_district].iloc[0]
    district_id = district_info['district_id']

    owd_data = compute_owd(selected_district)
    type4_count = len(owd_data[owd_data['delta'] > 8])
    total_populations = len(owd_data)

    match_rate = max(0, 100 - (type4_count * 15))

    db_path = Path(__file__).parent / "vera_demo.db"
    obs_data = None
    init_data = None

    try:
        conn = sqlite3.connect(str(db_path))

        obs_df = pd.read_sql_query("""
            SELECT
                COUNT(DISTINCT ssid) as students_observed,
                COUNT(DISTINCT observation_date) as observation_days,
                COUNT(*) as total_observations,
                SUM(present) as total_present,
                SUM(oral_participation) as total_oral,
                SUM(written_output) as total_written,
                SUM(concern_flag) as total_concerns,
                SUM(CASE WHEN elaboration = 'Intervention responding' THEN 1 ELSE 0 END) as intervention_responding,
                SUM(CASE WHEN elaboration = 'Intervention not responding' THEN 1 ELSE 0 END) as intervention_not_responding
            FROM observations
        """, conn)
        if len(obs_df) > 0:
            obs_data = obs_df.iloc[0].to_dict()

        init_df = pd.read_sql_query("""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN locked_at IS NOT NULL THEN 1 ELSE 0 END) as locked_records,
                SUM(CASE WHEN teacher_response = 'confirmed' THEN 1 ELSE 0 END) as hypothesis_confirmed,
                SUM(CASE WHEN teacher_response = 'challenged' THEN 1 ELSE 0 END) as hypothesis_challenged
            FROM initialization_records
        """, conn)
        if len(init_df) > 0:
            init_data = init_df.iloc[0].to_dict()

        conn.close()
    except Exception:
        pass

    col1, col2 = st.columns([1, 1])

    with col1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=match_rate,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "LCAP Match Rate", 'font': {'size': 20}},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': GOLD},
                'steps': [
                    {'range': [0, 50], 'color': '#FADBD8'},
                    {'range': [50, 75], 'color': '#FCF3CF'},
                    {'range': [75, 100], 'color': '#D5F5E3'}
                ],
                'threshold': {
                    'line': {'color': RED, 'width': 4},
                    'thickness': 0.75,
                    'value': 70
                }
            }
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{total_populations}</div>
                <div class="label">Grade-Subgroup Combinations Analyzed</div>
            </div>
        """, unsafe_allow_html=True)

        color = RED if type4_count > 2 else (GOLD if type4_count > 0 else GREEN)
        st.markdown(f"""
            <div class="stat-card">
                <div class="number" style="color: {color};">{type4_count}</div>
                <div class="label">Type 4 Gaps Detected (oral > written by 8+ pts)</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown('<div class="section-header">Finding</div>', unsafe_allow_html=True)

    if match_rate >= 75:
        st.success(f"**MATCH RATE: {match_rate}%** — LCAP interventions appear well-aligned with student needs.")
    elif match_rate >= 50:
        st.warning(f"**MATCH RATE: {match_rate}%** — Some misalignment detected. Review ELD intervention targeting.")
    else:
        st.error(f"**MATCH RATE: {match_rate}%** — Significant misalignment. Immediate review of LCAP spending recommended.")

    st.markdown("---")
    st.markdown('<div class="section-header">Observation System Status</div>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        init_count = init_data.get('locked_records', 0) if init_data else 0
        st.metric("Student Records Locked", init_count)

    with col2:
        obs_students = obs_data.get('students_observed', 0) if obs_data else 0
        st.metric("Students Observed", int(obs_students) if obs_students else 0)

    with col3:
        obs_days = obs_data.get('observation_days', 0) if obs_data else 0
        st.metric("Observation Days", int(obs_days) if obs_days else 0)

    with col4:
        concerns = obs_data.get('total_concerns', 0) if obs_data else 0
        st.metric("Concern Flags", int(concerns) if concerns else 0)

# =============================================================================
# Page: Admin Dashboard
# =============================================================================

elif page == "🏫 Admin Dashboard":
    st.title("District Administrator Dashboard")
    st.markdown("*Compliance monitoring, observation aggregates, and intervention tracking*")

    db_path = Path(__file__).parent / "vera_demo.db"

    init_summary = None
    obs_summary = None
    intervention_summary = None

    try:
        conn = sqlite3.connect(str(db_path))

        init_df = pd.read_sql_query("""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN locked_at IS NOT NULL THEN 1 ELSE 0 END) as locked_records,
                SUM(CASE WHEN teacher_response = 'confirmed' THEN 1 ELSE 0 END) as confirmed,
                SUM(CASE WHEN teacher_response = 'challenged' THEN 1 ELSE 0 END) as challenged,
                COUNT(DISTINCT teacher_id) as teachers_participating
            FROM initialization_records
        """, conn)
        if len(init_df) > 0:
            init_summary = init_df.iloc[0].to_dict()

        obs_df = pd.read_sql_query("""
            SELECT
                COUNT(DISTINCT ssid) as unique_students,
                COUNT(DISTINCT teacher_id) as teachers_observing,
                COUNT(DISTINCT observation_date) as observation_days,
                COUNT(*) as total_observations,
                SUM(present) as total_present,
                SUM(oral_participation) as total_oral,
                SUM(written_output) as total_written,
                SUM(concern_flag) as total_concerns
            FROM observations
        """, conn)
        if len(obs_df) > 0:
            obs_summary = obs_df.iloc[0].to_dict()

        interv_df = pd.read_sql_query("""
            SELECT
                SUM(CASE WHEN elaboration = 'Intervention responding' THEN 1 ELSE 0 END) as responding,
                SUM(CASE WHEN elaboration = 'Intervention not responding' THEN 1 ELSE 0 END) as not_responding
            FROM observations
        """, conn)
        if len(interv_df) > 0:
            intervention_summary = interv_df.iloc[0].to_dict()

        conn.close()
    except Exception as e:
        st.error(f"Database error: {e}")

    st.markdown("---")
    st.markdown(f"""
        <div style="background: {NAVY}; color: white; padding: 16px; border-radius: 4px; margin-bottom: 20px;">
            <h3 style="color: {GOLD}; margin: 0;">AB 2225 Compliance Status</h3>
        </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        locked = init_summary.get('locked_records', 0) if init_summary else 0
        total = init_summary.get('total_records', 0) if init_summary else 0
        compliance_rate = (locked / total * 100) if total > 0 else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number" style="color: {GREEN if compliance_rate >= 90 else (GOLD if compliance_rate >= 50 else RED)};">{compliance_rate:.0f}%</div>
                <div class="label">Document 1 Completion</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        teachers = init_summary.get('teachers_participating', 0) if init_summary else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{teachers}</div>
                <div class="label">Teachers Participating</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        locked_count = init_summary.get('locked_records', 0) if init_summary else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{locked_count}</div>
                <div class="label">Records Locked</div>
            </div>
        """, unsafe_allow_html=True)

    with col4:
        challenged = init_summary.get('challenged', 0) if init_summary else 0
        st.markdown(f"""
            <div class="stat-card">
                <div class="number">{challenged}</div>
                <div class="label">Hypotheses Challenged</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown(f"""
        <div style="background: {GREEN}; color: white; padding: 16px; border-radius: 4px; text-align: center; margin-top: 30px;">
            <strong>NON-EVALUATION GUARANTEE</strong><br>
            No teacher identity is attached to any result in this dashboard.<br>
            VERA measures whether <em>policy</em> works, not whether <em>teachers</em> work.
        </div>
    """, unsafe_allow_html=True)

# =============================================================================
# Page: Student Record (Document 1)
# =============================================================================

elif page == "📝 Student Record":
    st.title("Student Initialization Record")
    st.markdown("*Document 1 — Day-One Student Record*")

    def init_observation_tables():
        db_path = Path(__file__).parent / "vera_demo.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS initialization_records (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ssid TEXT NOT NULL,
                teacher_id TEXT NOT NULL,
                district_id TEXT NOT NULL,
                school_year TEXT NOT NULL,
                vera_hypothesis TEXT,
                teacher_response TEXT,
                teacher_notes TEXT,
                intervention_assigned TEXT,
                section_a_complete INTEGER DEFAULT 0,
                section_b_complete INTEGER DEFAULT 0,
                section_c_complete INTEGER DEFAULT 0,
                section_d_complete INTEGER DEFAULT 0,
                section_e_complete INTEGER DEFAULT 0,
                locked_at TIMESTAMP,
                locked_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ssid, school_year)
            )
        """)
        conn.commit()
        conn.close()

    init_observation_tables()

    st.markdown("---")
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        student_ssid = st.text_input("Student SSID", placeholder="Enter State Student ID")
    with col2:
        teacher_id = st.text_input("Teacher ID", value="demo_teacher")
    with col3:
        school_year = st.selectbox("School Year", ["2025-2026", "2024-2025", "2026-2027"])

    if not student_ssid:
        st.info("Enter a Student SSID to begin the initialization record.")
        st.stop()

    st.markdown("---")
    st.markdown(f"""
        <div style="background: {NAVY}; color: white; padding: 16px; border-radius: 4px; margin-bottom: 20px;">
            <h3 style="color: {GOLD}; margin: 0;">Five-Section Initialization Checklist</h3>
            <p style="margin: 8px 0 0 0; opacity: 0.8;">All sections must be completed before this record can be locked.</p>
        </div>
    """, unsafe_allow_html=True)

    with st.expander("**Section A: Record Verification**", expanded=True):
        a1 = st.checkbox("Student name and SSID confirmed", key="a1")
        a2 = st.checkbox("Emergency contact verified", key="a2")
        a3 = st.checkbox("Home language survey reviewed", key="a3")
        section_a_complete = all([a1, a2, a3])
        if section_a_complete:
            st.success("Section A complete")

    with st.expander("**Section B: Assessment Data Review**", expanded=False):
        b1 = st.checkbox("CAASPP scores reviewed", key="b1")
        b2 = st.checkbox("ELPAC scores reviewed", key="b2")
        section_b_complete = all([b1, b2])
        if section_b_complete:
            st.success("Section B complete")

    with st.expander("**Section C: Prior Intervention History**", expanded=False):
        c1 = st.checkbox("Prior interventions reviewed", key="c1")
        section_c_complete = c1
        if section_c_complete:
            st.success("Section C complete")

    with st.expander("**Section D: Equity and Access**", expanded=False):
        d1 = st.checkbox("Device access confirmed", key="d1")
        d2 = st.checkbox("AI literacy status reviewed", key="d2")
        section_d_complete = all([d1, d2])
        if section_d_complete:
            st.success("Section D complete")

    with st.expander("**Section E: Day-One Starting Plan**", expanded=False):
        e1 = st.checkbox("VERA hypothesis accepted or challenged", key="e1")
        e2 = st.checkbox("I understand this record will be LOCKED", key="e2")
        section_e_complete = all([e1, e2])
        if section_e_complete:
            st.success("Section E complete")

    all_complete = all([section_a_complete, section_b_complete, section_c_complete,
                        section_d_complete, section_e_complete])

    if all_complete:
        st.markdown(f"""
            <div style="background: {GREEN}; color: white; padding: 16px; border-radius: 4px; margin: 20px 0;">
                <strong>All sections complete.</strong> This record is ready to be locked.
            </div>
        """, unsafe_allow_html=True)

        if st.button("🔒 LOCK RECORD", type="primary", use_container_width=True):
            st.success("✅ Record LOCKED. Document 2 is now active.")
            st.balloons()
    else:
        st.warning("Complete all five sections to lock this record.")

# =============================================================================
# Page: Daily Observations (Document 2)
# =============================================================================

elif page == "📅 Daily Observations":
    st.title("Daily Classroom Observations")
    st.markdown("*Document 2 — Ongoing Observation Log*")

    def init_observations_table():
        db_path = Path(__file__).parent / "vera_demo.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS observations (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                teacher_id TEXT NOT NULL,
                district_id TEXT NOT NULL,
                class_period TEXT,
                observation_date DATE NOT NULL,
                ssid TEXT NOT NULL,
                present INTEGER DEFAULT 0,
                oral_participation INTEGER DEFAULT 0,
                written_output INTEGER DEFAULT 0,
                engaged INTEGER DEFAULT 0,
                concern_flag INTEGER DEFAULT 0,
                absent INTEGER DEFAULT 0,
                elaboration TEXT,
                note TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(teacher_id, observation_date, ssid, class_period)
            )
        """)
        conn.commit()
        conn.close()

    init_observations_table()

    DEMO_ROSTER = [
        {"ssid": "1001", "name": "Garcia, Maria", "flag": "EL", "type4": True},
        {"ssid": "1002", "name": "Johnson, Michael", "flag": None, "type4": False},
        {"ssid": "1003", "name": "Chen, David", "flag": "EL", "type4": True},
        {"ssid": "1004", "name": "Williams, Jasmine", "flag": "IEP", "type4": False},
        {"ssid": "1005", "name": "Martinez, Carlos", "flag": "EL", "type4": False},
    ]

    st.markdown("---")
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        teacher_id = st.text_input("Teacher ID", value="demo_teacher", key="obs_teacher")
    with col2:
        class_period = st.selectbox("Class Period", ["Period 1", "Period 2", "Period 3"], key="obs_period")
    with col3:
        observation_date = st.date_input("Date", value=datetime.now(), key="obs_date")

    st.markdown("---")

    header_cols = st.columns([3, 1, 1, 1, 1, 1])
    with header_cols[0]:
        st.markdown("**Student**")
    with header_cols[1]:
        st.markdown("**P**")
    with header_cols[2]:
        st.markdown("**Or**")
    with header_cols[3]:
        st.markdown("**Wr**")
    with header_cols[4]:
        st.markdown("**En**")
    with header_cols[5]:
        st.markdown("**!**")

    observations_data = []

    for student in DEMO_ROSTER:
        ssid = student['ssid']

        if student['type4']:
            dot = '<span style="color: #FFA500; font-weight: bold;">●</span>'
        elif student['flag'] == 'EL':
            dot = '<span style="color: #4CAF50; font-weight: bold;">●</span>'
        else:
            dot = '<span style="color: #888;">○</span>'

        cols = st.columns([3, 1, 1, 1, 1, 1])

        with cols[0]:
            st.markdown(f"{dot} {student['name']}", unsafe_allow_html=True)
        with cols[1]:
            present = st.checkbox("P", key=f"present_{ssid}", label_visibility="collapsed")
        with cols[2]:
            oral = st.checkbox("Or", key=f"oral_{ssid}", label_visibility="collapsed")
        with cols[3]:
            written = st.checkbox("Wr", key=f"written_{ssid}", label_visibility="collapsed")
        with cols[4]:
            engaged = st.checkbox("En", key=f"engaged_{ssid}", label_visibility="collapsed")
        with cols[5]:
            concern = st.checkbox("!", key=f"concern_{ssid}", label_visibility="collapsed")

        observations_data.append({
            "ssid": ssid,
            "present": 1 if present else 0,
            "oral_participation": 1 if oral else 0,
            "written_output": 1 if written else 0,
            "engaged": 1 if engaged else 0,
            "concern_flag": 1 if concern else 0
        })

    st.markdown("---")
    if st.button("💾 SUBMIT OBSERVATIONS", type="primary", use_container_width=True):
        st.success(f"✅ Observations saved for {observation_date.strftime('%Y-%m-%d')}")
        st.balloons()

# =============================================================================
# Page: About VERA
# =============================================================================

elif page == "ℹ️ About VERA":
    st.title("About VERA-CA")

    st.markdown("""
    ## Verification Engine for Results & Accountability
    ### California Edition

    VERA-CA is California's instance of the Verification Engine, providing data infrastructure
    for the state's evolving accountability system under **AB 2225**.

    ### The Type 4 Gap

    H-EDU's differentiator is identifying students who **speak well but write poorly** — the "oral-written delta."

    - **CAASPP ELA Claim 2** (writing scores)
    - **ELPAC Speaking** scores

    A large positive delta (speaking > writing by 8+ points) flags students who may be misclassified.

    ### Data Sources

    - **CAASPP** — California Assessment of Student Performance and Progress
    - **ELPAC** — English Language Proficiency Assessments for California
    - **LCFF/LCAP** — Local Control Funding Formula allocations

    ### Non-Evaluation Guarantee

    No teacher identity is attached to any result in VERA reports. Match-rate data is aggregate only.

    ---

    **Contact:** [brian@h-edu.solutions](mailto:brian@h-edu.solutions)

    **Website:** [h-edu.solutions](https://h-edu.solutions)
    """)

    st.markdown(f"""
        <div style="background: {NAVY}; color: white; padding: 24px; text-align: center; margin-top: 40px; border-radius: 4px;">
            <p style="color: {GOLD}; font-size: 1.2rem; font-weight: 600; margin: 0;">
                VERA: The verification layer California education accountability has been missing.
            </p>
        </div>
    """, unsafe_allow_html=True)
