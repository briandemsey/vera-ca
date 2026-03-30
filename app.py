"""
VERA-CA - Verification Engine for Results & Accountability
Streamlit Web Application for California Education Data

Post-AB 2225 infrastructure for California's accountability transformation.
Connecting inputs, outputs, and outcomes at the student level.

Data sourced from NCES EDGE (nces.ed.gov) ArcGIS services.
California context: AB 2225 (2024), CAASPP, LCFF/LCAP compliance.
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import urllib.parse

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(
    page_title="VERA-CA | California Education Accountability",
    page_icon="🐻",
    layout="wide",
    initial_sidebar_state="expanded"
)

# California Colors (State flag inspired)
BLUE = "#002855"
DARK_BLUE = "#001a3a"
GOLD = "#FDB515"
RED = "#CC0000"
WHITE = "#FFFFFF"
CREAM = "#F8F8F5"
NAVY = "#1B2A4A"

# Custom CSS
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Public+Sans:wght@400;600;700&display=swap');

    .stApp {{
        background-color: {CREAM};
    }}

    section[data-testid="stSidebar"] {{
        background-color: {BLUE};
    }}
    section[data-testid="stSidebar"] .stMarkdown {{
        color: white;
    }}
    section[data-testid="stSidebar"] label {{
        color: white !important;
    }}
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stRadio label span,
    section[data-testid="stSidebar"] .stRadio label p,
    section[data-testid="stSidebar"] .stRadio label div {{
        color: white !important;
    }}

    h1, h2, h3 {{
        font-family: 'Public Sans', sans-serif;
        color: {BLUE};
    }}
    h1 {{
        border-bottom: 4px solid {GOLD};
        padding-bottom: 16px;
    }}

    .stat-card {{
        background: white;
        padding: 20px;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-left: 4px solid {BLUE};
        min-width: 0;
    }}
    .stat-card .value {{
        font-size: 1.8rem;
        font-weight: 700;
        color: {BLUE};
        white-space: nowrap;
    }}
    .stat-card .label {{
        font-size: 0.85rem;
        color: #666;
    }}

    .ab2225-badge {{
        background: {GOLD};
        color: {NAVY};
        padding: 6px 16px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-block;
        margin-bottom: 16px;
    }}

    .lcap-card {{
        background: white;
        padding: 24px;
        border-radius: 8px;
        border-top: 4px solid {BLUE};
        margin-bottom: 16px;
    }}
    .lcap-card h4 {{
        color: {BLUE};
        font-size: 1.1rem;
        margin-bottom: 12px;
    }}

    .priority-grid {{
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 12px;
        margin: 20px 0;
    }}
    .priority-item {{
        background: {BLUE};
        color: white;
        padding: 16px 8px;
        text-align: center;
        border-radius: 6px;
        font-size: 0.85rem;
        font-weight: 600;
    }}

    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Data Functions - NCES EDGE ArcGIS API (California Schools)
# =============================================================================

# NCES EDGE Schools endpoint
NCES_SCHOOLS_ENDPOINT = "https://nces.ed.gov/opengis/rest/services/K12_School_Locations/EDGE_GEOCODE_PUBLICSCH_2324/MapServer/0/query"


@st.cache_data(ttl=3600)
def fetch_california_schools():
    """Fetch all California schools from NCES EDGE endpoint."""
    all_schools = []
    offset = 0
    batch_size = 2000

    # California FIPS code is 06
    where_clause = "STFIP='06'"

    while True:
        try:
            params = {
                "where": where_clause,
                "outFields": "OBJECTID,NCESSCH,NAME,LEAID,STREET,CITY,STATE,ZIP,NMCNTY,LOCALE,LAT,LON",
                "f": "json",
                "resultRecordCount": batch_size,
                "resultOffset": offset
            }

            url = f"{NCES_SCHOOLS_ENDPOINT}?{urllib.parse.urlencode(params)}"
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                break

            for feature in features:
                attrs = feature.get("attributes", {})
                all_schools.append(attrs)

            if len(features) < batch_size:
                break
            offset += batch_size

        except Exception as e:
            st.error(f"Error fetching school data: {e}")
            break

    return all_schools


def process_schools_data(raw_data):
    """Process raw API data into a clean DataFrame."""
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # Rename columns for clarity
    df = df.rename(columns={
        "NAME": "school_name",
        "NCESSCH": "school_id",
        "LEAID": "district_id",
        "NMCNTY": "county",
        "CITY": "city",
        "ZIP": "zip",
        "LOCALE": "locale_code",
        "LAT": "latitude",
        "LON": "longitude"
    })

    # Filter out schools with no name
    df = df[df["school_name"].notna()]

    # Decode locale codes
    locale_map = {
        "11": "City-Large",
        "12": "City-Midsize",
        "13": "City-Small",
        "21": "Suburb-Large",
        "22": "Suburb-Midsize",
        "23": "Suburb-Small",
        "31": "Town-Fringe",
        "32": "Town-Distant",
        "33": "Town-Remote",
        "41": "Rural-Fringe",
        "42": "Rural-Distant",
        "43": "Rural-Remote"
    }
    df["locale"] = df["locale_code"].astype(str).map(locale_map).fillna("Unknown")

    # Simplify locale to category
    def locale_category(code):
        code = str(code)
        if code.startswith("1"):
            return "City"
        elif code.startswith("2"):
            return "Suburb"
        elif code.startswith("3"):
            return "Town"
        elif code.startswith("4"):
            return "Rural"
        return "Unknown"

    df["locale_category"] = df["locale_code"].apply(locale_category)

    return df


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

    # Display California flag
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

    page = st.radio(
        "Navigate",
        ["📊 School Dashboard", "📈 Locale Analysis", "🗺️ County Explorer", "🎯 LCAP Framework", "ℹ️ About VERA-CA"],
        label_visibility="collapsed"
    )

    st.markdown(f"""
        <div style="
            height: 4px;
            background: linear-gradient(90deg, {GOLD}, #FFD700, {GOLD});
            margin: 30px 0 20px 0;
            border-radius: 2px;
        "></div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <p style="color: {GOLD}; font-size: 1.4rem; font-weight: 700; text-align: center; margin: 12px 0 6px 0;">
            AB 2225
        </p>
        <p style="color: white; font-size: 0.85rem; text-align: center; margin: 0 0 4px 0;">
            Accountability Transformation
        </p>
        <p style="color: rgba(255,255,255,0.6); font-size: 0.75rem; text-align: center; margin: 0 0 20px 0;">
            Implementation 2025-26
        </p>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div style="
            background: rgba(255,255,255,0.1);
            border-radius: 8px;
            padding: 16px;
            margin-top: 10px;
        ">
            <p style="color: white; font-size: 0.8rem; line-height: 1.5; margin: 0;">
                <strong style="color: {GOLD};">VERA-CA</strong> provides verification infrastructure
                for California's accountability system under AB 2225.
            </p>
        </div>
    """, unsafe_allow_html=True)


# =============================================================================
# Page: School Dashboard
# =============================================================================

if page == "📊 School Dashboard":
    st.title("California School Dashboard")

    st.markdown(f'<span class="ab2225-badge">AB 2225 • Accountability Transformation</span>', unsafe_allow_html=True)

    # Load data
    with st.spinner("Loading California school data from NCES..."):
        raw_data = fetch_california_schools()
        df = process_schools_data(raw_data)

    if df.empty:
        st.error("Unable to load school data. Please try again later.")
        st.stop()

    # Key metrics
    total_schools = len(df)
    total_counties = df["county"].nunique()
    total_districts = df["district_id"].nunique()
    total_cities = df["city"].nunique()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{total_schools:,}</div>
                <div class="label">Public Schools</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{total_counties}</div>
                <div class="label">Counties</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{total_districts:,}</div>
                <div class="label">School Districts</div>
            </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{total_cities:,}</div>
                <div class="label">Cities Served</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Schools by Locale
    st.subheader("Schools by Locale Type")

    locale_counts = df["locale_category"].value_counts().reset_index()
    locale_counts.columns = ["Locale", "Count"]

    fig = px.bar(
        locale_counts,
        x="Locale",
        y="Count",
        color="Locale",
        color_discrete_map={
            "City": BLUE,
            "Suburb": GOLD,
            "Town": "#4A90A4",
            "Rural": "#2E7D32",
            "Unknown": "#999"
        }
    )
    fig.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        xaxis_title="",
        yaxis_title="Number of Schools"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Top 10 Counties
    st.subheader("Top 10 Counties by School Count")

    county_counts = df["county"].value_counts().head(10).reset_index()
    county_counts.columns = ["County", "Schools"]

    fig2 = px.bar(
        county_counts,
        x="Schools",
        y="County",
        orientation="h",
        color_discrete_sequence=[BLUE]
    )
    fig2.update_layout(
        plot_bgcolor="white",
        yaxis=dict(categoryorder="total ascending"),
        xaxis_title="Number of Schools",
        yaxis_title=""
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Data table
    st.subheader("School Directory")

    # County filter
    counties = ["All Counties"] + sorted(df["county"].dropna().unique().tolist())
    selected_county = st.selectbox("Filter by County", counties)

    display_df = df if selected_county == "All Counties" else df[df["county"] == selected_county]

    st.dataframe(
        display_df[["school_name", "city", "county", "locale", "zip"]].head(100),
        use_container_width=True,
        hide_index=True
    )

    st.caption(f"Showing {min(100, len(display_df))} of {len(display_df):,} schools")


# =============================================================================
# Page: Locale Analysis
# =============================================================================

elif page == "📈 Locale Analysis":
    st.title("Locale Analysis")

    st.markdown(f'<span class="ab2225-badge">Geographic Distribution</span>', unsafe_allow_html=True)

    with st.spinner("Loading data..."):
        raw_data = fetch_california_schools()
        df = process_schools_data(raw_data)

    if df.empty:
        st.error("Unable to load data.")
        st.stop()

    # Detailed locale breakdown
    st.subheader("Schools by Detailed Locale")

    locale_detail = df["locale"].value_counts().reset_index()
    locale_detail.columns = ["Locale Type", "Count"]

    fig = px.pie(
        locale_detail,
        values="Count",
        names="Locale Type",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)

    # Urban vs Rural comparison
    st.subheader("Urban vs Rural Distribution")

    urban_rural = df["locale_category"].value_counts().reset_index()
    urban_rural.columns = ["Category", "Schools"]

    col1, col2 = st.columns(2)

    with col1:
        fig2 = px.pie(
            urban_rural,
            values="Schools",
            names="Category",
            color="Category",
            color_discrete_map={
                "City": BLUE,
                "Suburb": GOLD,
                "Town": "#4A90A4",
                "Rural": "#2E7D32"
            }
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.markdown("### Distribution Summary")
        for _, row in urban_rural.iterrows():
            pct = (row["Schools"] / len(df)) * 100
            st.markdown(f"**{row['Category']}**: {row['Schools']:,} schools ({pct:.1f}%)")


# =============================================================================
# Page: County Explorer
# =============================================================================

elif page == "🗺️ County Explorer":
    st.title("County Explorer")

    st.markdown(f'<span class="ab2225-badge">58 Counties</span>', unsafe_allow_html=True)

    with st.spinner("Loading data..."):
        raw_data = fetch_california_schools()
        df = process_schools_data(raw_data)

    if df.empty:
        st.error("Unable to load data.")
        st.stop()

    # County selector
    counties = sorted(df["county"].dropna().unique().tolist())
    selected_county = st.selectbox("Select County", counties, index=counties.index("Los Angeles") if "Los Angeles" in counties else 0)

    county_df = df[df["county"] == selected_county]

    # County stats
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{len(county_df):,}</div>
                <div class="label">Schools in {selected_county}</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        districts = county_df["district_id"].nunique()
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{districts}</div>
                <div class="label">School Districts</div>
            </div>
        """, unsafe_allow_html=True)

    with col3:
        cities = county_df["city"].nunique()
        st.markdown(f"""
            <div class="stat-card">
                <div class="value">{cities}</div>
                <div class="label">Cities</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Schools by locale in county
    st.subheader(f"Locale Distribution in {selected_county} County")

    locale_counts = county_df["locale_category"].value_counts().reset_index()
    locale_counts.columns = ["Locale", "Count"]

    fig = px.bar(
        locale_counts,
        x="Locale",
        y="Count",
        color="Locale",
        color_discrete_map={
            "City": BLUE,
            "Suburb": GOLD,
            "Town": "#4A90A4",
            "Rural": "#2E7D32"
        }
    )
    fig.update_layout(showlegend=False, plot_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)

    # Map of schools in county
    if not county_df["latitude"].isna().all():
        st.subheader(f"Schools in {selected_county} County")
        map_df = county_df.dropna(subset=["latitude", "longitude"])
        if not map_df.empty:
            fig_map = px.scatter_mapbox(
                map_df,
                lat="latitude",
                lon="longitude",
                hover_name="school_name",
                hover_data=["city", "locale"],
                zoom=8,
                mapbox_style="open-street-map"
            )
            fig_map.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=400)
            st.plotly_chart(fig_map, use_container_width=True)

    # School list
    st.subheader("Schools")
    st.dataframe(
        county_df[["school_name", "city", "locale", "zip"]].sort_values("school_name"),
        use_container_width=True,
        hide_index=True
    )


# =============================================================================
# Page: LCAP Framework
# =============================================================================

elif page == "🎯 LCAP Framework":
    st.title("LCAP Framework")

    st.markdown(f'<span class="ab2225-badge">Local Control Accountability Plan</span>', unsafe_allow_html=True)

    st.markdown("""
        The **Local Control Accountability Plan (LCAP)** is a critical component of California's
        Local Control Funding Formula (LCFF). VERA-CA provides verification infrastructure to
        ensure funds reach the students who generated them.
    """)

    st.markdown("---")

    # LCFF Priorities
    st.subheader("LCFF State Priorities")

    st.markdown("""
        <div class="priority-grid">
            <div class="priority-item">1. Basic Services</div>
            <div class="priority-item">2. Implementation of Standards</div>
            <div class="priority-item">3. Parent Engagement</div>
            <div class="priority-item">4. Student Achievement</div>
            <div class="priority-item">5. Student Engagement</div>
            <div class="priority-item">6. School Climate</div>
            <div class="priority-item">7. Course Access</div>
            <div class="priority-item">8. Other Student Outcomes</div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # AB 2225 Context
    st.subheader("AB 2225: Accountability Transformation")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
            <div class="lcap-card">
                <h4>What AB 2225 Changes</h4>
                <ul>
                    <li>Shifts from punitive to supportive accountability</li>
                    <li>Emphasizes continuous improvement cycles</li>
                    <li>Strengthens local control with state oversight</li>
                    <li>Requires evidence of intervention effectiveness</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
            <div class="lcap-card">
                <h4>VERA's Role</h4>
                <ul>
                    <li>Verify LCAP fund allocation matches need</li>
                    <li>Track intervention outcomes at student level</li>
                    <li>Connect inputs (funding) to outputs (services)</li>
                    <li>Surface gaps between policy and practice</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)

    # High-need student groups
    st.subheader("Unduplicated Pupil Count Groups")

    st.markdown("""
        LCFF provides supplemental and concentration grants based on the unduplicated count of:
    """)

    groups = [
        ("English Learners", "Students designated as English Learners (EL) based on ELPAC assessment"),
        ("Low-Income Students", "Students eligible for free or reduced-price meals (FRPM)"),
        ("Foster Youth", "Students in foster care placement"),
        ("Homeless Youth", "Students experiencing homelessness under McKinney-Vento")
    ]

    for group, desc in groups:
        st.markdown(f"""
            <div class="lcap-card">
                <h4>{group}</h4>
                <p>{desc}</p>
            </div>
        """, unsafe_allow_html=True)


# =============================================================================
# Page: About VERA-CA
# =============================================================================

elif page == "ℹ️ About VERA-CA":
    st.title("About VERA-CA")

    st.markdown(f'<span class="ab2225-badge">Verification Engine for Results & Accountability</span>', unsafe_allow_html=True)

    st.markdown("""
        **VERA-CA** is California's instance of the Verification Engine for Results & Accountability,
        providing data infrastructure for the state's evolving accountability system under AB 2225.
    """)

    st.markdown("---")

    st.subheader("What VERA Does")

    st.markdown("""
        - **Aggregates** school data from authoritative sources (NCES, CDE)
        - **Visualizes** geographic and demographic distributions
        - **Verifies** alignment between funding and student need
        - **Connects** to the California School Dashboard ecosystem
    """)

    st.markdown("---")

    st.subheader("Data Sources")

    st.markdown("""
        | Source | Description |
        |--------|-------------|
        | NCES EDGE | National Center for Education Statistics school locations |
        | CDE DataQuest | California Department of Education assessment data |
        | LCFF/LCAP Reports | Local Control Funding Formula allocations |
    """)

    st.markdown("---")

    st.subheader("California Context")

    st.markdown("""
        California educates over **6 million students** across **58 counties** and **1,000+ school districts**.
        The state's Local Control Funding Formula (LCFF) directs additional resources to high-need students,
        but verification that these funds reach their intended recipients has been limited.

        **AB 2225** (2024) transforms California's accountability approach, shifting from punitive measures
        to continuous improvement. VERA-CA provides the verification infrastructure this transformation requires.
    """)

    st.markdown("---")

    st.markdown(f"""
        <div style="text-align: center; padding: 20px;">
            <p style="color: {BLUE}; font-size: 1.1rem; font-weight: 600;">
                Part of the H-EDU Global Education Initiative
            </p>
            <p>
                <a href="https://h-edu.solutions" target="_blank" style="color: {GOLD};">h-edu.solutions</a>
            </p>
        </div>
    """, unsafe_allow_html=True)
