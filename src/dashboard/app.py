import json
from pathlib import Path
import subprocess
import sys

import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="OTT Media Engagement",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# VERIFIED PROJECT METRICS
# =========================================================

TOTAL_USERS = 1000
TOTAL_CONTENT = 300

SOURCE_EVENTS = 52500
UNIQUE_EVENTS = 50000
DUPLICATES_REMOVED = 2500

WATCH_SECONDS = 9893021
BUFFER_SECONDS = 53373
BUFFER_RATIO = 0.5346

ML_MAE = 233.67
ML_R2 = 0.0086
BASELINE_MAE = 235.92
RF_R2 = -0.0280

RAG_VECTORS = 300
RAG_DIMENSION = 384
RAG_PRECISION = 0.77

PYTEST_TOTAL = 5

DEDUP_RATE = UNIQUE_EVENTS / SOURCE_EVENTS * 100
BUFFER_HEADROOM = 0.80 - BUFFER_RATIO


# =========================================================
# PREMIUM CSS
# =========================================================

st.markdown(
    """
    <style>

    /* ---------- GLOBAL ---------- */

    .stApp {
        background:
            radial-gradient(circle at 5% 0%,
                rgba(99,102,241,.13), transparent 25%),
            radial-gradient(circle at 95% 5%,
                rgba(20,184,166,.12), transparent 25%),
            linear-gradient(135deg,#eef2ff 0%,#f8fafc 48%,#ecfeff 100%);
    }

    .block-container {
        max-width: 1450px;
        padding-top: 1.2rem;
        padding-bottom: 1.5rem;
    }

    /* ---------- SIDEBAR ---------- */

    section[data-testid="stSidebar"] {
        background:
            linear-gradient(180deg,#0f172a 0%,#111827 55%,#172554 100%);
        border-right: 1px solid #1e293b;
    }

    section[data-testid="stSidebar"] * {
        color: #e2e8f0;
    }

    section[data-testid="stSidebar"] .stRadio label {
        font-size: 14px;
        font-weight: 600;
    }

    /* ---------- HERO ---------- */

    .hero {
        position: relative;
        overflow: hidden;
        background:
            radial-gradient(circle at 85% 20%,
                rgba(45,212,191,.28), transparent 28%),
            linear-gradient(120deg,#312e81,#4f46e5 48%,#0f766e);
        border-radius: 26px;
        padding: 34px 38px;
        color: white;
        margin-bottom: 25px;
        box-shadow:
            0 20px 45px rgba(49,46,129,.20);
    }

    .hero:after {
        content: "";
        position: absolute;
        width: 220px;
        height: 220px;
        border-radius: 50%;
        right: -70px;
        top: -90px;
        background: rgba(255,255,255,.08);
    }

    .hero-kicker {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 2px;
        opacity: .78;
        font-weight: 700;
        margin-bottom: 8px;
    }

    .hero-title {
        font-size: 34px;
        font-weight: 850;
        letter-spacing: -.8px;
        margin-bottom: 8px;
    }

    .hero-text {
        font-size: 15px;
        opacity: .88;
        max-width: 850px;
    }

    /* ---------- SECTION ---------- */

    .section-title {
        font-size: 23px;
        font-weight: 850;
        color: #172033;
        margin: 22px 0 4px 0;
        letter-spacing: -.3px;
    }

    .section-subtitle {
        color: #64748b;
        font-size: 13px;
        margin-bottom: 14px;
    }

    /* ---------- KPI ---------- */

    .kpi {
        border-radius: 19px;
        padding: 19px 20px;
        min-height: 128px;
        border: 1px solid rgba(148,163,184,.22);
        box-shadow: 0 9px 24px rgba(15,23,42,.065);
        transition: transform .2s ease;
    }

    .kpi:hover {
        transform: translateY(-2px);
    }

    .kpi-indigo {
        background: linear-gradient(145deg,#f5f3ff,#e9e7ff);
    }

    .kpi-teal {
        background: linear-gradient(145deg,#f0fdfa,#d4f8f0);
    }

    .kpi-amber {
        background: linear-gradient(145deg,#fffbeb,#fff1c7);
    }

    .kpi-rose {
        background: linear-gradient(145deg,#fff1f2,#ffe2e6);
    }

    .kpi-slate {
        background: linear-gradient(145deg,#f8fafc,#e2e8f0);
    }

    .kpi-label {
        color: #64748b;
        font-size: 12px;
        font-weight: 750;
        margin-bottom: 7px;
    }

    .kpi-value {
        color: #172033;
        font-size: 27px;
        font-weight: 850;
    }

    .kpi-note {
        color: #64748b;
        font-size: 11px;
        margin-top: 5px;
    }

    /* ---------- ARCHITECTURE CARDS ---------- */

    .arch-card {
        background: rgba(255,255,255,.72);
        border: 1px solid rgba(148,163,184,.24);
        border-radius: 17px;
        padding: 17px 15px;
        text-align: center;
        min-height: 88px;
        box-shadow: 0 7px 18px rgba(15,23,42,.055);
    }

    .arch-name {
        font-weight: 850;
        font-size: 14px;
        color: #1e293b;
        margin-bottom: 5px;
    }

    .arch-role {
        color: #64748b;
        font-size: 11px;
    }

    .arch-arrow {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 88px;
        color: #6366f1;
        font-size: 26px;
        font-weight: 800;
    }

    .layer-label {
        font-size: 10px;
        text-transform: uppercase;
        letter-spacing: 1.3px;
        font-weight: 800;
        color: #94a3b8;
        margin: 7px 0 5px 2px;
    }

    /* ---------- HEALTH CARDS ---------- */

    .health {
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 8px 22px rgba(15,23,42,.055);
    }

    .health-green {
        background: linear-gradient(145deg,#ecfdf5,#d1fae5);
        border: 1px solid #a7f3d0;
    }

    .health-indigo {
        background: linear-gradient(145deg,#eef2ff,#e0e7ff);
        border: 1px solid #c7d2fe;
    }

    .health-amber {
        background: linear-gradient(145deg,#fffbeb,#fef3c7);
        border: 1px solid #fde68a;
    }

    .health-title {
        color: #475569;
        font-size: 12px;
        font-weight: 750;
        margin-bottom: 6px;
    }

    .health-value {
        color: #172033;
        font-size: 25px;
        font-weight: 850;
    }

    .health-text {
        color: #64748b;
        font-size: 11px;
        margin-top: 4px;
    }

    /* ---------- INFO CARD ---------- */

    .info-card {
        background: rgba(255,255,255,.74);
        border: 1px solid rgba(148,163,184,.22);
        border-radius: 18px;
        padding: 19px;
        box-shadow: 0 8px 22px rgba(15,23,42,.055);
    }

    .info-title {
        color: #1e293b;
        font-size: 15px;
        font-weight: 850;
        margin-bottom: 7px;
    }

    .info-text {
        color: #64748b;
        font-size: 12px;
        line-height: 1.65;
    }

    /* ---------- STATUS ---------- */

    .status-pill {
        display: inline-block;
        padding: 5px 11px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 850;
        background: linear-gradient(135deg,#eef2ff,#e0e7ff);
        color: #4338ca;
        border: 1px solid #c7d2fe;
    }

    .status-card {
        background: rgba(255,255,255,.76);
        border: 1px solid rgba(148,163,184,.22);
        border-radius: 16px;
        padding: 14px 16px;
        margin-bottom: 9px;
        box-shadow: 0 5px 15px rgba(15,23,42,.04);
    }

    /* ---------- FOOTER ---------- */

    .footer {
        text-align: center;
        color: #64748b;
        font-size: 12px;
        font-weight: 650;
        padding: 28px 0 6px 0;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def kpi(label, value, note, style):
    st.markdown(
        f"""
        <div class="kpi {style}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def arch_card(name, role):
    st.markdown(
        f"""
        <div class="arch-card">
            <div class="arch-name">{name}</div>
            <div class="arch-role">{role}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def arrow():
    st.markdown(
        '<div class="arch-arrow">→</div>',
        unsafe_allow_html=True,
    )


def health_card(title, value, text, style):
    st.markdown(
        f"""
        <div class="health {style}">
            <div class="health-title">{title}</div>
            <div class="health-value">{value}</div>
            <div class="health-text">{text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def plotly_base(fig, height=300):
    fig.update_layout(
        height=height,
        margin=dict(l=15, r=15, t=45, b=15),
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,.48)",
        font=dict(
            family="Arial",
            color="#334155",
        ),
    )
    return fig


# =========================================================
# SIDEBAR
# =========================================================

with st.sidebar:

    st.markdown(
        """
        <div style="
            text-align:center;
            padding:8px 0 20px 0;
        ">
            <div style="font-size:48px;">🎬</div>
            <div style="
                font-size:21px;
                font-weight:850;
                color:#f8fafc;
            ">
                OTT Analytics
            </div>
            <div style="
                font-size:11px;
                color:#94a3b8;
                margin-top:4px;
            ">
                Data Engineering Platform
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    page = st.radio(
        "NAVIGATION",
        [
            "Overview",
            "Streaming & QoE",
            "Data Quality",
            "Machine Learning",
            "RAG & Semantic Search",
            "Technology Stack",
        ],
    )

    st.divider()

    st.markdown(
        """
        <div style="
            font-size:11px;
            letter-spacing:1px;
            font-weight:800;
            color:#64748b;
            margin-bottom:10px;
        ">
            PROJECT SCALE
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption("👥  1,000 Users")
    st.caption("🎬  300 Content Items")
    st.caption("📡  52,500 Source Events")
    st.caption("🔄  50,000 Unique Events")

    st.divider()

    st.markdown(
        """
        <div style="
            font-size:11px;
            letter-spacing:1px;
            font-weight:800;
            color:#64748b;
            margin-bottom:10px;
        ">
            VERIFIED
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.caption("QoE SLA       PASS")
    st.caption("Schema Drift  PASS")
    st.caption("Tests         5 / 5")
    st.caption("RAG P@5       77%")


# =========================================================
# HERO
# =========================================================

st.markdown(
    """
    <div class="hero">
        <div class="hero-kicker">Data Engineering Capstone</div>
        <div class="hero-title">🎬 OTT Media Engagement</div>
        <div class="hero-text">
            A complete streaming analytics platform combining event ingestion,
            distributed processing, data quality, business intelligence,
            machine learning and semantic retrieval.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# OVERVIEW
# =========================================================

if page == "Overview":

    st.markdown(
        '<div class="section-title">Executive Overview</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Verified project scale, processing results and platform health'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi(
            "👥 TOTAL USERS",
            "1,000",
            "Synthetic OTT population",
            "kpi-indigo",
        )

    with c2:
        kpi(
            "🎬 CONTENT",
            "300",
            "Movies and media items",
            "kpi-teal",
        )

    with c3:
        kpi(
            "📡 SOURCE EVENTS",
            "52,500",
            "Kafka replay input",
            "kpi-amber",
        )

    with c4:
        kpi(
            "🔄 UNIQUE EVENTS",
            "50,000",
            "After event_id deduplication",
            "kpi-rose",
        )

    st.write("")

    c5, c6, c7, c8 = st.columns(4)

    with c5:
        kpi(
            "♻️ DUPLICATES REMOVED",
            "2,500",
            f"{DEDUP_RATE:.2f}% retained",
            "kpi-slate",
        )

    with c6:
        kpi(
            "⏱ WATCH TIME",
            "9.89M",
            "Total watch seconds",
            "kpi-teal",
        )

    with c7:
        kpi(
            "📶 BUFFERING",
            "0.5346%",
            "SLA target < 0.80%",
            "kpi-indigo",
        )

    with c8:
        kpi(
            "🔎 RAG PRECISION",
            "77%",
            "Precision@5",
            "kpi-rose",
        )

    # -----------------------------------------------------
    # ARCHITECTURE
    # -----------------------------------------------------

    st.markdown(
        '<div class="section-title">Platform Architecture</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'From source systems to analytics and AI'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="layer-label">SOURCE → INGESTION → STORAGE</div>',
        unsafe_allow_html=True,
    )

    cols = st.columns([1.15,.35,1.15,.35,1.15,.35,1.15])

    with cols[0]:
        arch_card("MySQL", "Source database")
    with cols[1]:
        arrow()
    with cols[2]:
        arch_card("Sqoop", "Batch ingestion")
    with cols[3]:
        arrow()
    with cols[4]:
        arch_card("HDFS", "Distributed storage")
    with cols[5]:
        arrow()
    with cols[6]:
        arch_card("Kafka", "Event streaming")

    st.write("")

    st.markdown(
        '<div class="layer-label">STREAMING → LAKEHOUSE → SERVING</div>',
        unsafe_allow_html=True,
    )

    cols = st.columns([1.15,.35,1.15,.35,1.15,.35,1.15])

    with cols[0]:
        arch_card("Spark", "Structured Streaming")
    with cols[1]:
        arrow()
    with cols[2]:
        arch_card("Bronze", "Raw events")
    with cols[3]:
        arrow()
    with cols[4]:
        arch_card("Silver", "Cleaned + deduped")
    with cols[5]:
        arrow()
    with cols[6]:
        arch_card("Gold", "Analytics-ready")

    st.write("")

    cols = st.columns([1.15,.35,1.15,.35,1.15])

    with cols[0]:
        arch_card("Hive", "Analytical warehouse")
    with cols[1]:
        arrow()
    with cols[2]:
        arch_card("HBase", "Serving layer")
    with cols[3]:
        arrow()
    with cols[4]:
        arch_card("Power BI + Streamlit", "Analytics")

    # -----------------------------------------------------
    # HEALTH
    # -----------------------------------------------------

    st.markdown(
        '<div class="section-title">Platform Health</div>',
        unsafe_allow_html=True,
    )

    h1, h2, h3, h4 = st.columns(4)

    with h1:
        health_card(
            "DATA RETENTION",
            f"{DEDUP_RATE:.2f}%",
            "Unique events retained",
            "health-indigo",
        )

    with h2:
        health_card(
            "QOE SLA",
            "PASS",
            f"{BUFFER_RATIO:.4f}% buffering",
            "health-green",
        )

    with h3:
        health_card(
            "AUTOMATED TESTS",
            "5 / 5",
            "All implemented tests passed",
            "health-green",
        )

    with h4:
        health_card(
            "RAG EVALUATION",
            "77%",
            "Precision@5",
            "health-green",
        )

    # -----------------------------------------------------
    # PROCESSING CHARTS
    # -----------------------------------------------------

    st.markdown(
        '<div class="section-title">Processing Snapshot</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.15, .85])

    with left:

        event_data = {
            "Stage": ["Source", "Unique", "Removed"],
            "Events": [
                SOURCE_EVENTS,
                UNIQUE_EVENTS,
                DUPLICATES_REMOVED,
            ],
        }

        fig = px.bar(
            event_data,
            x="Stage",
            y="Events",
            text="Events",
        )

        fig.update_traces(
            marker_color=["#6366f1","#14b8a6","#f59e0b"],
            marker_line_width=0,
            textposition="outside",
        )

        fig.update_yaxes(
            showgrid=True,
            gridcolor="#e2e8f0",
            title=None,
        )

        fig.update_xaxes(title=None)

        # Same chart and data; only reduce its visual height.
        fig.update_layout(
            height=285,
            margin=dict(l=15, r=25, t=35, b=35),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with right:

        with st.container(border=True):
            st.markdown("### Architecture Highlights")

            st.markdown("**Streaming**")
            st.caption("Kafka → Spark Structured Streaming")

            st.markdown("**Storage**")
            st.caption("HDFS + Delta Bronze / Silver / Gold")

            st.markdown("**Serving**")
            st.caption("Hive star schema + HBase")

            st.markdown("**Analytics**")
            st.caption("Power BI + Streamlit")

        st.write("")

        st.markdown(
            f"""
            <div class="health health-green">
                <div class="health-title">PIPELINE RESULT</div>
                <div class="health-value">50,000 unique events</div>
                <div class="health-text">
                    2,500 replay duplicates removed using event_id.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# STREAMING & QOE
# =========================================================

elif page == "Streaming & QoE":

    st.markdown(
        '<div class="section-title">Streaming & Quality of Experience</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Kafka event processing, deduplication and buffering quality'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)

    c1.metric("Kafka Source Events", f"{SOURCE_EVENTS:,}")
    c2.metric("Unique Events", f"{UNIQUE_EVENTS:,}")
    c3.metric("Duplicates Removed", f"{DUPLICATES_REMOVED:,}")

    st.write("")

    h1, h2, h3 = st.columns(3)

    with h1:
        health_card(
            "BUFFERING SLA",
            "PASS",
            "Target < 0.80%",
            "health-green",
        )

    with h2:
        health_card(
            "BUFFERING RATIO",
            f"{BUFFER_RATIO:.4f}%",
            f"{BUFFER_HEADROOM:.4f}% below SLA limit",
            "health-indigo",
        )

    with h3:
        health_card(
            "WATCH TIME",
            "9.89M sec",
            "Total observed watch time",
            "health-teal",
        )

    st.markdown(
        '<div class="section-title">Quality Distribution</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns([1, 1])

    with left:

        qoe = go.Figure(
            go.Pie(
                labels=["Watch Time", "Buffer Time"],
                values=[WATCH_SECONDS, BUFFER_SECONDS],
                hole=.72,
                textinfo="percent",
                marker=dict(
                    colors=["#4f46e5","#f59e0b"],
                    line=dict(color="#ffffff", width=4),
                ),
            )
        )

        qoe.update_layout(
            height=340,
            margin=dict(l=10,r=10,t=30,b=10),
            paper_bgcolor="rgba(255,255,255,0)",
            showlegend=True,
            legend=dict(
                orientation="h",
                y=-.05,
            ),
            annotations=[
                dict(
                    text="99.46%<br><span style='font-size:12px'>watch</span>",
                    x=.5,
                    y=.5,
                    showarrow=False,
                    font=dict(
                        size=24,
                        color="#172033",
                    ),
                )
            ],
        )

        st.plotly_chart(
            qoe,
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with right:

        st.markdown(
            """
            <div class="info-card">
                <div class="info-title">Streaming Configuration</div>
                <div class="info-text">
                    <b>Kafka Topic:</b> watch_events_full<br>
                    <b>Partitions:</b> 3<br>
                    <b>Replication Factor:</b> 1<br>
                    <b>Watermark:</b> 10 minutes<br>
                    <b>Window:</b> 15 minutes<br>
                    <b>Deduplication:</b> event_id<br>
                    <b>Checkpoint:</b> /ott_media/checkpoints/watch_events
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.write("")

        st.markdown(
            """
            <div class="info-card">
                <div class="info-title">Processing Logic</div>
                <div class="info-text">
                    Events are ingested from Kafka, processed with Spark
                    Structured Streaming, watermarked for late data and
                    deduplicated using the event_id key before downstream
                    analytics.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# DATA QUALITY
# =========================================================

elif page == "Data Quality":

    st.markdown(
        '<div class="section-title">Data Quality & Validation</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Schema validation, drift detection, reconciliation and automated tests'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Schema Drift", "PASS")
    c2.metric("Automated Tests", "5 / 5")
    c3.metric("Duplicate Removal", "PASS")
    c4.metric("Buffer Validation", "PASS")

    st.markdown(
        '<div class="section-title">Validation Results</div>',
        unsafe_allow_html=True,
    )

    validation = [
        ("Schema validation", "PASS", "Expected event schema"),
        ("Schema drift detection", "PASS", "Missing/unexpected fields detected"),
        ("Duplicate-event removal", "PASS", "2,500 duplicates removed"),
        ("Bronze buffering", "PASS", "0.5366%"),
        ("Silver buffering", "PASS", "0.5346%"),
        ("Gold buffering", "PASS", "0.5346%"),
        ("Automated pytest", "5/5 PASS", "All tests passed"),
    ]

    for name, result, detail in validation:

        c1, c2, c3 = st.columns([2.6, 1, 4])

        with c1:
            st.markdown(
                f"""
                <div class="status-card">
                    <b>{name}</b>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with c2:
            st.markdown(
                f"""
                <div class="status-card" style="text-align:center;">
                    <span class="status-pill">{result}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with c3:
            st.markdown(
                f"""
                <div class="status-card">
                    <span style="color:#64748b;font-size:12px;">
                        {detail}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown(
        '<div class="section-title">Automated Test Evidence</div>',
        unsafe_allow_html=True,
    )

    st.code(
        "pytest -q tests/test_ott_project.py\n"
        "5 passed in 0.02s",
        language="text",
    )

    st.markdown(
        """
        <div class="info-card">
            <div class="info-title">Schema Drift Detection</div>
            <div class="info-text">
                The project validates the expected watch-event schema and
                identifies missing fields and unexpected columns. A failure
                case was also verified independently.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# MACHINE LEARNING
# =========================================================

elif page == "Machine Learning":

    st.markdown(
        '<div class="section-title">Machine Learning</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Verified model experiments on the current synthetic dataset'
        '</div>',
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)

    c1.metric("Linear Regression MAE", "233.67 sec")
    c2.metric("Linear Regression R²", "0.0086")
    c3.metric("Baseline MAE", "235.92 sec")

    st.markdown(
        '<div class="section-title">Model Comparison</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns(2)

    with left:

        fig = go.Figure(
            go.Bar(
                x=[BASELINE_MAE, ML_MAE],
                y=["Baseline", "Linear Regression"],
                orientation="h",
                text=["235.92", "233.67"],
                textposition="outside",
                marker_color=["#94a3b8","#6366f1"],
                marker_line_width=0,
            )
        )

        fig.update_layout(
            title="MAE — Lower is Better",
            height=285,
            margin=dict(l=20,r=55,t=50,b=20),
            paper_bgcolor="rgba(255,255,255,0)",
            plot_bgcolor="rgba(255,255,255,.48)",
            xaxis=dict(
                range=[225,240],
                title="Seconds",
                gridcolor="#e2e8f0",
            ),
            yaxis=dict(title=None),
            showlegend=False,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with right:

        fig = go.Figure(
            go.Bar(
                x=["Baseline","Linear Regression","Random Forest"],
                y=[0,ML_R2,RF_R2],
                text=["0.0000","0.0086","-0.0280"],
                textposition="outside",
                marker_color=["#94a3b8","#6366f1","#f59e0b"],
                marker_line_width=0,
            )
        )

        fig.update_layout(
            title="R² — Verified Results",
            height=285,
            margin=dict(l=20,r=20,t=50,b=20),
            paper_bgcolor="rgba(255,255,255,0)",
            plot_bgcolor="rgba(255,255,255,.48)",
            yaxis=dict(
                range=[-.04,.025],
                title="R²",
                gridcolor="#e2e8f0",
            ),
            xaxis=dict(title=None),
            showlegend=False,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False},
        )

    st.markdown(
        """
        <div class="health health-amber">
            <div class="health-title">MODEL INTERPRETATION</div>
            <div class="health-value">Strongest verified result</div>
            <div class="health-text">
                Linear Regression reduced MAE from 235.92 seconds to
                233.67 seconds. R² remains low at 0.0086, so the current
                result is reported as a dataset limitation rather than
                overstating predictive performance.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# RAG
# =========================================================

elif page == "RAG & Semantic Search":

    st.markdown(
        '<div class="section-title">RAG & Semantic Search</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Vector retrieval, evaluation and prompt-engineering layer'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-title">Search the content catalog</div>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Describe what you want to watch, such as “action movies” or "
        "“something relaxing for the evening.”"
    )

    query_column, count_column = st.columns([4, 1])
    with query_column:
        search_query = st.text_input(
            "What would you like to watch?",
            placeholder="Try: action movies",
            key="rag_search_query_input",
        )
    with count_column:
        result_count = st.selectbox(
            "Number of results",
            options=[3, 5, 10],
            index=1,
            key="rag_search_result_count",
        )

    if st.button(
        "Search titles",
        type="primary",
        disabled=not search_query.strip(),
        key="rag_search_button",
    ):
        st.session_state.pop("rag_search_results", None)
        st.session_state.pop("rag_last_search_query", None)
        with st.spinner("Searching the catalog..."):
            project_root = Path(__file__).resolve().parents[2]
            search_script = project_root / "src" / "rag" / "search_titles.py"
            search_process = subprocess.run(
                [
                    sys.executable,
                    str(search_script),
                    "--query",
                    search_query.strip(),
                    "--top-k",
                    str(result_count),
                    "--json",
                ],
                capture_output=True,
                text=True,
                cwd=project_root,
            )
        if search_process.returncode != 0:
            st.error(
                "Search failed. "
                f"{search_process.stderr.strip() or search_process.stdout.strip()}"
            )
        else:
            st.session_state["rag_search_results"] = json.loads(
                search_process.stdout
            )
            st.session_state["rag_last_search_query"] = search_query.strip()

    if st.session_state.get("rag_search_results"):
        st.markdown(
            f"**Results for:** {st.session_state['rag_last_search_query']}"
        )
        for result in st.session_state["rag_search_results"]:
            with st.container(border=True):
                st.subheader(result["title"])
                st.caption(
                    f"Content ID: {result['content_id']} · "
                    f"Similarity: {result['score']:.4f}"
                )
                st.write(result["document"])

    c1, c2, c3 = st.columns(3)

    c1.metric("FAISS Vectors", "300")
    c2.metric("Embedding Dimension", "384")
    c3.metric("Precision@5", "77%")

    st.markdown(
        '<div class="section-title">Semantic Retrieval Pipeline</div>',
        unsafe_allow_html=True,
    )

    cols = st.columns([1.15,.30,1.15,.30,1.15,.30,1.15,.30,1.4])

    steps = [
        ("User Query","Natural language"),
        ("Embedding","MiniLM"),
        ("FAISS","Vector search"),
        ("Top-K","Relevant titles"),
        ("Response","Structured output"),
    ]

    for i, (name, role) in enumerate(steps):

        with cols[i * 2]:
            arch_card(name, role)

        if i < 4:
            with cols[i * 2 + 1]:
                arrow()

    st.markdown(
        '<div class="section-title">Retrieval Health</div>',
        unsafe_allow_html=True,
    )

    h1, h2, h3 = st.columns(3)

    with h1:
        health_card(
            "INDEX SIZE",
            "300",
            "Indexed content documents",
            "health-indigo",
        )

    with h2:
        health_card(
            "VECTOR SIZE",
            "384D",
            "all-MiniLM-L6-v2 embeddings",
            "health-indigo",
        )

    with h3:
        health_card(
            "PRECISION@5",
            "77%",
            "Target ≥ 60%",
            "health-green",
        )

    st.markdown(
        '<div class="section-title">Prompt Engineering</div>',
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            """
            <div class="info-card">
                <div class="info-title">Prompt Components</div>
                <div class="info-text">
                    • System instructions<br>
                    • Few-shot examples<br>
                    • Retrieved context<br>
                    • Structured JSON-style response
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            """
            <div class="info-card">
                <div class="info-title">Verified Retrieval</div>
                <div class="info-text">
                    FAISS IndexFlatIP was used for semantic similarity search.
                    Precision@5 reached 77%, exceeding the implemented
                    evaluation target of 60%.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# =========================================================
# TECHNOLOGY STACK
# =========================================================

elif page == "Technology Stack":

    st.markdown(
        '<div class="section-title">Technology Stack</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-subtitle">'
        'Technologies used across the implemented platform'
        '</div>',
        unsafe_allow_html=True,
    )

    groups = [
        (
            "INGESTION",
            [
                ("MySQL","Source database"),
                ("Sqoop","HDFS ingestion"),
                ("Apache Kafka","Event streaming"),
            ],
        ),
        (
            "PROCESSING & STORAGE",
            [
                ("Apache Spark","Batch + streaming"),
                ("HDFS","Distributed storage"),
                ("Delta Lake","Bronze / Silver / Gold"),
            ],
        ),
        (
            "SERVING & ORCHESTRATION",
            [
                ("Hive","Analytical warehouse"),
                ("HBase","Serving layer"),
                ("Apache Airflow","Workflow orchestration"),
            ],
        ),
        (
            "ANALYTICS & AI",
            [
                ("Power BI","Business intelligence"),
                ("Streamlit","Interactive dashboard"),
                ("Scikit-learn","Machine learning"),
            ],
        ),
        (
            "SEMANTIC SEARCH",
            [
                ("FAISS","Vector similarity"),
                ("Sentence Transformers","Embeddings"),
                ("Prompt Engineering","Structured responses"),
            ],
        ),
    ]

    for group_name, items in groups:

        st.markdown(
            f'<div class="layer-label">{group_name}</div>',
            unsafe_allow_html=True,
        )

        cols = st.columns(3)

        for col, (name, role) in zip(cols, items):

            with col:
                arch_card(name, role)

        st.write("")

    st.markdown(
        '<div class="section-title">Final Validation Summary</div>',
        unsafe_allow_html=True,
    )

    summary = {
        "Component": [
            "Kafka + Spark Streaming",
            "Bronze / Silver / Gold",
            "QoE Buffering SLA",
            "Schema Drift",
            "Automated Testing",
            "Hive / HBase",
            "Airflow",
            "Power BI",
            "Machine Learning",
            "RAG",
            "Prompt Engineering",
        ],
        "Status": [
            "Validated",
            "Validated",
            "PASS — 0.5346%",
            "PASS",
            "5/5 PASS",
            "Validated",
            "Validated",
            "Completed",
            "Verified",
            "PASS — 77%",
            "Implemented",
        ],
    }

    st.table(summary)


# =========================================================
# FOOTER
# =========================================================

st.markdown(
    '<div class="footer">OTT Media Engagement</div>',
    unsafe_allow_html=True,
)
