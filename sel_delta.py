"""
SEL Δ — Phase 1 Implementation
================================
Add this module to C:\\MCP\\VERA\\ as sel_delta.py
Then add to vera_mcp_server.py:
    from sel_delta import register_sel_delta_tools, init_sel_delta_schema

Call both functions after your existing mcp = FastMCP(...) setup.

Requires no new data agreements — all sources are public.

Phase 1 data sources:
  - LCAP investment: seeded manually from district LCAPs (NLP phase 2)
  - Outcome proxies: CALPADS public files + CA Dashboard
  - CHKS: public dashboard (calschls.org)
  - Type 4 gap trend: computed from existing VERA CAASPP/ELPAC tables

Author: H-EDU.Solutions | Brian Demsey | April 2026
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

# FastMCP is optional - only needed for MCP server, not Streamlit app
try:
    from fastmcp import FastMCP
except ImportError:
    FastMCP = None  # Running in Streamlit mode without MCP

# ── Constants ─────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "vera_demo.db"

# CASEL evidence tier lookup — expand as districts are added
# Source: casel.org/guide-to-schoolwide-sel/
CASEL_SELECT_PROGRAMS = {
    "second step", "ruler", "caring school community",
    "positive action", "open circle", "responsive classroom",
    "4rs", "social decision making", "paths", "strong start",
    "strong kids", "strong teens", "too good for drugs",
    "overcoming obstacles", "character strong", "sanford harmony"
}

CASEL_PROMISING_PROGRAMS = {
    "mindfulness", "pbis", "positive behavioral interventions",
    "restorative practices", "restorative justice", "kimochis",
    "toolbox project", "zones of regulation", "conscious discipline",
    "love and logic", "whole brain teaching", "morning meeting"
}

# Zone thresholds
ZONE_OUTPERFORMING = -10
ZONE_ALIGNED_MAX = 10
ZONE_LAGGING_MAX = 25
# > 25 = Disconnected


# ── Schema Initialization ─────────────────────────────────────────────────────

def init_sel_delta_schema():
    """
    Create SEL Δ tables in vera_demo.db if they don't exist.
    Safe to call on every startup — uses IF NOT EXISTS.
    Seeds demo data for the 10 existing VERA districts.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        -- SEL Investment per district per year
        CREATE TABLE IF NOT EXISTS sel_investment (
            district_id       TEXT NOT NULL,
            year              INTEGER NOT NULL,
            program_name      TEXT,
            casel_tier        TEXT DEFAULT 'none',
            continuity_years  INTEGER DEFAULT 0,
            chks_participant  INTEGER DEFAULT 0,
            priority6_score   REAL DEFAULT 0.0,
            investment_index  REAL DEFAULT 0.0,
            notes             TEXT,
            PRIMARY KEY (district_id, year)
        );

        -- SEL Outcome trajectory per district per year
        CREATE TABLE IF NOT EXISTS sel_outcomes (
            district_id          TEXT NOT NULL,
            year                 INTEGER NOT NULL,
            type4_gap_trend      REAL DEFAULT 0.0,
            rfep_rate            REAL DEFAULT 0.0,
            rfep_trend           REAL DEFAULT 0.0,
            absenteeism_rate     REAL DEFAULT 0.0,
            absenteeism_trend    REAL DEFAULT 0.0,
            suspension_rate      REAL DEFAULT 0.0,
            suspension_trend     REAL DEFAULT 0.0,
            el_progress_score    REAL DEFAULT 0.0,
            outcome_index        REAL DEFAULT 0.0,
            PRIMARY KEY (district_id, year)
        );

        -- Demographic context per district (stable, updated annually)
        CREATE TABLE IF NOT EXISTS district_context (
            district_id    TEXT PRIMARY KEY,
            upp_pct        REAL DEFAULT 0.0,
            el_pct         REAL DEFAULT 0.0,
            enrollment_band TEXT DEFAULT 'medium',
            urbanicity     TEXT DEFAULT 'suburban',
            county         TEXT
        );

        -- Computed SEL Δ per district per year
        CREATE TABLE IF NOT EXISTS sel_delta (
            district_id      TEXT NOT NULL,
            year             INTEGER NOT NULL,
            investment_index REAL,
            outcome_index    REAL,
            expected_outcome REAL,
            sel_delta_value  REAL,
            zone             TEXT,
            trend            TEXT DEFAULT 'stable',
            data_quality     TEXT DEFAULT 'full',
            computed_at      TEXT,
            PRIMARY KEY (district_id, year)
        );

        -- Phase 2: Classroom fidelity observations
        CREATE TABLE IF NOT EXISTS fidelity_observations (
            id                            INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id                   TEXT NOT NULL,
            school_id                     TEXT,
            observer_role                 TEXT DEFAULT 'coordinator',
            obs_date                      TEXT NOT NULL,
            sel_lesson_delivered          INTEGER DEFAULT 0,
            sel_unit                      TEXT,
            minutes_delivered             INTEGER DEFAULT 0,
            oral_expression_opportunities INTEGER DEFAULT 0,
            emotional_vocab_instruction   INTEGER DEFAULT 0,
            oral_written_bridge           INTEGER DEFAULT 0,
            student_participation_pct     REAL DEFAULT 0.0,
            notes                         TEXT,
            recorded_at                   TEXT
        );
    """)

    # ── Seed demo data for 10 VERA districts ─────────────────────────────────
    # Investment data seeded from publicly available LCAP filings
    # Outcome data seeded from CDE public files and Dashboard
    # This represents the 2024-25 school year baseline

    demo_investment = [
        # (district_id, year, program, tier, continuity, chks, p6_score)
        ("36678196000000", 2025, "Second Step", "SELect", 6, 1, 3.2),      # Capistrano
        ("19647330000000", 2025, "Positive Action", "SELect", 8, 1, 2.9),  # LAUSD
        ("33670230000000", 2025, "RULER", "SELect", 5, 1, 3.0),            # Fresno
        ("54722060000000", 2025, "Conscious Discipline", "promising", 4, 1, 2.7),  # Cajon Valley
        ("43697780000000", 2025, "Second Step", "SELect", 7, 1, 3.1),      # San Diego
        ("01612590000000", 2025, "Restorative Practices", "promising", 5, 1, 2.5), # Oakland
        ("30664770000000", 2025, "Zones of Regulation", "promising", 3, 0, 2.4),   # Milpitas
        ("27660760000000", 2025, "Mindfulness Program", "promising", 2, 1, 2.2),   # Monterey
        ("24657990000000", 2025, None, "none", 0, 0, 1.8),                  # Lassen
        ("56725490000000", 2025, "Morning Meeting", "promising", 4, 1, 2.6), # Reed
    ]

    demo_outcomes = [
        # (district_id, year, t4_trend, rfep_rate, rfep_trend,
        #  absent_rate, absent_trend, susp_rate, susp_trend, el_prog)
        # Negative trend = improving (gaps closing, absenteeism falling)
        ("36678196000000", 2025, -3.2, 18.4, 2.1, 14.2, -1.8, 3.1, -0.4, 2.8),  # Capistrano
        ("19647330000000", 2025, -1.5, 12.8, 0.8, 24.6, -2.1, 5.8, -0.9, 2.3),  # LAUSD
        ("33670230000000", 2025, -2.1, 14.2, 1.4, 22.8, -1.2, 4.9, -0.6, 2.5),  # Fresno
        ("54722060000000", 2025, -4.0, 19.6, 2.8, 18.4, -3.1, 2.8, -1.2, 3.0),  # Cajon Valley
        ("43697780000000", 2025, -2.8, 17.1, 1.9, 15.6, -2.4, 3.4, -0.7, 2.7),  # San Diego
        ("01612590000000", 2025,  1.2, 11.4, 0.3, 32.8, -0.4, 8.9, -0.2, 1.9),  # Oakland
        ("30664770000000", 2025,  0.8, 13.6, 0.6, 19.2,  0.6, 4.2,  0.3, 2.1),  # Milpitas
        ("27660760000000", 2025,  2.4, 10.8, 0.2, 21.4,  1.8, 5.6,  0.8, 1.8),  # Monterey
        ("24657990000000", 2025,  4.1,  8.2, -0.4, 28.6,  3.2, 7.2,  1.4, 1.5), # Lassen
        ("56725490000000", 2025, -1.8, 16.4, 1.2, 17.8, -1.4, 3.8, -0.5, 2.4),  # Reed
    ]

    demo_context = [
        # (district_id, upp_pct, el_pct, enrollment_band, urbanicity, county)
        ("36678196000000", 42.0, 18.5, "large",  "suburban", "Orange"),
        ("19647330000000", 78.0, 38.2, "large",  "urban",    "Los Angeles"),
        ("33670230000000", 76.0, 41.8, "large",  "urban",    "Fresno"),
        ("54722060000000", 68.0, 28.4, "large",  "suburban", "San Diego"),
        ("43697780000000", 61.0, 26.9, "large",  "urban",    "San Diego"),
        ("01612590000000", 74.0, 35.1, "large",  "urban",    "Alameda"),
        ("30664770000000", 55.0, 32.6, "medium", "suburban", "Santa Clara"),
        ("27660760000000", 64.0, 38.4, "medium", "suburban", "Monterey"),
        ("24657990000000", 48.0, 12.1, "small",  "rural",    "Lassen"),
        ("56725490000000", 38.0,  9.8, "small",  "rural",    "Sonoma"),
    ]

    # Insert seed data (ignore if already exists)
    for row in demo_investment:
        conn.execute("""
            INSERT OR IGNORE INTO sel_investment
            (district_id, year, program_name, casel_tier, continuity_years,
             chks_participant, priority6_score)
            VALUES (?,?,?,?,?,?,?)
        """, row)

    for row in demo_outcomes:
        conn.execute("""
            INSERT OR IGNORE INTO sel_outcomes
            (district_id, year, type4_gap_trend, rfep_rate, rfep_trend,
             absenteeism_rate, absenteeism_trend, suspension_rate,
             suspension_trend, el_progress_score)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, row)

    for row in demo_context:
        conn.execute("""
            INSERT OR IGNORE INTO district_context
            (district_id, upp_pct, el_pct, enrollment_band, urbanicity, county)
            VALUES (?,?,?,?,?,?)
        """, row)

    conn.commit()
    conn.close()
    print("[SEL Δ] Schema initialized and demo data seeded.")


# ── Scoring Functions ─────────────────────────────────────────────────────────

def _score_investment(program_name: Optional[str], casel_tier: str,
                      continuity_years: int, chks_participant: int,
                      priority6_score: float) -> float:
    """Compute Investment Index 0–100 from component inputs."""

    # Program identification (20 pts)
    program_pts = 20 if program_name else 0

    # CASEL evidence tier (25 pts)
    tier_pts = {"SELect": 25, "promising": 15, "named": 8, "none": 0}.get(
        casel_tier, 0)

    # Continuity (30 pts)
    if continuity_years >= 8:
        continuity_pts = 30
    elif continuity_years >= 5:
        continuity_pts = 22
    elif continuity_years >= 3:
        continuity_pts = 12
    elif continuity_years >= 1:
        continuity_pts = 5
    else:
        continuity_pts = 0

    # CHKS participation (15 pts)
    chks_pts = 15 if chks_participant else 0

    # Priority 6 self-score — scale 1.0–4.0 → 0–10 pts
    p6_pts = min(10, max(0, (priority6_score - 1.0) / 3.0 * 10))

    return round(program_pts + tier_pts + continuity_pts + chks_pts + p6_pts, 1)


def _score_outcome(type4_gap_trend: float, rfep_rate: float, rfep_trend: float,
                   absenteeism_rate: float, absenteeism_trend: float,
                   suspension_rate: float, suspension_trend: float,
                   el_progress_score: float) -> float:
    """
    Compute Outcome Index 0–100 from trajectory data.
    All trends: negative = improving for gap/absenteeism/suspension.
    RFEP and EL progress: positive = improving.
    """

    def trajectory_score(trend: float, max_pts: float,
                         reverse: bool = False) -> float:
        """Score a trend value. reverse=True means positive trend is bad."""
        if reverse:
            trend = -trend
        if trend <= -5.0:  # Improving ≥ 5%
            return max_pts
        elif trend <= -1.0:  # Improving 1-5%
            return max_pts * 0.6
        elif trend <= 1.0:   # Stable ±1%
            return max_pts * 0.4
        elif trend <= 5.0:   # Declining 1-5%
            return max_pts * 0.2
        else:                # Declining ≥ 5%
            return 0.0

    # Type 4 Gap trend (35 pts) — negative trend = gap closing = good
    t4_pts = trajectory_score(type4_gap_trend, 35.0)

    # RFEP reclassification trend (25 pts) — positive trend = good
    rfep_pts = trajectory_score(-rfep_trend, 25.0)  # invert for scoring

    # Chronic absenteeism trend (20 pts) — negative trend = good
    absent_pts = trajectory_score(absenteeism_trend, 20.0)

    # Suspension trend (10 pts) — negative trend = good
    susp_pts = trajectory_score(suspension_trend, 10.0)

    # EL Progress Indicator (10 pts) — scale 1.0–4.0 → 0–10
    el_pts = min(10, max(0, (el_progress_score - 1.0) / 3.0 * 10))

    return round(t4_pts + rfep_pts + absent_pts + susp_pts + el_pts, 1)


def _context_adjustment(upp_pct: float, el_pct: float,
                        enrollment_band: str) -> float:
    """
    Return a multiplier 0.8–1.3 applied to Expected_Outcome.
    High-need districts face harder conditions → lower expected outcome
    at same investment level → multiplier < 1.0 (more lenient).
    Low-need districts face easier conditions → higher expected outcome
    at same investment level → multiplier > 1.0 (more demanding).
    """
    # UPP effect: high UPP → more lenient (lower multiplier)
    if upp_pct >= 70:
        upp_factor = 0.82
    elif upp_pct >= 55:
        upp_factor = 0.90
    elif upp_pct >= 40:
        upp_factor = 1.00
    elif upp_pct >= 25:
        upp_factor = 1.10
    else:
        upp_factor = 1.20

    # EL concentration: high EL → more lenient
    el_factor = 1.0 - (el_pct / 100 * 0.15)

    # Enrollment band: small districts have less capacity
    band_factor = {"large": 1.05, "medium": 1.00, "small": 0.90}.get(
        enrollment_band, 1.00)

    return round(min(1.30, max(0.80, upp_factor * el_factor * band_factor)), 3)


def _expected_outcome(investment_index: float, context_adj: float) -> float:
    """
    Linear model: Expected_Outcome = base + slope * investment_index
    Calibrated on VERA demo districts.
    Phase 2: replace with empirical peer distribution model.
    """
    # Base: minimum expected outcome even with zero investment
    base = 20.0
    # Slope: each investment point should produce ~0.5 outcome points
    slope = 0.55
    raw = base + slope * investment_index
    # Apply context adjustment
    return round(raw * context_adj, 1)


def _zone_label(sel_delta: float) -> str:
    if sel_delta < ZONE_OUTPERFORMING:
        return "outperforming"
    elif sel_delta <= ZONE_ALIGNED_MAX:
        return "aligned"
    elif sel_delta <= ZONE_LAGGING_MAX:
        return "lagging"
    else:
        return "disconnected"


def _compute_and_store(district_id: str, year: int,
                       conn: sqlite3.Connection) -> dict:
    """
    Full SEL Δ computation pipeline for one district-year.
    Returns dict with all computed values.
    """
    # Fetch investment
    inv = conn.execute(
        "SELECT * FROM sel_investment WHERE district_id=? AND year=?",
        (district_id, year)
    ).fetchone()

    # Fetch outcomes
    out = conn.execute(
        "SELECT * FROM sel_outcomes WHERE district_id=? AND year=?",
        (district_id, year)
    ).fetchone()

    # Fetch context
    ctx = conn.execute(
        "SELECT * FROM district_context WHERE district_id=?",
        (district_id,)
    ).fetchone()

    # Determine data quality
    quality = "full"
    if not inv:
        quality = "investment_stale"
    if not out:
        quality = "outcome_stale" if quality == "full" else "both_stale"

    # Score investment
    if inv:
        i_score = _score_investment(
            inv["program_name"], inv["casel_tier"],
            inv["continuity_years"], inv["chks_participant"],
            inv["priority6_score"]
        )
    else:
        i_score = 0.0
        quality = "investment_stale"

    # Score outcome
    if out:
        o_score = _score_outcome(
            out["type4_gap_trend"], out["rfep_rate"], out["rfep_trend"],
            out["absenteeism_rate"], out["absenteeism_trend"],
            out["suspension_rate"], out["suspension_trend"],
            out["el_progress_score"]
        )
    else:
        o_score = 0.0
        quality = "outcome_stale"

    # Context adjustment
    if ctx:
        ctx_adj = _context_adjustment(
            ctx["upp_pct"], ctx["el_pct"], ctx["enrollment_band"]
        )
    else:
        ctx_adj = 1.0

    # Expected outcome and delta
    exp = _expected_outcome(i_score, ctx_adj)
    delta = round(exp - o_score, 1)
    zone = _zone_label(delta)

    # Store computed result
    conn.execute("""
        INSERT OR REPLACE INTO sel_delta
        (district_id, year, investment_index, outcome_index,
         expected_outcome, sel_delta_value, zone, data_quality, computed_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        district_id, year, i_score, o_score, exp,
        delta, zone, quality,
        datetime.now().isoformat()
    ))

    # Update investment_index in sel_investment
    if inv:
        conn.execute(
            "UPDATE sel_investment SET investment_index=? "
            "WHERE district_id=? AND year=?",
            (i_score, district_id, year)
        )

    # Update outcome_index in sel_outcomes
    if out:
        conn.execute(
            "UPDATE sel_outcomes SET outcome_index=? "
            "WHERE district_id=? AND year=?",
            (o_score, district_id, year)
        )

    conn.commit()

    return {
        "district_id": district_id,
        "year": year,
        "investment_index": i_score,
        "outcome_index": o_score,
        "expected_outcome": exp,
        "context_adjustment": ctx_adj,
        "sel_delta": delta,
        "zone": zone,
        "data_quality": quality
    }


# ── Tool Registration ─────────────────────────────────────────────────────────

def register_sel_delta_tools(mcp):
    """
    Register all SEL Δ MCP tools with the FastMCP server instance.
    Call this after init_sel_delta_schema() in vera_mcp_server.py.
    """

    @mcp.tool(
        name="vera:compute_sel_delta",
        description=(
            "Compute the SEL Δ (SEL Delta) for a California school district. "
            "SEL Δ measures the gap between a district's documented SEL "
            "investment and its actual student outcome trajectory. "
            "A positive value means outcomes lag investment. "
            "A negative value means outcomes exceed investment prediction. "
            "Returns: investment_index, outcome_index, expected_outcome, "
            "sel_delta, zone (outperforming/aligned/lagging/disconnected), "
            "and data_quality flags."
        )
    )
    def compute_sel_delta(
        district_id: str,
        year: int = 2025
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            result = _compute_and_store(district_id, year, conn)
            district = conn.execute(
                "SELECT district_name FROM districts WHERE district_id=?",
                (district_id,)
            ).fetchone()
            name = district["district_name"] if district else district_id
            zone_emoji = {
                "outperforming": "🟢",
                "aligned": "🔵",
                "lagging": "🟡",
                "disconnected": "🔴"
            }.get(result["zone"], "⚪")

            return (
                f"SEL Δ Report — {name} ({year})\n"
                f"{'─' * 50}\n"
                f"Investment Index:    {result['investment_index']:>6.1f} / 100\n"
                f"Outcome Index:       {result['outcome_index']:>6.1f} / 100\n"
                f"Context Adjustment:  {result['context_adjustment']:>6.3f}×\n"
                f"Expected Outcome:    {result['expected_outcome']:>6.1f}\n"
                f"{'─' * 50}\n"
                f"SEL Δ:               {result['sel_delta']:>+6.1f}\n"
                f"Zone:                {zone_emoji} {result['zone'].upper()}\n"
                f"Data Quality:        {result['data_quality']}\n"
                f"{'─' * 50}\n"
                f"INTERPRETATION:\n"
                f"{'Investment and outcomes are tracking — on course.' if result['zone'] == 'aligned' else ''}"
                f"{'Outcomes are exceeding investment prediction. Study what this district is doing.' if result['zone'] == 'outperforming' else ''}"
                f"{'Outcomes are lagging investment. Investigate SEL delivery fidelity and curriculum quality.' if result['zone'] == 'lagging' else ''}"
                f"{'Significant gap between investment and outcomes. Data, delivery, or measurement failure likely.' if result['zone'] == 'disconnected' else ''}"
            )
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_investment_index",
        description=(
            "Return the SEL Investment Index score (0–100) for a district, "
            "broken down by component: program identified, CASEL evidence tier, "
            "years of continuity, CHKS participation, and Priority 6 score. "
            "Use this to understand why a district's investment score is high or low."
        )
    )
    def get_investment_index(
        district_id: str,
        year: int = 2025
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            inv = conn.execute(
                "SELECT * FROM sel_investment WHERE district_id=? AND year=?",
                (district_id, year)
            ).fetchone()
            if not inv:
                return (f"No investment data found for district {district_id} "
                        f"in year {year}. Add LCAP data first.")

            i_score = _score_investment(
                inv["program_name"], inv["casel_tier"],
                inv["continuity_years"], inv["chks_participant"],
                inv["priority6_score"]
            )
            # Component breakdown
            prog_pts = 20 if inv["program_name"] else 0
            tier_pts = {"SELect": 25, "promising": 15, "named": 8,
                        "none": 0}.get(inv["casel_tier"], 0)
            cont_yrs = inv["continuity_years"]
            if cont_yrs >= 8: cont_pts = 30
            elif cont_yrs >= 5: cont_pts = 22
            elif cont_yrs >= 3: cont_pts = 12
            elif cont_yrs >= 1: cont_pts = 5
            else: cont_pts = 0
            chks_pts = 15 if inv["chks_participant"] else 0
            p6_pts = round(min(10, max(0,
                (inv["priority6_score"] - 1.0) / 3.0 * 10)), 1)

            return (
                f"Investment Index — District {district_id} ({year})\n"
                f"{'─' * 50}\n"
                f"Program identified:  {inv['program_name'] or 'None':>20}  {prog_pts:>3}/20 pts\n"
                f"CASEL tier:          {inv['casel_tier']:>20}  {tier_pts:>3}/25 pts\n"
                f"Continuity:          {str(cont_yrs) + ' years':>20}  {cont_pts:>3}/30 pts\n"
                f"CHKS participant:    {'Yes' if inv['chks_participant'] else 'No':>20}  {chks_pts:>3}/15 pts\n"
                f"Priority 6 score:    {inv['priority6_score']:>20.1f}  {p6_pts:>3}/10 pts\n"
                f"{'─' * 50}\n"
                f"INVESTMENT INDEX:    {'':>20}  {i_score:>5.1f}/100\n"
            )
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_outcome_index",
        description=(
            "Return the SEL Outcome Index score (0–100) for a district, "
            "broken down by: Type 4 Gap trajectory, RFEP reclassification rate, "
            "chronic absenteeism trend, suspension rate trend, and EL Progress "
            "Indicator. Each component shows direction and score contribution."
        )
    )
    def get_outcome_index(
        district_id: str,
        year: int = 2025
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            out = conn.execute(
                "SELECT * FROM sel_outcomes WHERE district_id=? AND year=?",
                (district_id, year)
            ).fetchone()
            if not out:
                return (f"No outcome data found for district {district_id} "
                        f"in year {year}.")

            o_score = _score_outcome(
                out["type4_gap_trend"], out["rfep_rate"], out["rfep_trend"],
                out["absenteeism_rate"], out["absenteeism_trend"],
                out["suspension_rate"], out["suspension_trend"],
                out["el_progress_score"]
            )

            def arrow(trend, reverse=False):
                v = -trend if reverse else trend
                return "▼ Improving" if v < -1 else "▲ Worsening" if v > 1 else "→ Stable"

            return (
                f"Outcome Index — District {district_id} ({year})\n"
                f"{'─' * 56}\n"
                f"TYPE 4 GAP TREND:    {out['type4_gap_trend']:>+6.1f}  "
                f"{arrow(out['type4_gap_trend'])}   (35 pt max)\n"
                f"RFEP RECLASS RATE:   {out['rfep_rate']:>6.1f}%  "
                f"{arrow(out['rfep_trend'], reverse=True)}   (25 pt max)\n"
                f"ABSENTEEISM TREND:   {out['absenteeism_rate']:>6.1f}%  "
                f"{arrow(out['absenteeism_trend'])}   (20 pt max)\n"
                f"SUSPENSION TREND:    {out['suspension_rate']:>6.1f}%  "
                f"{arrow(out['suspension_trend'])}   (10 pt max)\n"
                f"EL PROGRESS:         {out['el_progress_score']:>6.1f}/4  "
                f"{'Good' if out['el_progress_score'] >= 2.5 else 'Needs attention':>12}   "
                f"(10 pt max)\n"
                f"{'─' * 56}\n"
                f"OUTCOME INDEX:       {o_score:>6.1f} / 100\n"
            )
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_context_profile",
        description=(
            "Return the demographic context profile for a district, including "
            "unduplicated pupil percentage, EL concentration, enrollment band, "
            "and urbanicity. Also shows the context adjustment multiplier used "
            "in SEL Δ computation to ensure fair peer comparison."
        )
    )
    def get_context_profile(district_id: str) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            ctx = conn.execute(
                "SELECT * FROM district_context WHERE district_id=?",
                (district_id,)
            ).fetchone()
            if not ctx:
                return f"No context profile found for district {district_id}."

            adj = _context_adjustment(
                ctx["upp_pct"], ctx["el_pct"], ctx["enrollment_band"])

            interp = (
                "More lenient threshold — high-need population faces greater challenges."
                if adj < 0.95 else
                "More demanding threshold — lower-need population expected to perform higher."
                if adj > 1.05 else
                "Neutral threshold — typical mixed-need district."
            )

            return (
                f"Context Profile — District {district_id}\n"
                f"{'─' * 50}\n"
                f"County:              {ctx['county']}\n"
                f"Unduplicated Pupil%: {ctx['upp_pct']:.1f}%\n"
                f"EL Concentration:    {ctx['el_pct']:.1f}%\n"
                f"Enrollment Band:     {ctx['enrollment_band'].title()}\n"
                f"Urbanicity:          {ctx['urbanicity'].title()}\n"
                f"{'─' * 50}\n"
                f"Context Adjustment:  {adj:.3f}×\n"
                f"Interpretation:      {interp}\n"
            )
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_sel_delta_peers",
        description=(
            "Find comparable peer districts for SEL Δ benchmarking. "
            "Peers are matched by enrollment band and UPP quartile. "
            "Returns each peer's Investment Index, Outcome Index, and SEL Δ "
            "so the queried district can see how it compares to similar districts."
        )
    )
    def get_sel_delta_peers(
        district_id: str,
        year: int = 2025
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            ctx = conn.execute(
                "SELECT * FROM district_context WHERE district_id=?",
                (district_id,)
            ).fetchone()
            if not ctx:
                return f"No context profile for district {district_id}."

            # Find peers: same enrollment band, UPP within ±20 points
            peers = conn.execute("""
                SELECT dc.district_id, d.district_name,
                       dc.upp_pct, dc.enrollment_band,
                       sd.investment_index, sd.outcome_index,
                       sd.sel_delta_value, sd.zone
                FROM district_context dc
                JOIN districts d ON d.district_id = dc.district_id
                LEFT JOIN sel_delta sd
                    ON sd.district_id = dc.district_id AND sd.year = ?
                WHERE dc.district_id != ?
                  AND dc.enrollment_band = ?
                  AND dc.upp_pct BETWEEN ? AND ?
                ORDER BY sd.sel_delta_value ASC
            """, (
                year, district_id,
                ctx["enrollment_band"],
                ctx["upp_pct"] - 20,
                ctx["upp_pct"] + 20
            )).fetchall()

            if not peers:
                return (f"No peer districts found for {district_id}. "
                        "Try expanding UPP range or enrollment band.")

            lines = [
                f"SEL Δ Peer Comparison — District {district_id} ({year})",
                f"Peers: {ctx['enrollment_band'].title()} districts, "
                f"UPP {ctx['upp_pct']-20:.0f}%–{ctx['upp_pct']+20:.0f}%",
                "─" * 72,
                f"{'District':<30} {'UPP%':>5} {'Invest':>6} {'Outcome':>7} "
                f"{'SEL Δ':>7} {'Zone':<14}"
            ]
            for p in peers:
                zone_e = {"outperforming": "🟢", "aligned": "🔵",
                          "lagging": "🟡", "disconnected": "🔴"
                          }.get(p["zone"] or "", "⚪")
                lines.append(
                    f"{(p['district_name'] or p['district_id']):<30} "
                    f"{p['upp_pct']:>5.1f} "
                    f"{p['investment_index'] or 0:>6.1f} "
                    f"{p['outcome_index'] or 0:>7.1f} "
                    f"{p['sel_delta_value'] or 0:>+7.1f} "
                    f"{zone_e} {(p['zone'] or 'no data'):<12}"
                )
            return "\n".join(lines)
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_sel_delta_report",
        description=(
            "Generate a formatted SEL Δ brief suitable for a district board "
            "presentation or COE review. Includes Investment Index breakdown, "
            "Outcome Index breakdown, peer comparison, and a plain-English "
            "interpretation with recommended next steps."
        )
    )
    def get_sel_delta_report(
        district_id: str,
        year: int = 2025
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            # Ensure computed
            result = _compute_and_store(district_id, year, conn)
            district = conn.execute(
                "SELECT district_name FROM districts WHERE district_id=?",
                (district_id,)
            ).fetchone()
            name = district["district_name"] if district else district_id

            inv = conn.execute(
                "SELECT * FROM sel_investment WHERE district_id=? AND year=?",
                (district_id, year)
            ).fetchone()
            ctx = conn.execute(
                "SELECT * FROM district_context WHERE district_id=?",
                (district_id,)
            ).fetchone()

            zone_desc = {
                "outperforming": (
                    "This district's student outcomes are exceeding what its "
                    "SEL investment profile would predict. This is a model "
                    "district — study what they are doing differently."
                ),
                "aligned": (
                    "This district's SEL investment and student outcomes are "
                    "tracking together appropriately. Continue current approach "
                    "and monitor for sustained improvement."
                ),
                "lagging": (
                    "This district's student outcomes are lagging behind what "
                    "its level of SEL investment should be producing. Investigate "
                    "curriculum delivery fidelity, teacher training, and program "
                    "implementation frequency."
                ),
                "disconnected": (
                    "There is a significant gap between this district's SEL "
                    "investment and its student outcomes. This signals a data, "
                    "delivery, or measurement failure that requires immediate "
                    "investigation. Consider a structured fidelity review."
                )
            }.get(result["zone"], "")

            next_steps = {
                "outperforming": [
                    "Document and share implementation practices",
                    "Consider serving as peer mentor for lagging districts",
                    "Investigate which specific components drive outperformance"
                ],
                "aligned": [
                    "Maintain current SEL investment level",
                    "Set 3-year target: move Type 4 Gap by 3+ points",
                    "Add CHKS Social Emotional Health Module if not already used"
                ],
                "lagging": [
                    "Conduct SEL delivery audit: are lessons happening as planned?",
                    "Review teacher training completeness for current curriculum",
                    "Examine if CASEL-tier program is being implemented with fidelity",
                    "Consider enrolling in VERA District Partnership for fidelity tracking"
                ],
                "disconnected": [
                    "Immediate: verify LCAP SEL expenditure documentation accuracy",
                    "Audit whether named program is actually being delivered",
                    "Review CHKS school connectedness scores for baseline",
                    "Engage COE for differentiated assistance review",
                    "Enroll in VERA District Partnership for structured fidelity observation"
                ]
            }.get(result["zone"], [])

            zone_emoji = {"outperforming": "🟢", "aligned": "🔵",
                          "lagging": "🟡", "disconnected": "🔴"
                          }.get(result["zone"], "⚪")

            lines = [
                "=" * 60,
                f"VERA SEL Δ DISTRICT BRIEF",
                f"H-EDU.Solutions | {datetime.now().strftime('%B %d, %Y')}",
                "=" * 60,
                f"District:  {name}",
                f"CDE ID:    {district_id}",
                f"Year:      {year}",
                "",
                "── SEL Δ SUMMARY ─────────────────────────────────────",
                f"Investment Index:   {result['investment_index']:>6.1f} / 100",
                f"Outcome Index:      {result['outcome_index']:>6.1f} / 100",
                f"Expected Outcome:   {result['expected_outcome']:>6.1f}",
                f"SEL Δ:              {result['sel_delta']:>+6.1f}",
                f"Zone:               {zone_emoji} {result['zone'].upper()}",
                "",
                "── SEL INVESTMENT PROFILE ────────────────────────────",
                f"Program:            {(inv['program_name'] if inv else 'Not identified') or 'Not identified'}",
                f"CASEL Tier:         {(inv['casel_tier'].title() if inv else 'None')}",
                f"Continuity:         {(str(inv['continuity_years']) + ' years' if inv else 'Unknown')}",
                f"CHKS Participant:   {'Yes' if inv and inv['chks_participant'] else 'No'}",
                "",
                "── CONTEXT ───────────────────────────────────────────",
                f"County:             {ctx['county'] if ctx else 'Unknown'}",
                f"Unduplicated Pupil: {ctx['upp_pct']:.1f}%" if ctx else "",
                f"EL Concentration:   {ctx['el_pct']:.1f}%" if ctx else "",
                f"Context Adj.:       {result['context_adjustment']:.3f}×",
                "",
                "── INTERPRETATION ────────────────────────────────────",
                zone_desc,
                "",
                "── RECOMMENDED NEXT STEPS ────────────────────────────",
            ]
            for i, step in enumerate(next_steps, 1):
                lines.append(f"{i}. {step}")

            lines += [
                "",
                "── DATA QUALITY ──────────────────────────────────────",
                f"Status: {result['data_quality'].replace('_', ' ').title()}",
                "Data sources: CAASPP, ELPAC, CALPADS, CA Dashboard, CalSCHLS",
                "=" * 60,
                "VERA v2.0 | SEL Δ Module | H-EDU.Solutions",
                "vera-app-lh4t.onrender.com | demsey.com"
            ]

            return "\n".join(lines)
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_all_sel_deltas",
        description=(
            "Compute and return SEL Δ for all districts in the VERA database "
            "for a given year, sorted by SEL Δ value (most disconnected first). "
            "Useful for a statewide or system-level overview of where investment "
            "and outcomes are misaligned across the district portfolio."
        )
    )
    def get_all_sel_deltas(year: int = 2025) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            districts = conn.execute(
                "SELECT district_id, district_name FROM districts"
            ).fetchall()

            results = []
            for d in districts:
                try:
                    r = _compute_and_store(d["district_id"], year, conn)
                    r["district_name"] = d["district_name"]
                    results.append(r)
                except Exception:
                    pass

            results.sort(key=lambda x: x["sel_delta"], reverse=True)

            lines = [
                f"SEL Δ System Overview — All Districts ({year})",
                "─" * 74,
                f"{'District':<30} {'Invest':>6} {'Outcome':>7} "
                f"{'SEL Δ':>7} {'Zone':<14} {'Quality'}"
            ]
            for r in results:
                zone_e = {"outperforming": "🟢", "aligned": "🔵",
                          "lagging": "🟡", "disconnected": "🔴"
                          }.get(r["zone"], "⚪")
                lines.append(
                    f"{r['district_name']:<30} "
                    f"{r['investment_index']:>6.1f} "
                    f"{r['outcome_index']:>7.1f} "
                    f"{r['sel_delta']:>+7.1f} "
                    f"{zone_e} {r['zone']:<12} "
                    f"{r['data_quality']}"
                )
            return "\n".join(lines)
        finally:
            conn.close()


    @mcp.tool(
        name="vera:record_fidelity_observation",
        description=(
            "PHASE 2 — District Partnership Tool. "
            "Record a structured classroom SEL observation. "
            "Observations aggregate to an Implementation Fidelity Score (IFS) "
            "that explains why a district's SEL Δ is wide or narrow. "
            "observer_role: coordinator | coach | admin. "
            "sel_lesson_delivered: 1=yes, 0=no. "
            "oral_expression_opportunities: count of structured oral activities. "
            "emotional_vocab_instruction: 1=yes, 0=no. "
            "oral_written_bridge: 1=yes (explicit oral→written connection made). "
            "student_participation_pct: estimated % of students actively engaged."
        )
    )
    def record_fidelity_observation(
        district_id: str,
        school_id: str,
        obs_date: str,
        sel_lesson_delivered: int,
        observer_role: str = "coordinator",
        sel_unit: str = "",
        minutes_delivered: int = 0,
        oral_expression_opportunities: int = 0,
        emotional_vocab_instruction: int = 0,
        oral_written_bridge: int = 0,
        student_participation_pct: float = 0.0,
        notes: str = ""
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("""
                INSERT INTO fidelity_observations
                (district_id, school_id, observer_role, obs_date,
                 sel_lesson_delivered, sel_unit, minutes_delivered,
                 oral_expression_opportunities, emotional_vocab_instruction,
                 oral_written_bridge, student_participation_pct,
                 notes, recorded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                district_id, school_id, observer_role, obs_date,
                sel_lesson_delivered, sel_unit, minutes_delivered,
                oral_expression_opportunities, emotional_vocab_instruction,
                oral_written_bridge, student_participation_pct,
                notes, datetime.now().isoformat()
            ))
            conn.commit()
            return (
                f"✅ Observation recorded for district {district_id}, "
                f"school {school_id} on {obs_date}.\n"
                f"SEL lesson delivered: {'Yes' if sel_lesson_delivered else 'No'} | "
                f"Minutes: {minutes_delivered} | "
                f"Oral expression opportunities: {oral_expression_opportunities}\n"
                f"Use vera:get_fidelity_score to see aggregate implementation score."
            )
        finally:
            conn.close()


    @mcp.tool(
        name="vera:get_fidelity_score",
        description=(
            "PHASE 2 — District Partnership Tool. "
            "Aggregate classroom observations into an Implementation Fidelity "
            "Score (IFS) for a district. "
            "Returns: lesson delivery rate, average minutes per lesson, "
            "oral expression frequency, vocab instruction rate, "
            "oral-written bridge rate, student engagement, and overall IFS (0–100). "
            "Specify date_from/date_to to filter by time window (YYYY-MM-DD)."
        )
    )
    def get_fidelity_score(
        district_id: str,
        date_from: str = "2025-08-01",
        date_to: str = "2026-06-30"
    ) -> str:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            obs = conn.execute("""
                SELECT * FROM fidelity_observations
                WHERE district_id = ?
                  AND obs_date BETWEEN ? AND ?
            """, (district_id, date_from, date_to)).fetchall()

            if not obs:
                return (f"No observations found for district {district_id} "
                        f"between {date_from} and {date_to}.")

            n = len(obs)
            lessons = sum(o["sel_lesson_delivered"] for o in obs)
            avg_min = sum(o["minutes_delivered"] for o in obs) / n
            avg_oral = sum(o["oral_expression_opportunities"] for o in obs) / n
            vocab_rate = sum(o["emotional_vocab_instruction"] for o in obs) / n
            bridge_rate = sum(o["oral_written_bridge"] for o in obs) / n
            avg_particip = sum(o["student_participation_pct"] for o in obs) / n
            delivery_rate = lessons / n

            # IFS computation (0–100)
            ifs = round(
                delivery_rate * 35 +          # Lesson delivery (35 pts)
                min(avg_min / 45, 1) * 20 +   # Time on task, max at 45 min (20 pts)
                min(avg_oral / 3, 1) * 20 +   # Oral expression, max at 3/lesson (20 pts)
                vocab_rate * 15 +              # Vocab instruction (15 pts)
                bridge_rate * 10,              # Oral-written bridge (10 pts)
                1
            )

            ifs_zone = (
                "Strong" if ifs >= 75 else
                "Adequate" if ifs >= 50 else
                "Weak" if ifs >= 25 else
                "Critical"
            )

            return (
                f"Implementation Fidelity Score — District {district_id}\n"
                f"Period: {date_from} to {date_to} | Observations: {n}\n"
                f"{'─' * 52}\n"
                f"Lesson delivery rate:    {delivery_rate:>6.1%}  (35 pt max)\n"
                f"Avg minutes/lesson:      {avg_min:>6.1f}       (20 pt max)\n"
                f"Oral expression/lesson:  {avg_oral:>6.1f}       (20 pt max)\n"
                f"Vocab instruction rate:  {vocab_rate:>6.1%}  (15 pt max)\n"
                f"Oral-written bridge:     {bridge_rate:>6.1%}  (10 pt max)\n"
                f"Avg student engagement:  {avg_particip:>6.1f}%\n"
                f"{'─' * 52}\n"
                f"FIDELITY SCORE (IFS):    {ifs:>6.1f} / 100  [{ifs_zone}]\n"
                f"\n"
                f"NOTE: Districts with IFS ≥ 75 and SEL Δ > +15 are candidates\n"
                f"for curriculum review. High fidelity + lagging outcomes suggests\n"
                f"a program effectiveness issue, not a delivery issue."
            )
        finally:
            conn.close()

    print("[SEL Δ] All tools registered successfully.")
