"""
Populate VERA-CA database with all 937 California school districts.
Generates sample CAASPP and ELPAC data for Type 4 analysis.
"""

import sqlite3
import pandas as pd
import hashlib
from pathlib import Path

DB_PATH = Path(__file__).parent / "vera_demo.db"
CSV_PATH = Path(__file__).parent / "ca_districts.csv"


def _hash_seed(name):
    """Generate consistent seed from district name."""
    return int(hashlib.md5(name.encode()).hexdigest()[:8], 16)


def populate_districts():
    """Load all California districts from CDE CSV into database."""
    # Read the CSV
    df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')

    print(f"Loaded {len(df)} districts from CSV")

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Recreate districts table
    cursor.execute("DROP TABLE IF EXISTS districts")
    cursor.execute("""
        CREATE TABLE districts (
            district_id TEXT PRIMARY KEY,
            district_name TEXT NOT NULL,
            county TEXT NOT NULL,
            district_type TEXT,
            enrollment INTEGER,
            el_count INTEGER,
            el_pct REAL
        )
    """)

    # Insert all districts
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO districts (district_id, district_name, county, district_type, enrollment, el_count, el_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            row['CDSCode'],
            row['DistrictName'],
            row['CountyName'],
            row['DistrictType'],
            int(row['EnrollTotal']) if pd.notna(row['EnrollTotal']) else 0,
            int(row['ELcount']) if pd.notna(row['ELcount']) else 0,
            float(row['ELpct']) if pd.notna(row['ELpct']) else 0.0
        ))

    conn.commit()
    print(f"Inserted {len(df)} districts into database")
    return df


def populate_caaspp_data(districts_df):
    """Generate CAASPP sample data for all districts."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Recreate CAASPP table
    cursor.execute("DROP TABLE IF EXISTS caaspp_results")
    cursor.execute("""
        CREATE TABLE caaspp_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id TEXT NOT NULL,
            district_name TEXT NOT NULL,
            county TEXT NOT NULL,
            grade INTEGER NOT NULL,
            subgroup TEXT NOT NULL,
            ela_overall_score REAL,
            ela_claim1_score REAL,
            ela_claim2_score REAL,
            ela_claim3_score REAL,
            ela_claim4_score REAL,
            math_overall_score REAL,
            year INTEGER DEFAULT 2024
        )
    """)

    subgroups = ['All Students', 'English Learners', 'Socioeconomically Disadvantaged',
                 'Students with Disabilities', 'Hispanic or Latino', 'White',
                 'Asian', 'African American']
    grades = [3, 4, 5, 6, 7, 8, 11]

    records = []
    for _, row in districts_df.iterrows():
        district_id = row['CDSCode']
        district_name = row['DistrictName']
        county = row['CountyName']
        enrollment = int(row['EnrollTotal']) if pd.notna(row['EnrollTotal']) else 500
        el_pct = float(row['ELpct']) if pd.notna(row['ELpct']) else 5.0

        seed = _hash_seed(district_name)

        for grade in grades:
            for subgroup in subgroups:
                # Generate realistic scores based on demographics
                # California average is around 2500 (met standard)
                base_score = 2480 + (seed % 80)

                # Adjustments based on subgroup
                if subgroup == 'English Learners':
                    base_score -= 60
                elif subgroup == 'Socioeconomically Disadvantaged':
                    base_score -= 40
                elif subgroup == 'Students with Disabilities':
                    base_score -= 80
                elif subgroup == 'Asian':
                    base_score += 40

                # Claim 2 (writing) - key for Type 4 detection
                # EL students often have lower writing scores
                claim2_adj = 0
                if subgroup == 'English Learners' or el_pct > 20:
                    claim2_adj = -15 - (seed % 20)  # Lower writing

                ela_overall = base_score + (seed % 30)
                ela_claim1 = ela_overall + (seed % 10) - 5  # Reading
                ela_claim2 = ela_overall + claim2_adj + (seed % 8)  # Writing
                ela_claim3 = ela_overall + (seed % 12) - 6  # Listening
                ela_claim4 = ela_overall + (seed % 10) - 5  # Research
                math_overall = base_score - 10 + (seed % 40)

                records.append((
                    district_id, district_name, county, grade, subgroup,
                    ela_overall, ela_claim1, ela_claim2, ela_claim3, ela_claim4,
                    math_overall, 2024
                ))

    cursor.executemany("""
        INSERT INTO caaspp_results
        (district_id, district_name, county, grade, subgroup,
         ela_overall_score, ela_claim1_score, ela_claim2_score, ela_claim3_score, ela_claim4_score,
         math_overall_score, year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)

    conn.commit()
    print(f"Inserted {len(records)} CAASPP records")


def populate_elpac_data(districts_df):
    """Generate ELPAC sample data for all districts."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Recreate ELPAC table
    cursor.execute("DROP TABLE IF EXISTS elpac_results")
    cursor.execute("""
        CREATE TABLE elpac_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id TEXT NOT NULL,
            district_name TEXT NOT NULL,
            county TEXT NOT NULL,
            grade INTEGER NOT NULL,
            subgroup TEXT NOT NULL,
            overall_score REAL,
            listening_score REAL,
            speaking_score REAL,
            reading_score REAL,
            writing_score REAL,
            year INTEGER DEFAULT 2024
        )
    """)

    subgroups = ['All Students', 'English Learners', 'Socioeconomically Disadvantaged',
                 'Hispanic or Latino']
    grades = [3, 4, 5, 6, 7, 8, 11]

    records = []
    for _, row in districts_df.iterrows():
        district_id = row['CDSCode']
        district_name = row['DistrictName']
        county = row['CountyName']
        el_pct = float(row['ELpct']) if pd.notna(row['ELpct']) else 5.0

        seed = _hash_seed(district_name)

        for grade in grades:
            for subgroup in subgroups:
                # ELPAC scores: 1-4 scale (1=Beginning, 4=Well Developed)
                # Most ELs score 2-3
                base_score = 2.4 + (grade * 0.05) + (seed % 60) / 100

                # Speaking scores typically higher than writing - KEY FOR TYPE 4
                speaking_adj = 0.3 + (seed % 40) / 100
                writing_adj = -0.2 - (seed % 30) / 100

                # Higher EL% districts tend to have larger gaps
                if el_pct > 30:
                    speaking_adj += 0.15
                    writing_adj -= 0.1

                overall = min(4.0, base_score)
                listening = min(4.0, base_score + 0.1)
                speaking = min(4.0, base_score + speaking_adj)
                reading = min(4.0, base_score - 0.05)
                writing = min(4.0, base_score + writing_adj)

                # Convert to CAASPP-comparable scale for delta calculation
                # ELPAC scale 1-4 maps roughly to CAASPP 2300-2600
                speaking_scaled = 2350 + (speaking * 75)

                records.append((
                    district_id, district_name, county, grade, subgroup,
                    overall, listening, speaking_scaled, reading, writing, 2024
                ))

    cursor.executemany("""
        INSERT INTO elpac_results
        (district_id, district_name, county, grade, subgroup,
         overall_score, listening_score, speaking_score, reading_score, writing_score, year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)

    conn.commit()
    print(f"Inserted {len(records)} ELPAC records")


def populate_sel_data(districts_df):
    """Generate SEL investment and outcome data for all districts."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # Recreate SEL tables
    cursor.execute("DROP TABLE IF EXISTS sel_investment")
    cursor.execute("DROP TABLE IF EXISTS sel_outcomes")
    cursor.execute("DROP TABLE IF EXISTS district_context")
    cursor.execute("DROP TABLE IF EXISTS sel_delta")

    cursor.execute("""
        CREATE TABLE sel_investment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            program_name TEXT,
            casel_tier TEXT DEFAULT 'emerging',
            continuity_years INTEGER DEFAULT 1,
            chks_participant INTEGER DEFAULT 0,
            priority6_score REAL DEFAULT 2.0,
            UNIQUE(district_id, year)
        )
    """)

    cursor.execute("""
        CREATE TABLE sel_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            type4_gap_trend REAL DEFAULT 0,
            rfep_rate REAL DEFAULT 10,
            absenteeism_rate REAL DEFAULT 10,
            suspension_rate REAL DEFAULT 3,
            el_progress_score REAL DEFAULT 2.5,
            UNIQUE(district_id, year)
        )
    """)

    cursor.execute("""
        CREATE TABLE district_context (
            district_id TEXT PRIMARY KEY,
            enrollment INTEGER,
            el_pct REAL,
            sed_pct REAL,
            district_type TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE sel_delta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id TEXT NOT NULL,
            year INTEGER NOT NULL,
            investment_index REAL,
            outcome_index REAL,
            sel_delta REAL,
            zone TEXT,
            UNIQUE(district_id, year)
        )
    """)

    sel_programs = ['Second Step', 'RULER', 'CASEL Framework', 'Positive Action',
                    'MindUP', 'Responsive Classroom', 'PBIS', None]
    casel_tiers = ['emerging', 'established', 'exemplary']

    for _, row in districts_df.iterrows():
        district_id = row['CDSCode']
        enrollment = int(row['EnrollTotal']) if pd.notna(row['EnrollTotal']) else 500
        el_pct = float(row['ELpct']) if pd.notna(row['ELpct']) else 5.0
        sed_pct = float(row['SEDpct']) if pd.notna(row.get('SEDpct', 50)) else 50.0

        seed = _hash_seed(row['DistrictName'])

        # SEL Investment
        program = sel_programs[seed % len(sel_programs)]
        tier = casel_tiers[seed % len(casel_tiers)]
        continuity = 1 + (seed % 5)
        chks = 1 if seed % 3 == 0 else 0
        priority6 = 1.5 + (seed % 25) / 10

        cursor.execute("""
            INSERT INTO sel_investment (district_id, year, program_name, casel_tier, continuity_years, chks_participant, priority6_score)
            VALUES (?, 2025, ?, ?, ?, ?, ?)
        """, (district_id, program, tier, continuity, chks, priority6))

        # SEL Outcomes
        type4_trend = -5 + (seed % 15)
        rfep = 8 + (seed % 20)
        absent = 5 + (seed % 15)
        suspend = 1 + (seed % 8)
        el_progress = 2.0 + (seed % 20) / 10

        cursor.execute("""
            INSERT INTO sel_outcomes (district_id, year, type4_gap_trend, rfep_rate, absenteeism_rate, suspension_rate, el_progress_score)
            VALUES (?, 2025, ?, ?, ?, ?, ?)
        """, (district_id, type4_trend, rfep, absent, suspend, el_progress))

        # District context
        cursor.execute("""
            INSERT OR REPLACE INTO district_context (district_id, enrollment, el_pct, sed_pct, district_type)
            VALUES (?, ?, ?, ?, ?)
        """, (district_id, enrollment, el_pct, sed_pct, row['DistrictType']))

    conn.commit()
    print(f"Inserted SEL data for {len(districts_df)} districts")


def main():
    print("Populating VERA-CA database with all California districts...")

    # Load districts from CSV
    districts_df = populate_districts()

    # Generate assessment data
    populate_caaspp_data(districts_df)
    populate_elpac_data(districts_df)
    populate_sel_data(districts_df)

    print("\nDatabase population complete!")
    print(f"Total districts: {len(districts_df)}")

    # Verify
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM districts")
    print(f"Districts in DB: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM caaspp_results")
    print(f"CAASPP records: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM elpac_results")
    print(f"ELPAC records: {cursor.fetchone()[0]}")

    conn.close()


if __name__ == "__main__":
    main()
