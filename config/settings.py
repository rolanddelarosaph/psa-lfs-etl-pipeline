# =============================================================================
# config/settings.py
# MASTER CONFIGURATION — All file paths, schema rules, and quarter settings
# live here. Nothing is hardcoded anywhere else in the pipeline.
# =============================================================================

import os

# -----------------------------------------------------------------------------
# BASE PATHS — Change only these two lines if you move your data
# -----------------------------------------------------------------------------
RAW_DATA_DIR  = "/Users/rolanddelarosa/Desktop/LFS/raw"
OUTPUT_DIR    = "/Users/rolanddelarosa/Desktop/LFS/output"
LOG_DIR       = "/Users/rolanddelarosa/Desktop/LFS/logs"

# -----------------------------------------------------------------------------
# QUARTERS TO EXCLUDE
# These files are missing from the PSA dataset entirely.
# -----------------------------------------------------------------------------
EXCLUDED_QUARTERS = {
    (2016, 1),   # Not released by PSA
    (2019, 1),   # Not released by PSA
    (2019, 2),   # Not released by PSA
}

# -----------------------------------------------------------------------------
# YEAR / QUARTER RANGE
# -----------------------------------------------------------------------------
YEARS    = range(2016, 2024)   # 2016 to 2023 inclusive
QUARTERS = [1, 2, 3, 4]

# 2024: only Q1 was available at time of thesis
EXTRA_QUARTERS = [(2024, 1)]

# -----------------------------------------------------------------------------
# SCHEMA CONFIG
# Each entry defines the raw column names for that quarter group.
# The pipeline uses this to read and rename columns correctly.
#
# Keys: (year, quarter) tuples
# Values: dict with:
#   hh_col       — raw column name for Household Number
#   urban_col    — raw column name for Urban/Rural (None if missing in raw file)
#   grade_map_v  — which grade mapping version to use: 1 or 2
# -----------------------------------------------------------------------------

def get_schema(year: int, quarter: int) -> dict:
    """
    Returns the schema config for a given year/quarter combination.
    Encodes all PSA structural changes across survey rounds.
    """

    # --- Special one-off: 2018 Q2 uses HHSQN instead of PUFHHNUM ---
    if year == 2018 and quarter == 2:
        return {
            "hh_col":      "HHSQN",
            "urban_col":   "PUFURB2K10",
            "grade_map_v": 2,
        }

    # --- 2023 Q4 and 2024 Q1: Urban_Rural column does not exist in raw file ---
    if (year == 2023 and quarter == 4) or (year == 2024 and quarter == 1):
        return {
            "hh_col":      "PUFHHNUM",
            "urban_col":   None,           # will be injected as NaN
            "grade_map_v": 2,
        }

    # --- 2020 Q1 through 2023 Q3: Urban_Rural column renamed to PUFURB2015 ---
    if (year == 2020) or \
       (year == 2021) or \
       (year == 2022) or \
       (year == 2023 and quarter <= 3):
        return {
            "hh_col":      "PUFHHNUM",
            "urban_col":   "PUFURB2015",
            "grade_map_v": 2,
        }

    # --- 2016 Q2: Pre-K12 education coding (grade_map v1) ---
    if year == 2016 and quarter == 2:
        return {
            "hh_col":      "PUFHHNUM",
            "urban_col":   "PUFURB2K10",
            "grade_map_v": 1,
        }

    # --- Default: 2016 Q3 through 2019 Q4 ---
    return {
        "hh_col":      "PUFHHNUM",
        "urban_col":   "PUFURB2K10",
        "grade_map_v": 2,
    }


def get_raw_filepath(year: int, quarter: int) -> str:
    """Constructs the expected raw CSV file path for a given year/quarter."""
    filename = f"LFS PUF Q{quarter} {year}.CSV"
    return os.path.join(RAW_DATA_DIR, filename)


def get_output_filepath(year: int, quarter: int) -> str:
    """Constructs the output Excel file path for a given year/quarter."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"{year}Q{quarter}_CleanedData.xlsx"
    return os.path.join(OUTPUT_DIR, filename)


def get_merged_csv_path() -> str:
    return os.path.join(OUTPUT_DIR, "LFS_Merged_2016_2024.csv")


def get_age_filtered_path() -> str:
    return os.path.join(OUTPUT_DIR, "LFS_Analysis_Ready.csv")


def get_final_output_path() -> str:
    return os.path.join(OUTPUT_DIR, "LFS_Final_WithLabels.xlsx")


# All columns that are constant across all schema groups (after renaming)
STANDARD_COLUMNS = [
    "Region", "Household_Number", "Urban_Rural", "Total_Household_Members",
    "Family_Relationship", "Sex", "Age", "Marital_Status",
    "Highest_Grade_Completed", "Currently_Attending_School",
    "Primary_Occupation", "WagePerDay", "Nature_of_Employment",
    "Class_of_Worker", "Working_Hours_PerDay", "Other_Job_indicator",
    "First_Time_Work",
]

# Fixed column rename map — same across ALL quarters (raw PSA name → standard name)
COLUMN_RENAME_MAP = {
    "PUFREG":       "Region",
    "PUFHHSIZE":    "Total_Household_Members",
    "PUFC03_REL":   "Family_Relationship",
    "PUFC04_SEX":   "Sex",
    "PUFC05_AGE":   "Age",
    "PUFC06_MSTAT": "Marital_Status",
    "PUFC07_GRADE": "Highest_Grade_Completed",
    "PUFC08_CURSCH":"Currently_Attending_School",
    "PUFC14_PROCC": "Primary_Occupation",
    "PUFC25_PBASIC":"WagePerDay",
    "PUFC17_NATEM": "Nature_of_Employment",
    "PUFC23_PCLASS":"Class_of_Worker",
    "PUFC18_PNWHRS":"Working_Hours_PerDay",
    "PUFC26_OJOB":  "Other_Job_indicator",
    "PUFC22_PFWRK": "First_Time_Work",
    # HH and Urban cols are dynamic — handled in loader.py
}
