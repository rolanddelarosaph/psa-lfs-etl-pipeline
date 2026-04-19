# =============================================================================
# transformers/base_cleaner.py
# TRANSFORM LAYER — applies all cleaning and transformation steps that are
# shared identically across every year and quarter.
#
# Steps (in order):
#   1.  Count children per household
#   2.  Exclude invalid grade codes
#   3.  Exclude invalid occupation codes
#   4.  Remove currently attending school
#   5.  Keep only Head (1), Spouse (2), Child (3) family relationships
#   6.  Keep only eligible households (have at least one parent AND one child)
#   7.  Build parent-child pairs (inner merge within household)
#   8.  Apply grade mapping (v1 or v2 based on schema config)
#   9.  Apply occupation mapping
#   10. Calculate Educational Attainment + Schooling Years for parent and child
#   11. Age filter (Parent ≤ 80, Child ≥ 20)
#   12. Select, reorder, and rename final columns
#   13. Generate new sequential Household IDs
# =============================================================================

import pandas as pd
from config.settings import get_schema
from transformers.grade_maps import apply_grade_map
from transformers.occupation_map import apply_occupation_map


# ---------------------------------------------------------------------------
# STEP 10 HELPER: Education attainment + schooling years
# Identical function logic for both parent and child.
# ---------------------------------------------------------------------------

def _education_and_schooling(grade: str) -> tuple:
    """
    Convert a grade label string into (attainment_code, schooling_years).
    Returns (pd.NA, pd.NA) if grade is unrecognized.
    """
    attainment    = pd.NA
    schooling_yrs = pd.NA

    # --- Educational Attainment Code ---
    if grade == "No Grade Completed" or grade in ["Nursery", "Kindergarten"]:
        attainment = 1
    elif grade in ["Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5"]:
        attainment = 2
    elif grade == "Elementary Graduate / Grade 6 Completer":
        attainment = 3
    elif grade in ["Grade 7 / 1st Year Junior High",
                   "Grade 8 / 2nd Year Junior High",
                   "Grade 9 / 3rd Year Junior High"]:
        attainment = 4
    elif (grade == "Junior High Graduate"
          or grade.startswith("Grade 11")
          or grade.startswith("Grade 12 Graduate")):
        attainment = 5
    elif grade == "Post-Secondary Non-Tertiary Undergraduate":
        attainment = 6
    elif grade == "Post-Secondary Non-Tertiary Graduate":
        attainment = 7
    elif grade.endswith("Bachelor Level"):
        attainment = 8
    elif grade == "College Graduate":
        attainment = 9

    # --- Schooling Years ---
    if grade == "No Grade Completed" or grade in ["Nursery", "Kindergarten"]:
        schooling_yrs = 0
    elif grade == "Grade 1":
        schooling_yrs = 1
    elif grade == "Grade 2":
        schooling_yrs = 2
    elif grade == "Grade 3":
        schooling_yrs = 3
    elif grade == "Grade 4":
        schooling_yrs = 4
    elif grade == "Grade 5":
        schooling_yrs = 5
    elif grade == "Elementary Graduate / Grade 6 Completer":
        schooling_yrs = 6
    elif grade == "Grade 7 / 1st Year Junior High":
        schooling_yrs = 7
    elif grade == "Grade 8 / 2nd Year Junior High":
        schooling_yrs = 8
    elif grade == "Grade 9 / 3rd Year Junior High":
        schooling_yrs = 9
    elif (grade == "Junior High Graduate"
          or grade.startswith("Grade 11")
          or grade.startswith("Grade 12 Graduate")):
        schooling_yrs = 10
    elif grade in ["Post-Secondary Non-Tertiary Undergraduate",
                   "1st Year Bachelor Level"]:
        schooling_yrs = 11
    elif grade in ["Post-Secondary Non-Tertiary Graduate",
                   "2nd Year Bachelor Level"]:
        schooling_yrs = 12
    elif grade == "3rd Year Bachelor Level":
        schooling_yrs = 13
    elif grade in ["4th Year Bachelor Level", "5th Year Bachelor Level",
                   "6th Year Bachelor Level", "College Graduate"]:
        schooling_yrs = 14

    return attainment, schooling_yrs


def _apply_education_columns(df: pd.DataFrame,
                              grade_col: str,
                              attainment_col: str,
                              schooling_col: str) -> pd.DataFrame:
    """Apply education + schooling calculation to a pair of columns."""
    results = df[grade_col].apply(
        lambda g: pd.Series(_education_and_schooling(str(g)),
                             index=[attainment_col, schooling_col])
    )
    df[attainment_col] = results[attainment_col]
    df[schooling_col]  = results[schooling_col]
    return df


# ---------------------------------------------------------------------------
# MAIN CLEANING FUNCTION
# ---------------------------------------------------------------------------

def clean(df: pd.DataFrame, year: int, quarter: int) -> pd.DataFrame:
    """
    Apply all shared cleaning and transformation steps to a raw quarterly
    LFS DataFrame. Returns the cleaned parent-child pairs DataFrame.

    Args:
        df:      Raw DataFrame from loader.py (already renamed columns)
        year:    Survey year
        quarter: Survey quarter

    Returns:
        pd.DataFrame of cleaned parent-child pairs for this quarter
    """
    schema = get_schema(year, quarter)
    grade_map_version = schema["grade_map_v"]

    # ------------------------------------------------------------------
    # STEP 1: Count children (Family_Relationship == 3) per household
    # ------------------------------------------------------------------
    df = df.copy()
    df["Children_Per_Household"] = (
        df.groupby("Household_Number")["Family_Relationship"]
        .transform(lambda x: (x == 3).sum())
    )

    # ------------------------------------------------------------------
    # STEP 2: Exclude invalid Highest_Grade_Completed codes
    # Codes 191 and 192 are PSA internal codes for "not applicable"
    # ------------------------------------------------------------------
    grades_to_exclude = {"191", "192"}
    df = df[~df["Highest_Grade_Completed"].astype(str).isin(grades_to_exclude)].copy()

    # ------------------------------------------------------------------
    # STEP 3: Exclude invalid Primary_Occupation codes
    # Codes 01, 02, 03, 1, 2, 3 are armed forces / not classified
    # ------------------------------------------------------------------
    occupations_to_exclude = {"01", "02", "03", "1", "2", "3"}
    df = df[~df["Primary_Occupation"].astype(str).isin(occupations_to_exclude)].copy()

    # ------------------------------------------------------------------
    # STEP 4: Remove individuals currently attending school
    # ------------------------------------------------------------------
    df = df[
        (df["Currently_Attending_School"] != 1)
        | df["Currently_Attending_School"].isna()
    ].copy()

    # ------------------------------------------------------------------
    # STEP 5: Keep only Head (1), Spouse (2), Child (3)
    # ------------------------------------------------------------------
    df["Family_Relationship"] = pd.to_numeric(
        df["Family_Relationship"], errors="coerce"
    )
    df = df[df["Family_Relationship"].isin([1, 2, 3])].copy()

    # ------------------------------------------------------------------
    # STEP 6: Keep only eligible households
    # Must have at least one parent (1 or 2) AND at least one child (3)
    # ------------------------------------------------------------------
    eligibility = df.groupby("Household_Number")["Family_Relationship"].agg(
        has_parent=lambda x: any(r in [1, 2] for r in x),
        has_child =lambda x: any(r == 3     for r in x),
    )
    eligible_hh = eligibility.query("has_parent & has_child").index
    df = df[df["Household_Number"].isin(eligible_hh)].copy()

    # ------------------------------------------------------------------
    # STEP 7: Drop helper columns, then split into parents and children
    # and create all parent-child pairs within each household
    # ------------------------------------------------------------------
    df = df.drop(columns=["Currently_Attending_School", "First_Time_Work"],
                 errors="ignore")

    common_cols = [
        "Year", "Quarter", "Region", "Household_Number", "Urban_Rural",
        "Children_Per_Household", "Total_Household_Members",
    ]

    parents  = df[df["Family_Relationship"].isin([1, 2])].copy()
    children = df[df["Family_Relationship"] == 3].copy()

    # Prefix non-common columns
    parents.rename(
        columns={c: f"Parent_{c}" for c in parents.columns if c not in common_cols},
        inplace=True,
    )
    children.rename(
        columns={c: f"Child_{c}" for c in children.columns if c not in common_cols},
        inplace=True,
    )

    # Inner merge — creates all valid parent-child combinations per household
    pairs = pd.merge(parents, children, on=common_cols, how="inner")

    # ------------------------------------------------------------------
    # STEP 8: Apply grade mapping to parent and child grade columns
    # ------------------------------------------------------------------
    pairs["Parent_Highest_Grade_Completed"] = apply_grade_map(
        pairs["Parent_Highest_Grade_Completed"], grade_map_version
    )
    pairs["Child_Highest_Grade_Completed"] = apply_grade_map(
        pairs["Child_Highest_Grade_Completed"], grade_map_version
    )

    # ------------------------------------------------------------------
    # STEP 9: Apply occupation mapping
    # ------------------------------------------------------------------
    pairs["Parent_Primary_Occupation"] = apply_occupation_map(
        pairs["Parent_Primary_Occupation"]
    )
    pairs["Child_Primary_Occupation"] = apply_occupation_map(
        pairs["Child_Primary_Occupation"]
    )

    # ------------------------------------------------------------------
    # STEP 10: Calculate Educational Attainment and Schooling Years
    # ------------------------------------------------------------------
    pairs = _apply_education_columns(
        pairs,
        grade_col      ="Parent_Highest_Grade_Completed",
        attainment_col ="Parent_Educational_Attainment",
        schooling_col  ="Parent_Schooling_Years",
    )
    pairs = _apply_education_columns(
        pairs,
        grade_col      ="Child_Highest_Grade_Completed",
        attainment_col ="Child_Educational_Attainment",
        schooling_col  ="Child_Schooling_Years",
    )

    # ------------------------------------------------------------------
    # STEP 11: Age filter — Parent ≤ 80, Child ≥ 20
    # (further tightened to ≥ 25 in the merge/post-processing step)
    # ------------------------------------------------------------------
    pairs = pairs[
        (pairs["Parent_Age"] <= 80) & (pairs["Child_Age"] >= 20)
    ].copy()

    # ------------------------------------------------------------------
    # STEP 12: Select and reorder final columns
    # ------------------------------------------------------------------
    final_cols = [
        "Year", "Quarter", "Region", "Household_Number", "Urban_Rural",
        "Children_Per_Household",
        "Parent_Sex", "Child_Sex",
        "Parent_Age", "Child_Age",
        "Parent_Marital_Status", "Child_Marital_Status",
        "Parent_Educational_Attainment", "Child_Educational_Attainment",
        "Parent_Schooling_Years", "Child_Schooling_Years",
        "Parent_Primary_Occupation", "Child_Primary_Occupation",
        "Parent_WagePerDay", "Child_WagePerDay",
        "Parent_Nature_of_Employment", "Child_Nature_of_Employment",
        "Parent_Class_of_Worker", "Child_Class_of_Worker",
        "Parent_Other_Job_indicator", "Child_Other_Job_indicator",
    ]
    pairs = pairs[[c for c in final_cols if c in pairs.columns]].copy()

    # ------------------------------------------------------------------
    # STEP 13: Generate sequential household IDs (YYYY-QQ-NNNNN format)
    # Preserves the original household number as OrigHHNUM
    # ------------------------------------------------------------------
    hh_seq = (
        pairs[["Household_Number"]]
        .drop_duplicates()
        .sort_values("Household_Number")
        .reset_index(drop=True)
    )
    hh_seq["New_HH"] = [
        f"{year}-{quarter:02d}-{i+1:05d}" for i in range(len(hh_seq))
    ]

    pairs = pairs.merge(hh_seq, on="Household_Number", how="left")
    pairs["OrigHHNUM"]        = pairs["Household_Number"]
    pairs["Household_Number"] = pairs["New_HH"]
    pairs = pairs.drop(columns=["New_HH"])

    # Bring OrigHHNUM to front
    front = ["OrigHHNUM", "Year", "Quarter", "Household_Number",
             "Region", "Urban_Rural", "Children_Per_Household"]
    rest  = [c for c in pairs.columns if c not in front]
    pairs = pairs[front + rest]

    return pairs
