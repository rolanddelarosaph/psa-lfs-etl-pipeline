# =============================================================================
# loaders/writer.py
# LOAD LAYER — saves cleaned quarterly outputs and runs the post-processing
# merge, age-filter, and legend-assignment steps.
# =============================================================================

import os
import pandas as pd
from config.settings import (
    OUTPUT_DIR,
    get_output_filepath,
    get_merged_csv_path,
    get_age_filtered_path,
    get_final_output_path,
)


# ---------------------------------------------------------------------------
# Region and other legend maps — used only in the final legends step
# ---------------------------------------------------------------------------
REGION_NAME_MAP = {
    13: "NCR",        14: "CAR",
    1:  "Region I",   2:  "Region II",  3:  "Region III",
    4:  "Region IV-A", 17: "MIMAROPA",
    5:  "Region V",   6:  "Region VI",  7:  "Region VII",
    8:  "Region VIII", 9: "Region IX",  10: "Region X",
    11: "Region XI",  12: "Region XII", 16: "Region XIII",
    15: "ARMM",
}

OCCUPATION_LABEL_MAP = {
    1: "Managers",
    2: "Professionals",
    3: "Technicians and Associate Professionals",
    4: "Clerical Support Workers",
    5: "Service and Sales Workers",
    6: "Skilled Agricultural, Forestry and Fishery Workers",
    7: "Craft and Related Trades Workers",
    8: "Plant and Machine Operators and Assemblers",
    9: "Elementary Occupations",
}


# ---------------------------------------------------------------------------
# QUARTERLY SAVE
# ---------------------------------------------------------------------------

def save_quarter(df: pd.DataFrame, year: int, quarter: int) -> str:
    """
    Save a single cleaned quarterly DataFrame to Excel.

    Args:
        df:      Cleaned, validated DataFrame
        year:    Survey year
        quarter: Survey quarter

    Returns:
        The output file path.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = get_output_filepath(year, quarter)
    df.to_excel(output_path, index=False)
    return output_path


# ---------------------------------------------------------------------------
# POST-PROCESSING: Merge → Age Filter → Legends
# ---------------------------------------------------------------------------

def merge_all_quarters() -> pd.DataFrame:
    """
    Read all quarterly Excel outputs and merge them into one CSV.
    Adds a 'Source' tag column.
    """
    import glob
    files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "*_CleanedData.xlsx")))

    if not files:
        raise FileNotFoundError(
            f"No cleaned quarterly files found in {OUTPUT_DIR}. "
            f"Run the main pipeline first."
        )

    frames = []
    for f in files:
        tag = os.path.basename(f).split("_")[0]   # e.g. "2016Q2"
        df  = pd.read_excel(f)
        df["Source"] = tag
        frames.append(df)
        print(f"  Merged: {os.path.basename(f)}  ({len(df):,} rows)")

    merged = pd.concat(frames, ignore_index=True)
    merged.to_csv(get_merged_csv_path(), index=False)
    print(f"\n✅ Merged file saved: {get_merged_csv_path()}")
    print(f"   Total rows: {len(merged):,}")
    return merged


def apply_age_filters(df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Apply final age filters and regional corrections to the merged dataset.

    Filters applied:
      - Child_Age >= 25
      - Parent_Age - Child_Age >= 15  (realistic parent-child gap)
      - Remove Region 18
      - Fix Region 19 → 15 (non-official code → ARMM/BARMM)
      - Add Child_Birth_Cohort column

    Drops columns no longer needed:
      Source, Total_Household_Members,
      Parent/Child Nature_of_Employment, Class_of_Worker,
      Other_Job_indicator

    Args:
        df: merged DataFrame (if None, reads from disk)

    Returns:
        Filtered DataFrame
    """
    if df is None:
        print(f"Reading merged file from disk...")
        df = pd.read_csv(get_merged_csv_path())

    original_rows = len(df)

    # Drop unnecessary columns
    cols_to_drop = [
        "Parent_Nature_of_Employment", "Child_Nature_of_Employment",
        "Parent_Class_of_Worker",      "Child_Class_of_Worker",
        "Parent_Other_Job_indicator",  "Child_Other_Job_indicator",
        "Source", "Total_Household_Members",
    ]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # Replace "Unknown" with blank in occupation columns
    for col in ["Parent_Primary_Occupation", "Child_Primary_Occupation"]:
        if col in df.columns:
            df[col] = df[col].replace("Unknown", "")

    # Convert numeric columns
    for col in ["Parent_Schooling_Years", "Child_Schooling_Years",
                "Children_Per_Household", "Child_Age", "Parent_Age"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Fix Region 19 → 15 (non-standard code)
    df.loc[df["Region"] == 19, "Region"] = 15

    # Add birth cohort
    df["Child_Birth_Cohort"] = df["Year"] - df["Child_Age"]

    # Age filters
    df = df[df["Child_Age"] >= 25].copy()
    df = df[(df["Parent_Age"] - df["Child_Age"]) >= 15].copy()

    # Remove Region 18
    df = df[df["Region"] != 18].copy()

    df.to_csv(get_age_filtered_path(), index=False)

    print(f"\n✅ Age-filtered file saved: {get_age_filtered_path()}")
    print(f"   Rows before filter: {original_rows:,}")
    print(f"   Rows after filter:  {len(df):,}")
    print(f"   Rows removed:       {original_rows - len(df):,}")

    return df


def apply_legends(df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Apply human-readable labels to coded columns and save the final
    analysis-ready Excel file.

    Applies:
      - Urban_Rural: 1 → Urban, 2 → Rural
      - Region: numeric codes → names
      - Primary_Occupation: numeric codes → labels
      - Parent_Sex: 1 → Father, 2 → Mother
      - Child_Sex: 1 → Son, 2 → Daughter
      - Drop raw Educational_Attainment columns (keep Schooling_Years)

    Args:
        df: age-filtered DataFrame (if None, reads from disk)

    Returns:
        Final labeled DataFrame
    """
    if df is None:
        print(f"Reading age-filtered file from disk...")
        # Read occupation cols as str to preserve any non-numeric values
        df = pd.read_csv(
            get_age_filtered_path(),
            dtype={
                "Parent_Primary_Occupation": str,
                "Child_Primary_Occupation":  str,
            }
        )

    # Urban / Rural
    df["Urban_Rural"] = df["Urban_Rural"].map({1: "Urban", 2: "Rural"})

    # Region names
    df["Region"] = pd.to_numeric(df["Region"], errors="coerce")
    df["Region"] = df["Region"].map(REGION_NAME_MAP)

    # Occupation labels
    for col in ["Parent_Primary_Occupation", "Child_Primary_Occupation"]:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .map(OCCUPATION_LABEL_MAP)
            )

    # Sex labels
    df["Parent_Sex"] = df["Parent_Sex"].map({1: "Father", 2: "Mother"})
    df["Child_Sex"]  = df["Child_Sex"].map({1: "Son",    2: "Daughter"})

    # Drop raw attainment columns (Schooling_Years is what the model uses)
    df = df.drop(
        columns=["Parent_Educational_Attainment", "Child_Educational_Attainment"],
        errors="ignore",
    )

    output_path = get_final_output_path()
    df.to_excel(output_path, index=False)

    print(f"\n✅ Final labeled file saved: {output_path}")
    print(f"   Total rows: {len(df):,}")

    return df
