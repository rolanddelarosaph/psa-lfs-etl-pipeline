# =============================================================================
# validators/validator.py
# VALIDATE LAYER — runs post-cleaning sanity checks on each quarterly output.
# Logs a summary of every run. Raises errors on critical failures so the
# pipeline stops rather than silently producing bad data.
# =============================================================================

import os
import logging
import pandas as pd
from datetime import datetime
from config.settings import LOG_DIR

# Valid PSA region codes after cleaning (Region 18 is excluded by design)
VALID_REGIONS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}

# Minimum rows expected per quarter after cleaning (safety floor)
MIN_ROWS_PER_QUARTER = 100

# Valid occupation codes after mapping
VALID_OCCUPATIONS = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "Unknown"}

# Set up logging to file and console
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(
    LOG_DIR, f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("lfs_pipeline")


def validate(df: pd.DataFrame, year: int, quarter: int,
             rows_in: int) -> pd.DataFrame:
    """
    Run post-cleaning validation checks on a quarterly DataFrame.
    Logs a summary and raises on critical failures.

    Args:
        df:       Cleaned DataFrame from base_cleaner.py
        year:     Survey year
        quarter:  Survey quarter
        rows_in:  Row count of the raw file before cleaning (for drop summary)

    Returns:
        The same DataFrame (unchanged) if all checks pass.

    Raises:
        ValueError: on any critical data quality failure.
    """
    tag = f"{year} Q{quarter}"
    rows_out = len(df)
    rows_dropped = rows_in - rows_out
    drop_pct = (rows_dropped / rows_in * 100) if rows_in > 0 else 0

    logger.info(f"{'='*60}")
    logger.info(f"VALIDATION REPORT — {tag}")
    logger.info(f"  Rows in (raw):     {rows_in:>10,}")
    logger.info(f"  Rows out (clean):  {rows_out:>10,}")
    logger.info(f"  Rows dropped:      {rows_dropped:>10,}  ({drop_pct:.1f}%)")

    errors = []

    # CHECK 1: Minimum row count
    if rows_out < MIN_ROWS_PER_QUARTER:
        errors.append(
            f"CRITICAL: Only {rows_out} rows after cleaning. "
            f"Expected at least {MIN_ROWS_PER_QUARTER}."
        )

    # CHECK 2: No negative ages
    if "Parent_Age" in df.columns:
        neg_parent = (df["Parent_Age"] < 0).sum()
        if neg_parent > 0:
            errors.append(f"CRITICAL: {neg_parent} rows with negative Parent_Age.")

    if "Child_Age" in df.columns:
        neg_child = (df["Child_Age"] < 0).sum()
        if neg_child > 0:
            errors.append(f"CRITICAL: {neg_child} rows with negative Child_Age.")

    # CHECK 3: Age gap — parent must be at least 15 years older than child
    if "Parent_Age" in df.columns and "Child_Age" in df.columns:
        bad_gap = (df["Parent_Age"] - df["Child_Age"] < 15).sum()
        logger.info(f"  Rows with age gap < 15 yrs: {bad_gap:,}  "
                    f"(will be filtered in post-processing)")

    # CHECK 4: Log Region 18 count (removed in post-processing, not per-quarter)
    if "Region" in df.columns:
        region18 = (df["Region"] == 18).sum()
        if region18 > 0:
            logger.info(
                f"  Region 18 rows present: {region18:,}  "
                f"(will be removed in post-processing)"
            )

    # CHECK 5: Schooling years in valid range [0, 14]
    for col in ["Parent_Schooling_Years", "Child_Schooling_Years"]:
        if col in df.columns:
            oob = df[col].dropna()
            oob = oob[(oob < 0) | (oob > 14)]
            if len(oob) > 0:
                errors.append(
                    f"WARNING: {len(oob)} out-of-range values in {col}."
                )

    # CHECK 6: Occupation codes are valid
    for col in ["Parent_Primary_Occupation", "Child_Primary_Occupation"]:
        if col in df.columns:
            bad_occ = ~df[col].isin(VALID_OCCUPATIONS)
            if bad_occ.sum() > 0:
                logger.warning(
                    f"  {bad_occ.sum()} unexpected occupation codes in {col}"
                )

    # CHECK 7: No duplicate household IDs
    if "Household_Number" in df.columns:
        # Each HH ID can appear multiple times (one per parent-child pair), that's fine.
        # But the raw OrigHHNUM within a quarter should not map to two New_HH IDs.
        if "OrigHHNUM" in df.columns:
            mapping = df[["OrigHHNUM", "Household_Number"]].drop_duplicates()
            dupes = mapping[mapping.duplicated("OrigHHNUM", keep=False)]
            if len(dupes) > 0:
                errors.append(
                    f"CRITICAL: {len(dupes)} OrigHHNUM values map to multiple "
                    f"Household_Number IDs. ID generation error."
                )

    # Log null counts for key columns
    key_cols = [
        "Parent_Age", "Child_Age", "Parent_Schooling_Years",
        "Child_Schooling_Years", "Parent_Primary_Occupation",
        "Child_Primary_Occupation",
    ]
    for col in key_cols:
        if col in df.columns:
            nulls = df[col].isna().sum()
            pct   = nulls / rows_out * 100 if rows_out > 0 else 0
            logger.info(f"  Nulls in {col:<35}: {nulls:>8,}  ({pct:.1f}%)")

    # Raise on any critical errors
    if errors:
        for e in errors:
            logger.error(f"  {e}")
        raise ValueError(
            f"Validation failed for {tag}:\n" + "\n".join(errors)
        )

    logger.info(f"  ✅ All checks passed for {tag}")
    return df
