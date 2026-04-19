# =============================================================================
# main.py
# PIPELINE ORCHESTRATOR — Run this file to execute the entire LFS pipeline.
#
# Usage:
#   python main.py                  # Run full pipeline (all steps)
#   python main.py --step extract   # Run only quarterly cleaning
#   python main.py --step merge     # Run only merge + post-processing
#   python main.py --step legends   # Run only final legends step
#
# What this does:
#   1. Loops through all 30 quarterly files (2016 Q2 – 2024 Q1)
#   2. For each quarter: loads raw CSV → cleans → validates → saves Excel
#   3. Merges all quarterly outputs into one CSV
#   4. Applies age filters and regional corrections
#   5. Applies human-readable legends → saves final Excel
# =============================================================================

import sys
import time
import traceback
import pandas as pd

from config.settings import (
    YEARS, QUARTERS, EXTRA_QUARTERS, EXCLUDED_QUARTERS,
    get_raw_filepath,
)
from extractors.loader      import load_quarter
from transformers.base_cleaner import clean
from validators.validator   import validate, logger
from loaders.writer         import (
    save_quarter,
    merge_all_quarters,
    apply_age_filters,
    apply_legends,
)


# ---------------------------------------------------------------------------
# STEP A: Process all quarters individually
# ---------------------------------------------------------------------------

def run_quarterly_pipeline():
    """
    Loop through all year/quarter combinations, run the full ETL
    for each, and save cleaned quarterly Excel files.
    """
    logger.info("=" * 60)
    logger.info("LFS PIPELINE — QUARTERLY PROCESSING START")
    logger.info("=" * 60)

    # Build the full list of quarters to process
    all_quarters = []
    for year in YEARS:
        for quarter in QUARTERS:
            if (year, quarter) not in EXCLUDED_QUARTERS:
                all_quarters.append((year, quarter))
    all_quarters.extend(EXTRA_QUARTERS)   # e.g. (2024, 1)
    all_quarters.sort()

    results = {"success": [], "skipped": [], "failed": []}

    for year, quarter in all_quarters:
        tag = f"{year} Q{quarter}"
        raw_path = get_raw_filepath(year, quarter)

        try:
            # ---- EXTRACT ----
            logger.info(f"\n▶ Processing {tag}...")
            df_raw = load_quarter(year, quarter)
            rows_in = len(df_raw)
            logger.info(f"  Loaded {rows_in:,} rows from raw file.")

            # ---- TRANSFORM ----
            df_clean = clean(df_raw, year, quarter)

            # ---- VALIDATE ----
            df_clean = validate(df_clean, year, quarter, rows_in)

            # ---- LOAD ----
            output_path = save_quarter(df_clean, year, quarter)
            logger.info(f"  Saved → {output_path}  ({len(df_clean):,} rows)")

            results["success"].append(tag)

        except FileNotFoundError as e:
            logger.warning(f"  ⚠ SKIPPED {tag}: {e}")
            results["skipped"].append(tag)

        except Exception as e:
            logger.error(f"  ✗ FAILED {tag}: {e}")
            logger.error(traceback.format_exc())
            results["failed"].append(tag)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("QUARTERLY PROCESSING COMPLETE")
    logger.info(f"  ✅ Succeeded: {len(results['success'])} quarters")
    logger.info(f"  ⚠ Skipped:  {len(results['skipped'])} quarters  "
                f"{results['skipped']}")
    logger.info(f"  ✗ Failed:   {len(results['failed'])} quarters  "
                f"{results['failed']}")
    logger.info("=" * 60)

    if results["failed"]:
        raise RuntimeError(
            f"Pipeline failed for: {results['failed']}. "
            f"Check logs for details."
        )

    return results


# ---------------------------------------------------------------------------
# STEP B: Merge + Post-processing
# ---------------------------------------------------------------------------

def run_merge_pipeline():
    """Merge all quarterly outputs, apply age filters and regional fixes."""
    logger.info("\n" + "=" * 60)
    logger.info("LFS PIPELINE — MERGE & POST-PROCESSING")
    logger.info("=" * 60)

    merged_df = merge_all_quarters()
    filtered_df = apply_age_filters(merged_df)

    return filtered_df


# ---------------------------------------------------------------------------
# STEP C: Final legends
# ---------------------------------------------------------------------------

def run_legends_pipeline(df: pd.DataFrame = None):
    """Apply human-readable labels and save final analysis-ready Excel."""
    logger.info("\n" + "=" * 60)
    logger.info("LFS PIPELINE — APPLYING LEGENDS")
    logger.info("=" * 60)

    final_df = apply_legends(df)
    return final_df


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    start = time.time()

    # Parse optional --step argument
    step = None
    if "--step" in sys.argv:
        idx  = sys.argv.index("--step")
        step = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    try:
        if step == "extract":
            run_quarterly_pipeline()

        elif step == "merge":
            run_merge_pipeline()

        elif step == "legends":
            run_legends_pipeline()

        else:
            # Default: run everything end-to-end
            run_quarterly_pipeline()
            filtered_df = run_merge_pipeline()
            run_legends_pipeline(filtered_df)

        elapsed = time.time() - start
        logger.info(f"\n🏁 Pipeline complete. Total time: {elapsed:.1f}s")

    except Exception as e:
        logger.error(f"\n🚨 Pipeline aborted: {e}")
        sys.exit(1)
