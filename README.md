# LFS Pipeline — Philippine Labor Force Survey ETL

**Author:** Roland de la Rosa  
**Data Source:** Philippine Statistics Authority (PSA) Labor Force Survey  
**Coverage:** 2016 Q2 – 2024 Q1 (30 quarterly files, ~1.75M rows)  
**Research Question:** Intergenerational occupational and educational mobility across regions in the Philippines

---

## Project Structure

```
lfs-pipeline/
├── config/
│   └── settings.py          # Master config: paths, schema rules, quarter registry
├── extractors/
│   └── loader.py            # Reads raw CSV per quarter, handles all 3 schema groups
├── transformers/
│   ├── base_cleaner.py      # All 12 shared cleaning steps (identical across all years)
│   ├── grade_maps.py        # Grade code → label mapping (v1: 2016Q2, v2: all others)
│   └── occupation_map.py    # PSOC occupation code → major group mapping
├── validators/
│   └── validator.py         # Post-clean sanity checks + run logging
├── loaders/
│   └── writer.py            # Saves quarterly outputs + merge + age filter + legends
├── logs/                    # Auto-generated run logs (one per pipeline run)
├── notebooks/               # Original exploratory notebooks (archived, not in pipeline)
├── main.py                  # Pipeline orchestrator — run this
└── README.md
```

---

## Setup

### 1. Install dependencies
```bash
pip install pandas openpyxl
```

### 2. Set your file paths
Open `config/settings.py` and update the two path variables:
```python
RAW_DATA_DIR = "/Users/rolanddelarosa/Desktop/LFS/raw"
OUTPUT_DIR   = "/Users/rolanddelarosa/Desktop/LFS/output"
LOG_DIR      = "/Users/rolanddelarosa/Desktop/LFS/logs"
```

### 3. Raw file naming convention
Raw CSV files must be named exactly:
```
LFS PUF Q1 2017.CSV
LFS PUF Q2 2017.CSV
...
```

---

## Running the Pipeline

```bash
# Full pipeline (recommended) — runs all steps end to end
python main.py

# Run only the quarterly cleaning step
python main.py --step extract

# Run only the merge + age filter step (after extract is done)
python main.py --step merge

# Run only the final legends step (after merge is done)
python main.py --step legends
```

---

## Output Files

| File | Description |
|------|-------------|
| `output/{year}Q{quarter}_CleanedData.xlsx` | One file per quarter, cleaned parent-child pairs |
| `output/Merged_CleanedData.csv` | All quarters merged, ~1.75M rows |
| `output/AgeFiltered_CleanedData.csv` | Child ≥ 25, age gap ≥ 15, Region 18 removed |
| `output/CleanedDataWithLegends.xlsx` | Final analysis-ready file with human-readable labels |
| `logs/pipeline_run_YYYYMMDD_HHMMSS.log` | Full run log with row counts and validation results |

---

## Key Engineering Decisions

### The 3-Schema Problem
The PSA LFS raw files changed column names across years. Instead of 30 separate cleaning scripts, the pipeline uses a **schema config registry** (`config/settings.py → get_schema()`) that maps each quarter to the correct column names. The rest of the pipeline is generic.

| Schema Group | Years | Urban/Rural Column | Notes |
|---|---|---|---|
| Group A | 2016 Q2 – 2019 Q4 | `PUFURB2K10` | Standard |
| Exception | 2018 Q2 | `PUFURB2K10` | Household col is `HHSQN` instead of `PUFHHNUM` |
| Group B | 2020 Q1 – 2023 Q3 | `PUFURB2015` | Renamed in PSA redesign |
| Group C | 2023 Q4, 2024 Q1 | *(missing)* | Injected as NaN |

### The 2-Grade-Map Problem
PSA changed education coding when K-12 was introduced mid-2016. The code `210` means **Grade 1** in 2016 Q2, but **Grade 7 (Junior High)** from 2016 Q3 onwards. The pipeline selects the correct mapping version per quarter via config.

### Excluded Quarters
| Quarter | Reason |
|---|---|
| 2016 Q1 | Not publicly released by PSA |
| 2019 Q1 | Not publicly released by PSA |
| 2019 Q2 | Not publicly released by PSA |

---

## Downstream Model
The final output (`CleanedDataWithLegends.xlsx`) feeds a **multilevel model (MLM)** analyzing intergenerational occupational and educational mobility across regions, birth cohorts, and urban/rural settings in the Philippines.
