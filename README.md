# Health Data Pipeline

A PySpark pipeline for processing DHIS2 health program data thus ingesting, resolving, quality-checking, and aggregating service delivery metrics across organisation units and periods.

Run the entire pipeline end-to-end with a single command:

```bash
python pipeline.py

# Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Star Schema](#star-schema)
- [Pipeline Stages](#pipeline-stages)
- [Design Decisions](#design-decisions)
- [Assumptions](#assumptions)
- [Known Limitations](#known-limitations)


 Project Structure

```
Py/
├── pipeline.py                  # Main entry point — run this
├── generate_data.py             # Synthetic data generator for testing
├── data/
│   ├── metadata.json            # Data element definitions
│   ├── org_units.json           # Organisation unit hierarchy
│   ├── programs.json            # Program metadata
│   └── data_values.json         # Raw service delivery values
├── models/
│   ├── ingest.py                # Data loading and exploding
│   ├── metadata.py              # UID resolution and ghost record quarantine
│   ├── hierarchy.py             # Org unit hierarchy resolution
│   ├── quality.py               # Data quality checks and flags
│   ├── warehouse.py             # Dimension and fact table builder
│   └── analytics.py             # Analytics: MoM change, rolling avg, reporting rate
└── output/
    ├── quarantine/ghost_records/ # Unresolved UID records (parquet)
    ├── dim_org/                  # Org unit dimension (parquet)
    ├── dim_period/               # Period dimension (parquet)
    ├── dim_data_element/         # Data element dimension (parquet)
    ├── fact_service_delivery/    # Fact table partitioned by period (parquet)
    ├── analytics/                # MoM and rolling average output (parquet)
    └── reporting_rate/           # Reporting rate by period (CSV)

## Requirements

- Python 3.10+
- Java 17 (Eclipse Temurin recommended)
- Apache Spark 4.x (via PySpark)
- Hadoop winutils (Windows only)

### Python Dependencies

bash
pip install pyspark
```

## Setup

### 1. Java 17

Download and install from [Adoptium](https://adoptium.net/temurin/releases/?version=17).
Select: **Windows → x64 → JDK → .msi**

### 2. Winutils (Windows only)

Download `winutils.exe` and `hadoop.dll` from [kontext-tech/winutils](https://github.com/kontext-tech/winutils/tree/master/hadoop-3.3.5/bin) and place both in `C:\hadoop\bin\`.

Then copy `hadoop.dll` to System32 (run PowerShell as Administrator):

```powershell
Copy-Item "C:\hadoop\bin\hadoop.dll" "C:\Windows\System32\hadoop.dll" -Force
```

### 3. Environment Variables

At the top of `pipeline.py`, update the Java path to match your installation:

python
import os

JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"  
HADOOP_HOME = r"C:\hadoop"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = JAVA_HOME + r"\bin;" + HADOOP_HOME + r"\bin;" + os.environ.get("PATH", "")

### 4. Create Output Directories
powershell
New-Item -ItemType Directory -Path "output\quarantine\ghost_records" -Force
New-Item -ItemType Directory -Path "C:\tmp\spark" -Force

## Running the Pipeline

```bash python pipeline.py
```

### Expected Output

INFO: Loading data...
INFO: Loaded rows — metadata: 40, org: 658, program: 1, values: 152802
INFO: Resolving metadata UIDs...
INFO: Ghost rate: 0.05
INFO: Resolving hierarchy...
INFO: Applying data quality checks...
INFO: Building dimensions...
INFO: Building fact table...
INFO: Running analytics...
INFO: Pipeline completed successfully
```

## Star Schema

```
                        +------------------+
                        |   dim_period     |
                        |------------------|
                        | period (PK)      |
                        +--------+---------+
                                 |
+-------------------+            |            +------------------------+
|    dim_org        |            |            |   dim_data_element     |
|-------------------|            |            |------------------------|
| orgUnit (PK)      +-----+      |      +-----+ dataElement (PK)       |
| org_name          |     |      |      |     | element_name           |
| path              |     |      |      |     | valueType              |
| level             |     |      |      |     | aggregationType        |
| country_id        |     |      |      |     +------------------------+
| region_id         |     |      |      |
| district_id       |     |      |      |
| facility_id       |     |      |      |
+-------------------+     |      |      |
                          |      |      |
                   +------+------+------+------+
                   |     fact_service_delivery  |
                   |----------------------------|
                   | orgUnit (FK)               |
                   | dataElement (FK)           |
                   | period (FK)                |
                   | value_numeric              |
                   | is_missing_value           |
                   | is_explicit_zero           |
                   | is_late_reported           |
                   +----------------------------+

## Pipeline Stages

| Stage | Module | Description |
|---|---|---|
| Ingest | `ingest.py` | Reads JSON files, explodes nested arrays into flat rows |
| UID Resolution | `metadata.py` | Joins values to metadata and org units; quarantines unresolved records |
| Hierarchy | `hierarchy.py` | Splits org unit path into country/region/district/facility IDs |
| Quality | `quality.py` | Flags missing values, explicit zeros, and late reporting |
| Warehouse | `warehouse.py` | Builds star schema dimensions and a period-partitioned fact table |
| Analytics | `analytics.py` | Computes month-over-month change, 3-month rolling average, reporting rate |

---

## Design Decisions

**Broadcast joins for metadata and org units**
Metadata (40 rows) and org units (658 rows) are small enough to broadcast, avoiding expensive shuffle joins against the 152k+ value records.

**Star schema over flat table**
Separating dimensions from facts avoids repeating descriptive attributes (org name, element name, value type) in every fact row, reducing storage and enabling clean slicing by dimension.

**Medallion-style processing**
The pipeline follows a bronze → silver → gold pattern: raw JSON ingestion → resolved and quality-checked records → aggregated analytics output.

**Ghost record quarantine**
Unresolvable UIDs are isolated into a quarantine folder rather than silently dropped, preserving auditability. The pipeline aborts if more than 10% of records are ghosts, indicating a data feed problem.

**Period partitioning on the fact table**
The fact table is partitioned by `period` so downstream queries filtered by time range avoid full scans.

**Null-safe division in analytics**
Month-over-month change uses `nullif` on the denominator to return NULL instead of raising a divide-by-zero error, which is the correct behaviour for periods with no prior data.

## Assumptions

- Input JSON files follow the DHIS2 2.38 export format
- `period` values are in `yyyyMM` format (e.g. `202409` = September 2024)
- Org unit paths use `/`-delimited UIDs with up to 5 levels (root/country/region/district/facility)
- A ghost rate below 10% is acceptable for the pipeline to continue
- `lastUpdated` is not available in the values export; `period` is used as a proxy for timeliness checks

## Known Limitations

- **Windows-only tested** — winutils dependency makes Linux/Mac setup different; on Linux, remove the `HADOOP_HOME` env block and winutils entirely
- **Single-node Spark** — runs on `local[*]`; not configured for a cluster without changes to the SparkSession master
- **No incremental loading** — all outputs are overwritten on each run; there is no checkpoint or delta mechanism
- **Ghost rate threshold is hardcoded** — the 10% abort threshold is fixed in `pipeline.py` and not configurable via argument
- **Analytics assume monthly periods only** — rolling 3-month window logic is based on row ordering, not calendar dates, so non-monthly periods would give incorrect results
- **`is_late_reported` is always 0** — because `lastUpdated` was not available in the data export, `period` is used for both `period_end` and `updated_date`, making all `days_late` values zero
