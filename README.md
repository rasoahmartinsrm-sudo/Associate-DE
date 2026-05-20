# Health Data Pipeline

A modular PySpark pipeline for processing DHIS2 health program data. It involves ingesting, resolving, quality-checking, and aggregating service delivery metrics across organisation units and periods.

Run the entire pipeline end-to-end with a single command:

```bash
python pipeline.py
```

---

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Star Schema](#star-schema)
- [Pipeline Stages](#pipeline-stages)
- [Design Decisions](#design-decisions)
- [Assumptions](#assumptions)
- [Known Limitations](#known-limitations)
- [Output](#output)
- [Tests](#tests)

---

## Project Structure

```
Py/
├── pipeline.py
├── generate_data.py
├── requirements.txt
├── data/
│   ├── metadata.json
│   ├── org_units.json
│   ├── programs.json
│   └── data_values.json
├── models/
│   ├── ingest.py
│   ├── metadata.py
│   ├── hierarchy.py
│   ├── quality.py
│   ├── warehouse.py
│   └── analytics.py
├── tests/
│   ├── conftest.py
│   └── test_contracts.py
└── output/
    ├── quarantine/ghost_records/
    ├── dim_org/
    ├── dim_period/
    ├── dim_data_element/
    ├── fact_service_delivery/
    ├── analytics/
    └── reporting_rate/
```

---

## Requirements

- Python 3.10+
- Java 17 (Eclipse Temurin recommended)
- Apache Spark 4.x via PySpark
- Hadoop winutils (Windows only)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Setup

### 1. Install Java 17

Download from [Adoptium](https://adoptium.net/temurin/releases/?version=17). Select Windows x64 JDK .msi.

### 2. Install Winutils (Windows only)

Download `winutils.exe` and `hadoop.dll` from [kontext-tech/winutils](https://github.com/kontext-tech/winutils/tree/master/hadoop-3.3.5/bin) and place both in `C:\hadoop\bin\`.

Then run PowerShell as Administrator:

```powershell
Copy-Item "C:\hadoop\bin\hadoop.dll" "C:\Windows\System32\hadoop.dll" -Force
```

### 3. Set Environment Variables

At the top of `pipeline.py`, update the Java path to match your installation:

```python
import os

JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
HADOOP_HOME = r"C:\hadoop"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = JAVA_HOME + r"\bin;" + HADOOP_HOME + r"\bin;" + os.environ.get("PATH", "")
```

### 4. Create Output Directories

```powershell
New-Item -ItemType Directory -Path "output\quarantine\ghost_records" -Force
New-Item -ItemType Directory -Path "C:\tmp\spark" -Force
```

---

## Running the Pipeline

```bash
python pipeline.py
```

Expected output:

```
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

---

## Star Schema

The pipeline produces a star schema with three dimension tables and one fact table.

**dim_org** — Organisation unit dimension

| Column | Description |
| --- | --- |
| orgUnit | Primary key |
| org_name | Facility name |
| path | Full UID path |
| level | Hierarchy level |
| country_id | Country UID |
| region_id | Region UID |
| district_id | District UID |
| facility_id | Facility UID |

**dim_period** — Period dimension

| Column | Description |
| --- | --- |
| period | Primary key (yyyyMM format) |

**dim_data_element** — Data element dimension

| Column | Description |
| --- | --- |
| dataElement | Primary key |
| element_name | Human readable name |
| valueType | Data type |
| aggregationType | Aggregation method |

**fact_service_delivery** — Fact table (partitioned by period)

| Column | Description |
| --- | --- |
| orgUnit | Foreign key to dim_org |
| dataElement | Foreign key to dim_data_element |
| period | Foreign key to dim_period |
| value_numeric | Numeric value |
| is_missing_value | 1 if value is NULL |
| is_explicit_zero | 1 if value is exactly 0 |
| is_late_reported | 1 if reported more than 60 days late |

---

## Pipeline Stages

| Stage | Module | Description |
| --- | --- | --- |
| Ingest | ingest.py | Reads JSON files and explodes nested arrays into flat rows |
| UID Resolution | metadata.py | Joins values to metadata and org units, quarantines ghost records |
| Hierarchy | hierarchy.py | Splits org unit path into country, region, district, facility IDs |
| Quality | quality.py | Flags missing values, explicit zeros, and late reporting |
| Warehouse | warehouse.py | Builds star schema dimensions and period-partitioned fact table |
| Analytics | analytics.py | Computes month-over-month change, 3-month rolling average, reporting rate |

---

## Design Decisions

**Broadcast joins for metadata and org units**
Metadata (40 rows) and org units (658 rows) are small enough to broadcast, avoiding expensive shuffle joins against the 152k+ value records.

**Star schema over flat table**
Separating dimensions from facts avoids repeating descriptive attributes in every fact row, reducing storage and enabling clean slicing by dimension.

**Medallion-style processing**
The pipeline follows a bronze to silver to gold pattern: raw JSON ingestion, then resolved and quality-checked records, then aggregated analytics output.

**Ghost record quarantine**
Unresolvable UIDs are isolated into a quarantine folder rather than silently dropped, preserving auditability. The pipeline aborts if more than 10% of records are ghosts.

**Period partitioning on the fact table**
The fact table is partitioned by period so downstream queries filtered by time range avoid full scans.

**Null-safe division in analytics**
Month-over-month change uses nullif on the denominator to return NULL instead of raising a divide-by-zero error.

---

## Assumptions

- Input JSON files follow the DHIS2 2.38 export format
- Period values are in yyyyMM format, for example 202409 means September 2024
- Org unit paths use forward-slash delimited UIDs with up to 5 levels: root, country, region, district, facility
- A ghost rate below 10% is acceptable for the pipeline to continue
- lastUpdated is not available in the values export so period is used as a proxy for timeliness checks

---

## Known Limitations

- Windows-only tested — on Linux or Mac, remove the HADOOP_HOME env block and winutils entirely
- Single-node Spark — runs on local mode and is not configured for a cluster without changes to the SparkSession master
- No incremental loading — all outputs are overwritten on each run with no checkpoint or delta mechanism
- Ghost rate threshold is hardcoded at 10% and is not configurable via argument
- Analytics assume monthly periods only — rolling 3-month window logic is based on row ordering not calendar dates
- is_late_reported is always 0 because lastUpdated was not available in the data export

---

## Output

All outputs are written to the output/ folder automatically when the pipeline runs.

| Path | Format | Description |
| --- | --- | --- |
| output/dim_org/ | Parquet | Organisation unit dimension |
| output/dim_period/ | Parquet | Period dimension |
| output/dim_data_element/ | Parquet | Data element dimension |
| output/fact_service_delivery/ | Parquet partitioned by period | Fact table |
| output/analytics/ | Parquet | Month-over-month and rolling average |
| output/reporting_rate/ | CSV | Reporting rate by period |
| output/quarantine/ghost_records/ | Parquet | Unresolved UID records |

---

## Tests

Data contract tests are in tests/test_contracts.py using pytest.

Install pytest:

```bash
pip install pytest
```

Run all tests:

```bash
pytest tests/ -v
```

Tests cover:

- Ingest schema and period format validation
- UID resolution ghost isolation and ghost rate threshold
- Quality flag correctness for missing values, explicit zeros, and numeric casting
- Fact table grain uniqueness and required columns
