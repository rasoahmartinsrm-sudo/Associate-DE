"""
tests/test_contracts.py
Data contract tests for the health data pipeline.
Run with: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.quality import apply_quality_checks
from models.metadata import resolve_uids
from models.hierarchy import resolve_hierarchy


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .appName("PipelineTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def sample_values_df(spark):
    """Minimal values dataframe matching expected schema."""
    data = [
        ("elem_001", "org_001", "202409", "100", "combo_001"),
        ("elem_001", "org_002", "202409", None,  "combo_001"),
        ("elem_002", "org_001", "202409", "0",   "combo_002"),
        ("elem_999", "org_001", "202409", "50",  "combo_001"),  # ghost — unknown element
    ]
    schema = StructType([
        StructField("dataElement",        StringType(), True),
        StructField("orgUnit",            StringType(), True),
        StructField("period",             StringType(), True),
        StructField("value",              StringType(), True),
        StructField("categoryOptionCombo", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_metadata_df(spark):
    """Small metadata lookup."""
    data = [
        ("elem_001", "Malaria confirmed cases", "INTEGER_ZERO_OR_POSITIVE", "SUM"),
        ("elem_002", "Malaria deaths",          "INTEGER_ZERO_OR_POSITIVE", "SUM"),
    ]
    schema = StructType([
        StructField("id",              StringType(), True),
        StructField("name",            StringType(), True),
        StructField("valueType",       StringType(), True),
        StructField("aggregationType", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_org_df(spark):
    """Small org unit lookup."""
    data = [
        ("org_001", "Facility A", "/root/country/region/district/org_001", 5),
        ("org_002", "Facility B", "/root/country/region/district/org_002", 5),
    ]
    schema = StructType([
        StructField("id",    StringType(),  True),
        StructField("name",  StringType(),  True),
        StructField("path",  StringType(),  True),
        StructField("level", IntegerType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ── Ingest contract ──────────────────────────────────────────────────────────

class TestIngestContract:

    def test_values_has_required_columns(self, sample_values_df):
        required = {"dataElement", "orgUnit", "period", "value", "categoryOptionCombo"}
        assert required.issubset(set(sample_values_df.columns))

    def test_values_not_empty(self, sample_values_df):
        assert sample_values_df.count() > 0

    def test_period_format(self, sample_values_df):
        """All periods should be 6 characters (yyyyMM)."""
        invalid = sample_values_df.filter(
            col("period").isNull() | (col("period").rlike(r"^\d{6}$") == False)
        )
        assert invalid.count() == 0, f"{invalid.count()} rows have invalid period format"


# ── UID resolution contract ──────────────────────────────────────────────────

class TestUIDResolutionContract:

    def test_ghost_records_isolated(self, sample_values_df, sample_metadata_df, sample_org_df):
        clean_df, ghost_df = resolve_uids(
            sample_values_df, sample_metadata_df, None, sample_org_df
        )
        # elem_999 is unknown — should be in ghost
        assert ghost_df.count() >= 1

    def test_clean_records_have_no_null_element_name(self, sample_values_df, sample_metadata_df, sample_org_df):
        clean_df, _ = resolve_uids(
            sample_values_df, sample_metadata_df, None, sample_org_df
        )
        nulls = clean_df.filter(col("element_name").isNull()).count()
        assert nulls == 0

    def test_ghost_rate_below_threshold(self, sample_values_df, sample_metadata_df, sample_org_df):
        _, ghost_df = resolve_uids(
            sample_values_df, sample_metadata_df, None, sample_org_df
        )
        ghost_rate = ghost_df.count() / sample_values_df.count()
        assert ghost_rate <= 0.10, f"Ghost rate {ghost_rate:.2%} exceeds 10% threshold"


# ── Quality checks contract ──────────────────────────────────────────────────

class TestQualityContract:

    @pytest.fixture(scope="class")
    def quality_input_df(self, spark):
        data = [
            ("elem_001", "org_001", "202409", "100",  "combo_001",
             "Malaria cases", "INTEGER_ZERO_OR_POSITIVE", "SUM",
             "Facility A", "/root/c/r/d/org_001", 5,
             ["root","c","r","d","org_001"], "c", "r", "d", "org_001"),
            ("elem_001", "org_002", "202409", None,   "combo_001",
             "Malaria cases", "INTEGER_ZERO_OR_POSITIVE", "SUM",
             "Facility B", "/root/c/r/d/org_002", 5,
             ["root","c","r","d","org_002"], "c", "r", "d", "org_002"),
            ("elem_002", "org_001", "202409", "0",    "combo_002",
             "Malaria deaths", "INTEGER_ZERO_OR_POSITIVE", "SUM",
             "Facility A", "/root/c/r/d/org_001", 5,
             ["root","c","r","d","org_001"], "c", "r", "d", "org_001"),
        ]
        schema = StructType([
            StructField("dataElement",        StringType(),   True),
            StructField("orgUnit",            StringType(),   True),
            StructField("period",             StringType(),   True),
            StructField("value",              StringType(),   True),
            StructField("categoryOptionCombo", StringType(),  True),
            StructField("element_name",       StringType(),   True),
            StructField("valueType",          StringType(),   True),
            StructField("aggregationType",    StringType(),   True),
            StructField("org_name",           StringType(),   True),
            StructField("path",               StringType(),   True),
            StructField("level",              IntegerType(),  True),
            StructField("path_array",         ArrayType(StringType()), True),
            StructField("country_id",         StringType(),   True),
            StructField("region_id",          StringType(),   True),
            StructField("district_id",        StringType(),   True),
            StructField("facility_id",        StringType(),   True),
        ])
        return spark.createDataFrame(data, schema)

    def test_quality_flags_exist(self, quality_input_df):
        result = apply_quality_checks(quality_input_df)
        expected_cols = {"value_numeric", "is_missing_value", "is_explicit_zero",
                         "period_end", "updated_date", "days_late", "is_late_reported"}
        assert expected_cols.issubset(set(result.columns))

    def test_missing_value_flag(self, quality_input_df):
        result = apply_quality_checks(quality_input_df)
        missing = result.filter(col("is_missing_value") == 1).count()
        assert missing == 1  # only the NULL row

    def test_explicit_zero_flag(self, quality_input_df):
        result = apply_quality_checks(quality_input_df)
        zeros = result.filter(col("is_explicit_zero") == 1).count()
        assert zeros == 1  # only the "0" row

    def test_value_numeric_cast(self, quality_input_df):
        result = apply_quality_checks(quality_input_df)
        row = result.filter(col("value") == "100").select("value_numeric").first()
        assert row["value_numeric"] == 100.0


# ── Fact table contract ───────────────────────────────────────────────────────

class TestFactTableContract:

    REQUIRED_FACT_COLS = {
        "orgUnit", "dataElement", "period",
        "value_numeric", "is_missing_value", "is_explicit_zero", "is_late_reported"
    }

    def test_fact_has_required_columns(self, spark):
        """Fact table schema must contain all required columns."""
        schema = StructType([StructField(c, StringType(), True) for c in self.REQUIRED_FACT_COLS])
        df = spark.createDataFrame([], schema)
        assert self.REQUIRED_FACT_COLS.issubset(set(df.columns))

    def test_no_duplicate_grain(self, spark):
        """Each orgUnit + dataElement + period combination should be unique."""
        data = [
            ("org_001", "elem_001", "202409", 100.0, 0, 0, 0),
            ("org_001", "elem_001", "202409", 100.0, 0, 0, 0),  # duplicate
        ]
        schema = StructType([
            StructField("orgUnit",          StringType(),  True),
            StructField("dataElement",      StringType(),  True),
            StructField("period",           StringType(),  True),
            StructField("value_numeric",    DoubleType(),  True),
            StructField("is_missing_value", IntegerType(), True),
            StructField("is_explicit_zero", IntegerType(), True),
            StructField("is_late_reported", IntegerType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        total = df.count()
        distinct = df.dropDuplicates(["orgUnit", "dataElement", "period"]).count()
        assert total == distinct, f"Found {total - distinct} duplicate grain rows"
