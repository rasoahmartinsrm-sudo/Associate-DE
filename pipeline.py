
import os
import subprocess

JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
HADOOP_HOME = r"C:\hadoop"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = JAVA_HOME + r"\bin;" + HADOOP_HOME + r"\bin;" + os.environ.get("PATH", "")

# imports
from pyspark.sql import SparkSession
from models.ingest import load_all_data
from models.metadata import resolve_uids
from models.hierarchy import resolve_hierarchy
from models.quality import apply_quality_checks
from models.warehouse import build_dimensions, build_fact_table
from models.analytics import run_analytics
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("Py")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.local.dir", r"C:\tmp\spark")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def main():

    logger.info("Loading data...")
    try:
        metadata_df, org_df, program_df, values_df = load_all_data(spark)
        logger.info(f"Loaded rows — metadata: {metadata_df.count()}, org: {org_df.count()}, program: {program_df.count()}, values: {values_df.count()}")
    except Exception as e:
        logger.error(f"Failed at load_all_data: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Resolving metadata UIDs...")
    try:
        resolved_df, ghost_df = resolve_uids(values_df, metadata_df, program_df, org_df)
        ghost_rate = ghost_df.count() / values_df.count()
        logger.info(f"Ghost rate: {ghost_rate}")
        if ghost_rate > 0.10:
            logger.error("Too many unresolved UIDs")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed at resolve_uids: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Resolving hierarchy...")
    try:
        hierarchy_df = resolve_hierarchy(resolved_df, org_df)
    except Exception as e:
        logger.error(f"Failed at resolve_hierarchy: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Applying data quality checks...")
    try:
        quality_df = apply_quality_checks(hierarchy_df)
    except Exception as e:
        logger.error(f"Failed at apply_quality_checks: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Building dimensions...")
    try:
        dimensions = build_dimensions(quality_df)
    except Exception as e:
        logger.error(f"Failed at build_dimensions: {e}", exc_info=True)
        sys.exit(1)
    logger.info("Building fact table...")
    try:
        fact_df = build_fact_table(quality_df)
    except Exception as e:
        logger.error(f"Failed at build_fact_table: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Running analytics...")
    try:
        run_analytics(fact_df)
    except Exception as e:
        logger.error(f"Failed at run_analytics: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()

