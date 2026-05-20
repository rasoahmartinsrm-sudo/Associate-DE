from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


def apply_quality_checks(df):

    quality_df = (
        df
        .withColumn(
            "value_numeric",
            col("value").cast(DoubleType())
        )
        .withColumn(
            "is_missing_value",
            when(col("value").isNull(), 1).otherwise(0)
        )
        .withColumn(
            "is_explicit_zero",
            when(col("value_numeric") == 0, 1).otherwise(0)
        )
        .withColumn(
            "period_end",
            to_date(col("period"), "yyyyMM")  
        )
        .withColumn(
            "updated_date",
            to_date(col("period"), "yyyyMM") 
        )
        .withColumn(
            "days_late",
            datediff(
                col("updated_date"),
                col("period_end")
            )
        )
        .withColumn(
            "is_late_reported",
            when(col("days_late") > 60, 1).otherwise(0) 
        )
    )

    return quality_df