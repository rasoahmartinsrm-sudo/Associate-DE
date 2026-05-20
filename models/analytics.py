from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import count, countDistinct


def run_analytics(df):

    window_spec = (
        Window
        .partitionBy("orgUnit")
        .orderBy("period")
    )

    analytics_df = (
        df
        .withColumn(
            "previous_month",
            lag("value_numeric").over(window_spec)
        )
        .withColumn(
            "month_over_month_change",
            (
                col("value_numeric") - col("previous_month")
            ) / nullif(col("previous_month"), lit(0))  
        )
    )

    rolling_window = (
        Window
        .partitionBy("orgUnit")
        .orderBy("period")
        .rowsBetween(-2, 0)
    )

    analytics_df = (
        analytics_df
        .withColumn(
            "rolling_avg_3m",
            avg("value_numeric").over(rolling_window)
        )
    )

    reporting_rate = (
        df.groupBy("period")
        .agg(
            (
                count("orgUnit") /
                nullif(countDistinct("orgUnit"), lit(0))
            ).alias("reporting_rate")
        )
    )

    analytics_df.write.mode("overwrite").parquet(
        "output/analytics"
    )

    reporting_rate.write.mode("overwrite").csv(
        "output/reporting_rate",
        header=True
    )