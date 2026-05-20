def build_dimensions(df):

    dim_org = (
        df.select(
            "orgUnit",
            "org_name"     
        ).dropDuplicates()
    )

    dim_period = (
        df.select(
            "period"
        ).dropDuplicates()
    )

    dim_data_element = (
        df.select(
            "dataElement",
            "element_name", 
            "valueType"
        ).dropDuplicates()
    )

    dim_org.write.mode("overwrite").parquet("output/dim_org")
    dim_period.write.mode("overwrite").parquet("output/dim_period")
    dim_data_element.write.mode("overwrite").parquet("output/dim_data_element")

    return {
        "org": dim_org,
        "period": dim_period,
        "data_element": dim_data_element
    }


def build_fact_table(df):

    fact_df = (
        df.select(
            "orgUnit",
            "dataElement",
            "period",
            "value_numeric",
            "is_missing_value",
            "is_explicit_zero",
            "is_late_reported"
        )
    )

    fact_df.write \
        .mode("overwrite") \
        .partitionBy("period") \
        .parquet("output/fact_service_delivery")

    return fact_df