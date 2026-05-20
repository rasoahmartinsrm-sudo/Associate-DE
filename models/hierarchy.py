from pyspark.sql.functions import split


def resolve_hierarchy(df, org_df):

    hierarchy = (
        df
        .withColumn(
            "path_array",
            split("path", "/")
        )
    )

    hierarchy = (
        hierarchy
        .withColumn("country_id", hierarchy.path_array[1])
        .withColumn("region_id", hierarchy.path_array[2])
        .withColumn("district_id", hierarchy.path_array[3])
        .withColumn("facility_id", hierarchy.path_array[4])
    )

    return hierarchy