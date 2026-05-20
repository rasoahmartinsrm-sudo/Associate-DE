from pyspark.sql.functions import broadcast, col


def resolve_uids(values_df, metadata_df, program_df, org_df):

    resolved = (
        values_df
        .join(broadcast(metadata_df), col("dataElement") == metadata_df.id, "left")
        .join(broadcast(org_df), col("orgUnit") == org_df.id, "left")
        .select(
            values_df.dataElement,
            values_df.orgUnit,
            values_df.period,
            values_df.value,
            values_df.categoryOptionCombo,
            metadata_df.name.alias("element_name"),      
            metadata_df.valueType,
            metadata_df.aggregationType,
            org_df.name.alias("org_name"),               
            org_df.path,
            org_df.level
        )
    )

    ghost_df = resolved.filter("element_name IS NULL")  

    ghost_df.write.mode("overwrite").parquet(
        "output/quarantine/ghost_records"
    )

    clean_df = resolved.filter("element_name IS NOT NULL")

    return clean_df, ghost_df