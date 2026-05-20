import os
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col


metadata_schema = StructType([
    StructField("dataElements", ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("valueType", StringType()),
            StructField("aggregationType", StringType())
        ])
    ))
])


org_schema = StructType([
    StructField("organisationUnits", ArrayType(
        StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("path", StringType()),
            StructField("level", IntegerType())
        ])
    ))
])


def load_all_data(spark):

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")

    metadata_path = os.path.join(DATA_DIR, "metadata.json")
    org_path = os.path.join(DATA_DIR, "org_units.json")
    program_path = os.path.join(DATA_DIR, "programs.json")
    values_path = os.path.join(DATA_DIR, "data_values.json")

    print("Loading from:")
    print(metadata_path)
    print(org_path)
    print(program_path)
    print(values_path)

    metadata_raw = spark.read.schema(metadata_schema).json(metadata_path)
    org_raw = spark.read.schema(org_schema).json(org_path)

    metadata_df = (
        metadata_raw
        .withColumn("dataElement", explode("dataElements"))
        .select("dataElement.*")
    )

    org_df = (
        org_raw
        .withColumn("org", explode("organisationUnits"))
        .select("org.*")
    )

    program_df = spark.read.json(program_path)

    # Explode the nested dataValues array
    values_raw = spark.read.json(values_path)
    values_df = (
        values_raw
        .withColumn("dv", explode("dataValues"))
        .select(
            col("dv.dataElement").alias("dataElement"),
            col("dv.orgUnit").alias("orgUnit"),
            col("dv.period").alias("period"),
            col("dv.value").alias("value"),
            col("dv.categoryOptionCombo").alias("categoryOptionCombo")
        )
    )

    return metadata_df, org_df, program_df, values_df