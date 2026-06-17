from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - Cleaned and standardized patient data

@dp.table(
    name="hospital_silver_patients",
    comment="Cleaned patient data with standardized fields and data quality checks",
    cluster_by=["patient_id", "state"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect("valid_patient_id", "patient_id IS NOT NULL")
@dp.expect("valid_birthdate", "birth_date IS NOT NULL")
def silver_patients():
    return (
        spark.readStream.table("hospital_bronze_patients")
        .select(
            F.col("Id").alias("patient_id"),
            F.to_date("BIRTHDATE").alias("birth_date"),
            F.to_date("DEATHDATE").alias("death_date"),
            F.initcap(F.col("FIRST")).alias("first_name"),
            F.initcap(F.col("LAST")).alias("last_name"),
            F.upper(F.col("GENDER")).alias("gender"),
            F.col("RACE").alias("race"),
            F.col("ETHNICITY").alias("ethnicity"),
            F.col("MARITAL").alias("marital_status"),
            F.col("ADDRESS").alias("address"),
            F.col("CITY").alias("city"),
            F.col("STATE").alias("state"),
            F.col("ZIP").alias("zip_code"),
            F.col("LAT").cast("double").alias("latitude"),
            F.col("LON").cast("double").alias("longitude")
        )
    )
