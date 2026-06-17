from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - Cleaned and standardized procedure data

@dp.table(
    name="hospital_silver_procedures",
    comment="Cleaned procedure data with standardized fields and data quality checks",
    cluster_by=["patient_id", "procedure_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect("valid_patient", "patient_id IS NOT NULL")
@dp.expect("valid_start", "procedure_start IS NOT NULL")
@dp.expect_or_drop("valid_cost", "base_cost >= 0")
def silver_procedures():
    return (
        spark.readStream.table("hospital_bronze_procedures")
        .select(
            F.to_timestamp("START").alias("procedure_start"),
            F.to_timestamp("STOP").alias("procedure_stop"),
            F.col("PATIENT").alias("patient_id"),
            F.col("ENCOUNTER").alias("encounter_id"),
            F.col("CODE").alias("procedure_code"),
            F.col("DESCRIPTION").alias("procedure_description"),
            F.col("BASE_COST").cast("decimal(10,2)").alias("base_cost"),
            F.col("REASONCODE").alias("reason_code"),
            F.col("REASONDESCRIPTION").alias("reason_description"),
            # Calculate procedure duration in minutes
            F.round(
                (F.unix_timestamp("STOP") - F.unix_timestamp("START")) / 60, 2
            ).alias("duration_minutes"),
            # Add date field for clustering
            F.to_date(F.to_timestamp("START")).alias("procedure_date")
        )
    )
