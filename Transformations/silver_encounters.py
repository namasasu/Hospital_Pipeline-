from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - Cleaned and standardized encounter data

@dp.table(
    name="hospital_silver_encounters",
    comment="Cleaned patient encounter data with standardized fields and data quality checks",
    cluster_by=["patient_id", "encounter_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect("valid_encounter_id", "encounter_id IS NOT NULL")
@dp.expect("valid_patient", "patient_id IS NOT NULL")
@dp.expect_or_drop("valid_cost", "base_encounter_cost >= 0")
def silver_encounters():
    return (
        spark.readStream.table("hospital_bronze_encounters")
        .select(
            F.col("Id").alias("encounter_id"),
            F.to_timestamp("START").alias("encounter_start"),
            F.to_timestamp("STOP").alias("encounter_stop"),
            F.col("PATIENT").alias("patient_id"),
            F.col("ORGANIZATION").alias("organization_id"),
            F.col("PAYER").alias("payer_id"),
            F.col("ENCOUNTERCLASS").alias("encounter_class"),
            F.col("CODE").alias("encounter_code"),
            F.col("DESCRIPTION").alias("encounter_description"),
            F.col("BASE_ENCOUNTER_COST").cast("decimal(10,2)").alias("base_encounter_cost"),
            F.col("TOTAL_CLAIM_COST").cast("decimal(10,2)").alias("total_claim_cost"),
            F.col("PAYER_COVERAGE").cast("decimal(10,2)").alias("payer_coverage"),
            F.col("REASONCODE").alias("reason_code"),
            F.col("REASONDESCRIPTION").alias("reason_description"),
            # Calculate encounter duration in hours
            F.round(
                (F.unix_timestamp("STOP") - F.unix_timestamp("START")) / 3600, 2
            ).alias("duration_hours"),
            # Add date fields for clustering
            F.to_date(F.to_timestamp("START")).alias("encounter_date")
        )
    )
