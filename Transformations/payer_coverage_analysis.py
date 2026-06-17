from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer - Payer Coverage Analysis

@dp.materialized_view(
    name="hospital_gold_payer_coverage_analysis",
    comment="Insurance payer coverage analysis including coverage rates, denied claims, and out-of-pocket costs",
    cluster_by=["payer_name"]
)
def gold_payer_coverage_analysis():
    encounters = spark.read.table("hospital_silver_encounters")
    payers = spark.read.table("hospital_silver_payers")
    patients = spark.read.table("hospital_silver_patient_demographics")
    
    return (
        encounters
        .join(payers, "payer_id", "inner")
        .join(patients, "patient_id", "left")
        .groupBy(
            payers.payer_id,
            payers.payer_name,
            payers.state,
            payers.city
        )
        .agg(
            F.count("encounter_id").alias("total_encounters"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum("base_encounter_cost").alias("total_base_cost"),
            F.sum("total_claim_cost").alias("total_claim_cost"),
            F.sum("payer_coverage").alias("total_payer_coverage"),
            F.avg("payer_coverage").alias("avg_payer_coverage_per_encounter"),
            F.avg("total_claim_cost").alias("avg_claim_cost"),
            # Calculate coverage metrics
            F.sum(F.col("total_claim_cost") - F.col("payer_coverage")).alias("total_out_of_pocket"),
            F.avg(F.col("total_claim_cost") - F.col("payer_coverage")).alias("avg_out_of_pocket_per_encounter"),
            # Encounter class breakdown
            F.sum(F.when(F.col("encounter_class") == "emergency", F.col("payer_coverage")).otherwise(0)).alias("emergency_coverage"),
            F.sum(F.when(F.col("encounter_class") == "inpatient", F.col("payer_coverage")).otherwise(0)).alias("inpatient_coverage"),
            F.sum(F.when(F.col("encounter_class") == "outpatient", F.col("payer_coverage")).otherwise(0)).alias("outpatient_coverage"),
            # Demographics
            F.avg(patients.age).alias("avg_patient_age"),
            F.countDistinct(patients.age_group).alias("age_group_diversity")
        )
        .withColumn(
            "coverage_rate",
            F.round((F.col("total_payer_coverage") / F.col("total_claim_cost")) * 100, 2)
        )
        .withColumn(
            "out_of_pocket_rate",
            F.round((F.col("total_out_of_pocket") / F.col("total_claim_cost")) * 100, 2)
        )
        .withColumn(
            "avg_coverage_per_patient",
            F.round(F.col("total_payer_coverage") / F.col("unique_patients"), 2)
        )
        .select(
            "payer_id",
            "payer_name",
            "state",
            "city",
            "total_encounters",
            "unique_patients",
            "total_claim_cost",
            "total_payer_coverage",
            "total_out_of_pocket",
            "coverage_rate",
            "out_of_pocket_rate",
            "avg_claim_cost",
            "avg_payer_coverage_per_encounter",
            "avg_out_of_pocket_per_encounter",
            "avg_coverage_per_patient",
            "emergency_coverage",
            "inpatient_coverage",
            "outpatient_coverage",
            "avg_patient_age",
            "age_group_diversity"
        )
        .orderBy(F.desc("total_payer_coverage"))
    )
