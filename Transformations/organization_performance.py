from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer - Organization Performance Metrics

@dp.materialized_view(
    name="hospital_gold_organization_performance",
    comment="Hospital performance metrics including encounter volumes, revenue, patient outcomes, and efficiency",
    cluster_by=["state", "organization_name"]
)
def gold_organization_performance():
    encounters = spark.read.table("hospital_silver_encounters")
    organizations = spark.read.table("hospital_silver_organizations")
    patients = spark.read.table("hospital_silver_patient_demographics")
    procedures = spark.read.table("hospital_silver_procedures")
    
    # Calculate procedure counts by organization
    procedure_counts = (
        procedures
        .join(encounters, "encounter_id", "inner")
        .groupBy("organization_id")
        .agg(
            F.count("*").alias("total_procedures"),
            F.sum("base_cost").alias("total_procedure_revenue")
        )
    )
    
    # Calculate patient demographics by organization
    org_demographics = (
        encounters
        .join(patients, "patient_id", "inner")
        .groupBy("organization_id")
        .agg(
            F.countDistinct("patient_id").alias("unique_patients"),
            F.avg("age").alias("avg_patient_age"),
            F.count(F.when(F.col("is_deceased"), 1)).alias("deceased_patient_count")
        )
    )
    
    # Main organization metrics
    org_metrics = (
        encounters
        .groupBy("organization_id")
        .agg(
            F.count("encounter_id").alias("total_encounters"),
            F.sum("base_encounter_cost").alias("total_base_revenue"),
            F.sum("total_claim_cost").alias("total_claim_revenue"),
            F.sum("payer_coverage").alias("total_payer_payments"),
            F.avg("base_encounter_cost").alias("avg_encounter_cost"),
            F.avg("duration_hours").alias("avg_encounter_duration_hours"),
            F.countDistinct("encounter_class").alias("encounter_type_diversity"),
            # Encounter class distribution
            F.sum(F.when(F.col("encounter_class") == "emergency", 1).otherwise(0)).alias("emergency_encounters"),
            F.sum(F.when(F.col("encounter_class") == "inpatient", 1).otherwise(0)).alias("inpatient_encounters"),
            F.sum(F.when(F.col("encounter_class") == "outpatient", 1).otherwise(0)).alias("outpatient_encounters"),
            F.sum(F.when(F.col("encounter_class") == "ambulatory", 1).otherwise(0)).alias("ambulatory_encounters"),
            F.sum(F.when(F.col("encounter_class") == "wellness", 1).otherwise(0)).alias("wellness_encounters")
        )
        .withColumn(
            "emergency_rate",
            F.round((F.col("emergency_encounters") / F.col("total_encounters")) * 100, 2)
        )
        .withColumn(
            "collection_efficiency",
            F.round((F.col("total_payer_payments") / F.col("total_claim_revenue")) * 100, 2)
        )
    )
    
    return (
        organizations
        .join(org_metrics, "organization_id", "left")
        .join(org_demographics, "organization_id", "left")
        .join(procedure_counts, "organization_id", "left")
        .select(
            "organization_id",
            "organization_name",
            "city",
            "state",
            "total_encounters",
            "unique_patients",
            "total_procedures",
            "avg_patient_age",
            "deceased_patient_count",
            "total_base_revenue",
            "total_claim_revenue",
            "total_procedure_revenue",
            "total_payer_payments",
            "avg_encounter_cost",
            "avg_encounter_duration_hours",
            "collection_efficiency",
            "encounter_type_diversity",
            "emergency_encounters",
            "emergency_rate",
            "inpatient_encounters",
            "outpatient_encounters",
            "ambulatory_encounters",
            "wellness_encounters"
        )
        .orderBy(F.desc("total_claim_revenue"))
    )
