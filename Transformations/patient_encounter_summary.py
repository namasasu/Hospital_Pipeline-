from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer - Patient encounter summary with aggregated metrics

@dp.materialized_view(
    name="hospital_gold_patient_encounter_summary",
    comment="Aggregated patient encounter metrics including total encounters, costs, and demographics",
    cluster_by=["state", "gender"]
)
def gold_patient_encounter_summary():
    encounters = spark.read.table("hospital_silver_encounters")
    patients = spark.read.table("hospital_silver_patients")
    organizations = spark.read.table("hospital_silver_organizations")
    
    return (
        encounters
        .join(patients, "patient_id", "inner")
        .join(organizations, "organization_id", "left")
        .groupBy(
            patients.patient_id,
            patients.first_name,
            patients.last_name,
            patients.gender,
            patients.state,
            patients.city
        )
        .agg(
            F.count(encounters.encounter_id).alias("total_encounters"),
            F.sum(encounters.base_encounter_cost).alias("total_base_cost"),
            F.sum(encounters.total_claim_cost).alias("total_claim_cost"),
            F.sum(encounters.payer_coverage).alias("total_payer_coverage"),
            F.avg(encounters.duration_hours).alias("avg_encounter_duration_hours"),
            F.min(encounters.encounter_start).alias("first_encounter_date"),
            F.max(encounters.encounter_start).alias("last_encounter_date"),
            F.collect_set(encounters.encounter_class).alias("encounter_types")
        )
    )
