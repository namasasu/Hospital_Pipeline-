from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - Encounter-Procedure Bridge Table

@dp.materialized_view(
    name="hospital_silver_encounter_procedures_enriched",
    comment="Enriched view joining encounters with all related procedures for comprehensive analysis"
)
def silver_encounter_procedures():
    encounters = spark.read.table("hospital_silver_encounters")
    procedures = spark.read.table("hospital_silver_procedures")
    
    return (
        encounters
        .join(procedures, "encounter_id", "left")
        .select(
            # Encounter fields
            encounters.encounter_id,
            encounters.patient_id,
            encounters.organization_id,
            encounters.payer_id,
            encounters.encounter_start,
            encounters.encounter_stop,
            encounters.encounter_class,
            encounters.encounter_description,
            encounters.base_encounter_cost,
            encounters.total_claim_cost,
            encounters.payer_coverage,
            encounters.duration_hours.alias("encounter_duration_hours"),
            # Procedure fields (null if no procedures)
            procedures.procedure_code,
            procedures.procedure_description,
            procedures.procedure_start,
            procedures.procedure_stop,
            procedures.base_cost.alias("procedure_cost"),
            procedures.duration_minutes.alias("procedure_duration_minutes"),
            procedures.reason_code.alias("procedure_reason_code"),
            procedures.reason_description.alias("procedure_reason_description")
        )
    )
