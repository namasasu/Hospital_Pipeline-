from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Gold Layer - Patient Cost Analysis

@dp.materialized_view(
    name="hospital_gold_patient_cost_analysis",
    comment="Patient-level cost analysis with total healthcare spend, out-of-pocket costs, and high-cost patient identification",
    cluster_by=["age_group", "state"]
)
def gold_patient_cost_analysis():
    encounters = spark.read.table("hospital_silver_encounters")
    patients = spark.read.table("hospital_silver_patient_demographics")
    procedures = spark.read.table("hospital_silver_procedures")
    
    # Calculate procedure costs by patient
    procedure_costs = (
        procedures
        .groupBy("patient_id")
        .agg(
            F.sum("base_cost").alias("total_procedure_cost"),
            F.count("*").alias("procedure_count")
        )
    )
    
    # Calculate encounter costs by patient
    patient_costs = (
        encounters
        .groupBy("patient_id")
        .agg(
            F.sum("base_encounter_cost").alias("total_encounter_cost"),
            F.sum("total_claim_cost").alias("total_claim_cost"),
            F.sum("payer_coverage").alias("total_payer_coverage"),
            F.count("encounter_id").alias("encounter_count")
        )
        .withColumn(
            "out_of_pocket_cost",
            F.col("total_claim_cost") - F.col("total_payer_coverage")
        )
        .withColumn(
            "coverage_percentage",
            F.round((F.col("total_payer_coverage") / F.col("total_claim_cost")) * 100, 2)
        )
    )
    
    # Join with demographics
    result = (
        patients
        .join(patient_costs, "patient_id", "left")
        .join(procedure_costs, "patient_id", "left")
        .withColumn(
            "total_healthcare_spend",
            F.coalesce(F.col("total_claim_cost"), F.lit(0)) + F.coalesce(F.col("total_procedure_cost"), F.lit(0))
        )
    )
    
    # Rank patients by cost within age group
    window_spec = Window.partitionBy("age_group").orderBy(F.desc("total_healthcare_spend"))
    
    return (
        result
        .withColumn("cost_rank_in_age_group", F.row_number().over(window_spec))
        .withColumn(
            "high_cost_patient",
            F.when(F.col("total_healthcare_spend") > 50000, True).otherwise(False)
        )
        .select(
            "patient_id",
            "first_name",
            "last_name",
            "age",
            "age_group",
            "gender",
            "race",
            "state",
            "city",
            "is_deceased",
            "encounter_count",
            "procedure_count",
            "total_encounter_cost",
            "total_procedure_cost",
            "total_healthcare_spend",
            "total_payer_coverage",
            "out_of_pocket_cost",
            "coverage_percentage",
            "cost_rank_in_age_group",
            "high_cost_patient"
        )
    )
