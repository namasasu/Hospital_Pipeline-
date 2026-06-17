from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Gold Layer - Patient Journey Analysis

@dp.materialized_view(
    name="hospital_gold_patient_journey_analysis",
    comment="Patient care pathway analysis including encounter sequences, readmission rates, and care continuity",
    cluster_by=["age_group", "state"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_patient_journey_analysis():
    encounters = spark.read.table("hospital_silver_encounters")
    patients = spark.read.table("hospital_silver_patient_demographics")
    
    # Create window for patient encounter sequencing
    patient_window = Window.partitionBy("patient_id").orderBy("encounter_start")
    
    # Calculate encounter sequences and intervals
    encounter_sequence = (
        encounters
        .withColumn("encounter_number", F.row_number().over(patient_window))
        .withColumn("previous_encounter_stop", F.lag("encounter_stop").over(patient_window))
        .withColumn(
            "days_since_last_encounter",
            F.when(
                F.col("previous_encounter_stop").isNotNull(),
                F.datediff(F.col("encounter_start"), F.col("previous_encounter_stop"))
            ).otherwise(None)
        )
        .withColumn(
            "is_readmission",
            F.when(
                (F.col("days_since_last_encounter").isNotNull()) & 
                (F.col("days_since_last_encounter") <= 30),
                True
            ).otherwise(False)
        )
        .withColumn(
            "is_followup",
            F.when(
                (F.col("days_since_last_encounter").isNotNull()) & 
                (F.col("days_since_last_encounter") > 30) &
                (F.col("days_since_last_encounter") <= 90),
                True
            ).otherwise(False)
        )
    )
    
    # Aggregate patient journey metrics
    patient_journey = (
        encounter_sequence
        .groupBy("patient_id")
        .agg(
            F.count("encounter_id").alias("total_encounters"),
            F.sum(F.when(F.col("is_readmission"), 1).otherwise(0)).alias("readmission_count"),
            F.sum(F.when(F.col("is_followup"), 1).otherwise(0)).alias("followup_count"),
            F.avg("days_since_last_encounter").alias("avg_days_between_encounters"),
            F.min("encounter_start").alias("first_encounter_date"),
            F.max("encounter_start").alias("last_encounter_date"),
            F.datediff(F.max("encounter_start"), F.min("encounter_start")).alias("care_span_days"),
            F.sum("base_encounter_cost").alias("total_encounter_cost"),
            F.sum("duration_hours").alias("total_care_hours"),
            F.countDistinct("organization_id").alias("unique_organizations_visited"),
            F.countDistinct("encounter_class").alias("encounter_type_diversity"),
            F.collect_list("encounter_class").alias("encounter_sequence_types"),
            # Calculate emergency encounters
            F.sum(F.when(F.col("encounter_class") == "emergency", 1).otherwise(0)).alias("emergency_visit_count"),
            # Calculate longest gap
            F.max("days_since_last_encounter").alias("longest_gap_days")
        )
        .withColumn(
            "readmission_rate",
            F.round((F.col("readmission_count") / F.col("total_encounters")) * 100, 2)
        )
        .withColumn(
            "avg_encounters_per_year",
            F.when(F.col("care_span_days") > 0,
                F.round(F.col("total_encounters") / (F.col("care_span_days") / 365.25), 2)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "care_continuity_score",
            F.round(
                100 - F.least(
                    (F.col("unique_organizations_visited") - 1) * 10,
                    F.lit(50)
                ),
                2
            )
        )
    )
    
    return (
        patient_journey
        .join(patients, "patient_id", "left")
        .select(
            "patient_id",
            "first_name",
            "last_name",
            "age",
            "age_group",
            "gender",
            "state",
            "city",
            "is_deceased",
            "total_encounters",
            "readmission_count",
            "readmission_rate",
            "followup_count",
            "emergency_visit_count",
            "avg_days_between_encounters",
            "longest_gap_days",
            "first_encounter_date",
            "last_encounter_date",
            "care_span_days",
            "avg_encounters_per_year",
            "total_encounter_cost",
            "total_care_hours",
            "unique_organizations_visited",
            "encounter_type_diversity",
            "care_continuity_score",
            "encounter_sequence_types"
        )
    )
