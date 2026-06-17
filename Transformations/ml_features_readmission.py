from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Gold Layer - ML Feature Engineering for Readmission Risk Prediction

@dp.materialized_view(
    name="hospital_ml_features_readmission",
    comment="Feature table for 30-day readmission risk prediction model. Combines patient demographics, encounter history, and healthcare utilization patterns.",
    cluster_by=["patient_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def ml_features_readmission():
    """
    Feature engineering for readmission prediction model.
    
    Features include:
    - Patient demographics (age, gender, race)
    - Historical encounter patterns (frequency, types, costs)
    - Readmission history (rate, count)
    - Emergency utilization
    - Care continuity metrics
    - Temporal features (days since last encounter)
    
    This table serves as input for ML model training and batch scoring.
    """
    
    # Load source tables
    demographics = spark.read.table("hospital_silver_patient_demographics").alias("demo")
    journey = spark.read.table("hospital_gold_patient_journey_analysis").alias("journey")
    encounters = spark.read.table("hospital_silver_encounters").alias("enc")
    
    # Calculate recent encounter features (last 90 days window)
    recent_window = Window.partitionBy("patient_id").orderBy(F.desc("encounter_start"))
    
    recent_encounters = (
        encounters
        .withColumn("encounter_rank", F.row_number().over(recent_window))
        .filter(F.col("encounter_rank") <= 5)  # Last 5 encounters
        .groupBy("patient_id")
        .agg(
            F.avg("duration_hours").alias("recent_avg_duration_hours"),
            F.sum("base_encounter_cost").alias("recent_total_cost"),
            F.sum(F.when(F.col("encounter_class") == "emergency", 1).otherwise(0)).alias("recent_emergency_count"),
            F.countDistinct("organization_id").alias("recent_unique_facilities")
        )
        .alias("recent")
    )
    
    # Combine all features
    return (
        demographics
        .join(journey, demographics.patient_id == journey.patient_id, "inner")
        .join(recent_encounters, demographics.patient_id == recent_encounters.patient_id, "left")
        .select(
            # Identifier
            demographics.patient_id,
            
            # Demographic features
            F.col("demo.age").alias("feature_age"),
            F.when(F.col("demo.gender") == "M", 1).otherwise(0).alias("feature_is_male"),
            F.col("demo.age_group").alias("feature_age_group"),
            F.col("demo.is_deceased").cast("int").alias("feature_is_deceased"),
            
            # Encounter history features
            F.col("journey.total_encounters").alias("feature_total_encounters"),
            F.col("journey.readmission_count").alias("feature_readmission_count"),
            F.col("journey.readmission_rate").alias("feature_readmission_rate"),
            F.col("journey.emergency_visit_count").alias("feature_emergency_visits"),
            F.col("journey.avg_days_between_encounters").alias("feature_avg_days_between_encounters"),
            F.col("journey.longest_gap_days").alias("feature_longest_gap_days"),
            
            # Care continuity features
            F.col("journey.unique_organizations_visited").alias("feature_facility_fragmentation"),
            F.col("journey.care_continuity_score").alias("feature_care_continuity_score"),
            F.col("journey.encounter_type_diversity").alias("feature_encounter_diversity"),
            
            # Cost features
            F.col("journey.total_encounter_cost").alias("feature_total_cost"),
            F.round(F.col("journey.total_encounter_cost") / F.col("journey.total_encounters"), 2).alias("feature_avg_cost_per_encounter"),
            
            # Recent activity features (last 5 encounters)
            F.coalesce(F.col("recent.recent_avg_duration_hours"), F.lit(0)).alias("feature_recent_avg_duration"),
            F.coalesce(F.col("recent.recent_total_cost"), F.lit(0)).alias("feature_recent_total_cost"),
            F.coalesce(F.col("recent.recent_emergency_count"), F.lit(0)).alias("feature_recent_emergency_count"),
            F.coalesce(F.col("recent.recent_unique_facilities"), F.lit(0)).alias("feature_recent_facility_changes"),
            
            # Temporal features
            F.datediff(F.current_date(), F.col("journey.last_encounter_date")).alias("feature_days_since_last_encounter"),
            F.col("journey.care_span_days").alias("feature_care_span_days"),
            
            # Derived risk indicators
            F.when(F.col("journey.readmission_rate") > 40, 1).otherwise(0).alias("feature_high_readmission_history"),
            F.when(F.col("journey.emergency_visit_count") >= 3, 1).otherwise(0).alias("feature_frequent_emergency_user"),
            F.when(F.col("journey.unique_organizations_visited") > 2, 1).otherwise(0).alias("feature_multi_facility_patient"),
            
            # Metadata for tracking
            F.col("demo.state").alias("patient_state"),
            F.col("demo.gender").alias("patient_gender")
        )
    )
