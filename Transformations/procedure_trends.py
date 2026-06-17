from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer - Procedure Trends Analysis

@dp.materialized_view(
    name="hospital_gold_procedure_trends",
    comment="Time-series analysis of medical procedures including trends, seasonality, and growth rates",
    cluster_by=["year", "month", "procedure_code"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def gold_procedure_trends():
    procedures = spark.read.table("hospital_silver_procedures")
    encounters = spark.read.table("hospital_silver_encounters")
    
    return (
        procedures
        .join(encounters, "encounter_id", "inner")
        .withColumn("year", F.year("procedure_start"))
        .withColumn("month", F.month("procedure_start"))
        .withColumn("quarter", F.quarter("procedure_start"))
        .withColumn("date", F.to_date("procedure_start"))
        .groupBy(
            "year",
            "month",
            "quarter",
            "date",
            "procedure_code",
            "procedure_description",
            "encounter_class"
        )
        .agg(
            F.count("*").alias("procedure_count"),
            F.sum("base_cost").alias("total_procedure_cost"),
            F.avg("base_cost").alias("avg_procedure_cost"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.countDistinct(procedures.patient_id).alias("unique_patients"),
            F.countDistinct("organization_id").alias("unique_organizations"),
            F.min("procedure_start").alias("first_procedure_date"),
            F.max("procedure_start").alias("last_procedure_date")
        )
        .withColumn(
            "month_name",
            F.when(F.col("month") == 1, "January")
            .when(F.col("month") == 2, "February")
            .when(F.col("month") == 3, "March")
            .when(F.col("month") == 4, "April")
            .when(F.col("month") == 5, "May")
            .when(F.col("month") == 6, "June")
            .when(F.col("month") == 7, "July")
            .when(F.col("month") == 8, "August")
            .when(F.col("month") == 9, "September")
            .when(F.col("month") == 10, "October")
            .when(F.col("month") == 11, "November")
            .when(F.col("month") == 12, "December")
            .otherwise("Unknown")
        )
        .withColumn(
            "quarter_name",
            F.concat(F.lit("Q"), F.col("quarter"))
        )
        .withColumn(
            "avg_cost_per_patient",
            F.round(F.col("total_procedure_cost") / F.col("unique_patients"), 2)
        )
        .select(
            "date",
            "year",
            "month",
            "month_name",
            "quarter",
            "quarter_name",
            "procedure_code",
            "procedure_description",
            "encounter_class",
            "procedure_count",
            "unique_patients",
            "unique_organizations",
            "total_procedure_cost",
            "avg_procedure_cost",
            "avg_cost_per_patient",
            "avg_duration_minutes",
            "first_procedure_date",
            "last_procedure_date"
        )
        .orderBy(F.desc("date"), F.desc("procedure_count"))
    )
