from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Layer - Patient Demographics Enrichment

@dp.table(
    name="hospital_silver_patient_demographics",
    comment="Enriched patient demographics with calculated age, age groups, and demographic segments",
    cluster_by=["age_group", "state"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def silver_patient_demographics():
    return (
        spark.readStream.table("hospital_silver_patients")
        .withColumn(
            "age",
            F.when(F.col("death_date").isNotNull(),
                   F.floor(F.months_between(F.col("death_date"), F.col("birth_date")) / 12))
            .otherwise(F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12))
        )
        .withColumn(
            "age_group",
            F.when(F.col("age") < 18, "0-17 (Pediatric)")
            .when((F.col("age") >= 18) & (F.col("age") < 30), "18-29 (Young Adult)")
            .when((F.col("age") >= 30) & (F.col("age") < 45), "30-44 (Adult)")
            .when((F.col("age") >= 45) & (F.col("age") < 65), "45-64 (Middle Age)")
            .when(F.col("age") >= 65, "65+ (Senior)")
            .otherwise("Unknown")
        )
        .withColumn(
            "is_deceased",
            F.when(F.col("death_date").isNotNull(), True).otherwise(False)
        )
        .withColumn(
            "demographic_segment",
            F.concat_ws("-", F.col("age_group"), F.col("gender"), F.col("race"))
        )
        .select(
            "patient_id",
            "first_name",
            "last_name",
            "birth_date",
            "death_date",
            "age",
            "age_group",
            "gender",
            "race",
            "ethnicity",
            "marital_status",
            "is_deceased",
            "demographic_segment",
            "address",
            "city",
            "state",
            "zip_code",
            "latitude",
            "longitude"
        )
    )
