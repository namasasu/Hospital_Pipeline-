from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze Layer - Ingest raw data from hospital_management catalog
# Uses processing_date configuration for idempotent ingestion tracking

def get_ingestion_date():
    """
    Get ingestion date from pipeline configuration or default to current date.
    Set 'processing_date' in pipeline configuration for idempotent runs.
    """
    try:
        processing_date = spark.conf.get("processing_date")
        return F.lit(processing_date).cast("date")
    except:
        return F.current_date()


@dp.table(
    name="hospital_bronze_patients",
    comment="Raw patient data ingested from hospital management system",
    cluster_by=["state", "ingestion_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_patients():
    return (
        spark.readStream.table("hospital_management.bronze.patients")
        .withColumn("ingestion_date", get_ingestion_date())
    )


@dp.table(
    name="hospital_bronze_organizations",
    comment="Raw organization/hospital data ingested from hospital management system",
    cluster_by=["state", "ingestion_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_organizations():
    return (
        spark.readStream.table("hospital_management.bronze.organizations")
        .withColumn("ingestion_date", get_ingestion_date())
    )


@dp.table(
    name="hospital_bronze_payers",
    comment="Raw insurance payer data ingested from hospital management system",
    cluster_by=["state_headquartered", "ingestion_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_payers():
    return (
        spark.readStream.table("hospital_management.bronze.payers")
        .withColumn("ingestion_date", get_ingestion_date())
    )


@dp.table(
    name="hospital_bronze_procedures",
    comment="Raw procedure data ingested from hospital management system",
    cluster_by=["patient", "ingestion_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_procedures():
    return (
        spark.readStream.table("hospital_management.bronze.procedures")
        .withColumn("ingestion_date", get_ingestion_date())
    )


@dp.table(
    name="hospital_bronze_encounters",
    comment="Raw patient encounter data ingested from hospital management system",
    cluster_by=["patient", "ingestion_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_encounters():
    return (
        spark.readStream.table("hospital_management.bronze.encounters")
        .withColumn("ingestion_date", get_ingestion_date())
    )
