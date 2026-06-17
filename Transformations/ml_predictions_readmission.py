from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold Layer - ML Model Inference for Readmission Risk Prediction

"""
ML MODEL INTEGRATION PATTERNS FOR SPARK DECLARATIVE PIPELINES

This file demonstrates three approaches to integrate ML models:

1. MOCK MODEL (Current): Rule-based logic for demonstration
2. MLFLOW MODEL (Recommended): Load registered models from MLflow
3. CUSTOM UDF: Apply custom Python functions as UDFs

To use a real MLflow model, uncomment the MLflow section and register your model.
"""

# ============================================================================
# APPROACH 1: MOCK MODEL (Current Implementation)
# ============================================================================
# Simple rule-based scoring for demonstration purposes
# Replace with real ML model for production use

def calculate_mock_risk_score(
    readmission_rate,
    emergency_visits, 
    age,
    total_encounters,
    care_continuity_score
):
    """
    Mock risk scoring logic based on business rules.
    Returns a risk score between 0.0 (low risk) and 1.0 (high risk).
    
    In production, replace this with MLflow model inference.
    """
    return (
        F.when(
            # High risk criteria
            (F.col(readmission_rate) > 40) | 
            (F.col(emergency_visits) >= 5) |
            ((F.col(age) > 75) & (F.col(total_encounters) > 20)),
            F.lit(0.85)  # High risk score
        )
        .when(
            # Medium risk criteria
            (F.col(readmission_rate).between(20, 40)) |
            (F.col(emergency_visits).between(2, 4)) |
            (F.col(care_continuity_score) < 80),
            F.lit(0.55)  # Medium risk score
        )
        .otherwise(F.lit(0.25))  # Low risk score
    )


# ============================================================================
# APPROACH 2: MLFLOW MODEL (Production Pattern)
# ============================================================================
# Uncomment and configure when you have a registered MLflow model

"""
import mlflow

# Load registered model from MLflow Model Registry
readmission_model_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/hospital_readmission_risk/Production",  # Change to your model name
    result_type="double"
)

# Alternative: Load by specific version
# readmission_model_udf = mlflow.pyfunc.spark_udf(
#     spark,
#     model_uri="models:/hospital_readmission_risk/3",
#     result_type="double"
# )

# Alternative: Load from experiment run
# readmission_model_udf = mlflow.pyfunc.spark_udf(
#     spark,
#     model_uri="runs:/abc123def456/model",
#     result_type="double"
# )
"""


# ============================================================================
# PREDICTION TABLE
# ============================================================================

@dp.materialized_view(
    name="hospital_ml_predictions_readmission",
    comment="30-day readmission risk predictions using ML model. Scores all patients with risk probability (0-1) and risk tier classification.",
    cluster_by=["risk_tier", "patient_state"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def ml_predictions_readmission():
    """
    Apply ML model to predict 30-day readmission risk for all patients.
    
    Output schema:
    - patient_id: Unique patient identifier
    - readmission_risk_score: Probability of readmission (0.0 - 1.0)
    - risk_tier: Classification (Low/Medium/High Risk)
    - top_risk_factors: Contributing factors for explainability
    - prediction_date: When the prediction was generated
    - model_version: Model identifier for tracking
    """
    
    features = spark.read.table("hospital_ml_features_readmission")
    
    # ========================================================================
    # APPLY MODEL: Switch between mock and MLflow based on your setup
    # ========================================================================
    
    # Current: Using mock model
    predictions = features.withColumn(
        "readmission_risk_score",
        calculate_mock_risk_score(
            "feature_readmission_rate",
            "feature_emergency_visits",
            "feature_age",
            "feature_total_encounters",
            "feature_care_continuity_score"
        )
    )
    
    # To use MLflow model instead, replace above with:
    # predictions = features.withColumn(
    #     "readmission_risk_score",
    #     readmission_model_udf(F.struct(
    #         "feature_age",
    #         "feature_is_male",
    #         "feature_total_encounters",
    #         "feature_readmission_rate",
    #         "feature_emergency_visits",
    #         "feature_care_continuity_score",
    #         "feature_avg_cost_per_encounter",
    #         "feature_days_since_last_encounter",
    #         "feature_high_readmission_history",
    #         "feature_frequent_emergency_user"
    #     ))
    # )
    
    # ========================================================================
    # POST-PROCESSING: Add risk tiers and metadata
    # ========================================================================
    
    return (
        predictions
        .withColumn(
            "risk_tier",
            F.when(F.col("readmission_risk_score") >= 0.7, "High Risk")
            .when(F.col("readmission_risk_score") >= 0.4, "Medium Risk")
            .otherwise("Low Risk")
        )
        .withColumn(
            "risk_tier_numeric",
            F.when(F.col("readmission_risk_score") >= 0.7, 3)
            .when(F.col("readmission_risk_score") >= 0.4, 2)
            .otherwise(1)
        )
        .withColumn(
            "top_risk_factors",
            F.array(
                F.when(F.col("feature_high_readmission_history") == 1, F.lit("High Readmission History")),
                F.when(F.col("feature_frequent_emergency_user") == 1, F.lit("Frequent Emergency User")),
                F.when(F.col("feature_multi_facility_patient") == 1, F.lit("Multiple Facilities")),
                F.when(F.col("feature_age") > 75, F.lit("Advanced Age (75+)")),
                F.when(F.col("feature_care_continuity_score") < 70, F.lit("Low Care Continuity"))
            )
        )
        .withColumn(
            "top_risk_factors",
            F.array_except(F.col("top_risk_factors"), F.array(F.lit(None)))
        )
        .withColumn("prediction_date", F.current_date())
        .withColumn("model_version", F.lit("mock_v1.0"))  # Change to actual model version
        .select(
            "patient_id",
            "readmission_risk_score",
            "risk_tier",
            "risk_tier_numeric",
            "top_risk_factors",
            
            # Key features for explainability
            "feature_age",
            "feature_total_encounters",
            "feature_readmission_rate",
            "feature_emergency_visits",
            "feature_care_continuity_score",
            "feature_days_since_last_encounter",
            
            # Metadata
            "patient_state",
            "patient_gender",
            "prediction_date",
            "model_version"
        )
        .orderBy(F.desc("readmission_risk_score"))
    )


# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================

"""
STEP 1: TRAIN YOUR MODEL (Outside Pipeline)
--------------------------------------------
Use the hospital_ml_features_readmission table as training data:

```python
# In a Databricks notebook
from sklearn.ensemble import RandomForestClassifier
import mlflow

# Load features
features_df = spark.table("workspace.default.hospital_ml_features_readmission")

# Prepare training data
# (Add label column based on actual readmissions from next 30 days)

# Train model
with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=100)
    model.fit(X_train, y_train)
    
    # Log model
    mlflow.sklearn.log_model(
        model, 
        "model",
        registered_model_name="hospital_readmission_risk"
    )
```

STEP 2: REGISTER MODEL TO PRODUCTION
-------------------------------------
In MLflow Model Registry:
1. Navigate to Models > hospital_readmission_risk
2. Select the best version
3. Transition to "Production" stage

STEP 3: ACTIVATE MLFLOW INTEGRATION
------------------------------------
1. Uncomment the MLflow import and model loading code above
2. Update model_uri to match your registered model name
3. Comment out the mock model logic
4. Update model_version to track actual version

STEP 4: RUN PIPELINE
--------------------
Pipeline will automatically score all patients on each refresh.
Results available in hospital_ml_predictions_readmission table.

STEP 5: CONSUME PREDICTIONS
---------------------------
```sql
-- Find high-risk patients
SELECT patient_id, readmission_risk_score, top_risk_factors
FROM workspace.default.hospital_ml_predictions_readmission
WHERE risk_tier = 'High Risk'
ORDER BY readmission_risk_score DESC
LIMIT 100;
```
"""
