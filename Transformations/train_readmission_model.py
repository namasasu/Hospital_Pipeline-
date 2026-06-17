# Databricks notebook source
# DBTITLE 1,Train Hospital Readmission Risk Model
# ============================================================================
# HOSPITAL READMISSION RISK MODEL TRAINING
# ============================================================================
# This notebook trains a machine learning model to predict 30-day hospital
# readmission risk using features from the pipeline's feature engineering table.
#
# Pipeline Integration:
# - Reads features from: workspace.default.hospital_ml_features_readmission
# - Reads encounters from: workspace.default.hospital_silver_encounters
# - Registers model to: MLflow Model Registry as 'hospital_readmission_risk'
#
# After training, update the pipeline's ml_predictions_readmission.py to use
# the registered model for batch scoring.
# ============================================================================

import warnings
warnings.filterwarnings('ignore')

# Core libraries
import pandas as pd
import numpy as np
from datetime import timedelta

# Spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ML libraries
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, 
    roc_auc_score, confusion_matrix, roc_curve, classification_report
)

# MLflow
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

print("✓ All libraries imported successfully")
print(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")


# ============================================================================
# PART 1: LABEL GENERATION
# ============================================================================
print("\n" + "="*80)
print("PART 1: GENERATING 30-DAY READMISSION LABELS")
print("="*80)

# Load encounter data
encounters_df = spark.table("workspace.default.hospital_silver_encounters")

print(f"Total encounters loaded: {encounters_df.count():,}")

# Define window for each patient ordered by encounter start time
window_spec = Window.partitionBy("patient_id").orderBy("encounter_start")

# Create labels: 1 if next encounter is within 30 days, 0 otherwise
labeled_encounters = (
    encounters_df
    .withColumn("next_encounter_start", F.lead("encounter_start").over(window_spec))
    .withColumn(
        "days_to_next_encounter",
        F.datediff(F.col("next_encounter_start"), F.col("encounter_start"))
    )
    .withColumn(
        "readmitted_30_days",
        F.when(
            (F.col("days_to_next_encounter").isNotNull()) & 
            (F.col("days_to_next_encounter") <= 30),
            1
        ).otherwise(0)
    )
    .select(
        "encounter_id",
        "patient_id",
        "encounter_start",
        "encounter_class",
        "days_to_next_encounter",
        "readmitted_30_days"
    )
)

# Calculate label distribution
label_counts = labeled_encounters.groupBy("readmitted_30_days").count().collect()
print("\nLabel Distribution:")
for row in label_counts:
    label = "Readmitted" if row["readmitted_30_days"] == 1 else "Not Readmitted"
    print(f"  {label}: {row['count']:,} encounters")

# Aggregate labels to patient level (max readmission flag across all encounters)
patient_labels = (
    labeled_encounters
    .groupBy("patient_id")
    .agg(
        F.max("readmitted_30_days").alias("has_readmission"),
        F.count("encounter_id").alias("total_encounters_for_label"),
        F.sum("readmitted_30_days").alias("readmission_count_label")
    )
)

print(f"\nPatients with labels: {patient_labels.count():,}")

# Load features and join with labels
features_df = spark.table("workspace.default.hospital_ml_features_readmission")

print(f"Patients with features: {features_df.count():,}")

# Join features with labels
training_data = (
    features_df
    .join(patient_labels, "patient_id", "inner")
    .select(
        "patient_id",
        "has_readmission",  # Target variable
        *[col for col in features_df.columns if col.startswith("feature_")]
    )
)

print(f"Training dataset size: {training_data.count():,} patients")

# Show class balance
class_balance = training_data.groupBy("has_readmission").count().collect()
print("\nClass Balance in Training Data:")
for row in class_balance:
    label = "Positive (Readmitted)" if row["has_readmission"] == 1 else "Negative (No Readmission)"
    pct = (row["count"] / training_data.count()) * 100
    print(f"  {label}: {row['count']:,} ({pct:.1f}%)")


# ============================================================================
# PART 2: DATA PREPARATION
# ============================================================================
print("\n" + "="*80)
print("PART 2: PREPARING DATA FOR TRAINING")
print("="*80)

# Convert to Pandas for sklearn
training_pdf = training_data.toPandas()

print(f"Pandas DataFrame shape: {training_pdf.shape}")

# Separate features and target - ONLY USE NUMERIC FEATURES
feature_columns = [col for col in training_pdf.columns if col.startswith("feature_")]

# Filter to numeric columns only (exclude categorical like feature_age_group)
numeric_feature_columns = []
for col in feature_columns:
    if training_pdf[col].dtype in ['int64', 'float64', 'int32', 'float32']:
        numeric_feature_columns.append(col)
    else:
        print(f"Excluding non-numeric column: {col} (dtype: {training_pdf[col].dtype})")

X = training_pdf[numeric_feature_columns]
y = training_pdf["has_readmission"]

print(f"\nNumeric feature columns: {len(numeric_feature_columns)}")
print(f"Target variable: has_readmission")

# Check for missing values
missing_counts = X.isnull().sum()
if missing_counts.sum() > 0:
    print("\n⚠ Missing values detected:")
    print(missing_counts[missing_counts > 0])
    print("\nFilling missing values with 0...")
    X = X.fillna(0)
else:
    print("\n✓ No missing values detected")

# Split data (80/20 train/test split, stratified by target)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, 
    test_size=0.2, 
    random_state=42, 
    stratify=y
)

print(f"\nTrain set size: {len(X_train):,} ({len(X_train)/len(X)*100:.1f}%)")
print(f"Test set size: {len(X_test):,} ({len(X_test)/len(X)*100:.1f}%)")
print(f"\nTrain set positive rate: {y_train.mean()*100:.1f}%")
print(f"Test set positive rate: {y_test.mean()*100:.1f}%")


# ============================================================================
# PART 3: MODEL TRAINING WITH MLFLOW
# ============================================================================
print("\n" + "="*80)
print("PART 3: TRAINING RANDOM FOREST MODEL")
print("="*80)

# Set MLflow experiment
mlflow.set_experiment("/Users/tatendanamasasu514@gmail.com/hospital_readmission_prediction")

# Start MLflow run
with mlflow.start_run(run_name="RandomForest_Readmission_v1") as run:
    
    print(f"MLflow Run ID: {run.info.run_id}")
    
    # Train Random Forest model
    print("\nTraining Random Forest Classifier...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        min_samples_split=20,
        min_samples_leaf=10,
        random_state=42,
        n_jobs=-1,
        class_weight='balanced'  # Handle class imbalance
    )
    
    model.fit(X_train, y_train)
    print("✓ Model training complete")
    
    # Make predictions
    print("\nGenerating predictions...")
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    y_test_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    print("\nCalculating metrics...")
    train_accuracy = accuracy_score(y_train, y_train_pred)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    test_precision = precision_score(y_test, y_test_pred)
    test_recall = recall_score(y_test, y_test_pred)
    test_f1 = f1_score(y_test, y_test_pred)
    test_auc = roc_auc_score(y_test, y_test_proba)
    
    # Log parameters
    mlflow.log_param("model_type", "RandomForestClassifier")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("min_samples_split", 20)
    mlflow.log_param("min_samples_leaf", 10)
    mlflow.log_param("class_weight", "balanced")
    mlflow.log_param("train_size", len(X_train))
    mlflow.log_param("test_size", len(X_test))
    mlflow.log_param("num_features", len(numeric_feature_columns))
    
    # Log metrics
    mlflow.log_metric("train_accuracy", train_accuracy)
    mlflow.log_metric("test_accuracy", test_accuracy)
    mlflow.log_metric("test_precision", test_precision)
    mlflow.log_metric("test_recall", test_recall)
    mlflow.log_metric("test_f1", test_f1)
    mlflow.log_metric("test_auc_roc", test_auc)
    
    print("\n" + "="*60)
    print("MODEL PERFORMANCE METRICS")
    print("="*60)
    print(f"Train Accuracy:    {train_accuracy:.4f}")
    print(f"Test Accuracy:     {test_accuracy:.4f}")
    print(f"Test Precision:    {test_precision:.4f}")
    print(f"Test Recall:       {test_recall:.4f}")
    print(f"Test F1 Score:     {test_f1:.4f}")
    print(f"Test ROC-AUC:      {test_auc:.4f}")
    print("="*60)
    
    # Log model with signature
    print("\nLogging model to MLflow...")
    signature = infer_signature(X_train, y_train_pred)
    mlflow.sklearn.log_model(
        model, 
        "model",
        signature=signature,
        registered_model_name="hospital_readmission_risk"
    )
    print("✓ Model logged and registered to MLflow Model Registry")
    
    # Save run ID for later reference
    run_id = run.info.run_id


# ============================================================================
# PART 4: MODEL EVALUATION & VISUALIZATION
# ============================================================================
print("\n" + "="*80)
print("PART 4: MODEL EVALUATION & VISUALIZATION")
print("="*80)

# Classification Report
print("\nClassification Report:")
print(classification_report(y_test, y_test_pred, target_names=["No Readmission", "Readmission"]))

# Confusion Matrix
print("\nGenerating visualizations...")
cm = confusion_matrix(y_test, y_test_pred)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# 1. Confusion Matrix
ax1 = axes[0, 0]
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax1,
            xticklabels=["No Readmission", "Readmission"],
            yticklabels=["No Readmission", "Readmission"])
ax1.set_title('Confusion Matrix', fontsize=14, fontweight='bold')
ax1.set_ylabel('Actual', fontsize=12)
ax1.set_xlabel('Predicted', fontsize=12)

# 2. ROC Curve
ax2 = axes[0, 1]
fpr, tpr, thresholds = roc_curve(y_test, y_test_proba)
ax2.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (AUC = {test_auc:.3f})')
ax2.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--', label='Random Classifier')
ax2.set_xlim([0.0, 1.0])
ax2.set_ylim([0.0, 1.05])
ax2.set_xlabel('False Positive Rate', fontsize=12)
ax2.set_ylabel('True Positive Rate', fontsize=12)
ax2.set_title('ROC Curve', fontsize=14, fontweight='bold')
ax2.legend(loc="lower right")
ax2.grid(alpha=0.3)

# 3. Feature Importance (Top 15)
ax3 = axes[1, 0]
feature_importance = pd.DataFrame({
    'feature': numeric_feature_columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False).head(15)

sns.barplot(data=feature_importance, y='feature', x='importance', ax=ax3, palette='viridis')
ax3.set_title('Top 15 Feature Importances', fontsize=14, fontweight='bold')
ax3.set_xlabel('Importance', fontsize=12)
ax3.set_ylabel('Feature', fontsize=12)

# 4. Prediction Distribution
ax4 = axes[1, 1]
ax4.hist(y_test_proba[y_test == 0], bins=30, alpha=0.6, label='No Readmission (Actual)', color='blue')
ax4.hist(y_test_proba[y_test == 1], bins=30, alpha=0.6, label='Readmission (Actual)', color='red')
ax4.set_xlabel('Predicted Probability', fontsize=12)
ax4.set_ylabel('Frequency', fontsize=12)
ax4.set_title('Distribution of Predicted Probabilities', fontsize=14, fontweight='bold')
ax4.legend()
ax4.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('/tmp/model_evaluation.png', dpi=300, bbox_inches='tight')
print("✓ Visualizations saved to /tmp/model_evaluation.png")
plt.show()

# Log visualization to MLflow
with mlflow.start_run(run_id=run_id):
    mlflow.log_artifact('/tmp/model_evaluation.png')
    print("✓ Visualizations logged to MLflow")

# Feature Importance Table
print("\n" + "="*60)
print("TOP 20 MOST IMPORTANT FEATURES")
print("="*60)
feature_importance_full = pd.DataFrame({
    'feature': numeric_feature_columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

for idx, row in feature_importance_full.head(20).iterrows():
    print(f"{row['feature']:50s} {row['importance']:.4f}")


# ============================================================================
# PART 5: SAMPLE PREDICTIONS
# ============================================================================
print("\n" + "="*80)
print("PART 5: SAMPLE PREDICTIONS")
print("="*80)

# Show predictions for a few test samples
sample_predictions = pd.DataFrame({
    'patient_id': training_pdf.loc[X_test.index, 'patient_id'],
    'actual': y_test,
    'predicted': y_test_pred,
    'probability': y_test_proba,
    'age': X_test['feature_age'].values,
    'total_encounters': X_test['feature_total_encounters'].values,
    'readmission_rate': X_test['feature_readmission_rate'].values,
    'emergency_visits': X_test['feature_emergency_visits'].values
}).sort_values('probability', ascending=False)

print("\nTop 10 Highest Risk Predictions:")
print(sample_predictions.head(10).to_string(index=False))

print("\nTop 10 Lowest Risk Predictions:")
print(sample_predictions.tail(10).to_string(index=False))


# ============================================================================
# DEPLOYMENT SUMMARY
# ============================================================================
print("\n" + "="*80)
print("DEPLOYMENT INSTRUCTIONS")
print("="*80)
print(f"""
Your model has been trained and registered to MLflow Model Registry!

MODEL DETAILS:
- Run ID: {run_id}
- Model Name: hospital_readmission_risk
- Model URI: models:/hospital_readmission_risk/Production
- Features Used: {len(numeric_feature_columns)} numeric features

NEXT STEPS:

1. TRANSITION MODEL TO PRODUCTION
   - Open MLflow Model Registry UI
   - Navigate to Models > hospital_readmission_risk
   - Select the latest version
   - Click "Transition to Production"

2. UPDATE PIPELINE TO USE THIS MODEL
   - Edit file: transformations/gold/ml_predictions_readmission.py
   - Uncomment the MLflow import section (lines ~60-75)
   - Comment out the mock model logic (lines ~120-135)
   - Replace with MLflow UDF code:
   
     import mlflow
     
     readmission_model_udf = mlflow.pyfunc.spark_udf(
         spark,
         model_uri="models:/hospital_readmission_risk/Production",
         result_type="double"
     )
     
     # Select only numeric features that were used in training
     numeric_features = {numeric_feature_columns}
     
     predictions = features.withColumn(
         "readmission_risk_score",
         readmission_model_udf(F.struct(*numeric_features))
     )
   
   - Update model_version to track the actual version number

3. RUN THE PIPELINE
   - Navigate to the pipeline editor
   - Start a pipeline update
   - The pipeline will now score all patients with your trained model!

4. VERIFY RESULTS
   - Query workspace.default.hospital_ml_predictions_readmission
   - Validate risk scores and predictions
   - Monitor model performance over time

5. MODEL MONITORING (RECOMMENDED)
   - Set up periodic model retraining
   - Monitor prediction drift
   - Update model as new data arrives
""")

print("\n" + "="*80)
print("MODEL TRAINING COMPLETE ✓")
print("="*80)
