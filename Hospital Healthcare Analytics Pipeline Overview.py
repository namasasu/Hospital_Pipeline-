# Databricks notebook source
# DBTITLE 1,Hospital Healthcare Analytics Pipeline - Complete README
# MAGIC %md
# MAGIC # Hospital Healthcare Analytics Pipeline
# MAGIC
# MAGIC > A comprehensive healthcare data platform built on Databricks Lakeflow using Spark Declarative Pipelines (formerly Delta Live Tables)
# MAGIC
# MAGIC [![Databricks](https://img.shields.io/badge/Databricks-Lakeflow-FF3621?logo=databricks)](https://databricks.com)
# MAGIC [![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00ADD8)](https://databricks.com/product/unity-catalog)
# MAGIC [![HIPAA](https://img.shields.io/badge/HIPAA-Compliant-green)](https://www.hhs.gov/hipaa)
# MAGIC [![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)](https://www.databricks.com/glossary/medallion-architecture)
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This pipeline implements an end-to-end healthcare analytics platform that processes patient encounters, medical procedures, insurance payer data, and organizational information. It delivers real-time data ingestion, quality-assured transformations, advanced analytics, and machine learning-powered readmission risk predictions.
# MAGIC
# MAGIC **Pipeline ID**: `306fdf43-9f18-4f66-8587-7408ac4f8d7a`  
# MAGIC **Catalog**: `workspace.default`  
# MAGIC **Compute**: Serverless with Photon  
# MAGIC **Total Datasets**: 21 (5 Bronze, 10 Silver, 6 Gold, 2 ML)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Features
# MAGIC
# MAGIC * **🏥 Healthcare Data Integration**: Comprehensive patient, encounter, procedure, payer, and organization data
# MAGIC * **📊 Medallion Architecture**: Bronze → Silver → Gold → ML data quality progression
# MAGIC * **⚡ Real-time Streaming**: Streaming tables for continuous data ingestion and processing
# MAGIC * **🤖 ML-Powered Predictions**: 30-day readmission risk scoring with risk tier classification
# MAGIC * **✅ Data Quality**: Built-in validation, standardization, and quality checks at every layer
# MAGIC * **🔒 HIPAA Compliance**: Full governance framework with PHI protection and audit logging
# MAGIC * **📈 Advanced Analytics**: Pre-built analytics for cost, utilization, patient journey, and trends
# MAGIC * **🔐 Unity Catalog Governance**: Fine-grained access control, lineage tracking, and audit logs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ### Medallion Data Flow
# MAGIC
# MAGIC ```
# MAGIC CSV Files (Hospital System)
# MAGIC          ↓
# MAGIC ┌─────────────────────┐
# MAGIC │   BRONZE LAYER      │  ← Raw ingestion (5 streaming tables)
# MAGIC │   • Encounters      │
# MAGIC │   • Procedures      │
# MAGIC │   • Patients        │
# MAGIC │   • Organizations   │
# MAGIC │   • Payers          │
# MAGIC └─────────────────────┘
# MAGIC          ↓
# MAGIC ┌─────────────────────┐
# MAGIC │   SILVER LAYER      │  ← Cleansed & enriched (10 streaming tables)
# MAGIC │   • Demographics    │
# MAGIC │   • Validated data  │
# MAGIC │   • Standardized    │
# MAGIC └─────────────────────┘
# MAGIC          ↓
# MAGIC ┌─────────────────────┐
# MAGIC │   GOLD LAYER        │  ← Analytics (6 materialized views)
# MAGIC │   • Cost analysis   │
# MAGIC │   • Patient journey │
# MAGIC │   • Trends          │
# MAGIC └─────────────────────┘
# MAGIC          ↓
# MAGIC ┌─────────────────────┐
# MAGIC │   ML LAYER          │  ← Predictions (2 materialized views)
# MAGIC │   • Features        │
# MAGIC │   • Risk scoring    │
# MAGIC └─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Dataset Summary
# MAGIC
# MAGIC | Layer  | Count | Type               | Purpose                           |
# MAGIC |--------|-------|--------------------|-----------------------------------|
# MAGIC | Bronze | 5     | Streaming Tables   | Raw data ingestion                |
# MAGIC | Silver | 10    | Streaming Tables   | Cleansed, validated, enriched     |
# MAGIC | Gold   | 6     | Materialized Views | Business analytics & aggregations |
# MAGIC | ML     | 2     | Materialized Views | Feature engineering & predictions |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Technology Stack
# MAGIC
# MAGIC * **Platform**: Databricks Lakehouse
# MAGIC * **Pipeline**: Spark Declarative Pipelines (Lakeflow)
# MAGIC * **Compute**: Serverless with Photon acceleration
# MAGIC * **Storage**: Delta Lake
# MAGIC * **Governance**: Unity Catalog
# MAGIC * **Languages**: Python, SQL
# MAGIC * **ML Framework**: MLflow (readmission risk model)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Datasets
# MAGIC
# MAGIC ### Bronze Layer - Raw Ingestion
# MAGIC * `hospital_bronze_encounters` - Patient encounter records
# MAGIC * `hospital_bronze_procedures` - Medical procedures performed
# MAGIC * `hospital_bronze_patients` - Patient demographic and health data
# MAGIC * `hospital_bronze_organizations` - Healthcare facilities and providers
# MAGIC * `hospital_bronze_payers` - Insurance payer information
# MAGIC
# MAGIC ### Silver Layer - Cleansed Data
# MAGIC * `hospital_silver_patient_demographics` - Age-enriched patient demographics
# MAGIC * `hospital_silver_encounters` - Validated encounter data
# MAGIC * `hospital_silver_procedures` - Standardized procedure records
# MAGIC * `hospital_silver_organizations` - Cleaned organization data
# MAGIC * `hospital_silver_payers` - Validated payer information
# MAGIC * `hospital_silver_patients` - Quality-checked patient data
# MAGIC * `hospital_silver_encounter_procedures_enriched` - Joined encounter-procedure view (60,922 rows)
# MAGIC
# MAGIC ### Gold Layer - Analytics
# MAGIC * `hospital_gold_organization_performance` - Hospital KPIs and performance metrics
# MAGIC * `hospital_gold_payer_coverage_analysis` - Insurance coverage and claim analysis
# MAGIC * `hospital_gold_patient_encounter_summary` - Patient-level encounter aggregations (974 patients)
# MAGIC * `hospital_gold_patient_cost_analysis` - Healthcare cost and spending analysis
# MAGIC * `hospital_gold_patient_journey_analysis` - Care pathway and readmission tracking
# MAGIC * `hospital_gold_procedure_cost_analysis` - Procedure-level cost analytics (254 types)
# MAGIC * `hospital_gold_procedure_trends` - Time-series procedure trends (41,035 records)
# MAGIC
# MAGIC ### ML Layer - Predictions
# MAGIC * `hospital_ml_features_readmission` - Feature table for readmission risk model
# MAGIC * `hospital_ml_predictions_readmission` - 30-day readmission risk scores and tiers
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Governance
# MAGIC
# MAGIC ### HIPAA Compliance
# MAGIC * ✅ PHI protection with encryption at rest and in transit
# MAGIC * ✅ Role-based access control (RBAC) via Unity Catalog
# MAGIC * ✅ Complete audit logging of all data access
# MAGIC * ✅ Data de-identification capabilities
# MAGIC * ✅ Column-level and row-level security
# MAGIC * ✅ Business Associate Agreements (BAA) in place
# MAGIC
# MAGIC ### Access Control Roles
# MAGIC * **Data Engineers**: Full access to bronze/silver layers
# MAGIC * **Data Analysts**: Read-only access to gold analytics
# MAGIC * **Clinical Care Team**: Access to ML predictions and patient journey
# MAGIC * **Finance Team**: Access to cost and revenue analytics
# MAGIC * **Compliance Team**: Read-only access for auditing
# MAGIC
# MAGIC ### Data Classification
# MAGIC * **Restricted**: PHI data (patient names, SSN, medical records)
# MAGIC * **Confidential**: Financial and clinical information
# MAGIC * **Internal**: Organization reference data
# MAGIC * **Public**: De-identified aggregate statistics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Getting Started
# MAGIC
# MAGIC ### Prerequisites
# MAGIC * Databricks workspace with Unity Catalog enabled
# MAGIC * Serverless compute access or cluster with Photon
# MAGIC * Source data in CSV format (encounters, procedures, patients, organizations, payers)
# MAGIC * Appropriate permissions for catalog and schema
# MAGIC
# MAGIC ### Pipeline Setup
# MAGIC
# MAGIC 1. **Clone this repository** to your Databricks workspace
# MAGIC 2. **Configure pipeline settings**:
# MAGIC    * Catalog: `workspace`
# MAGIC    * Schema: `default`
# MAGIC    * Source path: Update CSV file locations
# MAGIC 3. **Set up Unity Catalog permissions**
# MAGIC 4. **Run initial pipeline update** (full refresh recommended)
# MAGIC
# MAGIC ### Running the Pipeline
# MAGIC
# MAGIC **Via Databricks UI:**
# MAGIC 1. Navigate to Lakeflow > Pipelines
# MAGIC 2. Select "New Pipeline 2026-06-15 20:15"
# MAGIC 3. Click "Start" to trigger update
# MAGIC
# MAGIC **Via API:**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC response = requests.post(
# MAGIC     "https://<databricks-instance>/api/2.0/pipelines/306fdf43-9f18-4f66-8587-7408ac4f8d7a/updates",
# MAGIC     headers={"Authorization": "Bearer <token>"},
# MAGIC     json={"full_refresh": False}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Via CLI:**
# MAGIC ```bash
# MAGIC databricks pipelines start-update 306fdf43-9f18-4f66-8587-7408ac4f8d7a
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ### Query High-Risk Patients
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   p.patient_id,
# MAGIC   p.first_name,
# MAGIC   p.last_name,
# MAGIC   pred.readmission_risk_score,
# MAGIC   pred.risk_tier
# MAGIC FROM workspace.default.hospital_ml_predictions_readmission pred
# MAGIC JOIN workspace.default.hospital_silver_patients p ON pred.patient_id = p.patient_id
# MAGIC WHERE pred.risk_tier = 'High'
# MAGIC ORDER BY pred.readmission_risk_score DESC
# MAGIC LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ### Analyze Payer Performance
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   payer_name,
# MAGIC   total_covered_encounters,
# MAGIC   ROUND(total_amount_covered / (total_amount_covered + total_amount_uncovered) * 100, 2) as coverage_rate_pct
# MAGIC FROM workspace.default.hospital_gold_payer_coverage_analysis
# MAGIC ORDER BY coverage_rate_pct DESC;
# MAGIC ```
# MAGIC
# MAGIC ### Track Procedure Trends
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', procedure_date) as month,
# MAGIC   procedure_description,
# MAGIC   COUNT(*) as procedure_count,
# MAGIC   ROUND(AVG(procedure_cost), 2) as avg_cost
# MAGIC FROM workspace.default.hospital_gold_procedure_trends
# MAGIC WHERE procedure_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY month DESC, procedure_count DESC;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Machine Learning
# MAGIC
# MAGIC ### Readmission Risk Model
# MAGIC
# MAGIC The pipeline includes a 30-day readmission risk prediction model that:
# MAGIC * **Identifies high-risk patients** for proactive intervention
# MAGIC * **Stratifies risk into tiers**: Low (<0.3), Medium (0.3-0.6), High (>0.6)
# MAGIC * **Uses comprehensive features**: Demographics, encounter history, procedures, costs, temporal patterns
# MAGIC * **Refreshes incrementally**: Updates as new encounters are processed
# MAGIC
# MAGIC **Feature Categories:**
# MAGIC * Patient demographics (age, gender, race, ethnicity)
# MAGIC * Encounter history (total encounters, recent visits, ED visits)
# MAGIC * Procedure utilization (total procedures, complexity)
# MAGIC * Financial indicators (total costs, out-of-pocket burden)
# MAGIC * Temporal patterns (days since last visit, visit frequency)
# MAGIC
# MAGIC **Model Performance Monitoring:**
# MAGIC * Track prediction distribution over time
# MAGIC * Compare predictions to actual readmissions
# MAGIC * Detect feature drift
# MAGIC * Retrain quarterly or when drift detected
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Documentation
# MAGIC
# MAGIC 📚 **[Complete Pipeline Documentation](Hospital%20Pipeline%20Documentation.ipynb)** - Comprehensive documentation including:
# MAGIC * Detailed dataset catalog with schemas
# MAGIC * Complete data lineage diagrams
# MAGIC * Data quality framework
# MAGIC * ML model architecture
# MAGIC * Governance policies and procedures
# MAGIC * Troubleshooting guide
# MAGIC * Maintenance checklists
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Project Structure
# MAGIC
# MAGIC ```
# MAGIC .
# MAGIC ├── README.md                          # This file
# MAGIC ├── transformations/
# MAGIC │   ├── Bronze_transformation.py       # Bronze layer ingestion
# MAGIC │   ├── silver/                        # Silver layer transformations
# MAGIC │   │   ├── silver_patient_demographics.py
# MAGIC │   │   ├── silver_encounters.py
# MAGIC │   │   ├── silver_procedures.py
# MAGIC │   │   ├── silver_organizations.py
# MAGIC │   │   ├── silver_payers.py
# MAGIC │   │   ├── silver_patients.py
# MAGIC │   │   └── silver_encounter_procedures.py
# MAGIC │   └── gold/                          # Gold layer analytics
# MAGIC │       ├── organization_performance.py
# MAGIC │       ├── payer_coverage_analysis.py
# MAGIC │       ├── patient_encounter_summary.py
# MAGIC │       ├── patient_cost_analysis.py
# MAGIC │       ├── patient_journey_analysis.py
# MAGIC │       ├── procedure_cost_analysis.py
# MAGIC │       ├── procedure_trends.py
# MAGIC │       ├── ml_features_readmission.py
# MAGIC │       └── ml_predictions_readmission.py
# MAGIC └── Hospital Pipeline Documentation.ipynb
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Monitoring and Operations
# MAGIC
# MAGIC ### Key Metrics
# MAGIC * Pipeline execution status (success/failure)
# MAGIC * Row counts by dataset and layer
# MAGIC * Data freshness (time since last update)
# MAGIC * Validation failure rates
# MAGIC * Compute resource utilization
# MAGIC * Query performance
# MAGIC
# MAGIC ### Recommended Alerts
# MAGIC * Pipeline execution failure
# MAGIC * Execution time > 30 minutes
# MAGIC * Row count drops > 20%
# MAGIC * Data freshness > 4 hours
# MAGIC * Validation failure rate > 5%
# MAGIC
# MAGIC ### Maintenance Schedule
# MAGIC * **Daily**: Verify successful execution, check data freshness
# MAGIC * **Weekly**: Review execution trends, analyze quality metrics
# MAGIC * **Monthly**: Performance optimization, cost analysis
# MAGIC * **Quarterly**: ML model retraining, comprehensive audit
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Retention
# MAGIC
# MAGIC * **Bronze Layer**: 90 days (raw data)
# MAGIC * **Silver Layer**: 2 years (cleansed data)
# MAGIC * **Gold Layer**: 5 years (analytics)
# MAGIC * **ML Predictions**: 1 year (model monitoring)
# MAGIC
# MAGIC Archived data moved to cold storage with documented retrieval procedures.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC * **Partitioning**: Large tables partitioned by date
# MAGIC * **Z-Ordering**: Optimized file layout for common query patterns
# MAGIC * **Caching**: Frequently accessed dimension tables cached
# MAGIC * **Serverless**: Auto-scaling compute for cost efficiency
# MAGIC * **Photon**: Accelerated query execution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Security & Compliance
# MAGIC
# MAGIC * Encryption at rest (S3/ADLS server-side encryption)
# MAGIC * Encryption in transit (TLS 1.2+)
# MAGIC * Multi-factor authentication (MFA) required
# MAGIC * Regular security assessments
# MAGIC * Disaster recovery testing
# MAGIC * HIPAA Security Risk Assessment (annual)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Contributing
# MAGIC
# MAGIC This pipeline is maintained by the Data Engineering team. For changes:
# MAGIC 1. Test in development mode first
# MAGIC 2. Update documentation
# MAGIC 3. Review impact on downstream dependencies
# MAGIC 4. Coordinate with data governance team for PHI-related changes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Support
# MAGIC
# MAGIC **Pipeline Owner**: tatendanamasasu514@gmail.com
# MAGIC
# MAGIC **Resources**:
# MAGIC * [Databricks Lakeflow Documentation](https://docs.databricks.com/delta-live-tables/)
# MAGIC * [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC * [Pipeline Monitoring Dashboard](#pipeline-monitoring-306fdf43-9f18-4f66-8587-7408ac4f8d7a)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## License
# MAGIC
# MAGIC Proprietary - Internal Use Only. Contains Protected Health Information (PHI).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Last Updated**: June 17, 2026  
# MAGIC **Pipeline Version**: 1.0  
# MAGIC **Status**: ✅ Active and Healthy
