# Databricks notebook source
# MAGIC %md
# MAGIC # Hospital Data Pipeline Documentation
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC **Pipeline Name**: New Pipeline 2026-06-15 20:15  
# MAGIC **Pipeline ID**: `306fdf43-9f18-4f66-8587-7408ac4f8d7a`  
# MAGIC **Catalog**: `workspace`  
# MAGIC **Schema**: `default`  
# MAGIC **Architecture**: Medallion Architecture (Bronze → Silver → Gold → ML)  
# MAGIC **Compute**: Serverless with Photon Enabled  
# MAGIC **Root Path**: `/Workspace/Users/tatendanamasasu514@gmail.com/New Pipeline 2026-06-15 20:15`
# MAGIC
# MAGIC This pipeline implements a comprehensive healthcare analytics platform processing patient encounters, procedures, organizations, and insurance payer data. It follows the medallion architecture pattern with 21 datasets spanning bronze (raw ingestion), silver (cleansed/enriched), gold (analytics), and ML (machine learning predictions) layers.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Overview
# MAGIC
# MAGIC ### Medallion Architecture Layers
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                        SOURCE DATA                               │
# MAGIC │        (Hospital Management System - CSV Files)                  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               ↓
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      BRONZE LAYER                                │
# MAGIC │              (Streaming Tables - Raw Ingestion)                  │
# MAGIC │  • hospital_bronze_encounters                                    │
# MAGIC │  • hospital_bronze_procedures                                    │
# MAGIC │  • hospital_bronze_patients                                      │
# MAGIC │  • hospital_bronze_organizations                                 │
# MAGIC │  • hospital_bronze_payers                                        │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               ↓
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      SILVER LAYER                                │
# MAGIC │         (Streaming Tables - Cleansed & Enriched)                 │
# MAGIC │  • hospital_silver_patient_demographics (age calculations)       │
# MAGIC │  • hospital_silver_encounters (standardized fields)              │
# MAGIC │  • hospital_silver_procedures (quality checks)                   │
# MAGIC │  • hospital_silver_organizations (cleaned data)                  │
# MAGIC │  • hospital_silver_payers (standardized)                         │
# MAGIC │  • hospital_silver_patients (validated)                          │
# MAGIC │  • hospital_silver_encounter_procedures_enriched (joined view)   │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               ↓
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                       GOLD LAYER                                 │
# MAGIC │           (Materialized Views - Analytics)                       │
# MAGIC │  • hospital_gold_organization_performance                        │
# MAGIC │  • hospital_gold_payer_coverage_analysis                         │
# MAGIC │  • hospital_gold_patient_encounter_summary                       │
# MAGIC │  • hospital_gold_patient_cost_analysis                           │
# MAGIC │  • hospital_gold_patient_journey_analysis                        │
# MAGIC │  • hospital_gold_procedure_cost_analysis                         │
# MAGIC │  • hospital_gold_procedure_trends                                │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC                               ↓
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                        ML LAYER                                  │
# MAGIC │         (Materialized Views - Predictions)                       │
# MAGIC │  • hospital_ml_features_readmission (feature engineering)        │
# MAGIC │  • hospital_ml_predictions_readmission (risk scoring)            │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Design Principles
# MAGIC
# MAGIC 1. **Streaming-First**: Bronze and Silver layers use streaming tables for real-time data processing
# MAGIC 2. **Incremental Processing**: Materialized views in Gold/ML layers refresh incrementally
# MAGIC 3. **Data Quality**: Built-in validation and quality checks at each layer
# MAGIC 4. **Scalability**: Serverless compute with Photon optimization
# MAGIC 5. **Modularity**: Each dataset is self-contained with clear dependencies
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset Catalog
# MAGIC
# MAGIC ### Bronze Layer - Raw Ingestion (5 Streaming Tables)
# MAGIC
# MAGIC #### `workspace.default.hospital_bronze_encounters`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Raw patient encounter data ingested from hospital management system
# MAGIC * **Source**: CSV files from hospital management system
# MAGIC * **Key Fields**: Encounter ID, Patient ID, Organization ID, Start/Stop dates, Encounter class, Cost, Payer coverage
# MAGIC * **Refresh Pattern**: Streaming (real-time ingestion)
# MAGIC
# MAGIC #### `workspace.default.hospital_bronze_procedures`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Raw procedure data ingested from hospital management system
# MAGIC * **Source**: CSV files from hospital management system
# MAGIC * **Key Fields**: Procedure ID, Encounter ID, Patient ID, Procedure code, Description, Cost, Date
# MAGIC * **Refresh Pattern**: Streaming (real-time ingestion)
# MAGIC
# MAGIC #### `workspace.default.hospital_bronze_patients`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Raw patient data ingested from hospital management system
# MAGIC * **Source**: CSV files from hospital management system
# MAGIC * **Key Fields**: Patient ID, Birth date, Death date, SSN, Drivers license, Passport, Prefix, First/Last name, Address, City, State, County, Zip, Race, Ethnicity, Gender, Birthplace, Healthcare expenses/coverage
# MAGIC * **Refresh Pattern**: Streaming (real-time ingestion)
# MAGIC
# MAGIC #### `workspace.default.hospital_bronze_organizations`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Raw organization/hospital data ingested from hospital management system
# MAGIC * **Source**: CSV files from hospital management system
# MAGIC * **Key Fields**: Organization ID, Name, Address, City, State, Zip, Phone, Revenue, Utilization
# MAGIC * **Refresh Pattern**: Streaming (real-time ingestion)
# MAGIC
# MAGIC #### `workspace.default.hospital_bronze_payers`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Raw insurance payer data ingested from hospital management system
# MAGIC * **Source**: CSV files from hospital management system
# MAGIC * **Key Fields**: Payer ID, Name, Address, City, State, Zip, Phone, Amount covered/uncovered, Revenue, Covered encounters/medications/procedures/immunizations
# MAGIC * **Refresh Pattern**: Streaming (real-time ingestion)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Silver Layer - Cleansed & Enriched (10 Streaming Tables)
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_patient_demographics`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Enriched patient demographics with calculated age, age groups, and demographic segments
# MAGIC * **Source**: `hospital_bronze_patients`
# MAGIC * **Key Transformations**:
# MAGIC   * Age calculation from birth date
# MAGIC   * Age group segmentation (0-17, 18-35, 36-50, 51-65, 66+)
# MAGIC   * Demographic standardization
# MAGIC * **Data Quality**: Validates required fields, standardizes gender/race/ethnicity values
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_encounters`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Cleaned patient encounter data with standardized fields and data quality checks
# MAGIC * **Source**: `hospital_bronze_encounters`
# MAGIC * **Key Transformations**:
# MAGIC   * Duration calculation (stop - start)
# MAGIC   * Cost validation (non-negative)
# MAGIC   * Encounter class standardization
# MAGIC * **Data Quality**: Validates dates, costs, and required foreign keys
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_procedures`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Cleaned procedure data with standardized fields and data quality checks
# MAGIC * **Source**: `hospital_bronze_procedures`
# MAGIC * **Key Transformations**:
# MAGIC   * Procedure code validation
# MAGIC   * Cost standardization
# MAGIC   * Date validation
# MAGIC * **Data Quality**: Validates required fields and foreign key relationships
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_organizations`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Cleaned organization/hospital data with standardized fields
# MAGIC * **Source**: `hospital_bronze_organizations`
# MAGIC * **Key Transformations**:
# MAGIC   * Address standardization
# MAGIC   * Phone number formatting
# MAGIC   * Revenue validation
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_payers`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Cleaned insurance payer data with standardized fields
# MAGIC * **Source**: `hospital_bronze_payers`
# MAGIC * **Key Transformations**:
# MAGIC   * Payer name standardization
# MAGIC   * Coverage amount validation
# MAGIC   * Contact information formatting
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_patients`
# MAGIC * **Type**: Streaming Table
# MAGIC * **Description**: Cleaned patient data with standardized fields and data quality checks
# MAGIC * **Source**: `hospital_bronze_patients`
# MAGIC * **Key Transformations**:
# MAGIC   * Name standardization
# MAGIC   * Address validation
# MAGIC   * SSN/ID masking for privacy
# MAGIC
# MAGIC #### `workspace.default.hospital_silver_encounter_procedures_enriched`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Enriched view joining encounters with all related procedures for comprehensive analysis
# MAGIC * **Source**: Joins `hospital_silver_encounters` with `hospital_silver_procedures`
# MAGIC * **Row Count**: 60,922 procedures
# MAGIC * **Key Transformations**: Complete encounter-procedure relationship with all associated metadata
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Gold Layer - Analytics (6 Materialized Views)
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_organization_performance`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Hospital performance metrics including encounter volumes, revenue, patient outcomes, and efficiency
# MAGIC * **Row Count**: 1 organization
# MAGIC * **Key Metrics**:
# MAGIC   * Total encounters, patients, procedures
# MAGIC   * Total revenue and average cost per encounter
# MAGIC   * Utilization rates
# MAGIC   * Performance trends
# MAGIC * **Use Cases**: Executive dashboards, performance monitoring, operational efficiency analysis
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_payer_coverage_analysis`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Insurance payer coverage analysis including coverage rates, denied claims, and out-of-pocket costs
# MAGIC * **Row Count**: 10 payers
# MAGIC * **Key Metrics**:
# MAGIC   * Coverage amounts (covered vs uncovered)
# MAGIC   * Coverage rates by payer
# MAGIC   * Denied claim analysis
# MAGIC   * Out-of-pocket cost burden
# MAGIC * **Use Cases**: Revenue cycle management, payer negotiations, financial planning
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_patient_encounter_summary`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Aggregated patient encounter metrics including total encounters, costs, and demographics
# MAGIC * **Row Count**: 974 patients
# MAGIC * **Key Metrics**:
# MAGIC   * Total encounters per patient
# MAGIC   * Total healthcare costs
# MAGIC   * Average cost per encounter
# MAGIC   * Demographic breakdown
# MAGIC * **Use Cases**: Patient segmentation, cost analysis, population health management
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_patient_cost_analysis`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Patient-level cost analysis with total healthcare spend, out-of-pocket costs, and high-cost patient identification
# MAGIC * **Row Count**: 974 patients
# MAGIC * **Key Metrics**:
# MAGIC   * Total healthcare expenses
# MAGIC   * Payer coverage amounts
# MAGIC   * Out-of-pocket costs
# MAGIC   * High-cost patient flags (>$50K)
# MAGIC * **Use Cases**: Financial counseling, payment plan design, cost containment programs
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_patient_journey_analysis`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Patient care pathway analysis including encounter sequences, readmission rates, and care continuity
# MAGIC * **Row Count**: 974 patients
# MAGIC * **Key Metrics**:
# MAGIC   * Total encounters and timeline
# MAGIC   * Days between encounters
# MAGIC   * Readmission indicators
# MAGIC   * Care continuity measures
# MAGIC * **Use Cases**: Care coordination, readmission prevention, patient engagement
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_procedure_cost_analysis`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Aggregated procedure cost analytics by procedure type and organization
# MAGIC * **Row Count**: 254 procedure types
# MAGIC * **Key Metrics**:
# MAGIC   * Total procedures by type
# MAGIC   * Average cost per procedure
# MAGIC   * Cost variability
# MAGIC   * Volume trends
# MAGIC * **Use Cases**: Procedure standardization, cost benchmarking, capacity planning
# MAGIC
# MAGIC #### `workspace.default.hospital_gold_procedure_trends`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Time-series analysis of medical procedures including trends, seasonality, and growth rates
# MAGIC * **Row Count**: 41,035 time-series records
# MAGIC * **Key Metrics**:
# MAGIC   * Daily/weekly/monthly procedure volumes
# MAGIC   * Year-over-year growth rates
# MAGIC   * Seasonal patterns
# MAGIC   * Procedure mix changes
# MAGIC * **Use Cases**: Forecasting, resource planning, trend analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ML Layer - Predictions (2 Materialized Views)
# MAGIC
# MAGIC #### `workspace.default.hospital_ml_features_readmission`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: Feature table for 30-day readmission risk prediction model. Combines patient demographics, encounter history, and healthcare utilization patterns.
# MAGIC * **Row Count**: 974 patients
# MAGIC * **Key Features**:
# MAGIC   * Patient demographics (age, gender, race, ethnicity)
# MAGIC   * Encounter history (total encounters, recent encounters)
# MAGIC   * Healthcare utilization (total procedures, emergency visits)
# MAGIC   * Cost indicators (total costs, cost per encounter)
# MAGIC   * Temporal features (days since last encounter, encounter frequency)
# MAGIC * **Use Cases**: ML model training, feature analysis, risk stratification research
# MAGIC
# MAGIC #### `workspace.default.hospital_ml_predictions_readmission`
# MAGIC * **Type**: Materialized View
# MAGIC * **Description**: 30-day readmission risk predictions using ML model. Scores all patients with risk probability (0-1) and risk tier classification.
# MAGIC * **Row Count**: 974 patients
# MAGIC * **Key Outputs**:
# MAGIC   * Readmission risk score (0-1 probability)
# MAGIC   * Risk tier (Low: <0.3, Medium: 0.3-0.6, High: >0.6)
# MAGIC   * Contributing factors
# MAGIC   * Prediction confidence
# MAGIC * **Use Cases**: Care management prioritization, intervention targeting, population health management
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage
# MAGIC
# MAGIC ### Source to Bronze
# MAGIC ```
# MAGIC Hospital Management System (CSV)
# MAGIC   └──> hospital_bronze_encounters
# MAGIC   └──> hospital_bronze_procedures
# MAGIC   └──> hospital_bronze_patients
# MAGIC   └──> hospital_bronze_organizations
# MAGIC   └──> hospital_bronze_payers
# MAGIC ```
# MAGIC
# MAGIC ### Bronze to Silver
# MAGIC ```
# MAGIC hospital_bronze_patients
# MAGIC   └──> hospital_silver_patient_demographics
# MAGIC   └──> hospital_silver_patients
# MAGIC
# MAGIC hospital_bronze_encounters
# MAGIC   └──> hospital_silver_encounters
# MAGIC
# MAGIC hospital_bronze_procedures
# MAGIC   └──> hospital_silver_procedures
# MAGIC
# MAGIC hospital_bronze_organizations
# MAGIC   └──> hospital_silver_organizations
# MAGIC
# MAGIC hospital_bronze_payers
# MAGIC   └──> hospital_silver_payers
# MAGIC
# MAGIC hospital_silver_encounters + hospital_silver_procedures
# MAGIC   └──> hospital_silver_encounter_procedures_enriched
# MAGIC ```
# MAGIC
# MAGIC ### Silver to Gold
# MAGIC ```
# MAGIC hospital_silver_organizations + hospital_silver_encounters + hospital_silver_procedures
# MAGIC   └──> hospital_gold_organization_performance
# MAGIC
# MAGIC hospital_silver_payers + hospital_silver_encounters
# MAGIC   └──> hospital_gold_payer_coverage_analysis
# MAGIC
# MAGIC hospital_silver_patients + hospital_silver_encounters + hospital_silver_patient_demographics
# MAGIC   └──> hospital_gold_patient_encounter_summary
# MAGIC   └──> hospital_gold_patient_cost_analysis
# MAGIC   └──> hospital_gold_patient_journey_analysis
# MAGIC
# MAGIC hospital_silver_procedures + hospital_silver_encounters
# MAGIC   └──> hospital_gold_procedure_cost_analysis
# MAGIC   └──> hospital_gold_procedure_trends
# MAGIC ```
# MAGIC
# MAGIC ### Silver/Gold to ML
# MAGIC ```
# MAGIC hospital_silver_patients + hospital_silver_encounters + hospital_silver_procedures + hospital_silver_patient_demographics
# MAGIC   └──> hospital_ml_features_readmission
# MAGIC
# MAGIC hospital_ml_features_readmission
# MAGIC   └──> hospital_ml_predictions_readmission
# MAGIC ```
# MAGIC
# MAGIC ### Critical Path Dependencies
# MAGIC
# MAGIC The pipeline has the following critical dependencies that must be satisfied for successful execution:
# MAGIC
# MAGIC 1. **Bronze tables must complete first** - All silver transformations depend on bronze tables
# MAGIC 2. **Silver patient demographics** - Required by most gold analytics
# MAGIC 3. **Silver encounters** - Core fact table required by all gold analytics
# MAGIC 4. **ML features** - Must complete before predictions can be generated
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Approach
# MAGIC
# MAGIC ### Quality Checks by Layer
# MAGIC
# MAGIC #### Bronze Layer
# MAGIC * **Schema validation**: Ensures incoming data matches expected schema
# MAGIC * **File format validation**: Validates CSV structure and encoding
# MAGIC * **Null checks**: Identifies missing required fields
# MAGIC * **Duplicate detection**: Flags duplicate records based on primary keys
# MAGIC
# MAGIC #### Silver Layer
# MAGIC * **Data type validation**: Converts and validates data types
# MAGIC * **Referential integrity**: Validates foreign key relationships
# MAGIC * **Business rule validation**:
# MAGIC   * Date ranges (birth date < encounter date)
# MAGIC   * Non-negative costs and amounts
# MAGIC   * Valid code values (procedure codes, encounter classes)
# MAGIC * **Standardization**: Normalizes names, addresses, codes
# MAGIC * **Completeness checks**: Ensures critical fields are populated
# MAGIC
# MAGIC #### Gold Layer
# MAGIC * **Aggregation validation**: Verifies calculated metrics
# MAGIC * **Consistency checks**: Cross-validates metrics across datasets
# MAGIC * **Outlier detection**: Identifies anomalous values
# MAGIC * **Trend validation**: Flags unusual period-over-period changes
# MAGIC
# MAGIC #### ML Layer
# MAGIC * **Feature validation**: Ensures features are within expected ranges
# MAGIC * **Model input validation**: Validates feature completeness for scoring
# MAGIC * **Prediction quality**: Monitors prediction distributions and confidence
# MAGIC * **Drift detection**: Tracks feature and prediction drift over time
# MAGIC
# MAGIC ### Data Quality Monitoring
# MAGIC
# MAGIC Recommended monitoring:
# MAGIC * Row counts at each layer
# MAGIC * Null percentages for critical fields
# MAGIC * Failed validation counts
# MAGIC * Processing latencies
# MAGIC * Data freshness (time since last update)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ML Pipeline Details
# MAGIC
# MAGIC ### Readmission Risk Prediction Model
# MAGIC
# MAGIC #### Objective
# MAGIC Predict the probability of a patient being readmitted within 30 days of discharge to enable proactive care management and intervention.
# MAGIC
# MAGIC #### Model Architecture
# MAGIC The pipeline implements a two-stage ML workflow:
# MAGIC
# MAGIC 1. **Feature Engineering** (`hospital_ml_features_readmission`)
# MAGIC    * Aggregates patient history across all encounters
# MAGIC    * Calculates temporal features (days since last visit, visit frequency)
# MAGIC    * Computes utilization metrics (total procedures, emergency visits)
# MAGIC    * Derives cost indicators (total spend, cost per encounter)
# MAGIC    * Creates demographic features (age groups, risk factors)
# MAGIC
# MAGIC 2. **Prediction Scoring** (`hospital_ml_predictions_readmission`)
# MAGIC    * Applies trained ML model to feature table
# MAGIC    * Generates risk probability (0-1 scale)
# MAGIC    * Classifies into risk tiers (Low/Medium/High)
# MAGIC    * Calculates confidence scores
# MAGIC
# MAGIC #### Feature Categories
# MAGIC
# MAGIC **Demographics**
# MAGIC * Age, gender, race, ethnicity
# MAGIC * Geographic location
# MAGIC
# MAGIC **Encounter History**
# MAGIC * Total lifetime encounters
# MAGIC * Recent encounter count (last 90 days)
# MAGIC * Emergency department visits
# MAGIC * Inpatient admissions
# MAGIC
# MAGIC **Procedures & Treatments**
# MAGIC * Total procedures performed
# MAGIC * High-risk procedure indicators
# MAGIC * Procedure complexity scores
# MAGIC
# MAGIC **Cost & Coverage**
# MAGIC * Total healthcare costs
# MAGIC * Out-of-pocket burden
# MAGIC * Insurance coverage adequacy
# MAGIC
# MAGIC **Temporal Patterns**
# MAGIC * Days since last encounter
# MAGIC * Encounter frequency (encounters per year)
# MAGIC * Care continuity indicators
# MAGIC
# MAGIC #### Risk Stratification
# MAGIC
# MAGIC * **Low Risk** (<0.3): Standard follow-up care
# MAGIC * **Medium Risk** (0.3-0.6): Enhanced monitoring and patient engagement
# MAGIC * **High Risk** (>0.6): Intensive care management and proactive intervention
# MAGIC
# MAGIC #### Model Refresh Strategy
# MAGIC
# MAGIC * Features refresh incrementally as new encounters are processed
# MAGIC * Predictions update whenever feature table is refreshed
# MAGIC * Model retraining recommended quarterly or when drift detected
# MAGIC * Performance monitoring on actual readmission outcomes
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Data Governance
# MAGIC %md
# MAGIC ## Data Governance
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC This pipeline implements comprehensive data governance using Unity Catalog to ensure data quality, security, privacy, and compliance throughout the data lifecycle. As a healthcare analytics platform processing Protected Health Information (PHI), governance is critical for HIPAA compliance and patient privacy protection.
# MAGIC
# MAGIC ### Unity Catalog Governance Framework
# MAGIC
# MAGIC All pipeline datasets are registered in Unity Catalog (`workspace.default.*`), providing centralized governance capabilities:
# MAGIC
# MAGIC * **Centralized Access Control**: Fine-grained permissions at catalog, schema, table, and column levels
# MAGIC * **Automated Audit Logging**: Complete audit trail of all data access and modifications
# MAGIC * **Data Lineage Tracking**: Full lineage from source files through all transformations
# MAGIC * **Metadata Management**: Rich metadata including descriptions, tags, and ownership
# MAGIC * **Data Discovery**: Searchable catalog for data discovery and collaboration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Classification and Tagging
# MAGIC
# MAGIC #### Classification Levels
# MAGIC
# MAGIC All datasets are classified according to sensitivity:
# MAGIC
# MAGIC **Restricted (Highest Sensitivity)**
# MAGIC * Contains PHI (Protected Health Information)
# MAGIC * Requires strictest access controls
# MAGIC * Examples: Patient names, SSN, addresses, medical records
# MAGIC * Tables: `hospital_bronze_patients`, `hospital_silver_patients`
# MAGIC
# MAGIC **Confidential**
# MAGIC * Contains sensitive business or clinical information
# MAGIC * Restricted to authorized personnel only
# MAGIC * Examples: Financial data, insurance claims, clinical outcomes
# MAGIC * Tables: All bronze, silver, and gold tables with patient/encounter data
# MAGIC
# MAGIC **Internal**
# MAGIC * General business data for internal use
# MAGIC * Standard access controls apply
# MAGIC * Examples: Organization reference data, aggregated statistics
# MAGIC * Tables: Organization and payer reference tables, aggregated analytics
# MAGIC
# MAGIC **Public**
# MAGIC * De-identified aggregate statistics
# MAGIC * Can be shared externally with approval
# MAGIC * Examples: De-identified population health metrics
# MAGIC * Tables: Anonymized reporting views (if created)
# MAGIC
# MAGIC #### Recommended Tags
# MAGIC
# MAGIC Apply Unity Catalog tags to all datasets:
# MAGIC
# MAGIC ```python
# MAGIC # Example: Tag tables with data classification
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE workspace.default.hospital_bronze_patients 
# MAGIC   SET TAGS ('classification' = 'restricted', 'contains_phi' = 'true', 'hipaa_regulated' = 'true')
# MAGIC """)
# MAGIC
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE workspace.default.hospital_gold_organization_performance 
# MAGIC   SET TAGS ('classification' = 'confidential', 'contains_phi' = 'false', 'hipaa_regulated' = 'true')
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **Standard Tags:**
# MAGIC * `classification`: restricted | confidential | internal | public
# MAGIC * `contains_phi`: true | false
# MAGIC * `hipaa_regulated`: true | false
# MAGIC * `data_owner`: Team or individual responsible
# MAGIC * `retention_period`: Data retention requirement (e.g., "7_years")
# MAGIC * `last_reviewed`: Date of last governance review
# MAGIC * `business_domain`: healthcare_analytics | patient_care | finance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Access Control Policies
# MAGIC
# MAGIC #### Role-Based Access Control (RBAC)
# MAGIC
# MAGIC Implement the principle of least privilege using Unity Catalog grants:
# MAGIC
# MAGIC **1. Data Engineers**
# MAGIC ```sql
# MAGIC -- Full access to bronze and silver layers for pipeline development
# MAGIC GRANT SELECT, MODIFY ON SCHEMA workspace.default TO `data_engineers`;
# MAGIC GRANT SELECT ON CATALOG workspace TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC **2. Data Analysts**
# MAGIC ```sql
# MAGIC -- Read-only access to gold layer for analytics
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_gold_* TO `data_analysts`;
# MAGIC GRANT USAGE ON SCHEMA workspace.default TO `data_analysts`;
# MAGIC GRANT USAGE ON CATALOG workspace TO `data_analysts`;
# MAGIC ```
# MAGIC
# MAGIC **3. Clinical Care Team**
# MAGIC ```sql
# MAGIC -- Access to ML predictions and patient journey analytics only
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_ml_predictions_readmission TO `clinical_care_team`;
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_gold_patient_journey_analysis TO `clinical_care_team`;
# MAGIC GRANT USAGE ON SCHEMA workspace.default TO `clinical_care_team`;
# MAGIC GRANT USAGE ON CATALOG workspace TO `clinical_care_team`;
# MAGIC ```
# MAGIC
# MAGIC **4. Finance Team**
# MAGIC ```sql
# MAGIC -- Access to cost and revenue analytics only
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_gold_patient_cost_analysis TO `finance_team`;
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_gold_payer_coverage_analysis TO `finance_team`;
# MAGIC GRANT SELECT ON TABLE workspace.default.hospital_gold_procedure_cost_analysis TO `finance_team`;
# MAGIC GRANT USAGE ON SCHEMA workspace.default TO `finance_team`;
# MAGIC GRANT USAGE ON CATALOG workspace TO `finance_team`;
# MAGIC ```
# MAGIC
# MAGIC **5. Compliance and Audit**
# MAGIC ```sql
# MAGIC -- Read-only access to all tables for auditing
# MAGIC GRANT SELECT ON SCHEMA workspace.default TO `compliance_team`;
# MAGIC GRANT USAGE ON CATALOG workspace TO `compliance_team`;
# MAGIC ```
# MAGIC
# MAGIC #### Column-Level Security
# MAGIC
# MAGIC Restrict access to sensitive columns:
# MAGIC
# MAGIC ```sql
# MAGIC -- Deny access to PII columns for analysts
# MAGIC ALTER TABLE workspace.default.hospital_silver_patients 
# MAGIC SET ROW FILTER deny_pii ON (ssn, drivers_license, passport) FOR `data_analysts`;
# MAGIC
# MAGIC -- Create masked views for non-privileged users
# MAGIC CREATE OR REPLACE VIEW workspace.default.hospital_patients_masked AS
# MAGIC SELECT 
# MAGIC   patient_id,
# MAGIC   'XXX-XX-' || RIGHT(ssn, 4) as ssn_masked,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   age,
# MAGIC   gender,
# MAGIC   city,
# MAGIC   state,
# MAGIC   -- Exclude sensitive fields
# MAGIC   NULL as drivers_license,
# MAGIC   NULL as passport,
# MAGIC   NULL as address
# MAGIC FROM workspace.default.hospital_silver_patients;
# MAGIC
# MAGIC GRANT SELECT ON VIEW workspace.default.hospital_patients_masked TO `data_analysts`;
# MAGIC ```
# MAGIC
# MAGIC #### Row-Level Security
# MAGIC
# MAGIC Implement row filters for data isolation:
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: Restrict access to specific organizations
# MAGIC CREATE FUNCTION workspace.default.filter_by_organization(org_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN org_id IN (SELECT organization_id FROM workspace.default.user_org_access WHERE user_id = current_user());
# MAGIC
# MAGIC ALTER TABLE workspace.default.hospital_silver_encounters
# MAGIC SET ROW FILTER filter_by_organization(organization_id) FOR `regional_analysts`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### HIPAA Compliance
# MAGIC
# MAGIC #### Protected Health Information (PHI) Handling
# MAGIC
# MAGIC The pipeline processes the following PHI elements:
# MAGIC
# MAGIC **Direct Identifiers (18 HIPAA identifiers):**
# MAGIC 1. Names (first, last)
# MAGIC 2. Geographic subdivisions (address, city, zip)
# MAGIC 3. Dates (birth date, encounter dates)
# MAGIC 4. Phone numbers
# MAGIC 5. Email addresses (if present)
# MAGIC 6. Social Security Numbers
# MAGIC 7. Medical record numbers (patient_id)
# MAGIC 8. Health plan beneficiary numbers
# MAGIC 9. Account numbers
# MAGIC 10. Certificate/license numbers (drivers_license, passport)
# MAGIC 11. Vehicle identifiers
# MAGIC 12. Device identifiers and serial numbers
# MAGIC 13. Web URLs
# MAGIC 14. IP addresses
# MAGIC 15. Biometric identifiers
# MAGIC 16. Full-face photos
# MAGIC 17. Other unique identifying numbers
# MAGIC
# MAGIC **Safeguards Implemented:**
# MAGIC
# MAGIC 1. **Encryption**
# MAGIC    * Encryption at rest (AWS S3 server-side encryption)
# MAGIC    * Encryption in transit (TLS 1.2+)
# MAGIC    * Delta Lake encrypted tables
# MAGIC
# MAGIC 2. **Access Controls**
# MAGIC    * Role-based access control (RBAC)
# MAGIC    * Multi-factor authentication (MFA) required
# MAGIC    * Session timeouts and automatic logoff
# MAGIC    * Minimum necessary access principle
# MAGIC
# MAGIC 3. **Audit Logging**
# MAGIC    * All PHI access logged to Unity Catalog audit logs
# MAGIC    * Logs retained for 7 years (HIPAA requirement)
# MAGIC    * Regular audit log reviews
# MAGIC    * Automated alerts for suspicious access patterns
# MAGIC
# MAGIC 4. **Data Minimization**
# MAGIC    * Only necessary PHI collected and retained
# MAGIC    * De-identification for analytics when possible
# MAGIC    * Aggregation to prevent individual identification
# MAGIC
# MAGIC #### De-Identification Standards
# MAGIC
# MAGIC For analytics that don't require individual-level PHI, implement de-identification:
# MAGIC
# MAGIC **Safe Harbor Method** (Remove 18 identifiers):
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW workspace.default.hospital_patients_deidentified AS
# MAGIC SELECT 
# MAGIC   -- Replace patient_id with anonymous token
# MAGIC   SHA2(patient_id, 256) as patient_token,
# MAGIC   -- Generalize age to age groups
# MAGIC   CASE 
# MAGIC     WHEN age < 18 THEN 'under_18'
# MAGIC     WHEN age BETWEEN 18 AND 35 THEN '18_35'
# MAGIC     WHEN age BETWEEN 36 AND 50 THEN '36_50'
# MAGIC     WHEN age BETWEEN 51 AND 65 THEN '51_65'
# MAGIC     WHEN age > 65 THEN 'over_65'
# MAGIC   END as age_group,
# MAGIC   gender,
# MAGIC   race,
# MAGIC   ethnicity,
# MAGIC   -- Generalize geography to state only
# MAGIC   state,
# MAGIC   -- Remove all direct identifiers
# MAGIC   -- NO: ssn, name, address, city, zip, phone, dates (except year)
# MAGIC   YEAR(birth_date) as birth_year
# MAGIC FROM workspace.default.hospital_silver_patients;
# MAGIC ```
# MAGIC
# MAGIC **Statistical De-identification**:
# MAGIC * K-anonymity (k ≥ 5): Each record indistinguishable from at least 4 others
# MAGIC * L-diversity: Sensitive attributes have at least L distinct values per group
# MAGIC * Differential privacy: Add statistical noise to prevent individual identification
# MAGIC
# MAGIC #### Business Associate Agreements (BAA)
# MAGIC
# MAGIC * Databricks BAA executed for cloud platform
# MAGIC * All pipeline users covered under organizational BAA
# MAGIC * Third-party data processors require BAA
# MAGIC * Regular BAA compliance reviews
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Audit Logging and Monitoring
# MAGIC
# MAGIC #### Unity Catalog Audit Logs
# MAGIC
# MAGIC All data access is automatically logged:
# MAGIC
# MAGIC ```sql
# MAGIC -- Query audit logs for PHI access
# MAGIC SELECT 
# MAGIC   event_time,
# MAGIC   user_identity.email as user,
# MAGIC   request_params.full_name_arg as table_accessed,
# MAGIC   action_name,
# MAGIC   request_params.sql_query as query_text
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('getTable', 'readTable', 'SELECT')
# MAGIC   AND request_params.full_name_arg LIKE 'workspace.default.hospital_%'
# MAGIC   AND event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC ```
# MAGIC
# MAGIC #### Monitoring and Alerts
# MAGIC
# MAGIC **Automated Alerts:**
# MAGIC
# MAGIC 1. **Unauthorized Access Attempts**
# MAGIC    * Alert when access denied due to insufficient permissions
# MAGIC    * Trigger security review for repeated violations
# MAGIC
# MAGIC 2. **Bulk Data Exports**
# MAGIC    * Alert when large volumes of PHI exported
# MAGIC    * Require justification for exports >10,000 records
# MAGIC
# MAGIC 3. **After-Hours Access**
# MAGIC    * Monitor and review PHI access outside business hours
# MAGIC    * Flag unusual access patterns
# MAGIC
# MAGIC 4. **Privileged User Activity**
# MAGIC    * Enhanced monitoring for admin/super-user accounts
# MAGIC    * All admin actions logged and reviewed
# MAGIC
# MAGIC **Audit Report Schedule:**
# MAGIC
# MAGIC * **Daily**: Failed access attempts, bulk exports
# MAGIC * **Weekly**: PHI access by user, unusual patterns
# MAGIC * **Monthly**: Comprehensive access review by table
# MAGIC * **Quarterly**: Full compliance audit, access recertification
# MAGIC * **Annual**: HIPAA security risk assessment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Masking and Privacy
# MAGIC
# MAGIC #### Dynamic Data Masking
# MAGIC
# MAGIC Implement dynamic masking for non-production environments:
# MAGIC
# MAGIC ```python
# MAGIC # Example: Create masked development tables
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # Load production patient data
# MAGIC patients_prod = spark.table("workspace.default.hospital_silver_patients")
# MAGIC
# MAGIC # Apply masking transformations
# MAGIC patients_dev = patients_prod.select(
# MAGIC     F.col("patient_id"),
# MAGIC     # Mask names
# MAGIC     F.concat(F.lit("Patient_"), F.col("patient_id")).alias("first_name"),
# MAGIC     F.lit("Anonymous").alias("last_name"),
# MAGIC     # Mask SSN
# MAGIC     F.concat(F.lit("XXX-XX-"), F.substring(F.col("ssn"), -4, 4)).alias("ssn"),
# MAGIC     # Generalize dates (remove day)
# MAGIC     F.trunc(F.col("birth_date"), "MM").alias("birth_date"),
# MAGIC     # Mask address
# MAGIC     F.lit("123 Main St").alias("address"),
# MAGIC     # Keep analytical fields
# MAGIC     F.col("age"),
# MAGIC     F.col("gender"),
# MAGIC     F.col("race"),
# MAGIC     F.col("ethnicity"),
# MAGIC     F.col("city"),
# MAGIC     F.col("state")
# MAGIC )
# MAGIC
# MAGIC # Write to dev catalog
# MAGIC patients_dev.write.mode("overwrite").saveAsTable("workspace_dev.default.hospital_silver_patients")
# MAGIC ```
# MAGIC
# MAGIC #### Privacy-Enhancing Techniques
# MAGIC
# MAGIC 1. **Tokenization**: Replace sensitive values with random tokens
# MAGIC 2. **Pseudonymization**: Replace identifiers with consistent pseudonyms
# MAGIC 3. **Generalization**: Replace specific values with broader categories
# MAGIC 4. **Suppression**: Remove extremely rare values that risk identification
# MAGIC 5. **Noise Addition**: Add statistical noise (differential privacy)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Lineage and Impact Analysis
# MAGIC
# MAGIC #### Lineage Tracking
# MAGIC
# MAGIC Unity Catalog automatically tracks data lineage:
# MAGIC
# MAGIC * **Column-level lineage**: Track transformations from source to destination
# MAGIC * **Table-level lineage**: Visualize dependencies across layers
# MAGIC * **Query-level lineage**: See which queries access which tables
# MAGIC
# MAGIC **Access Lineage:**
# MAGIC ```sql
# MAGIC -- View lineage for a specific table
# MAGIC SELECT * FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = 'workspace.default.hospital_ml_predictions_readmission';
# MAGIC ```
# MAGIC
# MAGIC #### Impact Analysis
# MAGIC
# MAGIC Before making changes, assess downstream impact:
# MAGIC
# MAGIC 1. **Schema Changes**: Identify all dependent views and tables
# MAGIC 2. **Access Control Changes**: Understand who will be affected
# MAGIC 3. **Data Quality Issues**: Trace impact through pipeline layers
# MAGIC 4. **Security Incidents**: Quickly identify exposed data scope
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Governance Roles and Responsibilities
# MAGIC
# MAGIC #### Data Governance Team
# MAGIC
# MAGIC **Data Owner** (tatendanamasasu514@gmail.com)
# MAGIC * Ultimate accountability for data quality and security
# MAGIC * Approves access requests
# MAGIC * Defines retention and archival policies
# MAGIC * Reviews compliance reports
# MAGIC
# MAGIC **Data Stewards** (Assigned by Domain)
# MAGIC * Patient Data Steward: Manages patient demographic data
# MAGIC * Clinical Data Steward: Manages encounter and procedure data
# MAGIC * Financial Data Steward: Manages payer and cost data
# MAGIC * ML Data Steward: Manages features and predictions
# MAGIC
# MAGIC Responsibilities:
# MAGIC * Define and maintain data quality rules
# MAGIC * Review and approve schema changes
# MAGIC * Coordinate with data consumers
# MAGIC * Monitor data quality metrics
# MAGIC
# MAGIC **Data Custodians** (Data Engineering Team)
# MAGIC * Implement technical controls
# MAGIC * Maintain pipeline infrastructure
# MAGIC * Execute access provisioning
# MAGIC * Perform regular backups
# MAGIC
# MAGIC **Compliance Officer**
# MAGIC * Oversee HIPAA compliance
# MAGIC * Conduct regular audits
# MAGIC * Manage incident response
# MAGIC * Coordinate regulatory reporting
# MAGIC
# MAGIC #### Decision Authority Matrix
# MAGIC
# MAGIC | Decision | Data Owner | Data Steward | Data Custodian | Compliance Officer |
# MAGIC |----------|------------|--------------|----------------|--------------------|
# MAGIC | Grant PHI Access | Approve | Recommend | Implement | Review |
# MAGIC | Schema Changes | Approve | Recommend | Implement | Review |
# MAGIC | Data Retention Policy | Approve | Recommend | Implement | Review |
# MAGIC | De-identification Methods | Consult | Recommend | Implement | Approve |
# MAGIC | Security Incident Response | Inform | Support | Execute | Lead |
# MAGIC | New Use Case Approval | Approve | Assess | Support | Review |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Compliance and Certification
# MAGIC
# MAGIC #### Regulatory Requirements
# MAGIC
# MAGIC **HIPAA (Health Insurance Portability and Accountability Act)**
# MAGIC * Privacy Rule: PHI protection standards
# MAGIC * Security Rule: Technical safeguards
# MAGIC * Breach Notification Rule: Incident response
# MAGIC * Enforcement Rule: Penalty structure
# MAGIC
# MAGIC **State Privacy Laws**
# MAGIC * California CCPA/CPRA
# MAGIC * State-specific health privacy laws
# MAGIC * Varies by patient location
# MAGIC
# MAGIC #### Compliance Checklist
# MAGIC
# MAGIC **Quarterly Review:**
# MAGIC - [ ] Access permissions reviewed and recertified
# MAGIC - [ ] Audit logs reviewed for anomalies
# MAGIC - [ ] Data classification tags up to date
# MAGIC - [ ] No unauthorized PHI exports detected
# MAGIC - [ ] Encryption verified on all datasets
# MAGIC - [ ] BAAs current with all vendors
# MAGIC - [ ] Staff training completion verified
# MAGIC - [ ] Incident response plan tested
# MAGIC
# MAGIC **Annual Certification:**
# MAGIC - [ ] HIPAA Security Risk Assessment completed
# MAGIC - [ ] Third-party security assessments reviewed
# MAGIC - [ ] Disaster recovery tested
# MAGIC - [ ] Data retention policies enforced
# MAGIC - [ ] Compliance documentation updated
# MAGIC - [ ] Management attestation signed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Governance Best Practices
# MAGIC
# MAGIC 1. **Principle of Least Privilege**: Grant minimum necessary access
# MAGIC 2. **Defense in Depth**: Multiple layers of security controls
# MAGIC 3. **Privacy by Design**: Build privacy into pipeline from the start
# MAGIC 4. **Continuous Monitoring**: Real-time governance, not point-in-time
# MAGIC 5. **Documentation**: Maintain current governance documentation
# MAGIC 6. **Training**: Regular privacy and security training for all users
# MAGIC 7. **Incident Response**: Defined breach notification procedures
# MAGIC 8. **Vendor Management**: Vet all third parties handling PHI
# MAGIC 9. **Data Minimization**: Collect and retain only necessary data
# MAGIC 10. **Transparency**: Clear data usage policies communicated to patients
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ### Querying Pipeline Datasets
# MAGIC
# MAGIC All datasets are available in Unity Catalog under `workspace.default.*` and can be queried using SQL or PySpark.
# MAGIC
# MAGIC #### Example 1: High-Risk Patients for Readmission

# COMMAND ----------

# DBTITLE 1,Query high-risk patients
# MAGIC %sql
# MAGIC -- Find high-risk patients for readmission with their contact info and recent encounters
# MAGIC SELECT 
# MAGIC   p.patient_id,
# MAGIC   p.first_name,
# MAGIC   p.last_name,
# MAGIC   p.age,
# MAGIC   p.gender,
# MAGIC   pred.readmission_risk_score,
# MAGIC   pred.risk_tier,
# MAGIC   enc.total_encounters,
# MAGIC   enc.total_cost,
# MAGIC   enc.last_encounter_date
# MAGIC FROM workspace.default.hospital_ml_predictions_readmission pred
# MAGIC JOIN workspace.default.hospital_silver_patients p ON pred.patient_id = p.patient_id
# MAGIC JOIN workspace.default.hospital_gold_patient_encounter_summary enc ON pred.patient_id = enc.patient_id
# MAGIC WHERE pred.risk_tier = 'High'
# MAGIC ORDER BY pred.readmission_risk_score DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 2: Payer Performance Analysis

# COMMAND ----------

# DBTITLE 1,Analyze payer coverage performance
# MAGIC %sql
# MAGIC -- Analyze insurance payer coverage rates and patient out-of-pocket burden
# MAGIC SELECT 
# MAGIC   p.payer_name,
# MAGIC   p.total_covered_encounters,
# MAGIC   p.total_amount_covered,
# MAGIC   p.total_amount_uncovered,
# MAGIC   ROUND(p.total_amount_covered / (p.total_amount_covered + p.total_amount_uncovered) * 100, 2) as coverage_rate_pct,
# MAGIC   ROUND(p.total_amount_uncovered / p.total_covered_encounters, 2) as avg_patient_oop_per_encounter
# MAGIC FROM workspace.default.hospital_gold_payer_coverage_analysis p
# MAGIC ORDER BY coverage_rate_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 3: Procedure Cost Trends Over Time

# COMMAND ----------

# DBTITLE 1,Analyze procedure volume and cost trends
# MAGIC %sql
# MAGIC -- Analyze top procedures by volume with monthly trends
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', procedure_date) as month,
# MAGIC   procedure_description,
# MAGIC   COUNT(*) as procedure_count,
# MAGIC   ROUND(AVG(procedure_cost), 2) as avg_cost,
# MAGIC   ROUND(SUM(procedure_cost), 2) as total_revenue
# MAGIC FROM workspace.default.hospital_gold_procedure_trends
# MAGIC WHERE procedure_date >= DATE_SUB(CURRENT_DATE(), 90)  -- Last 90 days
# MAGIC GROUP BY DATE_TRUNC('month', procedure_date), procedure_description
# MAGIC HAVING procedure_count >= 10  -- Filter to high-volume procedures
# MAGIC ORDER BY month DESC, procedure_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 4: Patient Journey Analysis Using PySpark

# COMMAND ----------

# DBTITLE 1,Analyze patient care pathways
# Analyze patient journey patterns and identify frequent readmissions
from pyspark.sql import functions as F

# Load patient journey data
journey_df = spark.table("workspace.default.hospital_gold_patient_journey_analysis")

# Identify patients with frequent readmissions (>2 encounters in 30 days)
frequent_readmissions = (
    journey_df
    .filter(F.col("total_encounters") > 2)
    .filter(F.col("avg_days_between_encounters") < 30)
    .select(
        "patient_id",
        "total_encounters",
        "first_encounter_date",
        "last_encounter_date",
        "avg_days_between_encounters",
        "has_readmission_within_30_days"
    )
    .orderBy(F.col("avg_days_between_encounters").asc())
)

# Display results
display(frequent_readmissions.limit(50))

# Calculate summary statistics
print(f"Total patients with frequent readmissions: {frequent_readmissions.count()}")
print(f"Average encounters: {frequent_readmissions.agg(F.avg('total_encounters')).collect()[0][0]:.2f}")
print(f"Average days between encounters: {frequent_readmissions.agg(F.avg('avg_days_between_encounters')).collect()[0][0]:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Operations
# MAGIC
# MAGIC ### Running the Pipeline
# MAGIC
# MAGIC The pipeline can be triggered in several ways:
# MAGIC
# MAGIC 1. **Manual Trigger**: Via Databricks UI or API
# MAGIC    ```python
# MAGIC    # Using Databricks API
# MAGIC    import requests
# MAGIC    
# MAGIC    response = requests.post(
# MAGIC        "https://<databricks-instance>/api/2.0/pipelines/306fdf43-9f18-4f66-8587-7408ac4f8d7a/updates",
# MAGIC        headers={"Authorization": "Bearer <token>"},
# MAGIC        json={"full_refresh": False}
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 2. **Scheduled Execution**: Set up a job to run the pipeline on a schedule
# MAGIC 3. **Event-Driven**: Trigger on new data arrival (using Auto Loader or event notifications)
# MAGIC
# MAGIC ### Refresh Strategies
# MAGIC
# MAGIC #### Full Refresh
# MAGIC * Rebuilds all datasets from scratch
# MAGIC * Use when: Schema changes, data quality issues, or major transformations updated
# MAGIC * Command: Set `full_refresh=True` in the API call
# MAGIC
# MAGIC #### Incremental Refresh (Default)
# MAGIC * Processes only new/changed data
# MAGIC * Bronze/Silver streaming tables: Automatically process new records
# MAGIC * Gold/ML materialized views: Incremental refresh based on upstream changes
# MAGIC * Recommended for regular operations
# MAGIC
# MAGIC ### Monitoring and Alerts
# MAGIC
# MAGIC **Key Metrics to Monitor:**
# MAGIC * Pipeline execution status (success/failure)
# MAGIC * Execution duration trends
# MAGIC * Row counts by dataset
# MAGIC * Data freshness (time since last update)
# MAGIC * Failed validation counts
# MAGIC * Cluster resource utilization
# MAGIC
# MAGIC **Recommended Alerts:**
# MAGIC * Pipeline failure
# MAGIC * Execution time exceeds threshold (e.g., >30 minutes)
# MAGIC * Row count drops significantly (>20% decrease)
# MAGIC * Data freshness exceeds SLA (e.g., >4 hours old)
# MAGIC * High validation failure rate (>5%)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Maintenance Guidelines
# MAGIC
# MAGIC ### Regular Maintenance Tasks
# MAGIC
# MAGIC #### Daily
# MAGIC * ✅ Verify pipeline completed successfully
# MAGIC * ✅ Check data freshness of gold/ML tables
# MAGIC * ✅ Review any validation failures
# MAGIC * ✅ Monitor high-risk patient counts (should be stable)
# MAGIC
# MAGIC #### Weekly
# MAGIC * 🔍 Review pipeline execution trends
# MAGIC * 🔍 Analyze data quality metrics
# MAGIC * 🔍 Check for anomalies in key business metrics
# MAGIC * 🔍 Validate ML model predictions against outcomes
# MAGIC
# MAGIC #### Monthly
# MAGIC * 📊 Performance optimization review
# MAGIC * 📊 Storage and cost analysis
# MAGIC * 📊 Update documentation for any pipeline changes
# MAGIC * 📊 Review and update data retention policies
# MAGIC
# MAGIC #### Quarterly
# MAGIC * 🔄 ML model retraining and validation
# MAGIC * 🔄 Comprehensive data quality audit
# MAGIC * 🔄 Schema evolution assessment
# MAGIC * 🔄 Disaster recovery testing
# MAGIC
# MAGIC ### Troubleshooting Common Issues
# MAGIC
# MAGIC #### Issue: Pipeline Fails to Start
# MAGIC **Possible Causes:**
# MAGIC * Serverless compute not available
# MAGIC * Invalid pipeline configuration
# MAGIC * Source data not accessible
# MAGIC
# MAGIC **Resolution:**
# MAGIC 1. Check pipeline configuration in UI
# MAGIC 2. Verify source file paths exist
# MAGIC 3. Confirm compute cluster permissions
# MAGIC 4. Review event logs for error details
# MAGIC
# MAGIC #### Issue: Streaming Tables Not Updating
# MAGIC **Possible Causes:**
# MAGIC * No new source data
# MAGIC * Auto Loader checkpoint corruption
# MAGIC * Schema mismatch
# MAGIC
# MAGIC **Resolution:**
# MAGIC 1. Check source data location for new files
# MAGIC 2. Verify Auto Loader checkpoint path
# MAGIC 3. Review schema evolution settings
# MAGIC 4. Consider resetting checkpoint for full refresh
# MAGIC
# MAGIC #### Issue: Materialized Views Stale
# MAGIC **Possible Causes:**
# MAGIC * Upstream dependencies not refreshed
# MAGIC * Manual refresh required
# MAGIC * Incremental refresh logic issue
# MAGIC
# MAGIC **Resolution:**
# MAGIC 1. Check upstream table update timestamps
# MAGIC 2. Trigger manual refresh of the view
# MAGIC 3. Consider full refresh if incremental logic changed
# MAGIC 4. Review view definition for dependency issues
# MAGIC
# MAGIC #### Issue: ML Predictions Look Incorrect
# MAGIC **Possible Causes:**
# MAGIC * Feature drift
# MAGIC * Model needs retraining
# MAGIC * Data quality issues in features
# MAGIC
# MAGIC **Resolution:**
# MAGIC 1. Validate feature table data quality
# MAGIC 2. Check for null or outlier values in features
# MAGIC 3. Compare feature distributions to training data
# MAGIC 4. Retrain model with recent data
# MAGIC 5. Review prediction distribution for anomalies
# MAGIC
# MAGIC ### Schema Evolution
# MAGIC
# MAGIC When source data schema changes:
# MAGIC
# MAGIC 1. **Additive Changes** (new columns):
# MAGIC    * No action required for bronze tables (Auto Loader handles)
# MAGIC    * Update silver transformations to process new columns
# MAGIC    * Add to gold analytics if business-relevant
# MAGIC
# MAGIC 2. **Breaking Changes** (column removal, type changes):
# MAGIC    * Update bronze table definitions
# MAGIC    * Modify silver transformation logic
# MAGIC    * Full refresh of affected datasets
# MAGIC    * Update downstream gold/ML tables
# MAGIC
# MAGIC 3. **Testing Schema Changes:**
# MAGIC    * Use development mode for testing
# MAGIC    * Validate with sample data first
# MAGIC    * Monitor for data quality issues post-deployment
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC **Optimization Techniques:**
# MAGIC
# MAGIC 1. **Partitioning**:
# MAGIC    * Consider partitioning large tables by date
# MAGIC    * Benefits: Faster queries, easier maintenance
# MAGIC    * Apply to: encounter tables, procedure trends
# MAGIC
# MAGIC 2. **Caching**:
# MAGIC    * Cache frequently accessed dimension tables
# MAGIC    * Benefits: Faster joins, reduced compute
# MAGIC    * Apply to: patient demographics, organizations, payers
# MAGIC
# MAGIC 3. **Z-Ordering**:
# MAGIC    * Optimize file layout for common query patterns
# MAGIC    * Benefits: Faster filtered queries
# MAGIC    * Apply to: patient_id, encounter_date columns
# MAGIC
# MAGIC 4. **Compute Resources**:
# MAGIC    * Monitor cluster utilization
# MAGIC    * Adjust serverless settings if needed
# MAGIC    * Consider dedicated compute for large refreshes
# MAGIC
# MAGIC ### Data Retention and Archival
# MAGIC
# MAGIC **Recommended Retention Policies:**
# MAGIC
# MAGIC * **Bronze Layer**: 90 days (raw data, can be reprocessed)
# MAGIC * **Silver Layer**: 2 years (cleansed data for auditing)
# MAGIC * **Gold Layer**: 5 years (analytics and reporting)
# MAGIC * **ML Predictions**: 1 year (for model monitoring)
# MAGIC
# MAGIC **Archival Strategy:**
# MAGIC * Move aged data to cold storage (S3 Glacier, Azure Archive)
# MAGIC * Maintain metadata catalog for archived data
# MAGIC * Document retrieval procedures for compliance
# MAGIC
# MAGIC ### Security and Compliance
# MAGIC
# MAGIC **Access Control:**
# MAGIC * Use Unity Catalog fine-grained permissions
# MAGIC * Implement row-level and column-level security
# MAGIC * Regular access reviews (quarterly)
# MAGIC * Audit trail for sensitive data access
# MAGIC
# MAGIC **Data Privacy:**
# MAGIC * PII masking in non-production environments
# MAGIC * Encryption at rest and in transit
# MAGIC * Data anonymization for ML training
# MAGIC * HIPAA compliance for patient data
# MAGIC
# MAGIC **Compliance Requirements:**
# MAGIC * Document data lineage for audits
# MAGIC * Maintain data quality reports
# MAGIC * Regular security assessments
# MAGIC * Disaster recovery planning and testing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Contact and Support
# MAGIC
# MAGIC **Pipeline Owner**: tatendanamasasu514@gmail.com
# MAGIC
# MAGIC **Support Channels:**
# MAGIC * Databricks Workspace: `/Users/tatendanamasasu514@gmail.com/New Pipeline 2026-06-15 20:15`
# MAGIC * Pipeline Monitoring: [View Pipeline](#pipeline-monitoring-306fdf43-9f18-4f66-8587-7408ac4f8d7a)
# MAGIC * Documentation Updates: Edit this notebook
# MAGIC
# MAGIC **Additional Resources:**
# MAGIC * [Databricks Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC * [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC * [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Version History
# MAGIC
# MAGIC | Version | Date | Changes | Author |
# MAGIC |---------|------|---------|--------|
# MAGIC | 1.0 | 2026-06-17 | Initial documentation | Genie Code |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Last Updated**: June 17, 2026  
# MAGIC **Pipeline Status**: ✅ Active and Healthy
