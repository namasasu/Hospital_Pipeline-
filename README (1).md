# Hospital Healthcare Analytics Pipeline

> A comprehensive healthcare data platform built on Databricks Lakeflow using Spark Declarative Pipelines (formerly Delta Live Tables)

[![Databricks](https://img.shields.io/badge/Databricks-Lakeflow-FF3621?logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00ADD8)](https://databricks.com/product/unity-catalog)
[![HIPAA](https://img.shields.io/badge/HIPAA-Compliant-green)](https://www.hhs.gov/hipaa)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)](https://www.databricks.com/glossary/medallion-architecture)

## Overview

This pipeline implements an end-to-end healthcare analytics platform that processes patient encounters, medical procedures, insurance payer data, and organizational information. It delivers real-time data ingestion, quality-assured transformations, advanced analytics, and machine learning-powered readmission risk predictions.

**Pipeline ID**: `306fdf43-9f18-4f66-8587-7408ac4f8d7a`  
**Catalog**: `workspace.default`  
**Compute**: Serverless with Photon  
**Total Datasets**: 21 (5 Bronze, 10 Silver, 6 Gold, 2 ML)

## Key Features

* **🏥 Healthcare Data Integration**: Comprehensive patient, encounter, procedure, payer, and organization data
* **📊 Medallion Architecture**: Bronze → Silver → Gold → ML data quality progression
* **⚡ Real-time Streaming**: Streaming tables for continuous data ingestion and processing
* **🤖 ML-Powered Predictions**: 30-day readmission risk scoring with risk tier classification
* **✅ Data Quality**: Built-in validation, standardization, and quality checks at every layer
* **🔒 HIPAA Compliance**: Full governance framework with PHI protection and audit logging
* **📈 Advanced Analytics**: Pre-built analytics for cost, utilization, patient journey, and trends
* **🔐 Unity Catalog Governance**: Fine-grained access control, lineage tracking, and audit logs

## Architecture

### Medallion Data Flow

```
CSV Files (Hospital System)
         ↓
┌─────────────────────┐
│   BRONZE LAYER      │  ← Raw ingestion (5 streaming tables)
│   • Encounters      │
│   • Procedures      │
│   • Patients        │
│   • Organizations   │
│   • Payers          │
└─────────────────────┘
         ↓
┌─────────────────────┐
│   SILVER LAYER      │  ← Cleansed & enriched (10 streaming tables)
│   • Demographics    │
│   • Validated data  │
│   • Standardized    │
└─────────────────────┘
         ↓
┌─────────────────────┐
│   GOLD LAYER        │  ← Analytics (6 materialized views)
│   • Cost analysis   │
│   • Patient journey │
│   • Trends          │
└─────────────────────┘
         ↓
┌─────────────────────┐
│   ML LAYER          │  ← Predictions (2 materialized views)
│   • Features        │
│   • Risk scoring    │
└─────────────────────┘
```

### Dataset Summary

| Layer  | Count | Type               | Purpose                           |
|--------|-------|--------------------|-----------------------------------|
| Bronze | 5     | Streaming Tables   | Raw data ingestion                |
| Silver | 10    | Streaming Tables   | Cleansed, validated, enriched     |
| Gold   | 6     | Materialized Views | Business analytics & aggregations |
| ML     | 2     | Materialized Views | Feature engineering & predictions |

## Technology Stack

* **Platform**: Databricks Lakehouse
* **Pipeline**: Spark Declarative Pipelines (Lakeflow)
* **Compute**: Serverless with Photon acceleration
* **Storage**: Delta Lake
* **Governance**: Unity Catalog
* **Languages**: Python, SQL
* **ML Framework**: MLflow (readmission risk model)

## Pipeline Datasets

### Bronze Layer - Raw Ingestion
* `hospital_bronze_encounters` - Patient encounter records
* `hospital_bronze_procedures` - Medical procedures performed
* `hospital_bronze_patients` - Patient demographic and health data
* `hospital_bronze_organizations` - Healthcare facilities and providers
* `hospital_bronze_payers` - Insurance payer information

### Silver Layer - Cleansed Data
* `hospital_silver_patient_demographics` - Age-enriched patient demographics
* `hospital_silver_encounters` - Validated encounter data
* `hospital_silver_procedures` - Standardized procedure records
* `hospital_silver_organizations` - Cleaned organization data
* `hospital_silver_payers` - Validated payer information
* `hospital_silver_patients` - Quality-checked patient data
* `hospital_silver_encounter_procedures_enriched` - Joined encounter-procedure view (60,922 rows)

### Gold Layer - Analytics
* `hospital_gold_organization_performance` - Hospital KPIs and performance metrics
* `hospital_gold_payer_coverage_analysis` - Insurance coverage and claim analysis
* `hospital_gold_patient_encounter_summary` - Patient-level encounter aggregations (974 patients)
* `hospital_gold_patient_cost_analysis` - Healthcare cost and spending analysis
* `hospital_gold_patient_journey_analysis` - Care pathway and readmission tracking
* `hospital_gold_procedure_cost_analysis` - Procedure-level cost analytics (254 types)
* `hospital_gold_procedure_trends` - Time-series procedure trends (41,035 records)

### ML Layer - Predictions
* `hospital_ml_features_readmission` - Feature table for readmission risk model
* `hospital_ml_predictions_readmission` - 30-day readmission risk scores and tiers

## Data Governance

### HIPAA Compliance
* ✅ PHI protection with encryption at rest and in transit
* ✅ Role-based access control (RBAC) via Unity Catalog
* ✅ Complete audit logging of all data access
* ✅ Data de-identification capabilities
* ✅ Column-level and row-level security
* ✅ Business Associate Agreements (BAA) in place

### Access Control Roles
* **Data Engineers**: Full access to bronze/silver layers
* **Data Analysts**: Read-only access to gold analytics
* **Clinical Care Team**: Access to ML predictions and patient journey
* **Finance Team**: Access to cost and revenue analytics
* **Compliance Team**: Read-only access for auditing

### Data Classification
* **Restricted**: PHI data (patient names, SSN, medical records)
* **Confidential**: Financial and clinical information
* **Internal**: Organization reference data
* **Public**: De-identified aggregate statistics

## Getting Started

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Serverless compute access or cluster with Photon
* Source data in CSV format (encounters, procedures, patients, organizations, payers)
* Appropriate permissions for catalog and schema

### Pipeline Setup

1. **Clone this repository** to your Databricks workspace
2. **Configure pipeline settings**:
   * Catalog: `workspace`
   * Schema: `default`
   * Source path: Update CSV file locations
3. **Set up Unity Catalog permissions**
4. **Run initial pipeline update** (full refresh recommended)

### Running the Pipeline

**Via Databricks UI:**
1. Navigate to Lakeflow > Pipelines
2. Select "New Pipeline 2026-06-15 20:15"
3. Click "Start" to trigger update

**Via API:**
```python
import requests

response = requests.post(
    "https://<databricks-instance>/api/2.0/pipelines/306fdf43-9f18-4f66-8587-7408ac4f8d7a/updates",
    headers={"Authorization": "Bearer <token>"},
    json={"full_refresh": False}
)
```

**Via CLI:**
```bash
databricks pipelines start-update 306fdf43-9f18-4f66-8587-7408ac4f8d7a
```

## Usage Examples

### Query High-Risk Patients
```sql
SELECT 
  p.patient_id,
  p.first_name,
  p.last_name,
  pred.readmission_risk_score,
  pred.risk_tier
FROM workspace.default.hospital_ml_predictions_readmission pred
JOIN workspace.default.hospital_silver_patients p ON pred.patient_id = p.patient_id
WHERE pred.risk_tier = 'High'
ORDER BY pred.readmission_risk_score DESC
LIMIT 100;
```

### Analyze Payer Performance
```sql
SELECT 
  payer_name,
  total_covered_encounters,
  ROUND(total_amount_covered / (total_amount_covered + total_amount_uncovered) * 100, 2) as coverage_rate_pct
FROM workspace.default.hospital_gold_payer_coverage_analysis
ORDER BY coverage_rate_pct DESC;
```

### Track Procedure Trends
```sql
SELECT 
  DATE_TRUNC('month', procedure_date) as month,
  procedure_description,
  COUNT(*) as procedure_count,
  ROUND(AVG(procedure_cost), 2) as avg_cost
FROM workspace.default.hospital_gold_procedure_trends
WHERE procedure_date >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY 1, 2
ORDER BY month DESC, procedure_count DESC;
```

## Machine Learning

### Readmission Risk Model

The pipeline includes a 30-day readmission risk prediction model that:
* **Identifies high-risk patients** for proactive intervention
* **Stratifies risk into tiers**: Low (<0.3), Medium (0.3-0.6), High (>0.6)
* **Uses comprehensive features**: Demographics, encounter history, procedures, costs, temporal patterns
* **Refreshes incrementally**: Updates as new encounters are processed

**Feature Categories:**
* Patient demographics (age, gender, race, ethnicity)
* Encounter history (total encounters, recent visits, ED visits)
* Procedure utilization (total procedures, complexity)
* Financial indicators (total costs, out-of-pocket burden)
* Temporal patterns (days since last visit, visit frequency)

**Model Performance Monitoring:**
* Track prediction distribution over time
* Compare predictions to actual readmissions
* Detect feature drift
* Retrain quarterly or when drift detected

## Documentation

📚 **[Complete Pipeline Documentation](Hospital%20Pipeline%20Documentation.ipynb)** - Comprehensive documentation including:
* Detailed dataset catalog with schemas
* Complete data lineage diagrams
* Data quality framework
* ML model architecture
* Governance policies and procedures
* Troubleshooting guide
* Maintenance checklists

## Project Structure

```
.
├── README.md                          # This file
├── transformations/
│   ├── Bronze_transformation.py       # Bronze layer ingestion
│   ├── silver/                        # Silver layer transformations
│   │   ├── silver_patient_demographics.py
│   │   ├── silver_encounters.py
│   │   ├── silver_procedures.py
│   │   ├── silver_organizations.py
│   │   ├── silver_payers.py
│   │   ├── silver_patients.py
│   │   └── silver_encounter_procedures.py
│   └── gold/                          # Gold layer analytics
│       ├── organization_performance.py
│       ├── payer_coverage_analysis.py
│       ├── patient_encounter_summary.py
│       ├── patient_cost_analysis.py
│       ├── patient_journey_analysis.py
│       ├── procedure_cost_analysis.py
│       ├── procedure_trends.py
│       ├── ml_features_readmission.py
│       └── ml_predictions_readmission.py
└── Hospital Pipeline Documentation.ipynb
```

## Monitoring and Operations

### Key Metrics
* Pipeline execution status (success/failure)
* Row counts by dataset and layer
* Data freshness (time since last update)
* Validation failure rates
* Compute resource utilization
* Query performance

### Recommended Alerts
* Pipeline execution failure
* Execution time > 30 minutes
* Row count drops > 20%
* Data freshness > 4 hours
* Validation failure rate > 5%

### Maintenance Schedule
* **Daily**: Verify successful execution, check data freshness
* **Weekly**: Review execution trends, analyze quality metrics
* **Monthly**: Performance optimization, cost analysis
* **Quarterly**: ML model retraining, comprehensive audit

## Data Retention

* **Bronze Layer**: 90 days (raw data)
* **Silver Layer**: 2 years (cleansed data)
* **Gold Layer**: 5 years (analytics)
* **ML Predictions**: 1 year (model monitoring)

Archived data moved to cold storage with documented retrieval procedures.

## Performance Optimization

* **Partitioning**: Large tables partitioned by date
* **Z-Ordering**: Optimized file layout for common query patterns
* **Caching**: Frequently accessed dimension tables cached
* **Serverless**: Auto-scaling compute for cost efficiency
* **Photon**: Accelerated query execution

## Security & Compliance

* Encryption at rest (S3/ADLS server-side encryption)
* Encryption in transit (TLS 1.2+)
* Multi-factor authentication (MFA) required
* Regular security assessments
* Disaster recovery testing
* HIPAA Security Risk Assessment (annual)

## Contributing

This pipeline is maintained by the Data Engineering team. For changes:
1. Test in development mode first
2. Update documentation
3. Review impact on downstream dependencies
4. Coordinate with data governance team for PHI-related changes

## Support

**Pipeline Owner**: tatendanamasasu514@gmail.com

**Resources**:
* [Databricks Lakeflow Documentation](https://docs.databricks.com/delta-live-tables/)
* [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
* [Pipeline Monitoring Dashboard](#pipeline-monitoring-306fdf43-9f18-4f66-8587-7408ac4f8d7a)

## License

Proprietary - Internal Use Only. Contains Protected Health Information (PHI).

---

**Last Updated**: June 17, 2026  
**Pipeline Version**: 1.0  
**Status**: ✅ Active and Healthy
