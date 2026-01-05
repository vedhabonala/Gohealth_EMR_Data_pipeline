# GoHealth EMR Data Engineering Pipeline

## Overview
This repository contains a local, end-to-end data engineering pipeline built to ingest, validate, transform, and prepare EMR data for analytics use cases such as:

Patient outcomes

Provider performance

Operational reporting

The pipeline is designed for local execution to emphasize data engineering logic, validation strategies, and governance patterns independent of infrastructure.
It demonstrates real-world healthcare data engineering practices including data quality enforcement, quarantine handling, PHI masking, audit logging, and dimensional modeling.

This solution processes EMR extracts for: Patients, Visits, Lab results, ICD diagnosis reference data

## High-Level Pipeline Flow

The pipeline follows a linear, stage-based ETL pattern:

Ingest structured EMR extracts into in-memory dataframes

Clean and standardize core fields

Apply data quality validations with severity levels (WARN / ERROR)

Quarantine invalid records without silently dropping data

Apply PHI masking and identifier hashing

Build analytics-ready fact and dimension datasets

Generate audit logs and encrypted outputs

This approach mirrors production data pipelines while remaining infrastructure-independent for assessment execution.


## Repository Structure

    ├── data/
    │   └── Data_Eng_Data_Set.xlsx        # Raw EMR source extracts
    │
    ├── src/
    │   └── Pipeline.py                  # End-to-end pipeline implementation
    │
    ├── models/
    │   └── data_model.txt               # Star schema diagram and textual schema layout
    │
    ├── outputs/
    │   ├── gohealth_emr_output.xlsx     # Curated analytics output (PHI masked)
    │   ├── gohealth_emr_output.enc      # Encrypted analytics output
    │   ├── gohealth_emr_quarantine.xlsx # Quarantined invalid records
    │   └── gohealth_emr_audit.log       # Audit and validation logs
    │
    ├── requirements.txt                 # Python dependencies
    ├── README.md                        # Project overview and execution steps
    ├── ASSESSMENT_EXPLANATION.md        # Complete assessment explanation and design reasoning
    └── .gitignore                       # Git exclusions for local artifacts



## How to Run the Pipeline

1. Install dependencies

python -m pip install --upgrade pip
pip install -r requirements.txt

2. Execute the pipeline

python src/Pipeline.py

The pipeline runs locally and does not require any external configuration, credentials, or cloud services.

## Outputs

After execution, the pipeline generates:

1. Curated analytics output
outputs/gohealth_emr_output.xlsx 

Dimension tables (dim_patient, dim_provider, dim_icd)

Fact tables (fact_visit, fact_lab)

Basic analytics aggregates

2. Encrypted output file
outputs/gohealth_emr_output.enc

Demonstrates secure storage of analytics data

3. Quarantine file
outputs/gohealth_emr_quarantine.xlsx

Contains invalid or suspicious records with rule and severity

4. Audit log
outputs/gohealth_emr_audit.log

Records ingestion, validation, and export events

All analytics outputs apply PHI masking and identifier hashing prior to export.


## External Libraries Used

pandas – Used for data ingestion, cleaning, validation, transformation, and dimensional modeling. The transformation and validation logic mirrors patterns commonly used in distributed processing and warehouse-based systems.

openpyxl – Used for reading from and writing to Excel files to generate reviewer-friendly output artifacts.

hashlib (SHA-256) – Used to hash patient identifiers, enabling consistent record linkage while protecting PHI.

cryptography – Used to encrypt final analytics outputs to demonstrate protection of sensitive data at rest.

I limited the technology stack to avoid external configuration, credentials, or subscription requirements that could complicate execution or review. This allowed the assessment to be reproducible and straightforward to execute in a self-contained environment, while keeping the focus on data engineering logic, validation strategy, and governance patterns.










