# GoHealth EMR Data Engineering Pipeline

**End-to-end HIPAA-compliant data pipeline** for EMR data, including CSV-in-Excel handling, validation, quarantine management, PHI masking, and encrypted output generation.

---

## Features

- **CSV-in-Excel Worksheet Handling**  
  Supports worksheets that contain CSV-formatted text inside a single column.

- **Data Validations**  
  - Null checks (required fields)  
  - Duplicate detection  
  - Logical validations (e.g., visit date after patient DOB)  
  - ICD code validation (warn-level checks)

- **Quarantine Management**  
  Invalid or suspicious records are quarantined with severity levels (`ERROR` / `WARN`) for review.

- **HIPAA PHI Masking**  
  - Masks patient first and last names  
  - Hashes patient IDs using SHA-256  
  - Ensures anonymized analytics

- **Audit Logging**  
  Records every ingestion, validation, and export step for traceability.

- **Output**  
  - Dimension tables: `dim_patient`, `dim_provider`, `dim_icd`  
  - Fact tables: `fact_visit`, `fact_lab`  
  - Metrics: `provider_metrics`, `diagnosis_metrics`  
  - All outputs are written to Excel and encrypted (`.enc`)  

---

## Requirements

- Python 3.9+  
- Libraries:  
  ```txt
  pandas>=1.5.0
  cryptography>=41.0.0
  openpyxl>=3.1.2