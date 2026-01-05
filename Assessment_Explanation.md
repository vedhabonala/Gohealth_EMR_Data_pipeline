# Complete Explanation

## My Understanding of the Assessment (Problem Understanding)

I understood this assessment as a simulation of a real-world data engineering problem within a multi-state urgent care healthcare network. Patient visits are captured across EMR systems, and periodic data extracts must be ingested, validated, and prepared for analytics. The datasets are intentionally imperfect, containing missing values, inconsistent formats, invalid dates, duplicates, and broken relationships, which indicates that the focus is not simple data loading but handling realistic EMR data quality challenges.

From my perspective, the core objective of the assessment is to design an end-to-end ETL pipeline that explicitly enforces data quality and clinical integrity rules, handles invalid data in a controlled and auditable way, and produces analytics-ready datasets to support patient outcomes analysis, provider performance reporting, and operational metrics. Given the four datasets representing patients, visits, lab results, and diagnosis reference data, I approached this as a data trust and governance problem, emphasizing transparent validation, severity classification, quarantine handling, audit logging, and PHI-aware transformations to ensure downstream analytics are reliable.

## Execution Overview (Assessment 2a – Pipeline Execution)

I designed the solution so that the entire pipeline can be executed end to end from a single entry point, without relying on external services or environment-specific configuration. This approach ensures that anyone can run the pipeline consistently and observe the results exactly as produced, while keeping the focus on data engineering logic and behavior rather than infrastructure setup.

The pipeline is executed locally using Python, with all dependencies captured in a requirements.txt file to support controlled and repeatable setup. Execution is performed by running the Pipeline.py script, which serves as the single orchestration point and performs ingestion, data cleaning, validation, quarantine handling, PHI masking, data modeling, analytics output generation, audit logging, and encryption of the final output in a single run. All generated artifacts are written to the outputs directory, allowing everyone to directly inspect curated datasets, quarantined records, audit logs, and encrypted outputs.

To run the pipeline, I execute the following steps:

python -m pip install --upgrade pip
pip install -r requirements.txt
python src/Pipeline.py

## End-to-End Pipeline Flow (Assessment 2a – Pipeline Design and Flow)

I designed the pipeline as a linear, stage-based ETL flow, where each step has a clear responsibility and builds on the outcome of the previous stage. This structure reflects how I approach EMR data pipelines in practice by keeping ingestion, validation, and transformation explicit and traceable rather than implicit or tightly coupled.

At a high level, the pipeline progresses through a defined sequence of stages.

### 3.1 Ingestion

The pipeline begins by ingesting EMR data extracts into in-memory tabular structures. Each dataset—patients, visits, lab results, and ICD reference data—is loaded independently so it can be processed and validated according to its own business rules. At this stage, the goal is reliable ingestion into a consistent processing layer rather than transformation.

### 3.2 Cleaning and Standardization

After ingestion, I apply basic cleaning and standardization steps prior to validation. These include normalizing text fields, standardizing casing and trimming whitespace, and parsing date fields with explicit error handling. I perform these steps early because validation logic is only reliable when data formats are consistent, which is especially important for EMR data originating from heterogeneous systems.

### 3.3 Data Validation

Once data is standardized, I apply explicit data quality validations that cover both structural correctness, such as nulls, duplicates, and datatypes, and healthcare-specific business rules, such as visit dates relative to patient dates of birth. Each validation rule is executed and recorded independently, allowing overall data quality to be assessed without halting the pipeline at the first failure.

### 3.4 Severity-Based Handling (WARN vs ERROR)

Validation outcomes are classified using severity levels. ERROR indicates violations that break clinical or analytical integrity and must be excluded from curated datasets, while WARN indicates issues that should be flagged and tracked without necessarily blocking downstream analytics. This distinction allows strict enforcement where required while remaining flexible for reference or classification issues.

### 3.5 Quarantine Routing

Records that fail ERROR-level validations are routed to a quarantine dataset rather than being silently dropped. Each quarantined record is preserved along with the rule violated and its severity, maintaining visibility into data quality issues and enabling downstream review or remediation.

### 3.6 PHI Masking and Identifier Handling

Before generating analytics-ready outputs, I apply PHI-aware transformations by masking direct identifiers and replacing patient identifiers with hashed values. These steps ensure that downstream datasets can be safely used for analytics without exposing sensitive patient information.

### 3.7 Dimensional Modeling and Analytics Outputs

In the final stage, I construct fact and dimension datasets suitable for analytics and reporting. These curated outputs are written along with supporting artifacts such as audit logs and encrypted files. By separating ingestion, validation, quarantine, and modeling into distinct stages, the pipeline remains readable, auditable, and easy to reason about during both execution and review.

## Ingestion Strategy (Assessment 2a – Ingestion)

For this assessment, I treated ingestion as the responsibility of reliably bringing source EMR data into a controlled processing layer, rather than a place to apply business logic or transformations. Each dataset—patients, visits, lab results, and ICD reference data—is loaded independently into in-memory tabular structures so it can be validated against its own schema and business rules before being combined with other entities. This mirrors how EMR data is typically staged in production systems, where raw extracts are made available in a structured form prior to downstream processing.

Because the assessment data is delivered as batch extracts rather than through APIs or streaming interfaces, the ingestion logic emphasizes schema awareness and robustness rather than throughput or concurrency. I intentionally kept ingestion separate from validation and transformation, ensuring that no records are dropped, corrected, or masked at this stage. This approach guarantees that all source records remain accessible for inspection, validation, and auditing, and establishes a clear boundary between raw data and processing logic that supports the governance steps that follow.

## Data Cleaning and Standardization (Assessment 2a – Cleaning & Standardization)

After ingestion, I perform data cleaning and standardization before applying any validation rules. I consider this step essential because validation logic is only reliable when data values follow consistent formats. In EMR data, even small inconsistencies in text casing, whitespace, or date formats can lead to false validation failures or incorrect analytical results. In my implementation, I standardize patient name fields by trimming extra whitespace and normalizing casing, and I normalize fields such as laboratory test names so that equivalent values are treated uniformly during downstream processing. These steps do not change the meaning of the data; they only bring it into a consistent and predictable form.

For date fields such as date of birth and visit date, I explicitly parse values into date or timestamp types using error-tolerant parsing, coercing unparseable values into nulls rather than failing the pipeline. This ensures that malformed dates are handled explicitly during validation instead of being silently ignored or misinterpreted. I intentionally keep cleaning logic minimal and deterministic, avoiding any attempts to infer missing values or apply business assumptions. By separating cleaning and standardization from validation, I maintain clear responsibility boundaries and ensure that data quality issues are surfaced through explicit validation rules rather than hidden within transformation logic.

## Data Quality and Validation Logic (Assessment 2c – Validation & Edge Case Handling)

After cleaning and standardization, I apply explicit data quality validations to assess whether each record can be trusted for downstream analytics. I treated validation as a first-class concern rather than an afterthought, because in healthcare analytics the correctness of insights depends directly on the integrity of the underlying data. Validation logic is applied independently to each dataset based on its role in the EMR domain. For patient data, I validate the presence of required fields such as patient identifiers, names, and date of birth, and I check for duplicate patient identifiers to prevent multiple records from representing the same individual. Records missing critical attributes or containing duplicate identifiers are treated as invalid because they cannot be reliably linked or analyzed.

For visit data, I validate required identifiers, provider references, and visit dates, and I apply clinical consistency rules such as ensuring a visit date does not occur before a patient’s date of birth. For laboratory data, I validate required visit identifiers, test names, and test values, and I enforce referential integrity by ensuring that each lab record is associated with a valid visit. I also validate visit-level diagnosis codes against the provided ICD reference dataset, flagging invalid codes rather than blocking records entirely to account for reference data inconsistencies. All validation outcomes are recorded explicitly and applied consistently across the pipeline. By centralizing validation logic and applying it after standardization, I ensure that data quality issues are surfaced transparently and handled in a controlled manner, with the assumption that downstream analytics are only as trustworthy as the quality checks enforced upstream.

## Quarantine Handling Strategy (Assessment 2c – Edge Case Handling & Governance)

When records fail validation, I do not silently drop them or immediately fail the entire pipeline. Instead, I route those records into a dedicated quarantine dataset. I made this decision because, in healthcare data pipelines, discarding invalid data can hide systemic issues and make it difficult to understand why certain records are missing from downstream analytics. In my implementation, records that violate ERROR-level validation rules are removed from curated datasets and written to quarantine, with each record preserved along with metadata describing the violated rule and severity level. This ensures that data quality issues remain visible and auditable rather than being implicitly resolved through exclusion.

This approach allows the pipeline to continue processing valid data while still capturing a complete picture of data quality problems. It enables downstream teams to review quarantined records, identify recurring upstream issues, and decide whether remediation or reprocessing is appropriate, while avoiding unexpected changes in analytics caused by silent data loss. I intentionally treat quarantine as a data governance mechanism rather than an error-handling shortcut, making pipeline behavior explicit and explainable. By separating invalid data into a clearly defined quarantine layer, I maintain trust in curated datasets while preserving access to all original records for investigation and audit purposes, mirroring how production healthcare pipelines separate remediation workflows from analytics consumption.

## Referential Integrity and Healthcare-Specific Rules (Assessment 2c – Domain Rules & Edge Cases)

In addition to basic structural validations, I explicitly enforce referential integrity and healthcare-specific business rules within the pipeline. I consider these rules critical because EMR data is inherently relational, and analytics built on broken relationships can lead to misleading or clinically incorrect conclusions. One key rule I enforce is the logical relationship between patients and visits, ensuring that a visit date cannot occur before a patient’s date of birth. Records that violate this rule are treated as invalid because they represent clinical impossibilities and cannot be trusted for downstream analysis.

I also enforce referential integrity between visits and laboratory results by requiring each lab record to reference a valid visit, routing orphaned lab records to quarantine when this relationship is broken. For diagnosis codes, I validate visit-level ICD values against the provided reference dataset, normalizing codes before comparison to handle formatting differences. When a diagnosis code is missing from the reference data, I flag it rather than blocking the record entirely, recognizing that reference data mismatches often reflect synchronization or classification issues rather than invalid clinical events. By enforcing these domain-specific rules, I ensure that curated datasets preserve clinically plausible timelines and meaningful relationships between patients, visits, diagnoses, and lab results, supporting accurate and reliable analytics.

## PHI and HIPAA-Aligned Controls (Assessment 2c – Best Practices & Governance)

Because the data used in this assessment represents patient-level healthcare information, I treated protection of PHI as a core design requirement rather than an optional enhancement. My goal was not to claim full regulatory compliance, but to demonstrate controls aligned with HIPAA principles that are appropriate for analytics use cases, particularly around de-identification and controlled data exposure. In my implementation, PHI handling is applied before generating any analytics-ready outputs by masking direct patient identifiers such as first and last names so that downstream datasets cannot expose identifiable information, while still preserving limited analytical usefulness.

For patient identifiers, I replace the original patient ID with a SHA-256 hashed value, which provides a deterministic and irreversible way to link records consistently across datasets without exposing the original identifier. In addition to masking and hashing, I encrypt the final analytics output file to protect data at rest after curation. I also implement audit logging across ingestion, validation, and export steps to support traceability and accountability. Together, these measures ensure that PHI-aware transformations are applied consistently and transparently, reflecting how healthcare analytics pipelines are designed to reduce exposure risk, protect sensitive information, and maintain trust in downstream data usage without claiming full regulatory compliance.

## Auditability and Traceability (Assessment 2c – Governance & Best Practices)

In addition to validating and protecting the data, I designed the pipeline so that its behavior is auditable and traceable end to end. When working with healthcare data, it is important not only to understand the final outputs, but also to be able to explain how those outputs were produced and what decisions were made during processing. To support this, I record audit information for each major stage of the pipeline, including ingestion, validation execution, and output generation. Each audit entry captures the action performed along with contextual details and timestamps, allowing authorized users to trace when data was processed, what operation occurred, and at which stage of execution.

I intentionally treat audit logging as a separate concern from validation and transformation logic. Validation determines data correctness, while audit logs provide visibility into pipeline behavior without mixing operational metadata into business data. The audit log output supports transparency for authorized users, helps troubleshoot unexpected results, and demonstrates accountability when handling sensitive healthcare information. By including explicit audit logging, I aimed to show that reliable data engineering extends beyond transformations and analytics, and that traceability and accountability are essential for building trusted pipelines in healthcare contexts where data lineage and processing history matter as much as the data itself.

## Outputs and How to Interpret Them (Assessment 2a & 2c – Outputs, Governance, and Transparency)

After the pipeline completes execution, it produces a set of output artifacts designed to make processing results explicit and inspectable. The primary curated output is written to an Excel file containing the analytics-ready dimension and fact tables, with PHI already masked and patient identifiers replaced by hashed values. These datasets represent the cleaned, validated, and governed view of the EMR data intended for analytics consumption. By reviewing this file, a reviewer can confirm that invalid records have been excluded, relationships are preserved, and sensitive information is not exposed.

In addition to the curated output, the pipeline generates an encrypted version of the same data to demonstrate protection of analytics outputs at rest. It also produces a quarantine file containing all records that failed ERROR-level validations, with each record annotated with the violated rule and severity, making exclusion decisions explicit. An audit log is generated alongside these outputs to record ingestion, validation, and export events, providing a chronological view of pipeline execution. Together, these artifacts allow everyone to understand not only the final datasets, but also how decisions were made throughout processing, which is essential for evaluating a healthcare-focused data engineering solution.

## Data Model and Schema Design (Assessment 2b – Schema Diagram & Data Model)

For the analytics layer, I modeled the curated data using a dimensional (star) schema because this structure is well suited for healthcare analytics and reporting. The visit fact sits at the center of the model and represents individual patient encounters, which are the core analytical events in an urgent care setting. Each visit connects to patient, provider, and diagnosis dimensions, reflecting how analytics are typically performed in practice by slicing visit activity across patient attributes, providers, diagnoses, and operational timelines. In addition to the visit fact, I include a separate lab results fact that captures laboratory test information at the visit level, preserving the one-to-many relationship between visits and lab results without inflating visit-level metrics.

The patient dimension is designed to be PHI-aware and SCD Type 2–ready, using a hashed patient identifier instead of the original ID, masking patient names, and retaining demographic attributes such as date of birth and gender for analysis. Effective date fields and a current-record indicator demonstrate how patient history could be tracked over time, even though this assessment runs as a single execution. The provider dimension contains distinct provider identifiers derived from visit data, and the diagnosis dimension is built from the ICD reference dataset to standardize diagnosis analysis. Provider and diagnosis dimensions are intentionally linked at the visit level rather than directly to lab results, since providers and diagnoses describe the clinical encounter, not individual tests. This design preserves the correct analytical grain, avoids duplication, and supports accurate, visit-centric healthcare analytics while maintaining clear governance and relationship boundaries.

## Production Mapping and Scalability Considerations (Assessment 3 – Tooling & Production Equivalents)

For this assessment, I implemented the pipeline using Python and pandas with local execution to keep the focus on core data engineering logic such as data quality enforcement, validation strategy, quarantine handling, PHI protection, auditability, and analytical modeling, without introducing external infrastructure dependencies that could complicate execution or review. In a real production environment, the same business logic and pipeline stages would be preserved, while the execution environment and scale would change.

Assessment → Production Mapping

| Assessment Implementation | Production Equivalent       |
| ------------------------- | --------------------------- |
| pandas                    | Snowflake SQL / dbt         |
| Excel source files        | S3 / ADLS Gen2              |
| Python pipeline script    | Airflow / ADF orchestration |
| Local execution           | Cloud-based ELT pipelines   |
| Excel analytics outputs   | Snowflake analytics tables  |

In a production environment, the visual mapping above illustrates how this assessment implementation translates directly to an enterprise data architecture. The core logic for ingestion, validation, severity-based quarantine handling, PHI protection, and dimensional modeling remains unchanged. Only the execution environment, orchestration, and storage layers differ, allowing the same design to scale while preserving data quality, governance, and analytical intent.

## Design Decisions and Trade-offs (Assessment 2c – Design Decisions)

Throughout this assessment, I made deliberate design decisions to balance correctness, clarity, and scope, guided by the assessment requirements and by how healthcare data pipelines are typically evaluated in practice. One key decision was to implement severity-based validation using WARN and ERROR classifications. I treated clinical integrity and referential violations as ERROR conditions because they directly compromise analytical correctness, while treating reference data mismatches such as invalid ICD codes as WARN conditions. This approach enforces strict rules where necessary without unnecessarily blocking records that may still provide analytical value.

I also chose to quarantine invalid records rather than silently dropping them or failing the pipeline, preserving visibility into data quality issues and supporting remediation without unexpected data loss. Implementing the pipeline as a single end-to-end executable script was another trade-off made to simplify execution and review while keeping all stages visible in one place, even though these stages would be separated in production. Local execution using Python and pandas further reduced external dependencies and configuration overhead, allowing everyone to focus on data engineering logic rather than infrastructure. Throughout these decisions, I intentionally emphasized PHI handling, auditability, and data quality to reflect how healthcare data engineering prioritizes trust, transparency, and analytical readiness, even within the limited scope of an assessment.

## External Libraries / Tools (Assessment 3 – External Libraries and Tooling Choices)

For this assessment, I limited the technology stack to a small set of well-established Python libraries to keep the solution reproducible, self-contained, and focused on core data engineering logic rather than environment setup or external infrastructure configuration. The primary library used is pandas, which I used for data ingestion, cleaning, validation, transformation, and dimensional modeling. Pandas provides clear and expressive APIs for implementing row-level validation rules, joins, filtering, aggregations, severity-based validation, quarantine handling, and the construction of fact and dimension datasets. The transformation patterns used closely mirror those applied in distributed processing frameworks such as Spark or SQL-based ELT tools, making the logic directly portable to production systems.

I used openpyxl for reading from and writing to Excel files to generate reviewer-friendly output artifacts that can be inspected without additional tooling, with Excel serving only as a delivery format for this assessment. For PHI protection, I used Python’s built-in hashlib module to generate SHA-256 hashes for patient identifiers, enabling consistent record linkage without exposing original IDs, and the cryptography library to encrypt the final analytics output file to protect data at rest. I did not use managed platforms such as Snowflake, Airflow, or cloud storage services in the implementation to avoid external configuration or credential dependencies. Instead, I focused on implementing validation, governance, and modeling logic in a self-contained way that can be clearly mapped to enterprise-grade tools in a production environment, as described in the production mapping section.

## Conclusion

This assessment reflects how I approach healthcare data engineering problems with an emphasis on data quality, governance, and analytical trust. The pipeline makes data issues explicit through validation, severity classification, and quarantine handling, protects patient information through masking and hashing, and produces analytics-ready datasets that can be confidently used for reporting and analysis. While the implementation runs locally for review and reproducibility, the design patterns, validation logic, and data model directly align with how production healthcare pipelines are built, making the solution both practical for assessment and representative of real-world data engineering work.