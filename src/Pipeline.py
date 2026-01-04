"""
GoHealth EMR Data Engineering Assessment
End-to-end HIPAA-compliant pipeline
Includes:
- CSV-in-Excel worksheet handling
- Open validations
- Severity levels (WARN / ERROR)
- Quarantine handling
- PHI masking
- Audit logging
"""

import pandas as pd
from datetime import datetime
import getpass
import hashlib
from io import StringIO
from cryptography.fernet import Fernet
import os

# -----------------------------
# HIPAA SETTINGS
# -----------------------------
MASK_PH_NAMES = True
MASK_PATIENT_ID = True
AUDIT_LOG_FILE = "outputs/gohealth_emr_audit.log"

os.makedirs("outputs", exist_ok=True)

# Encryption key (should be stored securely in real systems)
ENCRYPTION_KEY = Fernet.generate_key()
cipher = Fernet(ENCRYPTION_KEY)

# -----------------------------
# AUDIT LOG
# -----------------------------
def audit_log(action, entity, details=""):
    user = getpass.getuser()
    ts = datetime.now().isoformat()
    with open(AUDIT_LOG_FILE, "a") as f:
        f.write(f"{ts} | {user} | {action} | {entity} | {details}\n")

# -----------------------------
# SMART WORKSHEET LOADER
# -----------------------------
def read_excel_or_csv_sheet(file_path, sheet_name):
    """
    Handles:
    - Proper Excel worksheets
    - CSV-formatted data embedded inside Excel worksheets
    """
    raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Detect CSV-in-Excel (single column with commas)
    if raw_df.shape[1] == 1 and raw_df.iloc[:, 0].astype(str).str.contains(",").any():
        audit_log("DETECT", sheet_name, "CSV_FORMAT_IN_EXCEL")
        csv_text = "\n".join(raw_df.iloc[:, 0].dropna().astype(str))
        return pd.read_csv(StringIO(csv_text))

    # Normal worksheet
    return pd.read_excel(file_path, sheet_name=sheet_name)

# -----------------------------
# INGESTION
# -----------------------------
FILE_PATH = "data/Data_Eng_Data_Set.xlsx"

xls = pd.ExcelFile(FILE_PATH)
sheet_map = {s.strip(): s for s in xls.sheet_names}

patients_df = read_excel_or_csv_sheet(FILE_PATH, sheet_map["Patient Data"])
visits_df   = read_excel_or_csv_sheet(FILE_PATH, sheet_map["Visit Data"])
labs_df     = read_excel_or_csv_sheet(FILE_PATH, sheet_map["Lab Results"])
icd_df      = read_excel_or_csv_sheet(FILE_PATH, sheet_map["Icd_reference"])

audit_log("INGEST", "ALL", "Data loaded successfully")

# -----------------------------
# QUARANTINE SETUP
# -----------------------------
quarantine = {
    "patients": pd.DataFrame(),
    "visits": pd.DataFrame(),
    "labs": pd.DataFrame()
}

# -----------------------------
# CLEANING
# -----------------------------
patients_df["first_name"] = patients_df["first_name"].astype(str).str.strip().str.title()
patients_df["last_name"] = patients_df["last_name"].astype(str).str.strip().str.title()
patients_df["date_of_birth"] = pd.to_datetime(patients_df["date_of_birth"], errors="coerce")

visits_df["visit_date"] = pd.to_datetime(visits_df["visit_date"], errors="coerce")
labs_df["test_name"] = labs_df["test_name"].astype(str).str.upper()

# -----------------------------
# VALIDATION UTILITIES
# -----------------------------
def log_validation(entity, rule, count, severity):
    print(f"[{severity}] {entity.upper()} | {rule} | count={count}")
    audit_log("VALIDATION", entity, f"{rule} | {count} | {severity}")

def quarantine_rows(df, invalid_df, entity, rule, severity):
    if not invalid_df.empty:
        temp = invalid_df.copy()
        temp["quarantine_rule"] = rule
        temp["severity"] = severity
        quarantine[entity] = pd.concat([quarantine[entity], temp], ignore_index=True)
    return df.drop(invalid_df.index)

def not_null_check(df, cols, entity):
    invalid = df[df[cols].isnull().any(axis=1)]
    log_validation(entity, "NULL_VIOLATION", len(invalid), "ERROR")
    return quarantine_rows(df, invalid, entity, "NULL_VIOLATION", "ERROR")

def duplicate_check(df, cols, entity):
    invalid = df[df.duplicated(subset=cols, keep=False)]
    log_validation(entity, "DUPLICATE_RECORD", len(invalid), "ERROR")
    return quarantine_rows(df, invalid, entity, "DUPLICATE_RECORD", "ERROR")

# -----------------------------
# PATIENT VALIDATIONS
# -----------------------------
patients_df = not_null_check(
    patients_df,
    ["patient_id", "first_name", "last_name", "date_of_birth"],
    "patients"
)

patients_df = duplicate_check(patients_df, ["patient_id"], "patients")

future_dob = patients_df[patients_df["date_of_birth"] > pd.Timestamp.today()]
log_validation("patients", "DOB_IN_FUTURE", len(future_dob), "ERROR")
patients_df = quarantine_rows(
    patients_df, future_dob, "patients", "DOB_IN_FUTURE", "ERROR"
)

# -----------------------------
# VISIT VALIDATIONS
# -----------------------------
visits_df = not_null_check(
    visits_df,
    ["visit_id", "patient_id", "provider_id", "visit_date"],
    "visits"
)

visits_df = duplicate_check(visits_df, ["visit_id"], "visits")

visits_df = visits_df.merge(
    patients_df[["patient_id", "date_of_birth"]],
    on="patient_id",
    how="left"
)

invalid_visits = visits_df[visits_df["visit_date"] < visits_df["date_of_birth"]]
log_validation("visits", "VISIT_BEFORE_DOB", len(invalid_visits), "ERROR")

visits_df = quarantine_rows(
    visits_df, invalid_visits, "visits", "VISIT_BEFORE_DOB", "ERROR"
).drop(columns=["date_of_birth"])

# -----------------------------
# LAB VALIDATIONS
# -----------------------------
labs_df = not_null_check(
    labs_df,
    ["visit_id", "test_name", "test_value"],
    "labs"
)

labs_df = duplicate_check(labs_df, ["visit_id", "test_name"], "labs")

orphan_labs = labs_df[~labs_df["visit_id"].isin(visits_df["visit_id"])]
log_validation("labs", "ORPHAN_VISIT_ID", len(orphan_labs), "ERROR")

labs_df = quarantine_rows(
    labs_df, orphan_labs, "labs", "ORPHAN_VISIT_ID", "ERROR"
)

# -----------------------------
# ICD VALIDATION (WARN)
# -----------------------------

# Normalize column names
icd_df.columns = icd_df.columns.str.strip().str.lower()
visits_df.columns = visits_df.columns.str.strip().str.lower()

# Possible ICD column names (real-world EMR safe)
ICD_CODE_CANDIDATES = [
    "code",
    "icd",
    "icd_code",
    "icd10",
    "diagnosis_code",
    "diag_code",
    "diagnosis"
]

def find_icd_column(df):
    for col in ICD_CODE_CANDIDATES:
        if col in df.columns:
            return col
    return None

# Detect ICD reference column
icd_code_col = find_icd_column(icd_df)

if not icd_code_col:
    raise ValueError(
        f"ICD reference missing ICD column. Found columns: {icd_df.columns.tolist()}"
    )

# Standardize ICD reference
icd_df.rename(columns={icd_code_col: "code"}, inplace=True)
icd_df["code"] = (
    icd_df["code"]
    .astype(str)
    .str.strip()
    .str.upper()
)

# Validate visits ICD column
if "icd_code" not in visits_df.columns:
    raise ValueError(
        f"Visits missing icd_code column. Found columns: {visits_df.columns.tolist()}"
    )

visits_df["icd_code"] = (
    visits_df["icd_code"]
    .astype(str)
    .str.strip()
    .str.upper()
)

# Perform validation
invalid_icd = visits_df[~visits_df["icd_code"].isin(icd_df["code"])]

log_validation(
    "visits",
    "INVALID_ICD_CODE",
    len(invalid_icd),
    "WARN"
)

# Flag but do NOT drop
visits_df["icd_valid_flag"] = visits_df["icd_code"].isin(icd_df["code"])

# Quarantine WARN records
if not invalid_icd.empty:
    temp = invalid_icd.copy()
    temp["quarantine_rule"] = "INVALID_ICD_CODE"
    temp["severity"] = "WARN"
    quarantine["visits"] = pd.concat(
        [quarantine["visits"], temp],
        ignore_index=True
    )

# -----------------------------
# HIPAA: PHI MASKING
# -----------------------------
if MASK_PH_NAMES:
    patients_df["first_name"] = patients_df["first_name"].str[0] + "*****"
    patients_df["last_name"] = patients_df["last_name"].str[0] + "*****"

if MASK_PATIENT_ID:
    patients_df["patient_id_hash"] = patients_df["patient_id"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )

    visits_df = visits_df.merge(
        patients_df[["patient_id", "patient_id_hash"]],
        on="patient_id",
        how="left"
    ).drop(columns=["patient_id"])

    labs_df = labs_df.merge(
        visits_df[["visit_id", "patient_id_hash"]],
        on="visit_id",
        how="left"
    )

# -----------------------------
# DIMENSIONS
# -----------------------------
patients_df["effective_from"] = datetime.today().date()
patients_df["effective_to"] = pd.Timestamp.max.date()
patients_df["is_current"] = True

dim_patient = patients_df[
    [
        "patient_id_hash",
        "first_name",
        "last_name",
        "date_of_birth",
        "gender",
        "effective_from",
        "effective_to",
        "is_current"
    ]
]

dim_provider = visits_df[["provider_id"]].drop_duplicates()

dim_icd = icd_df.rename(
    columns={"code": "icd_code", "description": "icd_description"}
)

# -----------------------------
# FACT TABLES
# -----------------------------
fact_visit = visits_df.copy()
fact_lab = labs_df.copy()

# -----------------------------
# ANALYTICS
# -----------------------------
provider_metrics = (
    fact_visit.groupby("provider_id")
    .agg(total_visits=("visit_id", "count"))
    .reset_index()
)

diagnosis_metrics = (
    fact_visit.groupby("icd_code")
    .size()
    .reset_index(name="diagnosis_count")
)

# -----------------------------
# OUTPUT
# -----------------------------
output_file = "outputs/gohealth_emr_output.xlsx"

with pd.ExcelWriter(output_file) as writer:
    dim_patient.to_excel(writer, "dim_patient", index=False)
    dim_provider.to_excel(writer, "dim_provider", index=False)
    dim_icd.to_excel(writer, "dim_icd", index=False)
    fact_visit.to_excel(writer, "fact_visit", index=False)
    fact_lab.to_excel(writer, "fact_lab", index=False)
    provider_metrics.to_excel(writer, "provider_metrics", index=False)
    diagnosis_metrics.to_excel(writer, "diagnosis_metrics", index=False)

audit_log("EXPORT", "EMR_OUTPUT", output_file)

# Encrypt output
with open(output_file, "rb") as f:
    encrypted = cipher.encrypt(f.read())

with open("outputs/gohealth_emr_output.enc", "wb") as f:
    f.write(encrypted)

# -----------------------------
# QUARANTINE OUTPUT
# -----------------------------
with pd.ExcelWriter("outputs/gohealth_emr_quarantine.xlsx") as writer:
    for entity, df in quarantine.items():
        if not df.empty:
            df.to_excel(writer, f"{entity}_quarantine", index=False)

audit_log("EXPORT", "QUARANTINE_OUTPUT", "gohealth_emr_quarantine.xlsx")

print("Pipeline completed successfully.")