# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Pipeline Orchestrator
# MAGIC
# MAGIC This notebook orchestrates the full **Bronze -> Silver** CDM banking data pipeline.
# MAGIC
# MAGIC ## Pipeline Steps:
# MAGIC 1. **Setup** — Load configuration and CDM schemas
# MAGIC 2. **Bronze** — Generate synthetic raw banking data with data quality issues
# MAGIC 3. **Silver** — Transform and clean data to CDM-conformant canonical layer
# MAGIC 4. **Validate** — Run data quality checks on the silver layer
# MAGIC
# MAGIC ## CDM Entities:
# MAGIC | Entity | CDM Source | Description |
# MAGIC |--------|-----------|-------------|
# MAGIC | Bank | `FinancialServicesCommonDataModel/Bank` | Bank institution |
# MAGIC | Branch | `FinancialServicesCommonDataModel/Branch` | Bank branches |
# MAGIC | Contact | `FinancialServicesCommonDataModel/Contact` | Customers/contacts |
# MAGIC | Account | `FinancialServicesCommonDataModel/Account` | Customer accounts |
# MAGIC | FinancialHolding | `RetailBankingCoreDataModel/Financialholding` | Financial products |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Configuration

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_cdm_schemas

# COMMAND ----------

print_config()
print()
print_schema_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Bronze Data
# MAGIC
# MAGIC Creates synthetic raw data simulating messy source system exports:
# MAGIC - Inconsistent date formats, mixed case, extra whitespace
# MAGIC - Missing values and duplicates
# MAGIC - Non-standard status/boolean representations

# COMMAND ----------

# MAGIC %run ./02_generate_bronze_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Data Summary

# COMMAND ----------

print("=" * 60)
print("Bronze Layer Summary")
print("=" * 60)

bronze_summary = []
for entity in CDM_ENTITIES:
    df = spark.read.format("delta").load(get_bronze_path(entity))
    count = df.count()
    cols = len(df.columns)
    bronze_summary.append({
        "entity": CDM_ENTITIES[entity],
        "records": count,
        "columns": cols,
        "path": get_bronze_path(entity),
    })
    print(f"  {CDM_ENTITIES[entity]:25s}  {count:6d} records  {cols:3d} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Bronze -> Silver Transformations
# MAGIC
# MAGIC Transforms raw bronze data to CDM-conformant silver:
# MAGIC - Column renaming to CDM attribute names
# MAGIC - Date/time format standardization
# MAGIC - Status code resolution (CDM option sets)
# MAGIC - Deduplication
# MAGIC - String normalization (trim, proper case)
# MAGIC - Schema enforcement (PySpark StructType)

# COMMAND ----------

# MAGIC %run ./03_bronze_to_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Data Summary

# COMMAND ----------

print("=" * 60)
print("Silver Layer Summary (CDM Conformant)")
print("=" * 60)

silver_summary = []
for entity in CDM_ENTITIES:
    df = spark.read.format("delta").load(get_silver_path(entity))
    count = df.count()
    cols = len(df.columns)
    silver_summary.append({
        "entity": CDM_ENTITIES[entity],
        "records": count,
        "columns": cols,
        "path": get_silver_path(entity),
    })
    print(f"  {CDM_ENTITIES[entity]:25s}  {count:6d} records  {cols:3d} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Data Quality Comparison (Bronze vs Silver)

# COMMAND ----------

print("=" * 60)
print("Bronze vs Silver — Record Count Comparison")
print("=" * 60)
print(f"  {'Entity':25s}  {'Bronze':>8s}  {'Silver':>8s}  {'Delta':>8s}  {'Note'}")
print(f"  {'-'*25}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*20}")

for entity in CDM_ENTITIES:
    bronze_count = spark.read.format("delta").load(get_bronze_path(entity)).count()
    silver_count = spark.read.format("delta").load(get_silver_path(entity)).count()
    delta = bronze_count - silver_count
    note = "deduplicated" if delta > 0 else "no duplicates" if delta == 0 else "expanded"
    print(f"  {CDM_ENTITIES[entity]:25s}  {bronze_count:8d}  {silver_count:8d}  {delta:+8d}  {note}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Silver Schema Validation

# COMMAND ----------

print("=" * 60)
print("Silver Schema Validation — CDM Compliance Check")
print("=" * 60)

for entity_name, expected_schema in CDM_SCHEMAS.items():
    silver_df = spark.read.format("delta").load(get_silver_path(entity_name))
    actual_schema = silver_df.schema

    # Compare field names
    expected_fields = {f.name for f in expected_schema.fields}
    actual_fields = {f.name for f in actual_schema.fields}

    missing = expected_fields - actual_fields
    extra = actual_fields - expected_fields

    print(f"\n  {CDM_ENTITIES[entity_name]}:")
    if not missing and not extra:
        print(f"    ✓ Schema matches CDM definition ({len(expected_fields)} fields)")
    else:
        if missing:
            print(f"    ✗ Missing fields: {missing}")
        if extra:
            print(f"    ✗ Extra fields: {extra}")

    # Validate data types
    type_mismatches = []
    for expected_field in expected_schema.fields:
        actual_field = next(
            (f for f in actual_schema.fields if f.name == expected_field.name), None
        )
        if actual_field and str(actual_field.dataType) != str(expected_field.dataType):
            type_mismatches.append(
                f"{expected_field.name}: expected {expected_field.dataType}, got {actual_field.dataType}"
            )

    if type_mismatches:
        print(f"    ⚠ Type mismatches:")
        for m in type_mismatches:
            print(f"      - {m}")
    else:
        print(f"    ✓ All data types match CDM specification")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete!
# MAGIC
# MAGIC ### What was demonstrated:
# MAGIC
# MAGIC 1. **CDM Schema Definitions** — PySpark StructTypes derived from the official
# MAGIC    [Microsoft CDM FinancialServices](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices) entities
# MAGIC
# MAGIC 2. **Bronze Layer** — Synthetic raw data with realistic data quality issues
# MAGIC    (inconsistent formats, nulls, duplicates, mixed case)
# MAGIC
# MAGIC 3. **Silver Layer** — Clean, CDM-conformant canonical data with:
# MAGIC    - Standardized column names matching CDM attributes
# MAGIC    - Proper data types (timestamps, decimals, booleans)
# MAGIC    - CDM option set resolution (statecode, statuscode, holdingType)
# MAGIC    - Deduplication and data cleansing
# MAGIC
# MAGIC ### CDM Entity Relationship:
# MAGIC ```
# MAGIC Bank (1) ──→ (N) Branch (1) ──→ (N) Contact (Customer)
# MAGIC                         │                    │
# MAGIC                         └──→ (N) Account ←───┘
# MAGIC                                    │
# MAGIC                              (1) ──→ (N) FinancialHolding
# MAGIC ```

# COMMAND ----------

print("\n✓ CDM Banking Demo Pipeline — Complete")
