# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Pipeline Orchestrator (Production)
# MAGIC
# MAGIC End-to-end pipeline: **Bronze → Silver → Validate**
# MAGIC
# MAGIC ## Production Architecture Demonstrated:
# MAGIC
# MAGIC | Component | Demo (old) | Production (this) |
# MAGIC |-----------|-----------|-------------------|
# MAGIC | Schema source | Hand-coded `StructType` | Parsed from `.cdm.json` at runtime |
# MAGIC | Extensions | Not supported | `extendsEntity` in `BankExtended.cdm.json` |
# MAGIC | Custom entities | Not supported | `KYCCheck.cdm.json` |
# MAGIC | Column mappings | Hardcoded in Python | `config/entity_mappings.yaml` |
# MAGIC | Quality rules | Post-hoc print statements | `config/quality_rules.yaml` + engine |
# MAGIC | CDM reference | Not used | `cdm_schemas/manifest.cdm.json` |
# MAGIC
# MAGIC ## Entities:
# MAGIC | Entity | Type | CDM Schema |
# MAGIC |--------|------|-----------|
# MAGIC | Bank | **Extended** | `BankExtended.cdm.json` (extends `Bank.cdm.json`) |
# MAGIC | Branch | Standard | `Branch.cdm.json` |
# MAGIC | Contact | Standard | `Contact.cdm.json` |
# MAGIC | Account | Standard | `Account.cdm.json` |
# MAGIC | FinancialHolding | Standard | `FinancialHolding.cdm.json` |
# MAGIC | KYCCheck | **Custom** | `KYCCheck.cdm.json` (net-new, not in CDM) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Configuration

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_cdm_schemas

# COMMAND ----------

print_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Bronze Data
# MAGIC
# MAGIC Creates synthetic raw data, now including:
# MAGIC - **BankExtended fields** (riskRating, swiftCode, nbbLicense, etc.)
# MAGIC - **KYCCheck records** (custom entity for AML/KYC compliance)

# COMMAND ----------

# MAGIC %run ./02_generate_bronze_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Data Summary

# COMMAND ----------

print("=" * 60)
print("Bronze Layer Summary")
print("=" * 60)

for entity in CDM_ENTITIES:
    df = spark.read.format("delta").load(get_bronze_path(entity))
    count = df.count()
    cols = len(df.columns)
    tag = ""
    if entity == "bank":
        tag = " (includes BankExtended fields)"
    elif entity == "kyc_check":
        tag = " (CUSTOM ENTITY)"
    print(f"  {CDM_ENTITIES[entity]:25s}  {count:6d} records  {cols:3d} columns{tag}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Bronze → Silver (Config-Driven)
# MAGIC
# MAGIC Transformations are driven by `config/entity_mappings.yaml`.
# MAGIC The `TransformEngine` reads the YAML, applies registered transforms,
# MAGIC and enforces CDM schemas parsed from `.cdm.json` files.

# COMMAND ----------

# MAGIC %run ./03_bronze_to_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Data Quality Comparison

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
# MAGIC ## Step 5: CDM Schema Compliance Validation

# COMMAND ----------

print("=" * 60)
print("Silver Schema Validation — CDM Compliance Check")
print("=" * 60)

for entity_name in CDM_ENTITIES:
    expected_schema = get_cdm_schema(entity_name)
    silver_df = spark.read.format("delta").load(get_silver_path(entity_name))
    actual_schema = silver_df.schema

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
# MAGIC ## Step 6: Data Quality Validation (Rule-Based)
# MAGIC
# MAGIC Quality rules are defined in `config/quality_rules.yaml` and executed
# MAGIC by the `QualityEngine`. Rules include null checks, uniqueness, referential
# MAGIC integrity, value ranges, string patterns, and completeness thresholds.

# COMMAND ----------

# Initialize the quality engine
quality_engine = QualityEngine(QUALITY_RULES_PATH, spark)

# Load all silver DataFrames for referential integrity checks
silver_dfs = {}
for entity_name in CDM_ENTITIES:
    silver_dfs[entity_name] = spark.read.format("delta").load(get_silver_path(entity_name))

# Run quality validation for all entities
all_quality_results = {}
for entity_name in CDM_ENTITIES:
    results = quality_engine.validate_entity(
        entity_name,
        silver_dfs[entity_name],
        reference_dfs=silver_dfs,
    )
    all_quality_results[entity_name] = results

# Print full quality report
quality_engine.print_full_report(all_quality_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Field Completeness Report

# COMMAND ----------

print("=" * 60)
print("Field Completeness Report — Silver Layer")
print("=" * 60)

for entity_name in CDM_ENTITIES:
    schema = get_cdm_schema(entity_name)
    silver_df = silver_dfs[entity_name]
    total_count = silver_df.count()

    print(f"\n  {CDM_ENTITIES[entity_name]} ({total_count} records):")

    for field in schema.fields:
        null_count = silver_df.filter(silver_df[field.name].isNull()).count()
        completeness = ((total_count - null_count) / total_count * 100) if total_count > 0 else 0
        bar_len = int(completeness / 5)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        req = " [REQ]" if not field.nullable else ""
        print(f"    {field.name:35s} {bar} {completeness:5.1f}%{req}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete!
# MAGIC
# MAGIC ### What was demonstrated (Production approach):
# MAGIC
# MAGIC 1. **Dynamic CDM schemas** — Parsed from `.cdm.json` files, not hand-coded
# MAGIC 2. **Entity extensions** — `BankExtended` inherits all Bank fields + adds custom fields
# MAGIC 3. **Custom entities** — `KYCCheck` created for AML/KYC (not in standard CDM)
# MAGIC 4. **Config-driven transforms** — Column mappings in `entity_mappings.yaml`
# MAGIC 5. **Rule-based quality validation** — Quality rules in `quality_rules.yaml`
# MAGIC 6. **No CDM repo fork** — Standard schemas in `standard/`, extensions in `extensions/`
# MAGIC
# MAGIC ### Entity Relationship:
# MAGIC ```
# MAGIC Bank/BankExtended (1) ──→ (N) Branch (1) ──→ (N) Contact (Customer)
# MAGIC                                    │                    │
# MAGIC                                    └──→ (N) Account ←───┘
# MAGIC                                               │
# MAGIC                                         (1) ──→ (N) FinancialHolding
# MAGIC                                                 │
# MAGIC                               Contact (1) ──→ (N) KYCCheck [CUSTOM]
# MAGIC ```
# MAGIC
# MAGIC ### How to extend this for your organization:
# MAGIC
# MAGIC | Task | What to do |
# MAGIC |------|-----------|
# MAGIC | Add field to existing entity | Create extension `.cdm.json` with `extendsEntity` |
# MAGIC | Add new entity | Create `.cdm.json` in `extensions/`, add to manifest |
# MAGIC | Change source column name | Edit `config/entity_mappings.yaml` |
# MAGIC | Add quality rule | Edit `config/quality_rules.yaml` |
# MAGIC | Add transform function | Register in `lib/transform_engine.py` |

# COMMAND ----------

print("\n✓ CDM Banking Demo Pipeline (Production) — Complete")
