# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze to Silver Transformations (Config-Driven)
# MAGIC
# MAGIC **Production approach:** Transformations are driven by `config/entity_mappings.yaml`.
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. **No per-entity transform functions** — the `TransformEngine` reads YAML config
# MAGIC 2. **Column mappings are externalized** — change source columns without code changes
# MAGIC 3. **Transform functions are registered** — add new transforms in `lib/transform_engine.py`
# MAGIC 4. **Schema enforcement** — CDM PySpark schemas from `.cdm.json` are applied at write time
# MAGIC 5. **Extension fields are handled seamlessly** — BankExtended fields flow through the same engine
# MAGIC 6. **Custom entities use the same engine** — KYCCheck transforms identically to standard entities
# MAGIC
# MAGIC **Key difference from demo:**
# MAGIC > Adding a new source system or changing a column name?
# MAGIC > Edit `config/entity_mappings.yaml`. No Python changes needed.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_cdm_schemas

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Initialize the config-driven transform engine
transform_engine = TransformEngine(ENTITY_MAPPINGS_PATH, spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Entity Mapping Configuration
# MAGIC
# MAGIC Before transforming, let's inspect what the YAML config defines for each entity.

# COMMAND ----------

import yaml

with open(ENTITY_MAPPINGS_PATH, "r") as f:
    mappings_config = yaml.safe_load(f)

print("=" * 60)
print("Entity Mapping Configuration (from YAML)")
print("=" * 60)

for entity_name, config in mappings_config["entities"].items():
    cdm_schema_path = config.get("cdm_schema", "N/A")
    description = config.get("description", "")
    num_mappings = len(config.get("mappings", {}))
    num_derived = len(config.get("derived", []))
    dedup_key = config.get("bronze", {}).get("dedup_key", "N/A")

    print(f"\n  {entity_name} — {description}")
    print(f"    CDM schema:      {cdm_schema_path}")
    print(f"    Column mappings: {num_mappings}")
    print(f"    Derived fields:  {num_derived}")
    print(f"    Dedup key:       {dedup_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform All Entities (Config-Driven)
# MAGIC
# MAGIC The same `transform_engine.transform()` call works for:
# MAGIC - Standard CDM entities (Branch, Contact, Account, FinancialHolding)
# MAGIC - Extended entities (BankExtended — inherits Bank + custom fields)
# MAGIC - Custom entities (KYCCheck — not in standard CDM)

# COMMAND ----------

def run_all_transformations():
    """Run config-driven bronze-to-silver transformations for all entities."""
    print("=" * 60)
    print("Bronze → Silver Transformations (Config-Driven)")
    print("=" * 60)

    results = {}

    for entity_name in CDM_ENTITIES:
        cdm_display_name = CDM_ENTITIES[entity_name]
        print(f"\n--- Transforming {cdm_display_name} (entity: {entity_name}) ---")

        # 1. Read bronze data
        bronze_path = get_bronze_path(entity_name)
        bronze_df = spark.read.format("delta").load(bronze_path)
        bronze_count = bronze_df.count()
        print(f"  Bronze records: {bronze_count}")

        # 2. Get the CDM schema (parsed from .cdm.json)
        cdm_schema = get_cdm_schema(entity_name)
        print(f"  CDM schema: {len(cdm_schema.fields)} fields")

        # 3. Get CDM descriptions for Unity Catalog column comments
        desc_info = get_cdm_descriptions(entity_name)

        # 4. Apply config-driven transformation
        silver_df = transform_engine.transform(
            entity_name=entity_name,
            bronze_df=bronze_df,
            cdm_schema=cdm_schema,
        )

        # 5. Write to silver layer (with UC registration if enabled)
        silver_path = get_silver_path(entity_name)
        transform_engine.write_silver(
            silver_df,
            silver_path,
            cdm_display_name,
            catalog_name=CATALOG_NAME if USE_UNITY_CATALOG else None,
            schema_name=SCHEMA_SILVER if USE_UNITY_CATALOG else None,
            table_name=entity_name if USE_UNITY_CATALOG else None,
            entity_description=desc_info["entity_description"],
            column_descriptions=desc_info["column_descriptions"],
        )

        results[entity_name] = silver_df

    print("\n" + "=" * 60)
    print("All transformations complete!")
    print("=" * 60)

    return results

# COMMAND ----------

silver_results = run_all_transformations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: BankExtended Fields in Silver
# MAGIC
# MAGIC The Bank silver table should contain BOTH standard CDM fields
# MAGIC AND the extension fields (riskRating, swiftCode, etc.).

# COMMAND ----------

print("=" * 60)
print("BankExtended — Silver Layer Verification")
print("=" * 60)

bank_silver = spark.read.format("delta").load(get_silver_path("bank"))

# Identify which fields are extensions
bank_base = cdm_parser.resolve_entity("standard/Bank.cdm.json", "Bank")
base_field_names = {attr["name"] for attr in bank_base["hasAttributes"]}

print("\n  Standard CDM Bank fields:")
for field in bank_silver.schema.fields:
    if field.name in base_field_names:
        print(f"    {field.name:40s} {str(field.dataType):20s}")

print("\n  Extension fields (BankExtended):")
for field in bank_silver.schema.fields:
    if field.name not in base_field_names:
        print(f"    {field.name:40s} {str(field.dataType):20s}  ← EXTENSION")

print("\n  Sample data (extension fields):")
bank_silver.select("bankId", "name", "riskRating", "swiftCode",
                    "regulatoryLicenseNumber", "onboardingChannel",
                    "isPSD2Compliant").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify: KYCCheck Custom Entity in Silver

# COMMAND ----------

print("=" * 60)
print("KYCCheck — Custom Entity Silver Verification")
print("=" * 60)

kyc_silver = spark.read.format("delta").load(get_silver_path("kyc_check"))
print(f"\n  Records: {kyc_silver.count()}")
print(f"  Fields: {len(kyc_silver.schema.fields)}")
print("\n  Schema:")
kyc_silver.printSchema()
print("\n  Sample data:")
kyc_silver.select("kycCheckId", "contactId", "checkType_display",
                   "checkResult_display", "riskScore", "verifiedBy").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data Quality Preview

# COMMAND ----------

print("=" * 60)
print("Silver Layer Summary")
print("=" * 60)

for entity in CDM_ENTITIES:
    df = spark.read.format("delta").load(get_silver_path(entity))
    count = df.count()
    cols = len(df.columns)
    print(f"  {CDM_ENTITIES[entity]:25s}  {count:6d} records  {cols:3d} columns")
