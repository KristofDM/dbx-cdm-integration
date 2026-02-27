# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - CDM Schema Loader (Dynamic from .cdm.json)
# MAGIC
# MAGIC **Production approach:** Schemas are parsed at runtime from `.cdm.json` files.
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. **Standard entities** — Loaded from `cdm_schemas/standard/` (mirrors the Microsoft CDM repo)
# MAGIC 2. **Extension entities** — Loaded from `cdm_schemas/extensions/BankExtended.cdm.json`
# MAGIC    - `BankExtended` uses `extendsEntity` to inherit ALL fields from standard `Bank`
# MAGIC    - Then adds org-specific fields (riskRating, swiftCode, regulatoryLicenseNumber, etc.)
# MAGIC 3. **Custom entities** — Loaded from `cdm_schemas/extensions/KYCCheck.cdm.json`
# MAGIC    - Net-new entity not in standard CDM, created for AML/KYC compliance
# MAGIC
# MAGIC **Why this matters:**
# MAGIC > - No hand-coded PySpark `StructType` definitions
# MAGIC > - Schema changes = edit the `.cdm.json` file, not Python code
# MAGIC > - Extensions are additive — standard CDM updates don't break your customizations
# MAGIC > - The parser resolves inheritance chains automatically

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load All Schemas from Manifest
# MAGIC
# MAGIC The manifest (`manifest.cdm.json`) lists every entity the deployment uses.
# MAGIC The parser resolves each one — including inheritance for extended entities.

# COMMAND ----------

# Load all PySpark schemas from the manifest
CDM_SCHEMAS = cdm_parser.load_all_from_manifest()

print("=" * 60)
print("CDM Schema Registry — Loaded from .cdm.json")
print("=" * 60)
for name, schema in CDM_SCHEMAS.items():
    logical = _ENTITY_NAME_MAP.get(name, name.lower())
    print(f"\n  {name} ({len(schema.fields)} fields) → silver entity: {logical}")
    for field in schema.fields:
        nullable_flag = "" if field.nullable else " [REQUIRED]"
        print(f"    {field.name:40s} {str(field.dataType):20s}{nullable_flag}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demonstrate Extension Resolution
# MAGIC
# MAGIC This is the KEY production pattern: `BankExtended` inherits from `Bank`.
# MAGIC The parser resolves the full attribute chain.

# COMMAND ----------

print("=" * 60)
print("Extension Resolution Demo: BankExtended → Bank")
print("=" * 60)

# Show the base Bank entity
print("\n--- Standard CDM Bank (18 fields) ---")
bank_base = cdm_parser.resolve_entity("standard/Bank.cdm.json", "Bank")
for attr in bank_base["hasAttributes"]:
    print(f"  {attr['name']:40s} {attr.get('dataType', 'string'):15s}")

# Show the extended entity (inherits Bank + adds custom fields)
print("\n--- BankExtended (inherits Bank + 5 custom fields) ---")
bank_ext = cdm_parser.resolve_entity("extensions/BankExtended.cdm.json", "BankExtended")
standard_count = len(bank_base["hasAttributes"])
for i, attr in enumerate(bank_ext["hasAttributes"]):
    marker = " ← EXTENSION" if i >= standard_count else ""
    print(f"  {attr['name']:40s} {attr.get('dataType', 'string'):15s}{marker}")

print(f"\nBase fields: {standard_count}")
print(f"Extension fields: {len(bank_ext['hasAttributes']) - standard_count}")
print(f"Total resolved: {len(bank_ext['hasAttributes'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Custom Entity: KYCCheck
# MAGIC
# MAGIC This entity does NOT exist in the standard CDM.
# MAGIC It was created for Belgian AML/CFT regulatory compliance.

# COMMAND ----------

print("=" * 60)
print("Custom Entity: KYCCheck (not in standard CDM)")
print("=" * 60)

cdm_parser.print_entity_summary("extensions/KYCCheck.cdm.json", "KYCCheck")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Option Sets (Lookup Values)
# MAGIC
# MAGIC Option sets are parsed directly from the `.cdm.json` definitions.

# COMMAND ----------

print("=" * 60)
print("Option Sets — Parsed from .cdm.json")
print("=" * 60)

for entry in MANIFEST_ENTITIES:
    name = entry["entityName"]
    path, ename = cdm_parser._parse_entity_reference(entry["entityPath"])
    option_sets = cdm_parser.get_option_sets(path, ename)
    if option_sets:
        print(f"\n  {name}:")
        for field_name, values in option_sets.items():
            print(f"    {field_name}:")
            for v in values:
                print(f"      {v['value']:>10} = {v['displayText']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Schema Lookup for Downstream Notebooks
# MAGIC
# MAGIC Creates `CDM_SCHEMAS_BY_ENTITY` dict mapping logical entity names
# MAGIC (bank, branch, etc.) to their PySpark StructType.

# COMMAND ----------

# Map logical entity names → PySpark schemas
CDM_SCHEMAS_BY_ENTITY = {}
for entry in MANIFEST_ENTITIES:
    cdm_name = entry["entityName"]
    logical_name = _ENTITY_NAME_MAP.get(cdm_name, cdm_name.lower())
    CDM_SCHEMAS_BY_ENTITY[logical_name] = CDM_SCHEMAS[cdm_name]


# Load CDM descriptions for Unity Catalog column comments
CDM_DESCRIPTIONS = cdm_parser.load_all_descriptions_from_manifest()
CDM_DESCRIPTIONS_BY_ENTITY = {}
for entry in MANIFEST_ENTITIES:
    cdm_name = entry["entityName"]
    logical_name = _ENTITY_NAME_MAP.get(cdm_name, cdm_name.lower())
    CDM_DESCRIPTIONS_BY_ENTITY[logical_name] = CDM_DESCRIPTIONS[cdm_name]


def get_cdm_schema(entity_name: str):
    """Retrieve the CDM PySpark schema for a given logical entity name."""
    if entity_name not in CDM_SCHEMAS_BY_ENTITY:
        raise ValueError(
            f"Unknown entity: {entity_name}. "
            f"Available: {list(CDM_SCHEMAS_BY_ENTITY.keys())}"
        )
    return CDM_SCHEMAS_BY_ENTITY[entity_name]


def get_cdm_descriptions(entity_name: str) -> dict:
    """Retrieve CDM entity and column descriptions for a given logical entity name.

    Returns:
        Dict with 'entity_description' (str) and 'column_descriptions' (dict).
    """
    if entity_name not in CDM_DESCRIPTIONS_BY_ENTITY:
        raise ValueError(
            f"Unknown entity: {entity_name}. "
            f"Available: {list(CDM_DESCRIPTIONS_BY_ENTITY.keys())}"
        )
    return CDM_DESCRIPTIONS_BY_ENTITY[entity_name]


print("\n✓ Schema registry ready:")
for name, schema in CDM_SCHEMAS_BY_ENTITY.items():
    desc_info = CDM_DESCRIPTIONS_BY_ENTITY[name]
    col_desc_count = len(desc_info["column_descriptions"])
    print(f"  get_cdm_schema('{name}') → {len(schema.fields)} fields, {col_desc_count} column descriptions")
