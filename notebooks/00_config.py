# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuration & Setup (Production)
# MAGIC
# MAGIC This notebook sets up the environment for the Databricks + Microsoft CDM integration.
# MAGIC
# MAGIC **Production approach:**
# MAGIC - Schemas are loaded dynamically from `.cdm.json` files (not hand-coded)
# MAGIC - Column mappings are externalized in `config/entity_mappings.yaml`
# MAGIC - Quality rules are defined in `config/quality_rules.yaml`
# MAGIC - Extension entities (`BankExtended`) demonstrate the CDM extension pattern
# MAGIC - Custom entities (`KYCCheck`) demonstrate creating net-new entities
# MAGIC
# MAGIC **Key difference from demo:**
# MAGIC > You do NOT fork the CDM repo. You EXTEND it.
# MAGIC > Standard CDM entities live in `cdm_schemas/standard/`.
# MAGIC > Your org-specific extensions live in `cdm_schemas/extensions/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Path & Dependencies

# COMMAND ----------

import sys
import os

# Add the lib/ directory to the Python path
# In Databricks Repos, this resolves relative to the repo root
_NOTEBOOK_DIR = os.path.dirname(os.path.abspath("__file__"))
_PROJECT_ROOT = os.path.abspath(os.path.join(_NOTEBOOK_DIR, ".."))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "lib"))

# Verify lib is importable
from cdm_schema_parser import CdmSchemaParser
from transform_engine import TransformEngine
from quality_engine import QualityEngine

print(f"Project root: {_PROJECT_ROOT}")
print(f"Lib path:     {os.path.join(_PROJECT_ROOT, 'lib')}")
print("✓ All library modules loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Configuration

# COMMAND ----------

# --- Storage Paths ---
BASE_PATH = "/mnt/cdm_demo"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"

# --- Unity Catalog Settings ---
CATALOG_NAME = "cdm_banking_demo"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
USE_UNITY_CATALOG = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDM Schema & Config Paths

# COMMAND ----------

# Paths to CDM schemas and configuration files
CDM_SCHEMAS_ROOT = os.path.join(_PROJECT_ROOT, "cdm_schemas")
CONFIG_DIR = os.path.join(_PROJECT_ROOT, "config")
ENTITY_MAPPINGS_PATH = os.path.join(CONFIG_DIR, "entity_mappings.yaml")
QUALITY_RULES_PATH = os.path.join(CONFIG_DIR, "quality_rules.yaml")

print(f"CDM Schemas:      {CDM_SCHEMAS_ROOT}")
print(f"Entity Mappings:  {ENTITY_MAPPINGS_PATH}")
print(f"Quality Rules:    {QUALITY_RULES_PATH}")

# Verify files exist
for path in [CDM_SCHEMAS_ROOT, ENTITY_MAPPINGS_PATH, QUALITY_RULES_PATH]:
    assert os.path.exists(path), f"Missing: {path}"
print("✓ All config files found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Registry
# MAGIC
# MAGIC The manifest (`cdm_schemas/manifest.cdm.json`) is the single source of truth
# MAGIC for which entities are in scope. This replaces a hard-coded dict.

# COMMAND ----------

# Initialize the CDM schema parser
cdm_parser = CdmSchemaParser(CDM_SCHEMAS_ROOT)

# Load entity list from the manifest
manifest = cdm_parser.load_manifest()
MANIFEST_ENTITIES = cdm_parser.get_manifest_entities()
MANIFEST_RELATIONSHIPS = cdm_parser.get_manifest_relationships()

# Build entity registry: logical_name → { cdm_name, path, schema }
CDM_ENTITIES = {}
_ENTITY_NAME_MAP = {
    "Bank": "bank",
    "Branch": "branch",
    "Contact": "contact",
    "Account": "account",
    "FinancialHolding": "financial_holding",
    "KYCCheck": "kyc_check",
}

for entry in MANIFEST_ENTITIES:
    cdm_name = entry["entityName"]
    logical_name = _ENTITY_NAME_MAP.get(cdm_name, cdm_name.lower())
    CDM_ENTITIES[logical_name] = cdm_name

print(f"\nEntities from manifest ({manifest['manifestName']}):")
for logical, cdm in CDM_ENTITIES.items():
    is_ext = any(
        e.get("explanation", "").startswith("Uses") or e.get("explanation", "").startswith("Custom")
        for e in MANIFEST_ENTITIES if e["entityName"] == cdm
    )
    tag = " [EXTENSION]" if is_ext else ""
    print(f"  {logical:25s} → {cdm}{tag}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path Helpers

# COMMAND ----------

# Bronze/Silver table path mappings
BRONZE_TABLES = {entity: f"{BRONZE_PATH}/{name}" for entity, name in CDM_ENTITIES.items()}
SILVER_TABLES = {entity: f"{SILVER_PATH}/{name}" for entity, name in CDM_ENTITIES.items()}


def get_bronze_path(entity: str) -> str:
    """Get the bronze Delta table path for a given entity."""
    return BRONZE_TABLES[entity]


def get_silver_path(entity: str) -> str:
    """Get the silver Delta table path for a given entity."""
    return SILVER_TABLES[entity]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Settings

# COMMAND ----------

BRONZE_RECORD_COUNTS = {
    "bank": 5,
    "branch": 20,
    "contact": 200,
    "account": 150,
    "financial_holding": 300,
    "kyc_check": 400,
}

RANDOM_SEED = 42

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Summary

# COMMAND ----------

def print_config():
    """Print the current configuration for verification."""
    print("=" * 60)
    print("CDM Banking Demo — Production Configuration")
    print("=" * 60)
    print(f"Manifest:      {manifest['manifestName']}")
    print(f"Base Path:     {BASE_PATH}")
    print(f"Bronze Path:   {BRONZE_PATH}")
    print(f"Silver Path:   {SILVER_PATH}")
    print(f"Unity Catalog: {USE_UNITY_CATALOG}")
    print(f"CDM Schemas:   {CDM_SCHEMAS_ROOT}")
    print()
    print("Entities:")
    for entity, cdm_name in CDM_ENTITIES.items():
        count = BRONZE_RECORD_COUNTS.get(entity, "N/A")
        print(f"  {entity:25s} → {cdm_name:20s} ({count} records)")
    print()
    print("Relationships:")
    for rel in MANIFEST_RELATIONSHIPS:
        print(f"  {rel['fromEntity']}.{rel['fromAttribute']} → {rel['toEntity']}.{rel['toAttribute']}")
    print("=" * 60)

# COMMAND ----------

print_config()
