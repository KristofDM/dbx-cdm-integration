# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuration & Setup
# MAGIC
# MAGIC This notebook defines the configuration for the Databricks + Microsoft CDM integration demo.
# MAGIC It sets up paths, catalog/schema names, and common utilities used across all notebooks.
# MAGIC
# MAGIC **CDM Reference:** [Microsoft Common Data Model](https://github.com/microsoft/CDM/tree/master)
# MAGIC
# MAGIC **Entities implemented (Retail Banking Core Data Model):**
# MAGIC - Bank
# MAGIC - Branch
# MAGIC - Contact (Customer)
# MAGIC - Account
# MAGIC - FinancialHolding (Fh_account)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Configuration

# COMMAND ----------

# --- Storage Paths ---
# Adjust these paths to match your Databricks workspace / mount points.
# For Unity Catalog, use catalog.schema format instead.

# Base path for data storage (DBFS or mounted storage)
BASE_PATH = "/mnt/cdm_demo"  # Change to your mount point or DBFS path

# Bronze layer: raw ingested data (messy, as-is from source systems)
BRONZE_PATH = f"{BASE_PATH}/bronze"

# Silver layer: cleaned, CDM-conformant canonical data
SILVER_PATH = f"{BASE_PATH}/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Configuration (Optional)
# MAGIC
# MAGIC If using Unity Catalog, uncomment and configure these settings.

# COMMAND ----------

# --- Unity Catalog Settings ---
# Uncomment below if using Unity Catalog instead of DBFS paths
# CATALOG_NAME = "cdm_banking_demo"
# SCHEMA_BRONZE = "bronze"
# SCHEMA_SILVER = "silver"

# To use Unity Catalog tables instead of Delta paths:
# USE_UNITY_CATALOG = True
USE_UNITY_CATALOG = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Table Names

# COMMAND ----------

# CDM Entity names (matching Microsoft CDM FinancialServices schema)
CDM_ENTITIES = {
    "bank": "Bank",
    "branch": "Branch",
    "contact": "Contact",
    "account": "Account",
    "financial_holding": "FinancialHolding",
}

# Bronze table/path mappings
BRONZE_TABLES = {
    entity: f"{BRONZE_PATH}/{name}" for entity, name in CDM_ENTITIES.items()
}

# Silver table/path mappings
SILVER_TABLES = {
    entity: f"{SILVER_PATH}/{name}" for entity, name in CDM_ENTITIES.items()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Settings

# COMMAND ----------

# Number of records to generate for each entity in the bronze layer
BRONZE_RECORD_COUNTS = {
    "bank": 5,
    "branch": 20,
    "contact": 200,
    "account": 150,
    "financial_holding": 300,
}

# Random seed for reproducibility
RANDOM_SEED = 42

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_bronze_path(entity: str) -> str:
    """Get the bronze Delta table path for a given entity."""
    return BRONZE_TABLES[entity]


def get_silver_path(entity: str) -> str:
    """Get the silver Delta table path for a given entity."""
    return SILVER_TABLES[entity]


def print_config():
    """Print the current configuration for verification."""
    print("=" * 60)
    print("CDM Banking Demo - Configuration")
    print("=" * 60)
    print(f"Base Path:    {BASE_PATH}")
    print(f"Bronze Path:  {BRONZE_PATH}")
    print(f"Silver Path:  {SILVER_PATH}")
    print(f"Unity Catalog: {USE_UNITY_CATALOG}")
    print()
    print("Bronze Tables:")
    for entity, path in BRONZE_TABLES.items():
        count = BRONZE_RECORD_COUNTS[entity]
        print(f"  {entity:25s} -> {path} ({count} records)")
    print()
    print("Silver Tables:")
    for entity, path in SILVER_TABLES.items():
        print(f"  {entity:25s} -> {path}")
    print("=" * 60)

# COMMAND ----------

print_config()
