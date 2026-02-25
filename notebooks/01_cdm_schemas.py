# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - CDM Schema Definitions (PySpark StructTypes)
# MAGIC
# MAGIC This notebook defines the **silver layer schemas** based on the
# MAGIC [Microsoft Common Data Model (CDM)](https://github.com/microsoft/CDM/tree/master)
# MAGIC FinancialServices / RetailBankingCoreDataModel entities.
# MAGIC
# MAGIC Each schema is a PySpark `StructType` derived from the official CDM `.cdm.json` definitions at:
# MAGIC - `schemaDocuments/FinancialServices/FinancialServicesCommonDataModel/`
# MAGIC - `schemaDocuments/FinancialServices/RetailBankingCoreDataModel/`
# MAGIC
# MAGIC These schemas enforce the canonical structure for the silver layer.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    BooleanType,
    TimestampType,
    DateType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bank Entity Schema
# MAGIC
# MAGIC **CDM Source:** `FinancialServicesCommonDataModel/Bank.1.0.cdm.json`
# MAGIC
# MAGIC Represents the bank institution. Key attributes include identification,
# MAGIC address information, and status fields.

# COMMAND ----------

BANK_SCHEMA = StructType(
    [
        # --- Primary Key ---
        StructField("bankId", StringType(), nullable=False),
        # --- Audit Fields ---
        StructField("createdOn", TimestampType(), nullable=True),
        StructField("modifiedOn", TimestampType(), nullable=True),
        # --- Status Fields (CDM listLookup) ---
        StructField("statecode", IntegerType(), nullable=False),
        StructField("statecode_display", StringType(), nullable=True),
        StructField("statuscode", IntegerType(), nullable=True),
        StructField("statuscode_display", StringType(), nullable=True),
        # --- Business Fields ---
        StructField("name", StringType(), nullable=True),
        StructField("bankCode", StringType(), nullable=True),
        StructField("addressLine1", StringType(), nullable=True),
        StructField("addressLine2", StringType(), nullable=True),
        StructField("addressLine3", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("stateOrProvince", StringType(), nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("telephone1", StringType(), nullable=True),
        StructField("integrationKey", StringType(), nullable=True),
    ]
)

# Lookup values for Bank status fields
BANK_STATUS_VALUES = {
    0: "Active",
    1: "Inactive",
}

BANK_STATUS_REASON_VALUES = {
    1: "Active",
    2: "Inactive",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch Entity Schema
# MAGIC
# MAGIC **CDM Source:** `FinancialServicesCommonDataModel/Branch.1.0.cdm.json`
# MAGIC
# MAGIC Represents a bank branch. Linked to a Bank entity.
# MAGIC Includes address, contact, and operational details.

# COMMAND ----------

BRANCH_SCHEMA = StructType(
    [
        # --- Primary Key ---
        StructField("branchId", StringType(), nullable=False),
        # --- Foreign Keys ---
        StructField("bankId", StringType(), nullable=True),
        # --- Audit Fields ---
        StructField("createdOn", TimestampType(), nullable=True),
        StructField("modifiedOn", TimestampType(), nullable=True),
        # --- Status Fields ---
        StructField("statecode", IntegerType(), nullable=False),
        StructField("statecode_display", StringType(), nullable=True),
        StructField("statuscode", IntegerType(), nullable=True),
        StructField("statuscode_display", StringType(), nullable=True),
        # --- Business Fields ---
        StructField("name", StringType(), nullable=True),
        StructField("branchCode", StringType(), nullable=True),
        StructField("addressLine1", StringType(), nullable=True),
        StructField("addressLine2", StringType(), nullable=True),
        StructField("addressLine3", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("stateOrProvince", StringType(), nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("telephone1", StringType(), nullable=True),
        StructField("integrationKey", StringType(), nullable=True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contact Entity Schema (Customer)
# MAGIC
# MAGIC **CDM Source:** `FinancialServicesCommonDataModel/Contact.1.0.cdm.json`
# MAGIC
# MAGIC Represents a person with whom the bank has a relationship (customer, prospect, etc.).
# MAGIC The CDM Contact entity maps to what banks typically refer to as a "Customer".

# COMMAND ----------

CONTACT_SCHEMA = StructType(
    [
        # --- Primary Key ---
        StructField("contactId", StringType(), nullable=False),
        # --- Foreign Keys ---
        StructField("branchId", StringType(), nullable=True),
        # --- Audit Fields ---
        StructField("createdOn", TimestampType(), nullable=True),
        StructField("modifiedOn", TimestampType(), nullable=True),
        # --- Status Fields ---
        StructField("statecode", IntegerType(), nullable=False),
        StructField("statecode_display", StringType(), nullable=True),
        StructField("statuscode", IntegerType(), nullable=True),
        StructField("statuscode_display", StringType(), nullable=True),
        # --- Business Fields ---
        StructField("firstName", StringType(), nullable=True),
        StructField("lastName", StringType(), nullable=True),
        StructField("fullName", StringType(), nullable=True),
        StructField("emailAddress", StringType(), nullable=True),
        StructField("telephone1", StringType(), nullable=True),
        StructField("dateOfBirth", DateType(), nullable=True),
        StructField("joinDate", TimestampType(), nullable=True),
        StructField("tenureYears", DecimalType(18, 2), nullable=True),
        StructField("isManagedByBankSystem", BooleanType(), nullable=True),
        StructField("integrationKey", StringType(), nullable=True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account Entity Schema
# MAGIC
# MAGIC **CDM Source:** `FinancialServicesCommonDataModel/Account.1.0.cdm.json`
# MAGIC
# MAGIC Represents a business/customer account at the bank.
# MAGIC Linked to Branch and Contact entities.

# COMMAND ----------

ACCOUNT_SCHEMA = StructType(
    [
        # --- Primary Key ---
        StructField("accountId", StringType(), nullable=False),
        # --- Foreign Keys ---
        StructField("branchId", StringType(), nullable=True),
        StructField("primaryContactId", StringType(), nullable=True),
        # --- Audit Fields ---
        StructField("createdOn", TimestampType(), nullable=True),
        StructField("modifiedOn", TimestampType(), nullable=True),
        # --- Status Fields ---
        StructField("statecode", IntegerType(), nullable=False),
        StructField("statecode_display", StringType(), nullable=True),
        StructField("statuscode", IntegerType(), nullable=True),
        StructField("statuscode_display", StringType(), nullable=True),
        # --- Business Fields ---
        StructField("name", StringType(), nullable=True),
        StructField("accountNumber", StringType(), nullable=True),
        StructField("accountType", StringType(), nullable=True),
        StructField("joinDate", TimestampType(), nullable=True),
        StructField("tenureYears", DecimalType(18, 2), nullable=True),
        StructField("integrationKey", StringType(), nullable=True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FinancialHolding Entity Schema
# MAGIC
# MAGIC **CDM Source:** `RetailBankingCoreDataModel/Financialholding.1.0.cdm.json`
# MAGIC
# MAGIC Represents a financial holding (e.g., deposit account, savings, loan).
# MAGIC This is a simplified version of the CDM FinancialHolding entity
# MAGIC capturing the most relevant attributes for a banking demo.

# COMMAND ----------

FINANCIAL_HOLDING_SCHEMA = StructType(
    [
        # --- Primary Key ---
        StructField("financialHoldingId", StringType(), nullable=False),
        # --- Foreign Keys ---
        StructField("contactId", StringType(), nullable=True),
        StructField("accountId", StringType(), nullable=True),
        # --- Audit Fields ---
        StructField("createdOn", TimestampType(), nullable=True),
        StructField("modifiedOn", TimestampType(), nullable=True),
        # --- Status Fields ---
        StructField("statecode", IntegerType(), nullable=False),
        StructField("statecode_display", StringType(), nullable=True),
        StructField("statuscode", IntegerType(), nullable=True),
        StructField("statuscode_display", StringType(), nullable=True),
        # --- Business Fields ---
        StructField("name", StringType(), nullable=True),
        StructField("holdingType", IntegerType(), nullable=True),
        StructField("holdingType_display", StringType(), nullable=True),
        StructField("balance", DecimalType(18, 2), nullable=True),
        StructField("balanceDefault", DecimalType(18, 2), nullable=True),
        StructField("balanceDefaultDisplayValue", StringType(), nullable=True),
        StructField("openedDate", TimestampType(), nullable=True),
        StructField("maturityDate", TimestampType(), nullable=True),
        StructField("interestRate", DecimalType(10, 4), nullable=True),
        StructField("currencyCode", StringType(), nullable=True),
        StructField("integrationKey", StringType(), nullable=True),
    ]
)

# Lookup values for FinancialHolding type (CDM option set)
FINANCIAL_HOLDING_TYPE_VALUES = {
    104800000: "Deposit Account",
    104800001: "Savings Account",
    104800002: "Current Account",
    104800003: "Loan",
    104800004: "Line of Credit",
    104800005: "Investment",
    104800006: "Term Deposit",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Registry (for programmatic access)

# COMMAND ----------

CDM_SCHEMAS = {
    "bank": BANK_SCHEMA,
    "branch": BRANCH_SCHEMA,
    "contact": CONTACT_SCHEMA,
    "account": ACCOUNT_SCHEMA,
    "financial_holding": FINANCIAL_HOLDING_SCHEMA,
}


def get_cdm_schema(entity_name: str) -> StructType:
    """Retrieve the CDM PySpark schema for a given entity."""
    if entity_name not in CDM_SCHEMAS:
        raise ValueError(
            f"Unknown entity: {entity_name}. "
            f"Available: {list(CDM_SCHEMAS.keys())}"
        )
    return CDM_SCHEMAS[entity_name]


def print_schema_summary():
    """Print a summary of all CDM schemas."""
    print("=" * 60)
    print("CDM Banking Schema Registry")
    print("=" * 60)
    for name, schema in CDM_SCHEMAS.items():
        fields = schema.fields
        required = [f.name for f in fields if not f.nullable]
        print(f"\n{name.upper()} ({len(fields)} fields)")
        print(f"  Required: {', '.join(required) if required else 'None'}")
        for field in fields:
            nullable_flag = "" if field.nullable else " [REQUIRED]"
            print(f"  - {field.name:35s} {str(field.dataType):20s}{nullable_flag}")

# COMMAND ----------

print_schema_summary()
