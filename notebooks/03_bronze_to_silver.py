# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronze to Silver Transformations
# MAGIC
# MAGIC This notebook transforms **bronze (raw) data** into **silver (CDM-conformant) data**.
# MAGIC
# MAGIC Each transformation:
# MAGIC 1. Reads messy bronze Delta data
# MAGIC 2. Cleans and standardizes fields (trimming, case normalization, date parsing)
# MAGIC 3. Deduplicates records
# MAGIC 4. Maps bronze column names to CDM attribute names
# MAGIC 5. Applies CDM PySpark schemas
# MAGIC 6. Resolves lookup/option-set values
# MAGIC 7. Writes clean silver Delta tables
# MAGIC
# MAGIC **CDM Reference:** [Microsoft CDM - FinancialServices](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_cdm_schemas

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    TimestampType,
    DateType,
    IntegerType,
    DecimalType,
    BooleanType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Transformation Utilities

# COMMAND ----------

# DBTITLE 1,Untitled
def parse_flexible_timestamp(col_name: str, output_alias: str = None) -> F.Column:
    """
    Parse a string column with inconsistent date/time formats into a TimestampType.
    Tries multiple common formats and returns the first successful parse.
    """
    alias = output_alias or col_name
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "MM/dd/yyyy HH:mm"),
        F.to_timestamp(F.col(col_name), "dd-MM-yyyy HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd"),
        F.to_timestamp(F.col(col_name), "MM/dd/yyyy"),
        F.to_timestamp(F.col(col_name), "dd/MM/yyyy"),
def parse_flexible_date(col_name: str, output_alias: str = None) -> F.Column:
    """Parse a string column with inconsistent date formats into a DateType."""
    alias = output_alias or col_name
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
        F.to_date(F.col(col_name), "yyyy/MM/dd"),
    ).alias(alias)
    """
    Convert various boolean representations to a proper BooleanType.
    Handles: yes/no, true/false, 1/0, Y/N, active/inactive
    """
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("yes", "true", "1", "y", "active"), F.lit(True))
        .when(lower_col.isin("no", "false", "0", "n", "inactive", "closed"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    )


def resolve_statecode(col_name: str) -> F.Column:
    """
    Map various status representations to CDM statecode (Active=0, Inactive=1).
    """
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("active", "open", "1", "yes", "true"), F.lit(0))
        .when(
            lower_col.isin("inactive", "closed", "0", "no", "false", "dormant", "matured"),
            F.lit(1),
        )
        .otherwise(F.lit(0))  # Default to active
        .cast(IntegerType())
    )


def resolve_statecode_display(statecode_col: str) -> F.Column:
    """Map statecode integer to display text."""
    return (
        F.when(F.col(statecode_col) == 0, F.lit("Active"))
        .when(F.col(statecode_col) == 1, F.lit("Inactive"))
        .otherwise(F.lit("Unknown"))
    )


def resolve_statuscode(statecode_col: str) -> F.Column:
    """Map statecode to correlated statuscode (1=Active, 2=Inactive per CDM spec)."""
    return (
        F.when(F.col(statecode_col) == 0, F.lit(1))
        .when(F.col(statecode_col) == 1, F.lit(2))
        .otherwise(F.lit(1))
        .cast(IntegerType())
    )


def resolve_statuscode_display(statuscode_col: str) -> F.Column:
    """Map statuscode integer to display text."""
    return (
        F.when(F.col(statuscode_col) == 1, F.lit("Active"))
        .when(F.col(statuscode_col) == 2, F.lit("Inactive"))
        .otherwise(F.lit("Unknown"))
    )


def normalize_country(col_name: str) -> F.Column:
    """Normalize country codes to ISO 3166-1 alpha-2 (e.g., 'BE')."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("be", "bel", "belgium"), F.lit("BE"))
        .when(lower_col.isin("nl", "nld", "netherlands"), F.lit("NL"))
        .when(lower_col.isin("fr", "fra", "france"), F.lit("FR"))
        .when(lower_col.isin("de", "deu", "germany"), F.lit("DE"))
        .when(lower_col.isin("gb", "gbr", "uk", "united kingdom"), F.lit("GB"))
        .when(lower_col.isin("us", "usa", "united states"), F.lit("US"))
        .otherwise(F.upper(F.trim(F.col(col_name))))
    )


def deduplicate(df: DataFrame, id_col: str, order_col: str) -> DataFrame:
    """
    Remove duplicates by keeping the most recently modified record per ID.
    """
    window = Window.partitionBy(id_col).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def trim_all_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, type(F.lit("").expr.dataType.__class__)) or str(field.dataType) == "StringType()":
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def write_silver(df: DataFrame, entity: str, schema: StructType):
    """Write a DataFrame to the silver layer with the CDM schema, as Delta."""
    silver_path = get_silver_path(entity)

    # Select and cast columns to match CDM schema
    select_cols = []
    for field in schema.fields:
        if field.name in df.columns:
            select_cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            select_cols.append(F.lit(None).cast(field.dataType).alias(field.name))

    silver_df = df.select(*select_cols)

    silver_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(silver_path)

    record_count = silver_df.count()
    print(f"  -> Written {record_count} records to {silver_path}")
    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Bank (Bronze -> Silver)

# COMMAND ----------

def transform_bank():
    """Transform bronze Bank data to CDM-conformant silver."""
    print("\n--- Transforming Bank ---")

    # Read bronze
    bronze_df = spark.read.format("delta").load(get_bronze_path("bank"))
    print(f"  Bronze records: {bronze_df.count()}")

    # Deduplicate on integration key (keep latest modified)
    deduped_df = deduplicate(bronze_df, "integration_key", "modified_date")

    # Transform to CDM schema
    silver_df = deduped_df.select(
        F.col("src_bank_id").alias("bankId"),
        parse_flexible_timestamp("created_date", "createdOn"),
        parse_flexible_timestamp("modified_date", "modifiedOn"),
        resolve_statecode("status").alias("statecode"),
        F.trim(F.col("bank_name")).alias("name"),
        F.trim(F.col("bank_code")).alias("bankCode"),
        F.trim(F.col("address_1")).alias("addressLine1"),
        F.trim(F.col("address_2")).alias("addressLine2"),
        F.lit(None).cast("string").alias("addressLine3"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.initcap(F.trim(F.col("province"))).alias("stateOrProvince"),
        F.trim(F.col("zip_code")).alias("postalCode"),
        normalize_country("country_code").alias("country"),
        F.trim(F.col("phone")).alias("telephone1"),
        F.col("integration_key").alias("integrationKey"),
    )

    # Add derived status fields
    silver_df = (
        silver_df
        .withColumn("statecode_display", resolve_statecode_display("statecode"))
        .withColumn("statuscode", resolve_statuscode("statecode"))
        .withColumn("statuscode_display", resolve_statuscode_display("statuscode"))
    )

    return write_silver(silver_df, "bank", BANK_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Branch (Bronze -> Silver)

# COMMAND ----------

def transform_branch():
    """Transform bronze Branch data to CDM-conformant silver."""
    print("\n--- Transforming Branch ---")

    bronze_df = spark.read.format("delta").load(get_bronze_path("branch"))
    print(f"  Bronze records: {bronze_df.count()}")

    deduped_df = deduplicate(bronze_df, "integration_key", "updated_at")

    silver_df = deduped_df.select(
        F.col("branch_id").alias("branchId"),
        F.col("fk_bank_id").alias("bankId"),
        parse_flexible_timestamp("created_at", "createdOn"),
        parse_flexible_timestamp("updated_at", "modifiedOn"),
        resolve_statecode("is_active").alias("statecode"),
        F.initcap(F.trim(F.col("branch_name"))).alias("name"),
        F.trim(F.col("branch_code")).alias("branchCode"),
        F.trim(F.col("street_address")).alias("addressLine1"),
        F.trim(F.col("address_line_2")).alias("addressLine2"),
        F.lit(None).cast("string").alias("addressLine3"),
        F.initcap(F.trim(F.col("city_name"))).alias("city"),
        F.initcap(F.trim(F.col("state_province"))).alias("stateOrProvince"),
        F.trim(F.col("postal_code")).alias("postalCode"),
        normalize_country("country").alias("country"),
        F.trim(F.col("phone_number")).alias("telephone1"),
        F.col("integration_key").alias("integrationKey"),
    )

    silver_df = (
        silver_df
        .withColumn("statecode_display", resolve_statecode_display("statecode"))
        .withColumn("statuscode", resolve_statuscode("statecode"))
        .withColumn("statuscode_display", resolve_statuscode_display("statuscode"))
    )

    return write_silver(silver_df, "branch", BRANCH_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Contact / Customer (Bronze -> Silver)

# COMMAND ----------

def transform_contact():
    """Transform bronze Contact data to CDM-conformant silver."""
    print("\n--- Transforming Contact ---")

    bronze_df = spark.read.format("delta").load(get_bronze_path("contact"))
    print(f"  Bronze records: {bronze_df.count()}")

    deduped_df = deduplicate(bronze_df, "src_integration_key", "last_modified")

    silver_df = deduped_df.select(
        F.col("customer_id").alias("contactId"),
        F.col("primary_branch_id").alias("branchId"),
        parse_flexible_timestamp("created_timestamp", "createdOn"),
        parse_flexible_timestamp("last_modified", "modifiedOn"),
        resolve_statecode("is_active").alias("statecode"),
        # Name fields: trim and proper-case
        F.initcap(F.trim(F.col("first_name"))).alias("firstName"),
        F.initcap(F.trim(F.col("last_name"))).alias("lastName"),
        # Derive fullName if missing
        F.coalesce(
            F.initcap(F.trim(F.col("full_name"))),
            F.concat_ws(
                " ",
                F.initcap(F.trim(F.col("first_name"))),
                F.initcap(F.trim(F.col("last_name"))),
            ),
        ).alias("fullName"),
        F.lower(F.trim(F.col("email"))).alias("emailAddress"),
        F.trim(F.col("phone")).alias("telephone1"),
        parse_flexible_date("date_of_birth", "dateOfBirth"),
        parse_flexible_timestamp("join_date", "joinDate"),
        F.col("tenure").cast(DecimalType(18, 2)).alias("tenureYears"),
        normalize_boolean("managed_by_system").alias("isManagedByBankSystem"),
        F.col("src_integration_key").alias("integrationKey"),
    )

    silver_df = (
        silver_df
        .withColumn("statecode_display", resolve_statecode_display("statecode"))
        .withColumn("statuscode", resolve_statuscode("statecode"))
        .withColumn("statuscode_display", resolve_statuscode_display("statuscode"))
    )

    return write_silver(silver_df, "contact", CONTACT_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: Account (Bronze -> Silver)

# COMMAND ----------

def resolve_account_type(col_name: str) -> F.Column:
    """Normalize account type strings to CDM-standard values."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col == "checking", F.lit("Checking"))
        .when(lower_col == "savings", F.lit("Savings"))
        .when(lower_col == "business", F.lit("Business"))
        .when(lower_col == "joint", F.lit("Joint"))
        .when(lower_col == "student", F.lit("Student"))
        .otherwise(F.initcap(F.trim(F.col(col_name))))
    )


def transform_account():
    """Transform bronze Account data to CDM-conformant silver."""
    print("\n--- Transforming Account ---")

    bronze_df = spark.read.format("delta").load(get_bronze_path("account"))
    print(f"  Bronze records: {bronze_df.count()}")

    deduped_df = deduplicate(bronze_df, "int_key", "modified")

    silver_df = deduped_df.select(
        F.col("acct_id").alias("accountId"),
        F.col("branch_ref").alias("branchId"),
        F.col("primary_contact_ref").alias("primaryContactId"),
        parse_flexible_timestamp("created", "createdOn"),
        parse_flexible_timestamp("modified", "modifiedOn"),
        resolve_statecode("status_flag").alias("statecode"),
        F.trim(F.col("acct_name")).alias("name"),
        F.trim(F.col("account_number")).alias("accountNumber"),
        resolve_account_type("account_type").alias("accountType"),
        parse_flexible_timestamp("join_date", "joinDate"),
        F.col("tenure_years").cast(DecimalType(18, 2)).alias("tenureYears"),
        F.col("int_key").alias("integrationKey"),
    )

    silver_df = (
        silver_df
        .withColumn("statecode_display", resolve_statecode_display("statecode"))
        .withColumn("statuscode", resolve_statuscode("statecode"))
        .withColumn("statuscode_display", resolve_statuscode_display("statuscode"))
    )

    return write_silver(silver_df, "account", ACCOUNT_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: FinancialHolding (Bronze -> Silver)

# COMMAND ----------

def resolve_holding_type(col_name: str) -> F.Column:
    """
    Map various holding type strings to CDM option set integer codes.

    CDM FinancialHolding type codes:
      104800000 = Deposit Account
      104800001 = Savings Account
      104800002 = Current Account
      104800003 = Loan
      104800004 = Line of Credit
      104800005 = Investment
      104800006 = Term Deposit
    """
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("deposit", "deposit account"), F.lit(104800000))
        .when(lower_col.isin("savings", "savings account"), F.lit(104800001))
        .when(lower_col.isin("current", "current account"), F.lit(104800002))
        .when(lower_col == "loan", F.lit(104800003))
        .when(lower_col.isin("credit_line", "line of credit"), F.lit(104800004))
        .when(lower_col == "investment", F.lit(104800005))
        .when(lower_col.isin("term_deposit", "term deposit"), F.lit(104800006))
        .otherwise(F.lit(104800000))  # Default to Deposit
        .cast(IntegerType())
    )


def resolve_holding_type_display(type_code_col: str) -> F.Column:
    """Map holding type code to display text."""
    return (
        F.when(F.col(type_code_col) == 104800000, F.lit("Deposit Account"))
        .when(F.col(type_code_col) == 104800001, F.lit("Savings Account"))
        .when(F.col(type_code_col) == 104800002, F.lit("Current Account"))
        .when(F.col(type_code_col) == 104800003, F.lit("Loan"))
        .when(F.col(type_code_col) == 104800004, F.lit("Line of Credit"))
        .when(F.col(type_code_col) == 104800005, F.lit("Investment"))
        .when(F.col(type_code_col) == 104800006, F.lit("Term Deposit"))
        .otherwise(F.lit("Unknown"))
    )


def normalize_currency(col_name: str) -> F.Column:
    """Normalize currency codes to uppercase ISO 4217."""
    return F.upper(F.trim(F.col(col_name)))


def transform_financial_holding():
    """Transform bronze FinancialHolding data to CDM-conformant silver."""
    print("\n--- Transforming FinancialHolding ---")

    bronze_df = spark.read.format("delta").load(get_bronze_path("financial_holding"))
    print(f"  Bronze records: {bronze_df.count()}")

    deduped_df = deduplicate(bronze_df, "int_key", "updated_timestamp")

    silver_df = deduped_df.select(
        F.col("holding_id").alias("financialHoldingId"),
        F.col("customer_ref").alias("contactId"),
        F.col("account_ref").alias("accountId"),
        parse_flexible_timestamp("created_timestamp", "createdOn"),
        parse_flexible_timestamp("updated_timestamp", "modifiedOn"),
        resolve_statecode("status").alias("statecode"),
        F.trim(F.col("holding_name")).alias("name"),
        resolve_holding_type("type").alias("holdingType"),
        F.col("balance_amount").cast(DecimalType(18, 2)).alias("balance"),
        F.col("balance_amount").cast(DecimalType(18, 2)).alias("balanceDefault"),
        F.trim(F.col("balance_display")).alias("balanceDefaultDisplayValue"),
        parse_flexible_timestamp("opened_on", "openedDate"),
        parse_flexible_timestamp("maturity_date", "maturityDate"),
        F.col("interest_rate").cast(DecimalType(10, 4)).alias("interestRate"),
        normalize_currency("currency").alias("currencyCode"),
        F.col("int_key").alias("integrationKey"),
    )

    # Add derived fields
    silver_df = (
        silver_df
        .withColumn("holdingType_display", resolve_holding_type_display("holdingType"))
        .withColumn("statecode_display", resolve_statecode_display("statecode"))
        .withColumn("statuscode", resolve_statuscode("statecode"))
        .withColumn("statuscode_display", resolve_statuscode_display("statuscode"))
    )

    return write_silver(silver_df, "financial_holding", FINANCIAL_HOLDING_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute All Transformations

# COMMAND ----------

def run_all_transformations():
    """Run all bronze-to-silver transformations."""
    print("=" * 60)
    print("Bronze -> Silver Transformations (CDM Conformant)")
    print("=" * 60)

    results = {}
    results["bank"] = transform_bank()
    results["branch"] = transform_branch()
    results["contact"] = transform_contact()
    results["account"] = transform_account()
    results["financial_holding"] = transform_financial_holding()

    print("\n" + "=" * 60)
    print("All transformations complete!")
    print("=" * 60)

    return results

# COMMAND ----------

silver_results = run_all_transformations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Data

# COMMAND ----------

def validate_silver_data():
    """Validate silver data against CDM schemas and print quality metrics."""
    print("=" * 60)
    print("Silver Data Validation Report")
    print("=" * 60)

    for entity_name, schema in CDM_SCHEMAS.items():
        silver_path = get_silver_path(entity_name)
        silver_df = spark.read.format("delta").load(silver_path)

        total_count = silver_df.count()

        # Check for required fields with nulls
        required_fields = [f.name for f in schema.fields if not f.nullable]
        null_counts = {}
        for field_name in required_fields:
            null_count = silver_df.filter(F.col(field_name).isNull()).count()
            if null_count > 0:
                null_counts[field_name] = null_count

        # Completeness metrics for all fields
        all_null_counts = {}
        for field in schema.fields:
            null_count = silver_df.filter(F.col(field.name).isNull()).count()
            completeness = ((total_count - null_count) / total_count * 100) if total_count > 0 else 0
            all_null_counts[field.name] = {
                "nulls": null_count,
                "completeness": completeness,
            }

        print(f"\n{'='*40}")
        print(f"Entity: {CDM_ENTITIES[entity_name]}")
        print(f"{'='*40}")
        print(f"  Total records: {total_count}")
        print(f"  Schema fields: {len(schema.fields)}")

        if null_counts:
            print(f"  ⚠ Required fields with nulls:")
            for field, count in null_counts.items():
                print(f"    - {field}: {count} nulls")
        else:
            print(f"  ✓ All required fields populated")

        # Print field completeness
        print(f"  Field completeness:")
        for field_name, metrics in all_null_counts.items():
            bar_len = int(metrics["completeness"] / 5)
            bar = "█" * bar_len + "░" * (20 - bar_len)
            print(f"    {field_name:35s} {bar} {metrics['completeness']:5.1f}%")

        # Show sample
        print(f"\n  Sample data:")
        silver_df.show(3, truncate=40)

# COMMAND ----------

validate_silver_data()
