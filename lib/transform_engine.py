"""
Config-Driven Transform Engine — Reads entity_mappings.yaml and applies transformations.

This module replaces hardcoded bronze-to-silver transformation logic.
Instead of writing per-entity transform functions, the engine:
  1. Reads column mappings from YAML config
  2. Applies registered transform functions dynamically
  3. Enforces the CDM schema at write time

Adding a new entity or changing a mapping only requires editing the YAML file.
No Python code changes needed.
"""

import yaml
import os
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    IntegerType,
    BooleanType,
    DecimalType,
)


# =============================================================================
# Transform Function Registry
# =============================================================================
# Each function takes a column name and returns a PySpark Column expression.

def _parse_flexible_timestamp(col_name: str) -> F.Column:
    """Parse a string column with inconsistent date/time formats into TimestampType."""
    return F.coalesce(
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "MM/dd/yyyy HH:mm"),
        F.to_timestamp(F.col(col_name), "dd-MM-yyyy HH:mm:ss"),
        F.to_timestamp(F.col(col_name), "yyyy/MM/dd"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(F.col(col_name), "yyyy-MM-dd"),
        F.to_timestamp(F.col(col_name), "MM/dd/yyyy"),
        F.to_timestamp(F.col(col_name), "dd/MM/yyyy"),
    )


def _parse_flexible_date(col_name: str) -> F.Column:
    """Parse a string column with inconsistent date formats into DateType."""
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
        F.to_date(F.col(col_name), "yyyy/MM/dd"),
    )


def _resolve_statecode(col_name: str) -> F.Column:
    """Map various status representations to CDM statecode (Active=0, Inactive=1)."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("active", "open", "1", "yes", "true"), F.lit(0))
        .when(lower_col.isin("inactive", "closed", "0", "no", "false", "dormant", "matured"), F.lit(1))
        .otherwise(F.lit(0))
        .cast(IntegerType())
    )


def _normalize_boolean(col_name: str) -> F.Column:
    """Convert various boolean text representations to BooleanType."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("yes", "true", "1", "y", "active"), F.lit(True))
        .when(lower_col.isin("no", "false", "0", "n", "inactive", "closed"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    )


def _normalize_country(col_name: str) -> F.Column:
    """Normalize country codes to ISO 3166-1 alpha-2."""
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


def _normalize_currency(col_name: str) -> F.Column:
    """Normalize currency codes to uppercase ISO 4217."""
    return F.upper(F.trim(F.col(col_name)))


def _resolve_account_type(col_name: str) -> F.Column:
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


def _resolve_holding_type(col_name: str) -> F.Column:
    """Map holding type strings to CDM option set integer codes."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("deposit", "deposit account"), F.lit(104800000))
        .when(lower_col.isin("savings", "savings account"), F.lit(104800001))
        .when(lower_col.isin("current", "current account"), F.lit(104800002))
        .when(lower_col == "loan", F.lit(104800003))
        .when(lower_col.isin("credit_line", "line of credit"), F.lit(104800004))
        .when(lower_col == "investment", F.lit(104800005))
        .when(lower_col.isin("term_deposit", "term deposit"), F.lit(104800006))
        .otherwise(F.lit(104800000))
        .cast(IntegerType())
    )


def _resolve_kyc_check_type(col_name: str) -> F.Column:
    """Map KYC check type strings to option set integer codes."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("id_verification", "id verification", "id"), F.lit(0))
        .when(lower_col.isin("address_proof", "address proof", "address"), F.lit(1))
        .when(lower_col.isin("income_verification", "income verification", "income"), F.lit(2))
        .when(lower_col.isin("pep_check", "pep check", "pep"), F.lit(3))
        .when(lower_col.isin("sanctions_screening", "sanctions screening", "sanctions"), F.lit(4))
        .otherwise(F.lit(0))
        .cast(IntegerType())
    )


def _resolve_kyc_check_result(col_name: str) -> F.Column:
    """Map KYC check result strings to option set integer codes."""
    lower_col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(lower_col.isin("pass", "passed", "approved"), F.lit(0))
        .when(lower_col.isin("fail", "failed", "rejected"), F.lit(1))
        .when(lower_col.isin("pending", "in_progress", "in progress"), F.lit(2))
        .when(lower_col.isin("expired", "lapsed"), F.lit(3))
        .otherwise(F.lit(2))  # Default to Pending
        .cast(IntegerType())
    )


# =============================================================================
# Derived Field Transform Registry
# =============================================================================

def _statecode_to_display(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 0, F.lit("Active"))
        .when(F.col(col_name) == 1, F.lit("Inactive"))
        .otherwise(F.lit("Unknown"))
    )


def _statecode_to_statuscode(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 0, F.lit(1))
        .when(F.col(col_name) == 1, F.lit(2))
        .otherwise(F.lit(1))
        .cast(IntegerType())
    )


def _statuscode_to_display(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 1, F.lit("Active"))
        .when(F.col(col_name) == 2, F.lit("Inactive"))
        .otherwise(F.lit("Unknown"))
    )


def _holding_type_to_display(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 104800000, F.lit("Deposit Account"))
        .when(F.col(col_name) == 104800001, F.lit("Savings Account"))
        .when(F.col(col_name) == 104800002, F.lit("Current Account"))
        .when(F.col(col_name) == 104800003, F.lit("Loan"))
        .when(F.col(col_name) == 104800004, F.lit("Line of Credit"))
        .when(F.col(col_name) == 104800005, F.lit("Investment"))
        .when(F.col(col_name) == 104800006, F.lit("Term Deposit"))
        .otherwise(F.lit("Unknown"))
    )


def _kyc_check_type_to_display(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 0, F.lit("ID Verification"))
        .when(F.col(col_name) == 1, F.lit("Address Proof"))
        .when(F.col(col_name) == 2, F.lit("Income Verification"))
        .when(F.col(col_name) == 3, F.lit("PEP Check"))
        .when(F.col(col_name) == 4, F.lit("Sanctions Screening"))
        .otherwise(F.lit("Unknown"))
    )


def _kyc_check_result_to_display(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) == 0, F.lit("Pass"))
        .when(F.col(col_name) == 1, F.lit("Fail"))
        .when(F.col(col_name) == 2, F.lit("Pending"))
        .when(F.col(col_name) == 3, F.lit("Expired"))
        .otherwise(F.lit("Unknown"))
    )


# =============================================================================
# Transform Registry — Maps YAML transform names to functions
# =============================================================================

TRANSFORM_REGISTRY = {
    "trim": lambda col: F.trim(F.col(col)),
    "initcap_trim": lambda col: F.initcap(F.trim(F.col(col))),
    "upper_trim": lambda col: F.upper(F.trim(F.col(col))),
    "lower_trim": lambda col: F.lower(F.trim(F.col(col))),
    "parse_timestamp": _parse_flexible_timestamp,
    "parse_date": _parse_flexible_date,
    "resolve_statecode": _resolve_statecode,
    "normalize_country": _normalize_country,
    "normalize_boolean": _normalize_boolean,
    "normalize_currency": _normalize_currency,
    "resolve_account_type": _resolve_account_type,
    "resolve_holding_type": _resolve_holding_type,
    "resolve_kyc_check_type": _resolve_kyc_check_type,
    "resolve_kyc_check_result": _resolve_kyc_check_result,
    "cast_integer": lambda col: F.col(col).cast(IntegerType()),
    "cast_decimal": lambda col: F.col(col).cast(DecimalType(18, 2)),
    "cast_decimal_rate": lambda col: F.col(col).cast(DecimalType(10, 4)),
}

DERIVED_REGISTRY = {
    "statecode_to_display": _statecode_to_display,
    "statecode_to_statuscode": _statecode_to_statuscode,
    "statuscode_to_display": _statuscode_to_display,
    "holding_type_to_display": _holding_type_to_display,
    "kyc_check_type_to_display": _kyc_check_type_to_display,
    "kyc_check_result_to_display": _kyc_check_result_to_display,
}


# =============================================================================
# Transform Engine
# =============================================================================

class TransformEngine:
    """
    Config-driven bronze-to-silver transformation engine.

    Reads entity_mappings.yaml, applies column mappings and transforms,
    deduplicates, and writes CDM-conformant silver Delta tables.
    """

    def __init__(self, config_path: str, spark: SparkSession):
        """
        Args:
            config_path: Path to entity_mappings.yaml
            spark: Active SparkSession
        """
        self.spark = spark
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.entities = self.config.get("entities", {})

    def get_entity_config(self, entity_name: str) -> dict:
        """Get the mapping configuration for an entity."""
        if entity_name not in self.entities:
            raise ValueError(
                f"Entity '{entity_name}' not in config. "
                f"Available: {list(self.entities.keys())}"
            )
        return self.entities[entity_name]

    def transform(
        self,
        entity_name: str,
        bronze_df: DataFrame,
        cdm_schema: StructType,
    ) -> DataFrame:
        """
        Apply config-driven transformations to a bronze DataFrame.

        Steps:
            1. Deduplicate based on config
            2. Apply column mappings with transforms
            3. Apply derived fields
            4. Enforce CDM schema types

        Args:
            entity_name: Logical entity name (e.g., "bank")
            bronze_df: Raw bronze DataFrame
            cdm_schema: Target CDM PySpark StructType

        Returns:
            Transformed DataFrame conforming to the CDM schema
        """
        config = self.get_entity_config(entity_name)
        bronze_config = config.get("bronze", {})
        mappings = config.get("mappings", {})
        derived = config.get("derived", [])

        # --- Step 1: Deduplicate ---
        dedup_key = bronze_config.get("dedup_key")
        dedup_order = bronze_config.get("dedup_order")
        if dedup_key and dedup_order:
            df = self._deduplicate(bronze_df, dedup_key, dedup_order)
        else:
            df = bronze_df

        # --- Step 2: Apply column mappings ---
        select_cols = []
        for target_col, mapping in mappings.items():
            col_expr = self._apply_mapping(target_col, mapping, df)
            select_cols.append(col_expr)

        df = df.select(*select_cols)

        # --- Step 3: Apply derived fields ---
        for derived_field in derived:
            field_name = derived_field["name"]
            from_col = derived_field["from"]
            transform_name = derived_field["transform"]

            if transform_name in DERIVED_REGISTRY:
                df = df.withColumn(field_name, DERIVED_REGISTRY[transform_name](from_col))

        # --- Step 4: Enforce CDM schema ---
        df = self._enforce_schema(df, cdm_schema)

        return df

    def _apply_mapping(self, target_col: str, mapping: dict, df: DataFrame) -> F.Column:
        """Apply a single column mapping from YAML config."""
        source_col = mapping.get("source")
        transform_name = mapping.get("transform")
        default = mapping.get("default")
        fallback_sources = mapping.get("fallback_sources")

        # Handle fullName derivation (special case)
        if transform_name == "derive_fullname" and fallback_sources:
            first_src, last_src = fallback_sources[0], fallback_sources[1]
            return F.coalesce(
                F.initcap(F.trim(F.col(source_col))),
                F.concat_ws(
                    " ",
                    F.initcap(F.trim(F.col(first_src))),
                    F.initcap(F.trim(F.col(last_src))),
                ),
            ).alias(target_col)

        # No source → use default value
        if source_col is None:
            return F.lit(default).alias(target_col)

        # No transform → just rename
        if transform_name is None:
            return F.col(source_col).alias(target_col)

        # Apply registered transform
        if transform_name in TRANSFORM_REGISTRY:
            return TRANSFORM_REGISTRY[transform_name](source_col).alias(target_col)

        # Unknown transform → pass through with trim
        print(f"  WARNING: Unknown transform '{transform_name}' for {target_col}, using trim")
        return F.trim(F.col(source_col)).alias(target_col)

    def _deduplicate(self, df: DataFrame, id_col: str, order_col: str) -> DataFrame:
        """Remove duplicates, keeping the most recently modified record per ID."""
        window = Window.partitionBy(id_col).orderBy(F.col(order_col).desc_nulls_last())
        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def _enforce_schema(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Select and cast columns to match the CDM schema exactly."""
        select_cols = []
        for field in schema.fields:
            if field.name in df.columns:
                select_cols.append(
                    F.col(field.name).cast(field.dataType).alias(field.name)
                )
            else:
                select_cols.append(
                    F.lit(None).cast(field.dataType).alias(field.name)
                )
        return df.select(*select_cols)

    def write_silver(
        self,
        df: DataFrame,
        silver_path: str,
        entity_display_name: str,
        table_fqn: Optional[str] = None,
        entity_description: Optional[str] = None,
        column_descriptions: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """
        Write transformed data to the silver layer.

        When table_fqn is provided (Unity Catalog enabled), writes using
        saveAsTable to a UC-managed table and applies CDM entity/column
        descriptions as metadata COMMENTs.

        When table_fqn is None, falls back to path-based Delta write.

        Args:
            df: Transformed DataFrame
            silver_path: Fallback Delta path (used when UC is disabled)
            entity_display_name: Human-readable entity name for logging
            table_fqn: Fully qualified UC table name, e.g. `catalog`.`schema`.`table` (optional)
            entity_description: CDM entity description for TABLE COMMENT (optional)
            column_descriptions: Dict of {column_name: description} for COLUMN COMMENTs (optional)
        """
        if table_fqn:
            # --- Unity Catalog: write as managed table ---
            try:
                df.write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(table_fqn)

                record_count = df.count()
                print(f"  -> Written {record_count} records to {table_fqn}")

                # Apply entity-level description as table comment
                if entity_description:
                    escaped_desc = entity_description.replace("'", "\\'").replace("\n", " ")
                    self.spark.sql(f"COMMENT ON TABLE {table_fqn} IS '{escaped_desc}'")
                    print(f"  -> Table comment: {entity_description[:80]}")

                # Apply column-level descriptions from CDM .cdm.json
                if column_descriptions:
                    col_count = 0
                    for col_name, description in column_descriptions.items():
                        if col_name in df.columns and description:
                            escaped = description.replace("'", "\\'").replace("\n", " ")
                            self.spark.sql(
                                f"ALTER TABLE {table_fqn} ALTER COLUMN `{col_name}` COMMENT '{escaped}'"
                            )
                            col_count += 1
                    print(f"  -> Set {col_count} column descriptions from CDM")

                return df
            except Exception as e:
                print(f"  ⚠ Unity Catalog write failed ({e}), falling back to path-based write")

        # --- Fallback: path-based Delta write ---
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(silver_path)

        record_count = df.count()
        print(f"  -> Written {record_count} records to {silver_path}")
        return df
