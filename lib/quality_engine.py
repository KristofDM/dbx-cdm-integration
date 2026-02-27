"""
Data Quality Validation Engine — Validates silver data against quality_rules.yaml.

Runs configurable quality checks on CDM-conformant silver data and produces
a structured quality report with pass/fail results per rule.

Rule types supported:
    - not_null: Column must not contain null values
    - unique: Column values must be unique
    - referential: FK values must exist in referenced entity
    - range: Numeric values must be within [min, max]
    - length: String length must be within [min, max]
    - pattern: String must match regex pattern
    - completeness: % of non-null values must be >= threshold
"""

import re
import yaml
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class QualityResult:
    """Result of a single quality check."""

    def __init__(self, rule_name: str, rule_type: str, column: str,
                 severity: str, passed: bool, details: str = "",
                 total_records: int = 0, failing_records: int = 0):
        self.rule_name = rule_name
        self.rule_type = rule_type
        self.column = column
        self.severity = severity
        self.passed = passed
        self.details = details
        self.total_records = total_records
        self.failing_records = failing_records

    @property
    def status_icon(self) -> str:
        if self.passed:
            return "✓"
        return "✗" if self.severity == "error" else "⚠"

    def __repr__(self):
        return f"{self.status_icon} [{self.severity.upper()}] {self.rule_name}: {self.details}"


class QualityEngine:
    """
    Config-driven data quality validation engine.

    Usage:
        engine = QualityEngine("config/quality_rules.yaml", spark)
        results = engine.validate_entity("bank", bank_df)
        engine.print_report(results)
    """

    def __init__(self, config_path: str, spark: SparkSession):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)
        self.spark = spark
        self.entities_config = self.config.get("entities", {})

    def validate_entity(
        self,
        entity_name: str,
        df: DataFrame,
        reference_dfs: Optional[Dict[str, DataFrame]] = None,
    ) -> List[QualityResult]:
        """
        Validate a silver DataFrame against its configured quality rules.

        Args:
            entity_name: Logical entity name (e.g., "bank")
            df: Silver DataFrame to validate
            reference_dfs: Dict of entity_name → DataFrame for referential checks

        Returns:
            List of QualityResult objects
        """
        if entity_name not in self.entities_config:
            return [QualityResult(
                "config_missing", "config", "", "warning", False,
                f"No quality rules configured for entity '{entity_name}'"
            )]

        rules = self.entities_config[entity_name].get("rules", [])
        results = []
        total = df.count()

        for rule in rules:
            result = self._evaluate_rule(rule, df, total, reference_dfs or {})
            results.append(result)

        return results

    def _evaluate_rule(
        self, rule: dict, df: DataFrame, total: int,
        reference_dfs: Dict[str, DataFrame]
    ) -> QualityResult:
        """Evaluate a single quality rule."""
        rule_name = rule["name"]
        rule_type = rule["type"]
        column = rule.get("column", "")
        severity = rule.get("severity", "warning")

        try:
            if rule_type == "not_null":
                return self._check_not_null(rule_name, column, severity, df, total)
            elif rule_type == "unique":
                return self._check_unique(rule_name, column, severity, df, total)
            elif rule_type == "referential":
                return self._check_referential(rule_name, column, severity, df, total, rule, reference_dfs)
            elif rule_type == "range":
                return self._check_range(rule_name, column, severity, df, total, rule)
            elif rule_type == "length":
                return self._check_length(rule_name, column, severity, df, total, rule)
            elif rule_type == "pattern":
                return self._check_pattern(rule_name, column, severity, df, total, rule)
            elif rule_type == "completeness":
                return self._check_completeness(rule_name, column, severity, df, total, rule)
            else:
                return QualityResult(rule_name, rule_type, column, severity, False,
                                     f"Unknown rule type: {rule_type}")
        except Exception as e:
            return QualityResult(rule_name, rule_type, column, severity, False,
                                 f"Error: {str(e)}")

    def _check_not_null(self, name, col, severity, df, total) -> QualityResult:
        null_count = df.filter(F.col(col).isNull()).count()
        passed = null_count == 0
        return QualityResult(name, "not_null", col, severity, passed,
                             f"{null_count}/{total} null values",
                             total, null_count)

    def _check_unique(self, name, col, severity, df, total) -> QualityResult:
        non_null_df = df.filter(F.col(col).isNotNull())
        distinct_count = non_null_df.select(col).distinct().count()
        actual_count = non_null_df.count()
        duplicates = actual_count - distinct_count
        passed = duplicates == 0
        return QualityResult(name, "unique", col, severity, passed,
                             f"{duplicates} duplicate values out of {actual_count}",
                             total, duplicates)

    def _check_referential(self, name, col, severity, df, total, rule, ref_dfs) -> QualityResult:
        ref_config = rule.get("references", {})
        ref_entity = ref_config.get("entity")
        ref_column = ref_config.get("column")

        if ref_entity not in ref_dfs:
            return QualityResult(name, "referential", col, severity, True,
                                 f"Skipped (reference entity '{ref_entity}' not loaded)")

        ref_df = ref_dfs[ref_entity]
        fk_values = df.filter(F.col(col).isNotNull()).select(col).distinct()
        pk_values = ref_df.select(F.col(ref_column).alias(col)).distinct()
        orphaned = fk_values.subtract(pk_values).count()
        passed = orphaned == 0
        return QualityResult(name, "referential", col, severity, passed,
                             f"{orphaned} orphaned references to {ref_entity}.{ref_column}",
                             total, orphaned)

    def _check_range(self, name, col, severity, df, total, rule) -> QualityResult:
        min_val = rule.get("min")
        max_val = rule.get("max")
        non_null = df.filter(F.col(col).isNotNull())
        out_of_range = non_null.filter(
            (F.col(col) < min_val) | (F.col(col) > max_val)
        ).count()
        passed = out_of_range == 0
        return QualityResult(name, "range", col, severity, passed,
                             f"{out_of_range} values outside [{min_val}, {max_val}]",
                             total, out_of_range)

    def _check_length(self, name, col, severity, df, total, rule) -> QualityResult:
        min_len = rule.get("min", 0)
        max_len = rule.get("max", 999999)
        non_null = df.filter(F.col(col).isNotNull())
        invalid = non_null.filter(
            (F.length(F.col(col)) < min_len) | (F.length(F.col(col)) > max_len)
        ).count()
        passed = invalid == 0
        return QualityResult(name, "length", col, severity, passed,
                             f"{invalid} values with length outside [{min_len}, {max_len}]",
                             total, invalid)

    def _check_pattern(self, name, col, severity, df, total, rule) -> QualityResult:
        pattern = rule.get("pattern", ".*")
        non_null = df.filter(F.col(col).isNotNull())
        non_matching = non_null.filter(~F.col(col).rlike(pattern)).count()
        checked = non_null.count()
        passed = non_matching == 0
        return QualityResult(name, "pattern", col, severity, passed,
                             f"{non_matching}/{checked} values don't match pattern '{pattern}'",
                             total, non_matching)

    def _check_completeness(self, name, col, severity, df, total, rule) -> QualityResult:
        threshold = rule.get("threshold", 100.0)
        null_count = df.filter(F.col(col).isNull()).count()
        completeness = ((total - null_count) / total * 100) if total > 0 else 0.0
        passed = completeness >= threshold
        return QualityResult(name, "completeness", col, severity, passed,
                             f"{completeness:.1f}% complete (threshold: {threshold}%)",
                             total, null_count)

    # =========================================================================
    # Reporting
    # =========================================================================

    def print_entity_report(self, entity_name: str, results: List[QualityResult]):
        """Print a formatted quality report for an entity."""
        errors = [r for r in results if not r.passed and r.severity == "error"]
        warnings = [r for r in results if not r.passed and r.severity == "warning"]
        passed = [r for r in results if r.passed]

        print(f"\n  {'='*50}")
        print(f"  Quality Report: {entity_name.upper()}")
        print(f"  {'='*50}")
        print(f"  Total rules: {len(results)}  |  "
              f"Passed: {len(passed)}  |  "
              f"Errors: {len(errors)}  |  "
              f"Warnings: {len(warnings)}")

        for r in results:
            print(f"    {r.status_icon} [{r.severity:7s}] {r.rule_name:35s} {r.details}")

    def print_full_report(self, all_results: Dict[str, List[QualityResult]]):
        """Print a full quality report across all entities."""
        print("=" * 60)
        print("DATA QUALITY REPORT — Silver Layer")
        print("=" * 60)

        total_rules = 0
        total_passed = 0
        total_errors = 0
        total_warnings = 0

        for entity_name, results in all_results.items():
            self.print_entity_report(entity_name, results)
            total_rules += len(results)
            total_passed += sum(1 for r in results if r.passed)
            total_errors += sum(1 for r in results if not r.passed and r.severity == "error")
            total_warnings += sum(1 for r in results if not r.passed and r.severity == "warning")

        print(f"\n{'='*60}")
        print(f"SUMMARY: {total_rules} rules | "
              f"{total_passed} passed | "
              f"{total_errors} errors | "
              f"{total_warnings} warnings")
        score = (total_passed / total_rules * 100) if total_rules > 0 else 0
        print(f"Overall Quality Score: {score:.1f}%")
        print(f"{'='*60}")
