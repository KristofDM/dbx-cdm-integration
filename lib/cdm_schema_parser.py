"""
CDM Schema Parser — Parses .cdm.json entity definitions into PySpark StructTypes.

This module is the bridge between the Microsoft CDM metadata format and PySpark.
It reads .cdm.json files (standard or extended), resolves entity inheritance,
and produces PySpark StructType schemas dynamically.

Production usage:
    - NO hand-coded PySpark schemas
    - Schemas are derived from CDM .cdm.json files at runtime
    - Extensions are resolved by following the extendsEntity chain
    - Manifests enumerate all entities used in the deployment

CDM SDK alternative:
    In a full production environment, you could use the official Microsoft CDM
    Python SDK (https://github.com/microsoft/CDM/tree/master/objectModel/Python)
    to resolve schemas from remote repositories. This module provides a lightweight
    local parser for environments where the full SDK is not needed.
"""

import json
import os
from typing import Dict, List, Optional, Tuple

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


# =============================================================================
# CDM Data Type → PySpark Type Mapping
# =============================================================================
# This mapping follows the official CDM type system:
# https://learn.microsoft.com/en-us/common-data-model/sdk/logical-definitions

CDM_TYPE_MAP = {
    "entityId": StringType(),
    "guid": StringType(),
    "string": StringType(),
    "name": StringType(),
    "integer": IntegerType(),
    "bigInteger": LongType(),
    "double": DoubleType(),
    "decimal": DecimalType(18, 2),
    "currency": DecimalType(18, 2),
    "boolean": BooleanType(),
    "dateTime": TimestampType(),
    "date": DateType(),
    "listLookup": IntegerType(),
    "listLookupWellKnown": IntegerType(),
}


class CdmSchemaParser:
    """
    Parses CDM .cdm.json entity definitions and converts them to PySpark StructTypes.

    Architecture:
        cdm_schemas/
        ├── manifest.cdm.json           ← lists all entities
        ├── standard/                   ← standard CDM entities
        │   ├── Bank.cdm.json
        │   └── ...
        └── extensions/                 ← org-specific extensions
            ├── BankExtended.cdm.json   ← extends standard/Bank.cdm.json
            └── KYCCheck.cdm.json       ← net-new custom entity

    Usage:
        parser = CdmSchemaParser("/path/to/cdm_schemas")
        manifest = parser.load_manifest()
        schema = parser.to_pyspark_schema("extensions/BankExtended.cdm.json", "BankExtended")
    """

    def __init__(self, schemas_root: str):
        """
        Initialize the parser with the root directory of CDM schema files.

        Args:
            schemas_root: Absolute path to the cdm_schemas/ directory.
        """
        self.schemas_root = schemas_root
        self._entity_cache: Dict[str, dict] = {}
        self._schema_cache: Dict[str, StructType] = {}

    # -------------------------------------------------------------------------
    # Manifest Loading
    # -------------------------------------------------------------------------

    def load_manifest(self, manifest_file: str = "manifest.cdm.json") -> dict:
        """
        Load the CDM manifest file listing all entities.

        Returns:
            Parsed manifest JSON as a dict.
        """
        path = os.path.join(self.schemas_root, manifest_file)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_manifest_entities(self, manifest_file: str = "manifest.cdm.json") -> List[dict]:
        """
        Get the list of entity entries from the manifest.

        Returns:
            List of dicts with keys: entityName, entityPath, explanation (optional).
        """
        manifest = self.load_manifest(manifest_file)
        return manifest.get("entities", [])

    def get_manifest_relationships(self, manifest_file: str = "manifest.cdm.json") -> List[dict]:
        """Get the relationship definitions from the manifest."""
        manifest = self.load_manifest(manifest_file)
        return manifest.get("relationships", [])

    # -------------------------------------------------------------------------
    # Entity Loading & Resolution
    # -------------------------------------------------------------------------

    def _read_json(self, schema_path: str) -> dict:
        """Read and parse a .cdm.json file."""
        full_path = os.path.join(self.schemas_root, schema_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"CDM schema file not found: {full_path}")
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_entity(self, schema_path: str, entity_name: Optional[str] = None) -> dict:
        """
        Load a single entity definition from a .cdm.json file.

        Args:
            schema_path: Path relative to schemas_root (e.g., "standard/Bank.cdm.json")
            entity_name: Specific entity name. If None, returns the first entity found.

        Returns:
            Entity definition dict with keys: entityName, hasAttributes, etc.
        """
        cache_key = f"{schema_path}:{entity_name or '*'}"
        if cache_key in self._entity_cache:
            return self._entity_cache[cache_key]

        doc = self._read_json(schema_path)
        definitions = doc.get("definitions", [])

        for defn in definitions:
            if "entityName" in defn:
                if entity_name is None or defn["entityName"] == entity_name:
                    self._entity_cache[cache_key] = defn
                    return defn

        available = [d["entityName"] for d in definitions if "entityName" in d]
        raise ValueError(
            f"Entity '{entity_name}' not found in {schema_path}. "
            f"Available: {available}"
        )

    def resolve_entity(self, schema_path: str, entity_name: Optional[str] = None) -> dict:
        """
        Resolve an entity including ALL inherited attributes from extendsEntity.

        This walks the inheritance chain (BankExtended → Bank → ...) and merges
        all attributes, with child attributes appended after parent attributes.

        Args:
            schema_path: Path to the .cdm.json file.
            entity_name: Entity name within the file.

        Returns:
            Resolved entity dict with merged hasAttributes list.
        """
        entity = self.load_entity(schema_path, entity_name)

        all_attributes = []
        option_sets = {}

        # Resolve parent entity if extendsEntity is specified
        if "extendsEntity" in entity:
            parent_ref = entity["extendsEntity"]
            # Parse "standard/Bank.cdm.json/Bank" format
            parent_path, parent_name = self._parse_entity_reference(parent_ref)
            parent = self.resolve_entity(parent_path, parent_name)
            all_attributes.extend(parent.get("hasAttributes", []))
            option_sets.update(parent.get("_optionSets", {}))

        # Add this entity's own attributes
        for attr in entity.get("hasAttributes", []):
            # Collect option sets for lookup fields
            if "optionSet" in attr:
                option_sets[attr["name"]] = attr["optionSet"]["values"]
            all_attributes.append(attr)

        return {
            "entityName": entity["entityName"],
            "description": entity.get("description", ""),
            "hasAttributes": all_attributes,
            "exhibitsTraits": entity.get("exhibitsTraits", []),
            "_optionSets": option_sets,
            "_isExtension": "extendsEntity" in entity,
            "_extendsEntity": entity.get("extendsEntity"),
        }

    def _parse_entity_reference(self, ref: str) -> Tuple[str, Optional[str]]:
        """
        Parse a CDM entity reference string.

        Formats:
            "standard/Bank.cdm.json/Bank"  → ("standard/Bank.cdm.json", "Bank")
            "standard/Bank.cdm.json"       → ("standard/Bank.cdm.json", None)
        """
        parts = ref.rsplit("/", 1)
        if len(parts) == 2 and not parts[1].endswith(".json"):
            return parts[0], parts[1]
        return ref, None

    # -------------------------------------------------------------------------
    # PySpark Schema Generation
    # -------------------------------------------------------------------------

    def to_pyspark_schema(
        self, schema_path: str, entity_name: Optional[str] = None
    ) -> StructType:
        """
        Convert a CDM entity definition to a PySpark StructType.

        Resolves inheritance, maps CDM types to PySpark types, and handles
        decimal precision/scale from traits.

        Args:
            schema_path: Path to the .cdm.json file.
            entity_name: Entity name (optional).

        Returns:
            PySpark StructType with all fields.
        """
        cache_key = f"schema:{schema_path}:{entity_name or '*'}"
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        resolved = self.resolve_entity(schema_path, entity_name)
        fields = []
        seen_names = set()

        for attr in resolved["hasAttributes"]:
            name = attr["name"]
            if name in seen_names:
                continue  # Skip duplicates from inheritance
            seen_names.add(name)

            cdm_type = attr.get("dataType", "string")
            is_nullable = attr.get("isNullable", True)

            # Handle decimal precision from traits
            spark_type = self._resolve_spark_type(cdm_type, attr)
            fields.append(StructField(name, spark_type, nullable=is_nullable))

        schema = StructType(fields)
        self._schema_cache[cache_key] = schema
        return schema

    def _resolve_spark_type(self, cdm_type: str, attr: dict):
        """Resolve a CDM data type to its PySpark equivalent, considering traits."""
        if cdm_type == "decimal" and "traits" in attr:
            precision, scale = 18, 2
            for trait in attr.get("traits", []):
                if isinstance(trait, dict) and trait.get("traitReference") == "is.dataFormat.numeric.shaped":
                    for arg in trait.get("arguments", []):
                        if arg.get("name") == "precision":
                            precision = int(arg["value"])
                        elif arg.get("name") == "scale":
                            scale = int(arg["value"])
            return DecimalType(precision, scale)

        return CDM_TYPE_MAP.get(cdm_type, StringType())

    # -------------------------------------------------------------------------
    # Option Set Utilities
    # -------------------------------------------------------------------------

    def get_option_sets(self, schema_path: str, entity_name: Optional[str] = None) -> Dict[str, List[dict]]:
        """
        Get all option set (lookup) values for an entity.

        Returns:
            Dict mapping field name → list of {value, displayText} dicts.
        """
        resolved = self.resolve_entity(schema_path, entity_name)
        return resolved.get("_optionSets", {})

    # -------------------------------------------------------------------------
    # Schema Summary / Inspection
    # -------------------------------------------------------------------------

    def print_entity_summary(self, schema_path: str, entity_name: Optional[str] = None):
        """Print a human-readable summary of a resolved entity schema."""
        resolved = self.resolve_entity(schema_path, entity_name)
        schema = self.to_pyspark_schema(schema_path, entity_name)

        ext_tag = " [EXTENSION]" if resolved["_isExtension"] else ""
        print(f"\n{'='*60}")
        print(f"Entity: {resolved['entityName']}{ext_tag}")
        if resolved.get("_extendsEntity"):
            print(f"Extends: {resolved['_extendsEntity']}")
        print(f"Description: {resolved.get('description', 'N/A')}")
        print(f"Fields: {len(schema.fields)}")
        print(f"{'='*60}")

        for field in schema.fields:
            nullable_flag = "" if field.nullable else " [REQUIRED]"
            print(f"  {field.name:40s} {str(field.dataType):20s}{nullable_flag}")

        # Print option sets
        option_sets = resolved.get("_optionSets", {})
        if option_sets:
            print(f"\n  Option Sets:")
            for field_name, values in option_sets.items():
                print(f"    {field_name}:")
                for v in values:
                    print(f"      {v['value']:>10} = {v['displayText']}")

    def get_column_descriptions(
        self, schema_path: str, entity_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Extract column descriptions from a resolved entity.

        Returns:
            Dict mapping column name → description string.
        """
        resolved = self.resolve_entity(schema_path, entity_name)
        descriptions = {}
        for attr in resolved["hasAttributes"]:
            name = attr["name"]
            desc = attr.get("description", "")
            if desc:
                descriptions[name] = desc
        return descriptions

    def get_entity_description(
        self, schema_path: str, entity_name: Optional[str] = None
    ) -> str:
        """
        Get the entity-level description from a resolved entity.

        Returns:
            Entity description string.
        """
        resolved = self.resolve_entity(schema_path, entity_name)
        return resolved.get("description", "")

    def load_all_from_manifest(self, manifest_file: str = "manifest.cdm.json") -> Dict[str, StructType]:
        """
        Load all entity schemas referenced in the manifest.

        Returns:
            Dict mapping logical entity name → PySpark StructType.
        """
        entities = self.get_manifest_entities(manifest_file)
        schemas = {}
        for entry in entities:
            entity_name = entry["entityName"]
            entity_path = entry["entityPath"]
            # Parse "extensions/BankExtended.cdm.json/BankExtended"
            path, name = self._parse_entity_reference(entity_path)
            schemas[entity_name] = self.to_pyspark_schema(path, name)
        return schemas

    def load_all_descriptions_from_manifest(
        self, manifest_file: str = "manifest.cdm.json"
    ) -> Dict[str, dict]:
        """
        Load entity and column descriptions for all entities in the manifest.

        Returns:
            Dict mapping CDM entity name → {
                "entity_description": str,
                "column_descriptions": {col_name: description}
            }
        """
        entities = self.get_manifest_entities(manifest_file)
        all_descriptions = {}
        for entry in entities:
            entity_name = entry["entityName"]
            entity_path = entry["entityPath"]
            path, name = self._parse_entity_reference(entity_path)
            all_descriptions[entity_name] = {
                "entity_description": self.get_entity_description(path, name),
                "column_descriptions": self.get_column_descriptions(path, name),
            }
        return all_descriptions
