# Databricks × Microsoft CDM — Banking Data Integration (Production Architecture)

A **production-grade** reference implementation integrating **Databricks** with the **Microsoft Common Data Model (CDM)** for the banking sector. Implements a **medallion architecture** (Bronze → Silver) with:

- **Dynamic schema loading** from `.cdm.json` files (not hand-coded PySpark schemas)
- **CDM entity extensions** via `extendsEntity` (no CDM repo forking)
- **Custom entities** for org-specific needs (KYCCheck for AML/CFT compliance)
- **Config-driven transformations** via externalized YAML mappings
- **Rule-based data quality validation** via YAML quality rules

## Architecture

```
┌─────────────────────────┐     ┌────────────────────────────────┐
│       BRONZE LAYER      │     │         SILVER LAYER           │
│   (Raw / Messy Data)    │ ──► │   (CDM Canonical Schema)       │
│                         │     │                                │
│ • Inconsistent dates    │     │ • CDM attribute names          │
│ • Mixed case / spaces   │     │ • Proper data types            │
│ • Duplicate records     │     │ • CDM option set codes         │
│ • Missing values        │     │ • Deduplicated & cleaned       │
│ • Non-standard booleans │     │ • Schema-enforced (from JSON)  │
└─────────────────────────┘     └────────────────────────────────┘
         Delta Lake                        Delta Lake

    ┌─────────────────────────────────────────────────────────┐
    │              CONFIGURATION LAYER                        │
    │                                                         │
    │  cdm_schemas/*.cdm.json    → Schema source of truth     │
    │  config/entity_mappings.yaml → Column mapping rules     │
    │  config/quality_rules.yaml   → Data quality rules       │
    │  lib/cdm_schema_parser.py    → JSON → PySpark schema    │
    │  lib/transform_engine.py     → Config-driven transforms │
    │  lib/quality_engine.py       → Rule-based validation    │
    └─────────────────────────────────────────────────────────┘
```

## CDM Entities

6 banking entities — 5 standard CDM + 1 custom:

| Entity | Type | CDM Schema | Description | PK |
|--------|------|-----------|-------------|-----|
| **Bank** | **Extended** | `BankExtended.cdm.json` | Banking institution + custom fields | `bankId` |
| **Branch** | Standard | `Branch.cdm.json` | Bank branches | `branchId` |
| **Contact** | Standard | `Contact.cdm.json` | Customers / contacts | `contactId` |
| **Account** | Standard | `Account.cdm.json` | Customer accounts | `accountId` |
| **FinancialHolding** | Standard | `FinancialHolding.cdm.json` | Financial products | `financialHoldingId` |
| **KYCCheck** | **Custom** | `KYCCheck.cdm.json` | AML/KYC compliance checks | `kycCheckId` |

### Extension Pattern: BankExtended

`BankExtended` inherits ALL 18 fields from the standard CDM `Bank` entity via `extendsEntity`, then adds 5 org-specific fields:

| Extension Field | Type | Description |
|----------------|------|-------------|
| `riskRating` | integer | Internal risk score (1–5) |
| `swiftCode` | string | SWIFT/BIC code |
| `regulatoryLicenseNumber` | string | NBB regulatory license |
| `onboardingChannel` | string | Onboarding channel (Digital, Branch, Partnership) |
| `isPSD2Compliant` | boolean | PSD2 compliance flag |

> **Key principle:** You do NOT fork the CDM repo. Standard entities live in `cdm_schemas/standard/`. Your org-specific extensions live in `cdm_schemas/extensions/`. The parser resolves the inheritance chain automatically.

### Custom Entity: KYCCheck

Net-new entity not in the standard CDM, created for Belgian AML/CFT regulatory compliance. Includes check types (ID Verification, Address Proof, PEP Check, Sanctions Screening), results (Pass/Fail/Pending/Expired), risk scores, and audit fields.

### Entity Relationships

```
Bank/BankExtended (1) ──► (N) Branch (1) ──► (N) Contact (Customer)
                                │                    │
                                └──► (N) Account ◄───┘
                                           │
                                     (1) ──► (N) FinancialHolding
                                             │
                           Contact (1) ──► (N) KYCCheck [CUSTOM]
```

## Project Structure

```
dbx-cdm-integration/
├── README.md
├── cdm_schemas/                            # CDM entity definitions (.cdm.json)
│   ├── manifest.cdm.json                  # Master manifest — all entities + relationships
│   ├── standard/                          # Standard CDM entities (mirrors MS CDM repo)
│   │   ├── Bank.cdm.json
│   │   ├── Branch.cdm.json
│   │   ├── Contact.cdm.json
│   │   ├── Account.cdm.json
│   │   └── FinancialHolding.cdm.json
│   └── extensions/                        # Org-specific extensions
│       ├── BankExtended.cdm.json          # Extends Bank with custom fields
│       └── KYCCheck.cdm.json              # Net-new custom entity
├── config/                                # Externalized configuration
│   ├── entity_mappings.yaml               # Bronze → Silver column mappings & transforms
│   └── quality_rules.yaml                 # Data quality validation rules
├── lib/                                   # Shared Python library modules
│   ├── __init__.py
│   ├── cdm_schema_parser.py              # Parses .cdm.json → PySpark StructType
│   ├── transform_engine.py               # Config-driven transformation engine
│   └── quality_engine.py                 # Rule-based data quality engine
└── notebooks/                             # Databricks notebooks
    ├── 00_config.py                       # Configuration, paths, entity registry
    ├── 01_cdm_schemas.py                  # Dynamic schema loading from .cdm.json
    ├── 02_generate_bronze_data.py         # Synthetic bronze data generator
    ├── 03_bronze_to_silver.py             # Config-driven Bronze → Silver transforms
    └── 04_run_pipeline.py                 # Full pipeline orchestrator (run this!)
```

## Key Components

### `lib/cdm_schema_parser.py` — CDM Schema Parser
Reads `.cdm.json` files and produces PySpark `StructType` schemas at runtime:
- Resolves entity inheritance chains (`extendsEntity`)
- Maps CDM types → PySpark types (entityId→String, listLookup→Integer, etc.)
- Handles decimal precision/scale from CDM traits
- Extracts option sets (lookup values) from entity definitions
- Loads the manifest for entity discovery

### `lib/transform_engine.py` — Config-Driven Transform Engine
Reads `config/entity_mappings.yaml` and applies transformations:
- **17 registered transforms:** trim, parse_timestamp, resolve_statecode, normalize_country, normalize_boolean, resolve_holding_type, resolve_kyc_check_type, etc.
- **6 derived field generators:** statecode_to_display, holding_type_to_display, etc.
- Applies: dedup → column mapping → derived fields → schema enforcement

### `lib/quality_engine.py` — Quality Validation Engine
Reads `config/quality_rules.yaml` and validates silver data:
- **7 rule types:** not_null, unique, referential, range, length, pattern, completeness
- Produces formatted reports with pass/fail status and overall quality scores

### `config/entity_mappings.yaml` — Column Mappings
Externalizes ALL column transformations. Example:
```yaml
entities:
  bank:
    cdm_schema: "extensions/BankExtended.cdm.json"
    mappings:
      bankId:     { source: "src_bank_id" }
      name:       { source: "bank_name", transform: "trim" }
      country:    { source: "country_code", transform: "normalize_country" }
      # Extension fields
      riskRating: { source: "risk_rating", transform: "cast_integer" }
      swiftCode:  { source: "swift_code", transform: "upper_trim" }
```

> **To change a column name or add a transform?** Edit the YAML. No Python changes needed.

## Notebooks

### `00_config.py` — Configuration & Setup
Sets up the Python path, imports library modules, loads the CDM manifest, and builds the entity registry. All other notebooks depend on this via `%run ./00_config`.

### `01_cdm_schemas.py` — Dynamic CDM Schema Loader
Uses `CdmSchemaParser` to load all schemas from `.cdm.json` files at runtime. Demonstrates:
- Extension resolution (BankExtended → Bank inheritance)
- Option set extraction from entity definitions
- Building a schema registry for downstream notebooks

### `02_generate_bronze_data.py` — Bronze Data Generator
Creates synthetic banking data simulating messy source system exports:
- **Inconsistent formats** — dates, booleans, status codes
- **Extension data** — BankExtended fields (riskRating, swiftCode, etc.)
- **Custom entity data** — KYCCheck records for AML/KYC compliance
- Belgian banking context (KBC, BNP Paribas Fortis, Belfius, ING Belgium, Argenta)

### `03_bronze_to_silver.py` — Config-Driven Transformations
Applies the `TransformEngine` to all entities using YAML configuration:
- No per-entity transform functions — one generic engine for all
- CDM schema enforcement from `.cdm.json` parsed schemas
- Extension fields handled seamlessly alongside standard fields

### `04_run_pipeline.py` — Pipeline Orchestrator
End-to-end pipeline: Bronze → Silver → Validate. Produces:
- Bronze/Silver record count comparison (deduplication delta)
- CDM schema compliance validation (field names + data types)
- Rule-based quality report (from quality_rules.yaml)
- Field completeness report with visual progress bars

**Run this notebook to execute the entire demo.**

## Getting Started

### Prerequisites
- Databricks workspace with Delta Lake support
- PySpark runtime (DBR 12.0+)
- PyYAML (`pip install pyyaml`)

### Setup

1. **Clone or import** this repo into your Databricks workspace:
   ```
   Repos → Add Repo → paste the Git URL
   ```

2. **Open** `notebooks/04_run_pipeline.py`

3. **Attach** to a cluster and click **Run All**

The pipeline will:
1. Load configuration and discover entities from the CDM manifest
2. Parse `.cdm.json` schemas (resolving extensions) into PySpark StructTypes
3. Generate synthetic bronze data (including extension + custom entity data)
4. Apply config-driven transformations (Bronze → Silver)
5. Validate CDM schema compliance
6. Run data quality rules and produce a quality report

### Storage Paths (defaults)

| Layer | Path |
|-------|------|
| Bronze | `/mnt/cdm_demo/bronze/{entity}` |
| Silver | `/mnt/cdm_demo/silver/{entity}` |

Edit `notebooks/00_config.py` to change paths or enable Unity Catalog.

## How to Extend for Your Organization

| Task | What to do |
|------|-----------|
| Add a field to an existing CDM entity | Create a `.cdm.json` in `extensions/` with `extendsEntity` pointing to the standard entity |
| Add a net-new entity | Create a `.cdm.json` in `extensions/`, add to `manifest.cdm.json` |
| Change a source column name | Edit `config/entity_mappings.yaml` |
| Add a quality validation rule | Edit `config/quality_rules.yaml` |
| Add a new transform function | Register in `lib/transform_engine.py` `TRANSFORM_REGISTRY` |
| Upgrade CDM version | Update files in `cdm_schemas/standard/` — extensions remain unchanged |

## CDM Option Sets

### State Code (all entities)
| Value | Label |
|-------|-------|
| 0 | Active |
| 1 | Inactive |

### Financial Holding Type
| CDM Code | Label |
|----------|-------|
| 104800000 | Deposit Account |
| 104800001 | Long-term Saving |
| 104800002 | Term Deposit |
| 104800003 | Credit Line |
| 104800004 | Investment |
| 104800005 | Loan |
| 104800006 | Term Deposit |

### KYC Check Type (Custom)
| Value | Label |
|-------|-------|
| 0 | ID Verification |
| 1 | Address Proof |
| 2 | Income Verification |
| 3 | PEP Check |
| 4 | Sanctions Screening |

### KYC Check Result (Custom)
| Value | Label |
|-------|-------|
| 0 | Pass |
| 1 | Fail |
| 2 | Pending |
| 3 | Expired |

## References

- [Microsoft CDM GitHub Repository](https://github.com/microsoft/CDM)
- [CDM Financial Services Entities](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices)
- [CDM FinancialServicesCommonDataModel](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices/FinancialServicesCommonDataModel)
- [CDM RetailBankingCoreDataModel](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices/RetailBankingCoreDataModel)
- [CDM Entity Extensibility](https://learn.microsoft.com/en-us/common-data-model/sdk/convert-logical-entities)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## License

This is a demo/reference project. The CDM schema definitions are based on the [Microsoft CDM](https://github.com/microsoft/CDM) (MIT License).
