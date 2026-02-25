# Databricks × Microsoft CDM — Banking Data Integration Demo

A demonstration project that integrates **Databricks** with the **Microsoft Common Data Model (CDM)** for the banking sector. It implements a **medallion architecture** (Bronze → Silver) where the silver layer conforms to CDM entity schemas from the official [Microsoft CDM Financial Services](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices) definitions.

## Architecture

```
┌─────────────────────────┐     ┌──────────────────────────────┐
│       BRONZE LAYER      │     │        SILVER LAYER          │
│   (Raw / Messy Data)    │ ──► │   (CDM Canonical Schema)     │
│                         │     │                              │
│ • Inconsistent dates    │     │ • CDM attribute names        │
│ • Mixed case / spaces   │     │ • Proper data types          │
│ • Duplicate records     │     │ • CDM option set codes       │
│ • Missing values        │     │ • Deduplicated & cleaned     │
│ • Non-standard booleans │     │ • Schema-enforced (PySpark)  │
└─────────────────────────┘     └──────────────────────────────┘
         Delta Lake                       Delta Lake
```

## CDM Entities

The demo implements 5 banking entities from the CDM Financial Services model:

| Entity | CDM Source Path | Description | PK |
|--------|----------------|-------------|-----|
| **Bank** | `FinancialServicesCommonDataModel/Bank` | Banking institution | `bankId` |
| **Branch** | `FinancialServicesCommonDataModel/Branch` | Bank branches | `branchId` |
| **Contact** | `FinancialServicesCommonDataModel/Contact` | Customers / contacts | `contactId` |
| **Account** | `FinancialServicesCommonDataModel/Account` | Customer accounts | `accountId` |
| **FinancialHolding** | `RetailBankingCoreDataModel/Financialholding` | Financial products | `financialHoldingId` |

### Entity Relationships

```
Bank (1) ──► (N) Branch (1) ──► (N) Contact (Customer)
                    │                    │
                    └──► (N) Account ◄───┘
                               │
                         (1) ──► (N) FinancialHolding
```

## Project Structure

```
dbx-cdm-integration/
├── README.md
└── notebooks/
    ├── 00_config.py                # Configuration — paths, entity names, record counts
    ├── 01_cdm_schemas.py           # PySpark StructType schemas from CDM definitions
    ├── 02_generate_bronze_data.py  # Synthetic bronze data with data quality issues
    ├── 03_bronze_to_silver.py      # Bronze → Silver transformation logic
    └── 04_run_pipeline.py          # Full pipeline orchestrator (run this!)
```

## Notebooks

### `00_config.py` — Configuration
Global settings for the demo: storage paths, entity names, record counts, and helper functions.

### `01_cdm_schemas.py` — CDM Schema Definitions
PySpark `StructType` schemas derived from the official CDM `.cdm.json` entity definitions. Includes:
- Field names matching CDM attribute names
- Proper data types (`StringType`, `IntegerType`, `TimestampType`, `DecimalType`, `BooleanType`, `DateType`)
- CDM option set lookup dictionaries (e.g., `statecode`: Active=0 / Inactive=1)

### `02_generate_bronze_data.py` — Bronze Data Generator
Creates synthetic banking data simulating messy source system exports:
- **Inconsistent date formats** — `YYYY-MM-DD`, `DD/MM/YYYY`, `YYYY/MM/DD HH:MM:SS`, etc.
- **Mixed case and whitespace** — `" BRUSSELS "`, `"brussels"`, `"Brussels"`
- **Duplicate records** — intentional copies with ~5% duplication rate
- **Missing values** — random nulls across non-key fields
- **Non-standard values** — `"yes"/"no"`, `"Y"/"N"`, `"true"/"false"` for booleans

Uses a Belgian banking context (KBC, BNP Paribas Fortis, Belfius, ING Belgium, Argenta).

### `03_bronze_to_silver.py` — Bronze to Silver Transformations
Cleans and conforms bronze data to CDM schemas:
- **Date parsing** — handles 8+ date formats → standard `TimestampType`
- **Deduplication** — window-based dedup keeping most recent records
- **Status code resolution** — maps text values to CDM option set integers
- **String normalization** — trim, proper case, country code standardization
- **Schema enforcement** — casts all columns to CDM-defined types

### `04_run_pipeline.py` — Pipeline Orchestrator
End-to-end pipeline runner. Chains all notebooks and produces:
- Bronze/Silver record count comparison
- CDM schema compliance validation
- Data quality summary report

**Run this notebook to execute the entire demo.**

## Getting Started

### Prerequisites
- Databricks workspace with Delta Lake support
- PySpark runtime (DBR 12.0+)

### Setup

1. **Clone or import** this repo into your Databricks workspace:
   ```
   Repos → Add Repo → paste the Git URL
   ```

2. **Open** `notebooks/04_run_pipeline.py`

3. **Attach** to a cluster and click **Run All**

The pipeline will:
1. Create configuration and load CDM schemas
2. Generate synthetic bronze data (Delta tables)
3. Transform bronze → silver with CDM conformance
4. Validate silver schemas against CDM definitions

### Storage Paths (defaults)

| Layer | Path |
|-------|------|
| Bronze | `/mnt/cdm_demo/bronze/{entity}` |
| Silver | `/mnt/cdm_demo/silver/{entity}` |

Edit `notebooks/00_config.py` to change paths or enable Unity Catalog.

## CDM Schema Mapping

Example field mapping for the **Bank** entity:

| Bronze (raw) | Silver (CDM) | Type | CDM Source |
|-------------|-------------|------|-----------|
| `bank_id` | `bankId` | `StringType` | `Bank.bankId` |
| `bank_name` | `name` | `StringType` | `Bank.name` |
| `bank_code` | `bankCode` | `StringType` | `Bank.bankCode` |
| `city` | `city` | `StringType` | `Bank.city` |
| `country` | `country` | `StringType` | ISO 3166-1 alpha-2 |
| `status` | `statecode` | `IntegerType` | 0=Active, 1=Inactive |
| `status_reason` | `statuscode` | `IntegerType` | 1=Active, 2=Inactive |

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

## References

- [Microsoft CDM GitHub Repository](https://github.com/microsoft/CDM)
- [CDM Financial Services Entities](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices)
- [CDM FinancialServicesCommonDataModel](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices/FinancialServicesCommonDataModel)
- [CDM RetailBankingCoreDataModel](https://github.com/microsoft/CDM/tree/master/schemaDocuments/FinancialServices/RetailBankingCoreDataModel)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## License

This is a demo/reference project. The CDM schema definitions are based on the [Microsoft CDM](https://github.com/microsoft/CDM) (MIT License).
