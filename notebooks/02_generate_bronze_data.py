# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Bronze (Raw) Data
# MAGIC
# MAGIC This notebook generates **synthetic bronze-layer data** that simulates
# MAGIC raw banking data as it would arrive from source systems.
# MAGIC
# MAGIC The bronze data intentionally includes common data quality issues:
# MAGIC - Inconsistent date formats
# MAGIC - Mixed case in string fields
# MAGIC - Null/missing values
# MAGIC - Denormalized or flattened structures
# MAGIC - Duplicate records
# MAGIC - Extra whitespace and formatting inconsistencies
# MAGIC
# MAGIC This data will be transformed to CDM-conformant silver data in notebook `03`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import uuid
import random
from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()
random.seed(RANDOM_SEED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Data Generation

# COMMAND ----------

def random_uuid():
    """Generate a random UUID string."""
    return str(uuid.uuid4())


def random_date(start_year=2015, end_year=2024):
    """Generate a random date string in various formats (simulating messy data)."""
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    dt = datetime(year, month, day, random.randint(0, 23), random.randint(0, 59))

    # Intentionally use inconsistent date formats (bronze data is messy!)
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%dT%H:%M:%S.000Z",
    ]
    return dt.strftime(random.choice(formats))


def random_date_consistent(start_year=2015, end_year=2024):
    """Generate a random datetime object (for cleaner date fields)."""
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return datetime(year, month, day, random.randint(0, 23), random.randint(0, 59))


def maybe_null(value, null_probability=0.1):
    """Return None with given probability, otherwise return the value."""
    if random.random() < null_probability:
        return None
    return value


def random_whitespace(value):
    """Add random leading/trailing whitespace to simulate data issues."""
    if value is None:
        return None
    if random.random() < 0.2:
        return f"  {value}  "
    if random.random() < 0.1:
        return f" {value}"
    return value


def random_case(value):
    """Randomly change case to simulate inconsistent data entry."""
    if value is None:
        return None
    r = random.random()
    if r < 0.15:
        return value.upper()
    elif r < 0.25:
        return value.lower()
    return value

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data (used for data generation)

# COMMAND ----------

# Belgian bank names (realistic for demo)
BANK_NAMES = [
    "KBC Group",
    "BNP Paribas Fortis",
    "Belfius Bank",
    "ING Belgium",
    "Argenta",
]

# Belgian cities for branches
BELGIAN_CITIES = [
    "Brussels", "Antwerp", "Ghent", "Bruges", "Leuven",
    "Liège", "Namur", "Mechelen", "Hasselt", "Kortrijk",
    "Charleroi", "Mons", "Tournai", "Aalst", "Dendermonde",
    "Genk", "Sint-Niklaas", "Roeselare", "Ostend", "Wavre",
]

STREET_NAMES = [
    "Rue de la Loi", "Meir", "Korenmarkt", "Grote Markt", "Bondgenotenlaan",
    "Place Saint-Lambert", "Rue de Fer", "IJzerenleen", "Hasseltweg", "Korte Steenstraat",
    "Boulevard Tirou", "Grand Place", "Quai Notre-Dame", "Molenstraat", "Kaasteelstraat",
    "Europalaan", "Stationsstraat", "Antwerpsesteenweg", "Langestraat", "Avenue de Tervueren",
]

FIRST_NAMES = [
    "Jan", "Pieter", "Marc", "Luc", "Thomas", "David", "Kevin", "Bart",
    "Sophie", "Marie", "Charlotte", "Emma", "Laura", "Julie", "Sarah", "Nathalie",
    "Ahmed", "Mohammed", "Fatima", "Youssef", "Ibrahim", "Amina", "Hassan", "Leila",
]

LAST_NAMES = [
    "Peeters", "Janssens", "Maes", "Jacobs", "Willems", "Claes", "Goossens", "Wouters",
    "De Smedt", "Mertens", "Dubois", "Lambert", "Martin", "Simon", "Laurent", "Renard",
    "Van den Berg", "De Backer", "Hermans", "Vermeersch", "Bogaert", "De Cock", "Coppens", "Stevens",
]

ACCOUNT_TYPES_BRONZE = [
    "checking", "Checking", "CHECKING", "savings", "Savings",
    "SAVINGS", "business", "Business", "joint", "Joint", "student",
]

HOLDING_TYPES_BRONZE = [
    "deposit", "Deposit Account", "DEPOSIT", "savings", "Savings Account",
    "current", "Current Account", "CURRENT", "loan", "Loan", "LOAN",
    "credit_line", "Line of Credit", "investment", "Investment",
    "term_deposit", "Term Deposit",
]

CURRENCY_CODES = ["EUR", "eur", "Eur", "USD", "GBP"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bronze Bank Data

# COMMAND ----------

def generate_bronze_banks(count: int) -> list:
    """Generate messy bronze bank records."""
    records = []
    for i in range(count):
        bank_id = random_uuid()
        name = BANK_NAMES[i % len(BANK_NAMES)]

        record = {
            "src_bank_id": bank_id,
            "bank_name": random_whitespace(random_case(name)),
            "bank_code": maybe_null(f"BE-{random.randint(100, 999)}"),
            "address_1": random_whitespace(f"{random.randint(1, 200)} {random.choice(STREET_NAMES)}"),
            "address_2": maybe_null(f"Floor {random.randint(1, 10)}", 0.6),
            "city": random_case(random.choice(BELGIAN_CITIES[:5])),
            "province": maybe_null(random_case(random.choice(["Flanders", "Wallonia", "Brussels-Capital"])), 0.3),
            "zip_code": str(random.randint(1000, 9999)),
            "country_code": random.choice(["BE", "Belgium", "be", "BEL", "belgium"]),
            "phone": maybe_null(f"+32-{random.randint(2,9)}-{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(10,99)}", 0.15),
            "status": random.choice(["active", "Active", "ACTIVE", "1", "inactive", "0"]),
            "created_date": random_date(2010, 2020),
            "modified_date": random_date(2020, 2024),
            "integration_key": f"BANK-{i+1:03d}",
        }
        records.append(record)

    # Add a deliberate duplicate (common bronze data issue)
    if records:
        duplicate = records[0].copy()
        duplicate["modified_date"] = random_date(2023, 2024)
        records.append(duplicate)

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bronze Branch Data

# COMMAND ----------

def generate_bronze_branches(count: int, bank_ids: list) -> list:
    """Generate messy bronze branch records."""
    records = []
    for i in range(count):
        branch_id = random_uuid()
        city = random.choice(BELGIAN_CITIES)

        record = {
            "branch_id": branch_id,
            "fk_bank_id": random.choice(bank_ids),
            "branch_name": random_whitespace(f"{random_case(city)} Branch {random.choice(['Main', 'Central', 'South', 'North', 'East', 'West'])}"),
            "branch_code": f"BR-{random.randint(1000, 9999)}",
            "street_address": f"{random.randint(1, 300)} {random.choice(STREET_NAMES)}",
            "address_line_2": maybe_null(f"Suite {random.randint(1, 50)}", 0.7),
            "city_name": random_case(city),
            "state_province": maybe_null(random_case(random.choice(["Flanders", "Wallonia", "Brussels-Capital"])), 0.2),
            "postal_code": str(random.randint(1000, 9999)),
            "country": random.choice(["BE", "Belgium", "be", "BEL"]),
            "phone_number": maybe_null(f"+32 {random.randint(2,9)} {random.randint(100,999)} {random.randint(10,99)} {random.randint(10,99)}", 0.2),
            "is_active": random.choice(["true", "True", "1", "yes", "false", "False", "0", "no"]),
            "created_at": random_date(2012, 2020),
            "updated_at": random_date(2020, 2024),
            "integration_key": f"BRANCH-{i+1:04d}",
        }
        records.append(record)

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bronze Contact (Customer) Data

# COMMAND ----------

def generate_bronze_contacts(count: int, branch_ids: list) -> list:
    """Generate messy bronze contact/customer records."""
    records = []
    for i in range(count):
        contact_id = random_uuid()
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        # Inconsistent email formats
        email_formats = [
            f"{first.lower()}.{last.lower().replace(' ', '')}@example.com",
            f"{first[0].lower()}{last.lower().replace(' ', '')}@mail.be",
            f"{first.lower()}_{last.lower().replace(' ', '')}@bank.be",
            None,  # Some contacts have no email
        ]

        # Inconsistent date of birth formats
        dob_year = random.randint(1950, 2005)
        dob_month = random.randint(1, 12)
        dob_day = random.randint(1, 28)
        dob_formats = [
            f"{dob_year}-{dob_month:02d}-{dob_day:02d}",
            f"{dob_day:02d}/{dob_month:02d}/{dob_year}",
            f"{dob_month}/{dob_day}/{dob_year}",
            None,
        ]

        record = {
            "customer_id": contact_id,
            "first_name": random_whitespace(random_case(first)),
            "last_name": random_whitespace(random_case(last)),
            "full_name": maybe_null(random_whitespace(f"{first} {last}"), 0.3),
            "email": random.choice(email_formats),
            "phone": maybe_null(f"+32-{random.randint(400,499)}-{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}", 0.25),
            "date_of_birth": random.choice(dob_formats),
            "join_date": random_date(2010, 2023),
            "tenure": maybe_null(str(round(random.uniform(0.5, 30.0), 1)), 0.15),
            "primary_branch_id": maybe_null(random.choice(branch_ids), 0.1),
            "managed_by_system": random.choice(["yes", "no", "true", "false", "1", "0", "Y", "N", None]),
            "is_active": random.choice(["active", "Active", "ACTIVE", "inactive", "closed", "1", "0"]),
            "created_timestamp": random_date(2010, 2020),
            "last_modified": random_date(2020, 2024),
            "src_integration_key": f"CUST-{i+1:05d}",
        }
        records.append(record)

    # Add deliberate duplicates (5% duplicate rate)
    num_duplicates = max(1, count // 20)
    for _ in range(num_duplicates):
        dup = random.choice(records).copy()
        dup["last_modified"] = random_date(2023, 2024)
        records.append(dup)

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bronze Account Data

# COMMAND ----------

def generate_bronze_accounts(count: int, branch_ids: list, contact_ids: list) -> list:
    """Generate messy bronze account records."""
    records = []
    for i in range(count):
        account_id = random_uuid()
        account_num = f"BE{random.randint(10, 99)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(10, 99)}"

        record = {
            "acct_id": account_id,
            "account_number": account_num,
            "acct_name": random_whitespace(f"Account {random_case(random.choice(ACCOUNT_TYPES_BRONZE))} - {random.choice(LAST_NAMES)}"),
            "account_type": random.choice(ACCOUNT_TYPES_BRONZE),
            "branch_ref": maybe_null(random.choice(branch_ids), 0.08),
            "primary_contact_ref": maybe_null(random.choice(contact_ids), 0.05),
            "join_date": random_date(2010, 2023),
            "tenure_years": maybe_null(str(round(random.uniform(0, 25), 2)), 0.2),
            "status_flag": random.choice(["open", "Open", "OPEN", "closed", "Closed", "CLOSED", "dormant", "1", "0"]),
            "created": random_date(2010, 2020),
            "modified": random_date(2020, 2024),
            "int_key": f"ACCT-{i+1:05d}",
        }
        records.append(record)

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Bronze FinancialHolding Data

# COMMAND ----------

def generate_bronze_financial_holdings(count: int, contact_ids: list, account_ids: list) -> list:
    """Generate messy bronze financial holding records."""
    records = []
    for i in range(count):
        holding_id = random_uuid()
        holding_type = random.choice(HOLDING_TYPES_BRONZE)

        # Generate realistic balance ranges based on holding type
        if holding_type.lower() in ("loan", "credit_line", "line of credit"):
            balance = round(random.uniform(-500000, -1000), 2)
        elif holding_type.lower() in ("term_deposit", "term deposit", "investment"):
            balance = round(random.uniform(5000, 2000000), 2)
        else:
            balance = round(random.uniform(100, 500000), 2)

        # Interest rates
        if holding_type.lower() in ("loan", "credit_line", "line of credit"):
            rate = round(random.uniform(1.5, 8.5), 4)
        elif holding_type.lower() in ("savings", "savings account", "term_deposit", "term deposit"):
            rate = round(random.uniform(0.1, 3.5), 4)
        else:
            rate = maybe_null(round(random.uniform(0.0, 1.0), 4), 0.5)

        opened_date = random_date_consistent(2010, 2023)
        maturity_offset = random.randint(365, 365 * 30)

        record = {
            "holding_id": holding_id,
            "customer_ref": maybe_null(random.choice(contact_ids), 0.03),
            "account_ref": maybe_null(random.choice(account_ids), 0.05),
            "holding_name": random_whitespace(f"{holding_type} - {random.choice(LAST_NAMES)}"),
            "type": holding_type,
            "balance_amount": str(balance),
            "balance_display": maybe_null(f"€{abs(balance):,.2f}" if random.random() > 0.3 else f"{abs(balance)} EUR", 0.1),
            "currency": random.choice(CURRENCY_CODES),
            "interest_rate": maybe_null(str(rate) if rate is not None else None, 0.15),
            "opened_on": random_date(2010, 2023),
            "maturity_date": maybe_null(
                (opened_date + timedelta(days=maturity_offset)).strftime(
                    random.choice(["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"])
                ),
                0.4,
            ),
            "status": random.choice(["active", "Active", "ACTIVE", "matured", "closed", "1", "0"]),
            "created_timestamp": random_date(2010, 2020),
            "updated_timestamp": random_date(2020, 2024),
            "int_key": f"FH-{i+1:06d}",
        }
        records.append(record)

    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Data to Delta Tables

# COMMAND ----------

def write_bronze_data():
    """Generate and write all bronze data to Delta tables."""

    print("=" * 60)
    print("Generating Bronze Data")
    print("=" * 60)

    # --- 1. Banks ---
    print("\n1. Generating Bank data...")
    bank_records = generate_bronze_banks(BRONZE_RECORD_COUNTS["bank"])
    bank_df = spark.createDataFrame([Row(**r) for r in bank_records])
    bank_df.write.format("delta").mode("overwrite").save(get_bronze_path("bank"))
    print(f"   Written {bank_df.count()} records to {get_bronze_path('bank')}")

    # Collect bank IDs for foreign key references
    bank_ids = [r["src_bank_id"] for r in bank_records]

    # --- 2. Branches ---
    print("\n2. Generating Branch data...")
    branch_records = generate_bronze_branches(BRONZE_RECORD_COUNTS["branch"], bank_ids)
    branch_df = spark.createDataFrame([Row(**r) for r in branch_records])
    branch_df.write.format("delta").mode("overwrite").save(get_bronze_path("branch"))
    print(f"   Written {branch_df.count()} records to {get_bronze_path('branch')}")

    # Collect branch IDs for FK references
    branch_ids = [r["branch_id"] for r in branch_records]

    # --- 3. Contacts (Customers) ---
    print("\n3. Generating Contact data...")
    contact_records = generate_bronze_contacts(BRONZE_RECORD_COUNTS["contact"], branch_ids)
    contact_df = spark.createDataFrame([Row(**r) for r in contact_records])
    contact_df.write.format("delta").mode("overwrite").save(get_bronze_path("contact"))
    print(f"   Written {contact_df.count()} records to {get_bronze_path('contact')}")

    # Collect contact IDs for FK references
    contact_ids = [r["customer_id"] for r in contact_records]

    # --- 4. Accounts ---
    print("\n4. Generating Account data...")
    account_records = generate_bronze_accounts(
        BRONZE_RECORD_COUNTS["account"], branch_ids, contact_ids
    )
    account_df = spark.createDataFrame([Row(**r) for r in account_records])
    account_df.write.format("delta").mode("overwrite").save(get_bronze_path("account"))
    print(f"   Written {account_df.count()} records to {get_bronze_path('account')}")

    # Collect account IDs for FK references
    account_ids = [r["acct_id"] for r in account_records]

    # --- 5. Financial Holdings ---
    print("\n5. Generating FinancialHolding data...")
    holding_records = generate_bronze_financial_holdings(
        BRONZE_RECORD_COUNTS["financial_holding"], contact_ids, account_ids
    )
    holding_df = spark.createDataFrame([Row(**r) for r in holding_records])
    holding_df.write.format("delta").mode("overwrite").save(
        get_bronze_path("financial_holding")
    )
    print(f"   Written {holding_df.count()} records to {get_bronze_path('financial_holding')}")

    print("\n" + "=" * 60)
    print("Bronze data generation complete!")
    print("=" * 60)

    return {
        "bank_ids": bank_ids,
        "branch_ids": branch_ids,
        "contact_ids": contact_ids,
        "account_ids": account_ids,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Bronze Data Generation

# COMMAND ----------

generated_ids = write_bronze_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Bronze Data

# COMMAND ----------

for entity in CDM_ENTITIES:
    print(f"\n{'='*60}")
    print(f"BRONZE - {CDM_ENTITIES[entity]}")
    print(f"{'='*60}")
    df = spark.read.format("delta").load(get_bronze_path(entity))
    print(f"Record count: {df.count()}")
    print(f"Schema:")
    df.printSchema()
    df.show(5, truncate=False)
