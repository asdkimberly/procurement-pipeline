"""
load.py — Procurement Intelligence Pipeline
============================================
Loads the transformed Peru Compras data into a SQLite database
and runs analytical SQL queries to validate and summarize results.
Uses INSERT OR IGNORE so re-running the pipeline is always safe.

Database schema:
    contracts      — main table, one row per purchase order (47 columns)
    quality_flags  — per-run summary of data quality flag counts
    load_log       — audit trail of every pipeline execution

Indexes:
    published_date, order_ym, entity_name, supplier_name,
    delivery_department, framework_agreement, value_tier,
    category, spec_processor_brand

Analytical queries (7):
    01. Monthly spend trend          — subtotal_pen by order_ym
    02. Top 10 suppliers             — by total contract value
    03. Top 10 buying entities       — by total spend
    04. Spend by value tier          — micro / small / medium / large / mega
    05. Spend by department          — top 15 delivery departments
    06. Top products by spend        — product_category + spec_model + unit_price_pen
    07. Processor brand share        — AMD vs Intel by order count and spend

Usage:
    python load.py                    # reads data/processed/transformed.csv
    python load.py --file <path>      # specific input file
    python load.py --reset            # drop and recreate all tables + indexes
    python load.py --queries          # run and print all 7 analytical queries

Output:
    data/procurement.db               # SQLite database

Author: Your Name
"""

import sys
import logging
import argparse
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from textwrap import dedent

# ─── Configuration ────────────────────────────────────────────────────────────

PROCESSED_DIR = Path("data/processed")
DB_DIR        = Path("data")
LOG_DIR       = Path("logs")

INPUT_FILE    = PROCESSED_DIR / "transformed.csv"
DB_PATH       = DB_DIR / "procurement.db"

PIPELINE_VERSION = "1.0.0"

# ─── Logging Setup ────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
DB_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "load.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─── Database Setup ───────────────────────────────────────────────────────────

DDL_CONTRACTS = """
CREATE TABLE IF NOT EXISTS contracts (
    -- Source identifiers
    row_number              INTEGER,
    order_id                TEXT PRIMARY KEY,
    framework_code          TEXT,
    framework_agreement     TEXT,

    -- Supplier
    supplier_ruc            INTEGER,
    supplier_name           TEXT,
    supplier_ubigeo         TEXT,

    -- Buying entity
    entity_ruc              INTEGER,
    entity_name             TEXT,
    executing_unit          TEXT,

    -- Order metadata
    procedure               TEXT,
    purchase_type           TEXT,
    order_status            TEXT,
    is_cancelled            INTEGER,        -- boolean: 0 / 1

    -- Dates
    published_date          TEXT,           -- ISO 8601: YYYY-MM-DD HH:MM:SS
    accepted_date           TEXT,
    order_year              INTEGER,
    order_month             INTEGER,
    order_quarter           INTEGER,
    order_ym                TEXT,           -- e.g. "2023-06"

    -- Delivery
    delivery_number         INTEGER,
    delivery_quantity       REAL,
    delivery_department     TEXT,
    delivery_province       TEXT,
    delivery_district       TEXT,

    -- Product
    catalog                 TEXT,
    category                TEXT,
    product_description     TEXT,
    product_brand           TEXT,
    part_number             TEXT,

    -- Financials (PEN — Peruvian Soles)
    unit_price_pen          REAL,
    subtotal_pen            REAL,
    value_tier              TEXT,           -- micro / small / medium / large / mega

    -- Product specs (extracted from product_description)
    product_category        TEXT,
    spec_processor          TEXT,
    spec_processor_brand    TEXT,
    spec_processor_sku      TEXT,
    spec_ram                TEXT,
    spec_storage            TEXT,
    spec_model              TEXT,

    -- Data quality flags
    flag_zero_value         INTEGER,        -- boolean: 0 / 1
    flag_negative_value     INTEGER,
    flag_future_date        INTEGER,
    flag_missing_supplier   INTEGER,
    flag_missing_entity     INTEGER,

    -- Pipeline metadata
    _source_file            TEXT,
    _extracted_at           TEXT,
    _pipeline_version       TEXT
);
"""

DDL_QUALITY_FLAGS = """
CREATE TABLE IF NOT EXISTS quality_flags (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT NOT NULL,
    loaded_at           TEXT NOT NULL,
    total_records       INTEGER,
    flag_zero_value     INTEGER,
    flag_future_date    INTEGER,
    flag_negative_value INTEGER,
    flag_missing_supplier INTEGER,
    flag_missing_entity INTEGER,
    igv_flag            INTEGER
);
"""

DDL_LOAD_LOG = """
CREATE TABLE IF NOT EXISTS load_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    loaded_at       TEXT NOT NULL,
    source_file     TEXT,
    records_loaded  INTEGER,
    records_skipped INTEGER,
    pipeline_version TEXT,
    status          TEXT        -- 'success' / 'error'
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_contracts_published_date    ON contracts (published_date);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_order_ym          ON contracts (order_ym);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_entity_name       ON contracts (entity_name);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_supplier_name     ON contracts (supplier_name);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_department        ON contracts (delivery_department);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_framework         ON contracts (framework_agreement);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_value_tier        ON contracts (value_tier);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_category          ON contracts (category);",
    "CREATE INDEX IF NOT EXISTS idx_contracts_processor_brand   ON contracts (spec_processor_brand);",
]


def init_database(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist."""
    cur = conn.cursor()
    cur.execute(DDL_CONTRACTS)
    cur.execute(DDL_QUALITY_FLAGS)
    cur.execute(DDL_LOAD_LOG)
    for idx_ddl in DDL_INDEXES:
        cur.execute(idx_ddl)
    conn.commit()
    log.info(f"Database initialized: {DB_PATH}")


def reset_database(conn: sqlite3.Connection) -> None:
    """Drop all tables and recreate from scratch. Use with --reset flag."""
    cur = conn.cursor()
    for table in ["contracts", "quality_flags", "load_log"]:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    log.warning("All tables dropped. Reinitializing ...")
    init_database(conn)


# ─── Data Preparation ─────────────────────────────────────────────────────────

def prepare_for_sqlite(df: pd.DataFrame, loaded_at: str) -> pd.DataFrame:
    """
    Final preparation before inserting into SQLite:
      - Convert booleans to 0/1 integers (SQLite has no bool type)
      - Convert datetimes to ISO 8601 strings
      - Convert pandas NA types to None
      - Add _loaded_at timestamp
    """
    df = df.copy()

    # Add load timestamp
    df["_loaded_at"] = loaded_at

    # Convert boolean columns to int
    bool_cols = [c for c in df.columns if c.startswith("flag_") or c == "is_cancelled" or c == "igv_flag"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(object).where(df[col].notna(), None)
            df[col] = df[col].apply(lambda x: int(x) if x is not None else None)

    # Convert datetime columns to ISO strings
    dt_cols = ["order_date", "last_status_date"]
    for col in dt_cols:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S").where(df[col].notna(), None)

    # Convert all pandas NA/NaN to None for SQLite compatibility
    df = df.where(pd.notna(df), None)

    return df


# ─── Loading ──────────────────────────────────────────────────────────────────

def load_contracts(df: pd.DataFrame, conn: sqlite3.Connection, run_id: str) -> dict:
    """
    Insert transformed records into the contracts table.
    Uses INSERT OR IGNORE so re-running the pipeline is safe —
    existing order_ids are skipped, not duplicated.

    Args:
        df:      Transformed DataFrame from transform.py
        conn:    SQLite connection
        run_id:  Unique identifier for this pipeline run

    Returns:
        Dict with loaded/skipped counts
    """
    loaded_at = datetime.now(timezone.utc).isoformat()
    df = prepare_for_sqlite(df, loaded_at)

    # Only keep columns that exist in the contracts table schema
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(contracts)")
    valid_cols = {row[1] for row in cur.fetchall()}
    df_cols    = [c for c in df.columns if c in valid_cols]
    df_insert  = df[df_cols]

    before_count = pd.read_sql("SELECT COUNT(*) as n FROM contracts", conn).iloc[0]["n"]

    # Use INSERT OR IGNORE via a custom method so duplicate order_ids are
    # silently skipped rather than raising an IntegrityError.
    # to_sql's built-in method="multi" does not support INSERT OR IGNORE.
    cols        = df_insert.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    col_names    = ", ".join(cols)
    insert_sql   = f"INSERT OR IGNORE INTO contracts ({col_names}) VALUES ({placeholders})"

    cur = conn.cursor()
    for chunk_start in range(0, len(df_insert), 500):
        chunk = df_insert.iloc[chunk_start : chunk_start + 500]
        rows  = [tuple(None if (v != v) else v for v in row) for row in chunk.itertuples(index=False)]
        cur.executemany(insert_sql, rows)
    conn.commit()

    after_count = pd.read_sql("SELECT COUNT(*) as n FROM contracts", conn).iloc[0]["n"]
    loaded  = int(after_count - before_count)
    skipped = int(len(df_insert) - loaded)

    log.info(f"Loaded {loaded:,} new records ({skipped:,} already existed — skipped)")
    return {"loaded": loaded, "skipped": skipped, "loaded_at": loaded_at}


def log_quality_flags(df: pd.DataFrame, conn: sqlite3.Connection, run_id: str, loaded_at: str) -> None:
    """Insert a quality flag summary row for this pipeline run."""
    flag_cols = [c for c in df.columns if c.startswith("flag_") or c == "igv_flag"]
    row = {
        "run_id":           run_id,
        "loaded_at":        loaded_at,
        "total_records":    len(df),
    }
    for col in ["flag_zero_value", "flag_future_date", "flag_negative_value",
                "flag_missing_supplier", "flag_missing_entity", "igv_flag"]:
        row[col] = int(df[col].sum()) if col in df.columns else 0

    pd.DataFrame([row]).to_sql("quality_flags", conn, if_exists="append", index=False)
    conn.commit()


def log_run(conn: sqlite3.Connection, run_id: str, loaded_at: str,
            source_file: str, loaded: int, skipped: int, status: str) -> None:
    """Insert an audit log entry for this pipeline run."""
    row = {
        "run_id":           run_id,
        "loaded_at":        loaded_at,
        "source_file":      source_file,
        "records_loaded":   loaded,
        "records_skipped":  skipped,
        "pipeline_version": PIPELINE_VERSION,
        "status":           status,
    }
    pd.DataFrame([row]).to_sql("load_log", conn, if_exists="append", index=False)
    conn.commit()


# ─── Analytical SQL Queries ───────────────────────────────────────────────────

ANALYTICAL_QUERIES = {

    "01_monthly_spend_trend": dedent("""
        -- Monthly spend trend (active orders only)
        SELECT
            order_ym                                AS month,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            ROUND(AVG(subtotal_pen), 2)             AS avg_order_value_pen
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND order_ym IS NOT NULL
        GROUP BY order_ym
        ORDER BY order_ym ASC;
    """),

    "02_top_suppliers": dedent("""
        -- Top 10 suppliers by total contract value
        SELECT
            supplier_ruc,
            supplier_name,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_value_pen,
            ROUND(AVG(subtotal_pen), 2)             AS avg_order_value_pen
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND supplier_name IS NOT NULL
        GROUP BY supplier_ruc, supplier_name
        ORDER BY total_value_pen DESC
        LIMIT 10;
    """),

    "03_top_entities": dedent("""
        -- Top 10 buying entities by total spend
        SELECT
            entity_ruc,
            entity_name,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            ROUND(AVG(subtotal_pen), 2)             AS avg_order_value_pen
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND entity_name IS NOT NULL
        GROUP BY entity_ruc, entity_name
        ORDER BY total_spend_pen DESC
        LIMIT 10;
    """),

    "04_spend_by_value_tier": dedent("""
        -- Order count and spend by value tier
        SELECT
            value_tier,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_orders
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND value_tier IS NOT NULL
        GROUP BY value_tier
        ORDER BY total_spend_pen DESC;
    """),

    "05_spend_by_department": dedent("""
        -- Order volume and total spend by delivery department
        SELECT
            delivery_department,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            COUNT(DISTINCT entity_name)             AS unique_entities
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND delivery_department IS NOT NULL
        GROUP BY delivery_department
        ORDER BY total_spend_pen DESC
        LIMIT 15;
    """),

    "06_top_products_by_spend": dedent("""
        -- Top 10 products by total spend (using product_category + spec_model)
        SELECT
            product_category,
            spec_model,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            ROUND(AVG(unit_price_pen), 2)           AS avg_unit_price_pen
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND spec_model IS NOT NULL
        GROUP BY product_category, spec_model
        ORDER BY total_spend_pen DESC
        LIMIT 10;
    """),

    "07_processor_brand_share": dedent("""
        -- Processor brand market share by order count and spend
        SELECT
            spec_processor_brand                    AS brand,
            COUNT(*)                                AS order_count,
            ROUND(SUM(subtotal_pen), 2)             AS total_spend_pen,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_orders
        FROM contracts
        WHERE is_cancelled = 0
          AND flag_zero_value = 0
          AND spec_processor_brand IS NOT NULL
        GROUP BY spec_processor_brand
        ORDER BY order_count DESC;
    """),
}


def run_analytical_queries(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """
    Execute all analytical queries and return results as DataFrames.

    Args:
        conn: SQLite connection

    Returns:
        Dict mapping query name → result DataFrame
    """
    results = {}
    for name, sql in ANALYTICAL_QUERIES.items():
        try:
            df = pd.read_sql_query(sql, conn)
            results[name] = df
            log.info(f"  Query '{name}': {len(df)} rows returned")
        except Exception as e:
            log.error(f"  Query '{name}' failed: {e}")
            results[name] = pd.DataFrame()
    return results


def print_query_results(results: dict[str, pd.DataFrame]) -> None:
    """Pretty-print all query results to the console."""
    for name, df in results.items():
        print(f"\n{'=' * 60}")
        print(f"  {name.upper().replace('_', ' ')}")
        print(f"{'=' * 60}")
        if df.empty:
            print("  (no results)")
        else:
            print(df.to_string(index=False))
    print()


def print_load_summary(conn: sqlite3.Connection, loaded: int, skipped: int) -> None:
    """Print a post-load summary from the database."""
    total = pd.read_sql("SELECT COUNT(*) as n FROM contracts", conn).iloc[0]["n"]
    spend = pd.read_sql(
        "SELECT ROUND(SUM(subtotal_pen),2) as s FROM contracts WHERE is_cancelled=0 AND flag_zero_value=0",
        conn
    ).iloc[0]["s"]

    print("\n" + "=" * 55)
    print("  PERU COMPRAS — LOAD SUMMARY")
    print("=" * 55)
    print(f"  Records loaded this run  : {loaded:>10,}")
    print(f"  Records skipped (exist)  : {skipped:>10,}")
    print(f"  Total records in DB      : {int(total):>10,}")
    print(f"  Total spend in DB (PEN)  : S/. {float(spend or 0):>12,.2f}")
    print(f"  Database location        : {DB_PATH}")
    print("=" * 55 + "\n")


# ─── Main Load Function ───────────────────────────────────────────────────────

def load(df: pd.DataFrame = None, filepath: Path = None,
         reset: bool = False, run_queries: bool = False) -> sqlite3.Connection:
    """
    Main load function. Connects to SQLite, inserts transformed data,
    logs the run, and optionally executes analytical queries.

    Args:
        df:          Transformed DataFrame from transform.py
        filepath:    Path to transformed CSV (if loading from disk)
        reset:       Drop and recreate the database before loading
        run_queries: Whether to execute and print analytical queries

    Returns:
        Open SQLite connection (for chaining with dashboard.py)
    """
    log.info("─── LOAD STEP STARTED ──────────────────────────────────")

    # Load from disk if df not provided
    if df is None:
        fp = Path(filepath) if filepath else INPUT_FILE
        if not fp.exists():
            raise FileNotFoundError(
                f"Input file not found: {fp}\n"
                "Run transform.py first to generate this file."
            )
        log.info(f"Loading transformed data from: {fp}")
        df = pd.read_csv(fp, low_memory=False)
        log.info(f"Loaded {len(df):,} rows")

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")     # better concurrent read performance
    conn.execute("PRAGMA foreign_keys=ON")

    # Init or reset schema
    if reset:
        reset_database(conn)
    else:
        init_database(conn)

    # Generate a unique run ID for this pipeline execution
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    source_file = str(df["_source_file"].iloc[0]) if "_source_file" in df.columns else "unknown"

    # Load data
    status = "success"
    try:
        result = load_contracts(df, conn, run_id)
        log_quality_flags(df, conn, run_id, result["loaded_at"])
        log_run(conn, run_id, result["loaded_at"], source_file,
                result["loaded"], result["skipped"], status)
    except Exception as e:
        status = "error"
        log_run(conn, run_id, datetime.now(timezone.utc).isoformat(), source_file, 0, 0, status)
        raise e

    print_load_summary(conn, result["loaded"], result["skipped"])

    # Run analytical queries if requested
    if run_queries:
        log.info("Running analytical queries ...")
        results = run_analytical_queries(conn)
        print_query_results(results)

    log.info(f"─── LOAD STEP COMPLETE — database at {DB_PATH} ───")
    return conn


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load transformed Peru Compras data into SQLite."
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        default=None,
        help="Path to transformed CSV from transform.py (default: data/processed/transformed.csv)"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate the database before loading (WARNING: deletes all existing data)"
    )
    parser.add_argument(
        "--queries",
        action="store_true",
        help="Run and print all analytical SQL queries after loading"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        conn = load(
            filepath=args.file,
            reset=args.reset,
            run_queries=args.queries
        )
        conn.close()
        print(f"Load complete. Database saved to: {DB_PATH}")

    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)
    except Exception as e:
        log.exception(f"Unexpected error during load: {e}")
        sys.exit(1)
