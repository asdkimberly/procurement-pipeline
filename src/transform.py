"""
transform.py — Procurement Intelligence Pipeline
=================================================
Cleans, validates, and enriches the extracted Peru Compras data.
Consumes the output of extract.py and produces a clean dataset
ready for load.py to insert into the database.

Transformations applied (8 steps):
    1. Parse dates         — published_date, accepted_date (DD/MM/YYYY → datetime)
    2. Clean numerics      — unit_price_pen, subtotal_pen (float); delivery_quantity,
                             supplier_ruc, entity_ruc (int)
    3. Standardize text    — strip, uppercase, nullify blanks across 13 text columns
    4. Derive columns      — order_year, order_month, order_quarter, order_ym,
                             value_tier (micro/small/medium/large/mega), is_cancelled
    5. Flag anomalies      — flag_zero_value, flag_negative_value, flag_future_date,
                             flag_missing_supplier, flag_missing_entity
    6. Filter rows         — drop zero/null unit_price_pen; keep only computing
                             categories (COMPUTADORA DE ESCRITORIO, COMPUTADORA PORTATIL,
                             COMPUTADORA TODO EN UNO, ESTACION DE TRABAJO,
                             ESTACION DE TRABAJO PORTATIL)
    7. Extract specs       — parse product_description into: product_category,
                             spec_processor, spec_ram, spec_storage, spec_model,
                             spec_processor_brand, spec_processor_sku
    8. Deduplicate         — on order_id, keep last occurrence

Usage:
    python transform.py                                        # reads data/processed/extracted.csv
    python transform.py --file data/processed/extracted.csv   # specific input file
    python transform.py --report                               # also save quality_report.csv

Output:
    data/processed/transformed.csv                            # clean, enriched dataset
    data/processed/quality_report.csv                        # per-column quality summary (--report)

Author: Your Name
"""

import sys
import logging
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

# ─── Configuration ────────────────────────────────────────────────────────────

PROCESSED_DIR = Path("data/processed")
LOG_DIR       = Path("logs")

INPUT_FILE    = PROCESSED_DIR / "extracted.csv"
OUTPUT_FILE   = PROCESSED_DIR / "transformed.csv"
REPORT_FILE   = PROCESSED_DIR / "quality_report.csv"

# Value tier thresholds in PEN (Peruvian Soles)
# Useful for segmenting contracts by size in analysis
VALUE_TIERS = [
    (0,         10_000,    "micro"),       # < S/. 10K
    (10_000,    100_000,   "small"),       # S/. 10K – 100K
    (100_000,   500_000,   "medium"),      # S/. 100K – 500K
    (500_000,   1_000_000, "large"),       # S/. 500K – 1M
    (1_000_000, float("inf"), "mega"),     # > S/. 1M
]

# Known processor brands — used to split spec_processor into brand + SKU.
# Add new brands here if they appear in the data.
PROCESSOR_BRANDS = ["INTEL", "AMD"]

# Computing product categories to keep — specs are only meaningful for these.
# All other categories (furniture, printers, supplies, etc.) are excluded.
RELEVANT_CATEGORIES = {
    "COMPUTADORA DE ESCRITORIO",
    "COMPUTADORA PORTATIL",
    "COMPUTADORA TODO EN UNO",
    "ESTACION DE TRABAJO",
    "ESTACION DE TRABAJO PORTATIL",
}

# Stop labels used in the regex lookahead — everything a KEY: VALUE spec can stop at.
# UNIDAD OPTICA must appear before the standalone UNIDAD stop (which has no colon).
_STOP_LABELS = (
    r"PROCESADOR|RAM|ALMACENAMIENTO|PANTALLA|LAN|WLAN|BLUETOOTH|USB|VGA|HDMI"
    r"|SIST[.] OPER|BATERIA|PESO|UNIDAD OPTICA|CAMARA WEB|TECLADO|MOUSE"
    r"|SUITE OFIMATICA(?:\s+PRE-INSTALADA)?"
    r"|G[.] F"
    r"|SIST[.] MANEJO RAEE"
)

# Two-branch lookahead:
#   branch 1 — standard "KEY:" stop labels
#   branch 2 — standalone UNIDAD (no colon — marks start of model field)
_LOOKAHEAD = rf"(?=\s+(?:{_STOP_LABELS})\s*:|\s+UNIDAD(?!\s+OPTICA)|\s*$)"

# Spec definitions: (column_name, regex_pattern)
# spec_model uses a self-contained pattern (no-colon label, bounded by SIST. MANEJO RAEE)
SPEC_PATTERNS = [
    ("spec_processor", rf"PROCESADOR\s*:\s*(.+?){_LOOKAHEAD}"),
    ("spec_ram",       rf"RAM\s*:\s*(.+?){_LOOKAHEAD}"),
    ("spec_storage",   rf"ALMACENAMIENTO\s*:\s*(.+?){_LOOKAHEAD}"),
    ("spec_model",     r"UNIDAD(?!\s+OPTICA)\s+(.+?)\s+SIST[.] MANEJO RAEE"),
]

# ─── Logging Setup ────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "transform.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─── Step 1: Date Parsing ─────────────────────────────────────────────────────

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse date columns from Peru Compras format (DD/MM/YYYY HH:MM:SS)
    into proper datetime objects.
    """
    date_cols = ["published_date", "accepted_date"]

    for col in date_cols:
        if col not in df.columns:
            continue
        original_nulls = df[col].isna().sum()
        df[col] = pd.to_datetime(
            df[col],
            dayfirst=True,          # DD/MM/YYYY format
            errors="coerce"         # unparseable → NaT (not an error)
        )
        new_nulls = df[col].isna().sum()
        failed    = new_nulls - original_nulls
        if failed > 0:
            log.warning(f"  '{col}': {failed} values could not be parsed as dates → set to NaT")
        else:
            log.info(f"  '{col}': parsed successfully")

    return df


# ─── Step 2: Numeric Columns ──────────────────────────────────────────────────

def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast monetary and numeric columns to float/int.
    Handles values that may contain currency symbols or thousand separators.
    """
    numeric_cols = ["unit_price_pen", "subtotal_pen"]
    int_cols     = ["delivery_quantity", "supplier_ruc", "entity_ruc"]

    for col in numeric_cols:
        if col not in df.columns:
            continue
        # Remove any stray currency symbols or spaces
        if df[col].dtype == object:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[S/.\s,]", "", regex=True)
                .str.replace(r"\.(?=\d{3})", "", regex=True)  # remove thousands dots
                .str.replace(",", ".", regex=False)            # decimal comma → dot
            )
        df[col] = pd.to_numeric(df[col], errors="coerce")
        log.info(f"  '{col}': cast to float ({df[col].isna().sum()} nulls)")

    for col in int_cols:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # nullable int
        log.info(f"  '{col}': cast to Int64 ({df[col].isna().sum()} nulls)")

    return df


# ─── Step 3: Text Standardization ────────────────────────────────────────────

def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize free-text fields: strip whitespace, normalize case,
    replace blank strings with NaN.
    """
    text_cols = [
        "supplier_name", "entity_name", "executing_unit",
        "order_status", "purchase_type", "procedure",
        "framework_agreement", "catalog", "category",
        "product_description", "product_brand",
        "delivery_department", "delivery_province", "delivery_district",
    ]

    for col in text_cols:
        if col not in df.columns:
            continue
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.upper()                         # consistent casing for grouping
            .replace({"NAN": np.nan, "": np.nan, "NONE": np.nan})
        )
        log.info(f"  '{col}': text standardized")

    return df


# ─── Step 4: Derived Columns ──────────────────────────────────────────────────

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add analytically useful derived columns.
    These are the columns you'll use most in SQL queries and dashboards.
    """

    # Time dimensions from published_date
    if "published_date" in df.columns:
        df["order_year"]    = df["published_date"].dt.year
        df["order_month"]   = df["published_date"].dt.month
        df["order_quarter"] = df["published_date"].dt.quarter
        df["order_ym"]      = df["published_date"].dt.to_period("M").astype(str)  # e.g. "2023-06"
        log.info("  Added: order_year, order_month, order_quarter, order_ym")

    # Value tier segmentation based on subtotal_pen
    if "subtotal_pen" in df.columns:
        def assign_tier(value):
            if pd.isna(value):
                return np.nan
            for low, high, label in VALUE_TIERS:
                if low <= value < high:
                    return label
            return "mega"

        df["value_tier"] = df["subtotal_pen"].apply(assign_tier)
        log.info("  Added: value_tier (micro / small / medium / large / mega)")

    # Is the order cancelled?
    if "order_status" in df.columns:
        df["is_cancelled"] = df["order_status"].str.contains("ANUL", na=False)
        log.info("  Added: is_cancelled")

    return df


# ─── Step 5: Data Quality Flags ───────────────────────────────────────────────

def flag_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify and flag data quality issues without dropping records.
    Each flag is a boolean column — analysts can filter or investigate later.

    Flags added:
        flag_zero_value       : subtotal_pen == 0 or missing
        flag_future_date      : published_date is in the future
        flag_missing_supplier : supplier_name or supplier_ruc is null
        flag_missing_entity   : entity_name or entity_ruc is null
        flag_negative_value   : subtotal_pen is negative
    """
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    flags = {}

    if "subtotal_pen" in df.columns:
        flags["flag_zero_value"]     = (df["subtotal_pen"].isna()) | (df["subtotal_pen"] == 0)
        flags["flag_negative_value"] = df["subtotal_pen"] < 0

    if "published_date" in df.columns:
        flags["flag_future_date"] = df["published_date"] > today

    if "supplier_name" in df.columns or "supplier_ruc" in df.columns:
        s_name = df.get("supplier_name", pd.Series(np.nan, index=df.index))
        s_ruc  = df.get("supplier_ruc",  pd.Series(np.nan, index=df.index))
        flags["flag_missing_supplier"] = s_name.isna() & s_ruc.isna()

    if "entity_name" in df.columns or "entity_ruc" in df.columns:
        e_name = df.get("entity_name", pd.Series(np.nan, index=df.index))
        e_ruc  = df.get("entity_ruc",  pd.Series(np.nan, index=df.index))
        flags["flag_missing_entity"] = e_name.isna() & e_ruc.isna()

    for flag_name, flag_series in flags.items():
        df[flag_name] = flag_series
        count = int(flag_series.sum())
        if count > 0:
            log.warning(f"  {flag_name}: {count:,} records flagged")
        else:
            log.info(f"  {flag_name}: 0 records flagged (clean)")

    return df


# ─── Step 5b: Spec Extraction ────────────────────────────────────────────────

def extract_specs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse semi-structured product descriptions into individual spec columns.

    Product descriptions follow a KEY: VALUE pattern, e.g.:
        COMPUTADORA DE ESCRITORIO : PROCESADOR: AMD RYZEN 7 PRO ...

    Two types of columns are extracted:
        product_category — text before the first colon (e.g. "COMPUTADORA DE ESCRITORIO")
        spec_*           — individual spec values extracted via regex (None if not present)

    Rows from non-computing categories have already been filtered out, so nulls
    in spec columns mean the spec was genuinely absent from the description.
    """
    import re

    if "product_description" not in df.columns:
        log.warning("  'product_description' not found — skipping spec extraction")
        return df

    desc = df["product_description"].astype(str).str.strip().str.upper()

    # Extract product category: everything before the first " : "
    df["product_category"] = desc.str.extract(r"^(.+?)\s*:")[0].str.strip()
    log.info("  Added: product_category")

    # Extract each spec using its pattern.
    # spec_model uses a self-contained 2-group pattern — group(1) is the capture.
    # All others use str.extract which returns group 1 by default.
    for col_name, pattern in SPEC_PATTERNS:
        df[col_name] = desc.str.extract(pattern, flags=re.IGNORECASE)[0].str.strip()
        n_extracted  = df[col_name].notna().sum()
        log.info(f"  Added: {col_name} ({n_extracted:,} values extracted)")

    # Derive processor brand and SKU from spec_processor.
    # Handles optional quantity prefix e.g. "2 X INTEL XEON GOLD 6426Y"
    # Pattern: optional "<N> X " prefix → brand → everything after is SKU
    if "spec_processor" in df.columns:
        proc = df["spec_processor"].str.strip()
        cpu_pattern = r"^(?:\d+\s*[Xx]\s*)?(" + "|".join(PROCESSOR_BRANDS) + r")\s+(.+)$"
        extracted = proc.str.extract(cpu_pattern, flags=re.IGNORECASE)
        df["spec_processor_brand"] = extracted[0].str.upper().str.strip()
        df["spec_processor_sku"]   = extracted[1].str.upper().str.strip()
        log.info(f"  Added: spec_processor_brand ({df['spec_processor_brand'].notna().sum():,} values extracted)")
        log.info(f"  Added: spec_processor_sku   ({df['spec_processor_sku'].notna().sum():,} values extracted)")

    return df


# ─── Step 6: Row Filters ─────────────────────────────────────────────────────

def apply_row_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows that do not meet basic data requirements.
    These are records with no analytical value, not just flagged anomalies.

    Filters applied:
        - unit_price_pen == 0 or null: orders with no unit price are uninformative
    """
    before = len(df)

    if "unit_price_pen" in df.columns:
        df = df[df["unit_price_pen"].notna() & (df["unit_price_pen"] != 0)]
        removed = before - len(df)
        if removed > 0:
            log.warning(f"  Row filter: dropped {removed:,} records with zero/null unit_price_pen")
        else:
            log.info("  Row filter: all records have a valid unit_price_pen")

    # Keep only relevant computing product categories.
    # Spec extraction in the next step only makes sense for these product types.
    # Nulls in spec columns would otherwise be ambiguous (missing spec vs wrong category).
    if "category" in df.columns:
        before_cat = len(df)
        df = df[df["category"].isin(RELEVANT_CATEGORIES)]
        removed_cat = before_cat - len(df)
        if removed_cat > 0:
            log.warning(f"  Row filter: dropped {removed_cat:,} records outside relevant categories")
        else:
            log.info("  Row filter: all records are in relevant categories")

    return df.reset_index(drop=True)


# ─── Step 7: Deduplication ────────────────────────────────────────────────────

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows based on order_id (the unique OCID).
    If order_id is not present, falls back to full-row deduplication.
    Keeps the last occurrence (most recent export tends to be more complete).
    """
    before = len(df)

    if "order_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id"], keep="last")
    else:
        log.warning("  'order_id' column not found — deduplicating on all columns")
        df = df.drop_duplicates(keep="last")

    removed = before - len(df)
    if removed > 0:
        log.warning(f"  Removed {removed:,} duplicate records")
    else:
        log.info(f"  No duplicates found")

    return df.reset_index(drop=True)


# ─── Quality Report ───────────────────────────────────────────────────────────

def generate_quality_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a per-column data quality summary: null counts, types, and
    unique value counts. Useful for README and stakeholder reporting.

    Returns:
        DataFrame with one row per column
    """
    report = pd.DataFrame({
        "column":       df.columns,
        "dtype":        [str(df[c].dtype) for c in df.columns],
        "null_count":   [int(df[c].isna().sum()) for c in df.columns],
        "null_pct":     [round(df[c].isna().mean() * 100, 2) for c in df.columns],
        "unique_count": [df[c].nunique(dropna=True) for c in df.columns],
        "sample_value": [df[c].dropna().iloc[0] if df[c].notna().any() else None for c in df.columns],
    })
    return report


def print_transform_summary(df: pd.DataFrame) -> None:
    """Print a clean summary after all transformations."""
    print("\n" + "=" * 55)
    print("  PERU COMPRAS — TRANSFORM SUMMARY")
    print("=" * 55)
    print(f"  Records after transform  : {len(df):>10,}")
    print(f"  Columns                  : {len(df.columns):>10}")

    if "subtotal_pen" in df.columns:
        clean_spend = df.loc[~df.get("flag_zero_value", pd.Series(False, index=df.index)), "subtotal_pen"]
        print(f"  Total spend (PEN)        : S/. {clean_spend.sum():>12,.2f}")
        print(f"  Avg order value (PEN)    : S/. {clean_spend.mean():>12,.2f}")

    if "order_year" in df.columns:
        years = sorted(df["order_year"].dropna().unique().astype(int))
        print(f"  Years covered            : {', '.join(map(str, years))}")

    if "delivery_department" in df.columns:
        top_depts = df["delivery_department"].value_counts().head(3)
        print(f"\n  Top 3 departments by orders:")
        for dept, count in top_depts.items():
            print(f"    {str(dept):<30} {count:>6,}")

    if "value_tier" in df.columns:
        print(f"\n  Order size distribution:")
        for tier, count in df["value_tier"].value_counts().sort_index().items():
            print(f"    {str(tier):<12} {count:>6,}")

    # Summarize quality flags
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    if flag_cols:
        print(f"\n  Data quality flags:")
        for flag in flag_cols:
            print(f"    {flag:<35} {int(df[flag].sum()):>6,}")

    print("=" * 55 + "\n")


# ─── Main Transform Function ──────────────────────────────────────────────────

def transform(df: pd.DataFrame = None, filepath: Path = None) -> pd.DataFrame:
    """
    Run all transformation steps on the extracted Peru Compras data.

    Args:
        df:       DataFrame from extract.py (if already in memory)
        filepath: Path to extracted CSV (if loading from disk)

    Returns:
        Fully cleaned and enriched DataFrame ready for load.py
    """
    log.info("─── TRANSFORM STEP STARTED ─────────────────────────────")

    # Load from disk if df not provided
    if df is None:
        fp = Path(filepath) if filepath else INPUT_FILE
        if not fp.exists():
            raise FileNotFoundError(
                f"Input file not found: {fp}\n"
                "Run extract.py first to generate this file."
            )
        log.info(f"Loading extracted data from: {fp}")
        df = pd.read_csv(fp, low_memory=False)
        log.info(f"Loaded {len(df):,} rows")

    log.info("Step 1/6: Parsing dates ...")
    df = parse_dates(df)

    log.info("Step 2/6: Cleaning numeric columns ...")
    df = clean_numeric_columns(df)

    log.info("Step 3/6: Standardizing text columns ...")
    df = clean_text_columns(df)

    log.info("Step 4/6: Adding derived columns ...")
    df = add_derived_columns(df)

    log.info("Step 5/8: Flagging anomalies ...")
    df = flag_anomalies(df)

    log.info("Step 6/8: Applying row filters ...")
    df = apply_row_filters(df)

    log.info("Step 7/8: Extracting product specs ...")
    df = extract_specs(df)

    log.info("Step 8/8: Deduplicating ...")
    df = deduplicate(df)

    print_transform_summary(df)

    log.info(f"─── TRANSFORM STEP COMPLETE — {len(df):,} clean records ready for load ───")
    return df


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform and validate extracted Peru Compras data."
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        default=None,
        help="Path to extracted CSV from extract.py (default: data/processed/extracted.csv)"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Save a data quality report to data/processed/quality_report.csv"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        df = transform(filepath=args.file)

        # Save transformed output for load.py
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
        log.info(f"Transformed data saved to: {OUTPUT_FILE}")
        print(f"Transform complete. Output saved to: {OUTPUT_FILE}")

        # Optionally save data quality report
        if args.report:
            report = generate_quality_report(df)
            report.to_csv(REPORT_FILE, index=False)
            log.info(f"Quality report saved to: {REPORT_FILE}")
            print(f"Quality report saved to: {REPORT_FILE}")

    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)
    except Exception as e:
        log.exception(f"Unexpected error during transformation: {e}")
        sys.exit(1)
