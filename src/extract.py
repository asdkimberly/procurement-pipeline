"""
extract.py — Procurement Intelligence Pipeline
================================================
Ingests public procurement purchase order data from Peru Compras
(Central de Compras Públicas del Perú) via their open data portal.

Source: https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub
Data:   Purchase orders (Órdenes de Compra) from the Electronic Catalogs
        module (Acuerdos Marco / Framework Agreements)
Format: CSV or XLSX downloaded from the portal

Steps performed:
    1. Auto-detect or accept the latest raw file in data/raw/
    2. Skip 5-row preamble (original exports only — snapshots are skipped)
    3. Select and rename the 27 official Peru Compras columns to English snake_case
    4. Drop any unrecognized source columns to keep the schema strict
    5. Add pipeline metadata (_source_file, _extracted_at, _pipeline_version)
    6. Save a timestamped snapshot to data/raw/ for reproducibility
    7. Write cleaned output to data/processed/extracted.csv

How to get the data:
    1. Go to https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub
    2. Select an "Acuerdo Marco" (e.g. Equipos Informáticos, Útiles de Escritorio)
    3. Click "Iniciar Búsqueda"
    4. Export as .csv or .xlsx
    5. Place the file in data/raw/

Usage:
    python extract.py                              # auto-detect latest file in data/raw/
    python extract.py --file data/raw/orders.csv   # specific file
    python extract.py --file data/raw/orders.xlsx  # Excel format also supported

Output:
    data/raw/snapshot_<timestamp>.<ext>            # timestamped backup of source file
    data/processed/extracted.csv                   # renamed and selected columns

Author: Your Name
"""

import os
import sys
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ─── Configuration ────────────────────────────────────────────────────────────

RAW_DATA_DIR  = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
LOG_DIR       = Path("logs")

# Official Peru Compras column names from the raw CSV/XLSX export.
# Maps original Spanish column names → clean English snake_case names.
# Only these columns will be kept — any extra columns in the source are dropped.
COLUMN_MAP = {
    "Nro":                            "row_number",           # Auto-incremented row correlative
    "Código Acuerdo Marco":           "framework_code",       # Framework agreement code
    "Ruc Proveedor":                  "supplier_ruc",         # Supplier tax ID
    "Razón Social Proveedor":         "supplier_name",        # Supplier legal name
    "Ruc Entidad":                    "entity_ruc",           # Buying entity tax ID
    "Razón Social Entidad":           "entity_name",          # Buying entity legal name
    "Unidad Ejecutora":               "executing_unit",       # Sub-unit within entity
    "Procedimiento":                  "procedure",            # Procurement procedure type
    "Tipo de Compra":                 "purchase_type",        # Compra Ordinaria / Gran Compra
    "Orden Electrónica":              "order_id",             # Unique electronic order ID (OCID)
    "Estado de la Orden Electrónica": "order_status",         # e.g. Formalizado, Anulado
    "Fecha Publicación":              "published_date",       # Date order was published
    "Fecha Aceptación":               "accepted_date",        # Date order was accepted by supplier
    "Acuerdo Marco":                  "framework_agreement",  # Framework agreement name
    "Ubigeo Proveedor":               "supplier_ubigeo",      # Supplier geographic code (UBIGEO)
    "Catálogo":                       "catalog",              # Catalog name
    "Categoría":                      "category",             # Product/service category
    "Descripción Ficha Producto":     "product_description",  # Product sheet description
    "Marca Ficha Producto":           "product_brand",        # Product brand
    "Nro. Parte":                     "part_number",          # Manufacturer part number
    "Nro. Entrega":                   "delivery_number",      # Delivery sequence number
    "Dep. Entrega":                   "delivery_department",  # Delivery department
    "Prov. Entrega":                  "delivery_province",    # Delivery province
    "Dist. Entrega":                  "delivery_district",    # Delivery district
    "Cantidad Entrega":               "delivery_quantity",    # Quantity ordered
    "Precio Unitario":                "unit_price_pen",       # Unit price in PEN
    "Sub Total":                      "subtotal_pen",         # Subtotal before tax (PEN)
}

# ─── Logging Setup ────────────────────────────────────────────────────────────

LOG_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "extract.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─── Helper Functions ─────────────────────────────────────────────────────────

def find_raw_file() -> Path:
    """
    Auto-detect the most recent CSV or XLSX file in data/raw/.
    Raises FileNotFoundError if none found.
    """
    candidates = list(RAW_DATA_DIR.glob("*.csv")) + list(RAW_DATA_DIR.glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(
            f"No CSV or XLSX files found in '{RAW_DATA_DIR}/'.\n"
            "Please download data from:\n"
            "  https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub\n"
            "and place the file in data/raw/"
        )
    # Return most recently modified file
    latest = max(candidates, key=lambda f: f.stat().st_mtime)
    log.info(f"Auto-detected raw file: {latest}")
    return latest


def load_raw_file(filepath: Path) -> pd.DataFrame:
    """
    Load a CSV or XLSX file exported from Peru Compras into a DataFrame.
    Handles common encoding issues with Spanish characters.

    Args:
        filepath: Path to the raw data file

    Returns:
        Raw DataFrame with original column names
    """
    suffix = filepath.suffix.lower()
    log.info(f"Loading file: {filepath} ({suffix})")

    df = None

    # Snapshots are clean files saved by this pipeline — no preamble to skip.
    # Only original exports from the Peru Compras portal have the 5-row preamble.
    is_snapshot = filepath.name.startswith("snapshot_")
    skip        = 0 if is_snapshot else 5
    if skip:
        log.info("Skipping 5 preamble rows (original Peru Compras export detected)")

    if suffix == ".csv":
        # Peru Compras CSVs can be UTF-8 or latin-1 encoded
        for encoding in ["utf-8", "latin-1", "utf-8-sig"]:
            try:
                df = pd.read_csv(
                    filepath,
                    encoding=encoding,
                    sep=",",
                    skiprows=skip,     # 5 for original exports, 0 for snapshots
                    thousands=".",     # Peru uses period as thousands separator
                    decimal=",",       # and comma as decimal separator
                    low_memory=False
                )
                log.info(f"Loaded with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                log.warning(f"Encoding {encoding} failed, trying next ...")

    elif suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath, engine="openpyxl", skiprows=skip)

    else:
        raise ValueError(f"Unsupported file format: {suffix}. Use .csv or .xlsx")

    if df is None:
        raise RuntimeError(f"Failed to load file: {filepath}")

    log.info(f"Raw data loaded: {len(df):,} rows x {len(df.columns)} columns")
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename Spanish Peru Compras column names to clean English snake_case.
    Only renames columns that exist — safe for partial exports.

    Args:
        df: Raw DataFrame

    Returns:
        DataFrame with renamed columns
    """
    # Strip whitespace from column names (common in portal exports)
    df.columns = df.columns.str.strip()

    matched   = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    unmatched = [c for c in df.columns if c not in COLUMN_MAP]

    df = df.rename(columns=matched)

    # Drop any columns not in COLUMN_MAP — keeps the pipeline schema strict
    # and prevents unexpected source columns from leaking downstream.
    known_cols = list(COLUMN_MAP.values())
    df = df[[c for c in df.columns if c in known_cols]]

    log.info(f"Renamed {len(matched)} columns to English snake_case")
    if unmatched:
        log.info(f"Dropped {len(unmatched)} unrecognized source columns: {unmatched}")

    return df


def add_metadata(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    """
    Add pipeline metadata columns for traceability and reproducibility.

    Args:
        df:          DataFrame
        source_file: Original source file path

    Returns:
        DataFrame with added metadata columns
    """
    df["_source_file"]      = source_file.name
    df["_extracted_at"]     = datetime.now(timezone.utc).isoformat()
    df["_pipeline_version"] = "1.0.0"
    return df


def save_snapshot(df: pd.DataFrame) -> Path:
    """
    Save a timestamped raw snapshot to data/raw/ for reproducibility.
    Allows tracing back to exactly what was ingested on each run.

    Args:
        df: Raw DataFrame

    Returns:
        Path to saved snapshot file
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path  = RAW_DATA_DIR / f"snapshot_{timestamp}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"Raw snapshot saved: {out_path}")
    return out_path


def print_summary(df: pd.DataFrame) -> None:
    """
    Print a human-readable ingestion summary to the console.
    """
    print("\n" + "=" * 55)
    print("  PERU COMPRAS — EXTRACTION SUMMARY")
    print("=" * 55)
    print(f"  Total records ingested : {len(df):>10,}")
    print(f"  Columns                : {len(df.columns):>10}")

    if "order_date" in df.columns:
        print(f"  Date range             : {df['order_date'].min()} -> {df['order_date'].max()}")

    if "total_pen" in df.columns:
        total_spend = pd.to_numeric(df["total_pen"], errors="coerce").sum()
        print(f"  Total spend (PEN)      : S/. {total_spend:>14,.2f}")

    if "entity_name" in df.columns:
        print(f"  Unique entities        : {df['entity_name'].nunique():>10,}")

    if "supplier_name" in df.columns:
        print(f"  Unique suppliers       : {df['supplier_name'].nunique():>10,}")

    if "order_status" in df.columns:
        print(f"\n  Order status breakdown:")
        for status, count in df["order_status"].value_counts().items():
            print(f"    {str(status):<30} {count:>6,}")

    print("=" * 55 + "\n")


# ─── Main Extraction Function ─────────────────────────────────────────────────

def extract(filepath: Path = None, save_snapshot_flag: bool = True) -> pd.DataFrame:
    """
    Main extraction function. Loads raw Peru Compras data, renames columns,
    adds traceability metadata, and optionally saves a timestamped snapshot.

    This output feeds directly into transform.py.

    Args:
        filepath:            Path to raw file. If None, auto-detects in data/raw/
        save_snapshot_flag:  Whether to save a timestamped raw snapshot

    Returns:
        DataFrame ready for the transform step
    """
    log.info("─── EXTRACT STEP STARTED ───────────────────────────────")

    # 1. Resolve file path
    if filepath is None:
        filepath = find_raw_file()
    else:
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

    # 2. Load raw data
    df = load_raw_file(filepath)

    # 3. Rename columns to English snake_case
    df = rename_columns(df)

    # 4. Add pipeline metadata for traceability
    df = add_metadata(df, filepath)

    # 5. Save timestamped raw snapshot
    if save_snapshot_flag:
        save_snapshot(df)

    # 6. Print ingestion summary
    print_summary(df)

    log.info(f"─── EXTRACT STEP COMPLETE — {len(df):,} records ready for transform ───")
    return df


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract Peru Compras procurement data into the pipeline."
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        default=None,
        help=(
            "Path to the raw CSV or XLSX file exported from Peru Compras. "
            "If not provided, auto-detects the latest file in data/raw/"
        )
    )
    parser.add_argument(
        "--no-snapshot",
        action="store_true",
        help="Skip saving a raw timestamped snapshot (useful in CI/CD)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        df = extract(
            filepath=args.file,
            save_snapshot_flag=not args.no_snapshot
        )
        # Save extracted output for transform.py to consume
        out_path = PROCESSED_DIR / "extracted.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info(f"Extracted data saved to: {out_path}")
        print(f"Extraction complete. Output saved to: {out_path}")

    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)
    except Exception as e:
        log.exception(f"Unexpected error during extraction: {e}")
        sys.exit(1)
