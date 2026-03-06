# 🇵🇪 Peru Procurement Intelligence Pipeline

> An end-to-end data engineering pipeline that ingests, cleans, and analyzes
> public government procurement data from **Peru Compras** — Peru's Central
> Purchasing Authority — surfacing spending trends, supplier patterns, hardware
> spec insights, and data quality metrics through an interactive dashboard.

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-lightgrey?logo=sqlite)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-Data%20Wrangling-150458?logo=pandas)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📊 Dashboard Preview

![alt text](https://github.com/asdkimberly/procurement-pipeline/blob/main/dashboard.png "Dashboard")


| Overview | Suppliers | Entities | Products | Data Quality |
|----------|-----------|----------|----------|--------------|
| KPIs + spend trend | Top vendors by value | Top agencies by spend | Processor, RAM & storage specs | Flag explorer |

**[Live Demo →](https://peru-procurement-pipeline.streamlit.app/)**

---

## 🔍 What It Does

This pipeline processes purchase orders (Órdenes de Compra) from Peru's
[Electronic Catalogs module](https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub),
which publishes open data under the OCDS (Open Contracting Data Standard).

**Key capabilities:**

- Ingests CSV / XLSX exports from the Peru Compras portal with automatic encoding detection and preamble skipping
- Selects and renames the 27 official source columns to clean English snake_case
- Cleans and standardizes dates, monetary values, and geographic delivery fields
- Filters to computing product categories (desktops, laptops, workstations, all-in-ones)
- Parses semi-structured `product_description` strings into structured spec columns: processor, RAM, storage, model
- Derives processor brand (AMD / Intel) and processor SKU from spec text, handling quantity prefixes like `2 X INTEL`
- Loads data into SQLite with `INSERT OR IGNORE` for safe re-runs and full audit logging
- Flags data quality issues: zero-value orders, future dates, missing suppliers and entities, negative values
- Exposes 7 pre-built analytical SQL queries covering spend trends, supplier concentration, product specs, and more
- Visualizes everything in a 5-page interactive Streamlit dashboard with 5 sidebar filters and 7 charts on the Products page alone

---

## 🏗️ Pipeline Architecture

```
Peru Compras Portal
        │
        ▼
┌──────────────┐     ┌───────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  extract.py  │────▶│  transform.py     │────▶│    load.py       │────▶│  dashboard.py   │
│              │     │                   │     │                  │     │                 │
│ - Ingest CSV │     │ - Parse dates     │     │ - SQLite schema  │     │ - Overview      │
│ - Skip       │     │ - Cast numerics   │     │ - INSERT OR      │     │ - Suppliers     │
│   preamble   │     │ - Standardize     │     │   IGNORE         │     │ - Entities      │
│ - Select &   │     │   text            │     │ - 7 SQL queries  │     │ - Products      │
│   rename     │     │ - Derive columns  │     │ - Audit log      │     │ - Data Quality  │
│   27 columns │     │ - Flag anomalies  │     └──────────────────┘     └─────────────────┘
│ - Snapshot   │     │ - Filter rows     │
└──────────────┘     │ - Extract specs   │
                     │ - Deduplicate     │
                     └───────────────────┘
```

**Data flow:**
```
data/raw/                          →   data/raw/snapshot_<timestamp>.<ext>
data/raw/snapshot_<timestamp>.csv  →   data/processed/extracted.csv
data/processed/extracted.csv       →   data/processed/transformed.csv
data/processed/transformed.csv     →   data/procurement.db
```

---

## 🛠️ Tech Stack

| Layer | Tools |
|-------|-------|
| Ingestion | Python, pandas |
| Transformation | pandas, NumPy, re (regex) |
| Storage | SQLite (via Python sqlite3) |
| Analysis | SQL, pandas |
| Visualization | Streamlit, Plotly |
| Version Control | Git, GitHub |

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/procurement-pipeline.git
cd procurement-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Download the data

1. Go to [catalogos.perucompras.gob.pe](https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub)
2. Select the **Equipos Informáticos** Acuerdo Marco
3. Click **Iniciar Búsqueda** → export as `.csv` or `.xlsx`
4. Place the file in `data/raw/`

### 4. Run the pipeline

All scripts must be run from the **project root**:

```bash
# Step 1 — Ingest
python src/extract.py

# Step 2 — Clean, validate & extract specs
python src/transform.py --report

# Step 3 — Load into database (use --reset on first run or after schema changes)
python src/load.py --reset --queries

# Step 4 — Launch dashboard
streamlit run src/dashboard.py
```

Or run all steps at once:

```bash
python src/extract.py && python src/transform.py && python src/load.py --queries && streamlit run src/dashboard.py
```

---

## 📁 Project Structure

```
procurement-pipeline/
│
├── data/
│   ├── raw/                  # Raw exports from Peru Compras + timestamped snapshots
│   └── processed/            # extracted.csv, transformed.csv, quality_report.csv
│
├── logs/                     # Per-step log files (extract.log, transform.log, load.log)
│
├── src/
│   ├── extract.py            # Step 1: Ingest, select & rename columns, snapshot
│   ├── transform.py          # Step 2: Clean, validate, filter, extract specs
│   ├── load.py               # Step 3: SQLite load + 7 analytical queries
│   └── dashboard.py          # Step 4: 5-page Streamlit dashboard
│
├── requirements.txt
└── README.md
```

---

## 📈 Key Insights (update with your real results)

After running the pipeline on [Acuerdo Marco: Equipos Informáticos]:

| Metric | Value |
|--------|-------|
| Total purchase orders |1690 |
| Total procurement spend (PEN) | 83,421,967 |
| Unique buying entities |497 |
| Unique suppliers |191 |
| Top department by spend | LIMA |
| Most common processor brand | INTEL |
| Most ordered processor SKU | INTEL CORE I7 - 14700 |

---

## 🔎 Analytical Queries

The pipeline ships with 7 pre-built SQL queries (run with `python src/load.py --queries`):

| # | Query | Business Question |
|---|-------|-------------------|
| 1 | Monthly spend trend | Is procurement growing or declining over time? |
| 2 | Top 10 suppliers | Who wins the most contract value? |
| 3 | Top 10 buying entities | Which agencies spend the most? |
| 4 | Spend by value tier | Are most orders micro or large contracts? |
| 5 | Spend by department | Where in Peru is money being spent? |
| 6 | Top products by spend | Which models and categories drive the most spend? |
| 7 | Processor brand share | What is the AMD vs Intel split by orders and spend? |

---

## ✅ Data Quality Checks

The transform step automatically flags the following anomalies without dropping records:

| Flag | Description |
|------|-------------|
| `flag_zero_value` | Orders with S/. 0 or missing unit price |
| `flag_future_date` | Published date is in the future |
| `flag_negative_value` | Negative monetary value |
| `flag_missing_supplier` | No supplier name or RUC |
| `flag_missing_entity` | No buying entity name or RUC |

All flagged records are preserved in the database and explorable in the Data Quality dashboard page.

---

## 📦 Dashboard Pages

| Page | Contents |
|------|----------|
| **Overview** | KPI cards (orders, spend, avg value, suppliers), monthly spend area chart, order status donut, value tier bar chart |
| **Suppliers** | Top 15 suppliers by contract value, market concentration pie, full sortable table |
| **Entities** | Top 15 buying entities by spend, spend by department pie, full sortable table |
| **Products** | Processor brand share (AMD vs Intel), top 15 processor SKUs by unit count, top 15 models by spend, RAM distribution, storage distribution, product brand vs processor SKU heatmap |
| **Data Quality** | Flag KPI cards with red/green indicators, flag trend over time, pipeline run log, anomaly explorer |

**Sidebar filters:** Year · Quarter · Department · Framework Code · Product Category · Exclude cancelled · Exclude zero-value

---

## 🛠️ Development Notes

This project was developed with the assistance of an AI coding assistant
(Claude by Anthropic) for code generation and iteration. All architectural
decisions, domain knowledge, data validation, and debugging were driven
by the author.

---

## 💡 Motivation

During my time at Intel Corporation, I built similar pipelines to collect,
clean, and analyze government procurement data across Latin America —
supporting ~$20M in annual revenue through data-driven business insights.

This project demonstrates those same data engineering skills on a fully
open, reproducible dataset, and extends them with structured spec extraction,
a SQLite database layer, and an interactive dashboard.

---

## 📦 Requirements

```
pandas>=2.0.0
numpy>=1.24.0
streamlit>=1.35.0
plotly>=5.18.0
openpyxl>=3.1.0
```

Install with:
```bash
pip install -r requirements.txt
```

---

## 📄 Data Source

- **Provider:** Central de Compras Públicas — Perú Compras
- **Portal:** https://www.catalogos.perucompras.gob.pe/ConsultaOrdenesPub
- **Standard:** OCDS (Open Contracting Data Standard)
- **License:** Open government data — free to use and redistribute
- **Dictionary:** [Diccionario de Datos v2.0](https://saeusceprod01.blob.core.windows.net/contproveedor/Documentos/Publico/Diccionario_de_Datos_Abiertos.pdf)

---

## 📬 Contact

**Your Name** — Data Engineer · Electronics Engineer 

- LinkedIn: [linkedin.com/in/kimberly-orozco-retana/](https://linkedin.com/in/your-profile)
- GitHub: [github.com/asdkimberly](https://github.com/asdkimberly)
- Email: asdkimberlyasd@gmail.com

---

_Built as part of a Data Engineering portfolio. Feedback welcome!_
