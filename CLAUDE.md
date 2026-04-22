# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

A collection of Databricks notebooks and PySpark scripts for exploratory analysis of Army aviation maintenance data. The core analytical flow links **CBM+ maintenance events / actions** with **PANDA case history** to produce descriptive stats, tail-normalized (per-aircraft) metrics, and time-series visuals.

There is no build system, test suite, package manifest, or dependency file — every file is run inside a Databricks workspace (or an equivalent PySpark + pandas environment). `pandas`, `numpy`, `matplotlib`, `plotly`, and `pyspark` are assumed present from the Databricks runtime.

## File format conventions

Files in the repo root without a `.py` extension are **Databricks notebook source exports**, not plain Python:

- Cell boundaries are `# COMMAND ----------`.
- Cell language is declared by `# %md` (markdown) or `# %python` at the top of the cell.
- `display(...)`, `spark`, and `dbutils` are Databricks built-ins — do not try to import them.
- When editing these files, preserve the `# COMMAND ----------` / `# %md` / `# %python` markers exactly; Databricks relies on them to split cells on re-import.

`.py` files (currently only `SBA_Case_Hist_DateDiff_Calc.py`) are PySpark scripts meant to be pasted into or `%run` from a Databricks cell — they still reference `spark` / `display` / a pre-existing `df` without importing them.

## Cross-file data dependencies

Several notebooks are **fragments** that assume variables from an earlier cell/notebook already exist in the session. Do not treat them as standalone:

- `Notebook_Template` is the entry point: it loads the three CSVs, builds the join, and defines `cbm_event`, `cbm_action`, `panda`, and `full`.
- `PANDA Case History Deep Dive`, `Tail-Normalized Metrics`, and `Trend Analysis by Aircraft` all depend on `full`, `panda`, `summary`, or `plt` being defined upstream. Running them in isolation will raise `NameError`.
- `DeepDive Matplotlib` is the one self-contained analysis notebook — it re-loads the CSVs itself.

## Canonical join key

Linking CBM+ to PANDA is always done through a composite key called `SN_JCN_PK`:

```python
cbm_event["SN_JCN_PK"] = cbm_event["serial_number"].astype(str) + " " + cbm_event["job_control_number"].astype(str)
panda["SN_JCN_PK"]     = panda["SERIAL_NUMBER"].astype(str)   + " " + panda["SBA_JCN"].astype(str)
```

Note the column-name casing difference between datasets (`serial_number` vs `SERIAL_NUMBER`, `job_control_number` vs `SBA_JCN`) — both sides must be cast to string and space-concatenated in that order. New code that joins these sources should reuse this key rather than inventing a new one.

## Data sources

Notebooks read from `/mnt/data/*.csv` on DBFS:
- `cbm_mx_event.csv` — maintenance events (has `start_date`, `stop_date`, `work_order_number`, `equipment_designator`, `aircraft_id`, `serial_number`, `job_control_number`)
- `cbm_mx_action.csv` — maintenance actions (joins to events on `work_order_number`; has `action_taken_code`, `how_malfunction_code`, `maintenance_action_number`)
- `lg_panda_hist_sil.csv` — PANDA silver-layer case history (has `Case_Open_Date`, `MX_Completed_Date`, `Current_Case_Status`, `Failure_Mode`, `SBA_Case_ID`, `SERIAL_NUMBER`, `SBA_JCN`)

When the same data is registered as Delta tables instead, paths should be replaced with `spark.table("...")` calls; the default catalog in use is Unity Catalog schema `bld_a4l_restricted_analytics` (see `DBFS_S3_MNT Scan`).

## PANDA date handling

PANDA date columns arrive as strings in format `M/d/yyyy H:mm` with **two-digit years** that must be expanded to four digits before `try_to_timestamp` will parse them. `SBA_Case_Hist_DateDiff_Calc.py` is the reference implementation — reuse its regex-expand-then-`try_to_timestamp` pattern (not `to_timestamp`, which raises instead of returning null on bad rows) when introducing new date columns. The known PANDA date columns are:

`Case_Open_Date`, `Closed_AutoClosed_Date`, `MX_Scheduled_Date`, `MX_Recommended_Date`, `MX_Completed_Date`, `JCN_Ant_Scheduled_MX_Date`.

## Storage-location inventory

`DBFS_S3_MNT Scan` is a utility, not part of the analysis pipeline. It walks `DESCRIBE DETAIL` over tables to map out physical storage (S3 / ADLS / DBFS). Toggle `FULL_INVENTORY = True` to sweep all catalogs/schemas, or leave `False` and set `TARGET_SCHEMA` for a single-schema scan. Output CSV is written to `/dbfs/FileStore/tables/table_locations.csv`.

## Running and testing

There is no local test harness. To exercise changes:

1. Import the file into a Databricks workspace (the `# COMMAND ----------` markers cause it to split into cells).
2. Attach to a cluster with the standard ML/Data Science runtime.
3. Run cells top-to-bottom — downstream notebooks like `Tail-Normalized Metrics` need the `Notebook_Template` cells to have already executed in the same session.

## Git workflow

Branch for documentation/feature work is `claude/add-claude-documentation-Mvirj` (per task instructions in this session). Commit messages in history follow a short imperative style ("Add ...", "Implement ...", "Create ...") — match that style.
