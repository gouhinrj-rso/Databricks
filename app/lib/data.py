"""Data access layer.

Supports three runtimes, in preference order:

1. Databricks Apps / interactive Databricks notebook with a SQL warehouse
   reachable via ``databricks-sql-connector``. Authenticates with the
   app's service principal OAuth token when available, falling back to
   ``DATABRICKS_TOKEN``.
2. A live ``SparkSession`` (e.g. running inside a Databricks notebook)
   — falls back to reading the Unity Catalog tables directly via Spark.
3. Local CSVs at the paths defined in :mod:`lib.config` for offline
   development.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

import pandas as pd
import streamlit as st

from .config import CSV_FALLBACKS, TableConfig, load_table_config, load_warehouse_config

# --- Columns we know exist in each dataset (from the analyst's notebooks) ---
EVENT_DATE_COLS = ["start_date", "stop_date"]
ACTION_DATE_COLS = ["start_date", "stop_date"]
PANDA_DATE_COLS = [
    "Case_Open_Date",
    "Closed_AutoClosed_Date",
    "MX_Scheduled_Date",
    "MX_Recommended_Date",
    "MX_Completed_Date",
    "JCN_Ant_Scheduled_MX_Date",
]


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------
def _databricks_token() -> Optional[str]:
    """Return the service-principal OAuth token when running as a
    Databricks App, or a PAT from ``DATABRICKS_TOKEN`` for local dev."""
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        return token
    try:
        from databricks.sdk.core import Config

        cfg = Config()
        auth = cfg.authenticate()
        bearer = auth.get("Authorization", "")
        if bearer.startswith("Bearer "):
            return bearer.split(" ", 1)[1]
    except Exception:
        return None
    return None


@lru_cache(maxsize=1)
def _sql_connection():
    wh = load_warehouse_config()
    if not wh.is_configured:
        return None
    token = _databricks_token()
    if not token:
        return None
    try:
        from databricks import sql as dbsql
    except ImportError:
        return None
    return dbsql.connect(
        server_hostname=wh.server_hostname,
        http_path=wh.http_path,
        access_token=token,
    )


def _try_spark():
    try:
        from pyspark.sql import SparkSession  # type: ignore

        return SparkSession.getActiveSession()
    except Exception:
        return None


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL against a warehouse or Spark, whichever is reachable."""
    conn = _sql_connection()
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()
    spark = _try_spark()
    if spark is not None:
        return spark.sql(query).toPandas()
    raise RuntimeError(
        "No Databricks SQL warehouse or active SparkSession reachable. "
        "Set DATABRICKS_WAREHOUSE_ID + DATABRICKS_SERVER_HOSTNAME "
        "(and DATABRICKS_TOKEN for local dev) or run the app inside "
        "a Databricks notebook."
    )


def _load_table(fq_name: str, csv_fallback: str, parse_dates: list[str]) -> pd.DataFrame:
    """Read a Unity Catalog table if reachable, else CSV on disk."""
    try:
        df = run_query(f"SELECT * FROM {fq_name}")
        # Some warehouses return strings for TIMESTAMP; normalize.
        for col in parse_dates:
            if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    except Exception as warehouse_err:
        if os.path.exists(csv_fallback):
            return pd.read_csv(csv_fallback, parse_dates=parse_dates)
        raise warehouse_err


# ---------------------------------------------------------------------------
# Date normalization (mirrors SBA_Case_Hist_DateDiff_Calc.py)
# ---------------------------------------------------------------------------
_TWO_DIGIT_YEAR_RE = r"^(\d{1,2}/\d{1,2}/)(\d{2})( \d{1,2}:\d{2})$"


def _normalize_panda_dates(panda: pd.DataFrame) -> pd.DataFrame:
    """PANDA dumps sometimes carry ``M/d/yy H:mm`` strings.

    We pad 2-digit years to 4-digit (20yy) before coercing to datetime,
    matching the Spark date-diff helper the analyst already uses.
    """
    df = panda.copy()
    for col in PANDA_DATE_COLS:
        if col not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        s = df[col].astype("string")
        padded = s.str.replace(_TWO_DIGIT_YEAR_RE, r"\g<1>20\g<2>\g<3>", regex=True)
        df[col] = pd.to_datetime(padded, errors="coerce")
    if "SERIAL_NUMBER" in df.columns:
        df["SERIAL_NUMBER"] = df["SERIAL_NUMBER"].astype("string")
    return df


def _add_panda_date_diffs(df: pd.DataFrame) -> pd.DataFrame:
    if {"Closed_AutoClosed_Date", "Case_Open_Date"}.issubset(df.columns):
        df["Days_Open_to_Close"] = (
            df["Closed_AutoClosed_Date"] - df["Case_Open_Date"]
        ).dt.days
    if {"MX_Completed_Date", "MX_Scheduled_Date"}.issubset(df.columns):
        df["Days_Sched_to_Comp"] = (
            df["MX_Completed_Date"] - df["MX_Scheduled_Date"]
        ).dt.days
    if {"MX_Completed_Date", "MX_Recommended_Date"}.issubset(df.columns):
        df["Days_Rec_to_Comp"] = (
            df["MX_Completed_Date"] - df["MX_Recommended_Date"]
        ).dt.days
    if {"MX_Completed_Date", "JCN_Ant_Scheduled_MX_Date"}.issubset(df.columns):
        df["Days_AntSched_to_Comp"] = (
            df["MX_Completed_Date"] - df["JCN_Ant_Scheduled_MX_Date"]
        ).dt.days
    if {"MX_Completed_Date", "Case_Open_Date"}.issubset(df.columns):
        df["case_cycle_days"] = (
            df["MX_Completed_Date"] - df["Case_Open_Date"]
        ).dt.days
    return df


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60 * 30, show_spinner="Loading CBM event data…")
def load_events(_cfg: TableConfig | None = None) -> pd.DataFrame:
    cfg = _cfg or load_table_config()
    df = _load_table(cfg.fq_event, CSV_FALLBACKS["event"], EVENT_DATE_COLS)
    if {"start_date", "stop_date"}.issubset(df.columns):
        df["event_duration_hrs"] = (
            df["stop_date"] - df["start_date"]
        ).dt.total_seconds() / 3600
    if {"serial_number", "job_control_number"}.issubset(df.columns):
        df["SN_JCN_PK"] = (
            df["serial_number"].astype("string").str.strip()
            + " "
            + df["job_control_number"].astype("string").str.strip()
        )
    return df


@st.cache_data(ttl=60 * 30, show_spinner="Loading CBM action data…")
def load_actions(_cfg: TableConfig | None = None) -> pd.DataFrame:
    cfg = _cfg or load_table_config()
    df = _load_table(cfg.fq_action, CSV_FALLBACKS["action"], ACTION_DATE_COLS)
    if {"start_date", "stop_date"}.issubset(df.columns):
        df["action_duration_hrs"] = (
            df["stop_date"] - df["start_date"]
        ).dt.total_seconds() / 3600
    return df


@st.cache_data(ttl=60 * 30, show_spinner="Loading PANDA case history…")
def load_panda(_cfg: TableConfig | None = None) -> pd.DataFrame:
    cfg = _cfg or load_table_config()
    df = _load_table(cfg.fq_panda, CSV_FALLBACKS["panda"], PANDA_DATE_COLS)
    df = _normalize_panda_dates(df)
    df = _add_panda_date_diffs(df)
    if {"SERIAL_NUMBER", "SBA_JCN"}.issubset(df.columns):
        df["SN_JCN_PK"] = (
            df["SERIAL_NUMBER"].astype("string").str.strip()
            + " "
            + df["SBA_JCN"].astype("string").str.strip()
        )
    return df


@st.cache_data(ttl=60 * 30, show_spinner="Joining CBM + PANDA…")
def load_linked(_cfg: TableConfig | None = None) -> pd.DataFrame:
    """Build the unified event↔action↔PANDA table used by the Deep Dive."""
    cfg = _cfg or load_table_config()
    events = load_events(cfg)
    actions = load_actions(cfg)
    panda = load_panda(cfg)
    if "work_order_number" not in events.columns or "work_order_number" not in actions.columns:
        raise RuntimeError(
            "work_order_number must be present in both event and action tables."
        )
    joined = events.merge(
        actions,
        on="work_order_number",
        suffixes=("_event", "_action"),
        how="left",
    )
    linked = joined.merge(panda, on="SN_JCN_PK", how="left", suffixes=("", "_panda"))
    if {"start_date_event", "stop_date_event"}.issubset(linked.columns):
        linked["event_duration_hrs"] = (
            linked["stop_date_event"] - linked["start_date_event"]
        ).dt.total_seconds() / 3600
    if {"start_date_action", "stop_date_action"}.issubset(linked.columns):
        linked["action_duration_hrs"] = (
            linked["stop_date_action"] - linked["start_date_action"]
        ).dt.total_seconds() / 3600
    return linked


def connection_status() -> dict:
    """Describe which backend is active — surfaced in the sidebar."""
    wh = load_warehouse_config()
    status = {
        "mode": "csv",
        "detail": "Local CSV fallback",
    }
    if _sql_connection() is not None:
        status = {
            "mode": "warehouse",
            "detail": f"SQL warehouse @ {wh.server_hostname}",
        }
    elif _try_spark() is not None:
        status = {"mode": "spark", "detail": "Active SparkSession"}
    return status
