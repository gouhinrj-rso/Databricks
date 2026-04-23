"""Storage inventory — a Streamlit port of the DBFS/S3 MNT scan notebook.

Runs ``DESCRIBE DETAIL`` across a target schema (or every schema in every
catalog) via the SQL warehouse, so analysts can audit table locations,
formats and sizes without dropping into a notebook.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from lib import data as data_mod
from lib.config import load_table_config
from lib.ui import download_button, format_int, metric_row, page_chrome

page_chrome(
    "Storage Inventory",
    "Enumerate table storage locations (S3 / ADLS / DBFS / managed) via "
    "DESCRIBE DETAIL.",
)

cfg = load_table_config()

mode = st.radio(
    "Scan scope",
    options=["Current schema", "Full inventory (all catalogs)"],
    index=0,
    horizontal=True,
    help="Full inventory iterates every catalog × schema × table. "
    "On large metastores this can take several minutes.",
)

target_catalog = st.text_input("Catalog", value=cfg.catalog)
target_schema = st.text_input("Schema", value=cfg.schema)


def _describe_detail(fq: str) -> dict:
    try:
        df = data_mod.run_query(f"DESCRIBE DETAIL {fq}")
        if df.empty:
            return {"table": fq, "error": "empty DESCRIBE DETAIL result"}
        row = df.iloc[0].to_dict()
        return {
            "catalog": row.get("catalogName"),
            "schema": row.get("schemaName"),
            "table": row.get("name") or fq,
            "table_type": row.get("type"),
            "format": row.get("format"),
            "location": row.get("location"),
            "numFiles": row.get("numFiles"),
            "sizeInBytes": row.get("sizeInBytes"),
            "comment": row.get("description"),
            "error": None,
        }
    except Exception as err:
        return {"table": fq, "error": str(err)[:300]}


def scan() -> pd.DataFrame:
    rows: list[dict] = []
    if mode == "Full inventory (all catalogs)":
        catalogs = data_mod.run_query("SHOW CATALOGS")
        catalog_col = catalogs.columns[0]
        for cat in catalogs[catalog_col]:
            try:
                schemas = data_mod.run_query(f"SHOW SCHEMAS IN {cat}")
            except Exception as err:
                rows.append({"catalog": cat, "error": str(err)[:300]})
                continue
            schema_col = schemas.columns[0]
            for sch in schemas[schema_col]:
                try:
                    tables = data_mod.run_query(f"SHOW TABLES IN {cat}.{sch}")
                except Exception as err:
                    rows.append({"catalog": cat, "schema": sch, "error": str(err)[:300]})
                    continue
                for _, rec in tables.iterrows():
                    tbl = rec.get("tableName") or rec.get("name")
                    if not tbl:
                        continue
                    rows.append(_describe_detail(f"{cat}.{sch}.{tbl}"))
    else:
        try:
            tables = data_mod.run_query(
                f"SHOW TABLES IN {target_catalog}.{target_schema}"
            )
        except Exception as err:
            st.error(f"Could not list tables: {err}")
            return pd.DataFrame()
        for _, rec in tables.iterrows():
            tbl = rec.get("tableName") or rec.get("name")
            if not tbl:
                continue
            rows.append(_describe_detail(f"{target_catalog}.{target_schema}.{tbl}"))
    df = pd.DataFrame(rows)
    if "sizeInBytes" in df:
        df["sizeMB"] = pd.to_numeric(df["sizeInBytes"], errors="coerce") / (1024 * 1024)
    return df


if st.button("Run scan", type="primary"):
    with st.spinner("Running DESCRIBE DETAIL across target scope…"):
        result = scan()
    if result.empty:
        st.warning("No tables discovered.")
    else:
        metric_row([
            ("Tables", format_int(len(result)), None),
            (
                "Successful",
                format_int(result["error"].isna().sum() if "error" in result else len(result)),
                None,
            ),
            (
                "Errors",
                format_int(result["error"].notna().sum() if "error" in result else 0),
                None,
            ),
            (
                "Total size (MB)",
                format_int(result["sizeMB"].sum() if "sizeMB" in result else 0),
                None,
            ),
        ])
        st.dataframe(result, use_container_width=True, hide_index=True)
        download_button(result, "table_locations.csv")
else:
    st.info("Choose a scope above and click **Run scan**.")
