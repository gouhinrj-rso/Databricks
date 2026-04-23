"""Linked Deep Dive — the full event↔action↔PANDA joined table.

Supports free-text search, column selection, and CSV export so the
analyst can pivot between the dashboard and raw rows without dropping
into a notebook.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_linked, sidebar_filters
from lib.ui import download_button, format_float, format_int, metric_row, page_chrome

page_chrome(
    "Linked Deep Dive",
    "cbm_mx_event ⨝ cbm_mx_action ⨝ PANDA (lg_panda_hist_sil) — searchable and exportable.",
)

filters = sidebar_filters()

try:
    linked = data_mod.load_linked()
except Exception as err:
    st.error(f"Unable to build linked table: {err}")
    st.stop()

linked = apply_to_linked(linked, filters)

if linked.empty:
    st.warning("Join produced zero rows for the current filters.")
    st.stop()

metric_row([
    ("Joined rows", format_int(len(linked)), None),
    (
        "Unique tails",
        format_int(linked["aircraft_id"].nunique() if "aircraft_id" in linked else 0),
        None,
    ),
    (
        "Unique PANDA cases",
        format_int(linked["SBA_Case_ID"].nunique() if "SBA_Case_ID" in linked else 0),
        None,
    ),
    (
        "Avg event duration (hrs)",
        format_float(
            linked["event_duration_hrs"].mean()
            if "event_duration_hrs" in linked
            else None
        ),
        None,
    ),
    (
        "Avg action duration (hrs)",
        format_float(
            linked["action_duration_hrs"].mean()
            if "action_duration_hrs" in linked
            else None
        ),
        None,
    ),
])

st.divider()

search = st.text_input(
    "Search across all string columns",
    placeholder="Tail number, WO, serial, failure mode, free text…",
).strip()

preferred_cols = [
    "aircraft_id",
    "work_order_number",
    "maintenance_action_number",
    "equipment_designator",
    "serial_number",
    "job_control_number",
    "SN_JCN_PK",
    "start_date_event",
    "stop_date_event",
    "event_duration_hrs",
    "start_date_action",
    "stop_date_action",
    "action_duration_hrs",
    "action_taken_code",
    "how_malfunction_code",
    "SBA_Case_ID",
    "SBA_JCN",
    "Current_Case_Status",
    "Failure_Mode",
    "Case_Open_Date",
    "MX_Completed_Date",
    "case_cycle_days",
    "Days_Sched_to_Comp",
    "Days_Rec_to_Comp",
]
default_cols = [c for c in preferred_cols if c in linked.columns]

cols = st.multiselect(
    "Columns to display",
    options=list(linked.columns),
    default=default_cols or list(linked.columns)[:15],
)

view = linked[cols] if cols else linked

if search:
    pattern = pd.Series(False, index=view.index)
    for c in view.columns:
        series = view[c]
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            pattern = pattern | series.astype("string").str.contains(
                search, case=False, na=False
            )
    view = view[pattern]

limit = st.slider(
    "Rows to display",
    min_value=100,
    max_value=10_000,
    value=1_000,
    step=100,
)

st.caption(f"Showing {min(limit, len(view)):,} of {len(view):,} joined rows.")
st.dataframe(view.head(limit), use_container_width=True, hide_index=True)

download_button(view, "cbm_linked_deep_dive.csv", label="Download filtered CSV")

with st.expander("Schema hints"):
    st.markdown(
        """
        The join uses:

        - ``work_order_number`` to link **events → actions**
        - ``SN_JCN_PK = serial_number + ' ' + job_control_number`` to link
          **events → PANDA** (paired with PANDA's
          ``SERIAL_NUMBER + ' ' + SBA_JCN``)

        Derived columns added during load:

        - ``event_duration_hrs``, ``action_duration_hrs``
        - ``case_cycle_days``, ``Days_Open_to_Close``, ``Days_Sched_to_Comp``,
          ``Days_Rec_to_Comp``, ``Days_AntSched_to_Comp``
        """
    )
