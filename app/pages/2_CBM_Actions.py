"""CBM Actions page — cbm_mx_action deep dive."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_actions, sidebar_filters
from lib.ui import (
    download_button,
    format_float,
    format_int,
    histogram,
    metric_row,
    page_chrome,
    top_n_bar,
)

page_chrome(
    "CBM Actions",
    "Deep dive on cbm_mx_action: action-taken codes and malfunction codes.",
)

filters = sidebar_filters()
actions = apply_to_actions(data_mod.load_actions(), filters)

if actions.empty:
    st.warning("No actions match the current filter selection.")
    st.stop()

duration = actions.get("action_duration_hrs", pd.Series(dtype="float"))
metric_row([
    (
        "Actions",
        format_int(
            actions["maintenance_action_number"].nunique()
            if "maintenance_action_number" in actions
            else len(actions)
        ),
        None,
    ),
    (
        "Distinct WOs",
        format_int(
            actions["work_order_number"].nunique()
            if "work_order_number" in actions
            else 0
        ),
        None,
    ),
    ("Avg duration (hrs)", format_float(duration.mean()), None),
    ("Median duration (hrs)", format_float(duration.median()), None),
    ("P95 duration (hrs)", format_float(duration.quantile(0.95)), None),
])

st.divider()

c1, c2 = st.columns(2)
with c1:
    top_n_bar(
        actions.get("action_taken_code", pd.Series(dtype="object")),
        n=15,
        title="Top action-taken codes",
        x_label="Action-taken code",
        y_label="Actions",
    )
with c2:
    top_n_bar(
        actions.get("how_malfunction_code", pd.Series(dtype="object")),
        n=15,
        title="Top how-malfunction codes",
        x_label="How-malfunction code",
        y_label="Actions",
    )

st.divider()

c3, c4 = st.columns(2)
with c3:
    histogram(
        duration,
        title="Action duration (hrs)",
        x_label="Duration (hrs)",
        bins=40,
    )
with c4:
    if {"action_taken_code", "action_duration_hrs"}.issubset(actions.columns):
        st.subheader("Median duration by action-taken code")
        top_codes = (
            actions["action_taken_code"]
            .dropna()
            .astype("string")
            .value_counts()
            .head(15)
            .index
        )
        subset = actions[actions["action_taken_code"].astype("string").isin(top_codes)]
        summary = (
            subset.groupby("action_taken_code")["action_duration_hrs"]
            .agg(["count", "median", "mean"])
            .reset_index()
            .sort_values("count", ascending=False)
            .rename(
                columns={
                    "count": "Actions",
                    "median": "Median hrs",
                    "mean": "Mean hrs",
                }
            )
        )
        st.dataframe(summary, use_container_width=True, hide_index=True)

with st.expander("Browse raw actions"):
    st.dataframe(actions.head(1000), use_container_width=True, hide_index=True)
    download_button(actions, "cbm_actions_filtered.csv")
