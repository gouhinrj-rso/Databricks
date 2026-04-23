"""PANDA Case History page — lg_panda_hist_sil deep dive."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_panda, sidebar_filters
from lib.ui import (
    PLOTLY_TEMPLATE,
    PRIMARY,
    download_button,
    format_float,
    format_int,
    histogram,
    metric_row,
    page_chrome,
    top_n_bar,
)

page_chrome(
    "PANDA Case History",
    "Status, failure modes, cycle time, and schedule slip derived from "
    "lg_panda_hist_sil.",
)

filters = sidebar_filters()
panda = apply_to_panda(data_mod.load_panda(), filters)

if panda.empty:
    st.warning("No PANDA cases match the current filter selection.")
    st.stop()

cycle = panda.get("case_cycle_days", pd.Series(dtype="float"))
open_cases = (
    panda["Current_Case_Status"].str.contains("Open", case=False, na=False).sum()
    if "Current_Case_Status" in panda
    else 0
)
closed_cases = (
    panda["Current_Case_Status"].str.contains("Closed", case=False, na=False).sum()
    if "Current_Case_Status" in panda
    else 0
)

metric_row([
    ("Cases", format_int(panda["SBA_Case_ID"].nunique() if "SBA_Case_ID" in panda else len(panda)), None),
    ("Open", format_int(open_cases), None),
    ("Closed", format_int(closed_cases), None),
    ("Median cycle (days)", format_float(cycle.median(), 0), None),
    ("P90 cycle (days)", format_float(cycle.quantile(0.90), 0), None),
])

st.divider()

c1, c2 = st.columns(2)
with c1:
    if "Current_Case_Status" in panda:
        status = (
            panda["Current_Case_Status"].fillna("(unknown)").value_counts().reset_index()
        )
        status.columns = ["Status", "Cases"]
        fig = px.pie(
            status,
            names="Status",
            values="Cases",
            template=PLOTLY_TEMPLATE,
            title="Case status distribution",
            hole=0.5,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)
with c2:
    top_n_bar(
        panda.get("Failure_Mode", pd.Series(dtype="object")),
        n=15,
        title="Top failure modes",
        x_label="Failure mode",
        y_label="Cases",
    )

st.divider()

st.subheader("Cases opened per month")
if "Case_Open_Date" in panda and panda["Case_Open_Date"].notna().any():
    monthly = (
        panda.dropna(subset=["Case_Open_Date"])
        .set_index("Case_Open_Date")
        .resample("MS")
        .size()
        .reset_index()
    )
    monthly.columns = ["Month", "Cases"]
    fig = px.line(
        monthly,
        x="Month",
        y="Cases",
        markers=True,
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY],
    )
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Schedule adherence (days)")
st.caption(
    "Derived from MX_Completed_Date minus the relevant planned/recommended "
    "date. Negative = completed ahead of plan, positive = slip."
)
cols = st.columns(3)
metrics = [
    ("Days_Sched_to_Comp", "vs. scheduled", cols[0]),
    ("Days_Rec_to_Comp", "vs. recommended", cols[1]),
    ("Days_AntSched_to_Comp", "vs. anticipated", cols[2]),
]
for col_name, label, col in metrics:
    with col:
        if col_name in panda and panda[col_name].notna().any():
            histogram(
                panda[col_name],
                title=label,
                x_label="Days (completed − planned)",
                bins=40,
                clip_quantile=0.98,
            )
        else:
            st.info(f"No {col_name} values in the current selection.")

st.divider()

c3, c4 = st.columns(2)
with c3:
    histogram(
        cycle,
        title="Case cycle time (days, Open → Completed)",
        x_label="Days",
        bins=40,
    )
with c4:
    if {"Failure_Mode", "case_cycle_days"}.issubset(panda.columns):
        top_failures = (
            panda["Failure_Mode"].dropna().astype("string").value_counts().head(10).index
        )
        subset = panda[panda["Failure_Mode"].astype("string").isin(top_failures)].copy()
        if not subset.empty:
            fig = px.box(
                subset,
                x="Failure_Mode",
                y="case_cycle_days",
                template=PLOTLY_TEMPLATE,
                title="Cycle time by failure mode (top 10)",
                color_discrete_sequence=[PRIMARY],
            )
            fig.update_layout(
                xaxis_tickangle=-35,
                margin=dict(l=10, r=10, t=60, b=10),
                yaxis_title="Days",
            )
            st.plotly_chart(fig, use_container_width=True)

with st.expander("Browse raw PANDA cases"):
    st.dataframe(panda.head(1000), use_container_width=True, hide_index=True)
    download_button(panda, "panda_cases_filtered.csv")
