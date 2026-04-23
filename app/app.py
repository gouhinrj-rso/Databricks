"""CBM+ Analyst Workbench — main entrypoint.

A Streamlit multi-page app that packages the analyst's CBM+ / PANDA
deep-dive notebooks into one interactive workbench. Designed to run as
a Databricks App, but also runs locally against CSV fallbacks.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import data as data_mod
from lib.filters import (
    apply_to_events,
    apply_to_panda,
    sidebar_filters,
)
from lib.ui import format_float, format_int, metric_row, page_chrome, PLOTLY_TEMPLATE, PRIMARY

page_chrome(
    "Executive Overview",
    "CBM+ maintenance events, actions and PANDA case history in one place.",
)

filters = sidebar_filters()

try:
    events = data_mod.load_events()
    actions = data_mod.load_actions()
    panda = data_mod.load_panda()
except Exception as err:
    st.error(f"Unable to load source data: {err}")
    st.stop()

events_f = apply_to_events(events, filters)
panda_f = apply_to_panda(panda, filters)

# --- KPI row ------------------------------------------------------------
unique_tails = events_f["aircraft_id"].nunique() if "aircraft_id" in events_f else None
event_count = events_f["work_order_number"].nunique() if "work_order_number" in events_f else len(events_f)
action_count = (
    actions["maintenance_action_number"].nunique()
    if "maintenance_action_number" in actions
    else len(actions)
)
avg_event_hrs = (
    events_f["event_duration_hrs"].mean() if "event_duration_hrs" in events_f else None
)
open_cases = (
    panda_f["Current_Case_Status"].str.contains("Open", case=False, na=False).sum()
    if "Current_Case_Status" in panda_f
    else None
)
median_cycle = (
    panda_f["case_cycle_days"].median() if "case_cycle_days" in panda_f else None
)

metric_row([
    ("Aircraft (tails)", format_int(unique_tails), None),
    ("CBM events", format_int(event_count), None),
    ("CBM actions", format_int(action_count), None),
    ("Avg event duration (hrs)", format_float(avg_event_hrs), None),
    ("Open PANDA cases", format_int(open_cases), None),
    ("Median case cycle (days)", format_float(median_cycle, 0), None),
])

st.divider()

left, right = st.columns([3, 2])

with left:
    st.subheader("Events per month")
    if "start_date" in events_f and events_f["start_date"].notna().any():
        monthly = (
            events_f.dropna(subset=["start_date"])
            .set_index("start_date")
            .resample("MS")["work_order_number"]
            .nunique()
            .reset_index()
            .rename(columns={"start_date": "Month", "work_order_number": "Events"})
        )
        fig = px.line(
            monthly,
            x="Month",
            y="Events",
            markers=True,
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No start_date column available to chart events over time.")

with right:
    st.subheader("PANDA case status")
    if "Current_Case_Status" in panda_f and not panda_f.empty:
        status = (
            panda_f["Current_Case_Status"]
            .fillna("(unknown)")
            .value_counts()
            .reset_index()
        )
        status.columns = ["Status", "Cases"]
        fig = px.pie(
            status,
            names="Status",
            values="Cases",
            template=PLOTLY_TEMPLATE,
            hole=0.5,
        )
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No PANDA case status data available for the current filters.")

st.divider()

bottom_left, bottom_right = st.columns(2)
with bottom_left:
    st.subheader("Top equipment designators (events)")
    if "equipment_designator" in events_f and not events_f.empty:
        top_eq = (
            events_f["equipment_designator"]
            .dropna()
            .astype("string")
            .value_counts()
            .head(10)
            .reset_index()
        )
        top_eq.columns = ["Equipment", "Events"]
        fig = px.bar(
            top_eq,
            x="Equipment",
            y="Events",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(xaxis_tickangle=-35, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No equipment_designator data for the current filters.")

with bottom_right:
    st.subheader("Top failure modes (PANDA)")
    if "Failure_Mode" in panda_f and not panda_f.empty:
        fm = (
            panda_f["Failure_Mode"]
            .dropna()
            .astype("string")
            .value_counts()
            .head(10)
            .reset_index()
        )
        fm.columns = ["Failure Mode", "Cases"]
        fig = px.bar(
            fm,
            x="Failure Mode",
            y="Cases",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(xaxis_tickangle=-35, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Failure_Mode data for the current filters.")

with st.expander("About this app"):
    st.markdown(
        """
        **CBM+ Analyst Workbench** — packages the deep-dive notebooks for
        `cbm_mx_event`, `cbm_mx_action`, and `lg_panda_hist_sil` into an
        interactive Streamlit app you can deploy as a Databricks App.

        Use the left sidebar to scope every page to the aircraft, equipment,
        failure modes, case statuses and date range you care about. Pages:

        - **CBM Events** — duration distributions, top equipment, monthly trend.
        - **CBM Actions** — action-taken and malfunction-code breakdowns.
        - **PANDA Cases** — status, failure modes, cycle time, schedule slip.
        - **Tail-Normalized** — per-aircraft event & action intensity.
        - **Trends by Aircraft** — side-by-side tail trendlines.
        - **Linked Deep Dive** — the full joined table with search and export.
        """
    )
