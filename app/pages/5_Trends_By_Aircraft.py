"""Per-aircraft trend analysis — compare multiple tails side by side."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_events, sidebar_filters
from lib.ui import PLOTLY_TEMPLATE, format_int, metric_row, page_chrome

page_chrome(
    "Trends by Aircraft",
    "Monthly event cadence compared across selected tails.",
)

filters = sidebar_filters()
events = apply_to_events(data_mod.load_events(), filters)

if "aircraft_id" not in events or events.empty:
    st.warning("No events with aircraft_id available for the current filters.")
    st.stop()

rank = (
    events.groupby("aircraft_id")["work_order_number"]
    .nunique()
    .sort_values(ascending=False)
    .reset_index()
    .rename(columns={"work_order_number": "events"})
)

default_tails = rank["aircraft_id"].head(3).tolist()
if filters.aircraft_ids:
    default_tails = filters.aircraft_ids

selected = st.multiselect(
    "Tails to compare",
    options=rank["aircraft_id"].tolist(),
    default=default_tails,
    help="Defaults to the three busiest tails in the current filter scope.",
)

resample_rule = st.radio(
    "Time bucket",
    options=["Month", "Week", "Quarter"],
    horizontal=True,
    index=0,
)
rule_map = {"Month": "MS", "Week": "W-MON", "Quarter": "QS"}
rule = rule_map[resample_rule]

metric_row([
    ("Tails compared", format_int(len(selected)), None),
    ("Events in scope", format_int(events["work_order_number"].nunique() if "work_order_number" in events else len(events)), None),
    ("Busiest tail events", format_int(rank["events"].iloc[0] if not rank.empty else 0), None),
])

if not selected:
    st.info("Select at least one tail to chart.")
    st.stop()

focus = events[events["aircraft_id"].astype("string").isin(selected)].dropna(
    subset=["start_date"]
)
if focus.empty:
    st.warning("No events in the selected tails for this time window.")
    st.stop()

monthly = (
    focus.set_index("start_date")
    .groupby("aircraft_id")
    .resample(rule)["work_order_number"]
    .nunique()
    .reset_index()
    .rename(columns={"start_date": "Period", "work_order_number": "Events"})
)

fig = px.line(
    monthly,
    x="Period",
    y="Events",
    color="aircraft_id",
    markers=True,
    template=PLOTLY_TEMPLATE,
    title=f"Events per {resample_rule.lower()} by aircraft",
)
fig.update_layout(margin=dict(l=10, r=10, t=60, b=10))
st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Cumulative events")
cum = monthly.sort_values(["aircraft_id", "Period"]).copy()
cum["Cumulative"] = cum.groupby("aircraft_id")["Events"].cumsum()
fig2 = px.line(
    cum,
    x="Period",
    y="Cumulative",
    color="aircraft_id",
    template=PLOTLY_TEMPLATE,
    title="Cumulative events per aircraft",
)
fig2.update_layout(margin=dict(l=10, r=10, t=60, b=10))
st.plotly_chart(fig2, use_container_width=True)

with st.expander("Aircraft ranking"):
    st.dataframe(rank, use_container_width=True, hide_index=True)
