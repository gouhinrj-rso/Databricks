"""CBM Events page — cbm_mx_event deep dive."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_events, sidebar_filters
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
    "CBM Events",
    "Deep dive on cbm_mx_event: duration, equipment, and monthly cadence.",
)

filters = sidebar_filters()

events = apply_to_events(data_mod.load_events(), filters)

if events.empty:
    st.warning("No events match the current filter selection.")
    st.stop()

duration = events.get("event_duration_hrs", pd.Series(dtype="float"))
metric_row([
    ("Events", format_int(events["work_order_number"].nunique() if "work_order_number" in events else len(events)), None),
    ("Unique tails", format_int(events["aircraft_id"].nunique() if "aircraft_id" in events else 0), None),
    ("Avg duration (hrs)", format_float(duration.mean()), None),
    ("Median duration (hrs)", format_float(duration.median()), None),
    ("P95 duration (hrs)", format_float(duration.quantile(0.95)), None),
])

st.divider()

c1, c2 = st.columns(2)
with c1:
    top_n_bar(
        events.get("equipment_designator", pd.Series(dtype="object")),
        n=15,
        title="Top equipment designators",
        x_label="Equipment designator",
        y_label="Events",
    )
with c2:
    histogram(
        duration,
        title="Event duration (hrs)",
        x_label="Duration (hrs)",
        bins=40,
    )

st.divider()

st.subheader("Events per month")
if "start_date" in events and events["start_date"].notna().any():
    monthly = (
        events.dropna(subset=["start_date"])
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

st.divider()

st.subheader("Top tails by event count")
if "aircraft_id" in events:
    per_tail = (
        events.groupby("aircraft_id")
        .agg(
            events=("work_order_number", "nunique")
            if "work_order_number" in events
            else ("aircraft_id", "size"),
            avg_duration_hrs=("event_duration_hrs", "mean")
            if "event_duration_hrs" in events
            else ("aircraft_id", "size"),
        )
        .sort_values("events", ascending=False)
        .head(20)
        .reset_index()
    )
    st.dataframe(per_tail, use_container_width=True, hide_index=True)

with st.expander("Browse raw events"):
    st.dataframe(events.head(1000), use_container_width=True, hide_index=True)
    download_button(events, "cbm_events_filtered.csv")
