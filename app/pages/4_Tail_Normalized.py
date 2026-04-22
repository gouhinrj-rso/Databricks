"""Tail-normalized metrics — per aircraft intensity of events and actions."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from lib import data as data_mod
from lib.filters import apply_to_linked, sidebar_filters
from lib.ui import (
    PLOTLY_TEMPLATE,
    PRIMARY,
    download_button,
    format_int,
    metric_row,
    page_chrome,
)

page_chrome(
    "Tail-Normalized Metrics",
    "Per-aircraft rollup of event counts, action counts, and mean action "
    "duration — ranked to surface high-touch tails.",
)

filters = sidebar_filters()

try:
    linked = data_mod.load_linked()
except Exception as err:
    st.error(f"Unable to build linked event↔action↔PANDA table: {err}")
    st.stop()

linked = apply_to_linked(linked, filters)
if "aircraft_id" not in linked or linked.empty:
    st.warning("No aircraft_id values available for the current filter selection.")
    st.stop()

agg_kwargs = {}
if "work_order_number" in linked:
    agg_kwargs["event_count"] = ("work_order_number", "nunique")
if "maintenance_action_number" in linked:
    agg_kwargs["action_count"] = ("maintenance_action_number", "nunique")
if "action_duration_hrs" in linked:
    agg_kwargs["avg_action_hrs"] = ("action_duration_hrs", "mean")
if "event_duration_hrs" in linked:
    agg_kwargs["avg_event_hrs"] = ("event_duration_hrs", "mean")
if "SBA_Case_ID" in linked:
    agg_kwargs["panda_cases"] = ("SBA_Case_ID", "nunique")

summary = (
    linked.groupby("aircraft_id")
    .agg(**agg_kwargs)
    .reset_index()
    .sort_values(list(agg_kwargs.keys())[0] if agg_kwargs else "aircraft_id", ascending=False)
)

metric_row([
    ("Tails in view", format_int(summary["aircraft_id"].nunique()), None),
    ("Total events", format_int(summary.get("event_count", pd.Series(dtype=int)).sum()), None),
    ("Total actions", format_int(summary.get("action_count", pd.Series(dtype=int)).sum()), None),
    ("Total PANDA cases", format_int(summary.get("panda_cases", pd.Series(dtype=int)).sum()), None),
])

st.divider()

top_n = st.slider("Show top N tails", min_value=5, max_value=50, value=15, step=5)
ranked = summary.head(top_n)

c1, c2 = st.columns(2)
with c1:
    if "event_count" in ranked:
        fig = px.bar(
            ranked,
            x="aircraft_id",
            y="event_count",
            title=f"Top {top_n} tails by event count",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(xaxis_tickangle=-35, margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)
with c2:
    if "avg_action_hrs" in ranked:
        fig = px.bar(
            ranked.sort_values("avg_action_hrs", ascending=False),
            x="aircraft_id",
            y="avg_action_hrs",
            title=f"Top {top_n} tails by mean action duration (hrs)",
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[PRIMARY],
        )
        fig.update_layout(xaxis_tickangle=-35, margin=dict(l=10, r=10, t=60, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Per-aircraft summary")
st.dataframe(summary, use_container_width=True, hide_index=True)
download_button(summary, "tail_normalized_metrics.csv")
