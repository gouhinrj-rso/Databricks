"""Shared sidebar filters.

All pages call :func:`sidebar_filters` and receive a :class:`FilterState`
that they can apply to their dataframes via :func:`apply_to_events`,
:func:`apply_to_actions`, :func:`apply_to_panda`, and
:func:`apply_to_linked`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Sequence

import pandas as pd
import streamlit as st

from . import data as data_mod


@dataclass
class FilterState:
    date_range: tuple[date, date] | None = None
    aircraft_ids: list[str] = field(default_factory=list)
    equipment: list[str] = field(default_factory=list)
    failure_modes: list[str] = field(default_factory=list)
    case_status: list[str] = field(default_factory=list)


def _sorted_unique(series: pd.Series) -> list[str]:
    if series is None or series.empty:
        return []
    return sorted(str(v) for v in series.dropna().unique())


def _default_date_range(events: pd.DataFrame, panda: pd.DataFrame) -> tuple[date, date]:
    dates: list[pd.Timestamp] = []
    if "start_date" in events and events["start_date"].notna().any():
        dates += [events["start_date"].min(), events["start_date"].max()]
    if "Case_Open_Date" in panda and panda["Case_Open_Date"].notna().any():
        dates += [panda["Case_Open_Date"].min(), panda["Case_Open_Date"].max()]
    if not dates:
        today = date.today()
        return today - timedelta(days=365), today
    lo = min(d for d in dates if pd.notna(d)).date()
    hi = max(d for d in dates if pd.notna(d)).date()
    if lo == hi:
        hi = hi + timedelta(days=1)
    return lo, hi


def sidebar_filters() -> FilterState:
    """Render the global filter sidebar and return the state.

    Reads the three core tables (cached) to populate the filter widgets
    with the actual values available in the data.
    """
    events = data_mod.load_events()
    actions = data_mod.load_actions()
    panda = data_mod.load_panda()

    st.sidebar.header("Filters")

    lo, hi = _default_date_range(events, panda)
    sel_lo, sel_hi = st.sidebar.date_input(
        "Date range",
        value=(lo, hi),
        min_value=lo,
        max_value=hi,
        help="Filters events by start_date and PANDA cases by Case_Open_Date.",
    )
    if isinstance(sel_lo, tuple):
        sel_lo, sel_hi = sel_lo

    aircraft_options = _sorted_unique(events.get("aircraft_id", pd.Series(dtype="object")))
    aircraft_sel = st.sidebar.multiselect(
        "Aircraft (tail)",
        options=aircraft_options,
        default=[],
        placeholder="All aircraft",
    )

    equipment_options = _sorted_unique(
        events.get("equipment_designator", pd.Series(dtype="object"))
    )
    equipment_sel = st.sidebar.multiselect(
        "Equipment designator",
        options=equipment_options,
        default=[],
        placeholder="All equipment",
    )

    failure_options = _sorted_unique(panda.get("Failure_Mode", pd.Series(dtype="object")))
    failure_sel = st.sidebar.multiselect(
        "Failure mode (PANDA)",
        options=failure_options,
        default=[],
        placeholder="All failure modes",
    )

    status_options = _sorted_unique(
        panda.get("Current_Case_Status", pd.Series(dtype="object"))
    )
    status_sel = st.sidebar.multiselect(
        "Case status (PANDA)",
        options=status_options,
        default=[],
        placeholder="All statuses",
    )

    st.sidebar.divider()
    status = data_mod.connection_status()
    badge = {"warehouse": "🟢", "spark": "🟡", "csv": "⚪"}[status["mode"]]
    st.sidebar.caption(f"Data source {badge} {status['detail']}")
    if st.sidebar.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    return FilterState(
        date_range=(sel_lo, sel_hi),
        aircraft_ids=aircraft_sel,
        equipment=equipment_sel,
        failure_modes=failure_sel,
        case_status=status_sel,
    )


def _mask_in(series: pd.Series, values: Sequence[str]) -> pd.Series:
    if not values:
        return pd.Series(True, index=series.index)
    return series.astype("string").isin(values)


def _mask_date(series: pd.Series, window: tuple[date, date] | None) -> pd.Series:
    if window is None:
        return pd.Series(True, index=series.index)
    lo, hi = window
    lo_ts = pd.Timestamp(lo)
    hi_ts = pd.Timestamp(hi) + pd.Timedelta(days=1)
    return series.between(lo_ts, hi_ts, inclusive="left") | series.isna()


def apply_to_events(df: pd.DataFrame, f: FilterState) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    if "start_date" in df:
        m &= _mask_date(df["start_date"], f.date_range)
    if "aircraft_id" in df:
        m &= _mask_in(df["aircraft_id"], f.aircraft_ids)
    if "equipment_designator" in df:
        m &= _mask_in(df["equipment_designator"], f.equipment)
    return df[m]


def apply_to_actions(df: pd.DataFrame, f: FilterState) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    if "start_date" in df:
        m &= _mask_date(df["start_date"], f.date_range)
    return df[m]


def apply_to_panda(df: pd.DataFrame, f: FilterState) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    if "Case_Open_Date" in df:
        m &= _mask_date(df["Case_Open_Date"], f.date_range)
    if "Failure_Mode" in df:
        m &= _mask_in(df["Failure_Mode"], f.failure_modes)
    if "Current_Case_Status" in df:
        m &= _mask_in(df["Current_Case_Status"], f.case_status)
    return df[m]


def apply_to_linked(df: pd.DataFrame, f: FilterState) -> pd.DataFrame:
    m = pd.Series(True, index=df.index)
    if "start_date_event" in df:
        m &= _mask_date(df["start_date_event"], f.date_range)
    if "aircraft_id" in df:
        m &= _mask_in(df["aircraft_id"], f.aircraft_ids)
    if "equipment_designator" in df:
        m &= _mask_in(df["equipment_designator"], f.equipment)
    if "Failure_Mode" in df:
        m &= _mask_in(df["Failure_Mode"], f.failure_modes)
    if "Current_Case_Status" in df:
        m &= _mask_in(df["Current_Case_Status"], f.case_status)
    return df[m]
