"""UI helpers shared across pages."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

PLOTLY_TEMPLATE = "plotly_dark"
PRIMARY = "#FF6F00"


def page_chrome(title: str, subtitle: str | None = None) -> None:
    st.set_page_config(
        page_title=f"CBM+ · {title}",
        page_icon="✈️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title(title)
    if subtitle:
        st.caption(subtitle)


def metric_row(metrics: list[tuple[str, str, str | None]]) -> None:
    cols = st.columns(len(metrics))
    for col, (label, value, delta) in zip(cols, metrics):
        with col:
            if delta is None:
                st.metric(label, value)
            else:
                st.metric(label, value, delta)


def top_n_bar(
    series: pd.Series,
    *,
    n: int = 10,
    title: str,
    x_label: str,
    y_label: str = "Count",
) -> None:
    counts = series.dropna().astype("string").value_counts().head(n)
    if counts.empty:
        st.info(f"No data for {title}.")
        return
    df = counts.reset_index()
    df.columns = [x_label, y_label]
    fig = px.bar(
        df,
        x=x_label,
        y=y_label,
        title=title,
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY],
    )
    fig.update_layout(xaxis_tickangle=-35, margin=dict(l=10, r=10, t=60, b=10))
    st.plotly_chart(fig, use_container_width=True)


def histogram(
    series: pd.Series,
    *,
    title: str,
    x_label: str,
    bins: int = 30,
    clip_quantile: float | None = 0.99,
) -> None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        st.info(f"No data for {title}.")
        return
    if clip_quantile is not None:
        hi = s.quantile(clip_quantile)
        s = s[s <= hi]
    fig = px.histogram(
        s,
        nbins=bins,
        title=title,
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=[PRIMARY],
    )
    fig.update_layout(
        xaxis_title=x_label,
        yaxis_title="Frequency",
        margin=dict(l=10, r=10, t=60, b=10),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def download_button(df: pd.DataFrame, filename: str, label: str = "Download CSV") -> None:
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def format_int(n: int | float | None) -> str:
    if n is None or pd.isna(n):
        return "—"
    return f"{int(n):,}"


def format_float(x: float | None, digits: int = 1) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"{x:,.{digits}f}"
