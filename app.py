"""
CSV AI Plotting App
Upload a CSV file and use natural language to create Plotly visualizations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import re
import traceback

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="CSV AI Plotter",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "plot_history" not in st.session_state:
    st.session_state.plot_history = []

# ---------------------------------------------------------------------------
# Sidebar – API configuration
# ---------------------------------------------------------------------------
st.sidebar.title("Settings")

provider = st.sidebar.selectbox("AI Provider", ["Anthropic (Claude)", "OpenAI (GPT)"])

api_key = st.sidebar.text_input(
    "API Key",
    type="password",
    help="Your Anthropic or OpenAI API key. Never stored beyond this session.",
)

if provider == "Anthropic (Claude)":
    model = st.sidebar.selectbox(
        "Model",
        ["claude-sonnet-4-20250514", "claude-haiku-4-20250414", "claude-opus-4-20250514"],
    )
else:
    model = st.sidebar.selectbox(
        "Model",
        ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
    )

st.sidebar.markdown("---")
st.sidebar.markdown(
    "**How it works:** Upload a CSV, then describe a plot in plain English. "
    "The AI writes Plotly code and renders the chart."
)

# ---------------------------------------------------------------------------
# Helper – call AI provider
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a data-visualization assistant. The user will give you:
1. A description of the DataFrame columns and dtypes.
2. A sample of the first rows (as JSON).
3. A natural-language request for a plot.

Your job is to return ONLY valid Python code that:
- Uses the variable `df` (a pandas DataFrame already loaded).
- Creates a Plotly figure assigned to `fig`.
- Uses plotly.express (imported as `px`) or plotly.graph_objects (imported as `go`).
- Does NOT call `fig.show()` or `st.plotly_chart()`.
- Includes any necessary pandas transformations on `df` before plotting.
- If dates or timestamps are detected, parse them with pd.to_datetime().
- Uses fig.update_layout() to set a descriptive title.

Return ONLY the Python code inside a single ```python code fence. No explanation.
"""


def call_ai(prompt: str) -> str:
    """Send a prompt to the selected AI provider and return the response text."""
    if provider == "Anthropic (Claude)":
        from anthropic import Anthropic

        client = Anthropic(api_key=api_key)
        response = client.messages.create(
            model=model,
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    else:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            max_tokens=2048,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content


def extract_code(text: str) -> str:
    """Pull the first Python code block out of the AI response."""
    match = re.search(r"```(?:python)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text.strip()


def run_plot_code(code: str, dataframe: pd.DataFrame):
    """Execute generated code and return the fig object or None."""
    exec_namespace = {
        "pd": pd,
        "px": px,
        "go": go,
        "df": dataframe.copy(),
    }
    exec(code, exec_namespace)
    return exec_namespace.get("fig")


# ---------------------------------------------------------------------------
# Main UI
# ---------------------------------------------------------------------------
st.title("CSV AI Plotter")
st.markdown(
    "Upload a CSV file, preview your data, then describe the plot you want in plain English."
)

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    # Load data (cache on file name + size to avoid re-reading on every rerun)
    @st.cache_data
    def load_csv(file):
        return pd.read_csv(file)

    df = load_csv(uploaded_file)
    st.success(f"Loaded **{len(df):,}** rows and **{len(df.columns)}** columns.")

    # Data preview section
    with st.expander("Data Preview", expanded=True):
        tab_head, tab_types, tab_stats = st.tabs(
            ["Head", "Column Types", "Statistics"]
        )
        with tab_head:
            st.dataframe(df.head(50), use_container_width=True)
        with tab_types:
            type_df = pd.DataFrame(
                {"Column": df.columns, "Type": df.dtypes.astype(str).values}
            )
            st.dataframe(type_df, use_container_width=True, hide_index=True)
        with tab_stats:
            st.dataframe(df.describe(include="all").T, use_container_width=True)

    # -------------------------------------------------------------------
    # Quick-plot shortcuts
    # -------------------------------------------------------------------
    st.subheader("Quick Plots")
    col1, col2 = st.columns(2)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    all_cols = df.columns.tolist()

    with col1:
        if numeric_cols:
            hist_col = st.selectbox("Histogram column", numeric_cols, key="hist")
            if st.button("Plot Histogram"):
                fig = px.histogram(df, x=hist_col, title=f"Histogram of {hist_col}")
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        if len(numeric_cols) >= 2:
            scatter_x = st.selectbox("Scatter X", numeric_cols, index=0, key="sx")
            scatter_y = st.selectbox(
                "Scatter Y",
                numeric_cols,
                index=min(1, len(numeric_cols) - 1),
                key="sy",
            )
            if st.button("Plot Scatter"):
                fig = px.scatter(
                    df,
                    x=scatter_x,
                    y=scatter_y,
                    title=f"{scatter_y} vs {scatter_x}",
                )
                st.plotly_chart(fig, use_container_width=True)

    # -------------------------------------------------------------------
    # AI-powered plotting
    # -------------------------------------------------------------------
    st.markdown("---")
    st.subheader("AI-Powered Plot Builder")

    user_request = st.text_area(
        "Describe the plot you want",
        placeholder=(
            "e.g. Show me a bar chart of average salary by department, colored by region"
        ),
        height=100,
    )

    generate_clicked = st.button("Generate Plot", type="primary")

    if generate_clicked:
        if not api_key:
            st.error("Please enter your API key in the sidebar.")
        elif not user_request.strip():
            st.error("Please describe the plot you want.")
        else:
            # Build context about the data
            col_info = "\n".join(
                f"- {col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)
            )
            sample = df.head(5).to_json(orient="records", date_format="iso")

            prompt = (
                f"DataFrame columns and types:\n{col_info}\n\n"
                f"Sample rows (JSON):\n{sample}\n\n"
                f"User request:\n{user_request}"
            )

            with st.spinner("Generating plot..."):
                try:
                    raw_response = call_ai(prompt)
                    code = extract_code(raw_response)

                    with st.expander("View generated code", expanded=False):
                        st.code(code, language="python")

                    fig = run_plot_code(code, df)

                    if fig is None:
                        st.error(
                            "The AI did not produce a `fig` variable. "
                            "Try rephrasing your request."
                        )
                    else:
                        st.plotly_chart(fig, use_container_width=True)
                        # Save to history
                        st.session_state.plot_history.append(
                            {"request": user_request, "code": code}
                        )
                except Exception:
                    st.error("Failed to generate or execute the plot.")
                    st.code(traceback.format_exc())

    # -------------------------------------------------------------------
    # Refinement – modify a previous plot
    # -------------------------------------------------------------------
    if st.session_state.plot_history:
        st.markdown("---")
        st.subheader("Refine a Previous Plot")

        refine_idx = st.selectbox(
            "Select a plot to refine",
            range(len(st.session_state.plot_history)),
            format_func=lambda i: f"Plot {i + 1}: {st.session_state.plot_history[i]['request'][:70]}",
            key="refine_select",
        )

        prev = st.session_state.plot_history[refine_idx]
        with st.expander("Previous code"):
            st.code(prev["code"], language="python")

        refinement = st.text_area(
            "What changes do you want?",
            placeholder="e.g. Add a trendline, change colors to red/blue, make it a log scale",
            key="refinement_input",
        )

        if st.button("Refine Plot"):
            if not api_key:
                st.error("Please enter your API key in the sidebar.")
            elif not refinement.strip():
                st.error("Describe the changes you want.")
            else:
                col_info = "\n".join(
                    f"- {col} ({dtype})"
                    for col, dtype in zip(df.columns, df.dtypes)
                )
                sample = df.head(5).to_json(orient="records", date_format="iso")

                prompt = (
                    f"DataFrame columns and types:\n{col_info}\n\n"
                    f"Sample rows (JSON):\n{sample}\n\n"
                    f"Here is the existing code that produces a plot:\n"
                    f"```python\n{prev['code']}\n```\n\n"
                    f"User wants these changes:\n{refinement}"
                )

                with st.spinner("Refining plot..."):
                    try:
                        raw_response = call_ai(prompt)
                        code = extract_code(raw_response)

                        with st.expander("View refined code", expanded=False):
                            st.code(code, language="python")

                        fig = run_plot_code(code, df)

                        if fig is None:
                            st.error(
                                "The AI did not produce a `fig` variable. "
                                "Try rephrasing your request."
                            )
                        else:
                            st.plotly_chart(fig, use_container_width=True)
                            st.session_state.plot_history.append(
                                {
                                    "request": f"(Refined) {refinement}",
                                    "code": code,
                                }
                            )
                    except Exception:
                        st.error("Failed to refine the plot.")
                        st.code(traceback.format_exc())

    # -------------------------------------------------------------------
    # Plot history gallery
    # -------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Plot History")
    if st.session_state.plot_history:
        for i, entry in enumerate(reversed(st.session_state.plot_history)):
            idx = len(st.session_state.plot_history) - i
            with st.expander(f"Plot {idx}: {entry['request'][:80]}"):
                st.code(entry["code"], language="python")
                try:
                    fig = run_plot_code(entry["code"], df)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    st.warning("Could not re-render this plot with the current data.")
    else:
        st.caption("Generated plots will appear here.")

else:
    # Landing page when no file is uploaded
    st.info("Upload a CSV file to get started.")
    st.markdown(
        """
### Features
- **Data Preview** -- Inspect column types, summary statistics, and raw rows
- **Quick Plots** -- One-click histograms and scatter plots
- **AI Plot Builder** -- Describe any visualization in plain English
- **Iterative Refinement** -- Tweak a previous plot with follow-up instructions
- **Plot History** -- All generated plots are saved for the session

### Getting Started
1. Paste your **Anthropic** or **OpenAI** API key in the sidebar
2. Upload a CSV file
3. Describe the chart you want and click **Generate Plot**
"""
    )
