# CBM+ Analyst Workbench — Databricks App

A Streamlit app that packages the CBM+ / PANDA deep-dive notebooks in
this repo into an interactive workbench. Designed to deploy as a
**Databricks App** (service-principal OAuth against a SQL warehouse),
with graceful fallbacks to an active `SparkSession` or local CSVs for
offline development.

## Pages

1. **Executive Overview** — KPIs, monthly event cadence, case status mix.
2. **CBM Events** — `cbm_mx_event` duration, equipment, and cadence.
3. **CBM Actions** — `cbm_mx_action` action-taken and malfunction codes.
4. **PANDA Cases** — status, failure modes, cycle time, schedule slip.
5. **Tail-Normalized Metrics** — per-aircraft event / action intensity.
6. **Trends by Aircraft** — side-by-side tail trendlines.
7. **Linked Deep Dive** — full event ⨝ action ⨝ PANDA join, searchable.
8. **Storage Inventory** — `DESCRIBE DETAIL` sweep, port of the DBFS/S3 scan notebook.

Every page shares the sidebar filter (date range, aircraft, equipment,
failure mode, case status).

## Deploy as a Databricks App

1. Push this repo to Databricks (via `databricks sync` or the UI).
2. Create a new **App** pointed at the `app/` folder.
3. Attach a SQL warehouse and set the `databricks_warehouse_id` app
   resource. `app.yaml` already maps that resource into the
   `DATABRICKS_WAREHOUSE_ID` environment variable.
4. Make sure the app's service principal has `SELECT` on:
   - `main.bld_a4l_restricted_analytics.cbm_mx_event`
   - `main.bld_a4l_restricted_analytics.cbm_mx_action`
   - `main.bld_a4l_restricted_analytics.lg_panda_hist_sil`

Override any of the table names via environment variables in
`app.yaml`:

| Variable | Default |
| --- | --- |
| `CBM_CATALOG` | `main` |
| `CBM_SCHEMA` | `bld_a4l_restricted_analytics` |
| `CBM_EVENT_TABLE` | `cbm_mx_event` |
| `CBM_ACTION_TABLE` | `cbm_mx_action` |
| `PANDA_TABLE` | `lg_panda_hist_sil` |

## Run locally

```bash
cd app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Option A — hit a real warehouse
export DATABRICKS_SERVER_HOSTNAME=adb-XXXX.XX.azuredatabricks.net
export DATABRICKS_WAREHOUSE_ID=abcd1234
export DATABRICKS_TOKEN=dapiXXXX

# Option B — point at local CSV dumps
export CBM_EVENT_CSV=/path/to/cbm_mx_event.csv
export CBM_ACTION_CSV=/path/to/cbm_mx_action.csv
export PANDA_CSV=/path/to/lg_panda_hist_sil.csv

streamlit run app.py
```

## Data model

The app mirrors the joins from `Notebook_Template`:

- Events ⨝ Actions on `work_order_number`
- Events ⨝ PANDA on `SN_JCN_PK = serial_number + ' ' + job_control_number`
  (PANDA side: `SERIAL_NUMBER + ' ' + SBA_JCN`)

Derived columns added at load time:

- `event_duration_hrs`, `action_duration_hrs`
- `case_cycle_days`, `Days_Open_to_Close`, `Days_Sched_to_Comp`,
  `Days_Rec_to_Comp`, `Days_AntSched_to_Comp`

PANDA dates in the `M/d/yy H:mm` format are normalized to 4-digit years
before being coerced to timestamps, matching `SBA_Case_Hist_DateDiff_Calc.py`.
