"""Runtime configuration for the CBM+ analyst app.

Resolution order for catalog/schema/table names:
  1. Environment variables (set in app.yaml when deployed as a Databricks App).
  2. Sensible defaults derived from the analyst's existing notebooks.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    catalog: str
    schema: str
    event_table: str
    action_table: str
    panda_table: str

    def fq(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    @property
    def fq_event(self) -> str:
        return self.fq(self.event_table)

    @property
    def fq_action(self) -> str:
        return self.fq(self.action_table)

    @property
    def fq_panda(self) -> str:
        return self.fq(self.panda_table)


def load_table_config() -> TableConfig:
    return TableConfig(
        catalog=os.getenv("CBM_CATALOG", "main"),
        schema=os.getenv("CBM_SCHEMA", "bld_a4l_restricted_analytics"),
        event_table=os.getenv("CBM_EVENT_TABLE", "cbm_mx_event"),
        action_table=os.getenv("CBM_ACTION_TABLE", "cbm_mx_action"),
        panda_table=os.getenv("PANDA_TABLE", "lg_panda_hist_sil"),
    )


@dataclass(frozen=True)
class WarehouseConfig:
    server_hostname: str | None
    http_path: str | None
    warehouse_id: str | None
    access_token: str | None

    @property
    def is_configured(self) -> bool:
        return bool(self.server_hostname and (self.http_path or self.warehouse_id))


def load_warehouse_config() -> WarehouseConfig:
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST")
    if host and host.startswith("https://"):
        host = host[len("https://"):]
    if host:
        host = host.rstrip("/")

    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not http_path and warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    token = os.getenv("DATABRICKS_TOKEN")

    return WarehouseConfig(
        server_hostname=host,
        http_path=http_path,
        warehouse_id=warehouse_id,
        access_token=token,
    )


CSV_FALLBACKS = {
    "event": os.getenv("CBM_EVENT_CSV", "/mnt/data/cbm_mx_event.csv"),
    "action": os.getenv("CBM_ACTION_CSV", "/mnt/data/cbm_mx_action.csv"),
    "panda": os.getenv("PANDA_CSV", "/mnt/data/lg_panda_hist_sil.csv"),
}
