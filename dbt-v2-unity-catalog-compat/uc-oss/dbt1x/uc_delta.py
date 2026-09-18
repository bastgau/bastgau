"""dbt-duckdb plugin: write a model as a Delta table, then register it in Unity Catalog OSS.

This reproduces the pattern the dataroots article describes, and shows why it works
where dbt v2's `type: unity` catalog does not: it never touches the Iceberg REST
catalog (read-only in UC OSS). It writes Delta files with delta-rs and registers the
table through UC's **native** REST API, which does accept writes.

Requires dbt-core 1.x + dbt-duckdb (the plugin API does not exist in dbt v2).

profiles.yml:

    my_profile:
      target: dev
      outputs:
        dev:
          type: duckdb
          path: local.duckdb
          module_paths:
            - /path/to/this/directory
          plugins:
            - module: uc_delta
              alias: uc
              config:
                endpoint: http://127.0.0.1:8081/api/2.1/unity-catalog
                catalog: dbt_oss
                schema: analytics
                delta_root: /tmp/delta

Model:

    {{ config(materialized='external', plugin='uc', location=target.path ~ '/stage/customers.parquet') }}
    select 1 as id, 'alice' as name
"""
import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict

import pyarrow.parquet as pq
from deltalake import write_deltalake

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig

# UC column type names, keyed by the DuckDB/dbt type the model produced.
_TYPE_MAP = {
    "BIGINT": ("LONG", "long"),
    "INTEGER": ("INT", "integer"),
    "DOUBLE": ("DOUBLE", "double"),
    "BOOLEAN": ("BOOLEAN", "boolean"),
    "DATE": ("DATE", "date"),
    "TIMESTAMP": ("TIMESTAMP", "timestamp"),
    "VARCHAR": ("STRING", "string"),
}


def _uc_type(dtype: str):
    return _TYPE_MAP.get((dtype or "VARCHAR").upper().split("(")[0], ("STRING", "string"))


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        self.endpoint = config["endpoint"].rstrip("/")
        self.catalog = config["catalog"]
        self.schema = config.get("schema", "default")
        self.delta_root = config["delta_root"]
        self.token = config.get("token", "")

    # --- Unity Catalog native REST API -------------------------------------------------
    def _call(self, path: str, payload=None, method=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(
            f"{self.endpoint}{path}",
            data=json.dumps(payload).encode() if payload is not None else None,
            headers=headers,
            method=method or ("POST" if payload is not None else "GET"),
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:300]
            # A table that already exists is fine: the Delta write above is the source of truth.
            if exc.code in (409,):
                return {"already_exists": True, "detail": detail}
            raise RuntimeError(f"Unity Catalog {method or 'POST'} {path} -> {exc.code}: {detail}")

    def _ensure_namespace(self):
        self._call("/catalogs", {"name": self.catalog})
        self._call("/schemas", {"name": self.schema, "catalog_name": self.catalog})

    def _register(self, table: str, location: str, columns):
        payload = {
            "name": table,
            "catalog_name": self.catalog,
            "schema_name": self.schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": location,
            "columns": columns,
        }
        # Re-registering the same name is rejected, so drop the previous definition first.
        if self._table_exists(table):
            self._call(f"/tables/{self.catalog}.{self.schema}.{table}", method="DELETE")
        return self._call("/tables", payload)

    def _table_exists(self, table: str) -> bool:
        try:
            self._call(f"/tables/{self.catalog}.{self.schema}.{table}")
            return True
        except RuntimeError:
            return False

    # --- dbt-duckdb hook ---------------------------------------------------------------
    def store(self, target_config: TargetConfig):
        """Convert the parquet the materialization just wrote into Delta, then register it."""
        staged = target_config.location.path
        table = target_config.relation.identifier
        delta_path = os.path.join(self.delta_root, self.catalog, self.schema, table)

        write_deltalake(delta_path, pq.read_table(staged), mode="overwrite")

        columns = []
        for position, column in enumerate(target_config.column_list):
            type_name, type_text = _uc_type(column.dtype)
            columns.append(
                {
                    "name": column.column,
                    "type_text": type_text,
                    "type_name": type_name,
                    "type_json": json.dumps(
                        {"name": column.column, "type": type_text, "nullable": True, "metadata": {}}
                    ),
                    "position": position,
                    "nullable": True,
                }
            )

        self._ensure_namespace()
        self._register(table, f"file://{delta_path}", columns)

    def default_materialization(self):
        return "external"
