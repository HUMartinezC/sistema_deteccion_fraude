"""DAG manual para compactar Iceberg, exportar un grafo y cargarlo en Neo4j.

El DAG usa la API HTTP de Trino y el endpoint transaccional HTTP de Neo4j para
no depender de conectores externos en el contenedor de Airflow.
"""

from __future__ import annotations

import base64
import csv
import importlib
import json
import os
import pathlib
import tempfile
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib import error, request

try:
    airflow_decorators = importlib.import_module("airflow.decorators")
    dag = airflow_decorators.dag
    task = airflow_decorators.task
    try:
        get_current_context = airflow_decorators.get_current_context
    except AttributeError:
        get_current_context = importlib.import_module("airflow.operators.python").get_current_context
    Param = importlib.import_module("airflow.models.param").Param
    AIRFLOW_AVAILABLE = True
except (ImportError, AttributeError):  # pragma: no cover - fallback para analisis local sin Airflow
    AIRFLOW_AVAILABLE = False

    def dag(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def task(func=None, **kwargs):
        if func is None:
            def decorator(inner_func):
                return inner_func

            return decorator
        return func

    def get_current_context():
        return {"params": {}}

    class Param:  # type: ignore[too-many-ancestors]
        def __init__(self, default, **kwargs):
            self.default = default


TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")

NEO4J_HTTP_URL = os.getenv("NEO4J_HTTP_URL", "http://neo4j:7474")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

LABEL_KEY_FIELDS = {
    "Customer": "customer_id",
    "Card": "card_id",
    "Device": "device_id",
    "Merchant": "merchant_id",
    "Payment": "payment_id",
    "Country": "country",
}


def _normalize_timestamp(value: str) -> str:
    """Convierte una fecha ISO a formato SQL sin zona horaria."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _http_json(
    url: str,
    method: str = "GET",
    body: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    req = request.Request(url, data=body, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)

    try:
        with request.urlopen(req, timeout=timeout) as response:
            payload = response.read()
    except error.HTTPError as exc:
        payload = exc.read()
        if not payload:
            raise

    if not payload:
        return {}
    return json.loads(payload.decode("utf-8"))


def _trino_request_headers() -> Dict[str, str]:
    return {
        "X-Trino-User": TRINO_USER,
        "X-Trino-Catalog": TRINO_CATALOG,
        "X-Trino-Schema": TRINO_SCHEMA,
        "Content-Type": "text/plain; charset=utf-8",
    }


def trino_execute(sql: str) -> None:
    """Ejecuta una sentencia SQL en Trino y valida el resultado final."""
    url = f"{TRINO_URL}/v1/statement"
    req = request.Request(url, data=sql.encode("utf-8"), method="POST")
    for key, value in _trino_request_headers().items():
        req.add_header(key, value)

    with request.urlopen(req, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))

    while payload.get("nextUri"):
        payload = _http_json(payload["nextUri"], timeout=120)

    if payload.get("error"):
        raise RuntimeError(payload["error"].get("message", "Trino SQL failed"))


def trino_query(sql: str) -> List[Dict[str, Any]]:
    """Ejecuta una consulta en Trino y devuelve las filas como diccionarios."""
    url = f"{TRINO_URL}/v1/statement"
    req = request.Request(url, data=sql.encode("utf-8"), method="POST")
    for key, value in _trino_request_headers().items():
        req.add_header(key, value)

    rows: List[Dict[str, Any]] = []
    columns: Sequence[str] = []

    with request.urlopen(req, timeout=120) as response:
        payload = json.loads(response.read().decode("utf-8"))

    while True:
        if not columns and payload.get("columns"):
            columns = [column["name"] for column in payload["columns"]]

        if payload.get("data") and columns:
            for row in payload["data"]:
                rows.append(dict(zip(columns, row)))

        next_uri = payload.get("nextUri")
        if not next_uri:
            break
        payload = _http_json(next_uri, timeout=120)

    if payload.get("error"):
        raise RuntimeError(payload["error"].get("message", "Trino query failed"))

    return rows


def neo4j_commit(statements: Iterable[Dict[str, Any]]) -> None:
    """Envía un lote de sentencias Cypher a Neo4j por HTTP."""
    url = f"{NEO4J_HTTP_URL}/db/neo4j/tx/commit"
    auth = base64.b64encode(f"{NEO4J_USER}:{NEO4J_PASSWORD}".encode("utf-8")).decode("ascii")
    payload = json.dumps({"statements": list(statements)}).encode("utf-8")
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    response = _http_json(url, method="POST", body=payload, headers=headers, timeout=120)
    if response.get("errors"):
        messages = "; ".join(error_item.get("message", "Neo4j error") for error_item in response["errors"])
        raise RuntimeError(messages)


def chunked(items: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _write_csv(path: pathlib.Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _build_graph_payload(rows: List[Dict[str, Any]], graph_name: str, export_dir: pathlib.Path) -> Dict[str, str]:
    export_dir.mkdir(parents=True, exist_ok=True)

    customer_rows: Dict[str, Dict[str, Any]] = {}
    card_rows: Dict[str, Dict[str, Any]] = {}
    device_rows: Dict[str, Dict[str, Any]] = {}
    merchant_rows: Dict[str, Dict[str, Any]] = {}
    country_rows: Dict[str, Dict[str, Any]] = {}
    payment_rows: List[Dict[str, Any]] = []
    relationship_rows: List[Dict[str, Any]] = []

    for row in rows:
        customer_id = row["customer_id"]
        card_id = row["card_id"]
        device_id = row["device_id"]
        merchant_id = row["merchant_id"]
        country = row["country"]
        payment_id = row["payment_id"]

        customer_rows.setdefault(customer_id, {"customer_id": customer_id, "graph_name": graph_name})
        card_rows.setdefault(card_id, {"card_id": card_id, "customer_id": customer_id, "graph_name": graph_name})
        device_rows.setdefault(device_id, {"device_id": device_id, "graph_name": graph_name})
        merchant_rows.setdefault(merchant_id, {"merchant_id": merchant_id, "graph_name": graph_name})
        country_rows.setdefault(country, {"country": country, "graph_name": graph_name})

        payment_rows.append(
            {
                "payment_id": payment_id,
                "event_ts": row["event_ts"],
                "customer_id": customer_id,
                "card_id": card_id,
                "device_id": device_id,
                "merchant_id": merchant_id,
                "country": country,
                "amount": row["amount"],
                "currency": row["currency"],
                "status": row["status"],
                "risk_score": row.get("risk_score"),
                "reasons": json.dumps(row.get("reasons") or []),
                "graph_name": graph_name,
            }
        )

        relationship_rows.extend(
            [
                {
                    "source_id": customer_id,
                    "source_label": "Customer",
                    "target_id": card_id,
                    "target_label": "Card",
                    "relationship_type": "OWNS_CARD",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
                {
                    "source_id": customer_id,
                    "source_label": "Customer",
                    "target_id": payment_id,
                    "target_label": "Payment",
                    "relationship_type": "MADE_PAYMENT",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
                {
                    "source_id": payment_id,
                    "source_label": "Payment",
                    "target_id": card_id,
                    "target_label": "Card",
                    "relationship_type": "USES_CARD",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
                {
                    "source_id": payment_id,
                    "source_label": "Payment",
                    "target_id": device_id,
                    "target_label": "Device",
                    "relationship_type": "FROM_DEVICE",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
                {
                    "source_id": payment_id,
                    "source_label": "Payment",
                    "target_id": merchant_id,
                    "target_label": "Merchant",
                    "relationship_type": "AT_MERCHANT",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
                {
                    "source_id": payment_id,
                    "source_label": "Payment",
                    "target_id": country,
                    "target_label": "Country",
                    "relationship_type": "IN_COUNTRY",
                    "payment_id": payment_id,
                    "event_ts": row["event_ts"],
                    "graph_name": graph_name,
                },
            ]
        )

    return {
        "customers_csv": _write_csv(
            export_dir / "customers.csv",
            list(customer_rows.values()),
            ["customer_id", "graph_name"],
        ),
        "cards_csv": _write_csv(
            export_dir / "cards.csv",
            list(card_rows.values()),
            ["card_id", "customer_id", "graph_name"],
        ),
        "devices_csv": _write_csv(
            export_dir / "devices.csv",
            list(device_rows.values()),
            ["device_id", "graph_name"],
        ),
        "merchants_csv": _write_csv(
            export_dir / "merchants.csv",
            list(merchant_rows.values()),
            ["merchant_id", "graph_name"],
        ),
        "countries_csv": _write_csv(
            export_dir / "countries.csv",
            list(country_rows.values()),
            ["country", "graph_name"],
        ),
        "payments_csv": _write_csv(
            export_dir / "payments.csv",
            payment_rows,
            [
                "payment_id",
                "event_ts",
                "customer_id",
                "card_id",
                "device_id",
                "merchant_id",
                "country",
                "amount",
                "currency",
                "status",
                "risk_score",
                "reasons",
                "graph_name",
            ],
        ),
        "relationships_csv": _write_csv(
            export_dir / "relationships.csv",
            relationship_rows,
            [
                "source_id",
                "source_label",
                "target_id",
                "target_label",
                "relationship_type",
                "payment_id",
                "event_ts",
                "graph_name",
            ],
        ),
    }


def _load_nodes_from_csv(label: str, csv_path: str, key_field: str, graph_name: str) -> None:
    path = pathlib.Path(csv_path)
    if not path.exists():
        return

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        return

    for batch in chunked(rows, 100):
        statement = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{{key_field}: row.{key_field}}}) "
            f"SET n += row, n.graph_name = $graph_name"
        )
        neo4j_commit(
            [
                {
                    "statement": statement,
                    "parameters": {"rows": list(batch), "graph_name": graph_name},
                }
            ]
        )


def _load_payments_from_csv(csv_path: str, graph_name: str) -> None:
    path = pathlib.Path(csv_path)
    if not path.exists():
        return

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        return

    for batch in chunked(rows, 100):
        statement = (
            "UNWIND $rows AS row "
            "MERGE (p:Payment {payment_id: row.payment_id}) "
                    "SET p.event_ts = row.event_ts, "
                    "    p.customer_id = row.customer_id, "
                    "    p.card_id = row.card_id, "
                    "    p.device_id = row.device_id, "
                    "    p.merchant_id = row.merchant_id, "
                    "    p.country = row.country, "
                    "    p.amount = toFloat(row.amount), "
                    "    p.currency = row.currency, "
                    "    p.status = row.status, "
                    "    p.risk_score = CASE WHEN row.risk_score IS NULL OR row.risk_score = '' THEN null ELSE toInteger(row.risk_score) END, "
                    "    p.reasons = row.reasons, "
                    "    p.graph_name = $graph_name"
        )
        neo4j_commit(
            [
                {
                    "statement": statement,
                    "parameters": {"rows": list(batch), "graph_name": graph_name},
                }
            ]
        )


def _load_relationships_from_csv(csv_path: str, graph_name: str) -> None:
    path = pathlib.Path(csv_path)
    if not path.exists():
        return

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        return

    for batch in chunked(rows, 50):
        statements = []
        for row in batch:
            source_label = row["source_label"]
            target_label = row["target_label"]
            source_key = LABEL_KEY_FIELDS[source_label]
            target_key = LABEL_KEY_FIELDS[target_label]
            relationship_type = row["relationship_type"]
            statements.append(
                {
                    "statement": (
                        f"MATCH (a:{source_label} {{{source_key}: $source_id, graph_name: $graph_name}}) "
                        f"MATCH (b:{target_label} {{{target_key}: $target_id, graph_name: $graph_name}}) "
                        f"MERGE (a)-[r:{relationship_type} {{payment_id: $payment_id, graph_name: $graph_name}}]->(b) "
                        "SET r.event_ts = $event_ts"
                    ),
                    "parameters": {
                        "source_id": row["source_id"],
                        "target_id": row["target_id"],
                        "payment_id": row["payment_id"],
                        "event_ts": row["event_ts"],
                        "graph_name": graph_name,
                    },
                }
            )
        neo4j_commit(statements)


@dag(
    dag_id="fraud_neo4j_orchestration",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraude", "iceberg", "neo4j", "orquestacion"],
    params={
        "source_table": Param("gold.payment_relations", type="string"),
        "start_ts": Param("2024-01-01T00:00:00Z", type="string"),
        "end_ts": Param("2099-12-31T23:59:59Z", type="string"),
        "graph_name": Param("fraud_graph", type="string"),
    },
    description="Compacta una tabla Iceberg, exporta un subgrafo y lo carga en Neo4j.",
)
def fraud_neo4j_orchestration():
    @task
    def compact_table() -> Dict[str, str]:
        context = get_current_context()
        params = context["params"]
        source_table = params["source_table"].strip()
        trino_execute(f"ALTER TABLE {TRINO_CATALOG}.{source_table} EXECUTE optimize")
        return {"source_table": source_table}

    @task
    def export_graph_dataset(compaction_result: Dict[str, str]) -> Dict[str, str]:
        context = get_current_context()
        params = context["params"]
        graph_name = params["graph_name"].strip()
        start_ts = _normalize_timestamp(params["start_ts"])
        end_ts = _normalize_timestamp(params["end_ts"])
        source_table = compaction_result["source_table"]

        sql = f"""
        SELECT
            payment_id,
            event_ts,
            customer_id,
            card_id,
            merchant_id,
            device_id,
            country,
            amount,
            currency,
            status,
            risk_score,
            reasons
        FROM {TRINO_CATALOG}.{source_table}
        WHERE event_ts BETWEEN TIMESTAMP '{start_ts}' AND TIMESTAMP '{end_ts}'
        ORDER BY event_ts
        """

        rows = trino_query(sql)
        export_root = pathlib.Path(tempfile.gettempdir()) / "fraud_graph_exports" / graph_name
        payload = _build_graph_payload(rows, graph_name=graph_name, export_dir=export_root)
        payload["graph_name"] = graph_name
        payload["row_count"] = str(len(rows))
        return payload

    @task
    def load_graph_into_neo4j(export_result: Dict[str, str]) -> Dict[str, str]:
        graph_name = export_result["graph_name"]
        _load_nodes_from_csv("Customer", export_result["customers_csv"], "customer_id", graph_name)
        _load_nodes_from_csv("Card", export_result["cards_csv"], "card_id", graph_name)
        _load_nodes_from_csv("Device", export_result["devices_csv"], "device_id", graph_name)
        _load_nodes_from_csv("Merchant", export_result["merchants_csv"], "merchant_id", graph_name)
        _load_nodes_from_csv("Country", export_result["countries_csv"], "country", graph_name)
        _load_payments_from_csv(export_result["payments_csv"], graph_name)
        _load_relationships_from_csv(export_result["relationships_csv"], graph_name)
        return {"graph_name": graph_name, "loaded_rows": export_result["row_count"]}

    compacted = compact_table()
    exported = export_graph_dataset(compacted)
    load_graph_into_neo4j(exported)


if AIRFLOW_AVAILABLE:
    fraud_neo4j_orchestration()
