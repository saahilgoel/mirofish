"""SQLite-backed storage for the local zep_cloud shim.

Schema is intentionally minimal — nodes, edges, episodes, graphs, ontology.
All JSON-y fields (labels, attributes, source_targets) are serialized as JSON.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple


_DB_PATH_ENV = "ZEP_LOCAL_DB_PATH"
_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "uploads",
    "graphs.db",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS graphs (
    graph_id     TEXT PRIMARY KEY,
    name         TEXT,
    description  TEXT,
    created_at   REAL
);

CREATE TABLE IF NOT EXISTS ontology (
    graph_id     TEXT PRIMARY KEY,
    entities     TEXT,      -- JSON: list of {name, description, attributes}
    edges        TEXT       -- JSON: list of {name, description, attributes, source_targets}
);

CREATE TABLE IF NOT EXISTS nodes (
    uuid         TEXT PRIMARY KEY,
    graph_id     TEXT NOT NULL,
    name         TEXT,
    labels       TEXT,      -- JSON list
    summary      TEXT,
    attributes   TEXT,      -- JSON dict
    created_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_nodes_graph ON nodes(graph_id);
CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(graph_id, name);

CREATE TABLE IF NOT EXISTS edges (
    uuid             TEXT PRIMARY KEY,
    graph_id         TEXT NOT NULL,
    name             TEXT,
    fact             TEXT,
    fact_type        TEXT,
    source_node_uuid TEXT,
    target_node_uuid TEXT,
    attributes       TEXT,  -- JSON
    episodes         TEXT,  -- JSON list
    created_at       REAL,
    valid_at         REAL,
    invalid_at       REAL,
    expired_at       REAL
);
CREATE INDEX IF NOT EXISTS idx_edges_graph ON edges(graph_id);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_node_uuid);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_node_uuid);

CREATE TABLE IF NOT EXISTS episodes (
    uuid         TEXT PRIMARY KEY,
    graph_id     TEXT NOT NULL,
    data         TEXT,
    type         TEXT,
    processed    INTEGER DEFAULT 0,
    created_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_episodes_graph ON episodes(graph_id);
"""


class _Store:
    """Thread-safe lightweight wrapper over a single SQLite file."""

    def __init__(self, path: Optional[str] = None):
        self.path = path or os.environ.get(_DB_PATH_ENV, _DEFAULT_DB_PATH)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    # ---- graphs ----------------------------------------------------------

    def create_graph(self, graph_id: str, name: str, description: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO graphs(graph_id, name, description, created_at) VALUES(?,?,?,?)",
                (graph_id, name, description, time.time()),
            )
            conn.commit()

    def delete_graph(self, graph_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM nodes WHERE graph_id=?", (graph_id,))
            conn.execute("DELETE FROM edges WHERE graph_id=?", (graph_id,))
            conn.execute("DELETE FROM episodes WHERE graph_id=?", (graph_id,))
            conn.execute("DELETE FROM ontology WHERE graph_id=?", (graph_id,))
            conn.execute("DELETE FROM graphs WHERE graph_id=?", (graph_id,))
            conn.commit()

    # ---- ontology --------------------------------------------------------

    def set_ontology(
        self,
        graph_id: str,
        entities: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ontology(graph_id, entities, edges) VALUES(?,?,?)",
                (graph_id, json.dumps(entities), json.dumps(edges)),
            )
            conn.commit()

    def get_ontology(self, graph_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT entities, edges FROM ontology WHERE graph_id=?",
                (graph_id,),
            ).fetchone()
        if not row:
            return [], []
        return json.loads(row["entities"] or "[]"), json.loads(row["edges"] or "[]")

    # ---- episodes --------------------------------------------------------

    def add_episode(self, graph_id: str, data: str, type_: str) -> str:
        ep_uuid = str(uuid.uuid4())
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO episodes(uuid, graph_id, data, type, processed, created_at) VALUES(?,?,?,?,0,?)",
                (ep_uuid, graph_id, data, type_, time.time()),
            )
            conn.commit()
        return ep_uuid

    def mark_episode_processed(self, ep_uuid: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("UPDATE episodes SET processed=1 WHERE uuid=?", (ep_uuid,))
            conn.commit()

    def get_episode(self, ep_uuid: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM episodes WHERE uuid=?", (ep_uuid,)
            ).fetchone()
        return dict(row) if row else None

    # ---- nodes -----------------------------------------------------------

    def upsert_node(
        self,
        graph_id: str,
        name: str,
        labels: List[str],
        summary: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Upsert by (graph_id, name). Returns node uuid."""
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT uuid, labels, summary, attributes FROM nodes WHERE graph_id=? AND name=?",
                (graph_id, name),
            ).fetchone()
            if existing:
                merged_labels = list({*(json.loads(existing["labels"] or "[]")), *labels})
                merged_attrs = {
                    **(json.loads(existing["attributes"] or "{}")),
                    **(attributes or {}),
                }
                merged_summary = summary or existing["summary"] or ""
                conn.execute(
                    "UPDATE nodes SET labels=?, summary=?, attributes=? WHERE uuid=?",
                    (json.dumps(merged_labels), merged_summary, json.dumps(merged_attrs), existing["uuid"]),
                )
                conn.commit()
                return existing["uuid"]
            node_uuid = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO nodes(uuid, graph_id, name, labels, summary, attributes, created_at) VALUES(?,?,?,?,?,?,?)",
                (
                    node_uuid,
                    graph_id,
                    name,
                    json.dumps(labels),
                    summary,
                    json.dumps(attributes or {}),
                    time.time(),
                ),
            )
            conn.commit()
            return node_uuid

    def get_node(self, node_uuid: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM nodes WHERE uuid=?", (node_uuid,)).fetchone()
        return dict(row) if row else None

    def list_nodes(
        self, graph_id: str, limit: int = 100, uuid_cursor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if uuid_cursor:
                rows = conn.execute(
                    "SELECT * FROM nodes WHERE graph_id=? AND uuid > ? ORDER BY uuid ASC LIMIT ?",
                    (graph_id, uuid_cursor, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM nodes WHERE graph_id=? ORDER BY uuid ASC LIMIT ?",
                    (graph_id, limit),
                ).fetchall()
        return [dict(r) for r in rows]

    # ---- edges -----------------------------------------------------------

    def add_edge(
        self,
        graph_id: str,
        name: str,
        fact: str,
        fact_type: str,
        source_node_uuid: str,
        target_node_uuid: str,
        attributes: Optional[Dict[str, Any]] = None,
        episodes: Optional[List[str]] = None,
    ) -> str:
        edge_uuid = str(uuid.uuid4())
        with self._lock, self._connect() as conn:
            conn.execute(
                """INSERT INTO edges(uuid, graph_id, name, fact, fact_type, source_node_uuid,
                                      target_node_uuid, attributes, episodes, created_at, valid_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    edge_uuid,
                    graph_id,
                    name,
                    fact,
                    fact_type,
                    source_node_uuid,
                    target_node_uuid,
                    json.dumps(attributes or {}),
                    json.dumps(episodes or []),
                    time.time(),
                    time.time(),
                ),
            )
            conn.commit()
        return edge_uuid

    def list_edges(
        self, graph_id: str, limit: int = 100, uuid_cursor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if uuid_cursor:
                rows = conn.execute(
                    "SELECT * FROM edges WHERE graph_id=? AND uuid > ? ORDER BY uuid ASC LIMIT ?",
                    (graph_id, uuid_cursor, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM edges WHERE graph_id=? ORDER BY uuid ASC LIMIT ?",
                    (graph_id, limit),
                ).fetchall()
        return [dict(r) for r in rows]

    def edges_for_node(self, node_uuid: str) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM edges WHERE source_node_uuid=? OR target_node_uuid=?",
                (node_uuid, node_uuid),
            ).fetchall()
        return [dict(r) for r in rows]


_SINGLETON: Optional[_Store] = None
_SINGLETON_LOCK = threading.Lock()


def get_store() -> _Store:
    global _SINGLETON
    if _SINGLETON is None:
        with _SINGLETON_LOCK:
            if _SINGLETON is None:
                _SINGLETON = _Store()
    return _SINGLETON
