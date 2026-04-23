"""SQLite-backed storage for the local zep_cloud shim.

Schema is intentionally minimal — nodes, edges, episodes, graphs, ontology.
All JSON-y fields (labels, attributes, source_targets) are serialized as JSON.
"""

from __future__ import annotations

import hashlib
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
    data_hash    TEXT,
    type         TEXT,
    processed    INTEGER DEFAULT 0,
    created_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_episodes_graph ON episodes(graph_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_graph_hash
    ON episodes(graph_id, data_hash);

-- Structured agent action log. Populated by simulation_runner as actions are
-- streamed back from the OASIS subprocess. The report agent queries this
-- directly via a new `query_actions` tool, instead of going through the
-- lossy "stringify -> graph -> re-extract" path.
CREATE TABLE IF NOT EXISTS agent_actions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    simulation_id   TEXT NOT NULL,
    run_id          TEXT,                  -- monte-carlo run id (NULL = single run)
    platform        TEXT,
    round_num       INTEGER,
    timestamp       TEXT,
    agent_id        INTEGER,
    agent_name      TEXT,
    action_type     TEXT,
    action_args     TEXT,                  -- JSON
    result          TEXT,                  -- JSON, nullable
    success         INTEGER DEFAULT 1,
    created_at      REAL
);
CREATE INDEX IF NOT EXISTS idx_actions_sim ON agent_actions(simulation_id);
CREATE INDEX IF NOT EXISTS idx_actions_sim_run ON agent_actions(simulation_id, run_id);
CREATE INDEX IF NOT EXISTS idx_actions_sim_agent ON agent_actions(simulation_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_actions_sim_type ON agent_actions(simulation_id, action_type);
CREATE INDEX IF NOT EXISTS idx_actions_sim_round ON agent_actions(simulation_id, round_num);
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
            # Forward-compatible migrations for databases created before
            # the resume-by-hash support landed.
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(episodes)").fetchall()}
            if "data_hash" not in cols:
                conn.execute("ALTER TABLE episodes ADD COLUMN data_hash TEXT")
                # Backfill existing rows so the unique index below can be built.
                rows = conn.execute("SELECT uuid, data FROM episodes").fetchall()
                for r in rows:
                    h = hashlib.sha1((r["data"] or "").encode("utf-8")).hexdigest()
                    conn.execute(
                        "UPDATE episodes SET data_hash=? WHERE uuid=?", (h, r["uuid"])
                    )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_graph_hash "
                    "ON episodes(graph_id, data_hash)"
                )
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

    def add_episode(self, graph_id: str, data: str, type_: str) -> Tuple[str, bool]:
        """Insert an episode idempotently. Returns (uuid, is_new).

        If an episode with the same (graph_id, sha1(data)) already exists, its
        existing uuid is returned and is_new=False. This lets graph_builder
        resubmit the same chunks during a resume without causing duplicate
        extraction / duplicate episodes.
        """
        data_hash = hashlib.sha1((data or "").encode("utf-8")).hexdigest()
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT uuid FROM episodes WHERE graph_id=? AND data_hash=?",
                (graph_id, data_hash),
            ).fetchone()
            if existing:
                return existing["uuid"], False
            ep_uuid = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO episodes(uuid, graph_id, data, data_hash, type, processed, created_at) "
                "VALUES(?,?,?,?,?,0,?)",
                (ep_uuid, graph_id, data, data_hash, type_, time.time()),
            )
            conn.commit()
            return ep_uuid, True

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

    # ---- agent actions (structured simulation log) ------------------------

    def record_action(
        self,
        simulation_id: str,
        action_data: Dict[str, Any],
        platform: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """Insert a single agent action. Best-effort — never raises on bad data.

        Expected keys in action_data: round, timestamp, agent_id, agent_name,
        action_type, action_args (dict), result, success.
        """
        try:
            args_json = json.dumps(action_data.get("action_args") or {}, ensure_ascii=False)
            result_field = action_data.get("result")
            result_json = (
                json.dumps(result_field, ensure_ascii=False)
                if result_field is not None
                else None
            )
            with self._lock, self._connect() as conn:
                conn.execute(
                    """INSERT INTO agent_actions(
                          simulation_id, run_id, platform, round_num, timestamp,
                          agent_id, agent_name, action_type, action_args,
                          result, success, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        simulation_id,
                        run_id,
                        platform or action_data.get("platform"),
                        int(action_data.get("round", 0) or 0),
                        action_data.get("timestamp") or "",
                        int(action_data.get("agent_id", 0) or 0),
                        action_data.get("agent_name") or "",
                        action_data.get("action_type") or "",
                        args_json,
                        result_json,
                        1 if action_data.get("success", True) else 0,
                        time.time(),
                    ),
                )
                conn.commit()
        except Exception:
            # Recording is best-effort; never block the simulation runner on it.
            pass

    def query_actions(
        self,
        simulation_id: str,
        action_type: Optional[str] = None,
        agent_name: Optional[str] = None,
        platform: Optional[str] = None,
        round_min: Optional[int] = None,
        round_max: Optional[int] = None,
        run_id: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Filter the structured action log. Returns up to `limit` rows.

        All filters are optional; passing none returns the most recent `limit`
        actions for the simulation.
        """
        clauses = ["simulation_id = ?"]
        params: List[Any] = [simulation_id]
        if action_type:
            clauses.append("action_type = ?")
            params.append(action_type)
        if agent_name:
            clauses.append("agent_name = ?")
            params.append(agent_name)
        if platform:
            clauses.append("platform = ?")
            params.append(platform)
        if round_min is not None:
            clauses.append("round_num >= ?")
            params.append(int(round_min))
        if round_max is not None:
            clauses.append("round_num <= ?")
            params.append(int(round_max))
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = " AND ".join(clauses)
        sql = (
            f"SELECT * FROM agent_actions WHERE {where} "
            f"ORDER BY round_num ASC, id ASC LIMIT ?"
        )
        params.append(int(limit))
        with self._lock, self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            # Re-hydrate JSON fields for caller convenience.
            try:
                d["action_args"] = json.loads(d.get("action_args") or "{}")
            except Exception:
                pass
            if d.get("result"):
                try:
                    d["result"] = json.loads(d["result"])
                except Exception:
                    pass
            out.append(d)
        return out

    def action_summary(
        self, simulation_id: str, run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Aggregate counts: per action_type, per agent, per round. Used by
        the report agent for fast overview without reading every row."""
        clauses = ["simulation_id = ?"]
        params: List[Any] = [simulation_id]
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = " AND ".join(clauses)
        with self._lock, self._connect() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) AS c FROM agent_actions WHERE {where}", params
            ).fetchone()["c"]
            by_type = conn.execute(
                f"SELECT action_type, COUNT(*) AS c FROM agent_actions WHERE {where} "
                f"GROUP BY action_type ORDER BY c DESC",
                params,
            ).fetchall()
            by_agent = conn.execute(
                f"SELECT agent_name, COUNT(*) AS c FROM agent_actions WHERE {where} "
                f"GROUP BY agent_name ORDER BY c DESC LIMIT 25",
                params,
            ).fetchall()
            rounds = conn.execute(
                f"SELECT MIN(round_num) AS r_min, MAX(round_num) AS r_max "
                f"FROM agent_actions WHERE {where}",
                params,
            ).fetchone()
        return {
            "total": int(total),
            "by_action_type": [{"type": r["action_type"], "count": r["c"]} for r in by_type],
            "top_agents": [{"agent": r["agent_name"], "count": r["c"]} for r in by_agent],
            "round_range": {"min": rounds["r_min"], "max": rounds["r_max"]},
        }


_SINGLETON: Optional[_Store] = None
_SINGLETON_LOCK = threading.Lock()


def get_store() -> _Store:
    global _SINGLETON
    if _SINGLETON is None:
        with _SINGLETON_LOCK:
            if _SINGLETON is None:
                _SINGLETON = _Store()
    return _SINGLETON
