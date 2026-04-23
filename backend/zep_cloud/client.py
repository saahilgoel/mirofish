"""Local Zep client shim.

Provides the `Zep` class and nested `graph` / `graph.node` / `graph.edge` /
`graph.episode` namespaces used by MiroFish's existing code.

All methods are synchronous. Episode extraction is performed inline inside
`graph.add` / `graph.add_batch` and the resulting episode is marked as
`processed=True` before returning, so the existing `_wait_for_episodes` loop
in graph_builder.py sees everything complete on the first poll.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from . import EpisodeData
from ._extractor import extract
from ._storage import get_store


logger = logging.getLogger("mirofish.zep_local.client")


# ---------- result objects (duck-typed to match Zep SDK) --------------------


@dataclass
class _Node:
    uuid_: str = ""
    name: str = ""
    labels: List[str] = field(default_factory=list)
    summary: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[str] = None

    # Some callers access `.uuid` instead of `.uuid_`
    @property
    def uuid(self) -> str:
        return self.uuid_


@dataclass
class _Edge:
    uuid_: str = ""
    name: str = ""
    fact: str = ""
    fact_type: str = ""
    source_node_uuid: str = ""
    target_node_uuid: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    episodes: List[str] = field(default_factory=list)
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    invalid_at: Optional[str] = None
    expired_at: Optional[str] = None

    @property
    def uuid(self) -> str:
        return self.uuid_


@dataclass
class _Episode:
    uuid_: str = ""
    graph_id: str = ""
    data: str = ""
    type: str = "text"
    processed: bool = False
    created_at: Optional[str] = None


@dataclass
class _SearchResult:
    edges: List[_Edge] = field(default_factory=list)
    nodes: List[_Node] = field(default_factory=list)


@dataclass
class _BatchEpisodeResult:
    """Returned from graph.add_batch — wraps the new episode uuid."""
    uuid_: str = ""

    @property
    def uuid(self) -> str:
        return self.uuid_


# ---------- helpers ----------------------------------------------------------


def _node_row_to_obj(row: Dict[str, Any]) -> _Node:
    import json as _json

    return _Node(
        uuid_=row["uuid"],
        name=row["name"] or "",
        labels=_json.loads(row["labels"] or "[]"),
        summary=row["summary"] or "",
        attributes=_json.loads(row["attributes"] or "{}"),
        created_at=str(row["created_at"]) if row["created_at"] is not None else None,
    )


def _edge_row_to_obj(row: Dict[str, Any]) -> _Edge:
    import json as _json

    return _Edge(
        uuid_=row["uuid"],
        name=row["name"] or "",
        fact=row["fact"] or "",
        fact_type=row["fact_type"] or "",
        source_node_uuid=row["source_node_uuid"] or "",
        target_node_uuid=row["target_node_uuid"] or "",
        attributes=_json.loads(row["attributes"] or "{}"),
        episodes=_json.loads(row["episodes"] or "[]"),
        created_at=str(row["created_at"]) if row["created_at"] is not None else None,
        valid_at=str(row["valid_at"]) if row["valid_at"] is not None else None,
        invalid_at=str(row["invalid_at"]) if row["invalid_at"] is not None else None,
        expired_at=str(row["expired_at"]) if row["expired_at"] is not None else None,
    )


def _episode_row_to_obj(row: Dict[str, Any]) -> _Episode:
    return _Episode(
        uuid_=row["uuid"],
        graph_id=row["graph_id"],
        data=row["data"] or "",
        type=row["type"] or "text",
        processed=bool(row["processed"]),
        created_at=str(row["created_at"]) if row["created_at"] is not None else None,
    )


# ---------- ontology serialization ------------------------------------------


def _dump_ontology(
    entities: Optional[Dict[str, Any]],
    edges: Optional[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Serialize the Pydantic classes created by graph_builder.set_ontology.

    Each entity-class is a subclass of EntityModel. Each edge entry is a tuple
    (edge_class, [EntityEdgeSourceTarget, ...]).
    """
    ent_out: List[Dict[str, Any]] = []
    if entities:
        for name, cls in entities.items():
            attrs = []
            try:
                for field_name, field_info in cls.model_fields.items():  # pydantic v2
                    attrs.append({
                        "name": field_name,
                        "description": getattr(field_info, "description", None) or field_name,
                    })
            except AttributeError:
                pass
            ent_out.append({
                "name": name,
                "description": cls.__doc__ or "",
                "attributes": attrs,
            })

    edge_out: List[Dict[str, Any]] = []
    if edges:
        for name, value in edges.items():
            try:
                cls, source_targets = value
            except (TypeError, ValueError):
                cls, source_targets = value, []
            attrs = []
            try:
                for field_name, field_info in cls.model_fields.items():
                    attrs.append({
                        "name": field_name,
                        "description": getattr(field_info, "description", None) or field_name,
                    })
            except AttributeError:
                pass
            st_serialized = []
            for st in (source_targets or []):
                st_serialized.append({
                    "source": getattr(st, "source", None),
                    "target": getattr(st, "target", None),
                })
            edge_out.append({
                "name": name,
                "description": cls.__doc__ or "",
                "attributes": attrs,
                "source_targets": st_serialized,
            })
    return ent_out, edge_out


# ---------- search scoring (keyword) ----------------------------------------


def _match_score(text: str, query: str, keywords: List[str]) -> int:
    if not text:
        return 0
    text_lower = text.lower()
    q_lower = query.lower()
    if q_lower and q_lower in text_lower:
        return 100
    score = 0
    for kw in keywords:
        if kw in text_lower:
            score += 10
    return score


def _keywords(query: str) -> List[str]:
    q = query.lower().replace(",", " ").replace(".", " ")
    return [w for w in q.split() if len(w) > 1]


# ---------- nested namespaces -----------------------------------------------


class _NodeAPI:
    def __init__(self, client: "Zep"):
        self._client = client

    def get(self, uuid_: str) -> _Node:
        row = self._client._store.get_node(uuid_)
        if not row:
            raise KeyError(f"Node {uuid_} not found")
        return _node_row_to_obj(row)

    def get_by_graph_id(
        self,
        graph_id: str,
        limit: int = 100,
        uuid_cursor: Optional[str] = None,
    ) -> List[_Node]:
        rows = self._client._store.list_nodes(graph_id, limit=limit, uuid_cursor=uuid_cursor)
        return [_node_row_to_obj(r) for r in rows]

    def get_entity_edges(self, node_uuid: str) -> List[_Edge]:
        rows = self._client._store.edges_for_node(node_uuid)
        return [_edge_row_to_obj(r) for r in rows]


class _EdgeAPI:
    def __init__(self, client: "Zep"):
        self._client = client

    def get_by_graph_id(
        self,
        graph_id: str,
        limit: int = 100,
        uuid_cursor: Optional[str] = None,
    ) -> List[_Edge]:
        rows = self._client._store.list_edges(graph_id, limit=limit, uuid_cursor=uuid_cursor)
        return [_edge_row_to_obj(r) for r in rows]


class _EpisodeAPI:
    def __init__(self, client: "Zep"):
        self._client = client

    def get(self, uuid_: str) -> _Episode:
        row = self._client._store.get_episode(uuid_)
        if not row:
            raise KeyError(f"Episode {uuid_} not found")
        return _episode_row_to_obj(row)


class _GraphAPI:
    def __init__(self, client: "Zep"):
        self._client = client
        self.node = _NodeAPI(client)
        self.edge = _EdgeAPI(client)
        self.episode = _EpisodeAPI(client)

    # -- graph lifecycle --

    def create(
        self,
        graph_id: str,
        name: str = "",
        description: str = "",
    ) -> Dict[str, Any]:
        self._client._store.create_graph(graph_id, name, description)
        return {"graph_id": graph_id, "name": name, "description": description}

    def delete(self, graph_id: str) -> None:
        self._client._store.delete_graph(graph_id)

    def set_ontology(
        self,
        graph_ids: Optional[List[str]] = None,
        entities: Optional[Dict[str, Any]] = None,
        edges: Optional[Dict[str, Any]] = None,
    ) -> None:
        ent_out, edge_out = _dump_ontology(entities, edges)
        for gid in graph_ids or []:
            self._client._store.set_ontology(gid, ent_out, edge_out)

    # -- episode ingest + extraction --

    def add(
        self,
        graph_id: str,
        data: Optional[str] = None,
        type: str = "text",
        **kwargs: Any,
    ) -> _BatchEpisodeResult:
        """Add a single episode. Accepts either positional `data` + `type`, or
        an `episode=EpisodeData(...)` keyword to mirror Zep's flexibility."""
        episode = kwargs.get("episode")
        if episode is not None:
            data = episode.data
            type = getattr(episode, "type", type)
        if data is None:
            raise ValueError("graph.add requires `data` or `episode=`")
        return self._ingest_one(graph_id, data, type)

    _DEFAULT_INGEST_PARALLELISM = 8

    def add_batch(
        self,
        graph_id: str,
        episodes: Iterable[EpisodeData],
    ) -> List[_BatchEpisodeResult]:
        """Ingest episodes concurrently. Each episode triggers an independent
        LLM extraction call; SQLite writes are serialized by the store's
        internal RLock, so parallelism gives us ~N× speedup bounded only by
        the LLM provider's rate limit.
        """
        import concurrent.futures
        import os as _os

        eps = list(episodes)
        if not eps:
            return []

        try:
            override = int(_os.environ.get("ZEP_LOCAL_INGEST_PARALLELISM", "0"))
        except (TypeError, ValueError):
            override = 0
        max_workers = override if override > 0 else self._DEFAULT_INGEST_PARALLELISM
        max_workers = max(1, min(max_workers, len(eps)))

        if max_workers == 1:
            return [
                self._ingest_one(graph_id, ep.data, getattr(ep, "type", "text"))
                for ep in eps
            ]

        results: List[Optional[_BatchEpisodeResult]] = [None] * len(eps)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    self._ingest_one, graph_id, ep.data, getattr(ep, "type", "text")
                ): idx
                for idx, ep in enumerate(eps)
            }
            for fut in concurrent.futures.as_completed(futures):
                idx = futures[fut]
                results[idx] = fut.result()
        return [r for r in results if r is not None]

    def _ingest_one(self, graph_id: str, data: str, type_: str) -> _BatchEpisodeResult:
        store = self._client._store
        ep_uuid, is_new = store.add_episode(graph_id, data, type_)

        # Resume short-circuit: if this chunk was already ingested + processed
        # in a prior run, do not re-run extraction.
        if not is_new:
            existing = store.get_episode(ep_uuid)
            if existing and existing.get("processed"):
                logger.info("graph.add: skipping already-processed chunk %s", ep_uuid)
                return _BatchEpisodeResult(uuid_=ep_uuid)

        # Load ontology for this graph (may be empty).
        ont_entities, ont_edges = store.get_ontology(graph_id)

        extraction = extract(
            text=data,
            ontology_entities=ont_entities,
            ontology_edges=ont_edges,
            llm_client=self._client._llm_client,
        )

        # Upsert entities
        name_to_uuid: Dict[str, str] = {}
        for ent in extraction.get("entities", []):
            etype = ent.get("type") or "Entity"
            labels = ["Entity", etype] if etype != "Entity" else ["Entity"]
            node_uuid = store.upsert_node(
                graph_id=graph_id,
                name=ent["name"],
                labels=labels,
                summary=ent.get("summary", ""),
                attributes=ent.get("attributes") or {},
            )
            name_to_uuid[ent["name"]] = node_uuid

        # Add relations
        for rel in extraction.get("relations", []):
            src_uuid = name_to_uuid.get(rel.get("source"))
            tgt_uuid = name_to_uuid.get(rel.get("target"))
            if not src_uuid or not tgt_uuid:
                continue
            store.add_edge(
                graph_id=graph_id,
                name=rel.get("type") or "RELATED_TO",
                fact=rel.get("fact") or "",
                fact_type=rel.get("type") or "",
                source_node_uuid=src_uuid,
                target_node_uuid=tgt_uuid,
                attributes=rel.get("attributes") or {},
                episodes=[ep_uuid],
            )

        store.mark_episode_processed(ep_uuid)
        return _BatchEpisodeResult(uuid_=ep_uuid)

    # -- search --

    def search(
        self,
        graph_id: str,
        query: str,
        limit: int = 10,
        scope: str = "edges",
        reranker: Optional[str] = None,
    ) -> _SearchResult:
        store = self._client._store
        kws = _keywords(query)

        result = _SearchResult()
        if scope in ("edges", "both", None):
            scored = []
            for row in store.list_edges(graph_id, limit=1000):
                e = _edge_row_to_obj(row)
                s = _match_score(e.fact, query, kws) + _match_score(e.name, query, kws)
                if s > 0:
                    scored.append((s, e))
            scored.sort(key=lambda x: x[0], reverse=True)
            result.edges = [e for _, e in scored[:limit]]

        if scope in ("nodes", "both"):
            scored_n = []
            for row in store.list_nodes(graph_id, limit=1000):
                n = _node_row_to_obj(row)
                s = _match_score(n.name, query, kws) + _match_score(n.summary, query, kws)
                if s > 0:
                    scored_n.append((s, n))
            scored_n.sort(key=lambda x: x[0], reverse=True)
            result.nodes = [n for _, n in scored_n[:limit]]

        return result


# ---------- top-level client ------------------------------------------------


class Zep:
    """Local drop-in replacement for `zep_cloud.client.Zep`."""

    def __init__(self, api_key: Optional[str] = None, **_ignored: Any):
        # api_key is accepted (for API compat) but unused — storage is local.
        self._api_key = api_key
        self._store = get_store()
        # Lazily created LLM client (reused across calls in this process).
        self._llm_client = None
        try:
            from app.utils.llm_client import LLMClient  # type: ignore

            self._llm_client = LLMClient()
        except Exception as e:  # pragma: no cover
            logger.info(
                "Local Zep shim: LLM client not initialized (extraction will be a no-op): %s", e
            )
        self.graph = _GraphAPI(self)


__all__ = ["Zep"]
