"""Local drop-in replacement for the zep_cloud SDK.

This package shadows the pip-installed `zep_cloud` at import time because
`backend/` is placed at the front of sys.path by `run.py`. It implements the
subset of the API actually used by MiroFish:

  * Zep client with `.graph.create/delete/set_ontology/add/add_batch/search`
  * `graph.node.get_by_graph_id`, `graph.node.get`, `graph.node.get_entity_edges`
  * `graph.edge.get_by_graph_id`
  * `graph.episode.get`
  * EpisodeData / EntityEdgeSourceTarget dataclasses
  * EntityModel / EdgeModel / EntityText ontology primitives (pydantic)
  * ApiError / InternalServerError exception types

Storage is SQLite (backend/uploads/graphs.db). Entity/edge extraction from
episodes is done by calling the configured LLM (LLM_API_KEY / LLM_MODEL_NAME).
Search falls back to keyword scoring — the MiroFish code already has a local
search fallback path that handles this well.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional

from .core.api_error import ApiError


class InternalServerError(ApiError):
    def __init__(self, body: Any = None, headers: Optional[dict] = None, message: str = ""):
        super().__init__(status_code=500, body=body, headers=headers, message=message)


@dataclass
class EpisodeData:
    """Payload container used by graph.add / graph.add_batch."""
    data: str
    type: str = "text"
    source_description: Optional[str] = None


@dataclass
class EntityEdgeSourceTarget:
    """Describes the source/target entity types an edge can connect."""
    source: Optional[str] = None
    target: Optional[str] = None


# Re-export the client at package level is NOT done (matches real SDK layout —
# callers do `from zep_cloud.client import Zep`).

__all__ = [
    "ApiError",
    "InternalServerError",
    "EpisodeData",
    "EntityEdgeSourceTarget",
]
