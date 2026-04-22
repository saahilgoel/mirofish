"""Ontology-guided entity + relation extraction using the configured LLM.

Called by the local Zep shim when episodes are added to a graph. The LLM is
asked to return a strict JSON object with `entities` and `relations` lists,
constrained to the ontology's entity/edge type names.

Extraction is best-effort: malformed JSON or LLM errors are logged and the
episode is still marked as processed (with zero extracted items), matching
how Zep Cloud behaves on extraction failures.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional


logger = logging.getLogger("mirofish.zep_local.extractor")


_SYSTEM_PROMPT = """You extract entities and relations from text to build a knowledge graph.

You will be given:
1. An ontology describing allowed entity types and edge (relation) types.
2. A text chunk.

Return a single JSON object with exactly this shape:
{
  "entities": [
    {"name": "<canonical name>", "type": "<one of the allowed entity type names>", "summary": "<1-2 sentences>", "attributes": {<optional key/value pairs>}}
  ],
  "relations": [
    {"type": "<one of the allowed edge type names>", "source": "<entity name>", "target": "<entity name>", "fact": "<natural-language statement of the relation>", "attributes": {<optional key/value pairs>}}
  ]
}

Rules:
- Only use entity and edge types listed in the ontology.
- Entity names should be canonical (e.g. "Apple Inc." not "it").
- `source` and `target` in relations must refer to an entity listed in `entities`.
- If nothing is extractable, return {"entities": [], "relations": []}.
- Output ONLY the JSON object, no prose, no markdown fences."""


def _build_user_prompt(
    ontology_entities: List[Dict[str, Any]],
    ontology_edges: List[Dict[str, Any]],
    text: str,
) -> str:
    ont = {
        "entity_types": [
            {
                "name": e.get("name"),
                "description": e.get("description", ""),
                "attributes": [a.get("name") for a in e.get("attributes", [])],
            }
            for e in ontology_entities
        ],
        "edge_types": [
            {
                "name": ed.get("name"),
                "description": ed.get("description", ""),
                "source_targets": ed.get("source_targets", []),
            }
            for ed in ontology_edges
        ],
    }
    return f"ONTOLOGY:\n{json.dumps(ont, ensure_ascii=False)}\n\nTEXT:\n{text}"


def _parse_json(raw: str) -> Dict[str, Any]:
    stripped = raw.strip()
    stripped = re.sub(r"^```(?:json)?\s*\n?", "", stripped, flags=re.IGNORECASE)
    stripped = re.sub(r"\n?```\s*$", "", stripped)
    return json.loads(stripped.strip())


def extract(
    text: str,
    ontology_entities: List[Dict[str, Any]],
    ontology_edges: List[Dict[str, Any]],
    llm_client: Optional[Any] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Extract entities and relations from `text` using the LLM.

    llm_client: an instance of app.utils.llm_client.LLMClient. Imported lazily
    to avoid a hard dependency from this package on the rest of the app.
    Returns {"entities": [...], "relations": [...]}, empty lists on failure.
    """
    if not text.strip():
        return {"entities": [], "relations": []}

    if llm_client is None:
        try:
            from app.utils.llm_client import LLMClient  # type: ignore

            llm_client = LLMClient()
        except Exception as e:  # pragma: no cover - config dependent
            logger.warning("Local Zep shim: LLM client unavailable, skipping extraction: %s", e)
            return {"entities": [], "relations": []}

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": _build_user_prompt(ontology_entities, ontology_edges, text)},
    ]

    try:
        raw = llm_client.chat(messages=messages, temperature=0.2, max_tokens=2048)
    except Exception as e:
        logger.warning("LLM extraction request failed: %s", e)
        return {"entities": [], "relations": []}

    try:
        parsed = _parse_json(raw)
    except Exception as e:
        logger.warning("LLM extraction returned non-JSON: %s", str(raw)[:200])
        return {"entities": [], "relations": []}

    entities = parsed.get("entities") or []
    relations = parsed.get("relations") or []

    allowed_entity_types = {e.get("name") for e in ontology_entities}
    allowed_edge_types = {e.get("name") for e in ontology_edges}

    clean_entities: List[Dict[str, Any]] = []
    seen_names = set()
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        name = (ent.get("name") or "").strip()
        etype = (ent.get("type") or "").strip()
        if not name or name in seen_names:
            continue
        if allowed_entity_types and etype not in allowed_entity_types:
            # Still keep, but label as generic Entity
            etype = "Entity"
        seen_names.add(name)
        clean_entities.append(
            {
                "name": name,
                "type": etype or "Entity",
                "summary": (ent.get("summary") or "").strip(),
                "attributes": ent.get("attributes") if isinstance(ent.get("attributes"), dict) else {},
            }
        )

    clean_relations: List[Dict[str, Any]] = []
    entity_names = {e["name"] for e in clean_entities}
    for rel in relations:
        if not isinstance(rel, dict):
            continue
        rtype = (rel.get("type") or "").strip()
        src = (rel.get("source") or "").strip()
        tgt = (rel.get("target") or "").strip()
        if not src or not tgt or src == tgt:
            continue
        if src not in entity_names or tgt not in entity_names:
            continue
        if allowed_edge_types and rtype not in allowed_edge_types:
            rtype = rtype or "RELATED_TO"
        clean_relations.append(
            {
                "type": rtype or "RELATED_TO",
                "source": src,
                "target": tgt,
                "fact": (rel.get("fact") or "").strip(),
                "attributes": rel.get("attributes") if isinstance(rel.get("attributes"), dict) else {},
            }
        )

    return {"entities": clean_entities, "relations": clean_relations}
