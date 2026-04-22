"""Local shim for zep_cloud.external_clients.ontology.

Provides Pydantic base classes that the graph builder uses to dynamically
define ontology entity/edge types.
"""

from typing import Optional

from pydantic import BaseModel


class EntityText(str):
    """A string-like annotation used by Zep for typed entity attributes."""


class EntityModel(BaseModel):
    """Base class for dynamically defined entity types."""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class EdgeModel(BaseModel):
    """Base class for dynamically defined edge types."""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
