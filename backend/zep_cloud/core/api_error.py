"""Local shim for zep_cloud.core.api_error."""

from typing import Any, Optional


class ApiError(Exception):
    def __init__(
        self,
        status_code: int = 0,
        body: Any = None,
        headers: Optional[dict] = None,
        message: str = "",
    ):
        self.status_code = status_code
        self.body = body
        self.headers = headers or {}
        super().__init__(message or f"ApiError(status={status_code})")
