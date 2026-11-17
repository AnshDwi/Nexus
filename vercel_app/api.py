"""FastAPI entrypoint for Vercel Functions: `[tool.vercel] entrypoint`."""

import vercel_app  # noqa: F401 - puts src/ on sys.path
from nexus.main import app

__all__ = ["app"]
