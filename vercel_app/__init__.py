"""Vercel entrypoint adapters.

Vercel resolves `[tool.vercel] entrypoint` and `[[tool.vercel.subscribers]]`
as files relative to the repository root, while NEXUS keeps its code in a
`src/` layout. These modules only make `nexus` importable and re-export the
real objects -- no application logic lives here.
"""

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent.parent / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
