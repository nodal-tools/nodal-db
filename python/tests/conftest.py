"""Shared plumbing for the generic Python suites under ``python/``.

Puts ``python/`` on ``sys.path`` so the in-tree packages (e.g.
``nodal_spine_import``) import without installation. The ``nodal_db`` skin
and ``spinedb_api`` come from the environment (the project ``.venv``; see
``tests/test_importer.py``'s module docstring for the run command).
"""

from __future__ import annotations

import sys
from pathlib import Path

PYTHON_ROOT = Path(__file__).resolve().parents[1]

if str(PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_ROOT))
