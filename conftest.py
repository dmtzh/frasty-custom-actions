"""Pytest path bootstrap for the customactions workspace.

The ``shared`` and ``infrastructure`` packages are not part of this repository:
``.deploy/copy_files.ps1`` copies them from the sibling ``frasty`` repository into
the deployment image. For local test runs we add both this folder and that sibling
folder to ``sys.path`` so that ``actions`` and ``shared`` can be imported.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_SIBLING_ROOT = _ROOT.parent / "frasty"

for _candidate in (_ROOT, _SIBLING_ROOT):
    if _candidate.is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))
