import sys
from pathlib import Path

# Several sh_util modules do a bare ``import settings``, expecting the host
# service's repo root (which ships a `settings.py` compatibility shim) to be
# on sys.path. Add it here so those modules are importable under test.
_admin_root = Path(__file__).resolve().parents[3]
if str(_admin_root) not in sys.path:
    sys.path.insert(0, str(_admin_root))
