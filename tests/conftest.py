import sys
from pathlib import Path

# Add src/ to sys.path so tests can import modules as the runtime scripts do.
src_path = Path(__file__).resolve().parents[1] / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

