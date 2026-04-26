from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from actimi_to_sensdoc import main  # ← direkt, kein src/

if __name__ == "__main__":
    raise SystemExit(main())