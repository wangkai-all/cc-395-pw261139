#!/usr/bin/env bash
# Entry point: run with isolated Python (-I) but make the project importable.
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"
# -I ignores PYTHONPATH, so we inject the path through a sitecustomize-free
# wrapper using python -c instead.
exec python3 -I -c "
import sys
sys.path.insert(0, '$PWD')
from app.server import main
main()
" "$@"
