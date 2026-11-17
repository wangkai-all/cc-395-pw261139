#!/usr/bin/env bash
# Run the offline demo suite (full / concurrency / idempotency).
set -euo pipefail
cd "$(dirname "$0")"
exec python3 -I -c "
import sys
sys.path.insert(0, '$PWD')
from app.demo import main
main()
" "$@"
