#!/bin/zsh
set -euo pipefail
SYSTEM_ROOT="${SCOUT_SYSTEM_ROOT:-$HOME/SCOUT_ARTIST_SYSTEM}"
FIELD_DIR="${SCOUT_FIELD_DIR:-$HOME/SCOUT_ARTIST_FIELD}"
PORT="${SCOUT_PORT:-8790}"
PY="$SYSTEM_ROOT/venv/bin/python"
[[ -x "$PY" ]] || PY="$(command -v python3)"
echo "SCOUT ARTIST FIELD"
echo "────────────────────────────────────────────────────────"
if [[ -f "$FIELD_DIR/field.json" ]]; then
  "$PY" "$SYSTEM_ROOT/SCOUT_artist_field_forge.py" status --field-dir "$FIELD_DIR"
else
  echo "field not built: $FIELD_DIR"
fi
echo
if /usr/bin/curl -fsS --max-time 2 "http://127.0.0.1:$PORT/field/v1/manifest"; then echo; else echo "live server not running on $PORT"; fi
