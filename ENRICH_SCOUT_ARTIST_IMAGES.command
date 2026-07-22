#!/bin/zsh
set -euo pipefail
HERE="${0:A:h}"
SYSTEM_ROOT="${SCOUT_SYSTEM_ROOT:-$HOME/SCOUT_ARTIST_SYSTEM}"
FIELD_DIR="${SCOUT_FIELD_DIR:-$HOME/SCOUT_ARTIST_FIELD}"
LIMIT="${SCOUT_IMAGE_LIMIT:-10000}"
mkdir -p "$SYSTEM_ROOT"
rsync -a "$HERE/SCOUT_enrich_artist_images.py" "$SYSTEM_ROOT/"
VENV="$SYSTEM_ROOT/venv"
[[ -x "$VENV/bin/python" ]] || { echo "Run DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command first."; exit 1; }
"$VENV/bin/python" -m pip install --disable-pip-version-check --quiet --upgrade requests
DB="$FIELD_DIR/live_index/metadata.sqlite3"
[[ -f "$DB" ]] || { echo "Start SCOUT once so the live index is created: $DB"; exit 1; }
exec "$VENV/bin/python" "$SYSTEM_ROOT/SCOUT_enrich_artist_images.py" --db "$DB" --limit "$LIMIT"
