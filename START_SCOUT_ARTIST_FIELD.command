#!/bin/zsh
set -euo pipefail
HERE="${0:A:h}"
SYSTEM_ROOT="${SCOUT_SYSTEM_ROOT:-$HOME/SCOUT_ARTIST_SYSTEM}"
FIELD_DIR="${SCOUT_FIELD_DIR:-$HOME/SCOUT_ARTIST_FIELD}"
PORT="${SCOUT_PORT:-8790}"
EMBED_URL="${ARBITER_EMBED_URL:-http://127.0.0.1:8000/v1/embed}"

mkdir -p "$SYSTEM_ROOT"
rsync -a "$HERE/SCOUT_artist_field_forge.py" "$HERE/SCOUT_artist_field_server.py" "$HERE/SCOUT_massive_artist_field.html" "$HERE/SCOUT_enrich_artist_images.py" "$SYSTEM_ROOT/"
if [[ -d "$HERE/scout_images" ]]; then rsync -a "$HERE/scout_images" "$SYSTEM_ROOT/"; fi

VENV="$SYSTEM_ROOT/venv"
if [[ ! -x "$VENV/bin/python" ]]; then /usr/bin/python3 -m venv "$VENV"; fi
"$VENV/bin/python" -m pip install --disable-pip-version-check --quiet --upgrade numpy requests
[[ -f "$FIELD_DIR/field.json" ]] || { echo "Missing $FIELD_DIR/field.json — run DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command first."; exit 1; }

(
  URL="http://127.0.0.1:$PORT/field/v1/manifest"
  while ! /usr/bin/curl -fsS --max-time 1 "$URL" >/dev/null 2>&1; do sleep 1; done
  /usr/bin/open "http://127.0.0.1:$PORT/"
) &

exec "$VENV/bin/python" "$SYSTEM_ROOT/SCOUT_artist_field_server.py" serve \
  --field-dir "$FIELD_DIR" \
  --live-dir "$FIELD_DIR/live_index" \
  --include-source musicbrainz-artists \
  --assets-dir "$SYSTEM_ROOT" \
  --html "$SYSTEM_ROOT/SCOUT_massive_artist_field.html" \
  --embed-url "$EMBED_URL" \
  --host 127.0.0.1 \
  --port "$PORT" \
  --search-chunk-rows 250000 \
  --sync-interval 5
