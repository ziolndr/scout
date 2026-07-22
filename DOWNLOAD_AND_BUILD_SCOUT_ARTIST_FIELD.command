#!/bin/zsh
set -euo pipefail

HERE="${0:A:h}"
SYSTEM_ROOT="${SCOUT_SYSTEM_ROOT:-$HOME/SCOUT_ARTIST_SYSTEM}"
FIELD_DIR="${SCOUT_FIELD_DIR:-$HOME/SCOUT_ARTIST_FIELD}"
DATA_DIR="${SCOUT_DATA_DIR:-$HOME/SCOUT_ARTIST_DATA}"
EMBED_URL="${ARBITER_EMBED_URL:-http://127.0.0.1:8000/v1/embed}"
SELECTION="${SCOUT_SELECTION:-discoverable}"
LIMIT="${SCOUT_LIMIT:-0}"
BATCH_SIZE="${SCOUT_EMBED_BATCH:-512}"
PENDING_SHARD_SIZE="${SCOUT_PENDING_SHARD_SIZE:-100000}"

mkdir -p "$SYSTEM_ROOT" "$FIELD_DIR" "$DATA_DIR"
rsync -a "$HERE/SCOUT_artist_field_forge.py" "$HERE/SCOUT_artist_field_server.py" "$HERE/SCOUT_massive_artist_field.html" "$HERE/SCOUT_enrich_artist_images.py" "$SYSTEM_ROOT/"
if [[ -d "$HERE/scout_images" ]]; then rsync -a "$HERE/scout_images" "$SYSTEM_ROOT/"; fi

VENV="$SYSTEM_ROOT/venv"
if [[ ! -x "$VENV/bin/python" ]]; then
  /usr/bin/python3 -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --disable-pip-version-check --quiet --upgrade pip numpy requests
PY="$VENV/bin/python"
FORGE="$SYSTEM_ROOT/SCOUT_artist_field_forge.py"

printf '\nSCOUT MASSIVE ARTIST FIELD\n'
printf '────────────────────────────────────────────────────────\n'
printf 'field:      %s\n' "$FIELD_DIR"
printf 'selection:  %s\n' "$SELECTION"
printf 'embed:      %s\n' "$EMBED_URL"

if ! /usr/bin/curl -fsS --max-time 5 -X POST "$EMBED_URL" \
  -H 'Content-Type: application/json' \
  -d '{"texts":["SCOUT artist field readiness test"],"use_freq":true}' >/dev/null; then
  echo
  echo "ARBITER /v1/embed is not reachable at $EMBED_URL"
  echo "Start the existing ARBITER embedding service, then rerun this command."
  exit 1
fi

echo
echo "1) Resolve and download the current official MusicBrainz artist JSON dump"
JSON_DUMPS="https://data.metabrainz.org/pub/musicbrainz/data/json-dumps"
LATEST_ID="$(/usr/bin/curl -fsSL --retry 8 --retry-delay 3 "$JSON_DUMPS/LATEST" | tr -d '[:space:]')"
[[ "$LATEST_ID" == <->-<-> ]] || { echo "Could not resolve MusicBrainz JSON dump version: $LATEST_ID"; exit 1; }
BASE="$JSON_DUMPS/$LATEST_ID"
DUMP="$DATA_DIR/musicbrainz-artist-$LATEST_ID.tar.xz"
SUMS="$DATA_DIR/musicbrainz-SHA256SUMS-$LATEST_ID"
echo "MusicBrainz JSON dump: $LATEST_ID"
/usr/bin/curl -fL --retry 8 --retry-delay 3 -C - -o "$DUMP" "$BASE/artist.tar.xz"
/usr/bin/curl -fL --retry 8 --retry-delay 3 -o "$SUMS" "$BASE/SHA256SUMS"
EXPECTED="$(awk '$2=="artist.tar.xz" || $2=="*artist.tar.xz" {print $1; exit}' "$SUMS")"
if [[ -n "$EXPECTED" ]]; then
  ACTUAL="$(/usr/bin/shasum -a 256 "$DUMP" | awk '{print $1}')"
  [[ "$ACTUAL" == "$EXPECTED" ]] || { echo "SHA-256 verification failed"; exit 1; }
  echo "artist.tar.xz SHA-256 verified"
else
  echo "Warning: artist.tar.xz was not listed in SHA256SUMS; continuing without checksum verification."
fi

echo
echo "2) Initialize the dedicated SCOUT artist field"
if [[ ! -f "$FIELD_DIR/field.json" ]]; then
  "$PY" "$FORGE" init --field-dir "$FIELD_DIR" --name "SCOUT Artist Discovery Field" --dim 72 --use-freq
fi

SOURCE_INGESTED="$($PY - "$FIELD_DIR/field.json" <<'PY'
import json,sys
p=sys.argv[1]
d=json.load(open(p))
s=(d.get('sources') or {}).get('musicbrainz-artists') or {}
print(int(s.get('ingested_records') or 0))
PY
)"

if [[ "$SOURCE_INGESTED" -gt 0 && "${SCOUT_FORCE_REINGEST:-0}" != "1" ]]; then
  echo "MusicBrainz artist source already contains $SOURCE_INGESTED ingested records; skipping duplicate ingestion."
else
  echo
  echo "3) Stream artist.tar.xz into durable pending artist shards"
  INGEST_ARGS=(
    ingest-musicbrainz-artists-json
    --field-dir "$FIELD_DIR"
    --dump "$DUMP"
    --selection "$SELECTION"
    --pending-shard-size "$PENDING_SHARD_SIZE"
    --max-text-chars 3000
  )
  if [[ "$LIMIT" -gt 0 ]]; then INGEST_ARGS+=(--limit "$LIMIT"); fi
  if [[ "${SCOUT_ALLOW_DERIVED_TAGS:-0}" == "1" ]]; then INGEST_ARGS+=(--allow-derived-tags); fi
  "$PY" "$FORGE" "${INGEST_ARGS[@]}"
fi

echo
echo "4) Embed every pending artist once through ARBITER"
"$PY" "$FORGE" embed-pending \
  --field-dir "$FIELD_DIR" \
  --source musicbrainz-artists \
  --endpoint "$EMBED_URL" \
  --batch-size "$BATCH_SIZE"

echo
echo "5) Verify the immutable artist shards"
"$PY" "$FORGE" verify --field-dir "$FIELD_DIR"
"$PY" "$FORGE" status --field-dir "$FIELD_DIR"

echo
echo "SCOUT artist vectors are built. Run START_SCOUT_ARTIST_FIELD.command."
