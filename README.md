# SCOUT Massive Artist Field

This package replaces SCOUT's 24 in-browser sample artists with a dedicated, pre-embedded artist discovery field.

## Architecture

1. **MusicBrainz official artist JSON dump** supplies the canonical artist registry and structured artist context.
2. **SCOUT artist forge** streams `artist.tar.xz` without extracting it, creates durable pending shards, and embeds each artist once through the existing ARBITER `/v1/embed` route.
3. **SCOUT artist server** hot-adds immutable vector shards, performs server-side cosine search, and applies explicit metadata filters before ranking.
4. **SCOUT frontend** sends one natural-language brief to `/field/v1/search`. The existing development surface still uses `/v1/compare` over the small set of competing next moves.

No Spotify catalog data is ingested. Spotify's current developer policy prohibits using Spotify content to train or otherwise ingest into an AI/ML model. Spotify links can later be attached as outbound identifiers only when properly licensed and authorized.

## Build

Double-click:

```text
DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command
```

Defaults:

- Field: `~/SCOUT_ARTIST_FIELD`
- Runtime: `~/SCOUT_ARTIST_SYSTEM`
- Data: `~/SCOUT_ARTIST_DATA`
- Embed endpoint: `http://127.0.0.1:8000/v1/embed`
- Selection: `discoverable`

The `discoverable` selection rejects name-only rows and requires at least two structured meaning signals. To keep every named MusicBrainz artist instead:

```bash
SCOUT_SELECTION=all ~/Downloads/SCOUT_MASSIVE_ARTIST_FIELD/DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command
```

For a small pipeline proof before the full build:

```bash
SCOUT_LIMIT=10000 ~/Downloads/SCOUT_MASSIVE_ARTIST_FIELD/DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command
```

MusicBrainz core artist data is CC0. Genres, tags, ratings, annotations, and other derived data have different licensing. They are excluded by default. Enable only after accepting the derived-data terms:

```bash
SCOUT_ALLOW_DERIVED_TAGS=1 .../DOWNLOAD_AND_BUILD_SCOUT_ARTIST_FIELD.command
```

## Start

Double-click:

```text
START_SCOUT_ARTIST_FIELD.command
```

SCOUT opens at `http://127.0.0.1:8790/` and searches the local pre-embedded field. The server endpoint is:

```text
POST http://127.0.0.1:8790/field/v1/search
```

Example request:

```json
{
  "text": "an emotionally raw alternative R&B vocalist with mainstream crossover potential",
  "k": 25,
  "filters": {
    "source": "musicbrainz-artists",
    "type": "artist",
    "gender": "female"
  }
}
```

## Artist images

After SCOUT has been started once and the live SQLite index exists, run:

```text
ENRICH_SCOUT_ARTIST_IMAGES.command
```

This follows MusicBrainz Wikidata relationships, reads Wikidata P18, and stores a Wikimedia Commons thumbnail together with its source page, creator, credit, and license metadata. It updates display metadata only; embeddings are not recalculated. Run it repeatedly to continue through additional artists.

## Current scope

This first massive field establishes the canonical artist layer. The next enrichment lanes should attach, without replacing the MusicBrainz identity:

- MusicBrainz release-group context for repertoire and career history
- ListenBrainz popularity and audience signals
- Wikimedia Commons images with license and attribution metadata
- label and distributor roster relationships
- official social and website links
- territory, language, touring, chart, playlist, and momentum signals from licensed sources

These enrichments should update metadata and, where they materially change meaning, generate a new versioned artist embedding rather than mutating an old vector silently.
