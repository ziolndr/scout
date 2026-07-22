#!/usr/bin/env python3
"""
ARBITER FIELD FORGE
===================

Build a universal, sharded, pre-embedded ARBITER field from massive public or
private datasets without re-embedding stable content at query time.

Core invariant:
    candidate text -> existing /v1/embed -> 72D ARBITER representation -> store once

No new ARBITER route is introduced.

Supported today:
  - initialize a universal field
  - import an existing pre-embedded SUMMON field without re-embedding
  - ingest OpenAlex Works from the API
  - ingest OpenAlex Works from the official JSONL .gz snapshot
  - ingest Open Library Works from the official monthly dump
  - ingest MusicBrainz recordings from a locally imported MusicBrainz PostgreSQL DB
  - ingest millions of MusicBrainz artists from the official artist JSON dump
  - ingest arbitrary JSONL / JSONL.GZ private or public corpora
  - embed pending records through the existing /v1/embed endpoint
  - resume interrupted embedding jobs safely
  - write immutable source shards with metadata, vectors, norms and manifests
  - verify all shards

Field layout:
  FIELD_DIR/
    field.json
    pending/
      openalex/
        pending-000001.jsonl
        pending-000002.jsonl
    shards/
      entertainment/
        shard-000001/
          manifest.json
          metadata.jsonl.gz
          vectors.f32
          norms.f32
      openalex/
        shard-000001/
          ...
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import random
import shutil
import sys
import time
import urllib.parse
import io
import tarfile
from pathlib import Path
from typing import Any, Iterable, Iterator

try:
    import numpy as np
except ImportError as exc:
    raise SystemExit("Missing dependency: numpy. Install with: pip install numpy") from exc

try:
    import requests
except ImportError as exc:
    raise SystemExit("Missing dependency: requests. Install with: pip install requests") from exc


DEFAULT_ARBITER_EMBED = "http://127.0.0.1:8000/v1/embed"
OPENALEX_API = "https://api.openalex.org/works"
FIELD_SCHEMA_VERSION = 1
DEFAULT_DIM = 72
DEFAULT_PENDING_SHARD_SIZE = 250_000
DEFAULT_EMBED_BATCH = 512
DEFAULT_MAX_TEXT_CHARS = 3_000


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------

def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    tmp.replace(path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(value)
    tmp.replace(path)


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        value = value.get("value") or value.get("text") or ""
    s = str(value)
    return " ".join(s.replace("\x00", " ").split()).strip()


def clamp_text(value: Any, max_chars: int) -> str:
    s = clean_text(value)
    if len(s) <= max_chars:
        return s
    cut = s[:max_chars].rsplit(" ", 1)[0].strip()
    return cut or s[:max_chars]


def normalize_doi(value: Any) -> str:
    s = clean_text(value).lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    return s.strip()


def normalize_isbn(value: Any) -> str:
    return "".join(ch for ch in clean_text(value).upper() if ch.isdigit() or ch == "X")


def record_id(source: str, source_id: Any) -> str:
    return f"{source}:{clean_text(source_id)}"


def human_count(n: int) -> str:
    return f"{n:,}"


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "rt"
    with opener(path, mode, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except Exception:
                continue
            if isinstance(value, dict):
                yield value


def response_vectors(data: Any) -> list[list[float]]:
    if isinstance(data, dict):
        if isinstance(data.get("vectors"), list):
            return data["vectors"]
        if isinstance(data.get("embeddings"), list):
            return data["embeddings"]
    if isinstance(data, list):
        return data
    raise RuntimeError("Unrecognized /v1/embed response shape")


def post_json(
    url: str,
    payload: dict[str, Any],
    attempts: int = 6,
    timeout: int = 900,
) -> Any:
    last: Exception | None = None
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "ngrok-skip-browser-warning": "true",
        "User-Agent": "ARBITER-Field-Forge/1.0",
    }

    for attempt in range(attempts):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if r.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:240]}")
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                delay = min(20.0, 0.9 * (2 ** attempt)) + random.random() * 0.4
                time.sleep(delay)

    raise RuntimeError(f"POST failed: {url}: {last}")


# -----------------------------------------------------------------------------
# Field registry
# -----------------------------------------------------------------------------

def field_json_path(field_dir: Path) -> Path:
    return field_dir / "field.json"


def init_field(field_dir: Path, name: str, dim: int, use_freq: bool) -> dict[str, Any]:
    field_dir.mkdir(parents=True, exist_ok=True)
    (field_dir / "pending").mkdir(exist_ok=True)
    (field_dir / "shards").mkdir(exist_ok=True)

    path = field_json_path(field_dir)
    if path.exists():
        return json.loads(path.read_text())

    field = {
        "schema_version": FIELD_SCHEMA_VERSION,
        "name": name,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "dim": int(dim),
        "dtype": "float32-little-endian",
        "use_freq": bool(use_freq),
        "total_records": 0,
        "pending_records": 0,
        "embedded_records": 0,
        "sources": {},
        "shards": [],
    }
    atomic_json(path, field)
    return field


def load_field(field_dir: Path) -> dict[str, Any]:
    path = field_json_path(field_dir)
    if not path.exists():
        raise SystemExit(f"No field.json at {path}. Run `init` first.")
    return json.loads(path.read_text())


def save_field(field_dir: Path, field: dict[str, Any]) -> None:
    field["updated_at"] = utc_now()
    atomic_json(field_json_path(field_dir), field)


def recompute_field_counts(field: dict[str, Any]) -> None:
    embedded = sum(int(s.get("count") or 0) for s in field.get("shards", []))
    pending = sum(
        int(p.get("count") or 0)
        for source in field.get("sources", {}).values()
        for p in source.get("pending", [])
        if p.get("status") != "embedded"
    )
    field["embedded_records"] = embedded
    field["pending_records"] = pending
    field["total_records"] = embedded + pending


def ensure_source(field: dict[str, Any], source: str) -> dict[str, Any]:
    sources = field.setdefault("sources", {})
    if source not in sources:
        sources[source] = {
            "created_at": utc_now(),
            "pending": [],
            "ingested_records": 0,
            "embedded_records": 0,
        }
    return sources[source]


# -----------------------------------------------------------------------------
# Canonical record format
# -----------------------------------------------------------------------------

def canonical_record(
    *,
    source: str,
    source_id: Any,
    record_type: str,
    title: Any,
    candidate_text: str,
    year: Any = None,
    aliases: list[str] | None = None,
    external_url: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rid = record_id(source, source_id)
    aliases_clean = []
    seen = set()

    for alias in [rid, *(aliases or [])]:
        alias = clean_text(alias)
        if alias and alias not in seen:
            seen.add(alias)
            aliases_clean.append(alias)

    out = {
        "id": rid,
        "source": source,
        "source_id": clean_text(source_id),
        "type": clean_text(record_type) or "item",
        "title": clean_text(title),
        "candidate_text": clean_text(candidate_text),
        "aliases": aliases_clean,
    }

    if year not in (None, ""):
        out["year"] = clean_text(year)
    if external_url:
        out["external_url"] = external_url
    if metadata:
        out["metadata"] = metadata

    return out


def chunk_long_text(
    *,
    source: str,
    source_id: Any,
    record_type: str,
    title: str,
    text: str,
    chunk_chars: int,
    overlap_chars: int,
    year: Any = None,
    aliases: list[str] | None = None,
    external_url: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    text = clean_text(text)

    if len(text) <= chunk_chars:
        yield canonical_record(
            source=source,
            source_id=source_id,
            record_type=record_type,
            title=title,
            candidate_text=text,
            year=year,
            aliases=aliases,
            external_url=external_url,
            metadata=metadata,
        )
        return

    step = max(1, chunk_chars - max(0, overlap_chars))
    chunk_index = 0

    for start in range(0, len(text), step):
        chunk = text[start:start + chunk_chars].strip()
        if not chunk:
            continue
        chunk_index += 1
        cid = f"{source_id}#chunk-{chunk_index:05d}"
        yield canonical_record(
            source=source,
            source_id=cid,
            record_type=record_type,
            title=title,
            candidate_text=chunk,
            year=year,
            aliases=[*(aliases or []), f"{source}:{source_id}"],
            external_url=external_url,
            metadata={
                **(metadata or {}),
                "parent_source_id": clean_text(source_id),
                "chunk_index": chunk_index,
            },
        )

        if start + chunk_chars >= len(text):
            break


# -----------------------------------------------------------------------------
# Pending shard writer
# -----------------------------------------------------------------------------

class PendingWriter:
    def __init__(
        self,
        field_dir: Path,
        field: dict[str, Any],
        source: str,
        shard_size: int,
    ) -> None:
        self.field_dir = field_dir
        self.field = field
        self.source = source
        self.shard_size = max(1, int(shard_size))
        self.source_state = ensure_source(field, source)
        self.pending_dir = field_dir / "pending" / source
        self.pending_dir.mkdir(parents=True, exist_ok=True)

        existing = list(self.pending_dir.glob("pending-*.jsonl"))
        self.next_index = 1
        if existing:
            nums = []
            for p in existing:
                try:
                    nums.append(int(p.stem.split("-")[-1]))
                except Exception:
                    pass
            if nums:
                self.next_index = max(nums) + 1

        self.file = None
        self.path: Path | None = None
        self.count = 0
        self.total_written = 0
        self._known_ids: set[str] = set()

    def _open(self) -> None:
        self.path = self.pending_dir / f"pending-{self.next_index:06d}.jsonl"
        self.file = self.path.open("w", encoding="utf-8")
        self.count = 0
        self._known_ids.clear()

    def _close_current(self) -> None:
        if not self.file or not self.path:
            return

        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()

        if self.count > 0:
            rel = str(self.path.relative_to(self.field_dir))
            self.source_state.setdefault("pending", []).append({
                "path": rel,
                "count": self.count,
                "status": "pending",
                "created_at": utc_now(),
            })
            self.source_state["ingested_records"] = int(
                self.source_state.get("ingested_records") or 0
            ) + self.count
            recompute_field_counts(self.field)
            save_field(self.field_dir, self.field)

        self.file = None
        self.path = None
        self.count = 0
        self.next_index += 1
        self._known_ids.clear()

    def add(self, record: dict[str, Any]) -> bool:
        rid = clean_text(record.get("id"))
        candidate_text = clean_text(record.get("candidate_text"))

        if not rid or not candidate_text:
            return False

        if rid in self._known_ids:
            return False

        if self.file is None:
            self._open()

        if self.count >= self.shard_size:
            self._close_current()
            self._open()

        self.file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._known_ids.add(rid)
        self.count += 1
        self.total_written += 1
        return True

    def close(self) -> None:
        self._close_current()


# -----------------------------------------------------------------------------
# Existing SUMMON field import
# -----------------------------------------------------------------------------

def entertainment_candidate(item: dict[str, Any], max_chars: int) -> str:
    parts = [
        f"{clean_text(item.get('title'))}.",
        f"Format: {clean_text(item.get('type'))}.",
    ]

    publisher = clean_text(item.get("publisher"))
    availability = clean_text(item.get("availability"))
    genres = [clean_text(x) for x in (item.get("genres") or []) if clean_text(x)]
    overview = clamp_text(item.get("overview"), max_chars)

    if publisher:
        parts.append(f"Publisher or rights context: {publisher}.")
    if availability:
        parts.append(f"Availability: {availability}.")
    if genres:
        parts.append(f"Themes and genres: {', '.join(genres)}.")
    parts.append(f"Description: {overview}")

    return clean_text(" ".join(parts))


def import_preembedded(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    source_dir = Path(args.source_dir)
    field = load_field(field_dir)

    manifest_path = source_dir / "SUMMON_field_manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    count = int(manifest.get("count") or 0)
    dim = int(manifest.get("dim") or field["dim"])
    use_freq = bool(manifest.get("use_freq", field["use_freq"]))

    if dim != int(field["dim"]):
        raise SystemExit(f"Dimension mismatch: source {dim} != field {field['dim']}")
    if use_freq != bool(field["use_freq"]):
        raise SystemExit(
            f"use_freq mismatch: source {use_freq} != field {field['use_freq']}"
        )

    metadata_files = manifest.get("metadata_files")
    if not metadata_files:
        metadata_files = [manifest.get("metadata_file") or "SUMMON_field_metadata.json"]

    metadata: list[dict[str, Any]] = []
    for filename in metadata_files:
        path = source_dir / filename
        if not path.exists():
            raise SystemExit(f"Missing metadata file: {path}")
        part = json.loads(path.read_text())
        if not isinstance(part, list):
            raise SystemExit(f"Metadata file is not a JSON array: {path}")
        metadata.extend(part)

    if len(metadata) != count:
        raise SystemExit(f"Metadata count {len(metadata)} != manifest count {count}")

    vectors_src = source_dir / (manifest.get("vectors_file") or "SUMMON_field_vectors.f32")
    norms_src = source_dir / (manifest.get("norms_file") or "SUMMON_field_norms.f32")

    expected_vectors = count * dim * 4
    expected_norms = count * 4

    if vectors_src.stat().st_size != expected_vectors:
        raise SystemExit(
            f"Vector byte mismatch: {vectors_src.stat().st_size} != {expected_vectors}"
        )
    if norms_src.stat().st_size != expected_norms:
        raise SystemExit(
            f"Norm byte mismatch: {norms_src.stat().st_size} != {expected_norms}"
        )

    source = args.source
    source_state = ensure_source(field, source)

    existing_nums = [
        int(Path(s["path"]).parent.name.split("-")[-1])
        for s in field.get("shards", [])
        if s.get("source") == source and "shard-" in Path(s["path"]).parent.name
    ]
    shard_num = max(existing_nums, default=0) + 1
    shard_dir = field_dir / "shards" / source / f"shard-{shard_num:06d}"
    shard_dir.mkdir(parents=True, exist_ok=False)

    metadata_out = shard_dir / "metadata.jsonl.gz"
    vectors_out = shard_dir / "vectors.f32"
    norms_out = shard_dir / "norms.f32"

    print(f"[import] writing canonical metadata for {human_count(count)} records")
    with gzip.open(metadata_out, "wt", encoding="utf-8", compresslevel=6) as gz:
        for i, item in enumerate(metadata):
            title = clean_text(item.get("title"))
            item_type = clean_text(item.get("type")) or "entertainment"
            source_id = (
                f"tmdb:{item.get('tmdbKind')}:{item.get('tmdbId')}"
                if item.get("tmdbId")
                else f"steam:{item.get('steamAppId')}"
                if item.get("steamAppId")
                else f"{item_type}:{i}:{title}"
            )
            aliases = []
            if item.get("tmdbId"):
                aliases.append(f"tmdb:{item.get('tmdbKind')}:{item.get('tmdbId')}")
            if item.get("steamAppId"):
                aliases.append(f"steam:{item.get('steamAppId')}")

            rec = canonical_record(
                source=source,
                source_id=source_id,
                record_type=item_type,
                title=title,
                candidate_text=entertainment_candidate(item, args.max_text_chars),
                year=item.get("year"),
                aliases=aliases,
                external_url=item.get("externalUrl"),
                metadata={
                    k: v
                    for k, v in item.items()
                    if k not in {"title", "type", "overview", "externalUrl"}
                },
            )
            gz.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

    if args.copy:
        shutil.copy2(vectors_src, vectors_out)
        shutil.copy2(norms_src, norms_out)
    else:
        try:
            os.link(vectors_src, vectors_out)
            os.link(norms_src, norms_out)
        except OSError:
            shutil.copy2(vectors_src, vectors_out)
            shutil.copy2(norms_src, norms_out)

    shard_manifest = {
        "schema_version": FIELD_SCHEMA_VERSION,
        "source": source,
        "count": count,
        "dim": dim,
        "dtype": "float32-little-endian",
        "use_freq": use_freq,
        "created_at": utc_now(),
        "metadata_file": "metadata.jsonl.gz",
        "vectors_file": "vectors.f32",
        "norms_file": "norms.f32",
        "metadata_sha256": sha256_file(metadata_out),
        "vectors_sha256": sha256_file(vectors_out),
        "norms_sha256": sha256_file(norms_out),
        "imported_from": str(source_dir.resolve()),
        "source_manifest": manifest,
    }
    atomic_json(shard_dir / "manifest.json", shard_manifest)

    rel_manifest = str((shard_dir / "manifest.json").relative_to(field_dir))
    field.setdefault("shards", []).append({
        "source": source,
        "path": rel_manifest,
        "count": count,
        "created_at": shard_manifest["created_at"],
    })
    source_state["embedded_records"] = int(source_state.get("embedded_records") or 0) + count
    recompute_field_counts(field)
    save_field(field_dir, field)

    print(f"[import] DONE · {human_count(count)} pre-embedded records imported")
    print(f"[import] shard: {shard_dir}")


# -----------------------------------------------------------------------------
# OpenAlex adapters
# -----------------------------------------------------------------------------

def openalex_abstract(inv: Any) -> str:
    if not isinstance(inv, dict) or not inv:
        return ""

    max_pos = -1
    for positions in inv.values():
        if isinstance(positions, list):
            for pos in positions:
                try:
                    max_pos = max(max_pos, int(pos))
                except Exception:
                    pass

    if max_pos < 0:
        return ""

    words = [""] * (max_pos + 1)
    for word, positions in inv.items():
        if not isinstance(positions, list):
            continue
        for pos in positions:
            try:
                i = int(pos)
            except Exception:
                continue
            if 0 <= i < len(words):
                words[i] = clean_text(word)

    return clean_text(" ".join(words))


def openalex_record(
    work: dict[str, Any],
    *,
    max_text_chars: int,
    require_abstract: bool,
) -> dict[str, Any] | None:
    source_id = clean_text(work.get("id")).rstrip("/").split("/")[-1]
    title = clean_text(work.get("display_name") or work.get("title"))
    abstract = openalex_abstract(work.get("abstract_inverted_index"))

    topics = []
    for topic in work.get("topics") or []:
        if isinstance(topic, dict):
            name = clean_text(topic.get("display_name"))
            if name:
                topics.append(name)

    keywords = []
    for kw in work.get("keywords") or []:
        if isinstance(kw, dict):
            name = clean_text(kw.get("display_name") or kw.get("keyword"))
            if name:
                keywords.append(name)

    if require_abstract and not abstract:
        return None

    if not title:
        return None

    work_type = clean_text(work.get("type")) or "scholarly work"
    year = work.get("publication_year")

    parts = [
        f"{title}.",
        f"Format: scholarly {work_type}.",
    ]
    if year:
        parts.append(f"Year: {year}.")
    if topics:
        parts.append(f"Topics: {', '.join(topics[:12])}.")
    if keywords:
        parts.append(f"Keywords: {', '.join(keywords[:16])}.")
    if abstract:
        parts.append(f"Abstract: {clamp_text(abstract, max_text_chars)}")

    authors = []
    for authorship in work.get("authorships") or []:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") or {}
        name = clean_text(author.get("display_name"))
        if name:
            authors.append(name)

    doi = normalize_doi(work.get("doi"))
    aliases = [f"openalex:{source_id}"]
    if doi:
        aliases.append(f"doi:{doi}")

    metadata = {
        "openalex_id": source_id,
        "doi": doi or None,
        "authors": authors[:50],
        "topics": topics[:24],
        "keywords": keywords[:32],
        "language": work.get("language"),
        "cited_by_count": work.get("cited_by_count"),
        "is_retracted": work.get("is_retracted"),
    }
    metadata = {k: v for k, v in metadata.items() if v not in (None, "", [], {})}

    return canonical_record(
        source="openalex",
        source_id=source_id,
        record_type="paper",
        title=title,
        candidate_text=" ".join(parts),
        year=year,
        aliases=aliases,
        external_url=clean_text(work.get("id")),
        metadata=metadata,
    )


def ingest_openalex_api(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, "openalex", args.pending_shard_size)

    params: dict[str, Any] = {
        "per-page": 200,
        "cursor": "*",
        "select": ",".join([
            "id",
            "doi",
            "display_name",
            "publication_year",
            "type",
            "authorships",
            "topics",
            "keywords",
            "abstract_inverted_index",
            "language",
            "cited_by_count",
            "is_retracted",
        ]),
    }

    filters = []
    if args.require_abstract:
        filters.append("has_abstract:true")
    if args.from_year:
        filters.append(f"from_publication_date:{args.from_year}-01-01")
    if args.to_year:
        filters.append(f"to_publication_date:{args.to_year}-12-31")
    if filters:
        params["filter"] = ",".join(filters)
    if args.api_key:
        params["api_key"] = args.api_key
    if args.mailto:
        params["mailto"] = args.mailto

    accepted = 0
    cursor = "*"

    try:
        while True:
            params["cursor"] = cursor
            r = requests.get(
                OPENALEX_API,
                params=params,
                headers={"User-Agent": "ARBITER-Field-Forge/1.0"},
                timeout=120,
            )
            r.raise_for_status()
            data = r.json()

            results = data.get("results") or []
            if not results:
                break

            for work in results:
                rec = openalex_record(
                    work,
                    max_text_chars=args.max_text_chars,
                    require_abstract=args.require_abstract,
                )
                if rec and writer.add(rec):
                    accepted += 1
                    if accepted % 10_000 == 0:
                        print(f"[openalex-api] accepted {human_count(accepted)}")

                if args.limit and accepted >= args.limit:
                    break

            if args.limit and accepted >= args.limit:
                break

            cursor = clean_text((data.get("meta") or {}).get("next_cursor"))
            if not cursor:
                break

            if args.delay:
                time.sleep(args.delay)

    finally:
        writer.close()

    print(f"[openalex-api] DONE · {human_count(accepted)} records")


def ingest_openalex_snapshot(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, "openalex", args.pending_shard_size)

    files: list[Path] = []
    for pattern in args.glob:
        files.extend(Path().glob(pattern))

    files = sorted({p.resolve() for p in files if p.is_file()})
    if not files:
        raise SystemExit("No snapshot files matched.")

    accepted = 0
    seen_rows = 0

    try:
        for file_index, path in enumerate(files, 1):
            print(f"[openalex-snapshot] file {file_index}/{len(files)} · {path}")
            for work in iter_jsonl(path):
                seen_rows += 1
                rec = openalex_record(
                    work,
                    max_text_chars=args.max_text_chars,
                    require_abstract=args.require_abstract,
                )
                if rec and writer.add(rec):
                    accepted += 1

                if accepted and accepted % 100_000 == 0:
                    print(
                        f"[openalex-snapshot] accepted {human_count(accepted)} · "
                        f"seen {human_count(seen_rows)}"
                    )

                if args.limit and accepted >= args.limit:
                    break

            if args.limit and accepted >= args.limit:
                break

    finally:
        writer.close()

    print(
        f"[openalex-snapshot] DONE · accepted {human_count(accepted)} · "
        f"seen {human_count(seen_rows)}"
    )


# -----------------------------------------------------------------------------
# Open Library Works dump adapter
# -----------------------------------------------------------------------------

def openlibrary_description(value: Any) -> str:
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, dict):
        return clean_text(value.get("value") or value.get("text"))
    return ""


def openlibrary_record(
    work: dict[str, Any],
    *,
    max_text_chars: int,
    min_meaning_chars: int,
) -> dict[str, Any] | None:
    key = clean_text(work.get("key"))
    source_id = key.strip("/").split("/")[-1]
    title = clean_text(work.get("title"))

    if not source_id or not title:
        return None

    description = openlibrary_description(work.get("description"))
    subjects = [clean_text(x) for x in (work.get("subjects") or []) if clean_text(x)]
    people = [clean_text(x) for x in (work.get("subject_people") or []) if clean_text(x)]
    places = [clean_text(x) for x in (work.get("subject_places") or []) if clean_text(x)]
    times = [clean_text(x) for x in (work.get("subject_times") or []) if clean_text(x)]

    meaning = " ".join([description, *subjects, *people, *places, *times]).strip()
    if len(meaning) < min_meaning_chars:
        return None

    parts = [f"{title}.", "Format: book."]
    if subjects:
        parts.append(f"Subjects: {', '.join(subjects[:24])}.")
    if people:
        parts.append(f"People: {', '.join(people[:12])}.")
    if places:
        parts.append(f"Places: {', '.join(places[:12])}.")
    if times:
        parts.append(f"Times: {', '.join(times[:12])}.")
    if description:
        parts.append(f"Description: {clamp_text(description, max_text_chars)}")

    covers = work.get("covers") or []
    external_url = f"https://openlibrary.org{key}" if key.startswith("/") else None

    return canonical_record(
        source="openlibrary",
        source_id=source_id,
        record_type="book",
        title=title,
        candidate_text=" ".join(parts),
        year=work.get("first_publish_date"),
        aliases=[f"openlibrary:{source_id}"],
        external_url=external_url,
        metadata={
            "subjects": subjects[:50],
            "subject_people": people[:30],
            "subject_places": places[:30],
            "subject_times": times[:30],
            "covers": covers[:10],
        },
    )


def ingest_openlibrary_dump(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, "openlibrary", args.pending_shard_size)

    path = Path(args.dump)
    if not path.exists():
        raise SystemExit(f"Missing dump: {path}")

    opener = gzip.open if path.suffix == ".gz" else open
    accepted = 0
    seen_rows = 0

    try:
        with opener(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                seen_rows += 1
                parts = line.rstrip("\n").split("\t")
                if not parts:
                    continue

                try:
                    work = json.loads(parts[-1])
                except Exception:
                    continue

                rec = openlibrary_record(
                    work,
                    max_text_chars=args.max_text_chars,
                    min_meaning_chars=args.min_meaning_chars,
                )
                if rec and writer.add(rec):
                    accepted += 1

                if accepted and accepted % 100_000 == 0:
                    print(
                        f"[openlibrary] accepted {human_count(accepted)} · "
                        f"seen {human_count(seen_rows)}"
                    )

                if args.limit and accepted >= args.limit:
                    break

    finally:
        writer.close()

    print(
        f"[openlibrary] DONE · accepted {human_count(accepted)} · "
        f"seen {human_count(seen_rows)}"
    )


# -----------------------------------------------------------------------------
# MusicBrainz PostgreSQL adapter
# -----------------------------------------------------------------------------

def ingest_musicbrainz_postgres(args: argparse.Namespace) -> None:
    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit(
            "MusicBrainz PostgreSQL ingestion requires psycopg. "
            "Install with: pip install 'psycopg[binary]'"
        ) from exc

    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, "musicbrainz", args.pending_shard_size)

    sql = """
    SELECT
      r.id,
      r.gid::text AS mbid,
      r.name AS title,
      COALESCE(ac.artists, '') AS artists,
      COALESCE(tg.tags, '') AS tags,
      r.length,
      r.comment
    FROM recording r
    LEFT JOIN (
      SELECT
        acn.artist_credit,
        string_agg(acn.name, ' / ' ORDER BY acn.position) AS artists
      FROM artist_credit_name acn
      GROUP BY acn.artist_credit
    ) ac ON ac.artist_credit = r.artist_credit
    LEFT JOIN (
      SELECT
        rt.recording,
        string_agg(t.name, ', ' ORDER BY rt.count DESC, t.name) AS tags
      FROM recording_tag rt
      JOIN tag t ON t.id = rt.tag
      GROUP BY rt.recording
    ) tg ON tg.recording = r.id
    ORDER BY r.id
    """

    accepted = 0
    seen_rows = 0

    with psycopg.connect(args.dsn) as conn:
        with conn.cursor(name="arbiter_musicbrainz_stream") as cur:
            cur.itersize = args.fetch_size
            cur.execute(sql)

            try:
                for row in cur:
                    seen_rows += 1
                    rid, mbid, title, artists, tags, length_ms, comment = row

                    title = clean_text(title)
                    artists = clean_text(artists)
                    tag_list = [clean_text(x) for x in clean_text(tags).split(",") if clean_text(x)]
                    comment = clean_text(comment)

                    if args.require_tags and not tag_list:
                        continue
                    if not title:
                        continue

                    parts = [
                        f"{title}.",
                        "Format: music recording.",
                    ]
                    if artists:
                        parts.append(f"Artist: {artists}.")
                    if tag_list:
                        parts.append(f"Tags and genres: {', '.join(tag_list[:24])}.")
                    if comment:
                        parts.append(f"Context: {comment}.")
                    if length_ms:
                        parts.append(f"Duration: {int(length_ms)} milliseconds.")

                    rec = canonical_record(
                        source="musicbrainz",
                        source_id=f"recording:{mbid}",
                        record_type="music",
                        title=title,
                        candidate_text=" ".join(parts),
                        aliases=[f"mbid:recording:{mbid}"],
                        external_url=f"https://musicbrainz.org/recording/{mbid}",
                        metadata={
                            "artists": artists,
                            "tags": tag_list[:50],
                            "length_ms": length_ms,
                            "comment": comment,
                            "musicbrainz_recording_id": rid,
                        },
                    )

                    if writer.add(rec):
                        accepted += 1

                    if accepted and accepted % 100_000 == 0:
                        print(
                            f"[musicbrainz] accepted {human_count(accepted)} · "
                            f"seen {human_count(seen_rows)}"
                        )

                    if args.limit and accepted >= args.limit:
                        break

            finally:
                writer.close()

    print(
        f"[musicbrainz] DONE · accepted {human_count(accepted)} · "
        f"seen {human_count(seen_rows)}"
    )



# -----------------------------------------------------------------------------
# MusicBrainz artist JSON dump adapter (SCOUT)
# -----------------------------------------------------------------------------

SCOUT_ARTIST_SOURCE = "musicbrainz-artists"


def _mb_value(obj: Any, key: str) -> str:
    if not isinstance(obj, dict):
        return ""
    value = obj.get(key)
    if isinstance(value, dict):
        value = value.get("name") or value.get("value") or value.get("text")
    return clean_text(value)


def _mb_list_names(value: Any, limit: int = 24) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(value, list):
        return out
    for item in value:
        if isinstance(item, dict):
            name = clean_text(item.get("name") or item.get("value") or item.get("sort-name"))
        else:
            name = clean_text(item)
        key = name.casefold()
        if name and key not in seen:
            seen.add(key)
            out.append(name)
        if len(out) >= limit:
            break
    return out


def _mb_relation_summary(relations: Any, limit: int = 20) -> tuple[list[str], dict[str, str]]:
    summaries: list[str] = []
    links: dict[str, str] = {}
    if not isinstance(relations, list):
        return summaries, links
    for rel in relations:
        if not isinstance(rel, dict):
            continue
        rel_type = clean_text(rel.get("type"))
        target = rel.get("url") or rel.get("artist") or rel.get("label") or rel.get("work") or rel.get("recording")
        target_name = ""
        resource = ""
        if isinstance(target, dict):
            target_name = clean_text(target.get("name") or target.get("title") or target.get("id"))
            resource = clean_text(target.get("resource"))
        elif target:
            target_name = clean_text(target)
        if resource:
            low = resource.lower()
            if "wikidata.org/wiki/" in low:
                links["wikidata_url"] = resource
                links["wikidata_id"] = resource.rstrip("/").rsplit("/", 1)[-1]
            elif "wikipedia.org/wiki/" in low:
                links["wikipedia_url"] = resource
            elif "instagram.com/" in low:
                links["instagram_url"] = resource
            elif "youtube.com/" in low or "youtu.be/" in low:
                links["youtube_url"] = resource
            elif "bandcamp.com" in low:
                links["bandcamp_url"] = resource
            elif rel_type.lower() in {"official homepage", "official site"}:
                links["official_url"] = resource
        label = target_name or resource
        if rel_type and label:
            summaries.append(f"{rel_type}: {label}")
        elif label:
            summaries.append(label)
        if len(summaries) >= limit:
            break
    return summaries, links


def iter_musicbrainz_artist_dump(path: Path) -> Iterator[dict[str, Any]]:
    """Stream the official artist.tar.xz without extracting it to disk."""
    with tarfile.open(path, mode="r|xz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            fh = archive.extractfile(member)
            if fh is None:
                continue
            for raw_line in fh:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    yield obj


def musicbrainz_artist_record(
    artist: dict[str, Any],
    *,
    selection: str,
    allow_derived_tags: bool,
    max_text_chars: int,
) -> dict[str, Any] | None:
    mbid = clean_text(artist.get("id"))
    name = clean_text(artist.get("name"))
    if not mbid or not name:
        return None

    artist_type = clean_text(artist.get("type")) or "Artist"
    gender = clean_text(artist.get("gender")).lower()
    country = clean_text(artist.get("country"))
    area = _mb_value(artist.get("area"), "name")
    begin_area = _mb_value(artist.get("begin-area"), "name")
    disambiguation = clean_text(artist.get("disambiguation"))
    aliases = _mb_list_names(artist.get("aliases"), 30)
    relation_summaries, relation_links = _mb_relation_summary(artist.get("relations"), 24)

    life = artist.get("life-span") if isinstance(artist.get("life-span"), dict) else {}
    begin = clean_text(life.get("begin"))
    end = clean_text(life.get("end"))
    ended = bool(life.get("ended"))

    genres: list[str] = []
    tags: list[str] = []
    if allow_derived_tags:
        genres = _mb_list_names(artist.get("genres"), 24)
        tags = _mb_list_names(artist.get("tags"), 36)

    meaning_signals = sum(bool(x) for x in (
        artist_type and artist_type.lower() != "artist",
        gender,
        country,
        area,
        begin_area,
        disambiguation,
        aliases,
        relation_summaries,
        begin,
        end,
        genres,
        tags,
    ))
    if selection == "discoverable" and meaning_signals < 2:
        return None

    parts = [f"{name}.", "Format: musical artist.", f"Artist type: {artist_type}."]
    if gender:
        parts.append(f"Gender: {gender}.")
    origin_bits = [x for x in (begin_area, area, country) if x]
    if origin_bits:
        parts.append(f"Origin and geographic context: {', '.join(dict.fromkeys(origin_bits))}.")
    if begin or end:
        span = " to ".join(x for x in (begin, end) if x)
        parts.append(f"Active lifespan: {span}.")
    if ended:
        parts.append("MusicBrainz marks this artist entity as ended.")
    if aliases:
        parts.append(f"Also known as: {', '.join(aliases[:20])}.")
    if disambiguation:
        parts.append(f"Context: {disambiguation}.")
    if genres:
        parts.append(f"Genres: {', '.join(genres)}.")
    if tags:
        parts.append(f"Community tags: {', '.join(tags[:28])}.")
    if relation_summaries:
        parts.append(f"Public relationships and identities: {'; '.join(relation_summaries)}.")

    candidate_text = clamp_text(" ".join(parts), max_text_chars)
    origin = area or begin_area or country
    external_url = f"https://musicbrainz.org/artist/{mbid}"
    metadata = {
        "musicbrainz_artist_id": mbid,
        "artist_type": artist_type,
        "gender": gender,
        "country": country,
        "area": area,
        "begin_area": begin_area,
        "begin": begin,
        "end": end,
        "ended": ended,
        "disambiguation": disambiguation,
        "aliases": aliases,
        "relations": relation_summaries,
        "genres": genres,
        "tags": tags,
        "origin": origin,
        "stage": "catalog artist",
        **relation_links,
    }
    return canonical_record(
        source=SCOUT_ARTIST_SOURCE,
        source_id=mbid,
        record_type="artist",
        title=name,
        candidate_text=candidate_text,
        year=begin[:4] if begin else None,
        aliases=[f"mbid:artist:{mbid}", *aliases],
        external_url=external_url,
        metadata={k: v for k, v in metadata.items() if v not in (None, "", [], {})},
    )


def ingest_musicbrainz_artists_json(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, SCOUT_ARTIST_SOURCE, args.pending_shard_size)
    path = Path(args.dump).expanduser()
    if not path.exists():
        raise SystemExit(f"Missing MusicBrainz artist dump: {path}")

    accepted = 0
    seen_rows = 0
    try:
        for artist in iter_musicbrainz_artist_dump(path):
            seen_rows += 1
            rec = musicbrainz_artist_record(
                artist,
                selection=args.selection,
                allow_derived_tags=args.allow_derived_tags,
                max_text_chars=args.max_text_chars,
            )
            if rec and writer.add(rec):
                accepted += 1
            if accepted and accepted % 100_000 == 0:
                print(f"[musicbrainz-artists] accepted {human_count(accepted)} · seen {human_count(seen_rows)}")
            if args.limit and accepted >= args.limit:
                break
    finally:
        writer.close()

    print(f"[musicbrainz-artists] DONE · accepted {human_count(accepted)} · seen {human_count(seen_rows)}")


# -----------------------------------------------------------------------------
# Generic JSONL adapter for private catalogs or other public corpora
# -----------------------------------------------------------------------------

def dotted_get(obj: dict[str, Any], path: str) -> Any:
    current: Any = obj
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def ingest_jsonl(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    writer = PendingWriter(field_dir, field, args.source, args.pending_shard_size)

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"Missing file: {path}")

    accepted = 0
    seen_rows = 0

    try:
        for obj in iter_jsonl(path):
            seen_rows += 1

            sid = dotted_get(obj, args.id_field)
            title = dotted_get(obj, args.title_field)
            text_value = dotted_get(obj, args.text_field)
            rec_type = dotted_get(obj, args.type_field) if args.type_field else args.default_type
            year = dotted_get(obj, args.year_field) if args.year_field else None
            url = dotted_get(obj, args.url_field) if args.url_field else None

            sid = clean_text(sid)
            title = clean_text(title)
            body = clean_text(text_value)

            if not sid or not body:
                continue

            prefix = []
            if title:
                prefix.append(f"{title}.")
            if rec_type:
                prefix.append(f"Format: {clean_text(rec_type)}.")
            prefix_text = " ".join(prefix)

            full_text = clean_text(f"{prefix_text} {body}")

            if args.chunk_chars and len(full_text) > args.chunk_chars:
                for rec in chunk_long_text(
                    source=args.source,
                    source_id=sid,
                    record_type=clean_text(rec_type) or args.default_type,
                    title=title,
                    text=full_text,
                    chunk_chars=args.chunk_chars,
                    overlap_chars=args.overlap_chars,
                    year=year,
                    external_url=clean_text(url) or None,
                    metadata={"original_record": obj if args.keep_original else None},
                ):
                    if writer.add(rec):
                        accepted += 1
            else:
                rec = canonical_record(
                    source=args.source,
                    source_id=sid,
                    record_type=clean_text(rec_type) or args.default_type,
                    title=title,
                    candidate_text=clamp_text(full_text, args.max_text_chars),
                    year=year,
                    external_url=clean_text(url) or None,
                    metadata={"original_record": obj if args.keep_original else None},
                )
                if writer.add(rec):
                    accepted += 1

            if accepted and accepted % 100_000 == 0:
                print(
                    f"[jsonl:{args.source}] accepted {human_count(accepted)} · "
                    f"seen {human_count(seen_rows)}"
                )

            if args.limit and accepted >= args.limit:
                break

    finally:
        writer.close()

    print(
        f"[jsonl:{args.source}] DONE · accepted {human_count(accepted)} · "
        f"seen {human_count(seen_rows)}"
    )


# -----------------------------------------------------------------------------
# Embed pending -> immutable shards
# -----------------------------------------------------------------------------

def pending_entries_for(
    field: dict[str, Any],
    source: str | None,
) -> Iterator[tuple[str, dict[str, Any]]]:
    for source_name, source_state in field.get("sources", {}).items():
        if source and source_name != source:
            continue
        for entry in source_state.get("pending", []):
            if entry.get("status") == "pending":
                yield source_name, entry


def next_shard_num(field: dict[str, Any], source: str) -> int:
    nums = []
    for shard in field.get("shards", []):
        if shard.get("source") != source:
            continue
        path = Path(shard.get("path") or "")
        for part in path.parts:
            if part.startswith("shard-"):
                try:
                    nums.append(int(part.split("-")[-1]))
                except Exception:
                    pass
    return max(nums, default=0) + 1


def update_pending_status(
    field: dict[str, Any],
    source: str,
    pending_path: str,
    status: str,
    embedded_shard_manifest: str | None = None,
) -> None:
    source_state = ensure_source(field, source)
    for entry in source_state.get("pending", []):
        if entry.get("path") == pending_path:
            entry["status"] = status
            if embedded_shard_manifest:
                entry["embedded_shard_manifest"] = embedded_shard_manifest
            entry["updated_at"] = utc_now()
            return


def embed_pending_file(
    *,
    field_dir: Path,
    field: dict[str, Any],
    source: str,
    entry: dict[str, Any],
    endpoint: str,
    batch_size: int,
    timeout: int,
    delete_pending: bool,
) -> None:
    pending_path = field_dir / entry["path"]
    expected_count = int(entry.get("count") or 0)
    dim = int(field["dim"])
    use_freq = bool(field["use_freq"])

    shard_num = next_shard_num(field, source)
    source_shards_dir = field_dir / "shards" / source
    source_shards_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = source_shards_dir / f"shard-{shard_num:06d}.tmp"
    final_dir = source_shards_dir / f"shard-{shard_num:06d}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    progress_path = tmp_dir / "progress.json"
    metadata_tmp = tmp_dir / "metadata.jsonl"
    vectors_path = tmp_dir / "vectors.f32"
    norms_path = tmp_dir / "norms.f32"

    rows_done = 0
    if progress_path.exists():
        progress = json.loads(progress_path.read_text())
        rows_done = int(progress.get("rows_done") or 0)
        progress_dim = int(progress.get("dim") or dim)
        if progress_dim != dim:
            raise RuntimeError(f"Resume dim mismatch: {progress_dim} != {dim}")

    expected_vector_bytes = rows_done * dim * 4
    expected_norm_bytes = rows_done * 4

    if vectors_path.exists() and vectors_path.stat().st_size != expected_vector_bytes:
        raise RuntimeError(
            f"Resume vector bytes mismatch: {vectors_path.stat().st_size} "
            f"!= {expected_vector_bytes}"
        )
    if norms_path.exists() and norms_path.stat().st_size != expected_norm_bytes:
        raise RuntimeError(
            f"Resume norm bytes mismatch: {norms_path.stat().st_size} "
            f"!= {expected_norm_bytes}"
        )

    print(
        f"[embed] {source} · {pending_path.name} · "
        f"resume {human_count(rows_done)}/{human_count(expected_count)}"
    )

    batch_records: list[dict[str, Any]] = []
    batch_texts: list[str] = []
    index = 0
    started = time.time()

    with (
        pending_path.open("r", encoding="utf-8", errors="replace") as pf,
        metadata_tmp.open("a", encoding="utf-8") as mf,
        vectors_path.open("ab") as vf,
        norms_path.open("ab") as nf,
    ):
        for line in pf:
            if index < rows_done:
                index += 1
                continue

            try:
                record = json.loads(line)
            except Exception:
                index += 1
                continue

            candidate_text = clean_text(record.get("candidate_text"))
            if not candidate_text:
                index += 1
                continue

            batch_records.append(record)
            batch_texts.append(candidate_text)
            index += 1

            if len(batch_records) >= batch_size:
                data = post_json(
                    endpoint,
                    {"texts": batch_texts, "use_freq": use_freq},
                    timeout=timeout,
                )
                vectors = response_vectors(data)
                if len(vectors) != len(batch_records):
                    raise RuntimeError(
                        f"/v1/embed returned {len(vectors)} vectors for "
                        f"{len(batch_records)} texts"
                    )

                arr = np.asarray(vectors, dtype=np.float32)
                if arr.ndim != 2 or arr.shape[1] != dim:
                    raise RuntimeError(f"Bad embedding shape {arr.shape}; expected (*,{dim})")

                norms = np.linalg.norm(arr, axis=1).astype(np.float32)

                vf.write(arr.tobytes(order="C"))
                nf.write(norms.tobytes(order="C"))
                for rec in batch_records:
                    mf.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

                vf.flush()
                nf.flush()
                mf.flush()
                os.fsync(vf.fileno())
                os.fsync(nf.fileno())
                os.fsync(mf.fileno())

                rows_done += len(batch_records)
                atomic_json(progress_path, {
                    "rows_done": rows_done,
                    "expected_count": expected_count,
                    "dim": dim,
                    "use_freq": use_freq,
                    "updated_at": utc_now(),
                })

                elapsed = max(0.001, time.time() - started)
                rate = max(0.001, rows_done / elapsed)
                remaining = max(0, expected_count - rows_done)
                eta_minutes = (remaining / rate) / 60.0

                print(
                    f"[embed] {human_count(rows_done)}/{human_count(expected_count)} · "
                    f"{rate:.1f} items/s · ETA {eta_minutes:.1f} min"
                )

                batch_records.clear()
                batch_texts.clear()

        if batch_records:
            data = post_json(
                endpoint,
                {"texts": batch_texts, "use_freq": use_freq},
                timeout=timeout,
            )
            vectors = response_vectors(data)
            if len(vectors) != len(batch_records):
                raise RuntimeError(
                    f"/v1/embed returned {len(vectors)} vectors for "
                    f"{len(batch_records)} texts"
                )

            arr = np.asarray(vectors, dtype=np.float32)
            if arr.ndim != 2 or arr.shape[1] != dim:
                raise RuntimeError(f"Bad embedding shape {arr.shape}; expected (*,{dim})")

            norms = np.linalg.norm(arr, axis=1).astype(np.float32)

            vf.write(arr.tobytes(order="C"))
            nf.write(norms.tobytes(order="C"))
            for rec in batch_records:
                mf.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

            vf.flush()
            nf.flush()
            mf.flush()
            os.fsync(vf.fileno())
            os.fsync(nf.fileno())
            os.fsync(mf.fileno())

            rows_done += len(batch_records)
            atomic_json(progress_path, {
                "rows_done": rows_done,
                "expected_count": expected_count,
                "dim": dim,
                "use_freq": use_freq,
                "updated_at": utc_now(),
            })

    if rows_done != expected_count:
        raise RuntimeError(
            f"Embedded rows {rows_done} != pending manifest count {expected_count}. "
            "Pending file may contain malformed/blank rows."
        )

    metadata_gz = tmp_dir / "metadata.jsonl.gz"
    with metadata_tmp.open("rb") as src, gzip.open(metadata_gz, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

    metadata_tmp.unlink()

    expected_vector_bytes = rows_done * dim * 4
    expected_norm_bytes = rows_done * 4

    if vectors_path.stat().st_size != expected_vector_bytes:
        raise RuntimeError(
            f"Vector byte mismatch: {vectors_path.stat().st_size} != {expected_vector_bytes}"
        )
    if norms_path.stat().st_size != expected_norm_bytes:
        raise RuntimeError(
            f"Norm byte mismatch: {norms_path.stat().st_size} != {expected_norm_bytes}"
        )

    shard_manifest = {
        "schema_version": FIELD_SCHEMA_VERSION,
        "source": source,
        "count": rows_done,
        "dim": dim,
        "dtype": "float32-little-endian",
        "use_freq": use_freq,
        "created_at": utc_now(),
        "candidate_schema": "canonical_record.candidate_text",
        "metadata_file": "metadata.jsonl.gz",
        "vectors_file": "vectors.f32",
        "norms_file": "norms.f32",
        "metadata_sha256": sha256_file(metadata_gz),
        "vectors_sha256": sha256_file(vectors_path),
        "norms_sha256": sha256_file(norms_path),
        "pending_source_file": entry["path"],
    }
    atomic_json(tmp_dir / "manifest.json", shard_manifest)
    progress_path.unlink(missing_ok=True)

    tmp_dir.rename(final_dir)

    manifest_rel = str((final_dir / "manifest.json").relative_to(field_dir))
    field.setdefault("shards", []).append({
        "source": source,
        "path": manifest_rel,
        "count": rows_done,
        "created_at": shard_manifest["created_at"],
    })

    source_state = ensure_source(field, source)
    source_state["embedded_records"] = int(source_state.get("embedded_records") or 0) + rows_done

    update_pending_status(
        field,
        source,
        entry["path"],
        "embedded",
        embedded_shard_manifest=manifest_rel,
    )

    recompute_field_counts(field)
    save_field(field_dir, field)

    if delete_pending:
        pending_path.unlink(missing_ok=True)

    print(f"[embed] DONE · {human_count(rows_done)} · {final_dir}")


def embed_pending(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)

    entries = list(pending_entries_for(field, args.source))
    if not entries:
        print("[embed] no pending files")
        return

    for source, entry in entries:
        # Reload after every shard so the registry is always current.
        field = load_field(field_dir)
        embed_pending_file(
            field_dir=field_dir,
            field=field,
            source=source,
            entry=entry,
            endpoint=args.endpoint,
            batch_size=args.batch_size,
            timeout=args.timeout,
            delete_pending=args.delete_pending,
        )


# -----------------------------------------------------------------------------
# Status / verify
# -----------------------------------------------------------------------------

def status(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    recompute_field_counts(field)

    print(json.dumps({
        "name": field.get("name"),
        "dim": field.get("dim"),
        "use_freq": field.get("use_freq"),
        "total_records": field.get("total_records"),
        "pending_records": field.get("pending_records"),
        "embedded_records": field.get("embedded_records"),
        "sources": {
            name: {
                "ingested_records": src.get("ingested_records", 0),
                "embedded_records": src.get("embedded_records", 0),
                "pending_files": sum(
                    1 for p in src.get("pending", []) if p.get("status") == "pending"
                ),
            }
            for name, src in field.get("sources", {}).items()
        },
        "shard_count": len(field.get("shards", [])),
    }, indent=2))


def verify(args: argparse.Namespace) -> None:
    field_dir = Path(args.field_dir)
    field = load_field(field_dir)
    failures = 0

    for i, shard_ref in enumerate(field.get("shards", []), 1):
        manifest_path = field_dir / shard_ref["path"]
        print(f"[verify] {i}/{len(field.get('shards', []))} · {manifest_path}")

        try:
            manifest = json.loads(manifest_path.read_text())
            shard_dir = manifest_path.parent
            count = int(manifest["count"])
            dim = int(manifest["dim"])

            vectors = shard_dir / manifest["vectors_file"]
            norms = shard_dir / manifest["norms_file"]
            metadata = shard_dir / manifest["metadata_file"]

            expected_vectors = count * dim * 4
            expected_norms = count * 4

            if vectors.stat().st_size != expected_vectors:
                raise RuntimeError(
                    f"vector bytes {vectors.stat().st_size} != {expected_vectors}"
                )
            if norms.stat().st_size != expected_norms:
                raise RuntimeError(
                    f"norm bytes {norms.stat().st_size} != {expected_norms}"
                )
            if args.hashes:
                if sha256_file(metadata) != manifest["metadata_sha256"]:
                    raise RuntimeError("metadata sha256 mismatch")
                if sha256_file(vectors) != manifest["vectors_sha256"]:
                    raise RuntimeError("vectors sha256 mismatch")
                if sha256_file(norms) != manifest["norms_sha256"]:
                    raise RuntimeError("norms sha256 mismatch")

        except Exception as exc:
            failures += 1
            print(f"[verify] FAILED · {exc}", file=sys.stderr)

    if failures:
        raise SystemExit(f"{failures} shard(s) failed verification")

    print(f"[verify] PASS · {len(field.get('shards', []))} shard(s)")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def add_common_ingest_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--field-dir", required=True)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--pending-shard-size", type=int, default=DEFAULT_PENDING_SHARD_SIZE)
    p.add_argument("--max-text-chars", type=int, default=DEFAULT_MAX_TEXT_CHARS)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build massive, sharded, pre-embedded ARBITER fields."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init")
    p.add_argument("--field-dir", required=True)
    p.add_argument("--name", default="ARBITER Universal Field")
    p.add_argument("--dim", type=int, default=DEFAULT_DIM)
    p.add_argument(
        "--use-freq",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p.set_defaults(func=lambda a: print(json.dumps(
        init_field(Path(a.field_dir), a.name, a.dim, a.use_freq),
        indent=2,
    )))

    p = sub.add_parser("import-preembedded")
    p.add_argument("--field-dir", required=True)
    p.add_argument("--source-dir", required=True)
    p.add_argument("--source", default="entertainment")
    p.add_argument("--max-text-chars", type=int, default=DEFAULT_MAX_TEXT_CHARS)
    p.add_argument(
        "--copy",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Copy binary files instead of hard-linking when possible.",
    )
    p.set_defaults(func=import_preembedded)

    p = sub.add_parser("ingest-openalex-api")
    add_common_ingest_args(p)
    p.add_argument(
        "--require-abstract",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p.add_argument("--from-year", type=int)
    p.add_argument("--to-year", type=int)
    p.add_argument("--api-key")
    p.add_argument("--mailto")
    p.add_argument("--delay", type=float, default=0.0)
    p.set_defaults(func=ingest_openalex_api)

    p = sub.add_parser("ingest-openalex-snapshot")
    add_common_ingest_args(p)
    p.add_argument(
        "--glob",
        action="append",
        required=True,
        help="Glob for official OpenAlex JSONL .gz files. Repeatable.",
    )
    p.add_argument(
        "--require-abstract",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p.set_defaults(func=ingest_openalex_snapshot)

    p = sub.add_parser("ingest-openlibrary-dump")
    add_common_ingest_args(p)
    p.add_argument("--dump", required=True)
    p.add_argument("--min-meaning-chars", type=int, default=40)
    p.set_defaults(func=ingest_openlibrary_dump)

    p = sub.add_parser("ingest-musicbrainz-postgres")
    add_common_ingest_args(p)
    p.add_argument("--dsn", required=True)
    p.add_argument("--fetch-size", type=int, default=10_000)
    p.add_argument(
        "--require-tags",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p.set_defaults(func=ingest_musicbrainz_postgres)


    p = sub.add_parser("ingest-musicbrainz-artists-json")
    add_common_ingest_args(p)
    p.add_argument("--dump", required=True, help="Official MusicBrainz artist.tar.xz JSON dump")
    p.add_argument(
        "--selection",
        choices=("discoverable", "all"),
        default="discoverable",
        help="discoverable requires at least two structured meaning signals; all keeps every named artist",
    )
    p.add_argument(
        "--allow-derived-tags",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include MusicBrainz genres/tags. These are derived data under CC BY-NC-SA, unlike the CC0 core artist data.",
    )
    p.set_defaults(func=ingest_musicbrainz_artists_json)

    p = sub.add_parser("ingest-jsonl")
    add_common_ingest_args(p)
    p.add_argument("--path", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--id-field", default="id")
    p.add_argument("--title-field", default="title")
    p.add_argument("--text-field", default="text")
    p.add_argument("--type-field")
    p.add_argument("--default-type", default="item")
    p.add_argument("--year-field")
    p.add_argument("--url-field")
    p.add_argument("--chunk-chars", type=int, default=0)
    p.add_argument("--overlap-chars", type=int, default=200)
    p.add_argument(
        "--keep-original",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    p.set_defaults(func=ingest_jsonl)

    p = sub.add_parser("embed-pending")
    p.add_argument("--field-dir", required=True)
    p.add_argument("--source")
    p.add_argument("--endpoint", default=DEFAULT_ARBITER_EMBED)
    p.add_argument("--batch-size", type=int, default=DEFAULT_EMBED_BATCH)
    p.add_argument("--timeout", type=int, default=900)
    p.add_argument(
        "--delete-pending",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    p.set_defaults(func=embed_pending)

    p = sub.add_parser("status")
    p.add_argument("--field-dir", required=True)
    p.set_defaults(func=status)

    p = sub.add_parser("verify")
    p.add_argument("--field-dir", required=True)
    p.add_argument(
        "--hashes",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    p.set_defaults(func=verify)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
