#!/usr/bin/env python3
"""Hydrate SCOUT artist photos from Wikidata P18 and Wikimedia Commons.

Only artists with a MusicBrainz -> Wikidata relationship are eligible. Every
image keeps its Commons description URL, license, creator and credit metadata.
The operation changes display metadata only; artist vectors remain immutable.
"""
from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import requests

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "SCOUT-Artist-Image-Enricher/1.0 (Actual General Intelligence; artist discovery metadata)"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean_html(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return " ".join(text.split())


def ext_value(ext: dict[str, Any], key: str) -> str:
    value = ext.get(key)
    if isinstance(value, dict):
        value = value.get("value")
    return clean_html(value)


def chunks(values: list[Any], size: int):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def fetch_p18(session: requests.Session, qids: list[str]) -> dict[str, str]:
    if not qids:
        return {}
    response = session.get(
        WIKIDATA_API,
        params={
            "action": "wbgetentities",
            "format": "json",
            "props": "claims",
            "ids": "|".join(qids),
        },
        timeout=60,
    )
    response.raise_for_status()
    entities = response.json().get("entities") or {}
    out: dict[str, str] = {}
    for qid, entity in entities.items():
        claims = entity.get("claims") if isinstance(entity, dict) else {}
        p18 = claims.get("P18") if isinstance(claims, dict) else None
        if not isinstance(p18, list) or not p18:
            continue
        try:
            filename = p18[0]["mainsnak"]["datavalue"]["value"]
        except Exception:
            continue
        if filename:
            out[qid] = str(filename)
    return out


def fetch_commons(session: requests.Session, filenames: list[str]) -> dict[str, dict[str, str]]:
    if not filenames:
        return {}
    titles = [f"File:{name}" for name in filenames]
    response = session.post(
        COMMONS_API,
        data={
            "action": "query",
            "format": "json",
            "prop": "imageinfo",
            "titles": "|".join(titles),
            "iiprop": "url|extmetadata",
            "iiurlwidth": "900",
        },
        timeout=90,
    )
    response.raise_for_status()
    pages = ((response.json().get("query") or {}).get("pages") or {})
    out: dict[str, dict[str, str]] = {}
    for page in pages.values():
        if not isinstance(page, dict):
            continue
        title = str(page.get("title") or "")
        filename = title[5:] if title.startswith("File:") else title
        infos = page.get("imageinfo")
        if not isinstance(infos, list) or not infos:
            continue
        info = infos[0] if isinstance(infos[0], dict) else {}
        ext = info.get("extmetadata") if isinstance(info.get("extmetadata"), dict) else {}
        out[filename] = {
            "imageUrl": str(info.get("thumburl") or info.get("url") or ""),
            "imageSourceUrl": str(info.get("descriptionurl") or ""),
            "imageLicense": ext_value(ext, "LicenseShortName"),
            "imageLicenseUrl": ext_value(ext, "LicenseUrl"),
            "imageArtist": ext_value(ext, "Artist"),
            "imageCredit": ext_value(ext, "Credit"),
            "imageDescription": ext_value(ext, "ImageDescription"),
        }
    return out


def run(args: argparse.Namespace) -> int:
    db = Path(args.db).expanduser()
    if not db.exists():
        raise SystemExit(f"Missing live metadata database: {db}")

    conn = sqlite3.connect(db, timeout=90)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    checked = 0
    hydrated = 0
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})

    try:
        where = [
            "source = 'musicbrainz-artists'",
            "coalesce(image_url, '') = ''",
            "json_extract(extra_json, '$.wikidataId') GLOB 'Q[0-9]*'",
        ]
        if not args.retry_missing:
            where.append("json_extract(extra_json, '$.imageCheckedAt') IS NULL")
        rows = conn.execute(
            "SELECT row_id, extra_json FROM items WHERE " + " AND ".join(where) + " ORDER BY row_id LIMIT ?",
            (args.limit,),
        ).fetchall()
        if not rows:
            print("[images] no eligible unchecked artists remain")
            return 0

        for batch in chunks(list(rows), args.batch_size):
            qid_rows: dict[str, list[sqlite3.Row]] = {}
            for row in batch:
                try:
                    extra = json.loads(row["extra_json"] or "{}")
                except Exception:
                    extra = {}
                qid = str(extra.get("wikidataId") or "")
                if qid:
                    qid_rows.setdefault(qid, []).append(row)

            p18 = fetch_p18(session, list(qid_rows))
            commons: dict[str, dict[str, str]] = {}
            for name_batch in chunks(list(dict.fromkeys(p18.values())), args.batch_size):
                commons.update(fetch_commons(session, name_batch))
                time.sleep(args.delay)

            now = utc_now()
            updates = []
            for qid, source_rows in qid_rows.items():
                filename = p18.get(qid, "")
                meta = commons.get(filename, {})
                for row in source_rows:
                    try:
                        extra = json.loads(row["extra_json"] or "{}")
                    except Exception:
                        extra = {}
                    extra["imageCheckedAt"] = now
                    if filename:
                        extra["commonsFilename"] = filename
                    for key, value in meta.items():
                        if value:
                            extra[key] = value
                    image_url = meta.get("imageUrl") or ""
                    updates.append((image_url, json.dumps(extra, ensure_ascii=False, separators=(",", ":")), row["row_id"]))
                    checked += 1
                    if image_url:
                        hydrated += 1

            conn.executemany("UPDATE items SET image_url = ?, extra_json = ? WHERE row_id = ?", updates)
            conn.commit()
            print(f"[images] checked {checked:,} · hydrated {hydrated:,}")
            time.sleep(args.delay)
    finally:
        conn.close()

    print(f"[images] DONE · checked {checked:,} · hydrated {hydrated:,}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydrate SCOUT artist images from Wikidata and Wikimedia Commons")
    parser.add_argument("--db", default="~/SCOUT_ARTIST_FIELD/live_index/metadata.sqlite3")
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--retry-missing", action=argparse.BooleanOptionalAction, default=False)
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
