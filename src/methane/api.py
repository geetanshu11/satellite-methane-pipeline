"""Bounded HTTP ingestion and immutable, checksum-verifiable input manifests."""

from __future__ import annotations
import gzip
import hashlib
import json
import os
import random
import time
import uuid
from email.utils import parsedate_to_datetime
from pathlib import Path
import requests
from .common import API, digest, iso, utcnow, write_json, sha_file


def valid_uuid(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value.lower()
    except (ValueError, AttributeError):
        return False


def decode_json(body):
    def invalid_constant(value):
        raise ValueError(f"Non-standard JSON constant: {value}")

    return json.loads(body, parse_constant=invalid_constant)


def retry_delay(response, attempt, cfg):
    delay = min(cfg.max_retry_wait_seconds, 2**attempt + random.random())
    if response is not None:
        value = response.headers.get("Retry-After", "")
        try:
            seconds = float(value)
        except ValueError:
            try:
                seconds = (parsedate_to_datetime(value) - utcnow()).total_seconds()
            except (ValueError, TypeError, OverflowError):
                seconds = 0
        # Never violate a longer server cooldown by retrying too soon.
        if seconds > cfg.max_retry_wait_seconds:
            raise RuntimeError("Retry-After exceeds retry budget; rerun after the provider cooldown")
        delay = max(delay, seconds)
    return max(0, min(cfg.max_retry_wait_seconds, delay))


def fetch_json(session, params, cfg, log, directory, sleep=time.sleep):
    directory = Path(directory)
    for attempt in range(cfg.max_retries + 1):
        response = None
        log.counts["api_requests"] += 1
        number = log.counts["api_requests"]
        started = time.monotonic()
        request = {
            "request_number": number,
            "endpoint": API,
            "query": params,
            "requested_at_utc": iso(utcnow()),
            "attempt": attempt + 1,
        }
        try:
            response = session.get(
                API,
                params=params,
                timeout=(cfg.connect_timeout_seconds, cfg.timeout_seconds),
                allow_redirects=False,
                stream=True,
            )
            chunks, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > cfg.max_response_bytes:
                    raise ValueError("API response exceeds max_response_bytes")
                chunks.append(chunk)
            body = b"".join(chunks)
            filename = f"response_{number:05d}.json.gz"
            with (directory / filename).open("xb") as raw:
                with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                    compressed.write(body)
                raw.flush()
                os.fsync(raw.fileno())
            request.update(
                status=response.status_code,
                file=filename,
                bytes=len(body),
                sha256=hashlib.sha256(body).hexdigest(),
                stored_sha256=sha_file(directory / filename),
                duration_seconds=round(time.monotonic() - started, 3),
            )
            write_json(directory / f"request_{number:05d}.json", request, immutable=True)
            if response.status_code in (401, 403):
                raise PermissionError("Carbon Mapper denied API access; no substitute observations used")
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == cfg.max_retries:
                    response.raise_for_status()
                delay = retry_delay(response, attempt, cfg)
                log.counts["api_failures"] += 1
                log.counts["api_retries"] += 1
                log.event("http_retry", status=response.status_code, wait_seconds=delay)
                sleep(delay)
                continue
            if not 200 <= response.status_code < 300:
                response.raise_for_status()
                raise ValueError("Unexpected redirect or non-success response")
            payload = decode_json(body)
            if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
                raise ValueError("API contract changed: expected object with items list")
            return payload, request
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
            log.counts["api_failures"] += 1
            write_json(
                directory / f"network_{number:05d}.json", {**request, "error_type": type(exc).__name__}, immutable=True
            )
            if attempt == cfg.max_retries:
                raise
            delay = retry_delay(None, attempt, cfg)
            log.counts["api_retries"] += 1
            log.event("http_retry", reason=type(exc).__name__, wait_seconds=delay)
            sleep(delay)
        except Exception:
            log.counts["api_failures"] += 1
            raise
        finally:
            if response is not None:
                response.close()
    raise RuntimeError("Retry budget exhausted")


def make_query(cfg, cutoff, lower_modified=None):
    q = {
        "plume_gas": "CH4",
        "status": "published",
        "instruments": list(cfg.instruments),
        "sort": "asc",
        "limit": cfg.page_size,
        "datetime": cfg.start_date + "T00:00:00Z/" + (cfg.end_date + "T00:00:00Z" if cfg.end_date else cutoff),
        "modified_at": f"{lower_modified or '..'}/{cutoff}",
    }
    if cfg.bbox:
        q["bbox"] = list(cfg.bbox)
    return q


def checked_page(payload, query):
    if payload.get("serialization_error_count") not in (None, 0):
        raise ValueError("Provider reported incomplete record serialization")
    count = payload.get("bbox_count", payload.get("total_count"))
    if type(count) is not int or count < 0:
        raise ValueError("API contract changed: missing integer result count")
    if "offset" in payload and payload["offset"] != query["offset"]:
        raise ValueError("Response offset does not match request")
    items = payload["items"]
    if len(items) > query["limit"]:
        raise ValueError("Response exceeds requested page size")
    ids = [x.get("id") if isinstance(x, dict) else None for x in items]
    if any(not valid_uuid(x) for x in ids):
        raise ValueError("Invalid observation IDs in API response; raw response retained")
    ids = [x.lower() for x in ids]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate IDs within page")
    return count, items, ids


def extract(cfg, run_id, cutoff, mode, lower_modified, log, session=None, known_ids=()):
    directory = cfg.work / "bronze" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    base = make_query(cfg, cutoff, lower_modified)
    manifest = {
        "archive_version": 2,
        "run_id": run_id,
        "scope_id": cfg.scope_id,
        "scope": cfg.scope,
        "mode": mode,
        "cutoff_utc": cutoff,
        "lower_modified_utc": lower_modified,
        "endpoint": API,
        "query": base,
        "complete": False,
        "pages": [],
        "source": "Carbon Mapper",
        "terms": "https://carbonmapper.org/terms",
        "confirmed_absent": [],
    }
    write_json(directory / "progress.json", manifest)
    own = session is None
    session = session or requests.Session()
    if own:
        session.headers["User-Agent"] = "SatelliteMethanePipeline/3.0 (educational data engineering)"
    seen, expected, offset = set(), None, 0
    try:
        for _ in range(cfg.max_pages):
            query = {**base, "offset": offset}
            payload, request = fetch_json(session, query, cfg, log, directory)
            count, items, ids = checked_page(payload, query)
            if expected is None:
                expected = count
            if count != expected:
                raise RuntimeError("The source changed while paging; restart extraction")
            if seen.intersection(ids):
                raise ValueError("Duplicate IDs across API pages")
            if not items and offset < expected:
                raise ValueError("Premature empty page; incomplete extraction")
            seen.update(ids)
            manifest["pages"].append({**request, "offset": offset, "rows": len(items), "kind": "main"})
            offset += len(items)
            manifest.update(expected_rows=expected, rows_archived=offset, next_offset=offset)
            write_json(directory / "progress.json", manifest)
            log.event("page_saved", collected=offset, expected=expected)
            if offset == expected:
                break
            if offset > expected:
                raise ValueError("More rows than filtered count")
            time.sleep(cfg.request_pause)
        else:
            raise RuntimeError("max_pages reached; no warehouse changes made")
        # Check missing known IDs independently WITHOUT a modification-time cutoff.
        # This prevents a revision made during paging from looking like a withdrawal.
        missing = sorted(set(known_ids) - seen) if mode == "full" else []
        for start in range(0, len(missing), min(cfg.page_size, 50)):
            ids_requested = missing[start : start + min(cfg.page_size, 50)]
            query = {k: v for k, v in base.items() if k != "modified_at"}
            query.update(plume_ids=ids_requested, offset=0, limit=len(ids_requested))
            payload, request = fetch_json(session, query, cfg, log, directory)
            count, items, ids = checked_page(payload, query)
            if count != len(items) or not set(ids) <= set(ids_requested):
                raise ValueError("Absence confirmation returned an incomplete or unrelated response")
            manifest["pages"].append({**request, "offset": 0, "rows": len(items), "kind": "confirmation"})
            manifest["confirmed_absent"].extend(sorted(set(ids_requested) - set(ids)))
            seen.update(ids)
            time.sleep(cfg.request_pause)
        manifest.update(
            complete=True,
            expected_rows=len(seen),
            main_expected_rows=expected,
            finished_at_utc=iso(utcnow()),
            api_metrics=dict(log.counts),
        )
        write_json(directory / "manifest.json", manifest, immutable=True)
        write_json(directory / "progress.json", {"complete": True, "manifest": "manifest.json"})
        return directory / "manifest.json"
    finally:
        if own:
            session.close()


def read_archive(manifest_path, scope_id=None):
    path = Path(manifest_path).resolve()
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        raise ValueError("Incomplete archive cannot be loaded")
    if scope_id and manifest["scope_id"] != scope_id:
        raise ValueError("Archive and project use different dataset scopes")
    if digest(manifest["scope"])[:16] != manifest["scope_id"]:
        raise ValueError("Archive scope fingerprint mismatch")
    if (
        manifest.get("mode") not in {"full", "incremental"}
        or not isinstance(manifest.get("pages"), list)
        or not manifest["pages"]
    ):
        raise ValueError("A complete archive needs a supported mode and at least one response page")
    if type(manifest.get("expected_rows")) is not int or manifest["expected_rows"] < 0:
        raise ValueError("Invalid expected archive row count")
    rid = manifest["run_id"]
    if not isinstance(rid, str) or not rid or Path(rid).name != rid or "/" in rid or "\\" in rid or rid in {".", ".."}:
        raise ValueError("Unsafe archive run ID")
    records, seen, main_offset = [], set(), 0
    confirmed, confirmation_targets = set(), set()
    for page in manifest["pages"]:
        page_path = (path.parent / page["file"]).resolve()
        if page_path.parent != path.parent or page_path == path:
            raise ValueError("Unsafe archive page path")
        if page.get("stored_sha256") and sha_file(page_path) != page["stored_sha256"]:
            raise ValueError("Stored raw-page checksum mismatch")
        with gzip.open(page_path, "rb") as f:
            body = f.read()
        if hashlib.sha256(body).hexdigest() != page["sha256"]:
            raise ValueError("Raw-page checksum mismatch")
        payload = decode_json(body)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError("Malformed archived payload")
        checked_page(payload, page["query"])
        items = payload["items"]
        if len(items) != page["rows"]:
            raise ValueError("Archive page row-count mismatch")
        kind = page.get("kind", "main")
        if kind == "main":
            reported = payload.get("bbox_count", payload.get("total_count"))
            if type(reported) is not int or reported != manifest.get("main_expected_rows", manifest["expected_rows"]):
                raise ValueError("Archived filtered counts do not reconcile")
            if page["offset"] != main_offset:
                raise ValueError("Archive offsets are not contiguous")
            main_offset += len(items)
        elif kind == "confirmation":
            targets = set(page["query"]["plume_ids"])
            returned = {x["id"].lower() for x in items if isinstance(x, dict) and valid_uuid(x.get("id"))}
            if confirmation_targets & targets or not returned <= targets:
                raise ValueError("Invalid confirmation targets")
            count = payload.get("bbox_count", payload.get("total_count"))
            if count != len(items):
                raise ValueError("Incomplete confirmation evidence")
            confirmation_targets.update(targets)
            confirmed.update(targets - returned)
        else:
            raise ValueError("Unknown page kind")
        for item in items:
            if not isinstance(item, dict) or not valid_uuid(item.get("id")):
                raise ValueError("Invalid archived observation ID")
            if item["id"].lower() in seen:
                raise ValueError("Duplicate ID in archive")
            seen.add(item["id"].lower())
            records.append((item, f"bronze/{rid}/{page['file']}"))
    if len(records) != manifest["expected_rows"] or main_offset != manifest.get(
        "main_expected_rows", manifest["expected_rows"]
    ):
        raise ValueError("Archive is missing expected records")
    if set(manifest.get("confirmed_absent", [])) != confirmed or confirmed & seen:
        raise ValueError("Absence evidence does not match manifest")
    return manifest, records
