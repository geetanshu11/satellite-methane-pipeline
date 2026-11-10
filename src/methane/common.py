"""Shared durable writes, canonical hashes and structured operational events."""

from __future__ import annotations
import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from filelock import FileLock, Timeout

API = "https://api.carbonmapper.org/api/v1/catalog/plumes/annotated"
CONTRACT_VERSION = "2"
SECTORS = {
    "1A1": "Energy industries",
    "1A2": "Manufacturing industries",
    "1B1a": "Coal mining",
    "1B2": "Oil and natural gas",
    "4A": "Enteric fermentation",
    "4B": "Manure management",
    "6A": "Solid waste disposal",
    "6B": "Wastewater handling",
    "other": "Other",
    "unknown": "Undetermined",
}


def utcnow():
    return datetime.now(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()


def sha_file(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def sync_directory(path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_json(path, value, *, immutable=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False).encode()
    if immutable:
        with path.open("xb") as f:
            f.write(body)
            f.flush()
            os.fsync(f.fileno())
    else:
        tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with tmp.open("xb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
    sync_directory(path.parent)


class RunLog:
    def __init__(self, path, run_id):
        self.path, self.run_id = Path(path), run_id
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.counts = {"api_requests": 0, "api_failures": 0, "api_retries": 0}

    def event(self, event, **values):
        row = {"at_utc": iso(utcnow()), "run_id": self.run_id, "event": event, **values}
        text = json.dumps(row, default=str, allow_nan=False)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(text + "\n")
            f.flush()
        print(text, flush=True)


@contextmanager
def project_lock(work):
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(work / ".pipeline.lock"), timeout=0):
            yield
    except Timeout as exc:
        raise RuntimeError("Another process holds the warehouse lock. Stop or wait for that run.") from exc
