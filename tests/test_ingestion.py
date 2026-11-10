from datetime import timedelta
from email.utils import format_datetime
from methane.common import utcnow, write_json
import dataclasses
import gzip
import json
import pytest
import requests
from methane.api import fetch_json, extract, read_archive, retry_delay
from conftest import Session, Response, page, observation, oid


def test_rate_limit_retry_archives_attempts(cfg, log, tmp_path):
    session = Session([Response({}, 429, {"Retry-After": "0"}), page([])])
    payload, _ = fetch_json(session, {}, cfg, log, tmp_path, sleep=lambda _: None)
    assert payload["items"] == [] and log.counts == {"api_requests": 2, "api_failures": 1, "api_retries": 1}
    assert len(list(tmp_path.glob("response_*.json.gz"))) == 2


@pytest.mark.parametrize("status", [401, 403, 404])
def test_permanent_http_failure_does_not_retry(cfg, log, tmp_path, status):
    s = Session([Response({}, status)])
    with pytest.raises((PermissionError, requests.HTTPError)):
        fetch_json(s, {}, cfg, log, tmp_path, sleep=lambda _: None)
    assert len(s.calls) == 1


@pytest.mark.parametrize("failure", [requests.Timeout(), requests.ConnectionError()])
def test_network_retry_bounded(cfg, log, tmp_path, failure):
    s = Session([failure, failure, failure])
    with pytest.raises(type(failure)):
        fetch_json(s, {}, dataclasses.replace(cfg, max_retries=2), log, tmp_path, sleep=lambda _: None)
    assert len(s.calls) == 3 and log.counts["api_failures"] == 3


def test_long_retry_after_aborts_instead_of_hammering(cfg, log, tmp_path):
    with pytest.raises(RuntimeError, match="cooldown"):
        fetch_json(Session([Response({}, 429, {"Retry-After": "3600"})]), {}, cfg, log, tmp_path, sleep=lambda _: None)
    assert log.counts == {"api_requests": 1, "api_failures": 1, "api_retries": 0}


@pytest.mark.parametrize("body", [b"<html>Error</html>", b"[]", b'{"items":{}}'])
def test_malformed_response_archived(cfg, log, tmp_path, body):
    with pytest.raises(ValueError):
        fetch_json(Session([Response(body=body)]), {}, cfg, log, tmp_path)
    assert list(tmp_path.glob("response_*.json.gz"))


def test_response_size_bounded(cfg, log, tmp_path):
    with pytest.raises(ValueError, match="max_response_bytes"):
        fetch_json(
            Session([Response(body=b"x" * 20)]), {}, dataclasses.replace(cfg, max_response_bytes=10), log, tmp_path
        )


def test_filtered_count_and_pagination(cfg, log):
    s = Session(
        [page([observation("a")], 2, total_count=900, offset=0), page([observation("b")], 2, total_count=900, offset=1)]
    )
    path = extract(dataclasses.replace(cfg, page_size=1), "pages", "2025-03-01T00:00:00Z", "full", None, log, s)
    manifest, rows = read_archive(path, cfg.scope_id)
    assert len(rows) == 2 and [x["params"]["offset"] for x in s.calls] == [0, 1]
    assert manifest["complete"]


@pytest.mark.parametrize(
    "responses,match",
    [
        ([page([observation()], 2), page([observation()], 2)], "Duplicate"),
        ([page([observation()], 2), page([], 2)], "Premature"),
        ([page([observation()], 2), page([observation("b")], 3)], "source changed"),
        ([page([observation()], 1, offset=3)], "offset"),
        ([page([observation(id="bad")])], "Invalid observation"),
        ([Response({"items": [], "bbox_count": True})], "integer"),
    ],
)
def test_partial_or_invalid_page_never_finalizes(cfg, log, responses, match):
    with pytest.raises((ValueError, RuntimeError), match=match):
        extract(
            dataclasses.replace(cfg, page_size=1), "bad", "2025-03-01T00:00:00Z", "full", None, log, Session(responses)
        )
    assert not (cfg.work / "bronze/bad/manifest.json").exists()


def test_max_pages_fail_closed(cfg, log):
    with pytest.raises(RuntimeError, match="max_pages"):
        extract(
            dataclasses.replace(cfg, page_size=1, max_pages=1),
            "limited",
            "2025-03-01T00:00:00Z",
            "full",
            None,
            log,
            Session([page([observation()], 2)]),
        )


def test_confirm_absence_removes_cutoff_and_archives_proof(cfg, log):
    s = Session([page([observation()]), page([])])
    p = extract(cfg, "full", "2025-03-01T00:00:00Z", "full", None, log, s, known_ids=[oid("a"), oid("b")])
    m, rows = read_archive(p)
    assert m["confirmed_absent"] == [oid("b")] and len(rows) == 1
    assert "modified_at" not in s.calls[-1]["params"]
    assert s.calls[-1]["params"]["plume_ids"] == [oid("b")]


def test_change_after_cutoff_is_retained_not_withdrawn(cfg, log):
    s = Session([page([]), page([observation(modified="2025-03-02T00:00:00Z")])])
    p = extract(cfg, "race", "2025-03-01T00:00:00Z", "full", None, log, s, known_ids=[oid()])
    m, rows = read_archive(p)
    assert not m["confirmed_absent"] and len(rows) == 1


def test_archive_corruption_detected(cfg, log):
    p = extract(cfg, "tamper", "2025-03-01T00:00:00Z", "full", None, log, Session([page([observation()])]))
    m = json.loads(p.read_text(encoding="utf-8"))
    with gzip.open(p.parent / m["pages"][0]["file"], "wb") as f:
        f.write(b"{}")
    with pytest.raises(ValueError, match="checksum"):
        read_archive(p)


@pytest.mark.parametrize("field,value", [("run_id", "../escape"), ("scope_id", "fake"), ("complete", False)])
def test_manifest_integrity(cfg, log, field, value):
    p = extract(cfg, "manifest", "2025-03-01T00:00:00Z", "full", None, log, Session([page([observation()])]))
    m = json.loads(p.read_text(encoding="utf-8"))
    m[field] = value
    p.write_text(json.dumps(m))
    with pytest.raises(ValueError):
        read_archive(p)


def test_retry_truncated_stream(cfg, log, tmp_path):
    class Truncated(Response):
        def iter_content(self, size):
            yield b'{"items":['
            raise requests.exceptions.ChunkedEncodingError("truncated")

    result, _ = fetch_json(Session([Truncated(), page([])]), {}, cfg, log, tmp_path, sleep=lambda _: None)
    assert result["items"] == []
    assert log.counts == {"api_requests": 2, "api_failures": 1, "api_retries": 1}
    assert (tmp_path / "network_00001.json").is_file()


def test_uuid_case_is_one_identity(cfg, log):
    a = observation()
    with pytest.raises(ValueError, match="Duplicate"):
        extract(
            cfg, "case", "2025-03-01T00:00:00Z", "full", None, log, Session([page([a, {**a, "id": a["id"].upper()}])])
        )


def test_bad_json_constant_archived_before_rejection(cfg, log, tmp_path):
    with pytest.raises(ValueError, match="Non-standard JSON"):
        fetch_json(Session([Response(body=b'{"items":[],"bad":NaN}')]), {}, cfg, log, tmp_path)
    assert len(list(tmp_path.glob("response_*.json.gz"))) == 1


def test_http_date_retry_after(cfg):
    future = format_datetime(utcnow() + timedelta(seconds=10), usegmt=True)
    assert 5 <= retry_delay(Response({}, 429, {"Retry-After": future}), 0, cfg) <= 12


def test_immutable_json_cannot_be_replaced(tmp_path):
    p = tmp_path / "manifest.json"
    write_json(p, {"x": 1}, immutable=True)
    with pytest.raises(FileExistsError):
        write_json(p, {"x": 2}, immutable=True)
    assert json.loads(p.read_text(encoding="utf-8")) == {"x": 1}


def test_provider_serialization_failure_is_incomplete(cfg, log):
    with pytest.raises(ValueError, match="serialization"):
        extract(
            cfg,
            "incomplete",
            "2025-03-01T00:00:00Z",
            "full",
            None,
            log,
            Session([page([observation()], serialization_error_count=1)]),
        )
    assert not (cfg.work / "bronze/incomplete/manifest.json").exists()
