"""Synthetic records exist only in tests; never a demo or live-data fallback."""

import json
import uuid
import pytest
import requests
from methane.config import Config
from methane.common import RunLog


def oid(label="a"):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "synthetic-test-" + label))


def observation(label="a", **updates):
    item = {
        "id": oid(label),
        "plume_id": "synthetic-" + label,
        "gas": "CH4",
        "scene_id": oid("scene-" + label),
        "scene_timestamp": "2025-01-01T12:00:00Z",
        "modified": "2025-02-01T12:00:00Z",
        "published_at": "2025-01-05T00:00:00Z",
        "instrument": "tan",
        "platform": "Synthetic test platform",
        "status": "published",
        "geometry_json": {"type": "Point", "coordinates": [-121.5, 37.4]},
        "emission_auto": 100.0,
        "emission_uncertainty_auto": 20.0,
        "hide_emission": False,
        "plume_quality": "good",
        "sector": "6A",
    }
    item.update(updates)
    return item


class Response:
    def __init__(self, payload=None, status=200, headers=None, body=None):
        self.body = body if body is not None else json.dumps(payload).encode()
        self.status_code, self.headers = status, headers or {}

    def iter_content(self, _size):
        yield self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def close(self):
        pass


class Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, **kwargs):
        self.calls.append(kwargs)
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def page(items, count=None, **kw):
    return Response({"items": items, "bbox_count": len(items) if count is None else count, **kw})


@pytest.fixture
def cfg(tmp_path):
    return Config(root=str(tmp_path / "runtime"), request_pause=0, max_withdrawal_fraction=1.0)


@pytest.fixture
def log(tmp_path):
    return RunLog(tmp_path / "log.jsonl", "test")


@pytest.fixture(autouse=True)
def offline_test_environment(monkeypatch):
    """No synthetic request can reach the real provider."""

    def block_network(*args, **kwargs):
        raise AssertionError("Tests must use archived data or mocked HTTP; external requests are disabled")

    monkeypatch.setattr(requests.sessions.Session, "request", block_network)
