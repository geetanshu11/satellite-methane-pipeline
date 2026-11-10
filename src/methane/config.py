from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import json
import os
from datetime import date
from .common import digest, CONTRACT_VERSION


@dataclass
class Config:
    root: str = "runtime"
    start_date: str = "2022-01-01"
    end_date: str | None = None
    bbox: tuple | None = None
    instruments: tuple = ("tan", "emi")
    page_size: int = 1000
    max_pages: int = 250
    overlap_hours: int = 48
    full_refresh_days: int = 7
    h3_resolution: int = 8
    request_pause: float = 0.35
    timeout_seconds: int = 60
    max_retries: int = 4
    max_map_cells: int = 500
    connect_timeout_seconds: int = 10
    max_response_bytes: int = 32_000_000
    max_withdrawal_fraction: float = 0.25
    max_retry_wait_seconds: int = 60

    def __post_init__(self):
        self.root = str(Path(self.root).resolve())
        self.instruments = tuple(self.instruments)
        if not self.instruments or not set(self.instruments) <= {"tan", "emi"}:
            raise ValueError("This project is scoped to satellite instruments tan and emi.")
        date.fromisoformat(self.start_date)
        if self.end_date:
            date.fromisoformat(self.end_date)
        if self.start_date > date.today().isoformat():
            raise ValueError("start_date cannot be in the future")
        if not 0 <= self.max_withdrawal_fraction <= 1:
            raise ValueError("max_withdrawal_fraction must be 0..1")
        if (
            min(
                self.connect_timeout_seconds,
                self.max_response_bytes,
                self.max_retry_wait_seconds,
            )
            <= 0
        ):
            raise ValueError("Operational bounds must be positive")
        if self.end_date is not None:
            if pd.Timestamp(self.end_date, tz="UTC") <= pd.Timestamp(self.start_date, tz="UTC"):
                raise ValueError("end_date must be later than start_date.")
        if self.bbox is not None:
            self.bbox = tuple(float(x) for x in self.bbox)
            if len(self.bbox) != 4:
                raise ValueError("bbox needs west, south, east, north.")
            west, south, east, north = self.bbox
            if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
                raise ValueError("Invalid bounding box; antimeridian boxes need separate runs.")
        if not 1 <= self.page_size <= 1000 or self.max_pages < 1:
            raise ValueError("Use page_size 1..1000 and a positive max_pages.")
        if not 0 <= self.h3_resolution <= 15 or self.overlap_hours < 0:
            raise ValueError("Invalid H3 resolution or overlap.")
        if self.max_retries < 0 or self.timeout_seconds <= 0 or self.request_pause < 0:
            raise ValueError("Retry, timeout, and pause settings must be non-negative, with a positive timeout.")
        if self.full_refresh_days < 1 or self.max_map_cells < 1:
            raise ValueError("full_refresh_days and max_map_cells must be positive.")

    @property
    def scope(self):
        # Filters define an isolated dataset; transport settings do not.
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "bbox": self.bbox,
            "instruments": sorted(self.instruments),
            "gas": "CH4",
            "status": "published",
            "contract": CONTRACT_VERSION,
            "h3_resolution": self.h3_resolution,
        }

    @property
    def scope_id(self):
        return digest(self.scope)[:16]

    @property
    def work(self):
        return Path(self.root) / self.scope_id

    @property
    def db_path(self):
        return self.work / "warehouse.duckdb"


def load_config(path=None, root=None):
    values = json.loads(Path(path).read_text(encoding="utf-8-sig")) if path else {}
    if root or os.getenv("METHANE_ROOT"):
        values["root"] = root or os.environ["METHANE_ROOT"]
    return Config(**values)
