from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union


DEFAULT_REPORTS_DIRNAME = "data_reports"


def _timestamp_folder_name(now: Optional[datetime] = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d_%H%M%S")


def ensure_timestamped_report_dir(
    base_dir: Union[str, Path] = DEFAULT_REPORTS_DIRNAME,
    *,
    now: Optional[datetime] = None,
) -> Path:
    """Create and return a timestamped output directory.

    Layout:
      <base_dir>/<YYYYMMDD_HHMMSS>/

    The returned path is absolute.
    """

    base = Path(base_dir).expanduser()
    out_dir = base / _timestamp_folder_name(now)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir.resolve()


def default_xlsx_path(
    report_filename: str,
    *,
    base_dir: Union[str, Path] = DEFAULT_REPORTS_DIRNAME,
    now: Optional[datetime] = None,
) -> Path:
    """Return an absolute XLSX path under a new timestamped report directory."""

    name = report_filename
    if not name.lower().endswith(".xlsx"):
        name = f"{name}.xlsx"

    out_dir = ensure_timestamped_report_dir(base_dir, now=now)
    return out_dir / name
