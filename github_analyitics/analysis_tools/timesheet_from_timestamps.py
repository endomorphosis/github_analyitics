#!/usr/bin/env python3
"""
Generate a timesheet calendar from timestamped activity.

Reads the User Timeline (or Commit/File events) from an analytics report and
creates a 15-minute calendar plus contiguous work-session estimates.
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


SHEET_PREFERENCE = [
    "All Events",
    "File Timestamp List",
    "User Timeline",
    "Commit Events",
    "File Events",
    "PR Events",
    "Issue Events",
]


def load_timestamps(path: Path, sheet: Optional[str]) -> pd.DataFrame:
    if sheet:
        return pd.read_excel(path, sheet_name=sheet)

    for candidate in SHEET_PREFERENCE:
        try:
            df = pd.read_excel(path, sheet_name=candidate)
            if not df.empty and "event_timestamp" in df.columns:
                return df
        except Exception:
            continue

    raise ValueError("No suitable sheet with event_timestamp found.")


def round_to_15(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def build_calendar(df: pd.DataFrame, lookback_minutes: int, forward_minutes: int) -> pd.DataFrame:
    df = df.copy()
    df = df.dropna(subset=["event_timestamp"])
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

    if "attributed_user" in df.columns:
        user_col = "attributed_user"
    elif "user" in df.columns:
        user_col = "user"
    else:
        user_col = "author"

    repo_col = "repository" if "repository" in df.columns else None

    rows = []
    lookback = timedelta(minutes=max(lookback_minutes, 0))
    forward = timedelta(minutes=max(forward_minutes, 0))

    for _, row in df.iterrows():
        ts = row["event_timestamp"].to_pydatetime()
        user = row.get(user_col, "Unknown")
        repo = row.get(repo_col) if repo_col else None
        start = ts - lookback
        end = ts + forward

        slot = round_to_15(start)
        while slot <= end:
            slot_naive = slot.replace(tzinfo=None)
            record = {
                "user": user,
                "date": slot.date().isoformat(),
                "slot_start": slot_naive,
            }
            if repo_col:
                record["repository"] = repo
            rows.append(record)
            slot += timedelta(minutes=15)

    calendar = pd.DataFrame(rows).drop_duplicates()
    if calendar.empty:
        return calendar

    calendar = calendar.sort_values(["user", "slot_start"])
    return calendar


def build_sessions(calendar: pd.DataFrame) -> pd.DataFrame:
    if calendar.empty:
        return calendar

    calendar = calendar.copy()
    calendar["slot_start"] = pd.to_datetime(calendar["slot_start"]).dt.tz_localize(None)

    sessions = []
    group_cols = ["user"]
    if "repository" in calendar.columns:
        group_cols.append("repository")

    for group_keys, user_df in calendar.groupby(group_cols):
        if isinstance(group_keys, tuple):
            user = group_keys[0]
            repo = group_keys[1] if len(group_keys) > 1 else None
        else:
            user = group_keys
            repo = None

        slots = user_df["slot_start"].sort_values().tolist()
        if not slots:
            continue

        session_start = slots[0]
        session_end = slots[0]
        for slot in slots[1:]:
            if slot - session_end <= timedelta(minutes=15):
                session_end = slot
            else:
                hours = ((session_end - session_start).total_seconds() / 3600.0) + 0.25
                record = {
                    "user": user,
                    "session_start": session_start,
                    "session_end": session_end + timedelta(minutes=15),
                    "estimated_hours": round(hours, 2),
                }
                if repo is not None:
                    record["repository"] = repo
                sessions.append(record)
                session_start = slot
                session_end = slot

        hours = ((session_end - session_start).total_seconds() / 3600.0) + 0.25
        record = {
            "user": user,
            "session_start": session_start,
            "session_end": session_end + timedelta(minutes=15),
            "estimated_hours": round(hours, 2),
        }
        if repo is not None:
            record["repository"] = repo
        sessions.append(record)

    return pd.DataFrame(sessions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a 15-minute timesheet from timestamped activity.")
    parser.add_argument("--input", required=True, help="Path to timestamps report (.xlsx)")
    parser.add_argument("--output", required=True, help="Output Excel file")
    parser.add_argument("--sheet", help="Optional sheet name to read")
    parser.add_argument("--window-minutes", type=int, default=None,
                        help="Minutes to expand around each timestamp (symmetric, legacy)")
    parser.add_argument("--lookback-minutes", type=int, default=15,
                        help="Minutes before each timestamp to include (default: 15)")
    parser.add_argument("--forward-minutes", type=int, default=0,
                        help="Minutes after each timestamp to include (default: 0)")
    parser.add_argument("--user", help="Optional single user filter")

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = load_timestamps(input_path, args.sheet)
    if args.user:
        for col in ["attributed_user", "user", "author"]:
            if col in df.columns:
                df = df[df[col] == args.user]
                break

    if args.window_minutes is not None:
        calendar = build_calendar(df, args.window_minutes, args.window_minutes)
    else:
        calendar = build_calendar(df, args.lookback_minutes, args.forward_minutes)
    sessions = build_sessions(calendar)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        calendar.to_excel(writer, sheet_name="Timesheet Calendar", index=False)
        sessions.to_excel(writer, sheet_name="Work Sessions", index=False)

    print(f"Timesheet saved to: {output_path}")


if __name__ == "__main__":
    main()