from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd


@dataclass
class DuckDbStore:
    """Tiny helper to append heterogeneous event dicts into DuckDB tables.

    Keeps table schemas flexible by adding missing columns as VARCHAR.
    Uses pandas DataFrames as the ingestion batch format.
    """

    db_path: Path

    def connect(self):
        import duckdb  # lazy import

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.db_path))

    @staticmethod
    def _table_exists(con, table: str) -> bool:
        try:
            row = con.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()
            return row is not None
        except Exception:
            try:
                return con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None
            except Exception:
                return False

    @staticmethod
    def _get_table_columns(con, table: str) -> list[str]:
        try:
            rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            return [r[1] for r in rows]
        except Exception:
            return []

    @staticmethod
    def _ensure_columns(con, table: str, columns: Iterable[str]) -> None:
        existing = set(DuckDbStore._get_table_columns(con, table))
        for col in columns:
            if col in existing:
                continue
            safe = str(col).replace('"', '""')
            con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{safe}" VARCHAR')
            existing.add(col)

    @staticmethod
    def append_rows(
        con,
        table: str,
        rows: Iterable[Dict],
        *,
        batch_size: int = 50_000,
    ) -> int:
        total = 0
        batch: list[Dict] = []
        for row in rows:
            if row is None:
                continue
            batch.append(dict(row))
            if len(batch) >= batch_size:
                total += DuckDbStore._append_batch(con, table, batch)
                batch = []
        if batch:
            total += DuckDbStore._append_batch(con, table, batch)
        return total

    @staticmethod
    def _append_batch(con, table: str, batch: list[Dict]) -> int:
        df = pd.DataFrame(batch)
        if df.empty:
            return 0

        view = f"__tmp_{table}"
        con.register(view, df)
        try:
            if not DuckDbStore._get_table_columns(con, table):
                con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM {view}')
                return int(len(df))

            DuckDbStore._ensure_columns(con, table, df.columns)

            table_cols = DuckDbStore._get_table_columns(con, table)
            for col in table_cols:
                if col not in df.columns:
                    df[col] = None

            con.unregister(view)
            con.register(view, df[table_cols])

            cols_sql = ",".join([f'"{c.replace("\"", "\"\"")}"' for c in table_cols])
            con.execute(f'INSERT INTO "{table}" ({cols_sql}) SELECT {cols_sql} FROM {view}')
            return int(len(df))
        finally:
            try:
                con.unregister(view)
            except Exception:
                pass


def write_query_to_excel(
    *,
    con,
    writer,
    sheet_base: str,
    query: str,
    excel_max_rows: int,
    allow_empty: bool = False,
) -> None:
    import pandas as pd

    max_data_rows = max(1, int(excel_max_rows) - 1)

    try:
        count = con.execute(f"SELECT COUNT(*) FROM ({query}) q").fetchone()[0]
        count = int(count or 0)
    except Exception as e:
        if not allow_empty:
            return
        # Common case: DuckDB enabled but the backing table was never created
        # (e.g., all sources produced zero rows).
        print(f"[DuckDB→XLSX] '{sheet_base}' query failed ({e.__class__.__name__}); writing empty sheet")
        pd.DataFrame().to_excel(writer, sheet_name=sheet_base[:31], index=False)
        return

    if count == 0:
        if not allow_empty:
            return
        df0 = con.execute(f"SELECT * FROM ({query}) q LIMIT 0").df()
        if df0 is None:
            df0 = pd.DataFrame()
        df0.to_excel(writer, sheet_name=sheet_base[:31], index=False)
        return

    total_sheets = (count + max_data_rows - 1) // max_data_rows
    if total_sheets > 1:
        print(f"[DuckDB→XLSX] '{sheet_base}' rows={count} splitting={total_sheets}")

    def sheet_name_with_suffix(base: str, idx: int) -> str:
        base = (base or '').strip() or 'Sheet'
        if idx <= 1:
            return base[:31]
        suffix = f" ({idx})"
        max_base_len = max(1, 31 - len(suffix))
        return f"{base[:max_base_len]}{suffix}"

    offset = 0
    sheet_idx = 1
    while offset < count:
        df = con.execute(f"{query} LIMIT {max_data_rows} OFFSET {offset}").df()
        if df is None:
            df = pd.DataFrame()
        df.to_excel(writer, sheet_name=sheet_name_with_suffix(sheet_base, sheet_idx), index=False)
        offset += max_data_rows
        sheet_idx += 1
