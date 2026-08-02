from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from plugins.utils.api_utils import request_json, request_text

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


@dataclass(frozen=True)
class MembershipInterval:
    source_ticker: str
    valid_from: date
    valid_to: Optional[date]

    @property
    def key(self) -> Tuple[str, date]:
        return self.source_ticker, self.valid_from


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_membership_csv(csv_text: str) -> List[MembershipInterval]:
    """Parse the repository's ticker,start_date,end_date interval file."""
    reader = csv.DictReader(io.StringIO(csv_text))
    expected = {"ticker", "start_date", "end_date"}
    if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
        raise ValueError("Membership CSV must contain ticker,start_date,end_date")

    records: List[MembershipInterval] = []
    for line_number, row in enumerate(reader, start=2):
        ticker = (row.get("ticker") or "").strip().upper()
        start_value = (row.get("start_date") or "").strip()
        end_value = (row.get("end_date") or "").strip()
        if not ticker or not start_value:
            raise ValueError(f"Missing ticker/start_date on CSV line {line_number}")
        try:
            valid_from = date.fromisoformat(start_value)
            valid_to = date.fromisoformat(end_value) if end_value else None
        except ValueError as exc:
            raise ValueError(f"Invalid ISO date on CSV line {line_number}") from exc
        records.append(MembershipInterval(ticker, valid_from, valid_to))
    return records


def validate_memberships(
    records: Sequence[MembershipInterval],
    *,
    minimum_rows: int = 1000,
    existing_count: int = 0,
    maximum_drop_fraction: float = 0.05,
) -> None:
    if len(records) < minimum_rows:
        raise ValueError(
            f"Membership source contains {len(records)} rows; expected at least {minimum_rows}"
        )
    if existing_count and len(records) < existing_count * (1 - maximum_drop_fraction):
        raise ValueError(
            f"Membership source shrank from {existing_count} to {len(records)} rows"
        )

    seen = set()
    by_ticker: Dict[str, List[MembershipInterval]] = {}
    for record in records:
        if record.valid_to is not None and record.valid_to <= record.valid_from:
            raise ValueError(f"Invalid interval for {record.source_ticker}: {record}")
        if record.key in seen:
            raise ValueError(f"Duplicate membership interval key: {record.key}")
        seen.add(record.key)
        by_ticker.setdefault(record.source_ticker, []).append(record)

    for ticker, ticker_records in by_ticker.items():
        ordered = sorted(ticker_records, key=lambda item: item.valid_from)
        for previous, current in zip(ordered, ordered[1:]):
            if previous.valid_to is None or current.valid_from < previous.valid_to:
                raise ValueError(
                    f"Overlapping membership intervals for {ticker}: "
                    f"{previous.valid_from}..{previous.valid_to} and "
                    f"{current.valid_from}..{current.valid_to}"
                )


def fetch_latest_commit_sha(
    source_cfg: Mapping[str, Any],
    *,
    github_token: str = "",
) -> str:
    headers = {"Accept": "application/vnd.github+json"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    payload = request_json(
        source_cfg["commits_url"],
        params={"path": source_cfg["path"], "per_page": 1},
        headers=headers,
        timeout=source_cfg.get("timeout", 30),
        fatal_statuses=(401, 403, 404),
    )
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise RuntimeError("GitHub did not return a latest commit for membership source")
    commit_sha = str(payload[0].get("sha") or "")
    if len(commit_sha) != 40:
        raise RuntimeError("GitHub returned an invalid membership commit SHA")
    return commit_sha


def download_membership_csv(
    source_cfg: Mapping[str, Any],
    *,
    commit_sha: str,
    github_token: str = "",
) -> str:
    raw_url = source_cfg["raw_url_template"].format(
        commit_sha=commit_sha,
        path=source_cfg["path"],
    )
    csv_text = request_text(
        raw_url,
        headers={"Authorization": f"Bearer {github_token}"} if github_token else None,
        timeout=source_cfg.get("timeout", 30),
        fatal_statuses=(401, 403, 404),
    )
    if csv_text is None:
        raise RuntimeError("Unable to download membership CSV at pinned commit")
    return csv_text


def fetch_latest_source(
    source_cfg: Mapping[str, Any],
    *,
    github_token: str = "",
) -> Tuple[str, str]:
    commit_sha = fetch_latest_commit_sha(source_cfg, github_token=github_token)
    return commit_sha, download_membership_csv(
        source_cfg,
        commit_sha=commit_sha,
        github_token=github_token,
    )


def get_latest_stored_commit(
    conn: PGConnection,
    *,
    membership_table: str,
    index_code: str,
    source_repo: str,
) -> Optional[str]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT source_commit_sha
            FROM {membership_table}
            WHERE index_code = %s AND source_repo = %s
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (index_code, source_repo),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()


def sync_membership_history(
    conn: PGConnection,
    *,
    records: Sequence[MembershipInterval],
    commit_sha: str,
    source_repo: str,
    membership_table: str,
    mapping_table: str,
    index_code: str = "SP500",
    minimum_rows: int = 1000,
    maximum_drop_fraction: float = 0.05,
) -> Dict[str, Any]:
    """Atomically replace the source-owned interval set and seed pending mappings."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM {membership_table} WHERE index_code = %s AND source_repo = %s",
            (index_code, source_repo),
        )
        existing_count = int(cursor.fetchone()[0])
        validate_memberships(
            records,
            minimum_rows=minimum_rows,
            existing_count=existing_count,
            maximum_drop_fraction=maximum_drop_fraction,
        )

        cursor.execute(
            f"""
            SELECT id, source_ticker, valid_from, valid_to
            FROM {membership_table}
            WHERE index_code = %s AND source_repo = %s
            """,
            (index_code, source_repo),
        )
        existing = {
            (row[1], row[2]): {"id": row[0], "valid_to": row[3]}
            for row in cursor.fetchall()
        }
        incoming = {record.key: record for record in records}
        changed_keys = {
            key
            for key, record in incoming.items()
            if key not in existing or existing[key]["valid_to"] != record.valid_to
        }
        deleted_keys = set(existing) - set(incoming)

        collected_at = utc_now()
        for record in records:
            cursor.execute(
                f"""
                INSERT INTO {membership_table} (
                    index_code, source_ticker, valid_from, valid_to,
                    source_repo, source_commit_sha, collected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (index_code, source_ticker, valid_from)
                DO UPDATE SET
                    valid_to = EXCLUDED.valid_to,
                    source_repo = EXCLUDED.source_repo,
                    source_commit_sha = EXCLUDED.source_commit_sha,
                    collected_at = EXCLUDED.collected_at,
                    updated_at = now()
                """,
                (
                    index_code,
                    record.source_ticker,
                    record.valid_from,
                    record.valid_to,
                    source_repo,
                    commit_sha,
                    collected_at,
                ),
            )

        for key in deleted_keys:
            cursor.execute(
                f"""
                DELETE FROM {membership_table}
                WHERE index_code = %s
                  AND source_repo = %s
                  AND source_ticker = %s
                  AND valid_from = %s
                """,
                (index_code, source_repo, key[0], key[1]),
            )

        cursor.execute(
            f"""
            INSERT INTO {mapping_table} (membership_id, provider)
            SELECT id, 'EODHD'
            FROM {membership_table}
            WHERE index_code = %s AND source_repo = %s
            ON CONFLICT (membership_id, provider) DO NOTHING
            """,
            (index_code, source_repo),
        )

        affected_ids: List[int] = []
        affected_tickers: List[str] = []
        if changed_keys:
            cursor.execute(
                f"""
                SELECT id, source_ticker, valid_from
                FROM {membership_table}
                WHERE index_code = %s AND source_repo = %s
                """,
                (index_code, source_repo),
            )
            for membership_id, ticker, valid_from in cursor.fetchall():
                if (ticker, valid_from) in changed_keys:
                    affected_ids.append(membership_id)
                    affected_tickers.append(ticker)
            if affected_ids:
                cursor.execute(
                    f"""
                    UPDATE {mapping_table}
                    SET price_backfill_status = 'pending',
                        updated_at = now()
                    WHERE membership_id = ANY(%s)
                      AND provider = 'EODHD'
                    """,
                    (affected_ids,),
                )
        conn.commit()
        return {
            "changed": bool(changed_keys or deleted_keys),
            "commit_sha": commit_sha,
            "source_rows": len(records),
            "affected_membership_ids": sorted(affected_ids),
            "affected_tickers": sorted(set(affected_tickers)),
            "deleted_count": len(deleted_keys),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _symbol_codes(source_ticker: str) -> List[str]:
    codes = [source_ticker.upper()]
    if "." in source_ticker:
        codes.append(source_ticker.replace(".", "-").upper())
    if "-" in source_ticker:
        codes.append(source_ticker.replace("-", ".").upper())
    return list(dict.fromkeys(codes))


def _index_symbols(
    records: Any,
    *,
    is_delisted: bool,
) -> Dict[str, List[Dict[str, Any]]]:
    if not isinstance(records, list):
        raise ValueError("EODHD symbol list must be a JSON list")
    indexed: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        code = str(record.get("Code") or "").strip().upper()
        if not code:
            continue
        item = dict(record)
        item["is_delisted"] = is_delisted
        indexed.setdefault(code, []).append(item)
    return indexed


def resolve_provider_symbol(
    source_ticker: str,
    *,
    is_open: bool,
    active_symbols: Mapping[str, Sequence[Mapping[str, Any]]],
    delisted_symbols: Mapping[str, Sequence[Mapping[str, Any]]],
    symbol_changes: Mapping[str, str],
    manual_override: Optional[str] = None,
) -> Dict[str, Any]:
    if manual_override:
        provider_ticker = manual_override.upper()
        if "." not in provider_ticker:
            provider_ticker = f"{provider_ticker}.US"
        return {
            "mapping_status": "resolved",
            "provider_ticker": provider_ticker,
            "resolution_method": "manual",
            "metadata": {},
        }

    preferred, fallback = (
        (active_symbols, delisted_symbols) if is_open else (delisted_symbols, active_symbols)
    )
    for position, symbol_index in enumerate((preferred, fallback)):
        for code in _symbol_codes(source_ticker):
            matches = list(symbol_index.get(code, ()))
            if len(matches) == 1:
                method = "exact" if code == source_ticker.upper() else "normalized"
                return {
                    "mapping_status": "resolved",
                    "provider_ticker": f"{code}.US",
                    "resolution_method": method,
                    "metadata": dict(matches[0]),
                }
            if len(matches) > 1:
                return {
                    "mapping_status": "ambiguous",
                    "provider_ticker": None,
                    "resolution_method": None,
                    "metadata": {},
                }
        if position == 0:
            continue

    renamed = symbol_changes.get(source_ticker.upper())
    if renamed:
        matches = list(active_symbols.get(renamed, ()))
        if len(matches) == 1:
            return {
                "mapping_status": "resolved",
                "provider_ticker": f"{renamed}.US",
                "resolution_method": "symbol_change",
                "metadata": dict(matches[0]),
            }
    return {
        "mapping_status": "unavailable",
        "provider_ticker": None,
        "resolution_method": None,
        "metadata": {},
    }


def fetch_eodhd_resolution_data(
    api_cfg: Mapping[str, Any],
    *,
    api_key: str,
) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]],
    Dict[str, str],
]:
    if not api_key:
        raise ValueError("EODHD API key is required to resolve S&P 500 tickers")
    common_params = {"api_token": api_key, "fmt": "json"}
    active = request_json(
        api_cfg["symbol_list_url"],
        params={**common_params, "delisted": 0},
        timeout=api_cfg.get("timeout", 30),
    )
    delisted = request_json(
        api_cfg["symbol_list_url"],
        params={**common_params, "delisted": 1},
        timeout=api_cfg.get("timeout", 30),
    )
    changes = request_json(
        api_cfg["symbol_change_url"],
        params={
            **common_params,
            "from": api_cfg.get("symbol_change_from", "2022-07-22"),
            "to": date.today().isoformat(),
        },
        timeout=api_cfg.get("timeout", 30),
    )
    change_map: Dict[str, str] = {}
    if isinstance(changes, list):
        for record in changes:
            if not isinstance(record, dict):
                continue
            old_symbol = str(record.get("old_symbol") or "").upper()
            new_symbol = str(record.get("new_symbol") or "").upper()
            if old_symbol and new_symbol:
                change_map[old_symbol] = new_symbol
    return (
        _index_symbols(active, is_delisted=False),
        _index_symbols(delisted, is_delisted=True),
        change_map,
    )


def _get_or_create_company(
    cursor: Any,
    *,
    companies_table: str,
    source_ticker: str,
    is_open: bool,
    metadata: Mapping[str, Any],
) -> int:
    cursor.execute(
        f"""
        SELECT id, name, is_active
        FROM {companies_table}
        WHERE ticker = %s
        ORDER BY id DESC
        """,
        (source_ticker,),
    )
    rows = cursor.fetchall()
    preferred = [row for row in rows if bool(row[2]) is is_open]
    if preferred:
        return int(preferred[0][0])

    metadata_name = str(metadata.get("Name") or "").strip().casefold()
    if metadata_name:
        for row in rows:
            if str(row[1] or "").strip().casefold() == metadata_name:
                return int(row[0])
    if is_open and rows:
        return int(rows[0][0])

    cursor.execute(
        f"""
        INSERT INTO {companies_table} (
            ticker, name, exchange, country, universe, is_active
        )
        VALUES (%s, %s, %s, %s, 'SP500', FALSE)
        RETURNING id
        """,
        (
            source_ticker,
            str(metadata.get("Name") or source_ticker),
            metadata.get("Exchange") or "US",
            metadata.get("Country") or "USA",
        ),
    )
    return int(cursor.fetchone()[0])


def _synchronize_company_id_sequence(
    cursor: Any,
    *,
    companies_table: str,
) -> None:
    """Advance a serial sequence after companies were loaded with explicit IDs."""
    cursor.execute(
        "SELECT pg_get_serial_sequence(%s, 'id')",
        (companies_table,),
    )
    row = cursor.fetchone()
    sequence_name = row[0] if row else None
    if not sequence_name:
        raise RuntimeError(f"No serial sequence found for {companies_table}.id")
    cursor.execute(
        f"""
        SELECT setval(
            %s::regclass,
            COALESCE(MAX(id), 1),
            MAX(id) IS NOT NULL
        )
        FROM {companies_table}
        """,
        (sequence_name,),
    )


def resolve_pending_mappings(
    conn: PGConnection,
    *,
    api_cfg: Mapping[str, Any],
    db_cfg: Mapping[str, Any],
    api_key: str,
    affected_membership_ids: Optional[Sequence[int]] = None,
    manual_overrides: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    membership_table = db_cfg["membership_table"]
    mapping_table = db_cfg["mapping_table"]
    companies_table = db_cfg["company_table"]
    cursor = conn.cursor()
    try:
        overrides = {
            str(key).upper(): value for key, value in (manual_overrides or {}).items()
        }
        parameters: List[Any] = []
        target_parts = ["mapping.mapping_status IN ('pending', 'ambiguous')"]
        if affected_membership_ids:
            target_parts.append("membership.id = ANY(%s)")
            parameters.append(list(affected_membership_ids))
        if overrides:
            target_parts.append("membership.source_ticker = ANY(%s)")
            parameters.append(list(overrides))
        target_clause = f"({' OR '.join(target_parts)})"
        cursor.execute(
            f"""
            SELECT
                membership.id,
                membership.source_ticker,
                membership.valid_to
            FROM {membership_table} AS membership
            JOIN {mapping_table} AS mapping
              ON mapping.membership_id = membership.id
             AND mapping.provider = 'EODHD'
            WHERE {target_clause}
            ORDER BY membership.source_ticker, membership.valid_from
            """,
            parameters,
        )
        rows = cursor.fetchall()
        counts = {"resolved": 0, "ambiguous": 0, "unavailable": 0}
        resolved_ids: List[int] = []
        resolved_tickers: List[str] = []
        unresolved_tickers: Dict[str, List[str]] = {
            "ambiguous": [],
            "unavailable": [],
        }
        if not rows:
            return {
                **counts,
                "resolved_membership_ids": resolved_ids,
                "affected_tickers": resolved_tickers,
                "unresolved_tickers": unresolved_tickers,
            }
        active, delisted, symbol_changes = fetch_eodhd_resolution_data(
            api_cfg,
            api_key=api_key,
        )
        _synchronize_company_id_sequence(
            cursor,
            companies_table=companies_table,
        )
        for membership_id, source_ticker, valid_to in rows:
            resolution = resolve_provider_symbol(
                source_ticker,
                is_open=valid_to is None,
                active_symbols=active,
                delisted_symbols=delisted,
                symbol_changes=symbol_changes,
                manual_override=overrides.get(source_ticker),
            )
            company_id = None
            if resolution["mapping_status"] == "resolved":
                company_id = _get_or_create_company(
                    cursor,
                    companies_table=companies_table,
                    source_ticker=source_ticker,
                    is_open=valid_to is None,
                    metadata=resolution["metadata"],
                )
                resolved_ids.append(membership_id)
                resolved_tickers.append(source_ticker)
            counts[resolution["mapping_status"]] += 1
            if resolution["mapping_status"] in unresolved_tickers:
                unresolved_tickers[resolution["mapping_status"]].append(source_ticker)
            cursor.execute(
                f"""
                UPDATE {mapping_table}
                SET provider_ticker = %s,
                    company_id = %s,
                    mapping_status = %s,
                    resolution_method = %s,
                    last_error = NULL,
                    last_verified_at = now(),
                    updated_at = now()
                WHERE membership_id = %s AND provider = 'EODHD'
                """,
                (
                    resolution["provider_ticker"],
                    company_id,
                    resolution["mapping_status"],
                    resolution["resolution_method"],
                    membership_id,
                ),
            )
        conn.commit()
        return {
            **counts,
            "resolved_membership_ids": resolved_ids,
            "affected_tickers": sorted(set(resolved_tickers)),
            "unresolved_tickers": {
                key: sorted(set(values)) for key, values in unresolved_tickers.items()
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


class _ConstituentsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.cell_parts: List[str] = []
        self.row: List[str] = []
        self.headers: List[str] = []
        self.records: List[Dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attributes = dict(attrs)
        if tag == "table" and attributes.get("id") == "constituents":
            self.in_table = True
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.row = []
        elif self.in_row and tag in {"th", "td"}:
            self.in_cell = True
            self.cell_parts = []

    def handle_data(self, data: str) -> None:
        if self.in_cell:
            self.cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self.in_cell and tag in {"th", "td"}:
            self.row.append(" ".join("".join(self.cell_parts).split()))
            self.in_cell = False
        elif self.in_row and tag == "tr":
            if not self.headers and "Symbol" in self.row:
                self.headers = self.row
            elif self.headers and len(self.row) >= len(self.headers):
                self.records.append(dict(zip(self.headers, self.row)))
            self.in_row = False
        elif self.in_table and tag == "table":
            self.in_table = False


def parse_wikipedia_constituents_html(html: str) -> List[Dict[str, str]]:
    parser = _ConstituentsParser()
    parser.feed(html)
    records = []
    for record in parser.records:
        ticker = record.get("Symbol", "").strip().upper()
        if ticker:
            records.append(
                {
                    "ticker": ticker,
                    "name": record.get("Security", "").strip() or ticker,
                }
            )
    if len(records) < 400:
        raise ValueError(
            f"Wikipedia constituents parser returned only {len(records)} records"
        )
    return records


def monitor_wikipedia_current(
    conn: PGConnection,
    *,
    html: str,
    companies_table: str,
    membership_table: str,
) -> Dict[str, Any]:
    records = parse_wikipedia_constituents_html(html)
    current = {record["ticker"]: record for record in records}
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT ticker
            FROM {companies_table}
            WHERE universe = 'SP500' AND is_active IS TRUE
            """
        )
        previous_active = {row[0] for row in cursor.fetchall()}

        for ticker, record in current.items():
            cursor.execute(
                f"""
                SELECT id
                FROM {companies_table}
                WHERE ticker = %s
                ORDER BY is_active DESC, id DESC
                LIMIT 1
                """,
                (ticker,),
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(
                    f"""
                    UPDATE {companies_table}
                    SET is_active = TRUE, universe = 'SP500', updated_at = now()
                    WHERE id = %s
                    """,
                    (row[0],),
                )
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {companies_table} (
                        ticker, name, exchange, country, universe, is_active
                    )
                    VALUES (%s, %s, 'US', 'USA', 'SP500', TRUE)
                    """,
                    (ticker, record["name"]),
                )

        cursor.execute(
            f"""
            UPDATE {companies_table}
            SET is_active = FALSE, updated_at = now()
            WHERE universe = 'SP500'
              AND is_active IS TRUE
              AND NOT (ticker = ANY(%s))
            """,
            (list(current),),
        )
        cursor.execute(
            f"""
            SELECT source_ticker
            FROM {membership_table}
            WHERE index_code = 'SP500' AND valid_to IS NULL
            """
        )
        open_history = {row[0] for row in cursor.fetchall()}
        conn.commit()
        return {
            "current_count": len(current),
            "added_vs_companies": sorted(set(current) - previous_active),
            "removed_vs_companies": sorted(previous_active - set(current)),
            "added_vs_history": sorted(set(current) - open_history),
            "removed_vs_history": sorted(open_history - set(current)),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
