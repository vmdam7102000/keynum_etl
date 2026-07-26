# plugins/utils/db_utils.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


def get_all_stock_codes(
    postgres_conn_id: str,
    stock_list_table: str,
    code_column: str = "code",
) -> List[str]:
    """
    Fetch all stock codes from the configured stock list table.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT {code_column} FROM {stock_list_table}")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def insert_dynamic_records(
    postgres_conn_id: str,
    table: str,
    records: List[Dict[str, Any]],
    columns_map: Sequence[Dict[str, str]],
    conflict_keys: Sequence[str],
    on_conflict_do_update: bool = False,
    update_columns: Optional[Sequence[str]] = None,
    conn: Optional[PGConnection] = None,
) -> None:
    """
    Insert multiple records into Postgres following a YAML-defined mapping.
    """
    if not records:
        return

    db_columns = [c["column"] for c in columns_map]
    json_keys = [c["json_key"] for c in columns_map]

    columns_sql = ", ".join(db_columns)
    placeholders_sql = ", ".join(["%s"] * len(db_columns))
    conflict_sql = ", ".join(conflict_keys)

    if on_conflict_do_update:
        if update_columns is None:
            columns_to_update = db_columns
        else:
            columns_to_update = list(update_columns)
            if not columns_to_update:
                raise ValueError("update_columns must not be empty when updating conflicts")
            if len(columns_to_update) != len(set(columns_to_update)):
                raise ValueError("update_columns must not contain duplicate columns")
            unknown_columns = set(columns_to_update) - set(db_columns)
            if unknown_columns:
                raise ValueError(
                    "update_columns must be present in columns_map: "
                    + ", ".join(sorted(unknown_columns))
                )
        set_clause = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in columns_to_update]
        )
        conflict_part = f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_clause}"
    else:
        conflict_part = f"ON CONFLICT ({conflict_sql}) DO NOTHING"

    insert_sql = f"""
        INSERT INTO {table} ({columns_sql})
        VALUES ({placeholders_sql})
        {conflict_part}
    """

    managed_conn = False
    if conn is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        managed_conn = True

    cursor = conn.cursor()
    try:
        for rec in records:
            values = [rec.get(k) for k in json_keys]
            cursor.execute(insert_sql, values)
        conn.commit()
    finally:
        cursor.close()
        if managed_conn and conn:
            conn.close()
