"""Small SQL helpers shared by the Sentix loader and publisher."""
from __future__ import annotations

import re

from psycopg2 import sql


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def qualified_identifier(name: str) -> sql.Identifier:
    parts = name.split(".")
    if not parts or any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Unsafe qualified PostgreSQL identifier: {name!r}")
    return sql.Identifier(*parts)

