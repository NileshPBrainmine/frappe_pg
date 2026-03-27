"""
MySQL → PostgreSQL Query Transformation Pipeline
================================================

Production-grade two-stage pipeline for Frappe / ERPNext on PostgreSQL.
Covers ERPNext, HRMS, Raven, CRM, Education, and any other Frappe app.

Stage 1 — sqlglot transpilation
    sqlglot is a battle-tested SQL parser/transpiler used in production
    by Apache Spark, DuckDB, Tobiko, and others.  It safely handles the
    vast majority of MySQL→PG syntax differences:

        IF(c,a,b)               → CASE WHEN c THEN a ELSE b END
        IFNULL(a,b)             → COALESCE(a,b)
        DATE_FORMAT(d,fmt)      → TO_CHAR(d, pg_fmt)
        GROUP_CONCAT(x)         → STRING_AGG(x::TEXT, ',')
        RAND()                  → RANDOM()
        CURDATE()               → CURRENT_DATE
        YEAR/MONTH/DAY(x)       → EXTRACT(… FROM x)
        TRUNCATE(n,d)           → TRUNC(n,d)
        STR_TO_DATE(s,f)        → TO_DATE / TO_TIMESTAMP
        DATEDIFF(a,b)           → (a::date - b::date)
        DATE_ADD/DATE_SUB       → interval arithmetic
        REGEXP / RLIKE          → ~*
        INSERT IGNORE           → INSERT … ON CONFLICT DO NOTHING
        REPLACE INTO            → INSERT … ON CONFLICT DO UPDATE
        ON DUPLICATE KEY UPDATE → ON CONFLICT DO UPDATE
        LIMIT x, y              → LIMIT y OFFSET x
        `backtick` identifiers  → "double_quote" identifiers

Stage 2 — post-processing regex fixups
    Handles patterns sqlglot may not cover or that live outside SQL:
    - INDEX hints (FORCE/USE/IGNORE INDEX) — stripped before parsing
    - DATE_FORMAT with additional format strings
    - GROUP_CONCAT with SEPARATOR keyword
    - CAST to MySQL-only types (UNSIGNED, SIGNED, CHAR)
    - ISNULL(x) → (x IS NULL)

Edge-case safety:
    - sqlglot failures are caught; the regex-only legacy pipeline runs
      as fallback so production is never blocked by a transpilation bug.
    - Non-SQL strings (None, empty) are returned unchanged.
    - DDL and transaction-control statements pass through cleanly.
"""

from __future__ import annotations

import re
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-processing: remove MySQL index hints (not valid SQL, break parsers)
# ---------------------------------------------------------------------------

_INDEX_HINT_RE = re.compile(
    r"\b(?:FORCE|USE|IGNORE)\s+INDEX\s*\([^)]*\)",
    re.IGNORECASE,
)

# MySQL REGEXP / RLIKE are case-insensitive by default → PG ~* (case-insensitive)
# Must be applied BEFORE sqlglot because sqlglot maps REGEXP → ~ (case-sensitive).
_REGEXP_PRE_RE = re.compile(r"\bR(?:EGEXP|LIKE)\b", re.IGNORECASE)

# Detect queries that contain MySQL-specific syntax and therefore need sqlglot.
# Frappe's query_builder already emits PG-compatible SQL with double-quoted
# identifiers ("col").  Passing those through sqlglot (read="mysql") causes
# sqlglot to treat "col" as a MySQL string literal → 'col', corrupting every
# such query.  We only run sqlglot when at least one MySQL marker is present.
_MYSQL_MARKER_RE = re.compile(
    r"`"                                            # backtick identifier
    r"|\bIFNULL\s*\("                              # IFNULL(
    r"|\bIF\s*\("                                  # IF(
    r"|\bDATE_FORMAT\s*\("                         # DATE_FORMAT(
    r"|\bGROUP_CONCAT\s*\("                        # GROUP_CONCAT(
    r"|\bDATEDIFF\s*\("                            # DATEDIFF(
    r"|\bTIMESTAMPDIFF\s*\("                       # TIMESTAMPDIFF(
    r"|\bINSERT\s+IGNORE\b"                        # INSERT IGNORE
    r"|\bREPLACE\s+INTO\b"                         # REPLACE INTO
    r"|\bON\s+DUPLICATE\s+KEY\b"                   # ON DUPLICATE KEY
    r"|\bR(?:EGEXP|LIKE)\b"                        # REGEXP / RLIKE
    r"|\bRAND\s*\(\s*\)"                           # RAND()
    r"|\bCURDATE\s*\(\s*\)"                        # CURDATE()
    r"|\bFIND_IN_SET\s*\("                         # FIND_IN_SET(
    r"|\bFIELD\s*\("                               # FIELD(
    r"|\bCAST\s*\([^)]+\bAS\s+(?:UNSIGNED|SIGNED|CHAR)\b"  # CAST(x AS UNSIGNED/SIGNED/CHAR)
    r"|\bISNULL\s*\("                              # ISNULL(
    r"|\bDATE_ADD\s*\("                            # DATE_ADD(
    r"|\bDATE_SUB\s*\("                            # DATE_SUB(
    r"|\bADDDATE\s*\("                             # ADDDATE(
    r"|\bSUBDATE\s*\("                             # SUBDATE(
    r"|\bSUBSTRING_INDEX\s*\(",                    # SUBSTRING_INDEX(
    re.IGNORECASE,
)

# DATEDIFF(a, b) → (a::date - b::date)  — pre-process so sqlglot doesn't
# generate invalid CAST(AGE(...) AS BIGINT) which errors in PostgreSQL.
_DATEDIFF_PRE_RE = re.compile(
    r"\bDATEDIFF\s*\(\s*([^,)]+?)\s*,\s*([^)]+?)\s*\)",
    re.IGNORECASE,
)

# TIMESTAMP(date, time) — MySQL two-arg form creates a datetime from date+time.
# PostgreSQL has no such function; use mk_timestamp() DB-level function instead.
# Must NOT match TIMESTAMPDIFF (word boundary + no trailing DIFF keyword).
_TIMESTAMP_2ARG_RE = re.compile(
    r"\bTIMESTAMP\s*\(\s*([^,)(]+?)\s*,\s*([^)(]+?)\s*\)",
    re.IGNORECASE,
)
# TIMESTAMP(expr) single-arg form → cast to timestamp.
# Exclude pure-digit args like TIMESTAMP(6) which are precision specifiers in DDL.
_TIMESTAMP_1ARG_RE = re.compile(
    r"\bTIMESTAMP\s*\(\s*([^)(]+?)\s*\)",
    re.IGNORECASE,
)

# INTERVAL 'N' UNIT  (Frappe core pattern: NOW() - INTERVAL '7' DAY)
# PostgreSQL requires INTERVAL '7 days' not INTERVAL '7' DAY.
_INTERVAL_QUOTED_NUM_RE = re.compile(
    r"\bINTERVAL\s+'(-?\d+)'\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?\b",
    re.IGNORECASE,
)

# INTERVAL N UNIT (bare unquoted: INTERVAL 1 DAY) — outside of DATE_ADD context.
# sqlglot handles DATE_ADD(x, INTERVAL 1 DAY) but standalone bare INTERVAL needs fixing.
_INTERVAL_BARE_RE = re.compile(
    r"\bINTERVAL\s+(-?\d+)\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?\b",
    re.IGNORECASE,
)

# ADDDATE / SUBDATE — MySQL aliases for DATE_ADD / DATE_SUB; normalise before sqlglot.
_ADDDATE_RE = re.compile(r"\bADDDATE\s*\(", re.IGNORECASE)
_SUBDATE_RE = re.compile(r"\bSUBDATE\s*\(", re.IGNORECASE)

# SUBSTRING_INDEX(str, delim, count) — no PostgreSQL built-in; use DB-level function.
_SUBSTRING_INDEX_RE = re.compile(
    r"\bSUBSTRING_INDEX\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(-?\d+)\s*\)",
    re.IGNORECASE,
)


def _preprocess(sql: str) -> str:
    """Strip index hints and fix MySQL-specific constructs before handing to sqlglot."""
    sql = _INDEX_HINT_RE.sub("", sql)
    sql = _REGEXP_PRE_RE.sub("~*", sql)
    sql = _DATEDIFF_PRE_RE.sub(
        lambda m: f"({m.group(1).strip()}::date - {m.group(2).strip()}::date)", sql
    )

    # TIMESTAMP(date, time) → mk_timestamp(date, time) [DB-level function]
    # Do 2-arg before 1-arg to avoid partial matches.
    sql = _TIMESTAMP_2ARG_RE.sub(
        lambda m: f"mk_timestamp({m.group(1).strip()}, {m.group(2).strip()})", sql
    )
    # TIMESTAMP(expr) single-arg — only when arg is not a bare integer (DDL precision).
    def _timestamp_1arg(m: re.Match) -> str:
        arg = m.group(1).strip()
        if arg.isdigit():
            return m.group(0)  # TIMESTAMP(6) — leave DDL precision alone
        return f"({arg})::timestamp"
    sql = _TIMESTAMP_1ARG_RE.sub(_timestamp_1arg, sql)

    # INTERVAL 'N' UNIT → INTERVAL 'N units'
    sql = _INTERVAL_QUOTED_NUM_RE.sub(
        lambda m: f"INTERVAL '{m.group(1)} {m.group(2).lower()}s'", sql
    )
    # INTERVAL N UNIT (bare) → INTERVAL 'N units'
    # Only apply outside DATE_ADD/DATE_SUB (those go to sqlglot which handles them).
    # We apply here too as a safety net; sqlglot reads PG-style INTERVAL fine.
    sql = _INTERVAL_BARE_RE.sub(
        lambda m: f"INTERVAL '{m.group(1)} {m.group(2).lower()}s'", sql
    )

    # ADDDATE/SUBDATE → DATE_ADD/DATE_SUB so sqlglot transpiles them correctly.
    sql = _ADDDATE_RE.sub("DATE_ADD(", sql)
    sql = _SUBDATE_RE.sub("DATE_SUB(", sql)

    return sql


# ---------------------------------------------------------------------------
# Stage 1: sqlglot transpilation
# ---------------------------------------------------------------------------

def _sqlglot_transpile(sql: str) -> str | None:
    """
    Attempt MySQL→PostgreSQL transpilation via sqlglot.

    Returns the transpiled string on success, or None if sqlglot is not
    installed or raises an unrecoverable error.
    """
    try:
        import sqlglot
        import sqlglot.errors

        results = sqlglot.transpile(
            sql,
            read="mysql",
            write="postgres",
            error_level=sqlglot.errors.ErrorLevel.IGNORE,
            pretty=False,
        )
        if results and results[0]:
            return results[0]
    except ImportError:
        # sqlglot not installed; fall back to regex pipeline
        pass
    except Exception as exc:
        logger.debug("sqlglot transpilation failed (%s); using regex fallback", exc)
    return None


# ---------------------------------------------------------------------------
# Stage 2: post-processing regex fixups
# ---------------------------------------------------------------------------

# --- DATE_FORMAT: cover the most common format strings used in ERPNext/HRMS ---

_DATE_FORMAT_MAP: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y-%m-%d'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY-MM-DD')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%d-%m-%Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'DD-MM-YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y/%m/%d'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY/MM/DD')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%d/%m/%Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'DD/MM/YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y-%m'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY-MM')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%m'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'MM')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%d'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'DD')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%H:%i:%s'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'HH24:MI:SS')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%H:%i'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'HH24:MI')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y-%m-%d %H:%i:%s'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY-MM-DD HH24:MI:SS')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%b %Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'Mon YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%M %Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'FMMonth YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%M'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'FMMonth')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%W'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'FMDay')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%u'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'IW')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%j'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'DDD')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%m-%Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'MM-YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%Y-%m-%d %H:%i'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'YYYY-MM-DD HH24:MI')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%d %b %Y'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'DD Mon YYYY')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%e'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'FMDD')"),
    (re.compile(r"\bDATE_FORMAT\s*\(\s*(.+?)\s*,\s*'%c'\s*\)", re.I | re.S),
     r"TO_CHAR(\1, 'FMMM')"),
    # Catch-all: any remaining DATE_FORMAT → TO_CHAR (args passed through)
    (re.compile(r"\bDATE_FORMAT\s*\(", re.I),
     r"TO_CHAR("),
]

# --- GROUP_CONCAT: sqlglot may not handle SEPARATOR keyword variant ---

_GROUP_CONCAT_DISTINCT_SEP_RE = re.compile(
    r"\bGROUP_CONCAT\s*\(\s*DISTINCT\s+(.+?)\s+(?:ORDER\s+BY\s+\S+\s+)?SEPARATOR\s+'([^']*)'\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_GROUP_CONCAT_DISTINCT_RE = re.compile(
    r"\bGROUP_CONCAT\s*\(\s*DISTINCT\s+(.+?)\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_GROUP_CONCAT_SEP_RE = re.compile(
    r"\bGROUP_CONCAT\s*\(\s*(.+?)\s+SEPARATOR\s+'([^']*)'\s*\)",
    re.IGNORECASE | re.DOTALL,
)
_GROUP_CONCAT_RE = re.compile(
    r"\bGROUP_CONCAT\s*\((.+?)\)",
    re.IGNORECASE | re.DOTALL,
)


def _fix_group_concat(sql: str) -> str:
    """Convert all GROUP_CONCAT variants to STRING_AGG."""
    # Most specific first
    sql = _GROUP_CONCAT_DISTINCT_SEP_RE.sub(
        lambda m: f"STRING_AGG(DISTINCT CAST({m.group(1)} AS TEXT), '{m.group(2)}')", sql
    )
    sql = _GROUP_CONCAT_DISTINCT_RE.sub(
        lambda m: f"STRING_AGG(DISTINCT CAST({m.group(1)} AS TEXT), ',')", sql
    )
    sql = _GROUP_CONCAT_SEP_RE.sub(
        lambda m: f"STRING_AGG(CAST({m.group(1)} AS TEXT), '{m.group(2)}')", sql
    )
    sql = _GROUP_CONCAT_RE.sub(
        lambda m: f"STRING_AGG(CAST({m.group(1)} AS TEXT), ',')", sql
    )
    return sql


# --- LIMIT x, y  (MySQL "LIMIT offset, count") ---

_LIMIT_OFFSET_RE = re.compile(r"\bLIMIT\s+(\d+)\s*,\s*(\d+)", re.IGNORECASE)


def _fix_limit_offset(sql: str) -> str:
    return _LIMIT_OFFSET_RE.sub(lambda m: f"LIMIT {m.group(2)} OFFSET {m.group(1)}", sql)


# --- CAST to MySQL-only types ---

_CAST_UNSIGNED_RE = re.compile(r"\bCAST\s*\((.+?)\s+AS\s+UNSIGNED(?:\s+INT)?\)", re.IGNORECASE)
_CAST_SIGNED_RE = re.compile(r"\bCAST\s*\((.+?)\s+AS\s+SIGNED(?:\s+INT)?\)", re.IGNORECASE)
_CAST_CHAR_RE = re.compile(r"\bCAST\s*\((.+?)\s+AS\s+CHAR(?:\(\d+\))?\)", re.IGNORECASE)

# --- ISNULL(x) → (x IS NULL) ---

_ISNULL_RE = re.compile(r"\bISNULL\s*\(([^)]+)\)", re.IGNORECASE)

# --- IFNULL safety net (sqlglot should handle this) ---

_IFNULL_RE = re.compile(r"\bIFNULL\s*\(", re.IGNORECASE)

# --- CONVERT(x USING charset) → x::TEXT ---

_CONVERT_USING_RE = re.compile(
    r"\bCONVERT\s*\(\s*(.+?)\s+USING\s+\w+\s*\)", re.IGNORECASE
)

# --- FIELD(val, v1, v2, ...) → CASE WHEN (for ORDER BY FIELD patterns) ---

_FIELD_RE = re.compile(r"\bFIELD\s*\((.+?)\)", re.IGNORECASE)


def _fix_field_function(sql: str) -> str:
    """
    FIELD(val, v1, v2, v3) → CASE val WHEN v1 THEN 1 WHEN v2 THEN 2 … END
    Used in ORDER BY FIELD(...) patterns in ERPNext.
    """
    def _replace(m: re.Match) -> str:
        args_str = m.group(1)
        # Split by commas (simple split; nested parens are rare in FIELD())
        args = [a.strip() for a in args_str.split(",")]
        if len(args) < 2:
            return m.group(0)
        val = args[0]
        cases = " ".join(
            f"WHEN {v} THEN {i + 1}" for i, v in enumerate(args[1:])
        )
        return f"CASE {val} {cases} ELSE {len(args)} END"

    return _FIELD_RE.sub(_replace, sql)


# --- FIND_IN_SET(val, set_col) → val = ANY(STRING_TO_ARRAY(set_col, ',')) ---

_FIND_IN_SET_RE = re.compile(
    r"\bFIND_IN_SET\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)", re.IGNORECASE
)


def _fix_find_in_set(sql: str) -> str:
    return _FIND_IN_SET_RE.sub(
        lambda m: f"({m.group(1)} = ANY(STRING_TO_ARRAY({m.group(2)}, ',')))", sql
    )


# --- REGEXP / RLIKE → ~* (case-insensitive like MySQL default) ---

_REGEXP_RE = re.compile(r"\bR(?:EGEXP|LIKE)\b", re.IGNORECASE)


def _fix_regexp(sql: str) -> str:
    return _REGEXP_RE.sub("~*", sql)


# ---------------------------------------------------------------------------
# Legacy regex pipeline (fallback when sqlglot is unavailable or fails)
# ---------------------------------------------------------------------------

def _if_to_case(query: str) -> str:
    """Convert MySQL IF(cond, a, b) to CASE WHEN cond THEN a ELSE b END."""
    if_pattern = re.compile(r"\bIF\s*\(", re.IGNORECASE)
    max_iter = 200
    i = 0
    while if_pattern.search(query) and i < max_iter:
        i += 1
        m = if_pattern.search(query)
        if not m:
            break
        start = m.start()
        # Guard against matching DIFF(, ENDIF(, etc.
        if start > 0 and (query[start - 1].isalnum() or query[start - 1] == "_"):
            query = query[:start] + "__NOTIF__(" + query[m.end():]
            continue
        open_pos = m.end() - 1
        depth = 1
        pos = open_pos + 1
        in_str = False
        str_ch = None
        while pos < len(query) and depth > 0:
            c = query[pos]
            if c in ("'", '"') and not in_str:
                in_str, str_ch = True, c
            elif in_str and c == str_ch and (pos == 0 or query[pos - 1] != "\\"):
                in_str = False
            elif not in_str:
                if c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
            pos += 1
        if depth != 0:
            query = query[:start] + "__BADIF__(" + query[open_pos + 1:]
            continue
        inner = query[open_pos + 1 : pos - 1]
        # Split by top-level commas
        parts, cur, d2, ins = [], [], 0, False
        for ch in inner:
            if ch in ("'", '"') and not ins:
                ins, str_ch = True, ch
                cur.append(ch)
            elif ins and ch == str_ch:
                ins = False
                cur.append(ch)
            elif ins:
                cur.append(ch)
            elif ch == "(":
                d2 += 1
                cur.append(ch)
            elif ch == ")":
                d2 -= 1
                cur.append(ch)
            elif ch == "," and d2 == 0:
                parts.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
        if cur:
            parts.append("".join(cur))
        if len(parts) != 3:
            query = query[:start] + "__BADIF__(" + query[open_pos + 1:]
            continue
        cond, tv, fv = (p.strip() for p in parts)
        query = (
            query[:start]
            + f"CASE WHEN {cond} THEN {tv} ELSE {fv} END"
            + query[pos:]
        )
    query = query.replace("__NOTIF__(", "IF(").replace("__BADIF__(", "IF(")
    return query


_IFNULL_LEGACY_RE = re.compile(r"\bIFNULL\s*\(", re.IGNORECASE)
_DATE_FORMAT_LEGACY_RE = re.compile(
    r"\bDATE_FORMAT\s*\(\s*([^,]+?)\s*,\s*['\"]%Y-%m-%d['\"]s*\)",
    re.IGNORECASE,
)


def _legacy_transform(sql: str) -> str:
    """Full regex-only pipeline used when sqlglot is unavailable."""
    sql = _if_to_case(sql)
    sql = _IFNULL_LEGACY_RE.sub("COALESCE(", sql)
    for pattern, replacement in _DATE_FORMAT_MAP:
        sql = pattern.sub(replacement, sql)
    if re.search(r"\bGROUP_CONCAT\b", sql, re.IGNORECASE):
        sql = _fix_group_concat(sql)
    sql = _fix_limit_offset(sql)
    sql = _CAST_UNSIGNED_RE.sub(r"CAST(\1 AS BIGINT)", sql)
    sql = _CAST_SIGNED_RE.sub(r"CAST(\1 AS BIGINT)", sql)
    sql = _CAST_CHAR_RE.sub(r"CAST(\1 AS TEXT)", sql)
    sql = _ISNULL_RE.sub(r"(\1 IS NULL)", sql)
    sql = _CONVERT_USING_RE.sub(r"(\1)", sql)
    sql = _fix_field_function(sql)
    sql = _fix_find_in_set(sql)
    sql = _fix_regexp(sql)
    if re.search(r"\bSUBSTRING_INDEX\b", sql, re.IGNORECASE):
        sql = _SUBSTRING_INDEX_RE.sub(
            lambda m: f"substring_index({m.group(1).strip()}, {m.group(2).strip()}, {m.group(3).strip()})",
            sql,
        )
    return sql


# ---------------------------------------------------------------------------
# Post-sqlglot fixups (patterns sqlglot may not cover fully)
# ---------------------------------------------------------------------------

_SPACED_PLACEHOLDER_RE = re.compile(r"%\s+s")

# CREATE INDEX column fix: sqlglot reads MySQL dialect where "col" is a string literal,
# so it converts "col_name" identifiers in CREATE INDEX → 'col_name' NULLS FIRST.
# PostgreSQL requires double-quoted identifiers, not single-quoted strings.
# Pattern matches: 'identifier' NULLS FIRST  or  'identifier' NULLS LAST
_INDEX_COL_SINGLEQUOTED_RE = re.compile(
    r"'([A-Za-z_][A-Za-z0-9_ ]*)'\s+NULLS\s+(?:FIRST|LAST)",
    re.IGNORECASE,
)


def _post_sqlglot_fixups(sql: str) -> str:
    """Apply targeted fixups after sqlglot transpilation."""
    # sqlglot may add spaces around psycopg2 placeholders: '%s' → ' % s'
    # which causes ValueError: unsupported format character ' ' (0x20)
    sql = _SPACED_PLACEHOLDER_RE.sub("%s", sql)

    # Fix CREATE INDEX column identifiers corrupted by sqlglot MySQL dialect parsing.
    # sqlglot treats "col" as a MySQL string literal → 'col' NULLS FIRST.
    # Restore to proper PostgreSQL double-quoted identifiers.
    if re.search(r"\bNULLS\s+(?:FIRST|LAST)\b", sql, re.IGNORECASE):
        sql = _INDEX_COL_SINGLEQUOTED_RE.sub(r'"\1"', sql)

    # DATE_FORMAT safety net (sqlglot handles common cases; catch stragglers)
    if re.search(r"\bDATE_FORMAT\b", sql, re.IGNORECASE):
        for pattern, replacement in _DATE_FORMAT_MAP:
            sql = pattern.sub(replacement, sql)

    # GROUP_CONCAT safety net
    if re.search(r"\bGROUP_CONCAT\b", sql, re.IGNORECASE):
        sql = _fix_group_concat(sql)

    # LIMIT x, y  (sqlglot should handle, but catch edge cases)
    if re.search(r"\bLIMIT\s+\d+\s*,\s*\d+", sql, re.IGNORECASE):
        sql = _fix_limit_offset(sql)

    # MySQL-only CAST types
    sql = _CAST_UNSIGNED_RE.sub(r"CAST(\1 AS BIGINT)", sql)
    sql = _CAST_SIGNED_RE.sub(r"CAST(\1 AS BIGINT)", sql)
    sql = _CAST_CHAR_RE.sub(r"CAST(\1 AS TEXT)", sql)

    # ISNULL(x)
    sql = _ISNULL_RE.sub(r"(\1 IS NULL)", sql)

    # CONVERT(x USING charset) → (x)
    sql = _CONVERT_USING_RE.sub(r"(\1)", sql)

    # FIELD() function
    if re.search(r"\bFIELD\s*\(", sql, re.IGNORECASE):
        sql = _fix_field_function(sql)

    # FIND_IN_SET
    if re.search(r"\bFIND_IN_SET\b", sql, re.IGNORECASE):
        sql = _fix_find_in_set(sql)

    # SUBSTRING_INDEX → substring_index() DB-level function
    if re.search(r"\bSUBSTRING_INDEX\b", sql, re.IGNORECASE):
        sql = _SUBSTRING_INDEX_RE.sub(
            lambda m: f"substring_index({m.group(1).strip()}, {m.group(2).strip()}, {m.group(3).strip()})",
            sql,
        )

    return sql


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# DDL statements that define or drop database objects whose signatures may
# contain MySQL function names as identifiers (not as SQL calls to transform).
# For example: CREATE FUNCTION datediff(d1, d2) → _DATEDIFF_PRE_RE mangles it.
_FUNC_DDL_RE = re.compile(
    r"^\s*(?:CREATE|DROP)\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|AGGREGATE|PROCEDURE|TRIGGER)\b",
    re.IGNORECASE,
)


def apply_all_query_transformations(query: str) -> str:
    """
    Transform a MySQL/MariaDB SQL query to PostgreSQL-compatible SQL.

    Pipeline
    --------
    1. Guard:   non-string or empty → return as-is
    2. Guard:   CREATE/DROP FUNCTION/AGGREGATE DDL → return as-is (function
                signatures contain MySQL names as identifiers, not SQL calls)
    3. Pre:     strip FORCE/USE/IGNORE INDEX hints
    4. Stage 1: sqlglot MySQL→PG transpilation (handles ~95 % of cases)
    5. Stage 2: post-sqlglot fixups for edge cases
       - or -
       Fallback: legacy regex pipeline when sqlglot fails/unavailable

    This function is intentionally never-raise: any unhandled exception
    inside returns the (possibly partially transformed) input unchanged
    so that production traffic is never blocked by a transformation bug.
    """
    if not isinstance(query, str) or not query.strip():
        return query

    # Skip transformation for function/aggregate DDL. These definitions contain
    # MySQL function names as their own identifiers (e.g. CREATE FUNCTION datediff),
    # which preprocessing would mangle (datediff( → (d1::date - d2::date)).
    if _FUNC_DDL_RE.search(query):
        return query

    try:
        # Pre-processing
        sql = _preprocess(query)

        # Stage 1: sqlglot — only for queries with MySQL-specific syntax.
        # Queries from frappe's query_builder already use PG double-quoted
        # identifiers; passing them through sqlglot (read="mysql") would
        # corrupt "col" → 'col' (string literal instead of identifier).
        needs_transform = bool(_MYSQL_MARKER_RE.search(sql))
        transpiled = _sqlglot_transpile(sql) if needs_transform else None

        if transpiled:
            result = _post_sqlglot_fixups(transpiled)
        else:
            # Fallback: full regex pipeline
            result = _legacy_transform(sql)

        return result

    except Exception as exc:
        logger.warning("apply_all_query_transformations failed: %s", exc)
        return query  # return original, never crash the caller
