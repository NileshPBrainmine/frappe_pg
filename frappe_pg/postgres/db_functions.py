"""
PostgreSQL Compatibility Functions
===================================

Creates database-level PostgreSQL functions that emulate MySQL built-ins
used by ERPNext, HRMS, Raven, CRM, Education, and any other Frappe app.

Why DB-level functions instead of pure SQL transpilation?
  Some MySQL functions are used in ORDER BY / GROUP BY / HAVING clauses
  or inside stored-procedure-style queries where sqlglot cannot reach
  them (e.g. values passed as query strings at runtime, report queries
  stored as DocType Report scripts, or Jinja-rendered SQL).
  Having DB-level aliases means those queries work even without
  transpilation.

Functions / aggregates provided
--------------------------------
  GROUP_CONCAT(text)                    STRING_AGG equivalent
  GROUP_CONCAT(text, separator)         with custom separator
  unix_timestamp()                      current epoch (bigint)
  unix_timestamp(timestamptz)           epoch for given timestamp
  unix_timestamp(timestamp)             epoch for given timestamp (no tz)
  timestampdiff(unit, start, end)       integer diff in unit
  datediff(end, start)                  integer diff in days
  last_day(date)                        last day of month
  month(date)                           month number
  year(date)                            year number
  day(date)                             day number
  hour(ts)                              hour number
  minute(ts)                            minute number
  second(ts)                            second number
  dayofweek(date)                       MySQL-compatible 1=Sun..7=Sat
  dayofmonth(date)                      alias for day()
  dayofyear(date)                       day of year
  weekofyear(date)                      ISO week number
  quarter(date)                         quarter (1-4)
  week(date)                            alias for weekofyear
  to_days(date)                         MySQL to_days (days since yr 0)
  from_days(n)                          inverse of to_days
  period_diff(p1, p2)                   diff in months between YYYYMM periods
  str_to_date(str, fmt)                 parse date string
  date(ts)                              extract date part from timestamp
  time(ts)                              extract time part from timestamp
  date_format_compat(d, fmt)            DATE_FORMAT fall-through helper
  field(val VARIADIC any)               MySQL FIELD() positional match
  find_in_set(val, set_str)             MySQL FIND_IN_SET
  greatest / least                      already built-in to PG ✓
  coalesce                              already built-in to PG ✓
  rand()                                alias for random()
  lpad / rpad / ltrim / rtrim           already built-in to PG ✓
  char_length / length                  already built-in to PG ✓
  substring / substr                    already built-in to PG ✓
  instr(str, sub)                       MySQL INSTR → PG POSITION
  locate(sub, str)                      MySQL LOCATE → PG POSITION
  mid(str, pos, len)                    MySQL MID → PG SUBSTRING
  elt(n, ...)                           MySQL ELT (pick nth element)
  nullif                                already built-in to PG ✓
  ifnull(a, b)                          alias for COALESCE
  if_compat(cond, a, b)                 IF() fallback for dynamic SQL
"""

from __future__ import annotations

import frappe


# ---------------------------------------------------------------------------
# Helper: execute a batch of DDL statements
# ---------------------------------------------------------------------------

def _exec(statements: list[str]) -> tuple[int, int]:
    """Run each statement; return (success_count, error_count)."""
    ok = 0
    err = 0
    for sql in statements:
        sql = sql.strip()
        if not sql:
            continue
        try:
            if hasattr(frappe.db, "_conn") and frappe.db._conn:
                cur = frappe.db._conn.cursor()
                cur.execute(sql)
                frappe.db._conn.commit()
            else:
                frappe.db.sql(sql)
                frappe.db.commit()
            ok += 1
        except Exception as e:
            msg = str(e).lower()
            if "already exists" not in msg:
                err += 1
                print(f"  ⚠  Warning: {str(e)[:120]}")
    return ok, err


# ---------------------------------------------------------------------------
# Function definitions
# ---------------------------------------------------------------------------

_FUNCTIONS: list[str] = [

    # ------------------------------------------------------------------
    # GROUP_CONCAT  (aggregate)
    # ------------------------------------------------------------------
    "DROP AGGREGATE IF EXISTS GROUP_CONCAT(text) CASCADE",
    "DROP FUNCTION  IF EXISTS group_concat_sfunc(text, text) CASCADE",

    """
    CREATE OR REPLACE FUNCTION group_concat_sfunc(text, text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
        SELECT CASE
            WHEN $1 IS NULL THEN $2
            WHEN $2 IS NULL THEN $1
            ELSE $1 || ',' || $2
        END
    $$
    """,

    """
    CREATE AGGREGATE GROUP_CONCAT(text) (
        SFUNC = group_concat_sfunc,
        STYPE = text
    )
    """,

    # GROUP_CONCAT with separator: group_concat_sep(text, sep)
    "DROP AGGREGATE IF EXISTS group_concat_sep(text, text) CASCADE",
    "DROP FUNCTION  IF EXISTS group_concat_sep_sfunc(text, text, text) CASCADE",

    """
    CREATE OR REPLACE FUNCTION group_concat_sep_sfunc(state text, val text, sep text)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
        SELECT CASE
            WHEN state IS NULL THEN val
            WHEN val   IS NULL THEN state
            ELSE state || sep || val
        END
    $$
    """,

    """
    CREATE AGGREGATE group_concat_sep(text, text) (
        SFUNC = group_concat_sep_sfunc,
        STYPE = text
    )
    """,

    # ------------------------------------------------------------------
    # UNIX_TIMESTAMP
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION unix_timestamp(ts timestamptz DEFAULT NOW())
    RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
        SELECT EXTRACT(EPOCH FROM ts)::bigint
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION unix_timestamp(ts timestamp)
    RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
        SELECT EXTRACT(EPOCH FROM ts AT TIME ZONE 'UTC')::bigint
    $$
    """,

    # ------------------------------------------------------------------
    # TIMESTAMPDIFF(unit, start, end)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION timestampdiff(unit text, t1 timestamp, t2 timestamp)
    RETURNS integer LANGUAGE plpgsql IMMUTABLE AS $$
    BEGIN
        CASE LOWER(unit)
            WHEN 'second'  THEN RETURN EXTRACT(EPOCH FROM (t2 - t1))::integer;
            WHEN 'minute'  THEN RETURN (EXTRACT(EPOCH FROM (t2 - t1)) / 60)::integer;
            WHEN 'hour'    THEN RETURN (EXTRACT(EPOCH FROM (t2 - t1)) / 3600)::integer;
            WHEN 'day'     THEN RETURN (t2::date - t1::date);
            WHEN 'week'    THEN RETURN ((t2::date - t1::date) / 7);
            WHEN 'month'   THEN RETURN (
                (EXTRACT(YEAR FROM t2) - EXTRACT(YEAR FROM t1)) * 12
                + EXTRACT(MONTH FROM t2) - EXTRACT(MONTH FROM t1)
            )::integer;
            WHEN 'quarter' THEN RETURN (
                ((EXTRACT(YEAR FROM t2) - EXTRACT(YEAR FROM t1)) * 12
                + EXTRACT(MONTH FROM t2) - EXTRACT(MONTH FROM t1)) / 3
            )::integer;
            WHEN 'year'    THEN RETURN (EXTRACT(YEAR FROM t2) - EXTRACT(YEAR FROM t1))::integer;
            ELSE RAISE EXCEPTION 'timestampdiff: unsupported unit %', unit;
        END CASE;
    END
    $$
    """,

    # ------------------------------------------------------------------
    # DATEDIFF(end, start)  → integer days
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION datediff(d1 date, d2 date)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT (d1 - d2)::integer
    $$
    """,

    # text overload (frappe sometimes passes date strings)
    """
    CREATE OR REPLACE FUNCTION datediff(d1 text, d2 text)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT (d1::date - d2::date)::integer
    $$
    """,

    # ------------------------------------------------------------------
    # LAST_DAY(date)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION last_day(d date)
    RETURNS date LANGUAGE sql IMMUTABLE AS $$
        SELECT (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::date
    $$
    """,

    # ------------------------------------------------------------------
    # Date component extractors
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION month(d date)      RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(MONTH FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION year(d date)       RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(YEAR  FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION day(d date)        RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(DAY   FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION dayofmonth(d date) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(DAY   FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION dayofyear(d date)  RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(DOY   FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION weekofyear(d date) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(WEEK  FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION week(d date)       RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(WEEK  FROM d)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION quarter(d date)    RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(QUARTER FROM d)::integer $$
    """,
    # dayofweek: MySQL = 1(Sun)..7(Sat), PG DOW = 0(Sun)..6(Sat)
    """
    CREATE OR REPLACE FUNCTION dayofweek(d date)  RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT (EXTRACT(DOW FROM d)::integer + 1) $$
    """,

    # Timestamp overloads
    """
    CREATE OR REPLACE FUNCTION month(ts timestamp)      RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(MONTH FROM ts)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION year(ts timestamp)       RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(YEAR  FROM ts)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION day(ts timestamp)        RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(DAY   FROM ts)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION hour(ts timestamp)       RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(HOUR  FROM ts)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION minute(ts timestamp)     RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(MINUTE FROM ts)::integer $$
    """,
    """
    CREATE OR REPLACE FUNCTION second(ts timestamp)     RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT EXTRACT(SECOND FROM ts)::integer $$
    """,

    # ------------------------------------------------------------------
    # TO_DAYS / FROM_DAYS  (MySQL day-number since year 0)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION to_days(d date)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT (d - '0001-01-01'::date + 366)::integer
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION from_days(n integer)
    RETURNS date LANGUAGE sql IMMUTABLE AS $$
        SELECT ('0001-01-01'::date + (n - 366) * INTERVAL '1 day')::date
    $$
    """,

    # ------------------------------------------------------------------
    # PERIOD_DIFF(p1, p2)  p1/p2 in YYYYMM format → months difference
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION period_diff(p1 integer, p2 integer)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT (
            (p1 / 100 * 12 + p1 % 100)
            - (p2 / 100 * 12 + p2 % 100)
        )
    $$
    """,

    # ------------------------------------------------------------------
    # STR_TO_DATE(str, fmt)  — basic MySQL format → PG TO_DATE
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION str_to_date(s text, fmt text)
    RETURNS date LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE
        pg_fmt text := fmt;
    BEGIN
        pg_fmt := REPLACE(pg_fmt, '%Y', 'YYYY');
        pg_fmt := REPLACE(pg_fmt, '%m', 'MM');
        pg_fmt := REPLACE(pg_fmt, '%d', 'DD');
        pg_fmt := REPLACE(pg_fmt, '%H', 'HH24');
        pg_fmt := REPLACE(pg_fmt, '%i', 'MI');
        pg_fmt := REPLACE(pg_fmt, '%s', 'SS');
        RETURN TO_DATE(s, pg_fmt);
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END
    $$
    """,

    # ------------------------------------------------------------------
    # DATE(timestamp) / TIME(timestamp)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION date(ts timestamp)
    RETURNS date LANGUAGE sql IMMUTABLE AS $$
        SELECT ts::date
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION time(ts timestamp)
    RETURNS time LANGUAGE sql IMMUTABLE AS $$
        SELECT ts::time
    $$
    """,

    # ------------------------------------------------------------------
    # RAND()  — alias for RANDOM()
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION rand()
    RETURNS double precision LANGUAGE sql VOLATILE AS $$
        SELECT RANDOM()
    $$
    """,

    # ------------------------------------------------------------------
    # INSTR(str, substr) → POSITION(substr IN str)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION instr(haystack text, needle text)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT POSITION(needle IN haystack)
    $$
    """,

    # ------------------------------------------------------------------
    # LOCATE(substr, str [, pos])
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION locate(needle text, haystack text)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT POSITION(needle IN haystack)
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION locate(needle text, haystack text, startpos integer)
    RETURNS integer LANGUAGE sql IMMUTABLE AS $$
        SELECT CASE
            WHEN POSITION(needle IN SUBSTRING(haystack FROM startpos)) = 0 THEN 0
            ELSE POSITION(needle IN SUBSTRING(haystack FROM startpos)) + startpos - 1
        END
    $$
    """,

    # ------------------------------------------------------------------
    # MID(str, pos, len)  → SUBSTRING
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION mid(s text, pos integer, len integer)
    RETURNS text LANGUAGE sql IMMUTABLE AS $$
        SELECT SUBSTRING(s FROM pos FOR len)
    $$
    """,

    # ------------------------------------------------------------------
    # IFNULL(a, b)  → COALESCE
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION ifnull(a anyelement, b anyelement)
    RETURNS anyelement LANGUAGE sql IMMUTABLE AS $$
        SELECT COALESCE(a, b)
    $$
    """,

    # ------------------------------------------------------------------
    # IF(cond, a, b)  — fallback for dynamic SQL that slips through
    # Note: SQL-level transpilation handles most cases; this is a
    # safety net for queries evaluated at runtime.
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION if_compat(cond boolean, a anyelement, b anyelement)
    RETURNS anyelement LANGUAGE sql IMMUTABLE AS $$
        SELECT CASE WHEN cond THEN a ELSE b END
    $$
    """,

    # ------------------------------------------------------------------
    # FIND_IN_SET(val, set_string)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION find_in_set(val text, set_str text)
    RETURNS integer LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE
        arr text[];
        i   integer;
    BEGIN
        arr := STRING_TO_ARRAY(set_str, ',');
        FOR i IN 1 .. ARRAY_LENGTH(arr, 1) LOOP
            IF TRIM(arr[i]) = val THEN RETURN i; END IF;
        END LOOP;
        RETURN 0;
    END
    $$
    """,

    # ------------------------------------------------------------------
    # FIELD(val, v1, v2, ...) — variadic positional match
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION field(val text, VARIADIC options text[])
    RETURNS integer LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE i integer;
    BEGIN
        FOR i IN 1 .. ARRAY_LENGTH(options, 1) LOOP
            IF val = options[i] THEN RETURN i; END IF;
        END LOOP;
        RETURN 0;
    END
    $$
    """,

    # ------------------------------------------------------------------
    # ELT(n, s1, s2, ...)  — return nth element
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION elt(n integer, VARIADIC options text[])
    RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
    BEGIN
        IF n < 1 OR n > ARRAY_LENGTH(options, 1) THEN RETURN NULL; END IF;
        RETURN options[n];
    END
    $$
    """,

    # ------------------------------------------------------------------
    # ISNULL(x)  — MySQL compatibility
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION isnull(x anyelement)
    RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
        SELECT x IS NULL
    $$
    """,

    # ------------------------------------------------------------------
    # CURDATE() / CURTIME() / SYSDATE()
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION curdate()
    RETURNS date LANGUAGE sql STABLE AS $$
        SELECT CURRENT_DATE
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION curtime()
    RETURNS time LANGUAGE sql STABLE AS $$
        SELECT CURRENT_TIME::time
    $$
    """,

    """
    CREATE OR REPLACE FUNCTION sysdate()
    RETURNS timestamp LANGUAGE sql STABLE AS $$
        SELECT NOW()::timestamp
    $$
    """,

    # ------------------------------------------------------------------
    # PERIOD_ADD(period, n)  — add n months to YYYYMM
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION period_add(period integer, n integer)
    RETURNS integer LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE
        yr  integer := period / 100;
        mo  integer := period % 100 + n;
    BEGIN
        yr := yr + (mo - 1) / 12;
        mo := ((mo - 1) % 12) + 1;
        RETURN yr * 100 + mo;
    END
    $$
    """,
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_missing_functions() -> None:
    """
    Create all PostgreSQL compatibility functions.

    Called during:
      - frappe_pg app installation  (hooks.py → after_install)
      - site migration              (hooks.py → after_migrate)
      - manual trigger              (bench execute frappe_pg.install_db_functions.install)
    """
    if not frappe.db:
        print("⚠  Database not available, skipping function creation")
        return

    print("Creating PostgreSQL compatibility functions…")
    ok, err = _exec(_FUNCTIONS)

    if err == 0:
        print(f"✓  All PostgreSQL compatibility functions ready ({ok} statements)")
    else:
        print(f"⚠  Functions created with {err} warning(s) ({ok} succeeded)")


def verify_db_functions() -> bool:
    """Smoke-test the most critical compatibility functions."""
    if not frappe.db:
        return False

    tests = [
        ("GROUP_CONCAT",
         "SELECT GROUP_CONCAT(v) FROM (VALUES ('a'),('b'),('c')) t(v)",
         "a,b,c"),
        ("unix_timestamp",
         "SELECT unix_timestamp() > 0",
         True),
        ("datediff",
         "SELECT datediff('2024-01-31'::date, '2024-01-01'::date)",
         30),
        ("timestampdiff days",
         "SELECT timestampdiff('day','2024-01-01'::timestamp,'2024-01-31'::timestamp)",
         30),
        ("last_day",
         "SELECT last_day('2024-02-01'::date)",
         "2024-02-29"),
        ("find_in_set",
         "SELECT find_in_set('b', 'a,b,c')",
         2),
        ("dayofweek Sunday",
         "SELECT dayofweek('2024-01-07'::date)",  # 2024-01-07 is Sunday
         1),
    ]

    passed = 0
    for name, query, expected in tests:
        try:
            row = frappe.db.sql(query, as_list=True)
            val = row[0][0] if row else None
            val_str = str(val) if not isinstance(val, bool) else val
            exp_str = str(expected) if not isinstance(expected, bool) else expected
            ok = (val_str == exp_str) or (val == expected)
            status = "✓" if ok else "✗"
            if not ok:
                print(f"  {status} {name}: expected {expected!r}, got {val!r}")
            else:
                passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    total = len(tests)
    print(f"  {passed}/{total} function tests passed")
    return passed == total


def drop_all_functions() -> None:
    """Drop all compatibility functions (for clean reinstall)."""
    drops = [
        "DROP AGGREGATE IF EXISTS GROUP_CONCAT(text) CASCADE",
        "DROP AGGREGATE IF EXISTS group_concat_sep(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS group_concat_sfunc(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS group_concat_sep_sfunc(text, text, text) CASCADE",
        "DROP FUNCTION IF EXISTS unix_timestamp(timestamptz) CASCADE",
        "DROP FUNCTION IF EXISTS unix_timestamp(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS unix_timestamp() CASCADE",
        "DROP FUNCTION IF EXISTS timestampdiff(text, timestamp, timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS datediff(date, date) CASCADE",
        "DROP FUNCTION IF EXISTS datediff(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS last_day(date) CASCADE",
        "DROP FUNCTION IF EXISTS month(date) CASCADE",
        "DROP FUNCTION IF EXISTS year(date) CASCADE",
        "DROP FUNCTION IF EXISTS day(date) CASCADE",
        "DROP FUNCTION IF EXISTS dayofmonth(date) CASCADE",
        "DROP FUNCTION IF EXISTS dayofyear(date) CASCADE",
        "DROP FUNCTION IF EXISTS weekofyear(date) CASCADE",
        "DROP FUNCTION IF EXISTS week(date) CASCADE",
        "DROP FUNCTION IF EXISTS quarter(date) CASCADE",
        "DROP FUNCTION IF EXISTS dayofweek(date) CASCADE",
        "DROP FUNCTION IF EXISTS month(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS year(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS day(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS hour(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS minute(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS second(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS to_days(date) CASCADE",
        "DROP FUNCTION IF EXISTS from_days(integer) CASCADE",
        "DROP FUNCTION IF EXISTS period_diff(integer, integer) CASCADE",
        "DROP FUNCTION IF EXISTS period_add(integer, integer) CASCADE",
        "DROP FUNCTION IF EXISTS str_to_date(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS date(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS time(timestamp) CASCADE",
        "DROP FUNCTION IF EXISTS rand() CASCADE",
        "DROP FUNCTION IF EXISTS instr(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS locate(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS locate(text, text, integer) CASCADE",
        "DROP FUNCTION IF EXISTS mid(text, integer, integer) CASCADE",
        "DROP FUNCTION IF EXISTS ifnull(anyelement, anyelement) CASCADE",
        "DROP FUNCTION IF EXISTS if_compat(boolean, anyelement, anyelement) CASCADE",
        "DROP FUNCTION IF EXISTS find_in_set(text, text) CASCADE",
        "DROP FUNCTION IF EXISTS field(text, text[]) CASCADE",
        "DROP FUNCTION IF EXISTS elt(integer, text[]) CASCADE",
        "DROP FUNCTION IF EXISTS isnull(anyelement) CASCADE",
        "DROP FUNCTION IF EXISTS curdate() CASCADE",
        "DROP FUNCTION IF EXISTS curtime() CASCADE",
        "DROP FUNCTION IF EXISTS sysdate() CASCADE",
    ]
    _exec(drops)
    print("✓  All PostgreSQL compatibility functions dropped")
