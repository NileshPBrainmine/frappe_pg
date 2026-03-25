"""
Database Method Patches for PostgreSQL Compatibility
=====================================================

Monkey-patches Frappe's PostgresDatabase class to make it production-ready
for ERPNext, HRMS, Raven, CRM, Education, and any other Frappe app.

Key behaviours
--------------

1. Query transformation  (patched_sql)
   Every SQL statement passes through apply_all_query_transformations()
   which uses sqlglot to transpile MySQL syntax to PostgreSQL, with a
   comprehensive regex fallback.  See query_transformers.py for details.

2. Per-query savepoints  (patched_sql)
   MySQL lets a failing query be retried; the surrounding transaction
   continues.  PostgreSQL aborts the ENTIRE transaction on any query
   error.  To match MySQL behaviour every query is wrapped in:
       SAVEPOINT frappe_pg_sp_N
       <actual query>
       RELEASE SAVEPOINT frappe_pg_sp_N   -- success
       ROLLBACK TO SAVEPOINT frappe_pg_sp_N  -- failure

   This ensures that a single bad query (e.g. a hook querying a column
   that doesn't exist yet) only rolls back itself; the caller's
   transaction — and its other work — remains intact.

3. Savepoint-aware rollback  (patched_rollback)
   Frappe v15 calls frappe.db.rollback(save_point=name) to roll back to
   a named savepoint during error recovery.  The original frappe_pg did
   not accept the save_point kwarg, causing a TypeError that killed the
   setup wizard.  patched_rollback handles both full rollback (no kwarg)
   and partial rollback (save_point=name).

4. Commit pass-through  (patched_commit)
   Thin wrapper; logs commit failures for diagnostics.

Production edge-case coverage
------------------------------
- Any exception inside a savepoint only affects that query.
- Transaction-control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT,
  RELEASE, SET) are never wrapped in savepoints.
- The savepoint counter is per-thread so concurrent requests are safe.
- Patches are applied exactly once (idempotent guard via _patches_applied).
- % escaping: lone % in queries without bound values is doubled so
  psycopg2 does not interpret it as a format-string placeholder.
"""

from __future__ import annotations

import threading

import psycopg2.errors
import psycopg2.extensions

import frappe
from frappe.database.postgres.database import PostgresDatabase

from .query_transformers import apply_all_query_transformations
from .db_functions import create_missing_functions


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_original_sql: object = None
_original_commit: object = None
_original_rollback: object = None
_patches_applied: bool = False

_sp_counter = threading.local()


def _next_sp_name() -> str:
    """Return a unique per-thread savepoint name."""
    if not hasattr(_sp_counter, "n"):
        _sp_counter.n = 0
    _sp_counter.n = (_sp_counter.n + 1) % 1_000_000
    return f"frappe_pg_sp_{_sp_counter.n}"


def _in_active_transaction(conn) -> bool:
    """Return True when the psycopg2 connection has an open transaction."""
    try:
        return conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION
    except Exception:
        return False


# ---------------------------------------------------------------------------
# patched_sql
# ---------------------------------------------------------------------------

# Statements that must NOT be wrapped in a savepoint
_TXN_CTRL_PREFIXES = (
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT",
    "RELEASE SAVEPOINT", "SET ", "SET\t",
)

# Import base Database.sql once at module level to avoid repeated lookups
import frappe.database.database as _frappe_db_module  # noqa: E402


def patched_sql(self, query, values=(), *args, **kwargs):
    """
    Replacement for PostgresDatabase.sql with:
      - MySQL→PG query transformation
      - % escaping
      - Per-query automatic savepoints
    """
    from frappe.database.postgres.database import (
        modify_query,
        modify_values as pg_modify_values,
    )

    _BASE_SQL = _frappe_db_module.Database.sql

    # ── Step 1: transform MySQL syntax ──────────────────────────────────────
    transformed = apply_all_query_transformations(query)

    # ── Step 2: apply Frappe's own PG normalisation ─────────────────────────
    pg_query = modify_query(transformed)
    pg_values = pg_modify_values(values)

    # ── Step 3: escape lone % when no bound values ───────────────────────────
    if not pg_values:
        pg_query = pg_query.replace("%", "%%")

    # ── Step 4: decide whether to use a per-query savepoint ─────────────────
    q_up = pg_query.strip().upper()
    is_txn_ctrl = any(q_up.startswith(pfx) for pfx in _TXN_CTRL_PREFIXES)

    sp_name: str | None = None
    # frappe v15 uses self.conn; older versions used self.con
    _db_conn = getattr(self, "conn", None) or getattr(self, "con", None)
    if not is_txn_ctrl and _in_active_transaction(_db_conn):
        sp_name = _next_sp_name()
        try:
            _BASE_SQL(self, f"SAVEPOINT {sp_name}")
        except Exception:
            sp_name = None  # couldn't create savepoint; proceed without

    # ── Step 5: execute ──────────────────────────────────────────────────────
    try:
        result = _BASE_SQL(self, pg_query, pg_values, *args, **kwargs)
        if sp_name:
            try:
                _BASE_SQL(self, f"RELEASE SAVEPOINT {sp_name}")
            except Exception:
                pass  # non-fatal; released automatically on COMMIT
        return result

    except Exception as exc:
        error_msg = str(exc).lower()

        if sp_name:
            # Roll back only this query; surrounding transaction survives
            try:
                _BASE_SQL(self, f"ROLLBACK TO SAVEPOINT {sp_name}")
            except Exception:
                # Savepoint gone (e.g. outer ROLLBACK beat us)
                _safe_rollback(self)
        else:
            # Not in a transaction, or already aborted — full rollback
            if (
                isinstance(exc, psycopg2.errors.InFailedSqlTransaction)
                or "transaction is aborted" in error_msg
                or "infailedsqltransaction" in error_msg
            ):
                _safe_rollback(self)

        # Diagnostic logging (never blocks the caller)
        _log_sql_error(exc, error_msg, transformed, query, values)

        raise


def _safe_rollback(db_instance) -> None:
    try:
        _original_rollback(db_instance)
    except Exception:
        pass


def _log_sql_error(exc, error_msg: str, transformed: str, original: str, values) -> None:
    try:
        if "syntax error" in error_msg or isinstance(exc, psycopg2.errors.SyntaxError):
            frappe.log_error(
                title="PostgreSQL Syntax Error",
                message=(
                    f"Transformed: {transformed[:1000]}\n\n"
                    f"Original:    {original[:1000]}\n\n"
                    f"Values:      {str(values)[:500]}\n\n"
                    f"Error:       {exc}"
                ),
            )
        elif "function" in error_msg and "does not exist" in error_msg:
            frappe.log_error(
                title="PostgreSQL Function Not Found",
                message=(
                    f"Transformed: {transformed[:1000]}\n\n"
                    f"Original:    {original[:1000]}\n\n"
                    f"Error:       {exc}\n\n"
                    "Hint: run bench execute frappe_pg.install_db_functions.install"
                ),
            )
    except Exception:
        pass  # logging must never crash the caller


# ---------------------------------------------------------------------------
# patched_commit
# ---------------------------------------------------------------------------

def patched_commit(self):
    try:
        return _original_commit(self)
    except Exception as exc:
        try:
            frappe.log_error(title="PostgreSQL Commit Failed", message=str(exc))
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# patched_rollback
# ---------------------------------------------------------------------------

def patched_rollback(self, *, save_point=None):
    """
    Full rollback (save_point=None) or savepoint rollback (save_point='name').

    Frappe v15 calls frappe.db.rollback(save_point=name) during error
    recovery inside the savepoint context manager.  Without this kwarg
    support the setup wizard would crash with TypeError.
    """
    if save_point:
        try:
            _frappe_db_module.Database.sql(
                self, f"ROLLBACK TO SAVEPOINT {save_point}"
            )
        except Exception:
            pass
        return
    try:
        return _original_rollback(self)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Patch application
# ---------------------------------------------------------------------------

def apply_postgres_fixes() -> None:
    """
    Monkey-patch PostgresDatabase exactly once.

    Idempotent: calling it multiple times is safe (re-entry is a no-op).
    """
    global _original_sql, _original_commit, _original_rollback, _patches_applied

    if _patches_applied:
        return

    _original_sql = PostgresDatabase.sql
    _original_commit = PostgresDatabase.commit
    _original_rollback = PostgresDatabase.rollback

    PostgresDatabase.sql = patched_sql
    PostgresDatabase.commit = patched_commit
    PostgresDatabase.rollback = patched_rollback

    _patches_applied = True

    print("=" * 60)
    print("PostgreSQL compatibility patches applied (frappe_pg)")
    print("  ✓ sqlglot query transpilation")
    print("  ✓ per-query savepoints (MySQL isolation behaviour)")
    print("  ✓ patched_rollback with save_point support")
    print("  ✓ % escaping for unbound queries")
    print("=" * 60)


def on_session_creation(login_manager) -> None:
    """Re-apply patches on login (guards against module reload)."""
    apply_postgres_fixes()


def after_migrate() -> None:
    """Post-migration hook: ensure patches + DB functions are in place."""
    print("\nRunning post-migration PostgreSQL setup…")
    apply_postgres_fixes()
    create_missing_functions()


def check_patches_status() -> dict:
    return {
        "patches_applied": _patches_applied,
        "sql_patched": PostgresDatabase.sql is patched_sql,
        "commit_patched": PostgresDatabase.commit is patched_commit,
        "rollback_patched": PostgresDatabase.rollback is patched_rollback,
    }


# ---------------------------------------------------------------------------
# Auto-apply on import
# ---------------------------------------------------------------------------

try:
    apply_postgres_fixes()
except Exception as _e:
    print(f"Warning: frappe_pg could not apply patches on import: {_e}")
