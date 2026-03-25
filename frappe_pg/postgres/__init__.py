"""
Frappe PostgreSQL - Core PostgreSQL Compatibility Modules
=========================================================

This package contains core modules for PostgreSQL compatibility:
- query_transformers: SQL query transformation functions
- database_patches: Database method monkey patches
- db_functions: PostgreSQL function creation and emulation
"""

from .query_transformers import apply_all_query_transformations

try:
    from .database_patches import (
        apply_postgres_fixes,
        on_session_creation,
        after_migrate,
    )
    from .db_functions import (
        create_missing_functions,
        verify_db_functions,
    )
except ImportError:
    pass

__all__ = [
    # Query transformers
    'apply_all_query_transformations',

    # Database patches
    'apply_postgres_fixes',
    'on_session_creation',
    'after_migrate',

    # Database functions
    'create_missing_functions',
    'verify_db_functions'
]
