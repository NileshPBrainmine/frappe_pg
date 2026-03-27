"""
Patch: Fix ERPNext Trends Report GROUP BY Issues
================================================

This patch fixes PostgreSQL GROUP BY strictness issues in ERPNext's trends.py.
PostgreSQL requires all non-aggregated columns in SELECT to be in GROUP BY.

Fixes GROUP BY for:
- Item-based queries (adds item_name)
- Customer-based queries (adds customer_name, territory)
- Supplier-based queries (adds supplier_name, supplier_group)
- All queries (adds default_currency - critical universal fix)

Date: 2025-12-09
Version: 1.1.0
"""

import frappe


def execute():
    """
    Execute the ERPNext trends.py GROUP BY fix patch.

    This function is called by Frappe's patch system during migration.
    """
    print("\n" + "=" * 70)
    print("PATCH: Fixing ERPNext Trends Report GROUP BY for PostgreSQL")
    print("=" * 70)

    try:
        apply_trends_patch()
        print("\n ERPNext Trends Report GROUP BY Fix Completed Successfully")
    except ImportError:
        print("\n  ERPNext not installed - skipping trends.py patch")
    except Exception as e:
        print(f"\n Error applying trends patch: {e}")
        raise

    print("=" * 70 + "\n")


def apply_trends_patch():
    """
    Patch ERPNext's trends.py to add missing columns to GROUP BY clauses.

    Idempotent: safe to call multiple times and after ERPNext module reloads.
    Re-applies automatically if the module is reloaded (detected via
    _frappe_pg_patched marker on the function object).

    Returns:
        bool: True if patch applied (or already applied)

    Raises:
        ImportError: If ERPNext is not installed
        Exception: If patching fails
    """
    from erpnext.controllers import trends

    # Idempotency guard: if our marker is already on the current function,
    # the patch is in place. If ERPNext module was reloaded, the original
    # function (without the marker) is restored and we re-patch.
    if getattr(trends.based_wise_columns_query, "_frappe_pg_patched", False):
        return True

    _original_based_wise_columns_query = trends.based_wise_columns_query

    def patched_based_wise_columns_query(based_on, trans):
        """
        Patched version that fixes GROUP BY clauses for PostgreSQL compatibility.

        PostgreSQL requires all non-aggregated columns in SELECT to be in GROUP BY.
        This patches ERPNext's trends.py which was written for MySQL's lenient behavior.
        """
        based_on_details = _original_based_wise_columns_query(based_on, trans)

        current_group_by = based_on_details.get("based_on_group_by", "")
        select_str = based_on_details.get("based_on_select", "")

        # Fix GROUP BY for Item-based queries
        # ERPNext SELECT: t2.item_code, t2.item_name — GROUP BY: t2.item_code
        if based_on == "Item":
            if "item_name" in select_str and "item_name" not in current_group_by:
                current_group_by += ", t2.item_name"

        # Fix GROUP BY for Customer-based queries
        # ERPNext SELECT: t1.party_name/t1.customer, t1.customer_name, t1.territory
        # GROUP BY: t1.party_name or t1.customer
        elif based_on == "Customer":
            if "customer_name" in select_str and "customer_name" not in current_group_by:
                current_group_by += ", t1.customer_name"
            if "territory" in select_str and "territory" not in current_group_by:
                current_group_by += ", t1.territory"

        # Fix GROUP BY for Supplier-based queries
        # ERPNext SELECT: t1.supplier, t1.supplier_name, t3.supplier_group
        # GROUP BY: t1.supplier  (missing supplier_name AND supplier_group)
        elif based_on == "Supplier":
            if "supplier_name" in select_str and "supplier_name" not in current_group_by:
                current_group_by += ", t1.supplier_name"
            if "supplier_group" in select_str and "supplier_group" not in current_group_by:
                current_group_by += ", t3.supplier_group"

        # CRITICAL FIX: t4.default_currency is appended to SELECT for ALL reports
        # in trends.py but is never added to GROUP BY — universal PostgreSQL failure.
        if "default_currency" in select_str and "default_currency" not in current_group_by:
            current_group_by += ", t4.default_currency"

        based_on_details["based_on_group_by"] = current_group_by
        return based_on_details

    # Mark so we can detect re-patching attempts (idempotency)
    patched_based_wise_columns_query._frappe_pg_patched = True

    trends.based_wise_columns_query = patched_based_wise_columns_query

    print(" ERPNext trends.py patched for PostgreSQL GROUP BY compatibility")
    return True
