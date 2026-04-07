from __future__ import annotations

from psycopg import Connection

from sentinelbudget.db.repositories.categories import CategoryRepository

DEFAULT_CATEGORY_TREE: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Income", ("Salary", "Bonus", "Interest", "Other Income")),
    ("Housing", ("Rent", "Mortgage", "Property Tax", "Maintenance")),
    ("Transportation", ("Fuel", "Public Transit", "Auto Maintenance", "Parking")),
    ("Food", ("Groceries", "Dining Out", "Coffee")),
    ("Utilities", ("Electric", "Water", "Internet", "Phone")),
    ("Insurance", ("Health Insurance", "Home Insurance", "Auto Insurance")),
    ("Healthcare", ("Medication", "Doctor Visits", "Dental")),
    ("Debt", ("Credit Card", "Student Loan", "Personal Loan")),
    ("Savings", ("Emergency Fund", "Retirement", "Investments")),
    ("Entertainment", ("Subscriptions", "Events", "Travel")),
    ("Miscellaneous", ("Gifts", "Charity", "Other")),
)


def bootstrap_default_categories(conn: Connection) -> int:
    """Insert a deterministic starter category hierarchy if categories are missing."""

    inserted = 0

    for parent_name, children in DEFAULT_CATEGORY_TREE:
        parent = CategoryRepository.get_by_name_and_parent(conn, parent_name, None)
        if parent is None:
            parent = CategoryRepository.create(conn, name=parent_name, parent_id=None)
            inserted += 1

        for child_name in children:
            child = CategoryRepository.get_by_name_and_parent(
                conn,
                name=child_name,
                parent_id=parent.category_id,
            )
            if child is None:
                CategoryRepository.create(conn, name=child_name, parent_id=parent.category_id)
                inserted += 1

    return inserted
