from __future__ import annotations

from dataclasses import dataclass

from psycopg import Connection


@dataclass(frozen=True, slots=True)
class Category:
    category_id: int
    name: str
    parent_id: int | None


class CategoryRepository:
    """Repository for categories table access."""

    @staticmethod
    def create(conn: Connection, name: str, parent_id: int | None = None) -> Category:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO categories (name, parent_id)
                VALUES (%s, %s)
                RETURNING category_id, name, parent_id;
                """,
                (name, parent_id),
            )
            row = cur.fetchone()

        if row is None:  # pragma: no cover
            raise RuntimeError("Failed to insert category")

        return Category(category_id=row[0], name=row[1], parent_id=row[2])

    @staticmethod
    def list_all(conn: Connection) -> list[Category]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT category_id, name, parent_id
                FROM categories
                ORDER BY parent_id NULLS FIRST, name ASC, category_id ASC;
                """
            )
            rows = cur.fetchall()

        return [Category(category_id=row[0], name=row[1], parent_id=row[2]) for row in rows]

    @staticmethod
    def get_by_name_and_parent(
        conn: Connection,
        name: str,
        parent_id: int | None,
    ) -> Category | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT category_id, name, parent_id
                FROM categories
                WHERE name = %s AND parent_id IS NOT DISTINCT FROM %s
                ORDER BY category_id ASC
                LIMIT 1;
                """,
                (name, parent_id),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return Category(category_id=row[0], name=row[1], parent_id=row[2])
