# -*- coding: utf-8 -*-
"""
Simple script to execute SQL queries against relational.db
"""

import sqlite3
import pandas as pd
import os

def execute_sql_query(query, description=""):
    """Execute an SQL query against the local relational.db and print results.

    Returns None. Prints a header if `description` is provided.
    """
    try:
        conn = sqlite3.connect(".." + os.sep + "relational.db")

        if description:
            print(f"\n=== {description} ===")

        df = pd.read_sql_query(query, conn)

        if not df.empty:
            print(f"Records found: {len(df)}")
            print(df.to_string(index=False))
        else:
            print("Empty result")

        conn.close()

    except Exception as e:
        print(f"Error executing query: {e}")

def main():
    """Main function with example SQL queries."""

    print("=== SQL query examples against relational.db ===\n")

    # 1. All areas
    execute_sql_query(
        "SELECT id FROM areas ORDER BY id",
        "All areas"
    )

    # 2. Categories with quartile Q1
    execute_sql_query(
        "SELECT id, quartile FROM categories WHERE quartile = 'Q1' ORDER BY id LIMIT 10",
        "Categories with quartile Q1 (first 10)"
    )

    # 3. Quartile statistics
    execute_sql_query(
        "SELECT quartile, COUNT(*) as count FROM categories GROUP BY quartile ORDER BY quartile",
        "Quartile statistics"
    )

    # 4. Top-10 categories by number of journals
    execute_sql_query(
        """
        SELECT c.id, c.quartile, COUNT(jc.issn) as journal_count
        FROM categories c
        LEFT JOIN journal_categories jc ON c.id = jc.category_id
        GROUP BY c.id, c.quartile
        ORDER BY journal_count DESC
        LIMIT 10
        """,
        "Top-10 categories by number of journals"
    )

    # 5. Journals in a specific category
    execute_sql_query(
        """
        SELECT DISTINCT jc.issn, c.id as category, c.quartile
        FROM journal_categories jc
        JOIN categories c ON jc.category_id = c.id
        WHERE c.id = 'Computer Science'
        LIMIT 10
        """,
        "Journals in category 'Computer Science' (first 10)"
    )

    # 6. Journals in a specific area
    execute_sql_query(
        """
        SELECT DISTINCT ja.issn, a.id as area
        FROM journal_areas ja
        JOIN areas a ON ja.area_id = a.id
        WHERE a.id = 'Computer Science'
        LIMIT 10
        """,
        "Journals in area 'Computer Science' (first 10)"
    )

    # 7. Journal-category-area relations
    execute_sql_query(
        """
        SELECT jc.issn, c.id as category, c.quartile, a.id as area
        FROM journal_categories jc
        JOIN categories c ON jc.category_id = c.id
        LEFT JOIN journal_areas ja ON jc.issn = ja.issn
        LEFT JOIN areas a ON ja.area_id = a.id
        WHERE c.id = 'Artificial Intelligence'
        LIMIT 5
        """,
        "Journals in category 'Artificial Intelligence' with areas"
    )

if __name__ == "__main__":
    main()
