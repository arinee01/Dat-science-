# -*- coding: utf-8 -*-
"""
Utility script for inspecting data in the relational.db SQLite database.
"""

import sqlite3
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from implementations.query_handlers import CategoryQueryHandler

def view_sqlite_data():
    """Inspect the contents of the SQLite database."""
    
    db_path = ".." + os.sep + "relational.db"
    
    print("=== Inspecting data in the SQLite database ===\n")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Show the database structure
        print("1. Database structure:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"   Table: {table_name}")
            
            # Show table columns
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            for col in columns:
                print(f"     - {col[1]} ({col[2]})")
            print()
        
        # 2. Table statistics
        print("2. Table statistics:")
        
        # Areas
        cursor.execute("SELECT COUNT(*) FROM areas")
        areas_count = cursor.fetchone()[0]
        print(f"   Areas: {areas_count}")
        
        # Categories
        cursor.execute("SELECT COUNT(*) FROM categories")
        categories_count = cursor.fetchone()[0]
        print(f"   Categories: {categories_count}")
        
        # Journal-category links
        cursor.execute("SELECT COUNT(*) FROM journal_categories")
        journal_categories_count = cursor.fetchone()[0]
        print(f"   Journal-category links: {journal_categories_count}")
        
        # Journal-area links
        cursor.execute("SELECT COUNT(*) FROM journal_areas")
        journal_areas_count = cursor.fetchone()[0]
        print(f"   Journal-area links: {journal_areas_count}")
        
        # 3. Sample areas
        print("\n3. Sample areas:")
        cursor.execute("SELECT id FROM areas ORDER BY id LIMIT 10")
        areas = cursor.fetchall()
        for i, area in enumerate(areas, 1):
            print(f"   {i}. {area[0]}")
        
        # 4. Sample categories
        print("\n4. Sample categories:")
        cursor.execute("SELECT id, quartile FROM categories ORDER BY id LIMIT 10")
        categories = cursor.fetchall()
        for i, category in enumerate(categories, 1):
            quartile = category[1] if category[1] else "N/A"
            print(f"   {i}. {category[0]} (quartile: {quartile})")
        
        # 5. Quartile statistics
        print("\n5. Quartile statistics:")
        cursor.execute("""
            SELECT quartile, COUNT(*) as count 
            FROM categories 
            WHERE quartile IS NOT NULL 
            GROUP BY quartile 
            ORDER BY quartile
        """)
        quartiles = cursor.fetchall()
        for quartile, count in quartiles:
            print(f"   {quartile}: {count} categories")
        
        # 6. Top 10 categories by number of journals
        print("\n6. Top 10 categories by number of journals:")
        cursor.execute("""
            SELECT c.id, c.quartile, COUNT(jc.issn) as journal_count
            FROM categories c
            LEFT JOIN journal_categories jc ON c.id = jc.category_id
            GROUP BY c.id, c.quartile
            ORDER BY journal_count DESC
            LIMIT 10
        """)
        top_categories = cursor.fetchall()
        for i, (category_id, quartile, count) in enumerate(top_categories, 1):
            quartile_str = quartile if quartile else "N/A"
            print(f"   {i}. {category_id} ({quartile_str}) - {count} journals")
        
        # 7. Top 10 areas by number of journals
        print("\n7. Top 10 areas by number of journals:")
        cursor.execute("""
            SELECT a.id, COUNT(ja.issn) as journal_count
            FROM areas a
            LEFT JOIN journal_areas ja ON a.id = ja.area_id
            GROUP BY a.id
            ORDER BY journal_count DESC
            LIMIT 10
        """)
        top_areas = cursor.fetchall()
        for i, (area_id, count) in enumerate(top_areas, 1):
            print(f"   {i}. {area_id} - {count} journals")
        
        conn.close()
        
    except Exception as e:
        print(f"Error while working with the database: {e}")

def test_category_query_handler():
    """Exercise the CategoryQueryHandler."""
    print("\n=== Testing CategoryQueryHandler ===\n")
    
    handler = CategoryQueryHandler()
    handler.setDbPathOrUrl(".." + os.sep + "relational.db")
    
    # Fetch all categories
    print("Retrieving all categories via the handler:")
    df = handler.getAllCategories()
    print(f"   Categories retrieved: {len(df)}")
    
    if not df.empty:
        print("   First five categories:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            quartile = row.get('quartile', 'N/A')
            print(f"   {i+1}. {row.get('id', 'N/A')} (quartile: {quartile})")
    
    # Fetch all areas
    print("\nRetrieving all areas via the handler:")
    df = handler.getAllAreas()
    print(f"   Areas retrieved: {len(df)}")
    
    if not df.empty:
        print("   First five areas:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            print(f"   {i+1}. {row.get('id', 'N/A')}")
    
    # Search by quartile
    print("\nLooking up categories with quartile Q1:")
    df = handler.getCategoriesWithQuartile({"Q1"})
    print(f"   Q1 categories found: {len(df)}")
    
    if not df.empty:
        print("   First five Q1 categories:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            print(f"   {i+1}. {row.get('id', 'N/A')}")

def view_journal_categories():
    """Show relationships between journals, categories, and areas."""
    print("\n=== Journal-category relationships ===\n")
    
    try:
        conn = sqlite3.connect(".." + os.sep + "relational.db")
        
        # Show sample relationships
        query = """
        SELECT jc.issn, c.id as category, c.quartile, a.id as area
        FROM journal_categories jc
        JOIN categories c ON jc.category_id = c.id
        LEFT JOIN journal_areas ja ON jc.issn = ja.issn
        LEFT JOIN areas a ON ja.area_id = a.id
        ORDER BY jc.issn
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            print("Sample journal-category-area links:")
            for i, (_, row) in enumerate(df.iterrows(), 1):
                issn = row['issn']
                category = row['category']
                quartile = row['quartile'] if row['quartile'] else 'N/A'
                area = row['area'] if row['area'] else 'N/A'
                print(f"   {i}. ISSN {issn} -> {category} ({quartile}) in area {area}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error while fetching links: {e}")

if __name__ == "__main__":
    view_sqlite_data()
    test_category_query_handler()
    view_journal_categories()
