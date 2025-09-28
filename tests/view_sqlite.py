# -*- coding: utf-8 -*-
"""
Скрипт для просмотра данных из SQLite базы relational.db
"""

import sqlite3
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from implementations.query_handlers import CategoryQueryHandler

def view_sqlite_data():
    """Просматривает данные в SQLite базе"""
    
    db_path = ".." + os.sep + "relational.db"
    
    print("=== Просмотр данных в SQLite базе ===\n")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Показываем структуру таблиц
        print("1. Структура базы данных:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            print(f"   Таблица: {table_name}")
            
            # Показываем структуру таблицы
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            for col in columns:
                print(f"     - {col[1]} ({col[2]})")
            print()
        
        # 2. Статистика по таблицам
        print("2. Статистика по таблицам:")
        
        # Области
        cursor.execute("SELECT COUNT(*) FROM areas")
        areas_count = cursor.fetchone()[0]
        print(f"   Областей: {areas_count}")
        
        # Категории
        cursor.execute("SELECT COUNT(*) FROM categories")
        categories_count = cursor.fetchone()[0]
        print(f"   Категорий: {categories_count}")
        
        # Связи журнал-категория
        cursor.execute("SELECT COUNT(*) FROM journal_categories")
        journal_categories_count = cursor.fetchone()[0]
        print(f"   Связей журнал-категория: {journal_categories_count}")
        
        # Связи журнал-область
        cursor.execute("SELECT COUNT(*) FROM journal_areas")
        journal_areas_count = cursor.fetchone()[0]
        print(f"   Связей журнал-область: {journal_areas_count}")
        
        # 3. Примеры областей
        print("\n3. Примеры областей:")
        cursor.execute("SELECT id FROM areas ORDER BY id LIMIT 10")
        areas = cursor.fetchall()
        for i, area in enumerate(areas, 1):
            print(f"   {i}. {area[0]}")
        
        # 4. Примеры категорий
        print("\n4. Примеры категорий:")
        cursor.execute("SELECT id, quartile FROM categories ORDER BY id LIMIT 10")
        categories = cursor.fetchall()
        for i, category in enumerate(categories, 1):
            quartile = category[1] if category[1] else "N/A"
            print(f"   {i}. {category[0]} (квартиль: {quartile})")
        
        # 5. Статистика по квартилям
        print("\n5. Статистика по квартилям:")
        cursor.execute("""
            SELECT quartile, COUNT(*) as count 
            FROM categories 
            WHERE quartile IS NOT NULL 
            GROUP BY quartile 
            ORDER BY quartile
        """)
        quartiles = cursor.fetchall()
        for quartile, count in quartiles:
            print(f"   {quartile}: {count} категорий")
        
        # 6. Топ-10 категорий по количеству журналов
        print("\n6. Топ-10 категорий по количеству журналов:")
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
            print(f"   {i}. {category_id} ({quartile_str}) - {count} журналов")
        
        # 7. Топ-10 областей по количеству журналов
        print("\n7. Топ-10 областей по количеству журналов:")
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
            print(f"   {i}. {area_id} - {count} журналов")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при работе с базой данных: {e}")

def test_category_query_handler():
    """Тестирует наш CategoryQueryHandler"""
    print("\n=== Тестирование CategoryQueryHandler ===\n")
    
    handler = CategoryQueryHandler()
    handler.setDbPathOrUrl(".." + os.sep + "relational.db")
    
    # Тестируем получение всех категорий
    print("Получение всех категорий через наш обработчик:")
    df = handler.getAllCategories()
    print(f"   Получено категорий: {len(df)}")
    
    if not df.empty:
        print("   Первые 5 категорий:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            quartile = row.get('quartile', 'N/A')
            print(f"   {i+1}. {row.get('id', 'N/A')} (квартиль: {quartile})")
    
    # Тестируем получение всех областей
    print("\nПолучение всех областей через наш обработчик:")
    df = handler.getAllAreas()
    print(f"   Получено областей: {len(df)}")
    
    if not df.empty:
        print("   Первые 5 областей:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            print(f"   {i+1}. {row.get('id', 'N/A')}")
    
    # Тестируем поиск по квартилям
    print("\nПоиск категорий с квартилем Q1:")
    df = handler.getCategoriesWithQuartile({"Q1"})
    print(f"   Найдено категорий Q1: {len(df)}")
    
    if not df.empty:
        print("   Первые 5 категорий Q1:")
        for i, (_, row) in enumerate(df.head(5).iterrows()):
            print(f"   {i+1}. {row.get('id', 'N/A')}")

def view_journal_categories():
    """Показывает связи между журналами и категориями"""
    print("\n=== Связи журнал-категория ===\n")
    
    try:
        conn = sqlite3.connect(".." + os.sep + "relational.db")
        
        # Показываем примеры связей
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
            print("Примеры связей журнал-категория-область:")
            for i, (_, row) in enumerate(df.iterrows(), 1):
                issn = row['issn']
                category = row['category']
                quartile = row['quartile'] if row['quartile'] else 'N/A'
                area = row['area'] if row['area'] else 'N/A'
                print(f"   {i}. ISSN {issn} -> {category} ({quartile}) в области {area}")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при просмотре связей: {e}")

if __name__ == "__main__":
    view_sqlite_data()
    test_category_query_handler()
    view_journal_categories()
