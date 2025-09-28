# -*- coding: utf-8 -*-
"""
Простой скрипт для выполнения SQL запросов к relational.db
"""

import sqlite3
import pandas as pd

def execute_sql_query(query, description=""):
    """Выполняет SQL запрос и показывает результат"""
    try:
        conn = sqlite3.connect(".." + os.sep + "relational.db")
        
        if description:
            print(f"\n=== {description} ===")
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            print(f"Найдено записей: {len(df)}")
            print(df.to_string(index=False))
        else:
            print("Результат пустой")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")

def main():
    """Основная функция с примерами запросов"""
    
    print("=== Примеры SQL запросов к relational.db ===\n")
    
    # 1. Все области
    execute_sql_query(
        "SELECT id FROM areas ORDER BY id",
        "Все области"
    )
    
    # 2. Категории с квартилем Q1
    execute_sql_query(
        "SELECT id, quartile FROM categories WHERE quartile = 'Q1' ORDER BY id LIMIT 10",
        "Категории с квартилем Q1 (первые 10)"
    )
    
    # 3. Статистика по квартилям
    execute_sql_query(
        "SELECT quartile, COUNT(*) as count FROM categories GROUP BY quartile ORDER BY quartile",
        "Статистика по квартилям"
    )
    
    # 4. Топ-10 категорий по количеству журналов
    execute_sql_query(
        """
        SELECT c.id, c.quartile, COUNT(jc.issn) as journal_count
        FROM categories c
        LEFT JOIN journal_categories jc ON c.id = jc.category_id
        GROUP BY c.id, c.quartile
        ORDER BY journal_count DESC
        LIMIT 10
        """,
        "Топ-10 категорий по количеству журналов"
    )
    
    # 5. Журналы в определенной категории
    execute_sql_query(
        """
        SELECT DISTINCT jc.issn, c.id as category, c.quartile
        FROM journal_categories jc
        JOIN categories c ON jc.category_id = c.id
        WHERE c.id = 'Computer Science'
        LIMIT 10
        """,
        "Журналы в категории 'Computer Science' (первые 10)"
    )
    
    # 6. Журналы в определенной области
    execute_sql_query(
        """
        SELECT DISTINCT ja.issn, a.id as area
        FROM journal_areas ja
        JOIN areas a ON ja.area_id = a.id
        WHERE a.id = 'Computer Science'
        LIMIT 10
        """,
        "Журналы в области 'Computer Science' (первые 10)"
    )
    
    # 7. Связи журнал-категория-область
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
        "Журналы в категории 'Artificial Intelligence' с областями"
    )

if __name__ == "__main__":
    main()
