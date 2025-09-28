# -*- coding: utf-8 -*-
"""
Скрипт для просмотра данных в Blazegraph
"""

import requests
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from implementations.query_handlers import JournalQueryHandler

def view_blazegraph_data():
    """Просматривает данные в Blazegraph"""
    
    endpoint = "http://localhost:8889/bigdata/sparql"
    
    print("=== Просмотр данных в Blazegraph ===\n")
    
    # 1. Проверяем общее количество журналов
    count_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
    }
    """
    
    print("1. Общее количество журналов:")
    result = execute_sparql_query(endpoint, count_query)
    if result:
        print(f"   Найдено журналов: {result[0]['count']}")
    
    # 2. Показываем первые 5 журналов
    sample_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT ?journal ?title ?issn ?publisher ?licence
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:title ?title .
        OPTIONAL { ?journal doaj:issn ?issn }
        OPTIONAL { ?journal doaj:publisher ?publisher }
        OPTIONAL { ?journal doaj:licence ?licence }
    }
    LIMIT 5
    """
    
    print("\n2. Примеры загруженных журналов:")
    result = execute_sparql_query(endpoint, sample_query)
    if result:
        for i, journal in enumerate(result, 1):
            print(f"   {i}. {journal.get('title', 'N/A')}")
            print(f"      ISSN: {journal.get('issn', 'N/A')}")
            print(f"      Издатель: {journal.get('publisher', 'N/A')}")
            print(f"      Лицензия: {journal.get('licence', 'N/A')}")
            print()
    
    # 3. Статистика по лицензиям
    license_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT ?licence (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:licence ?licence .
    }
    GROUP BY ?licence
    ORDER BY DESC(?count)
    """
    
    print("3. Статистика по лицензиям:")
    result = execute_sparql_query(endpoint, license_query)
    if result:
        for license_info in result:
            print(f"   {license_info['licence']}: {license_info['count']} журналов")
    
    # 4. Журналы с APC
    apc_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:hasAPC "true" .
    }
    """
    
    print("\n4. Журналы с Article Processing Charge (APC):")
    result = execute_sparql_query(endpoint, apc_query)
    if result:
        print(f"   Журналов с APC: {result[0]['count']}")
    
    # 5. Журналы с DOAJ Seal
    seal_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:hasDOAJSeal "true" .
    }
    """
    
    print("\n5. Журналы с DOAJ Seal:")
    result = execute_sparql_query(endpoint, seal_query)
    if result:
        print(f"   Журналов с DOAJ Seal: {result[0]['count']}")

def execute_sparql_query(endpoint, query):
    """Выполняет SPARQL запрос и возвращает результат"""
    try:
        response = requests.get(
            endpoint,
            params={'query': query, 'format': 'json'}
        )
        
        if response.status_code == 200:
            data = response.json()
            bindings = data.get('results', {}).get('bindings', [])
            
            # Преобразуем результат в список словарей
            result = []
            for binding in bindings:
                row = {}
                for var_name, var_value in binding.items():
                    row[var_name] = var_value.get('value', '')
                result.append(row)
            
            return result
        else:
            print(f"Ошибка SPARQL запроса: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Ошибка при выполнении SPARQL запроса: {e}")
        return None

def test_query_handler():
    """Тестирует наш QueryHandler"""
    print("\n=== Тестирование JournalQueryHandler ===\n")
    
    handler = JournalQueryHandler()
    handler.setDbPathOrUrl("http://localhost:8889/bigdata/sparql")
    
    # Тестируем получение всех журналов
    print("Получение всех журналов через наш обработчик:")
    df = handler.getAllJournals()
    print(f"   Получено журналов: {len(df)}")
    
    if not df.empty:
        print("   Первые 3 журнала:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"   {i+1}. {row.get('title', 'N/A')}")
    
    # Тестируем поиск по названию
    print("\nПоиск журналов с 'Journal' в названии:")
    df = handler.getJournalsWithTitle("Journal")
    print(f"   Найдено журналов: {len(df)}")
    
    if not df.empty:
        print("   Первые 3 результата:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"   {i+1}. {row.get('title', 'N/A')}")

if __name__ == "__main__":
    view_blazegraph_data()
    test_query_handler()
