# -*- coding: utf-8 -*-
"""
Обработчики запросов к базам данных.
Содержит классы: JournalQueryHandler, CategoryQueryHandler
"""

import requests
import sqlite3
import pandas as pd
from typing import List, Set, Optional
from .handlers import QueryHandler
from .models import Journal, Category, Area


class JournalQueryHandler(QueryHandler):
    """
    Обработчик запросов к журналам в графовой базе данных Blazegraph.
    """
    
    def getById(self, entity_id: str) -> pd.DataFrame:
        """
        Возвращает журнал по идентификатору (ISSN).
        
        Args:
            entity_id (str): ISSN журнала
            
        Returns:
            pd.DataFrame: Данные журнала или пустой DataFrame
        """
        try:
            sparql_query = f"""
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {{
                ?journal rdf:type doaj:Journal .
                ?journal doaj:issn "{entity_id}" .
                ?journal doaj:title ?title .
                OPTIONAL {{ ?journal doaj:issn ?issn }}
                OPTIONAL {{ ?journal doaj:eissn ?eissn }}
                OPTIONAL {{ ?journal doaj:language ?language }}
                OPTIONAL {{ ?journal doaj:publisher ?publisher }}
                OPTIONAL {{ ?journal doaj:hasDOAJSeal ?seal }}
                OPTIONAL {{ ?journal doaj:licence ?licence }}
                OPTIONAL {{ ?journal doaj:hasAPC ?apc }}
            }}
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при запросе журнала по ID: {e}")
            return pd.DataFrame()
    
    def getAllJournals(self) -> pd.DataFrame:
        """
        Возвращает все журналы из базы данных.
        
        Returns:
            pd.DataFrame: DataFrame со всеми журналами
        """
        try:
            sparql_query = """
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                OPTIONAL { ?journal doaj:issn ?issn }
                OPTIONAL { ?journal doaj:eissn ?eissn }
                OPTIONAL { ?journal doaj:language ?language }
                OPTIONAL { ?journal doaj:publisher ?publisher }
                OPTIONAL { ?journal doaj:hasDOAJSeal ?seal }
                OPTIONAL { ?journal doaj:licence ?licence }
                OPTIONAL { ?journal doaj:hasAPC ?apc }
            }
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при получении всех журналов: {e}")
            return pd.DataFrame()
    
    def getJournalsWithTitle(self, partialTitle: str) -> pd.DataFrame:
        """
        Возвращает журналы с частичным совпадением в названии.
        
        Args:
            partialTitle (str): Часть названия для поиска
            
        Returns:
            pd.DataFrame: DataFrame с найденными журналами
        """
        try:
            sparql_query = f"""
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {{
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                FILTER (CONTAINS(LCASE(?title), LCASE("{partialTitle}")))
                OPTIONAL {{ ?journal doaj:issn ?issn }}
                OPTIONAL {{ ?journal doaj:eissn ?eissn }}
                OPTIONAL {{ ?journal doaj:language ?language }}
                OPTIONAL {{ ?journal doaj:publisher ?publisher }}
                OPTIONAL {{ ?journal doaj:hasDOAJSeal ?seal }}
                OPTIONAL {{ ?journal doaj:licence ?licence }}
                OPTIONAL {{ ?journal doaj:hasAPC ?apc }}
            }}
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по названию: {e}")
            return pd.DataFrame()
    
    def getJournalsPublishedBy(self, partialName: str) -> pd.DataFrame:
        """
        Возвращает журналы с частичным совпадением в названии издателя.
        
        Args:
            partialName (str): Часть названия издателя для поиска
            
        Returns:
            pd.DataFrame: DataFrame с найденными журналами
        """
        try:
            sparql_query = f"""
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {{
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                ?journal doaj:publisher ?publisher .
                FILTER (CONTAINS(LCASE(?publisher), LCASE("{partialName}")))
                OPTIONAL {{ ?journal doaj:issn ?issn }}
                OPTIONAL {{ ?journal doaj:eissn ?eissn }}
                OPTIONAL {{ ?journal doaj:language ?language }}
                OPTIONAL {{ ?journal doaj:hasDOAJSeal ?seal }}
                OPTIONAL {{ ?journal doaj:licence ?licence }}
                OPTIONAL {{ ?journal doaj:hasAPC ?apc }}
            }}
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по издателю: {e}")
            return pd.DataFrame()
    
    def getJournalsWithLicense(self, licenses: Set[str]) -> pd.DataFrame:
        """
        Возвращает журналы с указанными лицензиями.
        
        Args:
            licenses (Set[str]): Множество лицензий для поиска
            
        Returns:
            pd.DataFrame: DataFrame с найденными журналами
        """
        try:
            # Формируем фильтр для лицензий
            license_filter = " || ".join([f'?licence = "{license}"' for license in licenses])
            
            sparql_query = f"""
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {{
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                ?journal doaj:licence ?licence .
                FILTER ({license_filter})
                OPTIONAL {{ ?journal doaj:issn ?issn }}
                OPTIONAL {{ ?journal doaj:eissn ?eissn }}
                OPTIONAL {{ ?journal doaj:language ?language }}
                OPTIONAL {{ ?journal doaj:publisher ?publisher }}
                OPTIONAL {{ ?journal doaj:hasDOAJSeal ?seal }}
                OPTIONAL {{ ?journal doaj:hasAPC ?apc }}
            }}
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по лицензии: {e}")
            return pd.DataFrame()
    
    def getJournalsWithAPC(self) -> pd.DataFrame:
        """
        Возвращает журналы с Article Processing Charge.
        
        Returns:
            pd.DataFrame: DataFrame с журналами, имеющими APC
        """
        try:
            sparql_query = """
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                ?journal doaj:hasAPC "true"^^xsd:boolean .
                OPTIONAL { ?journal doaj:issn ?issn }
                OPTIONAL { ?journal doaj:eissn ?eissn }
                OPTIONAL { ?journal doaj:language ?language }
                OPTIONAL { ?journal doaj:publisher ?publisher }
                OPTIONAL { ?journal doaj:hasDOAJSeal ?seal }
                OPTIONAL { ?journal doaj:licence ?licence }
                OPTIONAL { ?journal doaj:hasAPC ?apc }
            }
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при поиске журналов с APC: {e}")
            return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self) -> pd.DataFrame:
        """
        Возвращает журналы с DOAJ Seal.
        
        Returns:
            pd.DataFrame: DataFrame с журналами, имеющими DOAJ Seal
        """
        try:
            sparql_query = """
            PREFIX doaj: <http://doaj.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            
            SELECT ?journal ?title ?issn ?eissn ?language ?publisher ?seal ?licence ?apc
            WHERE {
                ?journal rdf:type doaj:Journal .
                ?journal doaj:title ?title .
                ?journal doaj:hasDOAJSeal "true"^^xsd:boolean .
                OPTIONAL { ?journal doaj:issn ?issn }
                OPTIONAL { ?journal doaj:eissn ?eissn }
                OPTIONAL { ?journal doaj:language ?language }
                OPTIONAL { ?journal doaj:publisher ?publisher }
                OPTIONAL { ?journal doaj:hasDOAJSeal ?seal }
                OPTIONAL { ?journal doaj:licence ?licence }
                OPTIONAL { ?journal doaj:hasAPC ?apc }
            }
            ORDER BY ?title
            """
            
            return self._execute_sparql_query(sparql_query)
            
        except Exception as e:
            print(f"Ошибка при поиске журналов с DOAJ Seal: {e}")
            return pd.DataFrame()
    
    def _execute_sparql_query(self, sparql_query: str) -> pd.DataFrame:
        """
        Выполняет SPARQL запрос и возвращает результат как DataFrame.
        
        Args:
            sparql_query (str): SPARQL запрос
            
        Returns:
            pd.DataFrame: Результат запроса
        """
        try:
            response = requests.get(
                self._dbPathOrUrl,
                params={'query': sparql_query, 'format': 'json'}
            )
            
            if response.status_code == 200:
                data = response.json()
                bindings = data.get('results', {}).get('bindings', [])
                
                if not bindings:
                    return pd.DataFrame()
                
                # Преобразуем результат в DataFrame
                rows = []
                for binding in bindings:
                    row = {}
                    for var_name, var_value in binding.items():
                        row[var_name] = var_value.get('value', '')
                    rows.append(row)
                
                return pd.DataFrame(rows)
            else:
                print(f"Ошибка SPARQL запроса: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Ошибка при выполнении SPARQL запроса: {e}")
            return pd.DataFrame()


class CategoryQueryHandler(QueryHandler):
    """
    Обработчик запросов к категориям и областям в реляционной базе данных SQLite.
    """
    
    def getById(self, entity_id: str) -> pd.DataFrame:
        """
        Возвращает сущность по идентификатору.
        
        Args:
            entity_id (str): Идентификатор сущности
            
        Returns:
            pd.DataFrame: Данные сущности или пустой DataFrame
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            # Проверяем, является ли это категорией
            category_query = "SELECT id, quartile FROM categories WHERE id = ?"
            category_df = pd.read_sql_query(category_query, conn, params=(entity_id,))
            
            if not category_df.empty:
                conn.close()
                return category_df
            
            # Проверяем, является ли это областью
            area_query = "SELECT id FROM areas WHERE id = ?"
            area_df = pd.read_sql_query(area_query, conn, params=(entity_id,))
            
            conn.close()
            return area_df
            
        except Exception as e:
            print(f"Ошибка при запросе сущности по ID: {e}")
            return pd.DataFrame()
    
    def getAllCategories(self) -> pd.DataFrame:
        """
        Возвращает все категории из базы данных.
        
        Returns:
            pd.DataFrame: DataFrame со всеми категориями
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            query = "SELECT DISTINCT id, quartile FROM categories ORDER BY id"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            print(f"Ошибка при получении всех категорий: {e}")
            return pd.DataFrame()
    
    def getAllAreas(self) -> pd.DataFrame:
        """
        Возвращает все области из базы данных.
        
        Returns:
            pd.DataFrame: DataFrame со всеми областями
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            query = "SELECT DISTINCT id FROM areas ORDER BY id"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            print(f"Ошибка при получении всех областей: {e}")
            return pd.DataFrame()
    
    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> pd.DataFrame:
        """
        Возвращает категории с указанными квартилями.
        
        Args:
            quartiles (Set[str]): Множество квартилей для поиска
            
        Returns:
            pd.DataFrame: DataFrame с найденными категориями
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not quartiles:
                # Если квартили не указаны, возвращаем все категории
                query = "SELECT DISTINCT id, quartile FROM categories ORDER BY id"
                df = pd.read_sql_query(query, conn)
            else:
                # Формируем запрос с фильтром по квартилям
                placeholders = ','.join(['?' for _ in quartiles])
                query = f"SELECT DISTINCT id, quartile FROM categories WHERE quartile IN ({placeholders}) ORDER BY id"
                df = pd.read_sql_query(query, conn, params=list(quartiles))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"Ошибка при поиске категорий по квартилю: {e}")
            return pd.DataFrame()
    
    def getCategoriesAssignedToAreas(self, area_ids: Set[str]) -> pd.DataFrame:
        """
        Возвращает категории, назначенные указанным областям.
        
        Args:
            area_ids (Set[str]): Множество идентификаторов областей
            
        Returns:
            pd.DataFrame: DataFrame с найденными категориями
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not area_ids:
                # Если области не указаны, возвращаем все категории
                query = """
                SELECT DISTINCT c.id, c.quartile 
                FROM categories c 
                ORDER BY c.id
                """
                df = pd.read_sql_query(query, conn)
            else:
                # Формируем запрос с фильтром по областям
                placeholders = ','.join(['?' for _ in area_ids])
                query = f"""
                SELECT DISTINCT c.id, c.quartile 
                FROM categories c
                JOIN journal_categories jc ON c.id = jc.category_id
                JOIN journal_areas ja ON jc.issn = ja.issn
                WHERE ja.area_id IN ({placeholders})
                ORDER BY c.id
                """
                df = pd.read_sql_query(query, conn, params=list(area_ids))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"Ошибка при поиске категорий по областям: {e}")
            return pd.DataFrame()
    
    def getAreasAssignedToCategories(self, category_ids: Set[str]) -> pd.DataFrame:
        """
        Возвращает области, назначенные указанным категориям.
        
        Args:
            category_ids (Set[str]): Множество идентификаторов категорий
            
        Returns:
            pd.DataFrame: DataFrame с найденными областями
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not category_ids:
                # Если категории не указаны, возвращаем все области
                query = "SELECT DISTINCT id FROM areas ORDER BY id"
                df = pd.read_sql_query(query, conn)
            else:
                # Формируем запрос с фильтром по категориям
                placeholders = ','.join(['?' for _ in category_ids])
                query = f"""
                SELECT DISTINCT a.id 
                FROM areas a
                JOIN journal_areas ja ON a.id = ja.area_id
                JOIN journal_categories jc ON ja.issn = jc.issn
                WHERE jc.category_id IN ({placeholders})
                ORDER BY a.id
                """
                df = pd.read_sql_query(query, conn, params=list(category_ids))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"Ошибка при поиске областей по категориям: {e}")
            return pd.DataFrame()
