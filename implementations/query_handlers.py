# -*- coding: utf-8 -*-
"""
Query handlers for databases.
Contains classes: JournalQueryHandler, CategoryQueryHandler
"""

import requests
import sqlite3
import pandas as pd
from typing import List, Set, Optional
from handlers import QueryHandler
from models import Journal, Category, Area


class JournalQueryHandler(QueryHandler):
    """
    Handler for journal queries against a Blazegraph graph database.
    """
    
    def getById(self, entity_id: str) -> pd.DataFrame:
        """
        Return a journal by identifier (ISSN).

            else:
                # Build query with area filter

        Returns:
            pd.DataFrame: Journal data or an empty DataFrame
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
            print(f"Error while querying journal by ID: {e}")
            return pd.DataFrame()
    
    def getAllJournals(self) -> pd.DataFrame:
        """
        Return all journals from the database.

        Returns:
            pd.DataFrame: DataFrame with all journals
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
            print(f"Error while fetching all journals: {e}")
            return pd.DataFrame()
    
    def getJournalsWithTitle(self, partialTitle: str) -> pd.DataFrame:
        """
        Return journals with partial title match.

        Args:
            partialTitle (str): Partial title to search for

        Returns:
            pd.DataFrame: DataFrame with found journals
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
            print(f"Error while searching journals by title: {e}")
            return pd.DataFrame()
    
    def getJournalsPublishedBy(self, partialName: str) -> pd.DataFrame:
        """
        Return journals with partial publisher name match.

        Args:
            partialName (str): Partial publisher name to search for

        Returns:
            pd.DataFrame: DataFrame with found journals
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
            print(f"Error while searching journals by publisher: {e}")
            return pd.DataFrame()
    
    def getJournalsWithLicense(self, licenses: Set[str]) -> pd.DataFrame:
        """
        Return journals with specified licenses.

        Args:
            licenses (Set[str]): Set of licenses to search for

        Returns:
            pd.DataFrame: DataFrame with found journals
        """
        try:
            # Build the license filter
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
            print(f"Error while searching journals by license: {e}")
            return pd.DataFrame()
    
    def getJournalsWithAPC(self) -> pd.DataFrame:
        """
        Return journals that have Article Processing Charge (APC).

        Returns:
            pd.DataFrame: DataFrame with journals that have APC
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
            print(f"Error while searching journals with APC: {e}")
            return pd.DataFrame()
    
    def getJournalsWithDOAJSeal(self) -> pd.DataFrame:
        """
        Return journals that have DOAJ Seal.

        Returns:
            pd.DataFrame: DataFrame with journals that have DOAJ Seal
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
            print(f"Error while searching journals with DOAJ Seal: {e}")
            return pd.DataFrame()
    
    def _execute_sparql_query(self, sparql_query: str) -> pd.DataFrame:
        """
        Execute a SPARQL query and return the result as a DataFrame.

        Args:
            sparql_query (str): SPARQL query

        Returns:
            pd.DataFrame: Query result
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
                
                # Convert the result to a DataFrame
                rows = []
                for binding in bindings:
                    row = {}
                    for var_name, var_value in binding.items():
                        row[var_name] = var_value.get('value', '')
                    rows.append(row)
                
                return pd.DataFrame(rows)
            else:
                print(f"SPARQL query error: {response.status_code}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error while executing SPARQL query: {e}")
            return pd.DataFrame()


class CategoryQueryHandler(QueryHandler):
    """
    Handler for categories and areas queries in a relational SQLite database.
    """
    
    def getById(self, entity_id: str) -> pd.DataFrame:
        """
        Return an entity by identifier.

        Args:
            entity_id (str): Entity identifier

        Returns:
            pd.DataFrame: Entity data or an empty DataFrame
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            # Check if this is a category
            category_query = "SELECT id, quartile FROM categories WHERE id = ?"
            category_df = pd.read_sql_query(category_query, conn, params=(entity_id,))
            
            if not category_df.empty:
                conn.close()
                return category_df
            
            # Check if this is an area
            area_query = "SELECT id FROM areas WHERE id = ?"
            area_df = pd.read_sql_query(area_query, conn, params=(entity_id,))
            
            conn.close()
            return area_df
            
        except Exception as e:
            print(f"Error while querying entity by ID: {e}")
            return pd.DataFrame()
    
    def getAllCategories(self) -> pd.DataFrame:
        """
        Return all categories from the database.

        Returns:
            pd.DataFrame: DataFrame with all categories
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            query = "SELECT DISTINCT id, quartile FROM categories ORDER BY id"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            print(f"Error while fetching all categories: {e}")
            return pd.DataFrame()
    
    def getAllAreas(self) -> pd.DataFrame:
        """
        Return all areas from the database.

        Returns:
            pd.DataFrame: DataFrame with all areas
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            query = "SELECT DISTINCT id FROM areas ORDER BY id"
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            print(f"Error while fetching all areas: {e}")
            return pd.DataFrame()
    
    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> pd.DataFrame:
        """
        Return categories with specified quartiles.

        Args:
            quartiles (Set[str]): Set of quartiles to search for

        Returns:
            pd.DataFrame: DataFrame with found categories
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not quartiles:
                # If quartiles are not specified, return all categories
                query = "SELECT DISTINCT id, quartile FROM categories ORDER BY id"
                df = pd.read_sql_query(query, conn)
            else:
                # Build query with quartile filter
                placeholders = ','.join(['?' for _ in quartiles])
                query = f"SELECT DISTINCT id, quartile FROM categories WHERE quartile IN ({placeholders}) ORDER BY id"
                df = pd.read_sql_query(query, conn, params=list(quartiles))
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"Error while searching categories by quartile: {e}")
            return pd.DataFrame()
    
    def getCategoriesAssignedToAreas(self, area_ids: Set[str]) -> pd.DataFrame:
        """
        Return categories assigned to specified areas.

        Args:
            area_ids (Set[str]): Set of area identifiers

        Returns:
            pd.DataFrame: DataFrame with found categories
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not area_ids:
                # If areas are not specified, return all categories
                query = """
                SELECT DISTINCT c.id, c.quartile 
                FROM categories c 
                ORDER BY c.id
                """
                df = pd.read_sql_query(query, conn)
            else:
                # Build query with area filter
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
            print(f"Error while searching categories by areas: {e}")
            return pd.DataFrame()
    
    def getAreasAssignedToCategories(self, category_ids: Set[str]) -> pd.DataFrame:
        """
        Return areas assigned to specified categories.

        Args:
            category_ids (Set[str]): Set of category identifiers

        Returns:
            pd.DataFrame: DataFrame with found areas
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            
            if not category_ids:
                # If categories are not specified, return all areas
                query = "SELECT DISTINCT id FROM areas ORDER BY id"
                df = pd.read_sql_query(query, conn)
            else:
                # Build query with category filter
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
            print(f"Error while searching areas by categories: {e}")
            return pd.DataFrame()
