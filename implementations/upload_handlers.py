# -*- coding: utf-8 -*-
"""
Upload handlers for importing data into databases.
Contains classes: JournalUploadHandler, CategoryUploadHandler
"""

import csv
import json
import sqlite3
import requests
from typing import List, Dict, Any
from .handlers import UploadHandler


class JournalUploadHandler(UploadHandler):
    """
    Handler for uploading journals from CSV into a Blazegraph graph database.
    """
    
    def pushDataToDb(self, path: str) -> bool:
        """
        Upload journal data from a CSV file to Blazegraph.

        Args:
            path (str): Path to the CSV file

        Returns:
            bool: True if the upload succeeded
        """
        try:
            # Read the CSV file
            journals_data = self._read_csv_file(path)
            if not journals_data:
                print(f"Error: failed to read file {path}")
                return False
            
            # Upload data to Blazegraph
            return self._upload_to_blazegraph(journals_data)
            
        except Exception as e:
            print(f"Error while uploading journals: {e}")
            return False
    
    def _read_csv_file(self, path: str) -> List[Dict[str, Any]]:
        """
        Read a CSV file with journal data.

        Args:
            path (str): Path to the CSV file

        Returns:
            List[Dict[str, Any]]: List of dictionaries with journal data
        """
        journals = []
        
        try:
            with open(path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    journal_data = {
                        'title': row['Journal title'].strip(),
                        'issn_print': row['Journal ISSN (print version)'].strip(),
                        'eissn': row['Journal EISSN (online version)'].strip(),
                        'languages': [lang.strip() for lang in row['Languages in which the journal accepts manuscripts'].split(', ') if lang.strip()],
                        'publisher': row['Publisher'].strip() if row['Publisher'].strip() else None,
                        'seal': row['DOAJ Seal'].strip().lower() == 'yes',
                        'licence': row['Journal license'].strip(),
                        'apc': row['APC'].strip().lower() == 'yes'
                    }
                    journals.append(journal_data)
        except Exception as e:
            print(f"Error while reading CSV file: {e}")
            return []
        
        return journals
    
    def _upload_to_blazegraph(self, journals_data: List[Dict[str, Any]]) -> bool:
        """
        Upload journal data to Blazegraph.

        Args:
            journals_data (List[Dict[str, Any]]): Journal data

        Returns:
            bool: True if the upload succeeded
        """
        try:
            if not journals_data:
                print("No data to upload to Blazegraph")
                return False
            
            total_records = len(journals_data)
            uploaded_records = 0
            batch_size = 200
            
            for batch in self._chunked(journals_data, batch_size):
                sparql_query = self._build_insert_query(batch)
                
                response = requests.post(
                    self._dbPathOrUrl,
                    data={'update': sparql_query},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                if response.status_code == 200:
                    uploaded_records += len(batch)
                else:
                    sample_issn = batch[0].get('issn_print') or batch[0].get('eissn') or 'unknown'
                    print(f"Error while uploading journal batch (sample ISSN {sample_issn}): {response.status_code}")
            
            if uploaded_records == total_records:
                print(f"Successfully uploaded {uploaded_records} of {total_records} journals to Blazegraph")
                return True
            
            if uploaded_records > 0:
                print(f"Partially uploaded {uploaded_records} of {total_records} journals")
            else:
                print("Failed to upload any journals")
            return False
            
        except Exception as e:
            print(f"Error while uploading to Blazegraph: {e}")
            return False
    
    def _build_insert_query(self, journals_data: List[Dict[str, Any]]) -> str:
        """
        Build a SPARQL INSERT query for uploading journals.

        Args:
            journals_data (List[Dict[str, Any]]): Journal data

        Returns:
            str: SPARQL INSERT query
        """
        # Define prefixes
        prefixes = """
        PREFIX doaj: <http://doaj.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """
        
        # Build INSERT DATA block
        insert_data = "INSERT DATA {\n"
        
        for journal in journals_data:
            # Use ISSN as the journal identifier
            journal_id = journal['issn_print'] or journal['eissn']
            if not journal_id:
                continue
                
            journal_uri = f"<http://doaj.org/journal/{journal_id}>"
            
            # Add triples for the journal
            insert_data += f"    {journal_uri} rdf:type doaj:Journal .\n"
            insert_data += f"    {journal_uri} doaj:title \"{self._escape_string(journal['title'])}\" .\n"
            
            if journal['issn_print']:
                insert_data += f"    {journal_uri} doaj:issn \"{journal['issn_print']}\" .\n"
            if journal['eissn']:
                insert_data += f"    {journal_uri} doaj:eissn \"{journal['eissn']}\" .\n"
            
            # Languages
            for lang in journal['languages']:
                insert_data += f"    {journal_uri} doaj:language \"{self._escape_string(lang)}\" .\n"
            
            # Publisher
            if journal['publisher']:
                insert_data += f"    {journal_uri} doaj:publisher \"{self._escape_string(journal['publisher'])}\" .\n"
            
            # DOAJ Seal
            insert_data += f"    {journal_uri} doaj:hasDOAJSeal \"{self._bool_literal(journal['seal'])}\"^^xsd:boolean .\n"
            
            # Licence
            insert_data += f"    {journal_uri} doaj:licence \"{self._escape_string(journal['licence'])}\" .\n"
            
            # APC
            insert_data += f"    {journal_uri} doaj:hasAPC \"{self._bool_literal(journal['apc'])}\"^^xsd:boolean .\n"
        
        insert_data += "}"
        
        return prefixes + insert_data
    
    def _build_single_journal_query(self, journal: Dict[str, Any]) -> str:
        """
        Build a SPARQL INSERT query for a single journal.

        Args:
            journal (Dict[str, Any]): Journal data

        Returns:
            str: SPARQL INSERT query
        """
        # Define prefixes
        prefixes = """
        PREFIX doaj: <http://doaj.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """
        
        # Use ISSN as the journal identifier
        journal_id = journal['issn_print'] or journal['eissn']
        if not journal_id:
            return ""
            
        journal_uri = f"<http://doaj.org/journal/{journal_id}>"
        
        # Build INSERT DATA block
        insert_data = f"INSERT DATA {{\n"
        insert_data += f"    {journal_uri} rdf:type doaj:Journal .\n"
        insert_data += f"    {journal_uri} doaj:title \"{self._escape_string(journal['title'])}\" .\n"
        
        if journal['issn_print']:
            insert_data += f"    {journal_uri} doaj:issn \"{journal['issn_print']}\" .\n"
        if journal['eissn']:
            insert_data += f"    {journal_uri} doaj:eissn \"{journal['eissn']}\" .\n"
        
        # Languages
        for lang in journal['languages']:
            insert_data += f"    {journal_uri} doaj:language \"{self._escape_string(lang)}\" .\n"
        
        # Publisher
        if journal['publisher']:
            insert_data += f"    {journal_uri} doaj:publisher \"{self._escape_string(journal['publisher'])}\" .\n"
        
        # DOAJ Seal
        insert_data += f"    {journal_uri} doaj:hasDOAJSeal \"{self._bool_literal(journal['seal'])}\"^^xsd:boolean .\n"
        
        # Licence
        insert_data += f"    {journal_uri} doaj:licence \"{self._escape_string(journal['licence'])}\" .\n"
        
        # APC
        insert_data += f"    {journal_uri} doaj:hasAPC \"{self._bool_literal(journal['apc'])}\"^^xsd:boolean .\n"
        
        insert_data += "}"
        
        return prefixes + insert_data
    
    def _escape_string(self, text: str) -> str:
        """
        Escape a string for SPARQL queries.

        Args:
            text (str): Original string

        Returns:
            str: Escaped string
        """
        return (
            text.replace('\\', '\\\\')
            .replace('"', '\\"')
            .replace('\n', '\\n')
            .replace('\r', '\\r')
        )

    def _bool_literal(self, value: bool) -> str:
        """Return a lowercase boolean literal for SPARQL."""
        return "true" if bool(value) else "false"
    
    @staticmethod
    def _chunked(items: List[Dict[str, Any]], size: int):
        """Split a list of dictionaries into batches of the specified size."""
        batch: List[Dict[str, Any]] = []
        for item in items:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch


class CategoryUploadHandler(UploadHandler):
    """
    Handler for uploading categories and areas from JSON into a relational SQLite database.
    """
    
    def pushDataToDb(self, path: str) -> bool:
        """
        Upload categories and areas data from a JSON file into SQLite.

        Args:
            path (str): Path to the JSON file

        Returns:
            bool: True if the upload succeeded
        """
        try:
            # Read the JSON file
            scimago_data = self._read_json_file(path)
            if not scimago_data:
                print(f"Error: failed to read file {path}")
                return False
            
            # Create tables and insert data
            return self._upload_to_sqlite(scimago_data)
            
        except Exception as e:
            print(f"Error while uploading categories: {e}")
            return False
    
    def _read_json_file(self, path: str) -> List[Dict[str, Any]]:
        """
        Read a JSON file with Scimago data.

        Args:
            path (str): Path to the JSON file

        Returns:
            List[Dict[str, Any]]: List of dictionaries with data
        """
        try:
            with open(path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                return data
        except Exception as e:
            print(f"Error while reading JSON file: {e}")
            return []
    
    def _upload_to_sqlite(self, scimago_data: List[Dict[str, Any]]) -> bool:
        """
        Upload data into the SQLite database.

        Args:
            scimago_data (List[Dict[str, Any]]): Scimago data

        Returns:
            bool: True if the upload succeeded
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            cursor = conn.cursor()
            
            # Create tables
            self._create_tables(cursor)
            
            # Insert data
            self._insert_data(cursor, scimago_data)
            
            conn.commit()
            conn.close()
            
            print(f"Successfully loaded data into SQLite database {self._dbPathOrUrl}")
            return True
            
        except Exception as e:
            print(f"Error while uploading to SQLite: {e}")
            return False
    
    def _create_tables(self, cursor) -> None:
        """
        Create tables in the SQLite database.

        Args:
            cursor: SQLite cursor
        """
        # Areas table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS areas (
                id TEXT PRIMARY KEY
            )
        ''')
        
        # Categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id TEXT PRIMARY KEY,
                quartile TEXT
            )
        ''')
        
        # Journal-category relation table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS journal_categories (
                issn TEXT,
                category_id TEXT,
                quartile TEXT,
                PRIMARY KEY (issn, category_id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        ''')
        
        # Journal-area relation table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS journal_areas (
                issn TEXT,
                area_id TEXT,
                PRIMARY KEY (issn, area_id),
                FOREIGN KEY (area_id) REFERENCES areas(id)
            )
        ''')
    
    def _insert_data(self, cursor, scimago_data: List[Dict[str, Any]]) -> None:
        """
        Insert data into the SQLite tables.

        Args:
            cursor: SQLite cursor
            scimago_data (List[Dict[str, Any]]): Scimago data
        """
        for entry in scimago_data:
            identifiers = entry.get('identifiers', [])
            categories = entry.get('categories', [])
            areas = entry.get('areas', [])
            
            # Insert areas
            for area in areas:
                cursor.execute('INSERT OR IGNORE INTO areas (id) VALUES (?)', (area,))
            
            # Insert categories
            for category in categories:
                category_id = category.get('id')
                quartile = category.get('quartile')
                cursor.execute('INSERT OR IGNORE INTO categories (id, quartile) VALUES (?, ?)', 
                             (category_id, quartile))
            
            # Insert journal-category relations
            for issn in identifiers:
                for category in categories:
                    category_id = category.get('id')
                    quartile = category.get('quartile')
                    cursor.execute('INSERT OR IGNORE INTO journal_categories (issn, category_id, quartile) VALUES (?, ?, ?)',
                                 (issn, category_id, quartile))
            
            # Insert journal-area relations
            for issn in identifiers:
                for area in areas:
                    cursor.execute('INSERT OR IGNORE INTO journal_areas (issn, area_id) VALUES (?, ?)',
                                 (issn, area))
