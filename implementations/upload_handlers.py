# -*- coding: utf-8 -*-
"""
Обработчики загрузки данных в базы данных.
Содержит классы: JournalUploadHandler, CategoryUploadHandler
"""

import csv
import json
import sqlite3
import requests
from typing import List, Dict, Any
from .handlers import UploadHandler


class JournalUploadHandler(UploadHandler):
    """
    Обработчик загрузки журналов из CSV в графовую базу данных Blazegraph.
    """
    
    def pushDataToDb(self, path: str) -> bool:
        """
        Загружает данные журналов из CSV файла в Blazegraph.
        
        Args:
            path (str): Путь к CSV файлу
            
        Returns:
            bool: True если загрузка прошла успешно
        """
        try:
            # Читаем CSV файл
            journals_data = self._read_csv_file(path)
            if not journals_data:
                print(f"Ошибка: не удалось прочитать файл {path}")
                return False
            
            # Загружаем данные в Blazegraph
            return self._upload_to_blazegraph(journals_data)
            
        except Exception as e:
            print(f"Ошибка при загрузке журналов: {e}")
            return False
    
    def _read_csv_file(self, path: str) -> List[Dict[str, Any]]:
        """
        Читает CSV файл с данными журналов.
        
        Args:
            path (str): Путь к CSV файлу
            
        Returns:
            List[Dict[str, Any]]: Список словарей с данными журналов
        """
        journals = []
        
        try:
            with open(path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                count = 0
                for row in reader:
                    if count >= 100:  # Ограничиваем для тестирования
                        break
                    journal_data = {
                        'title': row['Journal title'].strip(),
                        'issn_print': row['Journal ISSN (print version)'].strip(),
                        'eissn': row['Journal EISSN (online version)'].strip(),
                        'languages': [lang.strip() for lang in row['Languages in which the journal accepts manuscripts'].split(', ')],
                        'publisher': row['Publisher'].strip() if row['Publisher'].strip() else None,
                        'seal': row['DOAJ Seal'].strip().lower() == 'yes',
                        'licence': row['Journal license'].strip(),
                        'apc': row['APC'].strip().lower() == 'yes'
                    }
                    journals.append(journal_data)
                    count += 1
        except Exception as e:
            print(f"Ошибка при чтении CSV файла: {e}")
            return []
        
        return journals
    
    def _upload_to_blazegraph(self, journals_data: List[Dict[str, Any]]) -> bool:
        """
        Загружает данные журналов в Blazegraph.
        
        Args:
            journals_data (List[Dict[str, Any]]): Данные журналов
            
        Returns:
            bool: True если загрузка прошла успешно
        """
        try:
            # Загружаем журналы по одному для избежания ошибок
            success_count = 0
            
            for journal in journals_data:
                sparql_query = self._build_single_journal_query(journal)
                
                response = requests.post(
                    self._dbPathOrUrl,
                    data={'update': sparql_query},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                if response.status_code == 200:
                    success_count += 1
                else:
                    print(f"Ошибка при загрузке журнала {journal.get('issn_print', 'unknown')}: {response.status_code}")
            
            if success_count > 0:
                print(f"Успешно загружено {success_count} из {len(journals_data)} журналов в Blazegraph")
                return True
            else:
                print("Не удалось загрузить ни одного журнала")
                return False
                
        except Exception as e:
            print(f"Ошибка при загрузке в Blazegraph: {e}")
            return False
    
    def _build_insert_query(self, journals_data: List[Dict[str, Any]]) -> str:
        """
        Строит SPARQL INSERT запрос для загрузки журналов.
        
        Args:
            journals_data (List[Dict[str, Any]]): Данные журналов
            
        Returns:
            str: SPARQL INSERT запрос
        """
        # Определяем префиксы
        prefixes = """
        PREFIX doaj: <http://doaj.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """
        
        # Формируем INSERT DATA блок
        insert_data = "INSERT DATA {\n"
        
        for journal in journals_data:
            # Используем ISSN как идентификатор журнала
            journal_id = journal['issn_print'] or journal['eissn']
            if not journal_id:
                continue
                
            journal_uri = f"<http://doaj.org/journal/{journal_id}>"
            
            # Добавляем триплеты для журнала
            insert_data += f"    {journal_uri} rdf:type doaj:Journal .\n"
            insert_data += f"    {journal_uri} doaj:title \"{self._escape_string(journal['title'])}\" .\n"
            
            if journal['issn_print']:
                insert_data += f"    {journal_uri} doaj:issn \"{journal['issn_print']}\" .\n"
            if journal['eissn']:
                insert_data += f"    {journal_uri} doaj:eissn \"{journal['eissn']}\" .\n"
            
            # Языки
            for lang in journal['languages']:
                insert_data += f"    {journal_uri} doaj:language \"{self._escape_string(lang)}\" .\n"
            
            # Издатель
            if journal['publisher']:
                insert_data += f"    {journal_uri} doaj:publisher \"{self._escape_string(journal['publisher'])}\" .\n"
            
            # DOAJ Seal
            insert_data += f"    {journal_uri} doaj:hasDOAJSeal \"{journal['seal']}\"^^xsd:boolean .\n"
            
            # Лицензия
            insert_data += f"    {journal_uri} doaj:licence \"{self._escape_string(journal['licence'])}\" .\n"
            
            # APC
            insert_data += f"    {journal_uri} doaj:hasAPC \"{journal['apc']}\"^^xsd:boolean .\n"
        
        insert_data += "}"
        
        return prefixes + insert_data
    
    def _build_single_journal_query(self, journal: Dict[str, Any]) -> str:
        """
        Строит SPARQL INSERT запрос для одного журнала.
        
        Args:
            journal (Dict[str, Any]): Данные журнала
            
        Returns:
            str: SPARQL INSERT запрос
        """
        # Определяем префиксы
        prefixes = """
        PREFIX doaj: <http://doaj.org/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        """
        
        # Используем ISSN как идентификатор журнала
        journal_id = journal['issn_print'] or journal['eissn']
        if not journal_id:
            return ""
            
        journal_uri = f"<http://doaj.org/journal/{journal_id}>"
        
        # Формируем INSERT DATA блок
        insert_data = f"INSERT DATA {{\n"
        insert_data += f"    {journal_uri} rdf:type doaj:Journal .\n"
        insert_data += f"    {journal_uri} doaj:title \"{self._escape_string(journal['title'])}\" .\n"
        
        if journal['issn_print']:
            insert_data += f"    {journal_uri} doaj:issn \"{journal['issn_print']}\" .\n"
        if journal['eissn']:
            insert_data += f"    {journal_uri} doaj:eissn \"{journal['eissn']}\" .\n"
        
        # Языки
        for lang in journal['languages']:
            insert_data += f"    {journal_uri} doaj:language \"{self._escape_string(lang)}\" .\n"
        
        # Издатель
        if journal['publisher']:
            insert_data += f"    {journal_uri} doaj:publisher \"{self._escape_string(journal['publisher'])}\" .\n"
        
        # DOAJ Seal
        insert_data += f"    {journal_uri} doaj:hasDOAJSeal \"{journal['seal']}\" .\n"
        
        # Лицензия
        insert_data += f"    {journal_uri} doaj:licence \"{self._escape_string(journal['licence'])}\" .\n"
        
        # APC
        insert_data += f"    {journal_uri} doaj:hasAPC \"{journal['apc']}\" .\n"
        
        insert_data += "}"
        
        return prefixes + insert_data
    
    def _escape_string(self, text: str) -> str:
        """
        Экранирует строку для SPARQL запроса.
        
        Args:
            text (str): Исходная строка
            
        Returns:
            str: Экранированная строка
        """
        return text.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')


class CategoryUploadHandler(UploadHandler):
    """
    Обработчик загрузки категорий и областей из JSON в реляционную базу данных SQLite.
    """
    
    def pushDataToDb(self, path: str) -> bool:
        """
        Загружает данные категорий и областей из JSON файла в SQLite.
        
        Args:
            path (str): Путь к JSON файлу
            
        Returns:
            bool: True если загрузка прошла успешно
        """
        try:
            # Читаем JSON файл
            scimago_data = self._read_json_file(path)
            if not scimago_data:
                print(f"Ошибка: не удалось прочитать файл {path}")
                return False
            
            # Создаем таблицы и загружаем данные
            return self._upload_to_sqlite(scimago_data)
            
        except Exception as e:
            print(f"Ошибка при загрузке категорий: {e}")
            return False
    
    def _read_json_file(self, path: str) -> List[Dict[str, Any]]:
        """
        Читает JSON файл с данными Scimago.
        
        Args:
            path (str): Путь к JSON файлу
            
        Returns:
            List[Dict[str, Any]]: Список словарей с данными
        """
        try:
            with open(path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                return data
        except Exception as e:
            print(f"Ошибка при чтении JSON файла: {e}")
            return []
    
    def _upload_to_sqlite(self, scimago_data: List[Dict[str, Any]]) -> bool:
        """
        Загружает данные в SQLite базу данных.
        
        Args:
            scimago_data (List[Dict[str, Any]]): Данные Scimago
            
        Returns:
            bool: True если загрузка прошла успешно
        """
        try:
            conn = sqlite3.connect(self._dbPathOrUrl)
            cursor = conn.cursor()
            
            # Создаем таблицы
            self._create_tables(cursor)
            
            # Загружаем данные
            self._insert_data(cursor, scimago_data)
            
            conn.commit()
            conn.close()
            
            print(f"Успешно загружены данные в SQLite базу {self._dbPathOrUrl}")
            return True
            
        except Exception as e:
            print(f"Ошибка при загрузке в SQLite: {e}")
            return False
    
    def _create_tables(self, cursor) -> None:
        """
        Создает таблицы в SQLite базе данных.
        
        Args:
            cursor: Курсор SQLite
        """
        # Таблица областей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS areas (
                id TEXT PRIMARY KEY
            )
        ''')
        
        # Таблица категорий
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id TEXT PRIMARY KEY,
                quartile TEXT
            )
        ''')
        
        # Таблица связей журнал-категория
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS journal_categories (
                issn TEXT,
                category_id TEXT,
                quartile TEXT,
                PRIMARY KEY (issn, category_id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )
        ''')
        
        # Таблица связей журнал-область
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
        Вставляет данные в таблицы SQLite.
        
        Args:
            cursor: Курсор SQLite
            scimago_data (List[Dict[str, Any]]): Данные Scimago
        """
        for entry in scimago_data:
            identifiers = entry.get('identifiers', [])
            categories = entry.get('categories', [])
            areas = entry.get('areas', [])
            
            # Вставляем области
            for area in areas:
                cursor.execute('INSERT OR IGNORE INTO areas (id) VALUES (?)', (area,))
            
            # Вставляем категории
            for category in categories:
                category_id = category.get('id')
                quartile = category.get('quartile')
                cursor.execute('INSERT OR IGNORE INTO categories (id, quartile) VALUES (?, ?)', 
                             (category_id, quartile))
            
            # Вставляем связи журнал-категория
            for issn in identifiers:
                for category in categories:
                    category_id = category.get('id')
                    quartile = category.get('quartile')
                    cursor.execute('INSERT OR IGNORE INTO journal_categories (issn, category_id, quartile) VALUES (?, ?, ?)',
                                 (issn, category_id, quartile))
            
            # Вставляем связи журнал-область
            for issn in identifiers:
                for area in areas:
                    cursor.execute('INSERT OR IGNORE INTO journal_areas (issn, area_id) VALUES (?, ?)',
                                 (issn, area))
