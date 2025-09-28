# -*- coding: utf-8 -*-
"""
Движки запросов для выполнения сложных запросов к базам данных.
Содержит классы: BasicQueryEngine, FullQueryEngine
"""

from typing import List, Set, Optional
from .models import Journal, Category, Area, IdentifiableEntity
from .query_handlers import JournalQueryHandler, CategoryQueryHandler


class BasicQueryEngine:
    """
    Базовый движок запросов для работы с журналами и категориями.
    """
    
    def __init__(self):
        self._journalQuery: List[JournalQueryHandler] = []
        self._categoryQuery: List[CategoryQueryHandler] = []
    
    def cleanJournalHandlers(self) -> bool:
        """
        Очищает список обработчиков журналов.
        
        Returns:
            bool: True если очистка прошла успешно
        """
        try:
            self._journalQuery.clear()
            return True
        except Exception:
            return False
    
    def cleanCategoryHandlers(self) -> bool:
        """
        Очищает список обработчиков категорий.
        
        Returns:
            bool: True если очистка прошла успешно
        """
        try:
            self._categoryQuery.clear()
            return True
        except Exception:
            return False
    
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool:
        """
        Добавляет обработчик журналов.
        
        Args:
            handler (JournalQueryHandler): Обработчик журналов
            
        Returns:
            bool: True если добавление прошло успешно
        """
        try:
            if handler and handler not in self._journalQuery:
                self._journalQuery.append(handler)
            return True
        except Exception:
            return False
    
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool:
        """
        Добавляет обработчик категорий.
        
        Args:
            handler (CategoryQueryHandler): Обработчик категорий
            
        Returns:
            bool: True если добавление прошло успешно
        """
        try:
            if handler and handler not in self._categoryQuery:
                self._categoryQuery.append(handler)
            return True
        except Exception:
            return False
    
    def getEntityById(self, entity_id: str) -> Optional[IdentifiableEntity]:
        """
        Возвращает сущность по идентификатору.
        
        Args:
            entity_id (str): Идентификатор сущности
            
        Returns:
            IdentifiableEntity or None: Найденная сущность или None
        """
        try:
            # Ищем в журналах
            for handler in self._journalQuery:
                df = handler.getById(entity_id)
                if not df.empty:
                    return self._dataframe_to_journal(df.iloc[0])
            
            # Ищем в категориях
            for handler in self._categoryQuery:
                df = handler.getById(entity_id)
                if not df.empty:
                    row = df.iloc[0]
                    if 'quartile' in row:
                        return self._dataframe_to_category(row)
                    else:
                        return self._dataframe_to_area(row)
            
            return None
            
        except Exception as e:
            print(f"Ошибка при поиске сущности по ID: {e}")
            return None
    
    def getAllJournals(self) -> List[Journal]:
        """
        Возвращает все журналы.
        
        Returns:
            List[Journal]: Список всех журналов
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getAllJournals()
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при получении всех журналов: {e}")
            return []
    
    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        """
        Возвращает журналы с частичным совпадением в названии.
        
        Args:
            partialTitle (str): Часть названия для поиска
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithTitle(partialTitle)
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по названию: {e}")
            return []
    
    def getJournalsPublishedBy(self, partialName: str) -> List[Journal]:
        """
        Возвращает журналы с частичным совпадением в названии издателя.
        
        Args:
            partialName (str): Часть названия издателя для поиска
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsPublishedBy(partialName)
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по издателю: {e}")
            return []
    
    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        """
        Возвращает журналы с указанными лицензиями.
        
        Args:
            licenses (Set[str]): Множество лицензий для поиска
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithLicense(licenses)
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов по лицензии: {e}")
            return []
    
    def getJournalsWithAPC(self) -> List[Journal]:
        """
        Возвращает журналы с Article Processing Charge.
        
        Returns:
            List[Journal]: Список журналов с APC
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithAPC()
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов с APC: {e}")
            return []
    
    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        """
        Возвращает журналы с DOAJ Seal.
        
        Returns:
            List[Journal]: Список журналов с DOAJ Seal
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithDOASeal()
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов с DOAJ Seal: {e}")
            return []
    
    def getAllCategories(self) -> List[Category]:
        """
        Возвращает все категории.
        
        Returns:
            List[Category]: Список всех категорий
        """
        categories = []
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAllCategories()
                for _, row in df.iterrows():
                    category = self._dataframe_to_category(row)
                    if category:
                        categories.append(category)
            
            return categories
            
        except Exception as e:
            print(f"Ошибка при получении всех категорий: {e}")
            return []
    
    def getAllAreas(self) -> List[Area]:
        """
        Возвращает все области.
        
        Returns:
            List[Area]: Список всех областей
        """
        areas = []
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAllAreas()
                for _, row in df.iterrows():
                    area = self._dataframe_to_area(row)
                    if area:
                        areas.append(area)
            
            return areas
            
        except Exception as e:
            print(f"Ошибка при получении всех областей: {e}")
            return []
    
    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        """
        Возвращает категории с указанными квартилями.
        
        Args:
            quartiles (Set[str]): Множество квартилей для поиска
            
        Returns:
            List[Category]: Список найденных категорий
        """
        categories = []
        
        try:
            for handler in self._categoryQuery:
                df = handler.getCategoriesWithQuartile(quartiles)
                for _, row in df.iterrows():
                    category = self._dataframe_to_category(row)
                    if category:
                        categories.append(category)
            
            return categories
            
        except Exception as e:
            print(f"Ошибка при поиске категорий по квартилю: {e}")
            return []
    
    def getCategoriesAssignedToAreas(self, area_ids: Set[str]) -> List[Category]:
        """
        Возвращает категории, назначенные указанным областям.
        
        Args:
            area_ids (Set[str]): Множество идентификаторов областей
            
        Returns:
            List[Category]: Список найденных категорий
        """
        categories = []
        
        try:
            for handler in self._categoryQuery:
                df = handler.getCategoriesAssignedToAreas(area_ids)
                for _, row in df.iterrows():
                    category = self._dataframe_to_category(row)
                    if category:
                        categories.append(category)
            
            return categories
            
        except Exception as e:
            print(f"Ошибка при поиске категорий по областям: {e}")
            return []
    
    def getAreasAssignedToCategories(self, category_ids: Set[str]) -> List[Area]:
        """
        Возвращает области, назначенные указанным категориям.
        
        Args:
            category_ids (Set[str]): Множество идентификаторов категорий
            
        Returns:
            List[Area]: Список найденных областей
        """
        areas = []
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAreasAssignedToCategories(category_ids)
                for _, row in df.iterrows():
                    area = self._dataframe_to_area(row)
                    if area:
                        areas.append(area)
            
            return areas
            
        except Exception as e:
            print(f"Ошибка при поиске областей по категориям: {e}")
            return []
    
    def _dataframe_to_journal(self, row) -> Optional[Journal]:
        """
        Преобразует строку DataFrame в объект Journal.
        
        Args:
            row: Строка DataFrame
            
        Returns:
            Journal or None: Объект журнала или None
        """
        try:
            journal = Journal()
            
            # Устанавливаем идентификатор (ISSN)
            issn = row.get('issn', '') or row.get('eissn', '')
            if issn:
                journal.setId(issn)
            
            # Устанавливаем остальные поля
            journal.setTitle(row.get('title', ''))
            
            # Языки (могут быть в разных колонках)
            languages = []
            if 'language' in row and row['language']:
                languages = [row['language']]
            journal.setLanguages(languages)
            
            journal.setPublisher(row.get('publisher'))
            seal_value = row.get('seal', 'false')
            if isinstance(seal_value, str):
                journal.setSeal(seal_value.lower() == 'true')
            else:
                journal.setSeal(bool(seal_value))
            
            journal.setLicence(row.get('licence', ''))
            
            apc_value = row.get('apc', 'false')
            if isinstance(apc_value, str):
                journal.setAPC(apc_value.lower() == 'true')
            else:
                journal.setAPC(bool(apc_value))
            
            return journal
            
        except Exception as e:
            print(f"Ошибка при создании объекта Journal: {e}")
            return None
    
    def _dataframe_to_category(self, row) -> Optional[Category]:
        """
        Преобразует строку DataFrame в объект Category.
        
        Args:
            row: Строка DataFrame
            
        Returns:
            Category or None: Объект категории или None
        """
        try:
            category = Category()
            category.setId(row.get('id', ''))
            category.setQuartile(row.get('quartile'))
            return category
            
        except Exception as e:
            print(f"Ошибка при создании объекта Category: {e}")
            return None
    
    def _dataframe_to_area(self, row) -> Optional[Area]:
        """
        Преобразует строку DataFrame в объект Area.
        
        Args:
            row: Строка DataFrame
            
        Returns:
            Area or None: Объект области или None
        """
        try:
            area = Area()
            area.setId(row.get('id', ''))
            return area
            
        except Exception as e:
            print(f"Ошибка при создании объекта Area: {e}")
            return None


class FullQueryEngine(BasicQueryEngine):
    """
    Расширенный движок запросов для выполнения сложных mashup запросов.
    """
    
    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        """
        Возвращает журналы в указанных категориях с определенными квартилями.
        
        Args:
            category_ids (Set[str]): Множество идентификаторов категорий
            quartiles (Set[str]): Множество квартилей
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        try:
            # Получаем категории с указанными квартилями
            categories_with_quartile = self.getCategoriesWithQuartile(quartiles)
            
            # Фильтруем по указанным категориям, если они заданы
            if category_ids:
                categories_with_quartile = [cat for cat in categories_with_quartile 
                                          if cat.getIds()[0] in category_ids]
            
            # Получаем ISSN журналов из этих категорий
            journal_issns = set()
            for handler in self._categoryQuery:
                for category in categories_with_quartile:
                    category_id = category.getIds()[0]
                    # Здесь нужно получить ISSN журналов для данной категории
                    # Это требует дополнительного запроса к SQLite
                    issns = self._get_issns_for_category(handler, category_id)
                    journal_issns.update(issns)
            
            # Получаем журналы по ISSN
            journals = []
            for handler in self._journalQuery:
                for issn in journal_issns:
                    df = handler.getById(issn)
                    if not df.empty:
                        journal = self._dataframe_to_journal(df.iloc[0])
                        if journal:
                            journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов в категориях с квартилем: {e}")
            return []
    
    def getJournalsInAreasWithLicense(self, area_ids: Set[str], licenses: Set[str]) -> List[Journal]:
        """
        Возвращает журналы в указанных областях с определенными лицензиями.
        
        Args:
            area_ids (Set[str]): Множество идентификаторов областей
            licenses (Set[str]): Множество лицензий
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        try:
            # Получаем журналы с указанными лицензиями
            journals_with_license = self.getJournalsWithLicense(licenses)
            
            # Получаем ISSN журналов в указанных областях
            journal_issns_in_areas = set()
            for handler in self._categoryQuery:
                for area_id in area_ids:
                    issns = self._get_issns_for_area(handler, area_id)
                    journal_issns_in_areas.update(issns)
            
            # Фильтруем журналы по областям
            filtered_journals = []
            for journal in journals_with_license:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if journal_issn and journal_issn in journal_issns_in_areas:
                    filtered_journals.append(journal)
            
            return filtered_journals
            
        except Exception as e:
            print(f"Ошибка при поиске журналов в областях с лицензией: {e}")
            return []
    
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: Set[str], 
                                                          category_ids: Set[str], 
                                                          quartiles: Set[str]) -> List[Journal]:
        """
        Возвращает "алмазные" журналы (без APC) в указанных областях и категориях с определенными квартилями.
        
        Args:
            area_ids (Set[str]): Множество идентификаторов областей
            category_ids (Set[str]): Множество идентификаторов категорий
            quartiles (Set[str]): Множество квартилей
            
        Returns:
            List[Journal]: Список найденных журналов
        """
        try:
            # Получаем журналы без APC
            journals_without_apc = []
            all_journals = self.getAllJournals()
            for journal in all_journals:
                if not journal.hasAPC():
                    journals_without_apc.append(journal)
            
            # Получаем ISSN журналов в указанных областях
            journal_issns_in_areas = set()
            for handler in self._categoryQuery:
                for area_id in area_ids:
                    issns = self._get_issns_for_area(handler, area_id)
                    journal_issns_in_areas.update(issns)
            
            # Получаем ISSN журналов в указанных категориях с квартилями
            journal_issns_in_categories = set()
            categories_with_quartile = self.getCategoriesWithQuartile(quartiles)
            if category_ids:
                categories_with_quartile = [cat for cat in categories_with_quartile 
                                          if cat.getIds()[0] in category_ids]
            
            for handler in self._categoryQuery:
                for category in categories_with_quartile:
                    category_id = category.getIds()[0]
                    issns = self._get_issns_for_category(handler, category_id)
                    journal_issns_in_categories.update(issns)
            
            # Фильтруем журналы
            filtered_journals = []
            for journal in journals_without_apc:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if (journal_issn and 
                    journal_issn in journal_issns_in_areas and 
                    journal_issn in journal_issns_in_categories):
                    filtered_journals.append(journal)
            
            return filtered_journals
            
        except Exception as e:
            print(f"Ошибка при поиске алмазных журналов: {e}")
            return []
    
    def _get_issns_for_category(self, handler: CategoryQueryHandler, category_id: str) -> Set[str]:
        """
        Получает ISSN журналов для указанной категории.
        
        Args:
            handler (CategoryQueryHandler): Обработчик категорий
            category_id (str): Идентификатор категории
            
        Returns:
            Set[str]: Множество ISSN журналов
        """
        try:
            import sqlite3
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            query = "SELECT DISTINCT issn FROM journal_categories WHERE category_id = ?"
            cursor = conn.execute(query, (category_id,))
            issns = {row[0] for row in cursor.fetchall()}
            conn.close()
            return issns
        except Exception:
            return set()
    
    def _get_issns_for_area(self, handler: CategoryQueryHandler, area_id: str) -> Set[str]:
        """
        Получает ISSN журналов для указанной области.
        
        Args:
            handler (CategoryQueryHandler): Обработчик категорий
            area_id (str): Идентификатор области
            
        Returns:
            Set[str]: Множество ISSN журналов
        """
        try:
            import sqlite3
            conn = sqlite3.connect(handler.getDbPathOrUrl())
            query = "SELECT DISTINCT issn FROM journal_areas WHERE area_id = ?"
            cursor = conn.execute(query, (area_id,))
            issns = {row[0] for row in cursor.fetchall()}
            conn.close()
            return issns
        except Exception:
            return set()
