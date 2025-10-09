# -*- coding: utf-8 -*-
"""
Query engines for performing complex queries against databases.
Contains classes: BasicQueryEngine, FullQueryEngine
"""

from typing import List, Set, Optional
from .models import Journal, Category, Area, IdentifiableEntity
from .query_handlers import JournalQueryHandler, CategoryQueryHandler


class BasicQueryEngine:
    """
    Basic query engine for working with journals and categories.
    """
    
    def __init__(self):
        self._journalQuery: List[JournalQueryHandler] = []
        self._categoryQuery: List[CategoryQueryHandler] = []
    
    def cleanJournalHandlers(self) -> bool:
        """
        Clear the list of journal handlers.

        Returns:
            bool: True if the clear succeeded
        """
        try:
            self._journalQuery.clear()
            return True
        except Exception:
            return False
    
    def cleanCategoryHandlers(self) -> bool:
        """
        Clear the list of category handlers.

        Returns:
            bool: True if the clear succeeded
        """
        try:
            self._categoryQuery.clear()
            return True
        except Exception:
            return False
    
    def addJournalHandler(self, handler: JournalQueryHandler) -> bool:
        """
        Add a journal handler.

        Args:
            handler (JournalQueryHandler): Journal handler

        Returns:
            bool: True if the addition succeeded
        """
        try:
            if handler and handler not in self._journalQuery:
                self._journalQuery.append(handler)
            return True
        except Exception:
            return False
    
    def addCategoryHandler(self, handler: CategoryQueryHandler) -> bool:
        """
        Add a category handler.

        Args:
            handler (CategoryQueryHandler): Category handler

        Returns:
            bool: True if the addition succeeded
        """
        try:
            if handler and handler not in self._categoryQuery:
                self._categoryQuery.append(handler)
            return True
        except Exception:
            return False
    
    def getEntityById(self, entity_id: str) -> Optional[IdentifiableEntity]:
        """
        Return an entity by identifier.

        Args:
            entity_id (str): Entity identifier

        Returns:
            IdentifiableEntity or None: Found entity or None
        """
        try:
            # Search in journals
            for handler in self._journalQuery:
                df = handler.getById(entity_id)
                if not df.empty:
                    return self._dataframe_to_journal(df.iloc[0])
            
            # Search in categories
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
            print(f"Error while searching for entity by ID: {e}")
            return None
    
    def getAllJournals(self) -> List[Journal]:
        """
        Return all journals.

        Returns:
            List[Journal]: List of all journals
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
            print(f"Error while fetching all journals: {e}")
            return []
    
    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        """
        Return journals with partial title match.

        Args:
            partialTitle (str): Partial title to search for

        Returns:
            List[Journal]: List of found journals
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
            print(f"Error while searching journals by title: {e}")
            return []
    
    def getJournalsPublishedBy(self, partialName: str) -> List[Journal]:
        """
        Return journals with partial publisher name match.

        Args:
            partialName (str): Partial publisher name to search for

        Returns:
            List[Journal]: List of found journals
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
            print(f"Error while searching journals by publisher: {e}")
            return []
    
    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        """
        Return journals with specified licenses.

        Args:
            licenses (Set[str]): Set of licenses to search for

        Returns:
            List[Journal]: List of found journals
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
            print(f"Error while searching journals by license: {e}")
            return []
    
    def getJournalsWithAPC(self) -> List[Journal]:
        """
        Return journals that have Article Processing Charge (APC).

        Returns:
            List[Journal]: List of journals with APC
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
            print(f"Error while searching journals with APC: {e}")
            return []
    
    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        """
        Return journals that have DOAJ Seal.

        Returns:
            List[Journal]: List of journals with DOAJ Seal
        """
        journals = []
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithDOAJSeal()
                for _, row in df.iterrows():
                    journal = self._dataframe_to_journal(row)
                    if journal:
                        journals.append(journal)
            
            return journals
            
        except Exception as e:
            print(f"Error while searching journals with DOAJ Seal: {e}")
            return []
    
    def getAllCategories(self) -> List[Category]:
        """
        Return all categories.

        Returns:
            List[Category]: List of all categories
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
            print(f"Error while fetching all categories: {e}")
            return []
    
    def getAllAreas(self) -> List[Area]:
        """
        Return all areas.

        Returns:
            List[Area]: List of all areas
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
            print(f"Error while fetching all areas: {e}")
            return []
    
    def getCategoriesWithQuartile(self, quartiles: Set[str]) -> List[Category]:
        """
        Return categories with specified quartiles.

        Args:
            quartiles (Set[str]): Set of quartiles to search for

        Returns:
            List[Category]: List of found categories
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
            print(f"Error while searching categories by quartile: {e}")
            return []
    
    def getCategoriesAssignedToAreas(self, area_ids: Set[str]) -> List[Category]:
        """
        Return categories assigned to the specified areas.

        Args:
            area_ids (Set[str]): Set of area identifiers

        Returns:
            List[Category]: List of found categories
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
            print(f"Error while searching categories by areas: {e}")
            return []
    
    def getAreasAssignedToCategories(self, category_ids: Set[str]) -> List[Area]:
        """
        Return areas assigned to the specified categories.

        Args:
            category_ids (Set[str]): Set of category identifiers

        Returns:
            List[Area]: List of found areas
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
            print(f"Error while searching areas by categories: {e}")
            return []
    
    def _dataframe_to_journal(self, row) -> Optional[Journal]:
        """
        Convert a DataFrame row to a Journal object.

        Args:
            row: DataFrame row

        Returns:
            Journal or None: Journal object or None
        """
        try:
            journal = Journal()
            
            # Set identifier (ISSN)
            issn = row.get('issn', '') or row.get('eissn', '')
            if issn:
                journal.setId(issn)
            
            # Set other fields
            journal.setTitle(row.get('title', ''))
            
            # Languages (may be in different columns)
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
            print(f"Error while creating Journal object: {e}")
            return None
    
    def _dataframe_to_category(self, row) -> Optional[Category]:
        """
        Convert a DataFrame row to a Category object.

        Args:
            row: DataFrame row

        Returns:
            Category or None: Category object or None
        """
        try:
            category = Category()
            category.setId(row.get('id', ''))
            category.setQuartile(row.get('quartile'))
            return category
            
        except Exception as e:
            print(f"Error while creating Category object: {e}")
            return None
    
    def _dataframe_to_area(self, row) -> Optional[Area]:
        """
        Convert a DataFrame row to an Area object.

        Args:
            row: DataFrame row

        Returns:
            Area or None: Area object or None
        """
        try:
            area = Area()
            area.setId(row.get('id', ''))
            return area
            
        except Exception as e:
            print(f"Error while creating Area object: {e}")
            return None


class FullQueryEngine(BasicQueryEngine):
    """
    Extended query engine for performing complex mashup queries.
    """
    
    def getJournalsInCategoriesWithQuartile(self, category_ids: Set[str], quartiles: Set[str]) -> List[Journal]:
        """
        Return journals in specified categories with given quartiles.

        Args:
            category_ids (Set[str]): Set of category identifiers
            quartiles (Set[str]): Set of quartiles

        Returns:
            List[Journal]: List of found journals
        """
        try:
            # Get categories with specified quartiles
            categories_with_quartile = self.getCategoriesWithQuartile(quartiles)
            
            # Filter by specified categories if provided
            if category_ids:
                categories_with_quartile = [cat for cat in categories_with_quartile 
                                          if cat.getIds()[0] in category_ids]
            
            # Get ISSNs of journals from these categories
            journal_issns = set()
            for handler in self._categoryQuery:
                for category in categories_with_quartile:
                    category_id = category.getIds()[0]
                    # Here we need to get the ISSNs of journals for this category
                    # This requires an additional query to SQLite
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
            print(f"Error while searching journals in categories with quartile: {e}")
            return []
    
    def getJournalsInAreasWithLicense(self, area_ids: Set[str], licenses: Set[str]) -> List[Journal]:
        """
        Return journals in specified areas with given licenses.

        Args:
            area_ids (Set[str]): Set of area identifiers
            licenses (Set[str]): Set of licenses

        Returns:
            List[Journal]: List of found journals
        """
        try:
            # Get journals with specified licenses
            journals_with_license = self.getJournalsWithLicense(licenses)
            
            # Get ISSNs of journals in specified areas
            journal_issns_in_areas = set()
            for handler in self._categoryQuery:
                for area_id in area_ids:
                    issns = self._get_issns_for_area(handler, area_id)
                    journal_issns_in_areas.update(issns)
            
            # Filter journals by areas
            filtered_journals = []
            for journal in journals_with_license:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if journal_issn and journal_issn in journal_issns_in_areas:
                    filtered_journals.append(journal)
            
            return filtered_journals
            
        except Exception as e:
            print(f"Error while searching journals in areas with license: {e}")
            return []
    
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: Set[str], 
                                                          category_ids: Set[str], 
                                                          quartiles: Set[str]) -> List[Journal]:
        """
        Return diamond journals (no APC) in specified areas and categories with given quartiles.

        Args:
            area_ids (Set[str]): Set of area identifiers
            category_ids (Set[str]): Set of category identifiers
            quartiles (Set[str]): Set of quartiles

        Returns:
            List[Journal]: List of found journals
        """
        try:
            # Get journals without APC
            journals_without_apc = []
            all_journals = self.getAllJournals()
            for journal in all_journals:
                if not journal.hasAPC():
                    journals_without_apc.append(journal)
            
            # Get ISSNs of journals in specified areas
            journal_issns_in_areas = set()
            for handler in self._categoryQuery:
                for area_id in area_ids:
                    issns = self._get_issns_for_area(handler, area_id)
                    journal_issns_in_areas.update(issns)
            
            # Get ISSNs of journals in specified categories with quartiles
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
            
            # Filter journals
            filtered_journals = []
            for journal in journals_without_apc:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if (journal_issn and 
                    journal_issn in journal_issns_in_areas and 
                    journal_issn in journal_issns_in_categories):
                    filtered_journals.append(journal)
            
            return filtered_journals
            
        except Exception as e:
            print(f"Error while searching for diamond journals: {e}")
            return []
    
    def _get_issns_for_category(self, handler: CategoryQueryHandler, category_id: str) -> Set[str]:
        """
        Get ISSNs of journals for the specified category.

        Args:
            handler (CategoryQueryHandler): Category handler
            category_id (str): Category identifier

        Returns:
            Set[str]: Set of journal ISSNs
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
        Get ISSNs of journals for the specified area.

        Args:
            handler (CategoryQueryHandler): Category handler
            area_id (str): Area identifier

        Returns:
            Set[str]: Set of journal ISSNs
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
