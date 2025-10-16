# -*- coding: utf-8 -*-
"""
Query engines for performing complex queries against databases.
Contains classes: BasicQueryEngine, FullQueryEngine
"""

import math
from typing import Dict, Iterable, List, Set, Optional
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
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getAllJournals()
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
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
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithTitle(partialTitle)
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
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
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsPublishedBy(partialName)
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
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
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithLicense(licenses)
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
        except Exception as e:
            print(f"Error while searching journals by license: {e}")
            return []
    
    def getJournalsWithAPC(self) -> List[Journal]:
        """
        Return journals that have Article Processing Charge (APC).

        Returns:
            List[Journal]: List of journals with APC
        """
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithAPC()
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
        except Exception as e:
            print(f"Error while searching journals with APC: {e}")
            return []
    
    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        """
        Return journals that have DOAJ Seal.

        Returns:
            List[Journal]: List of journals with DOAJ Seal
        """
        journal_map: Dict[str, Journal] = {}
        
        try:
            for handler in self._journalQuery:
                df = handler.getJournalsWithDOAJSeal()
                self._collect_journals(df, journal_map)
            
            return list(journal_map.values())
            
        except Exception as e:
            print(f"Error while searching journals with DOAJ Seal: {e}")
            return []
    
    def getAllCategories(self) -> List[Category]:
        """
        Return all categories.

        Returns:
            List[Category]: List of all categories
        """
        category_map: Dict[str, Category] = {}
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAllCategories()
                self._collect_categories(df, category_map)
            
            return list(category_map.values())
            
        except Exception as e:
            print(f"Error while fetching all categories: {e}")
            return []
    
    def getAllAreas(self) -> List[Area]:
        """
        Return all areas.

        Returns:
            List[Area]: List of all areas
        """
        area_map: Dict[str, Area] = {}
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAllAreas()
                self._collect_areas(df, area_map)
            
            return list(area_map.values())
            
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
        category_map: Dict[str, Category] = {}
        
        try:
            for handler in self._categoryQuery:
                df = handler.getCategoriesWithQuartile(quartiles)
                self._collect_categories(df, category_map)
            
            return list(category_map.values())
            
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
        category_map: Dict[str, Category] = {}
        
        try:
            for handler in self._categoryQuery:
                df = handler.getCategoriesAssignedToAreas(area_ids)
                self._collect_categories(df, category_map)
            
            return list(category_map.values())
            
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
        area_map: Dict[str, Area] = {}
        
        try:
            for handler in self._categoryQuery:
                df = handler.getAreasAssignedToCategories(category_ids)
                self._collect_areas(df, area_map)
            
            return list(area_map.values())
            
        except Exception as e:
            print(f"Error while searching areas by categories: {e}")
            return []

    def _collect_journals(self, df, target: Dict[str, Journal]) -> None:
        """Merge journal rows into a deduplicated dictionary keyed by identifier."""
        if df is None or df.empty:
            return
        for idx, row in df.iterrows():
            key = self._get_journal_key(row, idx)
            if not key:
                continue
            if key in target:
                self._update_journal_from_row(target[key], row)
            else:
                journal = self._dataframe_to_journal(row)
                if journal:
                    target[key] = journal

    def _collect_categories(self, df, target: Dict[str, Category]) -> None:
        """Collect categories without duplicates."""
        if df is None or df.empty:
            return
        for idx, row in df.iterrows():
            category = self._dataframe_to_category(row)
            if not category:
                continue
            identifier = category.getIds()[0] if category.getIds() else f"__row_{idx}"
            if identifier in target:
                existing = target[identifier]
                if not existing.getQuartile() and category.getQuartile():
                    existing.setQuartile(category.getQuartile())
            else:
                target[identifier] = category

    def _collect_areas(self, df, target: Dict[str, Area]) -> None:
        """Collect areas without duplicates."""
        if df is None or df.empty:
            return
        for idx, row in df.iterrows():
            area = self._dataframe_to_area(row)
            if not area:
                continue
            identifier = area.getIds()[0] if area.getIds() else f"__row_{idx}"
            if identifier not in target:
                target[identifier] = area

    def _fetch_journals_by_issns(self, issns: Set[str]) -> List[Journal]:
        """Fetch journals in batches by ISSN using the registered handlers."""
        cleaned_ids = sorted({issn for issn in issns if issn})
        if not cleaned_ids:
            return []
        journal_map: Dict[str, Journal] = {}
        for handler in self._journalQuery:
            for chunk in self._chunked(cleaned_ids, 50):
                df = handler.getJournalsByIssns(set(chunk))
                self._collect_journals(df, journal_map)
        return list(journal_map.values())

    def _chunked(self, items: Iterable[str], size: int) -> Iterable[List[str]]:
        """Yield chunks of the input iterable with at most `size` elements."""
        batch: List[str] = []
        for item in items:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _get_journal_key(self, row, idx: int) -> Optional[str]:
        """Return a stable key for a journal row."""
        for candidate in ('issn', 'eissn', 'journal'):
            if candidate in row:
                value = row.get(candidate)
                if self._has_value(value):
                    return str(value).strip()
        return f"__row_{idx}"

    def _update_journal_from_row(self, journal: Journal, row) -> None:
        """Merge row data into an existing journal instance."""
        if self._has_value(row.get('title')) and not journal.getTitle():
            journal.setTitle(str(row.get('title')).strip())

        language = row.get('language')
        if self._has_value(language):
            languages = journal.getLanguages()
            lang_value = str(language).strip()
            if lang_value not in languages:
                languages.append(lang_value)
                journal.setLanguages(languages)

        if self._has_value(row.get('publisher')) and not journal.getPublisher():
            journal.setPublisher(str(row.get('publisher')).strip())

        seal_value = row.get('seal')
        if self._has_value(seal_value):
            journal.setSeal(self._to_bool(seal_value))

        licence_value = row.get('licence')
        if self._has_value(licence_value) and not journal.getLicence():
            journal.setLicence(str(licence_value).strip())

        apc_value = row.get('apc')
        if self._has_value(apc_value):
            journal.setAPC(self._to_bool(apc_value))

    @staticmethod
    def _has_value(value) -> bool:
        """Return True if the value is meaningful (not None/empty/nan)."""
        if value is None:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        if isinstance(value, str) and not value.strip():
            return False
        return True

    @staticmethod
    def _to_bool(value) -> bool:
        """Convert a mixed value into a boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {'1', 'true', 'yes'}
        return bool(value)
    
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
            issn = row.get('issn') if 'issn' in row else None
            if not self._has_value(issn):
                issn = row.get('eissn') if 'eissn' in row else None
            if self._has_value(issn):
                journal.setId(str(issn).strip())
            
            # Set other fields
            title_value = row.get('title')
            journal.setTitle(str(title_value).strip() if self._has_value(title_value) else "")
            
            # Languages (may be in different columns)
            language_value = row.get('language') if 'language' in row else None
            languages: List[str] = []
            if self._has_value(language_value):
                languages = [str(language_value).strip()]
            journal.setLanguages(languages)
            
            publisher_value = row.get('publisher')
            journal.setPublisher(str(publisher_value).strip() if self._has_value(publisher_value) else None)
            
            seal_value = row.get('seal')
            journal.setSeal(self._to_bool(seal_value) if self._has_value(seal_value) else False)
            
            licence_value = row.get('licence')
            journal.setLicence(str(licence_value).strip() if self._has_value(licence_value) else "")
            
            apc_value = row.get('apc')
            journal.setAPC(self._to_bool(apc_value) if self._has_value(apc_value) else False)
            
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
            identifier = row.get('id')
            category.setId(str(identifier).strip() if self._has_value(identifier) else "")
            quartile = row.get('quartile')
            category.setQuartile(str(quartile).strip() if self._has_value(quartile) else None)
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
            identifier = row.get('id')
            area.setId(str(identifier).strip() if self._has_value(identifier) else "")
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
            if not categories_with_quartile and quartiles:
                return []
            
            # Filter by specified categories if provided
            if category_ids:
                categories_with_quartile = [cat for cat in categories_with_quartile 
                                          if cat.getIds()[0] in category_ids]
                if not categories_with_quartile:
                    return []
            
            if not categories_with_quartile:
                return []
            
            # Get ISSNs of journals from these categories
            journal_issns: Set[str] = set()
            for handler in self._categoryQuery:
                for category in categories_with_quartile:
                    category_id = category.getIds()[0]
                    issns = self._get_issns_for_category(handler, category_id)
                    journal_issns.update(issns)
            
            return self._fetch_journals_by_issns(journal_issns)
            
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
            journal_issns_in_areas: Optional[Set[str]] = set()
            if area_ids:
                for handler in self._categoryQuery:
                    for area_id in area_ids:
                        issns = self._get_issns_for_area(handler, area_id)
                        journal_issns_in_areas.update(issns)
            else:
                journal_issns_in_areas = None
            
            # Filter journals by areas if needed
            if journal_issns_in_areas is None:
                return journals_with_license
            
            filtered: Dict[str, Journal] = {}
            for journal in journals_with_license:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if journal_issn and journal_issn in journal_issns_in_areas:
                    filtered[journal_issn] = journal
            
            return list(filtered.values())
            
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
            journals_without_apc = [journal for journal in self.getAllJournals() if not journal.hasAPC()]
            
            # Get ISSNs of journals in specified areas
            journal_issns_in_areas: Optional[Set[str]] = set()
            if area_ids:
                for handler in self._categoryQuery:
                    for area_id in area_ids:
                        issns = self._get_issns_for_area(handler, area_id)
                        journal_issns_in_areas.update(issns)
            else:
                journal_issns_in_areas = None
            
            # Get ISSNs of journals in specified categories with quartiles
            journal_issns_in_categories: Optional[Set[str]] = set()
            categories_with_quartile = self.getCategoriesWithQuartile(quartiles)
            if not categories_with_quartile and quartiles:
                return []
            if category_ids:
                categories_with_quartile = [cat for cat in categories_with_quartile 
                                          if cat.getIds()[0] in category_ids]
                if not categories_with_quartile:
                    return []
            
            if categories_with_quartile:
                for handler in self._categoryQuery:
                    for category in categories_with_quartile:
                        category_id = category.getIds()[0]
                        issns = self._get_issns_for_category(handler, category_id)
                        journal_issns_in_categories.update(issns)
            else:
                journal_issns_in_categories = None
            
            # Filter journals
            filtered: Dict[str, Journal] = {}
            for journal in journals_without_apc:
                journal_issn = journal.getIds()[0] if journal.getIds() else None
                if not journal_issn:
                    continue
                if journal_issns_in_areas is not None and journal_issn not in journal_issns_in_areas:
                    continue
                if journal_issns_in_categories is not None and journal_issn not in journal_issns_in_categories:
                    continue
                filtered[journal_issn] = journal
            
            return list(filtered.values())
            
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
