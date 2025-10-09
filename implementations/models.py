# -*- coding: utf-8 -*-
"""
Data model for the scientific journals analysis system.
Contains classes: IdentifiableEntity, Journal, Category, Area
"""

from typing import List, Optional, Union


class IdentifiableEntity:
    """
    Base class for all identifiable entities.
    """
    
    def __init__(self):
        self._ids: List[str] = []
    
    def getIds(self) -> List[str]:
        """
        Return the list of entity identifiers.

        Returns:
            List[str]: List of identifiers
        """
        return self._ids.copy()
    
    def addId(self, entity_id: str) -> None:
        """
        Add an identifier to the entity.

        Args:
            entity_id (str): Identifier to add
        """
        if entity_id and entity_id not in self._ids:
            self._ids.append(entity_id)
    
    def setId(self, entity_id: str) -> None:
        """
        Set a single identifier for the entity.

        Args:
            entity_id (str): Identifier
        """
        self._ids = [entity_id] if entity_id else []


class Journal(IdentifiableEntity):
    """
    Journal class with metadata from DOAJ.
    """
    
    def __init__(self):
        super().__init__()
        self._title: str = ""
        self._languages: List[str] = []
        self._publisher: Optional[str] = None
        self._seal: bool = False
        self._licence: str = ""
        self._apc: bool = False
        self._categories: List['Category'] = []
        self._areas: List['Area'] = []
    
    def getTitle(self) -> str:
        """Return the journal title."""
        return self._title
    
    def getLanguages(self) -> List[str]:
        """Return the journal languages list."""
        return self._languages.copy()
    
    def getPublisher(self) -> Optional[str]:
        """Return the journal publisher."""
        return self._publisher
    
    def hasDOASeal(self) -> bool:
        """Check for presence of DOAJ Seal."""
        return self._seal
    
    def getLicence(self) -> str:
        """Return the journal licence."""
        return self._licence
    
    def hasAPC(self) -> bool:
        """Check whether Article Processing Charge (APC) applies."""
        return self._apc
    
    def getCategories(self) -> List['Category']:
        """Return related categories."""
        return self._categories.copy()
    
    def getAreas(self) -> List['Area']:
        """Return related areas."""
        return self._areas.copy()
    
    def setTitle(self, title: str) -> None:
        """Set the journal title."""
        self._title = title
    
    def setLanguages(self, languages: List[str]) -> None:
        """Set the journal languages."""
        self._languages = languages.copy() if languages else []
    
    def setPublisher(self, publisher: Optional[str]) -> None:
        """Set the journal publisher."""
        self._publisher = publisher
    
    def setSeal(self, seal: bool) -> None:
        """Set the presence of DOAJ Seal."""
        self._seal = seal
    
    def setLicence(self, licence: str) -> None:
        """Set the journal licence."""
        self._licence = licence
    
    def setAPC(self, apc: bool) -> None:
        """Set whether APC applies."""
        self._apc = apc
    
    def addCategory(self, category: 'Category') -> None:
        """Add a category to the journal."""
        if category and category not in self._categories:
            self._categories.append(category)
    
    def addArea(self, area: 'Area') -> None:
        """Add an area to the journal."""
        if area and area not in self._areas:
            self._areas.append(area)


class Category(IdentifiableEntity):
    """
    Category class from Scimago Journal Rank.
    """
    
    def __init__(self):
        super().__init__()
        self._quartile: Optional[str] = None
    
    def getQuartile(self) -> Optional[str]:
        """Return the category quartile."""
        return self._quartile
    
    def setQuartile(self, quartile: Optional[str]) -> None:
        """Set the category quartile."""
        self._quartile = quartile


class Area(IdentifiableEntity):
    """
    Area class from Scimago Journal Rank.
    """
    
    def __init__(self):
        super().__init__()
