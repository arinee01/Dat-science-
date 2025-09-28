# -*- coding: utf-8 -*-
"""
Модель данных для системы анализа научных журналов.
Содержит классы: IdentifiableEntity, Journal, Category, Area
"""

from typing import List, Optional, Union


class IdentifiableEntity:
    """
    Базовый класс для всех идентифицируемых сущностей.
    """
    
    def __init__(self):
        self._ids: List[str] = []
    
    def getIds(self) -> List[str]:
        """
        Возвращает список идентификаторов сущности.
        
        Returns:
            List[str]: Список идентификаторов
        """
        return self._ids.copy()
    
    def addId(self, entity_id: str) -> None:
        """
        Добавляет идентификатор к сущности.
        
        Args:
            entity_id (str): Идентификатор для добавления
        """
        if entity_id and entity_id not in self._ids:
            self._ids.append(entity_id)
    
    def setId(self, entity_id: str) -> None:
        """
        Устанавливает единственный идентификатор.
        
        Args:
            entity_id (str): Идентификатор
        """
        self._ids = [entity_id] if entity_id else []


class Journal(IdentifiableEntity):
    """
    Класс журнала с метаданными из DOAJ.
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
        """Возвращает название журнала."""
        return self._title
    
    def getLanguages(self) -> List[str]:
        """Возвращает список языков журнала."""
        return self._languages.copy()
    
    def getPublisher(self) -> Optional[str]:
        """Возвращает издателя журнала."""
        return self._publisher
    
    def hasDOASeal(self) -> bool:
        """Проверяет наличие DOAJ Seal."""
        return self._seal
    
    def getLicence(self) -> str:
        """Возвращает лицензию журнала."""
        return self._licence
    
    def hasAPC(self) -> bool:
        """Проверяет наличие Article Processing Charge."""
        return self._apc
    
    def getCategories(self) -> List['Category']:
        """Возвращает связанные категории."""
        return self._categories.copy()
    
    def getAreas(self) -> List['Area']:
        """Возвращает связанные области."""
        return self._areas.copy()
    
    def setTitle(self, title: str) -> None:
        """Устанавливает название журнала."""
        self._title = title
    
    def setLanguages(self, languages: List[str]) -> None:
        """Устанавливает языки журнала."""
        self._languages = languages.copy() if languages else []
    
    def setPublisher(self, publisher: Optional[str]) -> None:
        """Устанавливает издателя журнала."""
        self._publisher = publisher
    
    def setSeal(self, seal: bool) -> None:
        """Устанавливает наличие DOAJ Seal."""
        self._seal = seal
    
    def setLicence(self, licence: str) -> None:
        """Устанавливает лицензию журнала."""
        self._licence = licence
    
    def setAPC(self, apc: bool) -> None:
        """Устанавливает наличие APC."""
        self._apc = apc
    
    def addCategory(self, category: 'Category') -> None:
        """Добавляет категорию к журналу."""
        if category and category not in self._categories:
            self._categories.append(category)
    
    def addArea(self, area: 'Area') -> None:
        """Добавляет область к журналу."""
        if area and area not in self._areas:
            self._areas.append(area)


class Category(IdentifiableEntity):
    """
    Класс категории из Scimago Journal Rank.
    """
    
    def __init__(self):
        super().__init__()
        self._quartile: Optional[str] = None
    
    def getQuartile(self) -> Optional[str]:
        """Возвращает квартиль категории."""
        return self._quartile
    
    def setQuartile(self, quartile: Optional[str]) -> None:
        """Устанавливает квартиль категории."""
        self._quartile = quartile


class Area(IdentifiableEntity):
    """
    Класс области из Scimago Journal Rank.
    """
    
    def __init__(self):
        super().__init__()
