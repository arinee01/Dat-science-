# -*- coding: utf-8 -*-
"""
Base handler classes for working with databases.
Contains classes: Handler, UploadHandler, QueryHandler
"""

from abc import ABC, abstractmethod
from typing import Optional


class Handler:
    """
    Base class for working with databases.
    """
    
    def __init__(
        self, dbPathOrUrl: str = ""
    ):  # ДОБАВЛЯЕМ ПАРАМЕТР ДЛЯ ТЕСТОВ И ТД И ЧТОБЫ ЛЕГЧЕ ВЫЗЫВАТЬ БД ЧЕРЕЗ handler = JournalQueryHandler("http://example.com/sparql")
        self._dbPathOrUrl: str = dbPathOrUrl

    
    def getDbPathOrUrl(self) -> str:
        """
        Return the database path or URL.

        Returns:
            str: Database path or URL
        """
        return self._dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        """
        Set the database path or URL.

        Args:
            pathOrUrl (str): Database path or URL

        Returns:
            bool: True if the assignment succeeded
        """
        try:
            self._dbPathOrUrl = pathOrUrl
            return True
        except Exception:
            return False


class UploadHandler(Handler):
    """
    Abstract class for uploading data to a database.
    """
    
    @abstractmethod
    def pushDataToDb(self, path: str) -> bool:
        """
        Upload data from a file to the database.

        Args:
            path (str): Path to the data file

        Returns:
            bool: True if upload succeeded
        """
        pass


class QueryHandler(Handler):
    """
    Base class for executing queries against a database.
    """
    
    def getById(self, entity_id: str):
        """
        Return an entity by identifier.

        Args:
            entity_id (str): Entity identifier

        Returns:
            DataFrame: Entity data or an empty DataFrame
        """
        # Default implementation - should be overridden in subclasses
        from pandas import DataFrame
        return DataFrame()
