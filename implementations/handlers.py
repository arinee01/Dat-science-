# -*- coding: utf-8 -*-
"""
Базовые классы обработчиков для работы с базами данных.
Содержит классы: Handler, UploadHandler, QueryHandler
"""

from abc import ABC, abstractmethod
from typing import Optional


class Handler:
    """
    Базовый класс для работы с базами данных.
    """
    
    def __init__(self):
        self._dbPathOrUrl: str = ""
    
    def getDbPathOrUrl(self) -> str:
        """
        Возвращает путь или URL базы данных.
        
        Returns:
            str: Путь или URL базы данных
        """
        return self._dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        """
        Устанавливает путь или URL базы данных.
        
        Args:
            pathOrUrl (str): Путь или URL базы данных
            
        Returns:
            bool: True если установка прошла успешно
        """
        try:
            self._dbPathOrUrl = pathOrUrl
            return True
        except Exception:
            return False


class UploadHandler(Handler):
    """
    Абстрактный класс для загрузки данных в базу данных.
    """
    
    @abstractmethod
    def pushDataToDb(self, path: str) -> bool:
        """
        Загружает данные из файла в базу данных.
        
        Args:
            path (str): Путь к файлу с данными
            
        Returns:
            bool: True если загрузка прошла успешно
        """
        pass


class QueryHandler(Handler):
    """
    Базовый класс для выполнения запросов к базе данных.
    """
    
    def getById(self, entity_id: str):
        """
        Возвращает сущность по идентификатору.
        
        Args:
            entity_id (str): Идентификатор сущности
            
        Returns:
            DataFrame: Данные сущности или пустой DataFrame
        """
        # Базовая реализация - должна быть переопределена в подклассах
        from pandas import DataFrame
        return DataFrame()
