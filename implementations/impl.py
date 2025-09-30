# -*- coding: utf-8 -*-
"""
Главный файл реализации системы анализа научных журналов.
Содержит импорты всех необходимых классов.
"""

# Импорты классов модели данных
from .models import IdentifiableEntity, Journal, Category, Area

# Импорты базовых обработчиков
from .handlers import Handler, UploadHandler, QueryHandler

# Импорты обработчиков загрузки
from .upload_handlers import JournalUploadHandler, CategoryUploadHandler

# Импорты обработчиков запросов
from .query_handlers import JournalQueryHandler, CategoryQueryHandler

# Импорты движков запросов
from .query_engines import BasicQueryEngine, FullQueryEngine

# Экспорт всех классов для использования в test.py
__all__ = [
    # Модель данных
    'IdentifiableEntity', 'Journal', 'Category', 'Area',
    
    # Базовые обработчики
    'Handler', 'UploadHandler', 'QueryHandler',
    
    # Обработчики загрузки
    'JournalUploadHandler', 'CategoryUploadHandler',
    
    # Обработчики запросов
    'JournalQueryHandler', 'CategoryQueryHandler',
    
    # Движки запросов
    'BasicQueryEngine', 'FullQueryEngine'
]
