# -*- coding: utf-8 -*-
"""
Main implementation module for the scientific journals analysis system.
Contains imports of all necessary classes.
"""

# Data model class imports
from .models import IdentifiableEntity, Journal, Category, Area

# Base handler imports
from .handlers import Handler, UploadHandler, QueryHandler

# Upload handler imports
from .upload_handlers import JournalUploadHandler, CategoryUploadHandler

# Query handler imports
from .query_handlers import JournalQueryHandler, CategoryQueryHandler

# Query engine imports
from .query_engines import BasicQueryEngine, FullQueryEngine

# Export all classes for use in test.py
__all__ = [
    # Data model
    'IdentifiableEntity', 'Journal', 'Category', 'Area',
    
    # Base handlers
    'Handler', 'UploadHandler', 'QueryHandler',
    
    # Upload handlers
    'JournalUploadHandler', 'CategoryUploadHandler',
    
    # Query handlers
    'JournalQueryHandler', 'CategoryQueryHandler',
    
    # Query engines
    'BasicQueryEngine', 'FullQueryEngine'
]
