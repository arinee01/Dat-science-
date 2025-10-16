# -*- coding: utf-8 -*-
"""
Utility script for inspecting data stored in Blazegraph.
"""

import requests
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from implementations.query_handlers import JournalQueryHandler

def view_blazegraph_data():
    """Inspect data stored in Blazegraph."""
    
    endpoint = "http://localhost:9999/bigdata/sparql"
    
    print("=== Inspecting data in Blazegraph ===\n")
    
    # 1. Check total number of journals
    count_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
    }
    """
    
    print("1. Total number of journals:")
    result = execute_sparql_query(endpoint, count_query)
    if result:
        print(f"   Journals found: {result[0]['count']}")
    
    # 2. Display first five journals
    sample_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT ?journal ?title ?issn ?publisher ?licence
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:title ?title .
        OPTIONAL { ?journal doaj:issn ?issn }
        OPTIONAL { ?journal doaj:publisher ?publisher }
        OPTIONAL { ?journal doaj:licence ?licence }
    }
    LIMIT 5
    """
    
    print("\n2. Sample journals:")
    result = execute_sparql_query(endpoint, sample_query)
    if result:
        for i, journal in enumerate(result, 1):
            print(f"   {i}. {journal.get('title', 'N/A')}")
            print(f"      ISSN: {journal.get('issn', 'N/A')}")
            print(f"      Publisher: {journal.get('publisher', 'N/A')}")
            print(f"      Licence: {journal.get('licence', 'N/A')}")
            print()
    
    # 3. Licence statistics
    license_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT ?licence (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:licence ?licence .
    }
    GROUP BY ?licence
    ORDER BY DESC(?count)
    """
    
    print("3. Licence statistics:")
    result = execute_sparql_query(endpoint, license_query)
    if result:
        for license_info in result:
            print(f"   {license_info['licence']}: {license_info['count']} journals")
    
    # 4. Journals with APC
    apc_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:hasAPC "true" .
    }
    """
    
    print("\n4. Journals with Article Processing Charge (APC):")
    result = execute_sparql_query(endpoint, apc_query)
    if result:
        print(f"   Journals with APC: {result[0]['count']}")
    
    # 5. Journals with DOAJ Seal
    seal_query = """
    PREFIX doaj: <http://doaj.org/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT (COUNT(?journal) as ?count)
    WHERE {
        ?journal rdf:type doaj:Journal .
        ?journal doaj:hasDOAJSeal "true" .
    }
    """
    
    print("\n5. Journals with DOAJ Seal:")
    result = execute_sparql_query(endpoint, seal_query)
    if result:
        print(f"   Journals with DOAJ Seal: {result[0]['count']}")

def execute_sparql_query(endpoint, query):
    """Execute a SPARQL query and return the result."""
    try:
        response = requests.get(
            endpoint,
            params={'query': query, 'format': 'json'}
        )
        
        if response.status_code == 200:
            data = response.json()
            bindings = data.get('results', {}).get('bindings', [])
            
            # Convert the result into a list of dictionaries
            result = []
            for binding in bindings:
                row = {}
                for var_name, var_value in binding.items():
                    row[var_name] = var_value.get('value', '')
                result.append(row)
            
            return result
        else:
            print(f"SPARQL query error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error while executing SPARQL query: {e}")
        return None

def test_query_handler():
    """Exercise the JournalQueryHandler."""
    print("\n=== Testing JournalQueryHandler ===\n")
    
    handler = JournalQueryHandler()
    handler.setDbPathOrUrl("http://localhost:8889/bigdata/sparql")
    
    # Fetch all journals
    print("Retrieving all journals via the handler:")
    df = handler.getAllJournals()
    print(f"   Journals retrieved: {len(df)}")
    
    if not df.empty:
        print("   First three journals:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"   {i+1}. {row.get('title', 'N/A')}")
    
    # Search by title
    print("\nLooking up journals containing 'Journal' in the title:")
    df = handler.getJournalsWithTitle("Journal")
    print(f"   Journals found: {len(df)}")
    
    if not df.empty:
        print("   First three results:")
        for i, (_, row) in enumerate(df.head(3).iterrows()):
            print(f"   {i+1}. {row.get('title', 'N/A')}")

if __name__ == "__main__":
    view_blazegraph_data()
    test_query_handler()
