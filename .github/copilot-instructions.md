# Copilot Instructions for Laba1

## Project Overview
This project processes journal metadata from CSV and JSON files, uploading them into two distinct databases: a graph database (Blazegraph) and a relational database (SQLite). It supports querying both databases simultaneously via specialized query engines.

## Architecture & Key Components
- **Data Sources:**
  - `data/doaj.csv`: Journal metadata for graph DB (DOAJ format, languages separated by `, `)
  - `data/scimago.json`: Category/area metadata for relational DB (Scimago format)
- **Databases:**
  - Graph: Blazegraph (SPARQL endpoint, e.g. `http://127.0.0.1:9999/blazegraph/sparql`)
  - Relational: SQLite (`relational.db`)
- **Core Modules:**
  - `implementations/models.py`: Data model classes (see UML diagrams in `img/`)
  - `implementations/handlers.py`, `upload_handlers.py`: Database upload logic
  - `implementations/query_handlers.py`, `query_engines.py`: Query logic for both DBs
  - `implementations/impl.py`: Main class implementations (import here for most workflows)
- **Testing:**
  - `tests/test.py`: Main compliance test (uses `unittest`). Must pass before submission.

## Developer Workflows
- **Run Blazegraph:**
  - Start with: `java -server -Xmx1g -jar blazegraph.jar`
- **Upload Data:**
  - Use `UploadHandler` subclasses to push data to DBs. Example:
    ```python
    from impl import CategoryUploadHandler, JournalUploadHandler
    cat = CategoryUploadHandler(); cat.setDbPathOrUrl('relational.db'); cat.pushDataToDb('data/scimago.json')
    jou = JournalUploadHandler(); jou.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql'); jou.pushDataToDb('data/doaj.csv')
    ```
- **Query Data:**
  - Use `QueryHandler` subclasses and `FullQueryEngine` for mashup queries. Example:
    ```python
    from impl import FullQueryEngine, CategoryQueryHandler, JournalQueryHandler
    que = FullQueryEngine()
    que.addCategoryHandler(CategoryQueryHandler())
    que.addJournalHandler(JournalQueryHandler())
    result = que.getAllJournals()
    ```
- **Run Tests:**
  - Edit imports/paths in `tests/test.py` as needed (lines 18-21, 32-35)
  - Run: `python -m unittest test`

## Project-Specific Patterns & Conventions
- **Class Methods:**
  - All *get* and *has* methods from UML diagrams must be implemented as specified
  - Constructors for UML classes take no parameters unless otherwise noted
- **Multiple Query Handlers:**
  - Engines support multiple handlers for scalability and redundancy (see FAQ in README)
- **DataFrame Returns:**
  - Query methods typically return pandas DataFrames with all relevant attributes
- **Language Field:**
  - Multiple languages in CSV are separated by `, `

## Integration Points
- **Blazegraph:** Must be running for graph DB operations
- **SQLite:** Local file-based relational DB
- **Exemplar Data:** Follows provided structure; user data must match exemplar formats

## References
- See `README.md` for UML diagrams, workflow, and FAQ
- See `img/` for architecture and class diagrams
- See `tests/test.py` for compliance requirements

---

If any section is unclear or missing, please provide feedback for further refinement.
