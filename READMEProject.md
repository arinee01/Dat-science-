# Data Science Project: Journal Analysis System

## 📋 Описание проекта

Система для анализа научных журналов, которая позволяет обрабатывать данные из разных источников и загружать их в две различные базы данных с возможностью одновременного запроса к ним.

### 🎯 Цель проекта
Разработать программное обеспечение для обработки данных научных журналов из:
- **DOAJ** (Directory of Open Access Journals) - метаданные журналов
- **Scimago Journal Rank** - категории и области журналов

## 🏗️ Архитектура системы

### Базы данных
- **Blazegraph** (графовая БД) - для журналов из DOAJ
- **SQLite** (реляционная БД) - для категорий и областей из Scimago

### Модель данных
- `IdentifiableEntity` - базовый класс с идентификатором
- `Journal` - журналы с метаданными (ISSN, название, издатель, лицензия, APC, DOAJ Seal)
- `Category` - категории с квартилями (Q1, Q2, Q3, Q4)
- `Area` - области знаний

## 📁 Структура проекта

```
project/
├── data/                          # Данные
│   ├── doaj.csv                   # Журналы из DOAJ (21,309 записей)
│   └── scimago.json               # Категории из Scimago (592,512 записей)
├── img/                           # Диаграммы UML
│   ├── classes-uml.png            # Диаграмма классов обработчиков
│   ├── datamodel-uml.png          # Диаграмма модели данных
│   ├── datamodel.png              # Расширенная диаграмма с методами
│   └── workflow.png               # Диаграмма рабочего процесса
├── implementations/               # Реализация системы
│   ├── models.py                  # Классы модели данных
│   ├── handlers.py                # Базовые обработчики
│   ├── upload_handlers.py         # Обработчики загрузки данных
│   ├── query_handlers.py          # Обработчики запросов
│   ├── query_engines.py           # Движки запросов
│   └── impl.py                    # Главный файл импортов
├── tests/                         # Тесты и скрипты
│   ├── test.py                    # Основные тесты unittest
│   ├── view_data.py               # Просмотр данных Blazegraph
│   ├── view_sqlite.py             # Просмотр данных SQLite
│   └── sql_queries.py             # Примеры SQL запросов
├── README.md                      # Оригинальная документация
├── relational.db                  # SQLite база данных
└── exemplar_execution.py          # Пример использования
```

## 🚀 Установка и настройка

### Требования
- Python 3.7+
- Blazegraph (запущен на http://localhost:8889/bigdata/sparql)
- Библиотеки: pandas, requests, sqlite3

### Запуск Blazegraph
```bash
java -server -Xmx1g -jar blazegraph.jar
```

## 📊 Данные

### DOAJ CSV (doaj.csv)
Содержит метаданные журналов:
- Journal title - название журнала
- Journal ISSN (print version) - ISSN печатной версии
- Journal EISSN (online version) - EISSN онлайн версии
- Languages - языки (разделены ", ")
- Publisher - издатель
- DOAJ Seal - наличие DOAJ Seal (Yes/No)
- Journal license - лицензия
- APC - Article Processing Charge (Yes/No)

### Scimago JSON (scimago.json)
Содержит категории и области:
- identifiers - массив ISSN журналов
- categories - массив категорий с квартилями
- areas - массив областей знаний

## 🔧 Использование

### Запуск тестов
```bash
cd tests
python -m unittest test -vpython
```

### Просмотр данных Blazegraph
```bash
cd tests
python view_data.py
```

### Просмотр данных SQLite
```bash
cd tests
python view_sqlite.py   
```

### SQL запросы
```bash
cd tests
python sql_queries.py
```

### Использование в коде
```python
from implementations.impl import JournalQueryHandler, CategoryQueryHandler, FullQueryEngine

# Создание обработчиков
journal_handler = JournalQueryHandler()
journal_handler.setDbPathOrUrl("http://localhost:8889/bigdata/sparql")

category_handler = CategoryQueryHandler()
category_handler.setDbPathOrUrl("relational.db")

# Создание движка запросов
engine = FullQueryEngine()
engine.addJournalHandler(journal_handler)
engine.addCategoryHandler(category_handler)

# Выполнение запросов
journals = engine.getAllJournals()
categories = engine.getAllCategories()
```

## 🌐 Веб-интерфейс Blazegraph

### Доступ к интерфейсу
- **Главная страница**: http://localhost:8889/bigdata
- **SPARQL запросы**: http://localhost:8889/bigdata/sparql

### Примеры SPARQL запросов

#### Все журналы
```sparql
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
ORDER BY ?title
LIMIT 20
```

#### Статистика по лицензиям
```sparql
PREFIX doaj: <http://doaj.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?licence (COUNT(?journal) as ?count)
WHERE {
    ?journal rdf:type doaj:Journal .
    ?journal doaj:licence ?licence .
}
GROUP BY ?licence
ORDER BY DESC(?count)
```

#### Поиск журналов по названию
```sparql
PREFIX doaj: <http://doaj.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?journal ?title ?issn ?publisher
WHERE {
    ?journal rdf:type doaj:Journal .
    ?journal doaj:title ?title .
    FILTER (CONTAINS(LCASE(?title), "journal"))
    OPTIONAL { ?journal doaj:issn ?issn }
    OPTIONAL { ?journal doaj:publisher ?publisher }
}
ORDER BY ?title
LIMIT 10
```

## 📈 Статистика данных

### Blazegraph (журналы)
- **Загружено журналов**: 100 (ограничение для тестирования)
- **Уникальных записей**: 172
- **Популярные лицензии**:
  - CC BY: 42 журнала
  - CC BY-NC: 18 журналов
  - CC BY-NC-ND: 17 журналов

### SQLite (категории и области)
- **Областей**: 27
- **Категорий**: 310 (308 с квартилем Q1, 2 с Q2)
- **Связей журнал-категория**: 111,589
- **Связей журнал-область**: 76,907

#### Топ-10 категорий по количеству журналов:
1. Medicine (miscellaneous) - 4,232 журнала
2. History - 2,604 журнала
3. Education - 2,395 журналов
4. Sociology and Political Science - 2,282 журнала
5. Cultural Studies - 1,989 журналов

#### Топ-10 областей по количеству журналов:
1. Social Sciences - 12,849 журналов
2. Medicine - 12,402 журнала
3. Arts and Humanities - 7,236 журналов
4. Engineering - 4,588 журналов
5. Agricultural and Biological Sciences - 3,851 журнал

## 🔍 Основные классы

### Модель данных
- **IdentifiableEntity**: Базовый класс с методами `getIds()`, `addId()`, `setId()`
- **Journal**: Журнал с методами `getTitle()`, `getLanguages()`, `hasAPC()`, `hasDOASeal()`, `getCategories()`, `getAreas()`
- **Category**: Категория с методом `getQuartile()`
- **Area**: Область знаний

### Обработчики
- **Handler**: Базовый класс с `getDbPathOrUrl()`, `setDbPathOrUrl()`
- **UploadHandler**: Абстрактный класс для загрузки данных
- **QueryHandler**: Базовый класс для запросов с методом `getById()`

### Специализированные обработчики
- **JournalUploadHandler**: Загрузка CSV в Blazegraph
- **CategoryUploadHandler**: Загрузка JSON в SQLite
- **JournalQueryHandler**: Запросы к журналам в Blazegraph
- **CategoryQueryHandler**: Запросы к категориям в SQLite

### Движки запросов
- **BasicQueryEngine**: Базовые запросы к обеим БД
- **FullQueryEngine**: Сложные mashup запросы

## 🧪 Тестирование

### Основные тесты (test.py)
- `test_01_JournalUploadHandler` - тест загрузки журналов
- `test_02_CategoryUploadHandler` - тест загрузки категорий
- `test_03_JournalQueryHandler` - тест запросов к журналам
- `test_04_ProcessDataQueryHandler` - тест запросов к категориям
- `test_05_FullQueryEngine` - тест движка запросов

### Результаты тестирования
```
Ran 5 tests in 2.781s
OK
```

## 🔗 Связи между данными

Журналы связываются с категориями и областями через ISSN:
- В JSON файле Scimago есть поле `identifiers` с ISSN журналов
- Эти ISSN соответствуют ISSN в CSV файле DOAJ
- Связи хранятся в таблицах `journal_categories` и `journal_areas`

## 🎯 Mashup запросы

### Примеры сложных запросов
- **Журналы в определенных категориях с квартилями**
- **Журналы в областях с определенными лицензиями**
- **"Алмазные" журналы** (без APC) в областях и категориях с квартилями

## 🛠️ Технические детали

### RDF namespace
- **Префикс**: `http://doaj.org/`
- **Типы**: `doaj:Journal`
- **Свойства**: `doaj:title`, `doaj:issn`, `doaj:publisher`, `doaj:licence`, `doaj:hasAPC`, `doaj:hasDOAJSeal`

### SQLite схема
- **areas**: id (TEXT PRIMARY KEY)
- **categories**: id (TEXT PRIMARY KEY), quartile (TEXT)
- **journal_categories**: issn (TEXT), category_id (TEXT), quartile (TEXT)
- **journal_areas**: issn (TEXT), area_id (TEXT)

### Обработка ошибок
- Система сообщает об ошибках и продолжает работу
- Возвращает пустые результаты при ошибках
- Логирует процесс загрузки данных

## 📝 Лицензия

Проект разработан в рамках курса Data Science. Все права защищены.

## 👥 Автор

Реализация выполнена в соответствии с техническим заданием и UML диаграммами.

---

**Статус проекта**: ✅ Завершен и протестирован  
**Последнее обновление**: 2024  
**Версия**: 1.0
