# goadb

[![PyPI version](https://badge.fury.io/py/goadb.svg)](https://badge.fury.io/py/goadb)
[![Python versions](https://img.shields.io/pypi/pyversions/goadb.svg)](https://pypi.org/project/goadb/)
[![License](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0.html)

`goadb` is a lightweight abstraction layer over [SQLAlchemy](https://www.sqlalchemy.org/) designed to simplify
working with relational databases, especially MySQL. It provides a clean interface for running queries and
inserting `pandas` DataFrames, with built‑in support for batch inserts, and callbacks.

The library is database‑agnostic in theory, but currently focuses on MySQL.
It does not bundle any MySQL driver, you choose the one that fits your project's license and performance requirements.

## Features

- **Simple query execution** – Run arbitrary SQL statements and get back a `pandas.DataFrame` for `SELECT` queries or the number of affected rows for others.
- **DataFrame insertion** – Insert entire DataFrames into a table with configurable batch size and optional `IGNORE` or `ON DUPLICATE KEY UPDATE` semantics.
- **Progress callbacks** – Receive the number of rows processed after each batch insert.
- **Driver flexibility** – Works with any MySQL driver supported by SQLAlchemy (`pymysql`, `mariadb`, `mysqlclient`, `mysql-connector-python`).

## Installation

`goadb` is available on PyPI. Install it with:

```bash
pip install goadb
```

**Important:** `goadb` does not include a MySQL driver by default. You must install at least one of the supported drivers manually. For example:

```bash
pip install pymysql          # MIT license
# or
pip install mariadb          # LGPL
# or
pip install mysqlclient      # GPL
# or
pip install mysql-connector-python   # GPL
```

Alternatively, you can install a driver along with `goadb` using the extras:

```bash
pip install goadb[mysql]          # installs default for mysql (pymysql)
pip install goadb[pymysql]        # installs pymysql
pip install goadb[mariadb]        # installs mariadb
pip install goadb[mysqlclient]    # installs mysqlclient
pip install goadb[mysqlconnector] # installs mysql-connector-python
```

### License considerations

- `pymysql` (MIT) and `mariadb` (LGPL) are permissive and do not impose additional license restrictions on your project.
- `mysqlclient` and `mysql-connector-python` are licensed under the GPL. Using them may require your project to also be GPL‑compatible.

Choose the driver that best matches your project's license and deployment environment.

## Quick Start

### 1. Configure the database connection

```python
from goadb.sql import DBConfig, DataBase

config = DBConfig(
    username="your_user",
    password="your_password",
    host="localhost",
    database="your_db"
)
db = DataBase(config)
```

### 2. Run a SELECT query

```python
import pandas as pd
from datetime import datetime

_SQL_DT = "%Y-%m-%d %H:%M:%S"

def read_measurements(db: DataBase, station: str, first: datetime, last: datetime) -> pd.DataFrame:
    query = (
        f"SELECT * FROM measurement WHERE station = '{station}' AND "
        f"fecha_lectura BETWEEN '{first.strftime(_SQL_DT)}' AND '{last.strftime(_SQL_DT)}'"
    )
    return db.run_query(query)   # returns a DataFrame
```

### 3. Insert a DataFrame

```python
# Assuming we have a DataFrame 'df' with columns matching the target table
df = pd.DataFrame({
    "station": ["A", "B"],
    "value": [1.2, 3.4],
    "timestamp": ["2025-01-01 00:00:00", "2025-01-01 01:00:00"]
})

inserted = db.insert_dataframe(df, table="measurements")
print(f"Inserted {inserted} rows")
```

## Advanced Usage

### Batch inserts with a progress callback

The `insert_dataframe` method splits the DataFrame into chunks and commits each chunk separately. You can pass a callback to be notified after each chunk:

```python
def progress(rows_processed: int):
    print(f"{rows_processed} rows inserted so far")

db.insert_dataframe(df, table="measurements", callback_iter=progress, chunk_size=2000)
```

### Controlling duplicate behaviour

- **Ignore duplicates** (default): uses `INSERT IGNORE`.  
  ```python
  db.insert_dataframe(df, table="measurements", ignore=True)
  ```
- **Upsert** (update on duplicate key): uses `ON DUPLICATE KEY UPDATE`.  
  ```python
  db.insert_dataframe(df, table="measurements", ignore=False)
  ```

### Running non‑SELECT queries

`run_query` also works for `UPDATE`, `DELETE`, `INSERT`, etc. It returns the number of affected rows:

```python
rows_affected = db.run_query("UPDATE measurements SET value = 0 WHERE station = 'A'")
```

### Automatic `'None'` conversion

If your query contains the string `'None'`, it is automatically replaced by `NULL`. This is helpful when building queries with pandas that may produce `'None'` as a string representation of missing values.

## Development

To set up a development environment:

1. Clone the repository:
   ```bash
   git clone https://gitlab.com/goa-uva/goadb.git
   cd goadb
   ```

2. Install in editable mode with development dependencies:
   ```bash
   pip install -e .[dev]
   ```

3. Run tests:
   ```bash
   pytest
   ```

### Code style

We use `pre-commit` to enforce code quality. Install the hooks with:

```bash
pre-commit install
```

## License

`goadb` is released under the **GNU Lesser General Public License v3.0** (LGPL-3.0). See the [LICENSE](LICENSE) file for details.

Note that the license of the MySQL driver you choose may impose additional conditions. We recommend using `pymysql` (MIT) or `mariadb` (LGPL) for the most permissive combination.

## Links

- [Repository](https://gitlab.com/goa-uva/goadb)
- [Issues](https://gitlab.com/goa-uva/goadb/-/issues)
- [PyPI](https://pypi.org/project/goadb/)

---

**Maintainer:** Javier Gatón Herguedas – [gaton@goa.uva.es](mailto:gaton@goa.uva.es)
