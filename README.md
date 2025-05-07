# goadb

`goadb` is a lightweight abstraction layer over `SQLAlchemy` for working with relational databases.
It simplifies querying while allowing flexibility in the underlying database drivers.

## Example usage

```python
from datetime import datetime
import pandas as pd
from goadb.sql import DataBase, DBConfig

_SQL_DT = "%Y-%m-%d %H:%M:%S"

def read_measurement_dataframe(
    config: DBConfig, station: str, first: datetime, last: datetime
) -> pd.DataFrame:
    db = DataBase(config)
    df = db.run_query(
        (
            f"SELECT * FROM measurement WHERE station = '{station}' AND "
            f"fecha_lectura BETWEEN '{first.strftime(_SQL_DT)}' AND '{last.strftime(_SQL_DT)}'"
        )
    )
    return df

def read_site_dataframe(config: DBConfig, station: str) -> pd.DataFrame:
    db = DataBase(config)
    df = db.run_query(f"SELECT * FROM site WHERE station = '{station}'")
    return df
```

## Installation notes

`goadb` does not bundle a default MySQL driver. To use it, you must install one of the supported
drivers manually.

The following ones are valid, but their specs and licenses differ:
- pymysql: pymysql (MIT)
- mariadb: mariadb (LGPL)
- MySQLdb: mysqlclient (GPL)
- mysql.connector: mysql-connector-python (GPL)

To install one, for example:
```sh
pip install pymysql
```

Or:
```sh
pip install goadb[mysql]
```

### License Awareness
Some drivers, like `mysqlclient` and `mysql-connector-python`, are licensed under GPL,
which may affect the license of your overall application.

Use `pymysql` or `mariadb` for more permissive alternatives (MIT/LGPL).
