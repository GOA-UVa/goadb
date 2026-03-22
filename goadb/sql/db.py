"""Access the MySQL database."""
from typing import Union, List, Callable
from urllib.parse import quote_plus
import importlib.util
import logging

import sqlalchemy
import sqlalchemy.exc
import pandas as pd

from goadb.sql.models import DBConfig
from goadb.common.interface import IDataBase


_MYSQL_DRIVERS= [
    (["MySQLdb"], "mysqldb"), # GPL - mysqlclient
    (["mysql", "mysql.connector"], "mysqlconnector"), # GPL - Oracle
    (["mariadb"], "mariadb"), # LGPL
    (["pymysql"], "pymysql"), # MIT
]

def _has_modules(modules: List[str]):
    for mod in modules:
        if not importlib.util.find_spec(mod):
            return False
    return True

def _get_mysql_driver() -> str:
    for module_names, sqlalchemy_driver in _MYSQL_DRIVERS:
        if _has_modules(module_names):
            return sqlalchemy_driver
    raise ImportError((
        "No MySQL driver found. Install one of: pymysql, mariadb, "
        "mysqlclient, or mysql-connector-python."
    ))

def _get_sql_engine(cf: DBConfig) -> sqlalchemy.Engine:
    """Creates a sqlalchemy engine to connect to the Database.

    Parameters
    ----------
    cf: DBConfig
        configuration data to create the connection to the database

    Returns
    -------
    sqlalchemy.Engine
        Generated sqlalchemy engine.
    """
    # in case that the password contains '@' or weird characters:
    password = quote_plus(cf.password)
    driver = _get_mysql_driver()
    url = f"mysql+{driver}://{cf.username}:{password}@{cf.host}/{cf.database}?ssl_mode=REQUIRED"
    return sqlalchemy.create_engine(url)


class DataBase(IDataBase):
    """Representation of a mysql database using sqlalchemy logic down below.

    Attributes
    ----------
    config: DBConfig
        DataBase configuration.
    """

    def __init__(self, config: DBConfig):
        """
        Create the a connection with database `dbname` with the configuration `config`

        Parameters
        ----------
        config: DBConfig
            DataBase configuration.
        """
        self._engine = None
        self.config = config
        self._update_engine_new()

    def _update_engine_new(self) -> None:
        self._engine = _get_sql_engine(self.config)

    def _get_engine(self) -> sqlalchemy.Engine:
        """Obtain the database associated sqlalchemy.Engine.

        Returns
        -------
        sqlalchemy.Engine
            Working engine associated to the database.
        """
        if self._engine is None:
            self._update_engine_new()
        return self._engine

    def run_query(self, query: str = "") -> Union[pd.DataFrame, int, None]:
        """Runs a query for the current database.

        Runs the given query for the database configured in the DataBase object.

        Parameters
        ----------
        query: str
            query to execute in the database.

        Returns
        -------
        pandas.DataFrame | int | None
            DataFrame with the result of the query if it is a SELECT query,
            integer with the amount of modified rows otherwise.
        """
        log = logging.getLogger(__name__)
        engine = self._get_engine()
        if query.strip().upper().startswith("SELECT"):
            data = pd.read_sql(query, engine)
        else:
            data = None
            try:
                with engine.connect() as conn:
                    res = conn.execute(sqlalchemy.text(query))
                    conn.commit()  # write effectively in the database
                    data = res.rowcount
            except sqlalchemy.exc.IntegrityError as err:
                log.error(f"Duplicate Entry: {err}")
            except sqlalchemy.exc.InterfaceError as err:
                log.error(f"Error: {err} on query: {query}. (Error: {err})")
        return data

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        ignore: bool = True,
        callback_iter: Callable = None,
        chunk_size: int = 4000,
    ) -> int:
        """Inserts the given DataFrame into the DB in the table `table`.

        It will do it in multiple commits of size `chunk_size`.

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame to be inserted into the database. The index column is ignored,
            so reset the index beforehand if it's an SQL column that is expected to be inserted.
        table: str
            Name of the table that will contain the dataframe rows.
        ignore: bool
            Behaviour in case of colition. Ignore or update. By default ignore.
        callback_iter: Callable
            If defined, callback that will be called with the accumulated number of rows sent
            to the database. The callback will be called each time a new chunk insertion is
            performed.
        chunk_size: int
            Chunk insertion size. By default it's 4000.

        Returns
        -------
        affected_rows: int
            Number of affected rows (insertions).
            The number of returned rows affected is the sum of the rowcount attribute of SQLAlchemy
            connectable which may not reflect the exact number of written rows as stipulated
            in SQLAlchemy.
        """
        def _get_inserter(ignore_inserter: bool) -> Callable:
            from sqlalchemy.dialects.mysql.dml import Insert

            def _inserter(
                other, conn: sqlalchemy.Connection, keys: list[str], data_iter
            ) -> int:
                data = [dict(zip(keys, row)) for row in data_iter]
                stmt = Insert(other.table).values(data)
                if ignore_inserter:
                    stmt = stmt.prefix_with("IGNORE", dialect="mysql")
                else:
                    stmt = stmt.on_duplicate_key_update({key: stmt.inserted[key] for key in keys})
                result = conn.execute(stmt)
                return result.rowcount
            return _inserter

        if df.empty:
            return 0
        engine = self._get_engine()
        inserts = 0
        step = int(chunk_size)
        for i in range(0, len(df), step):
            if callback_iter:
                callback_iter(i)
            inserts += df.iloc[i : i + step].to_sql(
                table,
                engine,
                if_exists="append",
                index=False,
                method=_get_inserter(ignore),
            )
        return inserts


def insert_dataframe(df: pd.DataFrame, config: DBConfig, table: str, ignore: bool = True) -> int:
    """Insert the dataframe `df` in the table `table` in the database specified in `config`.
    
    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to be inserted into the database. The index column is ignored,
        so reset the index beforehand if it's an SQL column that is expected to be inserted.
    config: DBConfig
        Configuration used to connect to the database.
    table: str
        Name of the table that will contain the dataframe rows.
    ignore: bool
        Behaviour in case of colition. Ignore or update. By default ignore.

    Returns
    -------
    affected_rows: int
        Number of affected rows (insertions).
        The number of returned rows affected is the sum of the rowcount attribute of SQLAlchemy
        connectable which may not reflect the exact number of written rows as stipulated
        in SQLAlchemy.
    """
    db = DataBase(config)
    return db.insert_dataframe(df, table, ignore)
