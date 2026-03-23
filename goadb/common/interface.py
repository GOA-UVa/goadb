from typing import Union, Callable
from abc import abstractmethod, ABC

import pandas as pd


class IDataBase(ABC):
    """Representation of a database. Abstract interface."""

    @abstractmethod
    def run_query(self, query: str = "") -> Union[pd.DataFrame, int, None]:
        """Runs a query for the current database.
        WARNING: Some implementations only allow SELECT queries.

        Runs the given query for the database configured in the DataBase object.
        It understands (replaces) "'None'" as "NULL".

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

    @abstractmethod
    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        ignore: bool = False,
        callback_iter: Callable = None,
        chunk_size: int = 4000,
    ) -> int:
        """Inserts the given DataFrame into the DB in the table `table`.

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame to be inserted into the database. The index column is ignored,
            so reset the index beforehand if it's an SQL column that is expected to be inserted.
        table: str
            Name of the table that will contain the dataframe rows.
        callback_iter: Callable
            If defined, callback that will be called with the accumulated number of rows sent
            to the database. The callback will be called each time a new chunk insertion is
            performed.
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
        None or int
            Number of rows affected (number of insertions). In some implementations this value
            might not be entirely accurate.
        """

