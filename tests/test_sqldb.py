"""Unit tests for goadb.sql.db module."""
import pytest
from unittest.mock import patch, MagicMock
from urllib.parse import quote_plus

import pandas as pd
import sqlalchemy

from goadb.sql.db import (
    _has_modules,
    _get_mysql_driver,
    _get_sql_engine,
    DataBase,
    insert_dataframe,
)
from goadb.sql.models import DBConfig


class TestHasModules:
    """Test _has_modules function."""

    @patch('importlib.util.find_spec')
    def test_has_modules_all_present(self, mock_find_spec):
        mock_find_spec.return_value = True
        assert _has_modules(['mod1', 'mod2']) is True

    @patch('importlib.util.find_spec')
    def test_has_modules_one_missing(self, mock_find_spec):
        mock_find_spec.side_effect = [True, None]
        assert _has_modules(['mod1', 'mod2']) is False


class TestGetMySQLDriver:
    """Test _get_mysql_driver function."""

    @patch('goadb.sql.db._has_modules')
    def test_get_mysql_driver_first_available(self, mock_has_modules):
        mock_has_modules.side_effect = [False, True, False, False]
        assert _get_mysql_driver() == "mysqlconnector"

    @patch('goadb.sql.db._has_modules')
    def test_get_mysql_driver_none_available(self, mock_has_modules):
        mock_has_modules.return_value = False
        with pytest.raises(ImportError, match="No MySQL driver found"):
            _get_mysql_driver()


class TestGetSQLEngine:
    """Test _get_sql_engine function."""

    @patch('goadb.sql.db._get_mysql_driver')
    @patch('sqlalchemy.create_engine')
    def test_get_sql_engine(self, mock_create_engine, mock_get_driver):
        mock_get_driver.return_value = "pymysql"
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DBConfig(
            username="user",
            password="pass@word",
            host="localhost",
            database="testdb"
        )

        engine = _get_sql_engine(config)

        expected_url = f"mysql+pymysql://user:{quote_plus('pass@word')}@localhost/testdb?ssl_mode=REQUIRED"
        mock_create_engine.assert_called_once_with(expected_url)
        assert engine == mock_engine


class TestDataBase:
    """Test DataBase class."""

    def test_init(self):
        config = DBConfig(
            username="user",
            password="pass",
            host="host",
            database="db"
        )
        db = DataBase(config)
        assert db.config == config
        assert db._engine is not None

    @patch('goadb.sql.db._get_sql_engine')
    def test_get_engine(self, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine

        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        engine = db._get_engine()
        assert engine == mock_engine

    @patch('goadb.sql.db._get_sql_engine')
    @patch('pandas.read_sql')
    def test_run_query_select(self, mock_read_sql, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_df = pd.DataFrame({'col': [1, 2]})
        mock_read_sql.return_value = mock_df

        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        result = db.run_query("SELECT * FROM table")
        assert result.equals(mock_df)
        mock_read_sql.assert_called_once_with("SELECT * FROM table", mock_engine)

    @patch('goadb.sql.db._get_sql_engine')
    @patch('sqlalchemy.text')
    def test_run_query_insert(self, mock_text, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_res = MagicMock()
        mock_res.rowcount = 5
        mock_conn.execute.return_value = mock_res
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        result = db.run_query("INSERT INTO table VALUES (1)")
        assert result == 5
        mock_conn.commit.assert_called_once()

    @patch('goadb.sql.db._get_sql_engine')
    @patch('sqlalchemy.text')
    def test_run_query_none_replacement(self, mock_text, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_res = MagicMock()
        mock_res.rowcount = 1
        mock_conn.execute.return_value = mock_res
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        db.run_query("INSERT INTO table VALUES ('None')")
        mock_text.assert_called_once_with("INSERT INTO table VALUES (NULL)")
        mock_conn.execute.assert_called_once_with(mock_text.return_value)

    @patch('goadb.sql.db._get_sql_engine')
    @patch('pandas.DataFrame.to_sql')
    def test_insert_dataframe_empty(self, mock_to_sql, mock_get_engine):
        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        df = pd.DataFrame()
        result = db.insert_dataframe(df, "table")
        assert result == 0
        mock_to_sql.assert_not_called()

    @patch('goadb.sql.db._get_sql_engine')
    @patch('pandas.DataFrame.to_sql')
    def test_insert_dataframe_with_data(self, mock_to_sql, mock_get_engine):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_to_sql.return_value = 10

        config = DBConfig(username="u", password="p", host="h", database="d")
        db = DataBase(config)

        df = pd.DataFrame({'a': [1, 2, 3]})
        result = db.insert_dataframe(df, "table", ignore=True, chunk_size=2)
        assert result == 20  # 10 + 10 for two chunks
        assert mock_to_sql.call_count == 2


class TestInsertDataFrameFunction:
    """Test standalone insert_dataframe function."""

    @patch('goadb.sql.db.DataBase')
    def test_insert_dataframe_function(self, mock_db_class):
        mock_db_instance = MagicMock()
        mock_db_class.return_value = mock_db_instance
        mock_db_instance.insert_dataframe.return_value = 42

        config = DBConfig(username="u", password="p", host="h", database="d")
        df = pd.DataFrame({'col': [1]})

        result = insert_dataframe(df, config, "table", ignore=False)
        assert result == 42
        mock_db_class.assert_called_once_with(config)
        mock_db_instance.insert_dataframe.assert_called_once_with(df, "table", False)