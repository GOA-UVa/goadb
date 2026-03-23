"""Unit tests for goadb.common.interface module."""
import pytest
from abc import ABC

from goadb.common.interface import IDataBase


class TestIDataBase:
    """Test IDataBase abstract class."""

    def test_idatabase_is_abstract(self):
        assert issubclass(IDataBase, ABC)

    def test_idatabase_has_abstract_methods(self):
        # Check that the abstract methods are defined
        assert hasattr(IDataBase, 'run_query')
        assert hasattr(IDataBase, 'insert_dataframe')

        # Try to instantiate should raise TypeError
        with pytest.raises(TypeError):
            IDataBase()
