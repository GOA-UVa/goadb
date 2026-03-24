from dataclasses import dataclass, field
from typing import Dict, List, Callable

import pandas as pd

@dataclass
class ParquetDBConfig:
    """
    All partitions must be str-type columns, and derived_parts must produce str-type columns.
    """
    parquet_dir: str
    database: str
    primary_keys: Dict[str, List[str]]
    partition_schema: Dict[str, List[str]]
    derived_parts: Dict[str, Callable[pd.DataFrame, pd.Series]] = field(default_factory=lambda: dict())
    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_fail: bool = True
