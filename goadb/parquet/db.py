from typing import Union, Callable, List, Tuple, Dict
import os
import itertools
from threading import Lock
import re
import glob
import shutil
import uuid
import sys
import ctypes
import ctypes.util
import errno
import time
import random
from contextlib import ExitStack
import logging

import sqlglot
import pandas as pd
import redis
import redis.exceptions
import sqlparse
import dask.dataframe as dd
import portalocker
import duckdb

from .models import ParquetDBConfig
from ..common.interface import IDataBase



_REDISLOCK_TIMEOUT = 600  # seconds


class _ValKeyLock(portalocker.RedisLock):
    """Lock synchronized with the ValKey server.
    A connection is established and then the Lock is created
    """

    def __init__(self, channel: str, host: str, port: int):
        """Inits a _ValkeyLock for a server at "`host`:`port`", for the ID `channel`

        It will be synchronized with any other _ValkeyLock created in any machine
        for the same host, port and channel.

        Parameters
        ----------
        channel: str
            ValKey channel used for this Lock.
        host: str
            ValKey server host.
        port: int
            ValKey server port.
        """
        conn = redis.Redis(host, port, health_check_interval=10)
        super().__init__(channel, conn, _REDISLOCK_TIMEOUT)

    def acquire(
        self,
        timeout: Union[float, None] = None,
        check_interval: Union[float, None] = None,
        fail_when_locked: Union[bool, None] = None,
    ):
        try:
            return super().acquire(timeout, check_interval, fail_when_locked)
        except portalocker.AlreadyLocked as e:
            raise TimeoutError(
                f"Timed out when trying to acquire lock for channel {self.channel}"
            ) from e


class _FRMLock:
    """Lock that will be acquired everytime any IDataBase access a table.
    In case `valkey_host` is specified, it will be a _Valkey_Lock underneath.
    Otherwise it will be a simple Lock that would act as a mock.

    Attributes
    ----------
    valkey_fail: bool
        Flag that will propagate the raised error in case the object underneath lock
        fails, or that otherwise will ignore the error if the flag is False.
    lock: _ValkeyLock | Lock
        Lock that will be operated each time _FRMLock is operated.
    """

    def __init__(
        self,
        channel: str,
        valkey_host: Union[str, None],
        valkey_port: int,
        valkey_fail: bool,
    ):
        """Init a _FRMLock with a _ValKeyLock if `valkey_host` specified, or a regular
        Lock used as a mock if not.

        Parameters
        ----------
        channel: str
            If using a ValKey lock, ValKey channel used for the Lock.
        valkey_host: str | None
            ValKey server host, or None if not using a ValKey lock.
        valkey_port: int
            ValKey server port.
        valkey_fail: bool
            Boolean flag, if using a valkey server and it's not found or the connection fails,
            should the process fail (True) or not (False). By default it's True.
        """
        self.valkey_fail = valkey_fail
        if valkey_host is None:
            self.lock = Lock()
        else:
            self.lock = _ValKeyLock(channel, valkey_host, valkey_port)

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        return self.release()

    def acquire(self):
        try:
            return self.lock.acquire()
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log = logging.getLogger(__name__)
            log.warning(f"Could not create _ValKeyLock: {e}")
            return True

    def release(self):
        try:
            return self.lock.release()
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log = logging.getLogger(__name__)
            log.warning(f"Could not create _ValKeyLock: {e}")
            return True


class _ValkeySameTypeManyLock:
    """
    Distributed lock (using Redis/Valkey) that allows:
      - Many concurrent readers together, OR
      - Many concurrent writers together,
    but never readers and writers at the same time.

    Implementation:
      - MODE key stores which mode is currently active: "R" or "W"
      - HOLDERS set stores the tokens of all current participants
    """

    def __init__(
        self,
        host: str,
        port: int,
        key_prefix: str,
        want_mode: str,
        backoff_range=(0.01, 0.05),
    ):
        """
        Parameters
        ----------
        host: str
            ValKey server host.
        port: int
            ValKey server port.
        key_prefix : str
            Prefix for lock keys (e.g., "lock:table:mytable")
        want_mode: str
            R or W, wanted mode of the lock user.
        backoff_range : tuple(float, float)
            Min/max sleep time (in seconds) between retries when the desired
            mode is currently blocked. A random delay is chosen in this range
            to reduce contention.
        """
        self.r = redis.Redis(host, port, health_check_interval=10)
        self.pfx = key_prefix
        self.backoff_range = backoff_range
        if want_mode not in ("R", "W"):
            raise ValueError("want_mode must be R or W")
        self.want_mode = want_mode
        self._token = None
        self._mode = None  # "R" or "W"

    def _k_mode(self):
        return f"{self.pfx}:MODE"

    def _k_holders(self):
        return f"{self.pfx}:HOLDERS"

    def _acquire(self, want_mode: str, timeout) -> bool:
        token = str(uuid.uuid4())
        t0 = time.time()
        while True:
            with self.r.pipeline() as p:
                try:
                    p.watch(self._k_mode(), self._k_holders())
                    mode = p.get(self._k_mode())
                    mode = mode.decode() if mode else None
                    holders_count = int(p.scard(self._k_holders()) or 0)
                    if holders_count == 0:
                        mode = None  # reset if nobody left
                    if mode is None or mode == want_mode:
                        p.multi()
                        if mode is None:
                            p.set(self._k_mode(), want_mode)
                        p.sadd(self._k_holders(), token)
                        p.execute()
                        self._token, self._mode = token, want_mode
                        return True
                    p.unwatch()
                except redis.WatchError:
                    pass  # state changed, retry
            if timeout is not None and (time.time() - t0) >= timeout:
                return False
            # sleep a little with jitter to avoid thundering herd
            time.sleep(random.uniform(*self.backoff_range))

    def acquire(self, timeout=None) -> bool:
        """Acquire the lock in desired mode (shared with other desireds)."""
        return self._acquire(self.want_mode, timeout)

    def release(self):
        """Release the lock. If last holder, clear MODE."""
        if not self._token:
            return
        tok = self._token
        self._token = None
        while True:
            with self.r.pipeline() as p:
                try:
                    p.watch(self._k_mode(), self._k_holders())
                    if not p.sismember(self._k_holders(), tok):
                        p.unwatch()
                        self._mode = None
                        return
                    holders_count = int(p.scard(self._k_holders()) or 0)
                    p.multi()
                    p.srem(self._k_holders(), tok)
                    if holders_count == 1:
                        p.delete(self._k_mode())
                    p.execute()
                    self._mode = None
                    return
                except redis.WatchError:
                    continue


class _FRMSameTypeManyLock:
    """Lock that will be acquired everytime any thread access a table. It allows multiple
    readers at the same time, and multiple writers at the same time, but not of both kinds.
    In case `valkey_host` is specified, it will be a _ValkeySameTypeManyLock underneath.
    Otherwise it will be a simple Lock that would act as a mock.

    Attributes
    ----------
    valkey_fail: bool
        Flag that will propagate the raised error in case the object underneath lock
        fails, or that otherwise will ignore the error if the flag is False.
    lock: _ValkeySameTypeManyLock | Lock
        Lock that will be operated each time _FRMSameTypeManyLock is operated.
    """

    def __init__(
        self,
        want_mode: str,
        channel: str,
        valkey_host: Union[str, None],
        valkey_port: int,
        valkey_fail: bool,
    ):
        """Init a _FRMSameTypeManyLock with a _ValkeySameTypeManyLock if `valkey_host`
        specified, or a regular Lock used as a mock if not.

        Parameters
        ----------
        want_mode: str
            R or W, wanted mode of the lock user.
        channel: str
            If using a ValKey lock, ValKey channel used for the Lock.
        valkey_host: str | None
            ValKey server host, or None if not using a ValKey lock.
        valkey_port: int
            ValKey server port.
        valkey_fail: bool
            Boolean flag, if using a valkey server and it's not found or the connection fails,
            should the process fail (True) or not (False). By default it's True.
        """
        self.valkey_fail = valkey_fail
        if valkey_host is None:
            self.lock = Lock()
        else:
            self.lock = _ValkeySameTypeManyLock(
                valkey_host, valkey_port, channel, want_mode
            )

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        return self.release()

    def acquire(self):
        try:
            return self.lock.acquire()
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log = logging.getLogger(__name__)
            log.warning(f"Could not create _ValkeySameTypeManyLock: {e}")
            return True

    def release(self):
        try:
            return self.lock.release()
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log = logging.getLogger(__name__)
            log.warning(f"Could not create _ValkeySameTypeManyLock: {e}")
            return True


def _get_all_parquet_paths(root: str):
    return glob.glob(f"{root}/**/*.parquet", recursive=True)


def _get_all_final_parquet_paths(root: str):
    parquet_files = [
        f
        for f in _get_all_parquet_paths(root)
        if "__old__" not in f and "__tmp__" not in f
    ]
    return parquet_files


def _linux_rename_exchange(src: str, dst: str) -> bool:
    """
    Tries renameat2(RENAME_EXCHANGE) on Linux. It returns True if the swap is done.
    If its not allowed it returns False, so the caller does the fallback.
    """
    if not sys.platform.startswith("linux"):
        return False
    libc_path = ctypes.util.find_library("c")
    if not libc_path:
        return False
    libc = ctypes.CDLL(libc_path, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError:
        return False  # kernel/libc without renameat2
    AT_FDCWD = -100
    RENAME_EXCHANGE = 0x2
    src_b = os.fsencode(src)
    dst_b = os.fsencode(dst)
    ret = renameat2(
        AT_FDCWD,
        ctypes.c_char_p(src_b),
        AT_FDCWD,
        ctypes.c_char_p(dst_b),
        ctypes.c_uint(RENAME_EXCHANGE),
    )
    if ret == 0:
        return True
    e = ctypes.get_errno()
    # Not supported by the FS or invalid flags
    if e in (errno.ENOSYS, errno.EINVAL, errno.EXDEV, errno.EPERM, errno.ENOTSUP):
        return False
    # Other errors
    raise OSError(e, os.strerror(e))


def _atomic_replace_dir(src_dir: str, dst_dir: str) -> None:
    os.makedirs(os.path.dirname(dst_dir), exist_ok=True)
    if os.path.exists(dst_dir):
        success = _linux_rename_exchange(src_dir, dst_dir)
        old = src_dir
        if not success:
            old = f"{dst_dir}.__old__.{uuid.uuid4()}"
            os.replace(dst_dir, old)
            os.replace(src_dir, dst_dir)
            logging.getLogger(__name__).warning(
                "Linux renameat2 couldn't be carried out. Replaced non atomically."
            )
        shutil.rmtree(old, ignore_errors=True)
    else:
        os.replace(src_dir, dst_dir)


def _publish_single_leaf(tmp_root: str, dst_root: str) -> None:
    leafs = {os.path.dirname(p) for p in _get_all_parquet_paths(tmp_root)}
    if len(leafs) > 1:
        raise RuntimeError(f"Expected 1 leaf, found {len(leafs)}: {leafs}")
    if len(leafs) != 0:
        src_part = next(iter(leafs))
        dst_part = src_part.replace(tmp_root, dst_root, 1)
        _atomic_replace_dir(src_part, dst_part)
    shutil.rmtree(tmp_root, ignore_errors=True)


def get_primary_keys_from_sql(path: str) -> Dict[str, List[str]]:
        with open(path, encoding="utf-8") as fp:
            lines = fp.readlines()
        lines = [l for l in [li.strip() for li in lines] if not l.startswith("--")]
        content = " ".join(lines)
        sp = sqlparse.split(content)
        prim_lines = [
            s for s in sp if s.startswith("ALTER TABLE") and "ADD PRIMARY KEY" in s
        ]
        values = [p.split("`")[1::2] for p in prim_lines]
        tables_pks = {v[0]: v[1:] for v in values}
        return tables_pks


def _sanitize_redis_key(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_-]', '_', s)


class ParquetDatabase(IDataBase):
    """Representation of a parquet database using duckdb, pandas, dask and pyarrow.

    Attributes
    ----------
    dirpath: str
        Path of the directory where the parquet files are stored.
    valkey_host: str
        Valkey server hostname for reading and writing in parallel.
    valkey_port: int
        Valkey server port. Default is 6379.
    valkey_fail: bool
        Boolean flag, if using a valkey server and it's not found, should the process
        fail (True) or not (False). By default it's True.
    indices: dict[str, list[str]]
        Mapper for table names and its primary keys.
        {table1: [pk1, pk2, ...], ...}
    """

    def __init__(self, config: ParquetDBConfig):
        """
        Create the a connection with a parquet database with the configuration `config`

        Parameters
        ----------
        config: ParquetDBConfig
            DataBase configuration.
        """
        self.dirpath = config.parquet_dir
        self.valkey_host = config.valkey_host
        self.valkey_port = config.valkey_port
        self.valkey_fail = config.valkey_fail
        self.name = _sanitize_redis_key(config.database)
        self.partition_schema = config.partition_schema
        self.derived_parts = config.derived_parts
        self.indices = config.primary_keys
        os.makedirs(self.dirpath, exist_ok=True)

    def _get_partition_schema(self, table: str) -> List[str]:
        return self.partition_schema.get(table, [])

    def _read_parquet(self, path: str, table: str, filters=None):
        ddf = dd.read_parquet(path, filters=filters)
        parts = self._get_partition_schema(table)
        for part in parts:
            ddf[part] = ddf[part].astype("str")
        return ddf

    def _get_redislock(
        self, table: str, filters: List[Tuple[str, str, str]] = []
    ) -> _FRMLock:
        log = logging.getLogger(__name__)
        channel = f"lock_{self.name}_db_{table}"
        if filters:
            channel += "__" + "__".join([f"{f[0]}_{f[1]}_{f[2]}" for f in filters])
        try:
            lock = _FRMLock(
                channel, self.valkey_host, self.valkey_port, self.valkey_fail
            )
            log.debug(f"Created lock with valkey server for {channel}")
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log.warning(f"Could not create _ValKeyLock: {e}")
            return Lock()
        return lock

    def __get_sametypemany_redislock(
        self, table: str, want_mode: str
    ) -> _FRMSameTypeManyLock:
        log = logging.getLogger(__name__)
        channel = f"lock_sametypemany_{self.name}_db_{table}"
        try:
            lock = _FRMSameTypeManyLock(
                want_mode, channel, self.valkey_host, self.valkey_port, self.valkey_fail
            )
            log.debug(f"Created lock with valkey server for {channel}")
        except redis.exceptions.ConnectionError as e:
            if self.valkey_fail:
                raise e
            log.warning(f"Could not create _ValkeySameTypeManyLock: {e}")
            return Lock()
        return lock

    def _get_sametypemany_redislock_write(self, table: str) -> _FRMSameTypeManyLock:
        return self.__get_sametypemany_redislock(table, "W")

    def _get_sametypemany_redislock_read(self, table: str) -> _FRMSameTypeManyLock:
        return self.__get_sametypemany_redislock(table, "R")

    def _get_parquet_query_and_manylocks(
        self, query: str, is_read: bool = True
    ) -> Tuple[str, List[_FRMSameTypeManyLock]]:
        query = query.replace("'None'", "NULL").replace(";", "")
        parsed = sqlglot.parse_one(query, dialect="mysql")
        if parsed is None:
            return None, None
        locks = []
        class MissingTableException(Exception):
            pass
        def replace_table(node):
            if isinstance(node, sqlglot.exp.Table):
                table_name = node.name
                af = os.path.join(self.dirpath, f"{table_name}.parquet")
                if not os.path.exists(af):
                    raise MissingTableException(f'Missing table files for {node}')
                if os.path.isdir(af):
                    parquet_files = _get_all_final_parquet_paths(af)
                    if not parquet_files:
                        raise MissingTableException(f'Missing table files for {node}')
                    files_expr = ", ".join([f"'{file}'" for file in parquet_files])
                    files_expr = f"read_parquet([{files_expr}])"
                    read_node = sqlglot.parse_one(files_expr, dialect="duckdb")
                else:
                    read_node = sqlglot.parse_one(f"read_parquet('{af}')", dialect="duckdb")
                if node.alias:
                    read_node = read_node.as_(node.alias)
                return read_node
            return node
        try:
            transformed = parsed.transform(replace_table)
        except MissingTableException:
            return None, None
        tables = set(t.name for t in parsed.find_all(sqlglot.exp.Table))
        for tab in tables:
            if is_read:
                lock = self._get_sametypemany_redislock_read(tab)
            else:
                lock = self._get_sametypemany_redislock_write(tab)
            locks.append(lock)
        new_query = transformed.sql(dialect="duckdb")
        return new_query, locks


    def run_query(self, query: str = "") -> pd.DataFrame:
        """Runs a query for the current database.

        WARNING: Only allows SELECT queries. Otherwise it will raise a NotImplementedError.

        Runs the given query for the database configured.
        It understands (replaces) "'None'" as "NULL".

        Parameters
        ----------
        query: str
            query to execute in the database.

        Returns
        -------
        pandas.DataFrame
            DataFrame with the result of the query, as it is a SELECT query.
        """
        log = logging.getLogger(__name__)
        log.debug(f"Running query: {query}")
        # ONLY FOR SELECT
        if not query.strip().upper().startswith("SELECT"):
            raise NotImplementedError(
                f'ParquetDatabase only allows SELECT queries. Query passed: "{query}".'
            )
        query, locks = self._get_parquet_query_and_manylocks(query)
        if not query:
            return pd.DataFrame([])
        with ExitStack() as stack:
            for lock in locks:
                stack.enter_context(lock)
            with duckdb.connect() as con:
                log.debug(f"Running query processed: {query}")
                ans = con.execute(query).df()
                for col in ans.select_dtypes(include=["datetime64"]).columns:
                    ans[col] = ans[col].astype("datetime64[ns]")
        log.debug(f"Finished running query: {query}")
        dropcols = [c for c in list(self.derived_parts) + ["__null_dask_index__"] if c in ans]
        if dropcols:
            ans = ans.drop(columns=dropcols)
        return ans

    def _insert_df_chunk(
        self, df: pd.DataFrame, table: str, filters: list, ignore: bool, parts: list
    ) -> int:
        inserts = len(df)  # inserts of new or replace
        if inserts == 0:
            return inserts
        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = pd.to_datetime(df[col]).astype("datetime64[ns]")
        df = dd.from_pandas(df, npartitions=1)
        filepath = os.path.join(self.dirpath, f"{table}.parquet")
        if os.path.exists(filepath):
            dfpre = self._read_parquet(filepath, table, filters)
            df = dd.concat([dfpre, df])
            keep = "first" if ignore else "last"
            df = df.drop_duplicates(self.indices[table], keep=keep)
            if ignore:
                inserts = len(df) - len(dfpre)
        if "__null_dask_index__" in df.columns:
            df = df.drop("__null_dask_index__", axis=1)
        tmp_partkey = "__".join(f"{f[0]}{f[1]}{f[2]}" for f in filters)
        tmp_partkey = re.sub(r"[^A-Za-z0-9_.=\-]", "_", tmp_partkey)
        tmp_filepath = f"{filepath}.{tmp_partkey}.__tmp__.{uuid.uuid4()}"
        df.reset_index(drop=True).to_parquet(
            tmp_filepath,
            engine="pyarrow",
            partition_on=parts,
            write_index=True,
            write_metadata_file=False,
            overwrite=False,
        )
        _publish_single_leaf(tmp_filepath, filepath)
        return inserts

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table: str,
        callback_iter: Callable = None,
        ignore: bool = False,
    ) -> int:
        """Inserts the given DataFrame into the DB in the table `table`.

        It applies some filters to the dataframe depending on the table, and inserts
        each chunk in the appropiate parquet directory.

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
            performed. A new chunk iteration is performed for each filtered dataframe chunk.
        ignore: bool
            Flag indicating if the insert should be an INSERT IGNORE (True) or
            a REPLACE INTO (False). By default it's False.
            - IGNORE: If the key/row exists, skip insertion.
            - REPLACE: If key/row exists, delete the match and insert again.

        Returns
        -------
        affected_rows: int
            Number of affected rows (insertions).
        """
        # indices shall be reseted beforehand
        log = logging.getLogger(__name__)
        log.debug(f"Inserting dataframe into: {table}")
        parts = self._get_partition_schema(table)
        for derp in self.derived_parts:
            if derp in parts:
                df[derp] = self.derived_parts[derp](df)
        filters = []
        for part in parts:
            filt_vals = list(df[part].unique())
            filters.append((part, "=", filt_vals))
        chunks = [tuple()]
        if filters:
            chunks = list(itertools.product(*[f[2] for f in filters]))
        inserts = 0
        rows_sent = 0
        manylock = self._get_sametypemany_redislock_write(table)
        with manylock:
            for chunk in chunks:
                if callback_iter:
                    callback_iter(rows_sent)
                ch_filters = [(f[0], "=", c) for f, c in zip(filters, chunk)]
                conds = " & ".join([f"({chf[0]} == '{chf[2]}')" for chf in ch_filters])
                dfi = df
                if conds:
                    dfi = df.query(conds)
                rows_sent += len(dfi)
                lock = self._get_redislock(table, ch_filters)
                with lock:
                    inserts += self._insert_df_chunk(dfi, table, ch_filters, ignore, parts)
        log.debug(f"Finished inserting dataframe into: {table}")
        return inserts
