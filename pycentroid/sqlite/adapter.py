from .dialect import SqliteDialect, SqliteFormatter
from pycentroid.query import QueryExpression, DataAdapter, DataTable, DataView, DataTableIndex
import sqlite3
import re
import time
from typing import Callable
from pycentroid.common import AnyObject
import logging


class SqliteTableIndex(DataTableIndex):

    def __init__(self, table, adapter):
        super().__init__(table, adapter)

    async def create(self, name: str, columns: list):
        await self.drop(name)
        sql = 'CREATE INDEX'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(name)
        sql += SqliteDialect.Space
        sql += 'ON'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(self.table)
        sql += '('
        sql += ','.join(map(lambda x: SqliteDialect().escape_name(x.name), columns))
        sql += ');'
        await self.__adapter__.execute(sql)

    async def exists(self, name: str):
        table = SqliteDialect().escape_name(self.table)
        results = await self.__adapter__.execute(f'PRAGMA INDEX_LIST({table})')
        return next(filter(lambda x: x.origin == 'c' and x.name == name, results), None) is not None

    async def drop(self, name: str):
        exists = await self.exists(name)
        if exists:
            index = SqliteDialect().escape_name(name)
            await self.__adapter__.execute(f'DROP INDEX {index}')

    async def list(self):
        table = SqliteDialect().escape_name(self.table)
        # get index list
        results = await self.__adapter__.execute(f'PRAGMA INDEX_LIST({table})')
        # prepare index list
        filtered = filter(lambda x: x.origin == 'c', results)
        # with name and columns
        indexes = list(map(lambda x: AnyObject(name=x.name, columns=[]), filtered))
        # enumerate indexes
        for index in indexes:
            # and get column list
            columns = await self.__adapter__.execute(f'PRAGMA INDEX_INFO({index.name})')
            index.columns = list(map(lambda x: AnyObject(name=x.name), columns))
        return indexes


class SqliteTable(DataTable):
    def __init__(self, table, adapter):
        super().__init__(table, adapter)

    async def create(self, fields: list):
        if len(fields) == 0:
            Exception('Field collection cannot be empty while creating a database table.')
        dialect = SqliteDialect()
        sql = 'CREATE TABLE'
        sql += SqliteDialect.Space
        sql += dialect.escape_name(self.table)
        sql += '('
        sql += ','.join(map(lambda x: dialect.format_type(name=x.name, type=x.type, nullable=x.nullable, size=x.size,
                                                          scale=x.scale), fields))
        sql += ')'
        return await self.__adapter__.execute(sql)

    async def change(self, fields: list):
        exists = await self.__adapter__.table(self.table).exists()
        if not exists:
            return await self.create(fields)
        # get table fields
        existing_fields = await self.columns()
        dialect = SqliteDialect()
        sqls = []
        table = dialect.escape_name(self.table)
        should_copy_table = False
        for field in fields:
            existing_field = next(filter(lambda x: x.name == field.name, existing_fields), None)
            if existing_field is not None:
                if not existing_field.primary:
                    existing_field_type = dialect.escape_name(existing_field.name)
                    existing_field_type += SqliteDialect.Space
                    existing_field_type = existing_field.type
                    existing_field_type += SqliteDialect.Space
                    if not existing_field.nullable:
                        existing_field_type += 'NOT NULL'
                    else:
                        existing_field_type += 'NULL'
                    args = field
                    new_field_type = dialect.format_type(**args)
                    if new_field_type != existing_field_type:
                        # important note: ALTER COLUMN is not supported by SQLite
                        # so, we should try table copy operation
                        should_copy_table = True
                        break
            else:
                args = field.__dict__
                new_field_type = dialect.format_type(**args)
                # add column
                sqls.append(f'ALTER TABLE {table} ADD COLUMN {new_field_type};')
        if should_copy_table:
            # rename table (with a random name)
            rename = dialect.escape_name('__' + self.table + '_' + str(int(time.time())) + '__')

            # drop indexes
            table_indexes = self.indexes()
            indexes = await table_indexes.list()
            for index in indexes:
                await table_indexes.drop(index.name)

            await self.__adapter__.execute(f'ALTER TABLE {table} RENAME TO {rename}')
            # create new table
            await self.create(fields)
            # get all existing fields that are existing also into the new table
            insert_fields = []
            for existing_field in existing_fields:
                insert_field = next(filter(lambda x: x.name == existing_field.name, fields), None)
                if insert_field is not None:
                    insert_fields.append(insert_field)
            # copy data
            sql = 'INSERT INTO'
            sql += SqliteDialect.Space
            sql += table
            sql += SqliteDialect.Space
            sql += '('
            sql += ','.join(map(lambda x: dialect.escape_name(x.name), insert_fields))
            sql += ')'
            sql += SqliteDialect.Space
            sql += 'SELECT'
            sql += SqliteDialect.Space
            sql += ','.join(map(lambda x: dialect.escape_name(x.name), insert_fields))
            sql += SqliteDialect.Space
            sql += 'FROM'
            sql += SqliteDialect.Space
            sql += rename
            await self.__adapter__.execute(sql)
            # important note: the renamed table is not being dropped for security reasons
            # this cleanup operation may be done by using SQLite data tools
            return

        if len(sqls) > 0:
            for sql in sqls:
                await self.__adapter__.execute(sql)

    async def exists(self):
        table = self.table
        results = await self.__adapter__.execute(
            f'SELECT COUNT(*) count FROM sqlite_master WHERE name=\'{table}\' AND type=\'table\';')
        if type(results) is list:
            if len(results) > 0:
                return results[0].count > 0
        return False

    async def drop(self):
        return await self.__adapter__.execute(f'DROP TABLE IF EXISTS {self.table};')

    async def version(self):
        migration_exists = await self.__adapter__.table('migrations').exists()
        if not migration_exists:
            return None
        table = SqliteDialect().escape(self.table)
        results = await self.__adapter__.execute(
            f'SELECT MAX(version) AS version FROM migrations WHERE appliesTo={table}'
            )
        if len(results) > 0:
            return results[0].version
        return None

    async def columns(self):
        results = await self.__adapter__.execute(f'PRAGMA table_info({self.table});')
        cols = []
        for result in results:
            col = AnyObject(name=result.name, ordinal=result.cid, type=result.type,
                            nullable=False if result.notnull == 1 else True, primary=(result.pk == 1))
            col.size = None
            col.scale = None
            matches = re.match(r'(\w+)\((\d+)\,?(\d+)?\)', col.type)
            if matches is not None:
                col.size = int(matches.group(2))
                if matches.group(3) is not None:
                    col.scale = int(matches.group(3))
            cols.append(col)
        return cols

    def indexes(self):
        return SqliteTableIndex(self.table, self.__adapter__)


class SqliteView(DataView):

    def __init__(self, view: str, adapter: DataAdapter):
        super().__init__(view, adapter)

    async def create(self, query: QueryExpression):
        # drop view if exists
        await self.drop()
        # and create
        sql = 'CREATE VIEW'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(self.view)
        sql += SqliteDialect.Space
        sql += 'AS'
        sql += SqliteDialect.Space
        sql += SqliteFormatter().format_select(query)
        return await self.__adapter__.execute(sql)

    async def exists(self):
        view = self.view
        results = await self.__adapter__.execute(
            f'SELECT COUNT(*) count FROM sqlite_master WHERE name=\'{view}\' AND type=\'view\';')
        if type(results) is list:
            if len(results) > 0:
                return results[0].count > 0
        return False

    async def drop(self):
        view = SqliteDialect().escape_name(self.view)
        return await self.__adapter__.execute(f'DROP VIEW IF EXISTS {view};')


def regexp_like(value, pattern, match_type=None):
    if value is None:
        return None
    flags = re.MULTILINE | re.UNICODE
    if match_type is not None:
        if match_type.__contains__('i'):  # i: Case-insensitive matching.
            flags = flags | re.IGNORECASE
        if match_type.__contains__('n'):
            # n: The . character matches line terminators. The default is for . matching to stop at the end of a line.
            flags = flags | re.DOTALL
    pattern = re.compile(pattern)
    return 1 if pattern.search(value) is not None else 0


def regexp(value, pattern):
    return regexp_like(value, pattern)


class SqliteAdapter(DataAdapter):

    def __init__(self, options):
        super().__init__()
        self.__raw_connection__: sqlite3.Connection
        self.__transaction__ = False
        self.__last_insert_id__ = None
        self.options = options

    async def open(self):
        if self.__raw_connection__ is None:
            self.__raw_connection__ = sqlite3.connect(self.options.database)
            self.__raw_connection__.create_function('REGEXP', 2, regexp)
            self.__raw_connection__.create_function('REGEXP_LIKE', 3, regexp_like)

    async def close(self):
        if self.__raw_connection__ is not None:
            self.__raw_connection__.close()
            self.__raw_connection__ = None

    async def execute(self, query, values=None):
        cur: sqlite3.Cursor or None = None
        try:
            self.__last_insert_id__ = None
            # ensure that database connection is open
            await self.open()
            # open cursor
            cur = self.__raw_connection__.cursor()
            sql = None
            # format query
            if type(query) is str:
                sql = query
            elif isinstance(query, QueryExpression):
                sql = SqliteFormatter().format(query)
            else:
                TypeError('Expected string or an instance of query expression')
            # execute query
            logging.debug('SQL:%s', sql)
            try:
                cur.execute(sql)
            except Exception as error:
                logging.error('SQL:%s', sql)
                raise error
            # if query is SELECT or PRAGMA
            if re.search('^(SELECT|PRAGMA)', sql, re.DOTALL) is not None:
                # fetch records
                results = cur.fetchall()
                items = []
                cols = []
                for description in cur.description:
                    cols.append(description[0])
                for result in results:
                    item = AnyObject()
                    i = 0
                    for col in cols:
                        setattr(item, col, result[i])
                        i += 1
                    items.append(item)
                return items
            elif re.search('^(INSERT)', sql, re.DOTALL) is not None:
                cur.fetchone()
                insert_id = cur.lastrowid
                if insert_id is not None:
                    self.__last_insert_id__ = insert_id
            else:
                cur.fetchone()
            return None
        finally:
            if cur is not None:
                cur.close()

    async def execute_in_transaction(self, func: Callable):
        """Begins a transactional operation by executing the given callback

        Args:
            func (Callable): A callable to execute

        Raises:
            ex: Any exception that will be thrown by the callable
        """
        if self.__transaction__ is True:
            await func()
            return
        await self.open()
        # begin transaction
        await self.execute('BEGIN;')
        self.__transaction__ = True
        # execute callable
        try:
            await func()
            await self.execute('COMMIT;')
        except Exception as error:
            await self.execute('ROLLBACK;')
            raise error
        finally:
            self.__transaction__ = False

    async def select_identity(self):
        raise NotImplementedError()

    async def last_identity(self):
        return self.__last_insert_id__

    def table(self, table: str) -> SqliteTable:
        return SqliteTable(table, self)

    def view(self, view: str) -> SqliteView:
        return SqliteView(view, self)

    def indexes(self, table: str) -> SqliteTableIndex:
        return SqliteTableIndex(table, self)
