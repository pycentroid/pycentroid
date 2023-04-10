from .dialect import SqliteDialect, SqliteFormatter
from themost_framework.query import QueryExpression, DataAdapter, DataTable, DataView, DataTableIndex
import sqlite3
import re
import time
from typing import Callable
from themost_framework.common import expect, AnyObject
import logging

class SqliteTableIndex(DataTableIndex):

    def __init__(self, table, adapter):
        super().__init__(table, adapter)

    def create(self, name: str, columns: list):
        self.drop(name)
        sql = 'CREATE INDEX'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(name)
        sql += SqliteDialect.Space
        sql += 'ON'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(self.table)
        sql += '('
        sql += ','.join(map(lambda x:SqliteDialect().escape_name(x.name), columns))
        sql += ');'
        self.__adapter__.execute(sql)

    def exists(self, name: str):
        table = SqliteDialect().escape_name(self.table)
        results = self.__adapter__.execute(f'PRAGMA INDEX_LIST({table})')
        return next(filter(lambda x:x.origin=='c' and x.name==name, results), None) is not None

    def drop(self, name: str):
        exists = self.exists(name)
        if exists:
            index = SqliteDialect().escape_name(name)
            self.__adapter__.execute(f'DROP INDEX {index}')

    def list(self):
        table = SqliteDialect().escape_name(self.table)
        # get index list
        results = self.__adapter__.execute(f'PRAGMA INDEX_LIST({table})')
        # prepare index list
        filtered = filter(lambda x:x.origin=='c', results)
        # with name and columns
        indexes = list(map(lambda x:AnyObject(name=x.name,columns=[]), filtered))
        # enumerate indexes
        for index in indexes:
            # and get column list
            columns = self.__adapter__.execute(f'PRAGMA INDEX_INFO({index.name})')
            index.columns = list(map(lambda x:AnyObject(name=x.name), columns))
        return indexes


class SqliteTable(DataTable):
    def __init__(self, table, adapter):
        super().__init__(table, adapter)
    
    def create(self, fields: list):
        if len(fields) == 0:
            Exception('Field collection cannot be empty while creating a database table.')
        dialect = SqliteDialect()
        sql = 'CREATE TABLE'
        sql += SqliteDialect.Space
        sql += dialect.escape_name(self.table)
        sql += '('
        sql += ','.join(map(lambda x: dialect.format_type(name=x.name, type=x.type, nullable=x.nullable, size=x.size, scale=x.scale), fields))
        sql += ')'
        return self.__adapter__.execute(sql)
    
    def change(self, fields: list):
        exists = self.__adapter__.table(self.table).exists();
        if exists == False:
            return self.create(fields)
        # get table fields
        existing_fields = self.columns()
        dialect = SqliteDialect()
        sqls = [];
        table = dialect.escape_name(self.table)
        should_copy_table = False
        for field in fields:
            existing_field = next(filter(lambda x:x.name==field.name, existing_fields),None)
            if existing_field is not None:
                if existing_field.primary == False:
                    existing_field_type = dialect.escape_name(existing_field.name)
                    existing_field_type += SqliteDialect.Space
                    existing_field_type = existing_field.type
                    existing_field_type += SqliteDialect.Space
                    if existing_field.nullable == False:
                        existing_field_type += 'NOT NULL'
                    else:
                        existing_field_type += 'NULL'
                    args = field.__dict__
                    new_field_type = dialect.format_type(**args)
                    if new_field_type != existing_field_type:
                        # important note: ALTER COLUMN is not supported by SQLite
                        # so we should try table copy operation
                        should_copy_table = True
                        break;
            else:
                args = field.__dict__
                new_field_type = dialect.format_type(**args)
                # add column
                sqls.append(f'ALTER TABLE {table} ADD COLUMN {new_field_type};')
        if should_copy_table == True:
            # rename table (with a random name)
            rename = dialect.escape_name('__' + self.table + '_' + str(int(time.time())) + '__')

            # drop indexes
            table_indexes = self.indexes()
            indexes = table_indexes.list()
            for index in indexes:
                table_indexes.drop(index.name)

            self.__adapter__.execute(f'ALTER TABLE {table} RENAME TO {rename}')
            # create new table
            self.create(fields)
            # get all existing fields that are existing also into the new table
            insert_fields = []
            for existing_field in existing_fields:
                    insert_field = next(filter(lambda x:x.name==existing_field.name, fields),None)
                    if insert_field is not None:
                        insert_fields.append(insert_field)
            # copy data
            sql = 'INSERT INTO'
            sql += SqliteDialect.Space
            sql += table
            sql += SqliteDialect.Space
            sql += '('
            sql += ','.join(map(lambda x:dialect.escape_name(x.name), insert_fields))
            sql += ')'
            sql += SqliteDialect.Space
            sql += 'SELECT'
            sql += SqliteDialect.Space
            sql += ','.join(map(lambda x:dialect.escape_name(x.name), insert_fields))
            sql += SqliteDialect.Space
            sql += 'FROM'
            sql += SqliteDialect.Space
            sql += rename
            self.__adapter__.execute(sql)
            # important note: the renamed table is not being dropped for security reasons
            # this cleanup operation may be done by using SQLite data tools
            return

        if len(sqls) > 0:
            for sql in sqls:
                self.__adapter__.execute(sql)
    
    def exists(self):
        table = self.table
        results = self.__adapter__.execute(f'SELECT COUNT(*) count FROM sqlite_master WHERE name=\'{table}\' AND type=\'table\';')
        if type(results) is list:
            if len(results) > 0:
                return results[0].count > 0
        return false

    def drop(self):
        return self.__adapter__.execute(f'DROP TABLE IF EXISTS {self.table};')

    def version(self):
        migration_exists = self.__adapter__.table('migrations').exists();
        if (migration_exists == False):
            return None
        table = SqliteDialect().escape(self.table)
        results = self.__adapter__.execute(f'SELECT MAX(version) AS version FROM migrations WHERE appliesTo={table}')
        if len(results) > 0:
            return results[0].version
        return None

    def columns(self):
        results = self.__adapter__.execute(f'PRAGMA table_info({self.table});')
        cols = []
        for result in results:
            col = AnyObject(name=result.name, ordinal=result.cid, type=result.type,
                nullable = False if result.notnull == 1 else True, primary = (result.pk == 1))
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

    def __init__(self,view: str, adapter: DataAdapter):
        super().__init__(view, adapter)

    def create(self, query: QueryExpression):
        # drop view if exists
        self.drop()
        ## and create
        sql = 'CREATE VIEW'
        sql += SqliteDialect.Space
        sql += SqliteDialect().escape_name(self.view)
        sql += SqliteDialect.Space
        sql += 'AS'
        sql += SqliteDialect.Space
        sql += SqliteFormatter().format_select(query)
        return self.__adapter__.execute(sql)

    def exists(self):
        view = self.view
        results = self.__adapter__.execute(f'SELECT COUNT(*) count FROM sqlite_master WHERE name=\'{view}\' AND type=\'view\';')
        if type(results) is list:
            if len(results) > 0:
                return results[0].count > 0
        return false

    def drop(self):
        view = SqliteDialect().escape_name(self.view)
        return self.__adapter__.execute(f'DROP VIEW IF EXISTS {view};')


def regexp_like(value, pattern, match_type = None):
    if value is None:
        return None
    flags = re.MULTILINE | re.UNICODE
    if match_type is not None:
        if match_type.__contains__('i'): # i: Case-insensitive matching.
            flags = flags | re.IGNORECASE
        if match_type.__contains__('n'): # n: The . character matches line terminators. The default is for . matching to stop at the end of a line.
            flags = flags | re.DOTALL
    pattern = re.compile(pattern)
    return 1 if pattern.search(value) is not None else 0

def regexp(value, pattern):
    return regexp_like(value, pattern)


class SqliteAdapter(DataAdapter):
    
    def __init__(self, options):
        self.__raw_connection__: sqlite3.Connection = None
        self.__transaction__ = False
        self.__last_insert_id__ = None
        self.options = options

    def open(self):
        if self.__raw_connection__ is None:
            self.__raw_connection__ = sqlite3.connect(self.options.database)
            self.__raw_connection__.create_function('REGEXP', 2, regexp)
            self.__raw_connection__.create_function('REGEXP_LIKE', 3, regexp_like)
    
    def close(self):
        if self.__raw_connection__ is not None:
            self.__raw_connection__.close()
            self.__raw_connection__ = None
    
    def execute(self, query, values = None):
        cur: sqlite3.Cursor = None
        results = []
        try:
            self.last_insert_id = None
            # ensure that database connection is open
            self.open()
            # open cursor
            cur = self.__raw_connection__.cursor()
            sql = None;
            # format query
            if type(query) is str:
                sql = query
            elif type(query) is QueryExpression:
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
                ## fetch records
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
                    self.last_insert_id = insert_id
            else:
                cur.fetchone()
            return None
        finally:
            if cur is not None:
                cur.close()
    
    def execute_in_transaction(self, func: Callable):
        """Begins a transactional operation by executing the given callback

        Args:
            func (Callable): A callable to execute

        Raises:
            ex: Any exception that will be thrown by the callable
        """
        if self.__transaction__ == True:
            func()
            return
        self.open()
        # begin transaction
        self.__raw_connection__.execute('BEGIN;');
        self.__transaction__ = True
        # execute callable
        try:
            func()
            self.__raw_connection__.execute('COMMIT;');
        except Exception as error:
            self.__raw_connection__.execute('ROLLBACK;');
            raise error
        finally:
            self.__transaction__ = False

    def select_identity(self):
        raise NotImplementedError()

    def last_identity(self):
        return self.__last_insert_id__

    def table(self, table: str) -> SqliteTable:
        return SqliteTable(table, self)
    
    def view(self, view: str) -> SqliteView:
        return SqliteView(view, self)
    
    def indexes(self, table: str) -> SqliteTableIndex:
        return SqliteTableIndex(table, self)






