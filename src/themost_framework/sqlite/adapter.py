from .dialect import SqliteDialect, SqliteFormatter
from themost_framework.query import QueryExpression
import sqlite3
import re;
from typing import Callable
from themost_framework.common import ObjectMap, DataAdapter, DatabaseTable, DatabaseView, DatabaseTableIndexes


class SqliteTableIndexes:

    def __init__(self, table, adapter):
        super().__init__(table, adapter)

    def create(self, name: str, columns: list):
        raise NotImplementedError()

    def exists(self, name: str):
        raise NotImplementedError()

    def drop(self, name: str):
        raise NotImplementedError()

    def list(self):
        raise NotImplementedError()

class SqliteTable(DatabaseTable):
    def __init__(self, table, adapter):
        super().__init__(table, adapter)
    
    def create(self, fields: list):
        raise NotImplementedError()
    
    def change(self, fields: list):
        raise NotImplementedError()
    
    def exists(self):
        table = self.table
        results = self.__adapter__.execute(f'SELECT COUNT(*) count FROM sqlite_master WHERE name=\'{table}\' AND type=\'table\';')
        if type(results) is list:
            if len(results) > 0:
                return results[0].count > 0
        return false

    def drop(self):
        raise NotImplementedError()

    def version(self):
        raise NotImplementedError()

    def columns(self):
        results = self.__adapter__.execute(f'PRAGMA table_info({self.table})')
        return results

    def indexes(self):
        return SqliteTableIndexes(self.table, self.__adapter__)

class SqliteView(DatabaseView):

    def __init__(self,view: str, adapter: DataAdapter):
        super().__init__(table, adapter)

    def create(self, query):
        raise NotImplementedError()

    def exists(self):
        raise NotImplementedError()

    def drop(self):
        raise NotImplementedError()

class SqliteAdapter(DataAdapter):
    
    def __init__(self, options):
        self.__raw_connection__: sqlite3.Connection = None
        self.__transaction__ = False
        self.__last_insert_id__ = None
        self.options = options

    def open(self):
        if self.__raw_connection__ is None:
            self.__raw_connection__ = sqlite3.connect(self.options.database)
    
    def close(self):
        if (self.__raw_connection__):
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
                sql = SqliteFormatter().format_select(query)
            else:
                TypeError('Expected string or an instance of query expression')
            # execute query
            cur.execute(query)
            # if query is SELECT or PRAGMA
            if re.search('^(SELECT|PRAGMA)', sql, re.DOTALL) is not None:
                ## fetch records
                results = cur.fetchall()
                items = []
                cols = []
                for description in cur.description:
                    cols.append(description[0])
                for result in results:
                    item = ObjectMap()
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
        except ex:
            self.__raw_connection__.execute('ROLLBACK;');
            raise ex
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






