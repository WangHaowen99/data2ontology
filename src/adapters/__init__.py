"""Database adapters for multi-database support."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ColumnInfo:
    """Column metadata."""
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class TableInfo:
    """Table metadata."""
    name: str
    schema: str
    columns: List[ColumnInfo]
    row_count: Optional[int] = None
    comment: Optional[str] = None


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize adapter with connection config."""
        self.config = config
        self._connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection. Returns True if successful."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is alive."""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[TableInfo]:
        """Get all tables with metadata."""
        pass
    
    @abstractmethod
    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows from a table."""
        pass
    
    @abstractmethod
    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        pass
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connection is not None


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""
    
    def connect(self) -> bool:
        try:
            import psycopg2
            self._connection = psycopg2.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5432),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password")
            )
            self._connection.autocommit = True  # 避免事务问题
            return True
        except Exception as e:
            raise ConnectionError(f"PostgreSQL connection failed: {e}")
    
    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def test_connection(self) -> bool:
        if not self._connection:
            return False
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            return False
    
    def _reset_connection(self):
        """Reset connection if in failed transaction state."""
        try:
            self._connection.rollback()
        except:
            pass
    
    def get_tables(self) -> List[TableInfo]:
        self._reset_connection()  # 确保事务状态干净
        schema = self.config.get("schema_name", "public")
        cursor = self._connection.cursor()
        
        try:
            # Get tables
            cursor.execute("""
                SELECT table_name, obj_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass, 'pg_class') as comment
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """, (schema,))
            
            tables = []
            for row in cursor.fetchall():
                table_name, comment = row
                
                # Get columns
                cursor.execute("""
                    SELECT 
                        c.column_name, 
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        pgd.description,
                        CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk
                    FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_description pgd 
                        ON pgd.objoid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
                        AND pgd.objsubid = c.ordinal_position
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                        WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON pk.column_name = c.column_name
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (schema, table_name, schema, table_name))
                
                columns = []
                for col_row in cursor.fetchall():
                    columns.append(ColumnInfo(
                        name=col_row[0],
                        data_type=col_row[1],
                        nullable=col_row[2] == 'YES',
                        default_value=col_row[3],
                        comment=col_row[4],
                        is_primary_key=col_row[5]
                    ))
                
                # Get row count estimate
                cursor.execute(f"""
                    SELECT reltuples::bigint FROM pg_class 
                    WHERE relname = %s AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
                """, (table_name, schema))
                row_count_result = cursor.fetchone()
                row_count = int(row_count_result[0]) if row_count_result else None
                
                tables.append(TableInfo(
                    name=table_name,
                    schema=schema,
                    columns=columns,
                    row_count=row_count,
                    comment=comment
                ))
            
            cursor.close()
            return tables
        except Exception as e:
            cursor.close()
            self._reset_connection()
            raise e
    
    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        self._reset_connection()
        schema = self.config.get("schema_name", "public")
        cursor = self._connection.cursor()
        
        try:
            cursor.execute(f'SELECT * FROM "{schema}"."{table_name}" LIMIT %s', (limit,))
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return rows
        except Exception as e:
            cursor.close()
            self._reset_connection()
            raise e
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        self._reset_connection()
        cursor = self._connection.cursor()
        
        try:
            cursor.execute(sql)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return {"columns": columns, "rows": rows}
            else:
                self._connection.commit()
                cursor.close()
                return {"message": "Query executed successfully", "rows": []}
        except Exception as e:
            cursor.close()
            self._reset_connection()
            raise e




class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter."""
    
    def connect(self) -> bool:
        try:
            import pymysql
            self._connection = pymysql.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 3306),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
                charset='utf8mb4'
            )
            return True
        except Exception as e:
            raise ConnectionError(f"MySQL connection failed: {e}")
    
    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def test_connection(self) -> bool:
        if not self._connection:
            return False
        try:
            self._connection.ping(reconnect=True)
            return True
        except:
            return False
    
    def get_tables(self) -> List[TableInfo]:
        cursor = self._connection.cursor()
        database = self.config.get("database")
        
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_COMMENT 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """, (database,))
        
        tables = []
        for row in cursor.fetchall():
            table_name, comment = row
            
            cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_COMMENT,
                    COLUMN_KEY
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table_name))
            
            columns = []
            for col_row in cursor.fetchall():
                columns.append(ColumnInfo(
                    name=col_row[0],
                    data_type=col_row[1],
                    nullable=col_row[2] == 'YES',
                    default_value=col_row[3],
                    comment=col_row[4],
                    is_primary_key=col_row[5] == 'PRI'
                ))
            
            tables.append(TableInfo(
                name=table_name,
                schema=database,
                columns=columns,
                comment=comment
            ))
        
        cursor.close()
        return tables
    
    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s", (limit,))
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        cursor = self._connection.cursor()
        cursor.execute(sql)
        
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return {"columns": columns, "rows": rows}
        else:
            self._connection.commit()
            cursor.close()
            return {"message": "Query executed successfully", "rows": []}


class SQLServerAdapter(DatabaseAdapter):
    """SQL Server database adapter."""
    
    def connect(self) -> bool:
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.get('host', 'localhost')},{self.config.get('port', 1433)};"
                f"DATABASE={self.config.get('database')};"
                f"UID={self.config.get('user')};"
                f"PWD={self.config.get('password')}"
            )
            self._connection = pyodbc.connect(conn_str)
            return True
        except Exception as e:
            raise ConnectionError(f"SQL Server connection failed: {e}")
    
    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def test_connection(self) -> bool:
        if not self._connection:
            return False
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            return False
    
    def get_tables(self) -> List[TableInfo]:
        cursor = self._connection.cursor()
        
        cursor.execute("""
            SELECT t.TABLE_NAME, ep.value as comment
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
            LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
            WHERE t.TABLE_TYPE = 'BASE TABLE' AND t.TABLE_CATALOG = DB_NAME()
        """)
        
        tables = []
        for row in cursor.fetchall():
            table_name, comment = row
            
            cursor.execute("""
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    c.COLUMN_DEFAULT,
                    ep.value as comment,
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_pk
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN sys.columns sc ON sc.name = c.COLUMN_NAME AND sc.object_id = OBJECT_ID(c.TABLE_NAME)
                LEFT JOIN sys.extended_properties ep ON ep.major_id = sc.object_id AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
                LEFT JOIN (
                    SELECT ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
                WHERE c.TABLE_NAME = ?
                ORDER BY c.ORDINAL_POSITION
            """, (table_name, table_name))
            
            columns = []
            for col_row in cursor.fetchall():
                columns.append(ColumnInfo(
                    name=col_row[0],
                    data_type=col_row[1],
                    nullable=col_row[2] == 'YES',
                    default_value=col_row[3],
                    comment=col_row[4],
                    is_primary_key=bool(col_row[5])
                ))
            
            tables.append(TableInfo(
                name=table_name,
                schema='dbo',
                columns=columns,
                comment=comment
            ))
        
        cursor.close()
        return tables
    
    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        cursor = self._connection.cursor()
        cursor.execute(f"SELECT TOP {limit} * FROM [{table_name}]")
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        cursor = self._connection.cursor()
        cursor.execute(sql)
        
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return {"columns": columns, "rows": rows}
        else:
            self._connection.commit()
            cursor.close()
            return {"message": "Query executed successfully", "rows": []}


class CSVAdapter(DatabaseAdapter):
    """CSV file adapter using pandas."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._dataframes = {}
    
    def connect(self) -> bool:
        try:
            import pandas as pd
            file_path = self.config.get("file_path")
            if file_path:
                df = pd.read_csv(file_path)
                table_name = self.config.get("name") or file_path.split("/")[-1].replace(".csv", "")
                self._dataframes[table_name] = df
            self._connection = True
            return True
        except Exception as e:
            raise ConnectionError(f"CSV loading failed: {e}")
    
    def add_csv(self, file_path: str, table_name: str = None):
        """Add a CSV file as a table."""
        import pandas as pd
        df = pd.read_csv(file_path)
        name = table_name or file_path.split("/")[-1].replace(".csv", "")
        self._dataframes[name] = df
    
    def disconnect(self):
        self._dataframes = {}
        self._connection = None
    
    def test_connection(self) -> bool:
        return self._connection is not None
    
    def get_tables(self) -> List[TableInfo]:
        tables = []
        for name, df in self._dataframes.items():
            columns = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                columns.append(ColumnInfo(
                    name=col,
                    data_type=dtype,
                    nullable=df[col].isnull().any()
                ))
            
            tables.append(TableInfo(
                name=name,
                schema="csv",
                columns=columns,
                row_count=len(df)
            ))
        
        return tables
    
    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        if table_name in self._dataframes:
            return self._dataframes[table_name].head(limit).to_dict('records')
        return []
    
    def execute_query(self, sql: str) -> Dict[str, Any]:
        # Use pandasql for SQL on DataFrames
        try:
            import pandasql as psql
            env = self._dataframes.copy()
            result = psql.sqldf(sql, env)
            return {"columns": list(result.columns), "rows": result.to_dict('records')}
        except Exception as e:
            return {"message": f"Query failed: {e}", "rows": []}


def get_adapter(db_type: str, config: Dict[str, Any]) -> DatabaseAdapter:
    """Factory function to get appropriate adapter."""
    adapters = {
        'postgresql': PostgresAdapter,
        'mysql': MySQLAdapter,
        'sqlserver': SQLServerAdapter,
        'csv': CSVAdapter,
    }
    
    adapter_class = adapters.get(db_type.lower())
    if not adapter_class:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return adapter_class(config)
