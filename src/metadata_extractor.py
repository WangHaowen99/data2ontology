"""PostgreSQL metadata extractor module."""

import re
from typing import Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

from .config import DatabaseConfig, AnalysisConfig
from .models.metadata import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
    DatabaseMetadata,
    DetectedRelationship,
    RelationshipConfidence,
)


class MetadataExtractor:
    """Extracts metadata from PostgreSQL database."""

    def __init__(self, db_config: DatabaseConfig, analysis_config: Optional[AnalysisConfig] = None):
        """Initialize the metadata extractor.
        
        Args:
            db_config: Database connection configuration
            analysis_config: Analysis configuration (optional)
        """
        self.db_config = db_config
        self.analysis_config = analysis_config or AnalysisConfig()
        self._engine: Optional[Engine] = None

    def connect(self) -> Engine:
        """Create and return database engine."""
        if self._engine is None:
            self._engine = create_engine(self.db_config.connection_string)
        return self._engine

    def close(self):
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def extract_metadata(self) -> DatabaseMetadata:
        """Extract complete database metadata.
        
        Returns:
            DatabaseMetadata object containing all table information
        """
        engine = self.connect()
        inspector = inspect(engine)
        
        tables = []
        schemas_to_analyze = self.analysis_config.schemas if self.analysis_config.schemas else ["public"]
        
        for schema in schemas_to_analyze:
            table_names = inspector.get_table_names(schema=schema)
            
            # Apply exclusions
            if self.analysis_config.exclude_tables:
                table_names = [t for t in table_names if t not in self.analysis_config.exclude_tables]
            
            # Apply limit
            if self.analysis_config.max_tables > 0:
                table_names = table_names[:self.analysis_config.max_tables]
            
            for table_name in table_names:
                table_info = self._extract_table_info(inspector, table_name, schema, engine)
                tables.append(table_info)
            
            # Include views if configured
            if self.analysis_config.include_views:
                view_names = inspector.get_view_names(schema=schema)
                for view_name in view_names:
                    view_info = self._extract_table_info(inspector, view_name, schema, engine, is_view=True)
                    tables.append(view_info)
        
        return DatabaseMetadata(
            database_name=self.db_config.database,
            tables=tables,
            detected_relationships=[],  # Will be populated by RelationshipAnalyzer
        )

    def _extract_table_info(
        self, 
        inspector, 
        table_name: str, 
        schema: str,
        engine: Engine,
        is_view: bool = False
    ) -> TableInfo:
        """Extract information for a single table.
        
        Args:
            inspector: SQLAlchemy inspector
            table_name: Name of the table
            schema: Schema name
            engine: Database engine for additional queries
            is_view: Whether this is a view
            
        Returns:
            TableInfo object
        """
        # Get columns
        columns = []
        pk_columns = set(inspector.get_pk_constraint(table_name, schema=schema).get("constrained_columns", []))
        
        for col in inspector.get_columns(table_name, schema=schema):
            col_info = ColumnInfo(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col.get("nullable", True),
                default=str(col.get("default")) if col.get("default") else None,
                is_primary_key=col["name"] in pk_columns,
                is_unique=False,  # Will be updated from unique constraints
                comment=col.get("comment"),
                ordinal_position=len(columns) + 1,
            )
            columns.append(col_info)
        
        # Get foreign keys
        foreign_keys = []
        for fk in inspector.get_foreign_keys(table_name, schema=schema):
            for i, col in enumerate(fk.get("constrained_columns", [])):
                ref_cols = fk.get("referred_columns", [])
                ref_col = ref_cols[i] if i < len(ref_cols) else ref_cols[0] if ref_cols else ""
                fk_info = ForeignKeyInfo(
                    constraint_name=fk.get("name", ""),
                    column=col,
                    references_table=fk.get("referred_table", ""),
                    references_column=ref_col,
                    references_schema=fk.get("referred_schema", schema),
                )
                foreign_keys.append(fk_info)
        
        # Get indexes
        indexes = []
        for idx in inspector.get_indexes(table_name, schema=schema):
            idx_info = IndexInfo(
                name=idx.get("name", ""),
                columns=idx.get("column_names", []),
                is_unique=idx.get("unique", False),
                is_primary=False,  # Primary key index handled separately
            )
            indexes.append(idx_info)
        
        # Update unique constraints on columns
        for idx in indexes:
            if idx.is_unique and len(idx.columns) == 1:
                for col in columns:
                    if col.name == idx.columns[0]:
                        col.is_unique = True
        
        # Get table comment
        table_comment = self._get_table_comment(engine, table_name, schema)
        
        # Get row count estimate
        row_count = self._get_row_count_estimate(engine, table_name, schema)
        
        return TableInfo(
            name=table_name,
            schema=schema,
            columns=columns,
            primary_keys=list(pk_columns),
            foreign_keys=foreign_keys,
            indexes=indexes,
            comment=table_comment,
            row_count_estimate=row_count,
        )

    def _get_table_comment(self, engine: Engine, table_name: str, schema: str) -> Optional[str]:
        """Get table comment from PostgreSQL."""
        query = text("""
            SELECT obj_description(
                (quote_ident(:schema) || '.' || quote_ident(:table))::regclass, 
                'pg_class'
            )
        """)
        try:
            with engine.connect() as conn:
                result = conn.execute(query, {"schema": schema, "table": table_name})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception:
            return None

    def _get_row_count_estimate(self, engine: Engine, table_name: str, schema: str) -> Optional[int]:
        """Get estimated row count from pg_stat_user_tables."""
        query = text("""
            SELECT n_live_tup 
            FROM pg_stat_user_tables 
            WHERE schemaname = :schema AND relname = :table
        """)
        try:
            with engine.connect() as conn:
                result = conn.execute(query, {"schema": schema, "table": table_name})
                row = result.fetchone()
                return int(row[0]) if row and row[0] else None
        except Exception:
            return None

    def get_table_sample(self, table_name: str, schema: str = "public", limit: int = 5) -> list[dict]:
        """Get sample rows from a table for analysis.
        
        Args:
            table_name: Table name
            schema: Schema name
            limit: Number of rows to fetch
            
        Returns:
            List of row dictionaries
        """
        engine = self.connect()
        query = text(f'SELECT * FROM "{schema}"."{table_name}" LIMIT :limit')
        try:
            with engine.connect() as conn:
                result = conn.execute(query, {"limit": limit})
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception:
            return []


def extract_database_metadata(db_config: DatabaseConfig, analysis_config: Optional[AnalysisConfig] = None) -> DatabaseMetadata:
    """Convenience function to extract database metadata.
    
    Args:
        db_config: Database configuration
        analysis_config: Analysis configuration (optional)
        
    Returns:
        DatabaseMetadata object
    """
    extractor = MetadataExtractor(db_config, analysis_config)
    try:
        return extractor.extract_metadata()
    finally:
        extractor.close()
