"""Metadata models for database schema representation."""

from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class RelationshipConfidence(str, Enum):
    """Confidence level of detected relationships."""
    HIGH = "high"       # 100% - From foreign key constraint
    MEDIUM = "medium"   # 80% - From naming convention
    LOW = "low"         # 60% - From similarity analysis


class ColumnInfo(BaseModel):
    """Information about a database column."""
    name: str = Field(..., description="Column name")
    data_type: str = Field(..., description="PostgreSQL data type")
    nullable: bool = Field(True, description="Whether the column allows NULL")
    default: Optional[str] = Field(None, description="Default value")
    is_primary_key: bool = Field(False, description="Whether this is a primary key")
    is_unique: bool = Field(False, description="Whether this column has unique constraint")
    comment: Optional[str] = Field(None, description="Column comment/description")
    ordinal_position: int = Field(0, description="Column position in the table")


class ForeignKeyInfo(BaseModel):
    """Information about a foreign key constraint."""
    constraint_name: str = Field(..., description="Name of the FK constraint")
    column: str = Field(..., description="Column in the current table")
    references_table: str = Field(..., description="Referenced table name")
    references_column: str = Field(..., description="Referenced column name")
    references_schema: str = Field("public", description="Referenced schema")


class IndexInfo(BaseModel):
    """Information about an index."""
    name: str = Field(..., description="Index name")
    columns: list[str] = Field(default_factory=list, description="Indexed columns")
    is_unique: bool = Field(False, description="Whether this is a unique index")
    is_primary: bool = Field(False, description="Whether this is the primary key index")


class TableInfo(BaseModel):
    """Complete information about a database table."""
    name: str = Field(..., description="Table name")
    schema: str = Field("public", description="Schema name")
    columns: list[ColumnInfo] = Field(default_factory=list, description="Table columns")
    primary_keys: list[str] = Field(default_factory=list, description="Primary key columns")
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list, description="Foreign keys")
    indexes: list[IndexInfo] = Field(default_factory=list, description="Table indexes")
    comment: Optional[str] = Field(None, description="Table comment/description")
    row_count_estimate: Optional[int] = Field(None, description="Estimated row count")

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}"

    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


class DetectedRelationship(BaseModel):
    """A detected relationship between two tables."""
    source_table: str = Field(..., description="Source table name")
    source_column: str = Field(..., description="Source column name")
    target_table: str = Field(..., description="Target table name")
    target_column: str = Field(..., description="Target column name")
    confidence: RelationshipConfidence = Field(..., description="Confidence level")
    detection_method: str = Field(..., description="How the relationship was detected")
    reason: str = Field(..., description="Explanation for this relationship")


class DatabaseMetadata(BaseModel):
    """Complete metadata for a database."""
    database_name: str = Field(..., description="Database name")
    tables: list[TableInfo] = Field(default_factory=list, description="All tables")
    detected_relationships: list[DetectedRelationship] = Field(
        default_factory=list, 
        description="All detected relationships"
    )

    def get_table(self, name: str, schema: str = "public") -> Optional[TableInfo]:
        """Get table by name and schema."""
        for table in self.tables:
            if table.name == name and table.schema == schema:
                return table
        return None

    @property
    def table_count(self) -> int:
        """Get total table count."""
        return len(self.tables)

    @property
    def column_count(self) -> int:
        """Get total column count across all tables."""
        return sum(len(t.columns) for t in self.tables)

    @property
    def foreign_key_count(self) -> int:
        """Get total foreign key count."""
        return sum(len(t.foreign_keys) for t in self.tables)
