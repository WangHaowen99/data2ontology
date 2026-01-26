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


# ============================================================================
# Unstructured Data Analysis Models
# ============================================================================

class InsightSource(str, Enum):
    """Source of an insight."""
    METADATA = "metadata"  # From database metadata
    LOG = "log"           # From log analysis
    CODE = "code"         # From code analysis


class EntityReference(BaseModel):
    """An entity reference found in logs or code."""
    entity_name: str = Field(..., description="Name of the entity")
    entity_id: Optional[str] = Field(None, description="ID or identifier if found")
    source_location: str = Field(..., description="Where this reference was found")
    context: Optional[str] = Field(None, description="Surrounding context")
    confidence: float = Field(..., description="Confidence score 0-1")


class OperationPattern(BaseModel):
    """An operation pattern detected in logs."""
    operation_type: str = Field(..., description="Type of operation (CREATE, READ, UPDATE, DELETE)")
    entities_involved: list[str] = Field(default_factory=list, description="Entities involved")
    frequency: int = Field(0, description="How many times this pattern appears")
    timestamp_range: Optional[tuple[str, str]] = Field(None, description="Time range of occurrences")
    sample_log_lines: list[str] = Field(default_factory=list, description="Sample log lines")


class CodeEntity(BaseModel):
    """An entity definition found in source code."""
    name: str = Field(..., description="Entity/class name")
    entity_type: str = Field(..., description="Type: class, model, dto, etc.")
    file_path: str = Field(..., description="Source file path")
    line_number: int = Field(0, description="Line number in file")
    fields: list[dict] = Field(default_factory=list, description="Field definitions")
    methods: list[str] = Field(default_factory=list, description="Method names")
    relationships: list[str] = Field(default_factory=list, description="Related entities")
    description: Optional[str] = Field(None, description="Description from docstring/comments")


class ApiEndpoint(BaseModel):
    """An API endpoint found in source code."""
    path: str = Field(..., description="API path")
    method: str = Field(..., description="HTTP method")
    handler: str = Field(..., description="Handler function/method name")
    entities_referenced: list[str] = Field(default_factory=list, description="Entities used")
    file_path: str = Field(..., description="Source file path")
    line_number: int = Field(0, description="Line number in file")


class LogInsight(BaseModel):
    """Insights gathered from log analysis."""
    entity_references: list[EntityReference] = Field(default_factory=list)
    operation_patterns: list[OperationPattern] = Field(default_factory=list)
    entity_cooccurrences: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Entities that frequently appear together"
    )
    total_log_lines_analyzed: int = Field(0)
    log_files_analyzed: list[str] = Field(default_factory=list)


class CodeInsight(BaseModel):
    """Insights gathered from code analysis."""
    entities: list[CodeEntity] = Field(default_factory=list)
    api_endpoints: list[ApiEndpoint] = Field(default_factory=list)
    entity_relationships: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Entity relationships found in code"
    )
    total_files_analyzed: int = Field(0)
    code_files_analyzed: list[str] = Field(default_factory=list)


class EntityInsight(BaseModel):
    """Combined insight about a specific entity."""
    entity_name: str = Field(..., description="Entity name")
    table_name: Optional[str] = Field(None, description="Mapped database table")
    sources: list[InsightSource] = Field(default_factory=list, description="Where this entity was found")
    description_from_code: Optional[str] = Field(None)
    description_from_logs: Optional[str] = Field(None)
    operations_from_logs: list[str] = Field(default_factory=list)
    related_entities: list[str] = Field(default_factory=list)
    confidence: float = Field(1.0, description="Overall confidence 0-1")


class RelationshipInsight(BaseModel):
    """Combined insight about a relationship between entities."""
    source_entity: str = Field(..., description="Source entity name")
    target_entity: str = Field(..., description="Target entity name")
    relationship_type: str = Field(..., description="Type of relationship")
    sources: list[InsightSource] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list, description="Evidence for this relationship")
    confidence: float = Field(1.0, description="Overall confidence 0-1")


class EnhancedDatabaseMetadata(DatabaseMetadata):
    """Extended metadata including insights from unstructured data."""
    log_insights: Optional[LogInsight] = Field(None, description="Insights from logs")
    code_insights: Optional[CodeInsight] = Field(None, description="Insights from code")
    entity_insights: list[EntityInsight] = Field(
        default_factory=list,
        description="Combined entity insights"
    )
    relationship_insights: list[RelationshipInsight] = Field(
        default_factory=list,
        description="Combined relationship insights"
    )

