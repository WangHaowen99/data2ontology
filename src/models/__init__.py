"""Data models for Auto Pipeline Builder."""

from .metadata import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
    DatabaseMetadata,
    DetectedRelationship,
)

from .ontology import (
    PropertyType,
    ObjectType,
    LinkType,
    Ontology,
)

from .pipeline import (
    JoinType,
    JoinCondition,
    ColumnMapping,
    PipelineStep,
    Pipeline,
    Dataset,
)

__all__ = [
    # Metadata models
    "ColumnInfo",
    "ForeignKeyInfo",
    "IndexInfo",
    "TableInfo",
    "DatabaseMetadata",
    "DetectedRelationship",
    # Ontology models
    "PropertyType",
    "ObjectType",
    "LinkType",
    "Ontology",
    # Pipeline models
    "JoinType",
    "JoinCondition",
    "ColumnMapping",
    "PipelineStep",
    "Pipeline",
    "Dataset",
]
