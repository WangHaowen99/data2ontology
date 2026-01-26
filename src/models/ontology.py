"""Ontology models for representing domain entities and relationships."""

from typing import Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


class OntologyDataType(str, Enum):
    """Standard Ontology data types."""
    STRING = "String"
    INTEGER = "Integer"
    LONG = "Long"
    DOUBLE = "Double"
    DECIMAL = "Decimal"
    BOOLEAN = "Boolean"
    DATETIME = "DateTime"
    DATE = "Date"
    TIMESTAMP = "Timestamp"
    OBJECT = "Object"
    ARRAY = "Array"
    BINARY = "Binary"
    GEOLOCATION = "GeoLocation"


# Mapping from PostgreSQL types to Ontology types
PG_TO_ONTOLOGY_TYPE: dict[str, OntologyDataType] = {
    # Integers
    "smallint": OntologyDataType.INTEGER,
    "integer": OntologyDataType.INTEGER,
    "int": OntologyDataType.INTEGER,
    "int4": OntologyDataType.INTEGER,
    "bigint": OntologyDataType.LONG,
    "int8": OntologyDataType.LONG,
    "serial": OntologyDataType.INTEGER,
    "bigserial": OntologyDataType.LONG,
    # Floating point
    "real": OntologyDataType.DOUBLE,
    "float4": OntologyDataType.DOUBLE,
    "double precision": OntologyDataType.DOUBLE,
    "float8": OntologyDataType.DOUBLE,
    "numeric": OntologyDataType.DECIMAL,
    "decimal": OntologyDataType.DECIMAL,
    "money": OntologyDataType.DECIMAL,
    # Boolean
    "boolean": OntologyDataType.BOOLEAN,
    "bool": OntologyDataType.BOOLEAN,
    # Text
    "character varying": OntologyDataType.STRING,
    "varchar": OntologyDataType.STRING,
    "character": OntologyDataType.STRING,
    "char": OntologyDataType.STRING,
    "text": OntologyDataType.STRING,
    "name": OntologyDataType.STRING,
    "uuid": OntologyDataType.STRING,
    # Date/Time
    "date": OntologyDataType.DATE,
    "timestamp": OntologyDataType.TIMESTAMP,
    "timestamp without time zone": OntologyDataType.TIMESTAMP,
    "timestamp with time zone": OntologyDataType.TIMESTAMP,
    "timestamptz": OntologyDataType.TIMESTAMP,
    "time": OntologyDataType.STRING,
    "time without time zone": OntologyDataType.STRING,
    "time with time zone": OntologyDataType.STRING,
    "interval": OntologyDataType.STRING,
    # JSON
    "json": OntologyDataType.OBJECT,
    "jsonb": OntologyDataType.OBJECT,
    # Binary
    "bytea": OntologyDataType.BINARY,
    # Arrays (default to ARRAY, specific handling needed)
    "ARRAY": OntologyDataType.ARRAY,
    # Geo
    "point": OntologyDataType.GEOLOCATION,
    "geometry": OntologyDataType.GEOLOCATION,
    "geography": OntologyDataType.GEOLOCATION,
}


def map_pg_type_to_ontology(pg_type: str) -> OntologyDataType:
    """Map a PostgreSQL type to an Ontology type."""
    # Normalize the type name
    normalized = pg_type.lower().strip()
    
    # Handle array types
    if normalized.endswith("[]") or "array" in normalized:
        return OntologyDataType.ARRAY
    
    # Handle varchar(n), char(n), etc.
    if "(" in normalized:
        normalized = normalized.split("(")[0].strip()
    
    return PG_TO_ONTOLOGY_TYPE.get(normalized, OntologyDataType.STRING)


class PropertyType(BaseModel):
    """An Ontology property type (attribute of an entity)."""
    id: str = Field(..., description="Unique identifier for the property")
    name: str = Field(..., description="Human-readable property name")
    data_type: OntologyDataType = Field(..., description="Property data type")
    description: Optional[str] = Field(None, description="Property description")
    source_table: str = Field(..., description="Source database table")
    source_column: str = Field(..., description="Source database column")
    nullable: bool = Field(True, description="Whether the property can be null")
    is_primary_key: bool = Field(False, description="Whether this is part of primary key")
    creation_reason: str = Field(..., description="Why this property was created")


class ObjectType(BaseModel):
    """An Ontology object type (entity type)."""
    id: str = Field(..., description="Unique identifier for the object type")
    name: str = Field(..., description="Human-readable object type name")
    description: Optional[str] = Field(None, description="Object type description")
    source_table: str = Field(..., description="Source database table")
    primary_key: list[str] = Field(default_factory=list, description="Primary key property IDs")
    properties: list[PropertyType] = Field(default_factory=list, description="Properties of this object")
    creation_reason: str = Field(..., description="Why this object type was created")

    def get_property(self, name: str) -> Optional[PropertyType]:
        """Get property by name."""
        for prop in self.properties:
            if prop.name == name:
                return prop
        return None


class LinkType(BaseModel):
    """An Ontology link type (relationship between entities)."""
    id: str = Field(..., description="Unique identifier for the link type")
    name: str = Field(..., description="Human-readable link name")
    description: Optional[str] = Field(None, description="Link type description")
    source_object_type: str = Field(..., description="Source object type ID")
    target_object_type: str = Field(..., description="Target object type ID")
    cardinality: str = Field("many-to-one", description="Relationship cardinality")
    source_property: Optional[str] = Field(None, description="Source property for the link")
    confidence: str = Field("high", description="Confidence level of this link")
    creation_reason: str = Field(..., description="Why this link was created")


class Ontology(BaseModel):
    """Complete Ontology definition."""
    name: str = Field(..., description="Ontology name")
    description: Optional[str] = Field(None, description="Ontology description")
    version: str = Field("1.0.0", description="Ontology version")
    source_database: str = Field(..., description="Source database name")
    object_types: list[ObjectType] = Field(default_factory=list, description="Object types")
    link_types: list[LinkType] = Field(default_factory=list, description="Link types")
    created_at: Optional[str] = Field(None, description="Creation timestamp")
    
    def get_object_type(self, id: str) -> Optional[ObjectType]:
        """Get object type by ID."""
        for obj in self.object_types:
            if obj.id == id:
                return obj
        return None

    def get_link_type(self, id: str) -> Optional[LinkType]:
        """Get link type by ID."""
        for link in self.link_types:
            if link.id == id:
                return link
        return None

    @property
    def object_type_count(self) -> int:
        return len(self.object_types)

    @property
    def link_type_count(self) -> int:
        return len(self.link_types)

    @property
    def total_property_count(self) -> int:
        return sum(len(obj.properties) for obj in self.object_types)

    def to_json(self) -> dict[str, Any]:
        """Export ontology to JSON-serializable dict."""
        return self.model_dump(exclude_none=True)
