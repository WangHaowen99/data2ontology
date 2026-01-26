"""Unit tests for metadata models."""

import pytest
from src.models.metadata import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
    DatabaseMetadata,
    DetectedRelationship,
    RelationshipConfidence,
)


class TestColumnInfo:
    """Tests for ColumnInfo model."""
    
    def test_create_column(self):
        col = ColumnInfo(
            name="id",
            data_type="integer",
            nullable=False,
            is_primary_key=True,
        )
        assert col.name == "id"
        assert col.data_type == "integer"
        assert col.nullable is False
        assert col.is_primary_key is True
    
    def test_column_defaults(self):
        col = ColumnInfo(name="name", data_type="varchar")
        assert col.nullable is True
        assert col.is_primary_key is False
        assert col.is_unique is False
        assert col.default is None


class TestTableInfo:
    """Tests for TableInfo model."""
    
    def test_create_table(self):
        table = TableInfo(
            name="users",
            schema="public",
            columns=[
                ColumnInfo(name="id", data_type="integer", is_primary_key=True),
                ColumnInfo(name="name", data_type="varchar"),
            ],
            primary_keys=["id"],
        )
        assert table.name == "users"
        assert table.full_name == "public.users"
        assert len(table.columns) == 2
    
    def test_get_column(self):
        table = TableInfo(
            name="users",
            columns=[
                ColumnInfo(name="id", data_type="integer"),
                ColumnInfo(name="name", data_type="varchar"),
            ],
        )
        col = table.get_column("name")
        assert col is not None
        assert col.name == "name"
        
        col = table.get_column("nonexistent")
        assert col is None


class TestDatabaseMetadata:
    """Tests for DatabaseMetadata model."""
    
    def test_create_metadata(self):
        metadata = DatabaseMetadata(
            database_name="testdb",
            tables=[
                TableInfo(name="users", columns=[ColumnInfo(name="id", data_type="integer")]),
                TableInfo(name="orders", columns=[
                    ColumnInfo(name="id", data_type="integer"),
                    ColumnInfo(name="user_id", data_type="integer"),
                ]),
            ],
        )
        assert metadata.database_name == "testdb"
        assert metadata.table_count == 2
        assert metadata.column_count == 3
    
    def test_get_table(self):
        metadata = DatabaseMetadata(
            database_name="testdb",
            tables=[TableInfo(name="users", schema="public")],
        )
        table = metadata.get_table("users")
        assert table is not None
        assert table.name == "users"


class TestDetectedRelationship:
    """Tests for DetectedRelationship model."""
    
    def test_create_relationship(self):
        rel = DetectedRelationship(
            source_table="orders",
            source_column="user_id",
            target_table="users",
            target_column="id",
            confidence=RelationshipConfidence.HIGH,
            detection_method="foreign_key_constraint",
            reason="FK constraint",
        )
        assert rel.source_table == "orders"
        assert rel.target_table == "users"
        assert rel.confidence == RelationshipConfidence.HIGH
