"""Unit tests for relationship analyzer."""

import pytest
from src.relationship_analyzer import RelationshipAnalyzer
from src.models.metadata import (
    ColumnInfo,
    ForeignKeyInfo,
    TableInfo,
    DatabaseMetadata,
    RelationshipConfidence,
)


class TestRelationshipAnalyzer:
    """Tests for RelationshipAnalyzer."""
    
    @pytest.fixture
    def sample_metadata(self):
        """Create sample metadata for testing."""
        return DatabaseMetadata(
            database_name="testdb",
            tables=[
                TableInfo(
                    name="users",
                    columns=[
                        ColumnInfo(name="id", data_type="integer", is_primary_key=True),
                        ColumnInfo(name="name", data_type="varchar"),
                    ],
                    primary_keys=["id"],
                ),
                TableInfo(
                    name="orders",
                    columns=[
                        ColumnInfo(name="id", data_type="integer", is_primary_key=True),
                        ColumnInfo(name="user_id", data_type="integer"),
                        ColumnInfo(name="amount", data_type="numeric"),
                    ],
                    primary_keys=["id"],
                    foreign_keys=[
                        ForeignKeyInfo(
                            constraint_name="fk_orders_user",
                            column="user_id",
                            references_table="users",
                            references_column="id",
                        ),
                    ],
                ),
                TableInfo(
                    name="products",
                    columns=[
                        ColumnInfo(name="id", data_type="integer", is_primary_key=True),
                        ColumnInfo(name="category_id", data_type="integer"),
                    ],
                    primary_keys=["id"],
                ),
                TableInfo(
                    name="categories",
                    columns=[
                        ColumnInfo(name="id", data_type="integer", is_primary_key=True),
                        ColumnInfo(name="name", data_type="varchar"),
                    ],
                    primary_keys=["id"],
                ),
            ],
        )
    
    def test_extract_fk_relationships(self, sample_metadata):
        """Test extraction of foreign key relationships."""
        analyzer = RelationshipAnalyzer()
        result = analyzer.analyze(sample_metadata)
        
        # Should find the FK relationship
        fk_rels = [r for r in result.detected_relationships 
                   if r.confidence == RelationshipConfidence.HIGH]
        
        assert len(fk_rels) == 1
        assert fk_rels[0].source_table == "orders"
        assert fk_rels[0].target_table == "users"
    
    def test_detect_naming_relationships(self, sample_metadata):
        """Test detection of relationships by naming convention."""
        analyzer = RelationshipAnalyzer()
        result = analyzer.analyze(sample_metadata)
        
        # Should detect category_id -> categories relationship
        naming_rels = [r for r in result.detected_relationships 
                       if r.confidence == RelationshipConfidence.MEDIUM]
        
        # products.category_id should be detected as potential FK to categories
        category_rel = [r for r in naming_rels 
                        if r.source_table == "products" and "category" in r.source_column.lower()]
        assert len(category_rel) >= 1
    
    def test_get_join_path(self, sample_metadata):
        """Test finding join path between tables."""
        analyzer = RelationshipAnalyzer()
        analyzer.analyze(sample_metadata)
        
        path = analyzer.get_join_path("orders", "users")
        assert path is not None
        assert len(path) == 1
        assert path[0][0] == "orders"
        assert path[0][2] == "users"
    
    def test_get_relationship_stats(self, sample_metadata):
        """Test relationship statistics."""
        analyzer = RelationshipAnalyzer()
        analyzer.analyze(sample_metadata)
        
        stats = analyzer.get_relationship_stats()
        assert stats["total_tables"] > 0
        assert stats["total_relationships"] > 0
