"""Relationship analyzer for detecting table relationships."""

import re
from typing import Optional
from difflib import SequenceMatcher
import networkx as nx

from .config import AnalysisConfig
from .models.metadata import (
    DatabaseMetadata,
    TableInfo,
    DetectedRelationship,
    RelationshipConfidence,
)


class RelationshipAnalyzer:
    """Analyzes database metadata to detect relationships between tables."""

    def __init__(self, analysis_config: Optional[AnalysisConfig] = None):
        """Initialize the relationship analyzer.
        
        Args:
            analysis_config: Analysis configuration
        """
        self.config = analysis_config or AnalysisConfig()
        self.relationship_graph = nx.DiGraph()
        
        # Default FK naming patterns
        self._fk_patterns = [
            re.compile(pattern) for pattern in self.config.fk_column_patterns
        ]

    def analyze(self, metadata: DatabaseMetadata) -> DatabaseMetadata:
        """Analyze database metadata and detect relationships.
        
        Args:
            metadata: Database metadata to analyze
            
        Returns:
            Updated metadata with detected relationships
        """
        relationships = []
        
        # Build table lookup
        table_lookup = {t.name: t for t in metadata.tables}
        
        # Step 1: Extract explicit foreign key relationships
        fk_relationships = self._extract_fk_relationships(metadata)
        relationships.extend(fk_relationships)
        
        # Step 2: Detect relationships by naming convention
        naming_relationships = self._detect_naming_relationships(metadata, table_lookup, fk_relationships)
        relationships.extend(naming_relationships)
        
        # Step 3: Detect relationships by column similarity
        similarity_relationships = self._detect_similarity_relationships(metadata, table_lookup, relationships)
        relationships.extend(similarity_relationships)
        
        # Build relationship graph
        self._build_relationship_graph(relationships)
        
        # Update metadata with detected relationships
        metadata.detected_relationships = relationships
        
        return metadata

    def _extract_fk_relationships(self, metadata: DatabaseMetadata) -> list[DetectedRelationship]:
        """Extract relationships from explicit foreign key constraints.
        
        Args:
            metadata: Database metadata
            
        Returns:
            List of detected relationships
        """
        relationships = []
        
        for table in metadata.tables:
            for fk in table.foreign_keys:
                rel = DetectedRelationship(
                    source_table=table.name,
                    source_column=fk.column,
                    target_table=fk.references_table,
                    target_column=fk.references_column,
                    confidence=RelationshipConfidence.HIGH,
                    detection_method="foreign_key_constraint",
                    reason=f"外键约束 {fk.constraint_name}: {table.name}.{fk.column} 引用 {fk.references_table}.{fk.references_column}"
                )
                relationships.append(rel)
        
        return relationships

    def _detect_naming_relationships(
        self, 
        metadata: DatabaseMetadata, 
        table_lookup: dict[str, TableInfo],
        existing_rels: list[DetectedRelationship]
    ) -> list[DetectedRelationship]:
        """Detect relationships by naming convention (xxx_id -> xxx table).
        
        Args:
            metadata: Database metadata
            table_lookup: Table lookup by name
            existing_rels: Already detected relationships (to avoid duplicates)
            
        Returns:
            List of detected relationships
        """
        relationships = []
        existing_pairs = {(r.source_table, r.source_column) for r in existing_rels}
        
        for table in metadata.tables:
            for column in table.columns:
                # Skip if already has explicit FK
                if (table.name, column.name) in existing_pairs:
                    continue
                
                # Skip primary key columns
                if column.is_primary_key:
                    continue
                
                # Check naming patterns
                potential_table = self._extract_table_name_from_column(column.name)
                
                if potential_table and potential_table in table_lookup:
                    target_table = table_lookup[potential_table]
                    
                    # Find matching primary key column
                    target_column = self._find_matching_pk_column(target_table)
                    
                    if target_column:
                        rel = DetectedRelationship(
                            source_table=table.name,
                            source_column=column.name,
                            target_table=potential_table,
                            target_column=target_column,
                            confidence=RelationshipConfidence.MEDIUM,
                            detection_method="naming_convention",
                            reason=f"列名 '{column.name}' 符合命名规则模式，可能引用表 '{potential_table}'"
                        )
                        relationships.append(rel)
        
        return relationships

    def _detect_similarity_relationships(
        self, 
        metadata: DatabaseMetadata,
        table_lookup: dict[str, TableInfo],
        existing_rels: list[DetectedRelationship]
    ) -> list[DetectedRelationship]:
        """Detect relationships by column name/type similarity.
        
        Args:
            metadata: Database metadata
            table_lookup: Table lookup by name
            existing_rels: Already detected relationships
            
        Returns:
            List of detected relationships
        """
        relationships = []
        existing_pairs = {(r.source_table, r.source_column) for r in existing_rels}
        
        for table in metadata.tables:
            for column in table.columns:
                # Skip if already has relationship
                if (table.name, column.name) in existing_pairs:
                    continue
                
                # Skip primary key columns
                if column.is_primary_key:
                    continue
                
                # Look for similar columns in other tables
                best_match = None
                best_score = 0.0
                
                for other_table in metadata.tables:
                    if other_table.name == table.name:
                        continue
                    
                    for other_col in other_table.columns:
                        # Only match to primary key columns
                        if not other_col.is_primary_key:
                            continue
                        
                        # Calculate similarity
                        score = self._calculate_column_similarity(column, other_col)
                        
                        if score >= self.config.similarity_threshold and score > best_score:
                            best_score = score
                            best_match = (other_table.name, other_col.name)
                
                if best_match:
                    rel = DetectedRelationship(
                        source_table=table.name,
                        source_column=column.name,
                        target_table=best_match[0],
                        target_column=best_match[1],
                        confidence=RelationshipConfidence.LOW,
                        detection_method="similarity_analysis",
                        reason=f"列 '{column.name}' 与 '{best_match[0]}.{best_match[1]}' 具有高相似度 ({best_score:.0%})，可能存在关联关系"
                    )
                    relationships.append(rel)
        
        return relationships

    def _extract_table_name_from_column(self, column_name: str) -> Optional[str]:
        """Extract potential table name from column name (e.g., user_id -> user).
        
        Args:
            column_name: Column name to analyze
            
        Returns:
            Potential table name or None
        """
        # Common patterns for foreign key columns
        patterns = [
            (r"^(.+)_id$", 1),           # user_id -> user
            (r"^(.+)_fk$", 1),            # user_fk -> user
            (r"^(.+)Id$", 1),             # userId -> user
            (r"^fk_(.+)$", 1),            # fk_user -> user
            (r"^id_(.+)$", 1),            # id_user -> user
        ]
        
        for pattern, group in patterns:
            match = re.match(pattern, column_name, re.IGNORECASE)
            if match:
                potential = match.group(group)
                # Convert to common table name formats
                return self._normalize_table_name(potential)
        
        return None

    def _normalize_table_name(self, name: str) -> str:
        """Normalize a potential table name.
        
        Args:
            name: Name to normalize
            
        Returns:
            Normalized table name
        """
        # Convert camelCase to snake_case
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name).lower()
        
        # Handle pluralization (simple cases)
        if not name.endswith('s'):
            return name + 's'  # user -> users
        return name

    def _find_matching_pk_column(self, table: TableInfo) -> Optional[str]:
        """Find a primary key column that could be referenced.
        
        Args:
            table: Table to search
            
        Returns:
            Primary key column name or None
        """
        # Prefer 'id' column
        for col in table.columns:
            if col.is_primary_key and col.name.lower() == 'id':
                return col.name
        
        # Otherwise return first PK column
        for col in table.columns:
            if col.is_primary_key:
                return col.name
        
        return None

    def _calculate_column_similarity(self, col1, col2) -> float:
        """Calculate similarity between two columns.
        
        Args:
            col1: First column
            col2: Second column
            
        Returns:
            Similarity score (0-1)
        """
        score = 0.0
        
        # Name similarity (50% weight)
        name_sim = SequenceMatcher(None, col1.name.lower(), col2.name.lower()).ratio()
        score += name_sim * 0.5
        
        # Type compatibility (30% weight)
        if self._types_compatible(col1.data_type, col2.data_type):
            score += 0.3
        
        # Both are integer types suggest FK relationship (20% weight)
        if self._is_integer_type(col1.data_type) and self._is_integer_type(col2.data_type):
            score += 0.2
        
        return score

    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two PostgreSQL types are compatible.
        
        Args:
            type1: First type
            type2: Second type
            
        Returns:
            True if types are compatible
        """
        t1 = type1.lower().split('(')[0].strip()
        t2 = type2.lower().split('(')[0].strip()
        
        # Direct match
        if t1 == t2:
            return True
        
        # Integer family
        int_types = {'integer', 'int', 'int4', 'smallint', 'bigint', 'int8', 'serial', 'bigserial'}
        if t1 in int_types and t2 in int_types:
            return True
        
        # Text family
        text_types = {'varchar', 'character varying', 'text', 'char', 'character', 'name'}
        if t1 in text_types and t2 in text_types:
            return True
        
        # UUID
        if 'uuid' in t1 and 'uuid' in t2:
            return True
        
        return False

    def _is_integer_type(self, data_type: str) -> bool:
        """Check if type is an integer type."""
        t = data_type.lower().split('(')[0].strip()
        return t in {'integer', 'int', 'int4', 'smallint', 'bigint', 'int8', 'serial', 'bigserial'}

    def _build_relationship_graph(self, relationships: list[DetectedRelationship]):
        """Build a NetworkX graph from relationships.
        
        Args:
            relationships: List of detected relationships
        """
        self.relationship_graph.clear()
        
        for rel in relationships:
            self.relationship_graph.add_edge(
                rel.source_table,
                rel.target_table,
                source_column=rel.source_column,
                target_column=rel.target_column,
                confidence=rel.confidence.value,
                method=rel.detection_method,
            )

    def get_join_path(self, from_table: str, to_table: str) -> Optional[list[tuple[str, str, str, str]]]:
        """Find the shortest join path between two tables.
        
        Args:
            from_table: Source table name
            to_table: Target table name
            
        Returns:
            List of (from_table, from_col, to_table, to_col) tuples or None
        """
        try:
            path = nx.shortest_path(self.relationship_graph, from_table, to_table)
        except nx.NetworkXNoPath:
            return None
        
        if len(path) < 2:
            return None
        
        joins = []
        for i in range(len(path) - 1):
            edge_data = self.relationship_graph.get_edge_data(path[i], path[i + 1])
            joins.append((
                path[i],
                edge_data['source_column'],
                path[i + 1],
                edge_data['target_column'],
            ))
        
        return joins

    def get_all_paths_from(self, table: str, max_depth: int = 3) -> dict[str, list[tuple]]:
        """Get all join paths from a table up to max depth.
        
        Args:
            table: Starting table
            max_depth: Maximum path length
            
        Returns:
            Dict mapping target tables to their join paths
        """
        paths = {}
        
        for target in self.relationship_graph.nodes():
            if target == table:
                continue
            
            path = self.get_join_path(table, target)
            if path and len(path) <= max_depth:
                paths[target] = path
        
        return paths

    def get_relationship_stats(self) -> dict:
        """Get statistics about detected relationships.
        
        Returns:
            Statistics dictionary
        """
        return {
            "total_tables": self.relationship_graph.number_of_nodes(),
            "total_relationships": self.relationship_graph.number_of_edges(),
            "isolated_tables": len(list(nx.isolates(self.relationship_graph))),
            "connected_components": nx.number_weakly_connected_components(self.relationship_graph),
        }


def analyze_relationships(metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None) -> DatabaseMetadata:
    """Convenience function to analyze relationships.
    
    Args:
        metadata: Database metadata
        config: Analysis configuration
        
    Returns:
        Updated metadata with relationships
    """
    analyzer = RelationshipAnalyzer(config)
    return analyzer.analyze(metadata)
