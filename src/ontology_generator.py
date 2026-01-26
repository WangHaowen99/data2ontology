"""Ontology generator from database metadata."""

from typing import Optional
from datetime import datetime
import uuid
import re

from .config import AnalysisConfig
from .models.metadata import (
    DatabaseMetadata, 
    EnhancedDatabaseMetadata,
    TableInfo, 
    DetectedRelationship, 
    RelationshipConfidence,
    EntityInsight,
    InsightSource,
)
from .models.ontology import (
    PropertyType,
    ObjectType,
    LinkType,
    Ontology,
    OntologyDataType,
    map_pg_type_to_ontology,
)


class OntologyGenerator:
    """Generates Ontology definitions from database metadata."""

    def __init__(self, metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None):
        """Initialize the ontology generator.
        
        Args:
            metadata: Database metadata with detected relationships (can be EnhancedDatabaseMetadata)
            config: Analysis configuration
        """
        self.metadata = metadata
        self.config = config or AnalysisConfig()
        
        # Check if we have enhanced metadata with unstructured insights
        self.is_enhanced = isinstance(metadata, EnhancedDatabaseMetadata)
        if self.is_enhanced:
            self.entity_insights_map = {
                self._normalize_name(ei.table_name or ei.entity_name): ei 
                for ei in metadata.entity_insights
            }

    def generate(self) -> Ontology:
        """Generate complete Ontology from metadata.
        
        Returns:
            Ontology object
        """
        object_types = []
        link_types = []
        
        # Generate object types from tables
        for table in self.metadata.tables:
            obj_type = self._generate_object_type(table)
            object_types.append(obj_type)
        
        # Generate link types from relationships
        for rel in self.metadata.detected_relationships:
            link_type = self._generate_link_type(rel)
            link_types.append(link_type)
        
        return Ontology(
            name=f"{self.metadata.database_name}_ontology",
            description=f"从数据库 '{self.metadata.database_name}' 自动生成的 Ontology",
            version="1.0.0",
            source_database=self.metadata.database_name,
            object_types=object_types,
            link_types=link_types,
            created_at=datetime.now().isoformat(),
        )

    def _generate_object_type(self, table: TableInfo) -> ObjectType:
        """Generate an ObjectType from a table.
        
        Args:
            table: Table information
            
        Returns:
            ObjectType object
        """
        # Generate object type ID and name
        obj_id = self._to_pascal_case(table.name)
        obj_name = self._humanize_name(table.name)
        
        # Generate properties from columns
        properties = []
        for column in table.columns:
            prop = self._generate_property(table.name, column)
            properties.append(prop)
        
        # Determine creation reason
        reason = self._generate_object_type_reason(table)
        
        # Check for insights from unstructured data
        insights_from_code = None
        insights_from_logs = None
        if self.is_enhanced:
            entity_insight = self.entity_insights_map.get(self._normalize_name(table.name))
            if entity_insight:
                if InsightSource.CODE in entity_insight.sources:
                    insights_from_code = entity_insight.description_from_code or \
                        f"代码中发现此实体，相关实体: {', '.join(entity_insight.related_entities[:3])}"
                if InsightSource.LOG in entity_insight.sources:
                    ops = entity_insight.operations_from_logs
                    if ops:
                        insights_from_logs = f"日志中检测到操作: {', '.join(ops)}"
        
        return ObjectType(
            id=obj_id,
            name=obj_name,
            description=table.comment or f"从表 '{table.name}' 生成的实体类型",
            source_table=table.full_name,
            primary_key=[self._to_camel_case(pk) for pk in table.primary_keys],
            properties=properties,
            creation_reason=reason,
            insights_from_code=insights_from_code,
            insights_from_logs=insights_from_logs,
        )

    def _generate_property(self, table_name: str, column) -> PropertyType:
        """Generate a PropertyType from a column.
        
        Args:
            table_name: Parent table name
            column: Column information
            
        Returns:
            PropertyType object
        """
        prop_id = f"{self._to_pascal_case(table_name)}.{self._to_camel_case(column.name)}"
        prop_name = self._humanize_name(column.name)
        
        # Map PostgreSQL type to Ontology type
        ontology_type = map_pg_type_to_ontology(column.data_type)
        
        # Generate creation reason
        reason = f"从列 '{table_name}.{column.name}' ({column.data_type}) 映射而来"
        if column.is_primary_key:
            reason += "，作为主键标识"
        
        return PropertyType(
            id=prop_id,
            name=prop_name,
            data_type=ontology_type,
            description=column.comment or f"属性 {prop_name}",
            source_table=table_name,
            source_column=column.name,
            nullable=column.nullable,
            is_primary_key=column.is_primary_key,
            creation_reason=reason,
        )

    def _generate_link_type(self, rel: DetectedRelationship) -> LinkType:
        """Generate a LinkType from a relationship.
        
        Args:
            rel: Detected relationship
            
        Returns:
            LinkType object
        """
        # Generate link name
        source_name = self._to_pascal_case(rel.source_table)
        target_name = self._to_pascal_case(rel.target_table)
        
        # Create semantic link name
        link_name = self._generate_link_name(rel)
        link_id = f"{source_name}_to_{target_name}"
        
        # Determine cardinality (usually many-to-one for FK relationships)
        cardinality = "many-to-one"
        
        # Check for insights from unstructured data
        insights_from_code = None
        insights_from_logs = None
        if self.is_enhanced:
            # Look for matching relationship insights
            source_norm = self._normalize_name(rel.source_table)
            target_norm = self._normalize_name(rel.target_table)
            
            for rel_insight in self.metadata.relationship_insights:
                if (self._normalize_name(rel_insight.source_entity) == source_norm and 
                    self._normalize_name(rel_insight.target_entity) == target_norm):
                    
                    if InsightSource.CODE in rel_insight.sources:
                        code_evidence = [e for e in rel_insight.evidence if "代码" in e or "code" in e.lower()]
                        if code_evidence:
                            insights_from_code = "; ".join(code_evidence[:2])
                    
                    if InsightSource.LOG in rel_insight.sources:
                        log_evidence = [e for e in rel_insight.evidence if "日志" in e or "log" in e.lower()]
                        if log_evidence:
                            insights_from_logs = "; ".join(log_evidence[:2])
                    break
        
        return LinkType(
            id=link_id,
            name=link_name,
            description=f"从 {rel.source_table} 到 {rel.target_table} 的关联关系",
            source_object_type=source_name,
            target_object_type=target_name,
            cardinality=cardinality,
            source_property=self._to_camel_case(rel.source_column),
            confidence=rel.confidence.value,
            creation_reason=rel.reason,
            insights_from_code=insights_from_code,
            insights_from_logs=insights_from_logs,
        )

    def _generate_link_name(self, rel: DetectedRelationship) -> str:
        """Generate a semantic link name for a relationship.
        
        Args:
            rel: Detected relationship
            
        Returns:
            Human-readable link name
        """
        source_col = rel.source_column.lower()
        target_table = rel.target_table.lower()
        
        # Common patterns for link names
        if source_col.endswith('_id') or source_col.endswith('id'):
            # user_id -> hasUser
            base = source_col.replace('_id', '').replace('id', '').strip('_')
            if base:
                return f"has{self._to_pascal_case(base)}"
        
        # Default pattern
        return f"relatedTo{self._to_pascal_case(target_table)}"

    def _generate_object_type_reason(self, table: TableInfo) -> str:
        """Generate a reason for creating this object type.
        
        Args:
            table: Table information
            
        Returns:
            Explanation string
        """
        reasons = []
        
        # Basic reason
        reasons.append(f"表 '{table.name}' 代表业务实体")
        
        # Column count
        reasons.append(f"包含 {len(table.columns)} 个属性")
        
        # Primary key
        if table.primary_keys:
            reasons.append(f"使用 '{', '.join(table.primary_keys)}' 作为唯一标识")
        
        # Foreign keys
        if table.foreign_keys:
            fk_targets = [fk.references_table for fk in table.foreign_keys]
            reasons.append(f"与表 {', '.join(fk_targets)} 存在关联")
        
        # Row count
        if table.row_count_estimate:
            reasons.append(f"预估包含 {table.row_count_estimate:,} 条记录")
        
        return "；".join(reasons)

    def _to_pascal_case(self, name: str) -> str:
        """Convert name to PascalCase.
        
        Args:
            name: Snake_case or other format name
            
        Returns:
            PascalCase name
        """
        # Handle snake_case
        parts = name.split('_')
        return ''.join(word.capitalize() for word in parts if word)

    def _to_camel_case(self, name: str) -> str:
        """Convert name to camelCase.
        
        Args:
            name: Snake_case or other format name
            
        Returns:
            camelCase name
        """
        pascal = self._to_pascal_case(name)
        if pascal:
            return pascal[0].lower() + pascal[1:]
        return name

    def _humanize_name(self, name: str) -> str:
        """Convert name to human-readable format.
        
        Args:
            name: Technical name
            
        Returns:
            Human-readable name
        """
        # Split by underscore and capitalize each word
        parts = name.replace('_', ' ').split()
        return ' '.join(word.capitalize() for word in parts)

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for matching with insights.
        
        Args:
            name: Entity name
            
        Returns:
            Normalized name
        """
        return name.lower().strip().rstrip('s')

    def get_ontology_summary(self, ontology: Ontology) -> dict:
        """Get a summary of the ontology.
        
        Args:
            ontology: Generated ontology
            
        Returns:
            Summary dictionary
        """
        high_conf_links = len([l for l in ontology.link_types if l.confidence == "high"])
        medium_conf_links = len([l for l in ontology.link_types if l.confidence == "medium"])
        low_conf_links = len([l for l in ontology.link_types if l.confidence == "low"])
        
        return {
            "object_types_count": ontology.object_type_count,
            "total_properties_count": ontology.total_property_count,
            "link_types_count": ontology.link_type_count,
            "high_confidence_links": high_conf_links,
            "medium_confidence_links": medium_conf_links,
            "low_confidence_links": low_conf_links,
        }


def generate_ontology(metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None) -> Ontology:
    """Convenience function to generate ontology.
    
    Args:
        metadata: Database metadata
        config: Analysis configuration
        
    Returns:
        Generated Ontology
    """
    generator = OntologyGenerator(metadata, config)
    return generator.generate()
