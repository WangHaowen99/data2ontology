"""Unstructured data analyzer - integrates insights from logs and code with metadata."""

from typing import Optional
from collections import defaultdict
from difflib import SequenceMatcher

from .models.metadata import (
    DatabaseMetadata,
    EnhancedDatabaseMetadata,
    LogInsight,
    CodeInsight,
    EntityInsight,
    RelationshipInsight,
    InsightSource,
)
from .log_analyzer import LogAnalyzer, analyze_logs
from .code_analyzer import CodeAnalyzer, analyze_code


class UnstructuredAnalyzer:
    """Integrates insights from logs, code, and database metadata."""

    def __init__(self, similarity_threshold: float = 0.7):
        """Initialize the unstructured analyzer.
        
        Args:
            similarity_threshold: Minimum similarity score for name matching (0-1)
        """
        self.similarity_threshold = similarity_threshold

    def analyze(
        self,
        metadata: DatabaseMetadata,
        log_paths: Optional[list[str]] = None,
        code_paths: Optional[list[str]] = None,
        log_max_lines: int = 10000,
        code_languages: Optional[list[str]] = None,
        code_exclude_patterns: Optional[list[str]] = None,
    ) -> EnhancedDatabaseMetadata:
        """Analyze and integrate all data sources.
        
        Args:
            metadata: Database metadata
            log_paths: Optional paths to log files
            code_paths: Optional paths to code directories/files
            log_max_lines: Maximum log lines to process per file
            code_languages: Languages to analyze in code
            code_exclude_patterns: Patterns to exclude from code analysis
            
        Returns:
            EnhancedDatabaseMetadata with integrated insights
        """
        # Analyze logs if paths provided
        log_insights = None
        if log_paths:
            log_insights = analyze_logs(log_paths, max_lines=log_max_lines)
        
        # Analyze code if paths provided
        code_insights = None
        if code_paths:
            code_insights = analyze_code(
                code_paths,
                languages=code_languages,
                exclude_patterns=code_exclude_patterns
            )
        
        # Create enhanced metadata
        enhanced = EnhancedDatabaseMetadata(
            database_name=metadata.database_name,
            tables=metadata.tables,
            detected_relationships=metadata.detected_relationships,
            log_insights=log_insights,
            code_insights=code_insights,
        )
        
        # Generate entity insights by combining all sources
        enhanced.entity_insights = self._generate_entity_insights(
            metadata, log_insights, code_insights
        )
        
        # Generate relationship insights
        enhanced.relationship_insights = self._generate_relationship_insights(
            metadata, log_insights, code_insights
        )
        
        return enhanced

    def _generate_entity_insights(
        self,
        metadata: DatabaseMetadata,
        log_insights: Optional[LogInsight],
        code_insights: Optional[CodeInsight],
    ) -> list[EntityInsight]:
        """Generate combined entity insights from all sources.
        
        Args:
            metadata: Database metadata
            log_insights: Insights from logs
            code_insights: Insights from code
            
        Returns:
            List of EntityInsight objects
        """
        insights = []
        
        # Map: entity_name -> EntityInsight
        entity_map = {}
        
        # Add entities from database tables
        for table in metadata.tables:
            entity_name = self._normalize_name(table.name)
            insight = EntityInsight(
                entity_name=entity_name,
                table_name=table.name,
                sources=[InsightSource.METADATA],
                confidence=1.0
            )
            entity_map[entity_name] = insight
        
        # Integrate code insights
        if code_insights:
            for code_entity in code_insights.entities:
                entity_name = self._normalize_name(code_entity.name)
                
                # Try to match with existing table
                matched_table = self._find_matching_table(code_entity.name, metadata.tables)
                
                if entity_name in entity_map:
                    # Enhance existing insight
                    insight = entity_map[entity_name]
                    if InsightSource.CODE not in insight.sources:
                        insight.sources.append(InsightSource.CODE)
                    insight.description_from_code = code_entity.description
                    if code_entity.relationships:
                        insight.related_entities.extend(
                            self._normalize_name(r) for r in code_entity.relationships
                        )
                else:
                    # Create new insight from code
                    insight = EntityInsight(
                        entity_name=entity_name,
                        table_name=matched_table.name if matched_table else None,
                        sources=[InsightSource.CODE],
                        description_from_code=code_entity.description,
                        related_entities=[self._normalize_name(r) for r in code_entity.relationships],
                        confidence=0.8 if matched_table else 0.6
                    )
                    entity_map[entity_name] = insight
        
        # Integrate log insights
        if log_insights:
            # Get entity statistics from logs
            entity_stats = defaultdict(lambda: {
                "operations": set(),
                "related": set()
            })
            
            # Collect operations
            for pattern in log_insights.operation_patterns:
                for entity in pattern.entities_involved:
                    entity_stats[entity]["operations"].add(pattern.operation_type)
            
            # Collect cooccurrences
            for entity, related_list in log_insights.entity_cooccurrences.items():
                entity_stats[entity]["related"].update(related_list)
            
            # Update insights
            for entity, stats in entity_stats.items():
                entity_name = self._normalize_name(entity)
                
                # Try to match with existing table
                matched_table = self._find_matching_table(entity, metadata.tables)
                
                if entity_name in entity_map:
                    # Enhance existing insight
                    insight = entity_map[entity_name]
                    if InsightSource.LOG not in insight.sources:
                        insight.sources.append(InsightSource.LOG)
                    insight.operations_from_logs = list(stats["operations"])
                    insight.related_entities.extend(
                        self._normalize_name(r) for r in stats["related"]
                    )
                else:
                    # Create new insight from logs
                    insight = EntityInsight(
                        entity_name=entity_name,
                        table_name=matched_table.name if matched_table else None,
                        sources=[InsightSource.LOG],
                        operations_from_logs=list(stats["operations"]),
                        related_entities=[self._normalize_name(r) for r in stats["related"]],
                        confidence=0.7 if matched_table else 0.5
                    )
                    entity_map[entity_name] = insight
        
        # Deduplicate related entities
        for insight in entity_map.values():
            insight.related_entities = list(set(insight.related_entities))
        
        return list(entity_map.values())

    def _generate_relationship_insights(
        self,
        metadata: DatabaseMetadata,
        log_insights: Optional[LogInsight],
        code_insights: Optional[CodeInsight],
    ) -> list[RelationshipInsight]:
        """Generate combined relationship insights from all sources.
        
        Args:
            metadata: Database metadata
            log_insights: Insights from logs
            code_insights: Insights from code
            
        Returns:
            List of RelationshipInsight objects
        """
        insights = []
        
        # Track: (source, target) -> RelationshipInsight
        relationship_map = {}
        
        # Add relationships from database metadata (foreign keys)
        for rel in metadata.detected_relationships:
            source = self._normalize_name(rel.source_table)
            target = self._normalize_name(rel.target_table)
            key = (source, target)
            
            insight = RelationshipInsight(
                source_entity=source,
                target_entity=target,
                relationship_type=rel.detection_method,
                sources=[InsightSource.METADATA],
                evidence=[rel.reason],
                confidence=self._confidence_to_float(rel.confidence.value)
            )
            relationship_map[key] = insight
        
        # Add relationships from code
        if code_insights:
            for entity_name, related_list in code_insights.entity_relationships.items():
                source = self._normalize_name(entity_name)
                
                for related in related_list:
                    target = self._normalize_name(related)
                    key = (source, target)
                    
                    evidence = f"在代码中检测到 {entity_name} 引用 {related}"
                    
                    if key in relationship_map:
                        # Enhance existing relationship
                        insight = relationship_map[key]
                        if InsightSource.CODE not in insight.sources:
                            insight.sources.append(InsightSource.CODE)
                        insight.evidence.append(evidence)
                        # Increase confidence
                        insight.confidence = min(1.0, insight.confidence + 0.1)
                    else:
                        # Create new relationship from code
                        insight = RelationshipInsight(
                            source_entity=source,
                            target_entity=target,
                            relationship_type="code_reference",
                            sources=[InsightSource.CODE],
                            evidence=[evidence],
                            confidence=0.7
                        )
                        relationship_map[key] = insight
        
        # Add relationships from logs (cooccurrences)
        if log_insights:
            for entity, related_list in log_insights.entity_cooccurrences.items():
                source = self._normalize_name(entity)
                
                for related in related_list:
                    target = self._normalize_name(related)
                    key = (source, target)
                    
                    evidence = f"在日志中经常同时出现 {entity} 和 {related}"
                    
                    if key in relationship_map:
                        # Enhance existing relationship
                        insight = relationship_map[key]
                        if InsightSource.LOG not in insight.sources:
                            insight.sources.append(InsightSource.LOG)
                        insight.evidence.append(evidence)
                        # Slight confidence boost
                        insight.confidence = min(1.0, insight.confidence + 0.05)
                    else:
                        # Create new relationship from logs
                        insight = RelationshipInsight(
                            source_entity=source,
                            target_entity=target,
                            relationship_type="log_cooccurrence",
                            sources=[InsightSource.LOG],
                            evidence=[evidence],
                            confidence=0.5
                        )
                        relationship_map[key] = insight
        
        return list(relationship_map.values())

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for comparison.
        
        Args:
            name: Entity name
            
        Returns:
            Normalized name
        """
        # Convert to lowercase and remove common suffixes
        normalized = name.lower()
        suffixes = ["entity", "model", "table", "dto", "s"]
        
        for suffix in suffixes:
            if normalized.endswith(suffix) and len(normalized) > len(suffix):
                # Only remove if it's a clear suffix (preceded by underscore or different case)
                if suffix == "s":
                    # Handle plural
                    normalized = normalized.rstrip("s")
                elif normalized.endswith("_" + suffix):
                    normalized = normalized[:-len(suffix)-1]
        
        return normalized.strip("_")

    def _find_matching_table(self, entity_name: str, tables: list) -> Optional:
        """Find a database table matching the entity name.
        
        Args:
            entity_name: Entity name from code or logs
            tables: List of TableInfo objects
            
        Returns:
            Matching TableInfo or None
        """
        normalized_entity = self._normalize_name(entity_name)
        
        for table in tables:
            normalized_table = self._normalize_name(table.name)
            
            # Exact match
            if normalized_entity == normalized_table:
                return table
            
            # Similarity match
            similarity = self._calculate_similarity(normalized_entity, normalized_table)
            if similarity >= self.similarity_threshold:
                return table
        
        return None

    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two names.
        
        Args:
            name1: First name
            name2: Second name
            
        Returns:
            Similarity score 0-1
        """
        return SequenceMatcher(None, name1, name2).ratio()

    def _confidence_to_float(self, confidence_str: str) -> float:
        """Convert confidence string to float.
        
        Args:
            confidence_str: Confidence level (high, medium, low)
            
        Returns:
            Float value
        """
        mapping = {
            "high": 1.0,
            "medium": 0.8,
            "low": 0.6
        }
        return mapping.get(confidence_str.lower(), 0.5)


def analyze_unstructured(
    metadata: DatabaseMetadata,
    log_paths: Optional[list[str]] = None,
    code_paths: Optional[list[str]] = None,
    **kwargs
) -> EnhancedDatabaseMetadata:
    """Convenience function to analyze unstructured data.
    
    Args:
        metadata: Database metadata
        log_paths: Optional log file paths
        code_paths: Optional code paths
        **kwargs: Additional arguments for analyzers
        
    Returns:
        EnhancedDatabaseMetadata
    """
    analyzer = UnstructuredAnalyzer()
    return analyzer.analyze(metadata, log_paths, code_paths, **kwargs)
