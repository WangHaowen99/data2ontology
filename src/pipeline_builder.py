"""Pipeline builder for automatic join path and transformation generation."""

from typing import Optional
import uuid
import networkx as nx

from .config import AnalysisConfig
from .models.metadata import DatabaseMetadata, TableInfo, DetectedRelationship, RelationshipConfidence
from .models.pipeline import (
    JoinType,
    JoinCondition,
    ColumnMapping,
    PipelineStep,
    Pipeline,
    Dataset,
    JoinPath,
)


class PipelineBuilder:
    """Builds data transformation pipelines from database metadata."""

    def __init__(self, metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None):
        """Initialize the pipeline builder.
        
        Args:
            metadata: Database metadata with detected relationships
            config: Analysis configuration
        """
        self.metadata = metadata
        self.config = config or AnalysisConfig()
        self.relationship_graph = self._build_relationship_graph()
        self._table_lookup = {t.name: t for t in metadata.tables}

    def _build_relationship_graph(self) -> nx.DiGraph:
        """Build a directed graph from relationships."""
        graph = nx.DiGraph()
        
        # Add all tables as nodes
        for table in self.metadata.tables:
            graph.add_node(table.name, table=table)
        
        # Add relationships as edges (bidirectional for join path finding)
        for rel in self.metadata.detected_relationships:
            # Confidence to weight mapping (lower weight = higher confidence)
            weight = {
                RelationshipConfidence.HIGH: 1,
                RelationshipConfidence.MEDIUM: 2,
                RelationshipConfidence.LOW: 3,
            }.get(rel.confidence, 3)
            
            graph.add_edge(
                rel.source_table,
                rel.target_table,
                source_column=rel.source_column,
                target_column=rel.target_column,
                confidence=rel.confidence,
                weight=weight,
            )
            
            # Add reverse edge for join path finding (undirected in terms of joins)
            graph.add_edge(
                rel.target_table,
                rel.source_table,
                source_column=rel.target_column,
                target_column=rel.source_column,
                confidence=rel.confidence,
                weight=weight,
            )
        
        return graph

    def find_join_path(self, from_table: str, to_table: str) -> Optional[JoinPath]:
        """Find the optimal join path between two tables.
        
        Args:
            from_table: Source table name
            to_table: Target table name
            
        Returns:
            JoinPath object or None if no path exists
        """
        try:
            path = nx.dijkstra_path(self.relationship_graph, from_table, to_table, weight='weight')
            total_cost = nx.dijkstra_path_length(self.relationship_graph, from_table, to_table, weight='weight')
        except nx.NetworkXNoPath:
            return None
        
        if len(path) < 2:
            return None
        
        joins = []
        for i in range(len(path) - 1):
            edge_data = self.relationship_graph.get_edge_data(path[i], path[i + 1])
            join_condition = JoinCondition(
                left_table=path[i],
                left_column=edge_data['source_column'],
                right_table=path[i + 1],
                right_column=edge_data['target_column'],
            )
            joins.append(join_condition)
        
        return JoinPath(tables=path, joins=joins, total_cost=total_cost)

    def find_all_join_paths(self, from_table: str, max_depth: int = 3) -> list[JoinPath]:
        """Find all join paths from a table.
        
        Args:
            from_table: Starting table
            max_depth: Maximum path length
            
        Returns:
            List of JoinPath objects
        """
        paths = []
        
        for target_table in self.metadata.tables:
            if target_table.name == from_table:
                continue
            
            path = self.find_join_path(from_table, target_table.name)
            if path and len(path.tables) <= max_depth + 1:
                paths.append(path)
        
        # Sort by cost (lower is better)
        paths.sort(key=lambda p: p.total_cost)
        
        return paths

    def create_pipeline(
        self,
        name: str,
        source_tables: list[str],
        join_type: JoinType = JoinType.LEFT,
        include_all_columns: bool = True,
        selected_columns: Optional[dict[str, list[str]]] = None,
    ) -> Pipeline:
        """Create a pipeline to join multiple tables.
        
        Args:
            name: Pipeline name
            source_tables: List of tables to join
            join_type: Type of join to use
            include_all_columns: Whether to include all columns
            selected_columns: Optional dict of table -> column names to include
            
        Returns:
            Pipeline object
        """
        if len(source_tables) < 2:
            raise ValueError("At least 2 tables required to create a join pipeline")
        
        pipeline_id = f"pipeline_{uuid.uuid4().hex[:8]}"
        steps = []
        output_columns = []
        
        # Create join steps for each pair of tables
        base_table = source_tables[0]
        
        for i in range(1, len(source_tables)):
            target_table = source_tables[i]
            
            # Find join path
            join_path = self.find_join_path(base_table, target_table)
            
            if not join_path:
                # Try reverse
                join_path = self.find_join_path(target_table, base_table)
            
            if not join_path:
                raise ValueError(f"无法找到表 '{base_table}' 和 '{target_table}' 之间的连接路径")
            
            # Create join step
            step = PipelineStep(
                step_id=f"step_{i}",
                step_name=f"Join with {target_table}",
                step_type="join",
                description=f"将 {base_table} 与 {target_table} 进行 {join_type.value} JOIN",
                join_type=join_type,
                join_conditions=join_path.joins,
            )
            steps.append(step)
            
            # Update base table for next iteration (use the intermediate result)
            base_table = target_table
        
        # Create column mappings
        for table_name in source_tables:
            table = self._table_lookup.get(table_name)
            if not table:
                continue
            
            columns_to_include = table.columns
            if selected_columns and table_name in selected_columns:
                columns_to_include = [c for c in table.columns if c.name in selected_columns[table_name]]
            
            for col in columns_to_include:
                # Add table prefix to avoid naming conflicts
                target_name = f"{table_name}_{col.name}" if len(source_tables) > 1 else col.name
                
                mapping = ColumnMapping(
                    source_table=table_name,
                    source_column=col.name,
                    target_name=target_name,
                )
                output_columns.append(mapping)
        
        return Pipeline(
            pipeline_id=pipeline_id,
            name=name,
            description=f"自动生成的管道，连接表: {', '.join(source_tables)}",
            source_tables=source_tables,
            steps=steps,
            output_columns=output_columns,
        )

    def create_star_schema_pipeline(self, fact_table: str) -> Pipeline:
        """Create a pipeline for a star schema with fact table and all related dimension tables.
        
        Args:
            fact_table: The central fact table name
            
        Returns:
            Pipeline joining fact table with all related dimension tables
        """
        # Find all directly related tables
        related_tables = [fact_table]
        
        for rel in self.metadata.detected_relationships:
            if rel.source_table == fact_table and rel.target_table not in related_tables:
                related_tables.append(rel.target_table)
        
        if len(related_tables) < 2:
            raise ValueError(f"表 '{fact_table}' 没有检测到足够的关联表来创建星型模式管道")
        
        return self.create_pipeline(
            name=f"{fact_table}_star_schema",
            source_tables=related_tables,
            join_type=JoinType.LEFT,
        )

    def generate_datasets(self) -> list[Dataset]:
        """Generate suggested datasets based on table relationships.
        
        Returns:
            List of suggested Dataset objects
        """
        datasets = []
        processed_pairs = set()
        
        # Generate dataset for each high-confidence relationship
        for rel in self.metadata.detected_relationships:
            if rel.confidence == RelationshipConfidence.HIGH:
                pair_key = tuple(sorted([rel.source_table, rel.target_table]))
                
                if pair_key in processed_pairs:
                    continue
                
                processed_pairs.add(pair_key)
                
                try:
                    pipeline = self.create_pipeline(
                        name=f"{rel.source_table}_{rel.target_table}_joined",
                        source_tables=[rel.source_table, rel.target_table],
                    )
                    
                    dataset = Dataset(
                        dataset_id=f"ds_{uuid.uuid4().hex[:8]}",
                        name=f"{rel.source_table}_{rel.target_table}_dataset",
                        description=f"通过 {rel.source_column} -> {rel.target_column} 关系连接的数据集",
                        source_pipeline=pipeline.pipeline_id,
                        columns=pipeline.output_columns,
                        creation_reason=f"基于外键关系 {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column} 自动生成"
                    )
                    datasets.append(dataset)
                except Exception:
                    pass
        
        return datasets

    def get_join_recommendations(self) -> list[dict]:
        """Get recommendations for table joins based on relationship analysis.
        
        Returns:
            List of recommendation dictionaries
        """
        recommendations = []
        
        # Group tables by connectivity
        for table in self.metadata.tables:
            related_count = 0
            direct_relations = []
            
            for rel in self.metadata.detected_relationships:
                if rel.source_table == table.name:
                    related_count += 1
                    direct_relations.append({
                        "target": rel.target_table,
                        "via": f"{rel.source_column} -> {rel.target_column}",
                        "confidence": rel.confidence.value,
                    })
            
            if related_count >= 2:
                recommendations.append({
                    "table": table.name,
                    "type": "hub_table",
                    "description": f"表 '{table.name}' 是一个枢纽表，连接了 {related_count} 个其他表",
                    "relations": direct_relations,
                    "suggestion": f"建议创建以 '{table.name}' 为核心的星型模式数据集",
                })
            elif related_count == 0:
                recommendations.append({
                    "table": table.name,
                    "type": "isolated_table",
                    "description": f"表 '{table.name}' 是孤立表，没有检测到与其他表的关系",
                    "suggestion": "建议检查是否缺少外键约束或命名不符合规范",
                })
        
        return recommendations


def build_pipelines(metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None) -> list[Pipeline]:
    """Convenience function to build all suggested pipelines.
    
    Args:
        metadata: Database metadata
        config: Analysis configuration
        
    Returns:
        List of Pipeline objects
    """
    builder = PipelineBuilder(metadata, config)
    
    pipelines = []
    processed_pairs = set()
    
    for rel in metadata.detected_relationships:
        if rel.confidence == RelationshipConfidence.HIGH:
            pair_key = tuple(sorted([rel.source_table, rel.target_table]))
            
            if pair_key in processed_pairs:
                continue
            
            processed_pairs.add(pair_key)
            
            try:
                pipeline = builder.create_pipeline(
                    name=f"{rel.source_table}_{rel.target_table}_pipeline",
                    source_tables=[rel.source_table, rel.target_table],
                )
                pipelines.append(pipeline)
            except Exception:
                pass
    
    return pipelines
