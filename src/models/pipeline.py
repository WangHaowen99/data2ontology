"""Pipeline models for data transformation definitions."""

from typing import Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


class JoinType(str, Enum):
    """SQL join types."""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class AggregationType(str, Enum):
    """Aggregation function types."""
    SUM = "SUM"
    COUNT = "COUNT"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    FIRST = "FIRST"
    LAST = "LAST"
    ARRAY_AGG = "ARRAY_AGG"
    STRING_AGG = "STRING_AGG"


class JoinCondition(BaseModel):
    """A single join condition between two tables."""
    left_table: str = Field(..., description="Left table name")
    left_column: str = Field(..., description="Left column name")
    right_table: str = Field(..., description="Right table name")
    right_column: str = Field(..., description="Right column name")
    operator: str = Field("=", description="Comparison operator")

    def to_sql(self) -> str:
        """Generate SQL join condition."""
        return f"{self.left_table}.{self.left_column} {self.operator} {self.right_table}.{self.right_column}"


class ColumnMapping(BaseModel):
    """Mapping from source column to target column."""
    source_table: str = Field(..., description="Source table name")
    source_column: str = Field(..., description="Source column name")
    target_name: str = Field(..., description="Target column name in output")
    alias: Optional[str] = Field(None, description="Optional alias")
    transformation: Optional[str] = Field(None, description="SQL transformation expression")
    aggregation: Optional[AggregationType] = Field(None, description="Aggregation function if any")

    def to_sql(self) -> str:
        """Generate SQL select expression."""
        if self.transformation:
            expr = self.transformation
        elif self.aggregation:
            expr = f"{self.aggregation.value}({self.source_table}.{self.source_column})"
        else:
            expr = f"{self.source_table}.{self.source_column}"
        
        if self.alias:
            return f"{expr} AS {self.alias}"
        elif self.target_name != self.source_column:
            return f"{expr} AS {self.target_name}"
        return expr


class PipelineStep(BaseModel):
    """A single step in a data pipeline."""
    step_id: str = Field(..., description="Unique step identifier")
    step_name: str = Field(..., description="Human-readable step name")
    step_type: str = Field(..., description="Type of step: join, transform, filter, aggregate")
    description: str = Field(..., description="What this step does")
    
    # Join-specific fields
    join_type: Optional[JoinType] = Field(None, description="Type of join if this is a join step")
    join_conditions: list[JoinCondition] = Field(default_factory=list, description="Join conditions")
    
    # Column mapping
    column_mappings: list[ColumnMapping] = Field(default_factory=list, description="Column mappings")
    
    # Filter
    filter_condition: Optional[str] = Field(None, description="SQL WHERE condition")
    
    # Aggregation
    group_by_columns: list[str] = Field(default_factory=list, description="GROUP BY columns")

    def to_sql_fragment(self) -> str:
        """Generate SQL fragment for this step."""
        if self.step_type == "join" and self.join_conditions:
            join_clause = f"{self.join_type.value} JOIN" if self.join_type else "JOIN"
            conditions = " AND ".join(jc.to_sql() for jc in self.join_conditions)
            # Get the right table from the first join condition
            right_table = self.join_conditions[0].right_table
            return f"{join_clause} {right_table} ON {conditions}"
        elif self.step_type == "filter" and self.filter_condition:
            return f"WHERE {self.filter_condition}"
        return ""


class JoinPath(BaseModel):
    """A path of joins between tables."""
    tables: list[str] = Field(..., description="Ordered list of tables in the path")
    joins: list[JoinCondition] = Field(..., description="Join conditions along the path")
    total_cost: float = Field(0.0, description="Estimated cost of this join path")


class Pipeline(BaseModel):
    """A complete data transformation pipeline."""
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Pipeline name")
    description: str = Field(..., description="Pipeline description")
    source_tables: list[str] = Field(..., description="Input tables")
    steps: list[PipelineStep] = Field(default_factory=list, description="Pipeline steps")
    output_columns: list[ColumnMapping] = Field(default_factory=list, description="Final output columns")

    def to_sql(self) -> str:
        """Generate complete SQL query for this pipeline."""
        if not self.source_tables:
            return ""
        
        # SELECT clause
        select_parts = []
        for mapping in self.output_columns:
            select_parts.append(mapping.to_sql())
        select_clause = "SELECT " + ",\n       ".join(select_parts) if select_parts else "SELECT *"
        
        # FROM clause
        from_clause = f"FROM {self.source_tables[0]}"
        
        # JOIN clauses
        join_clauses = []
        filter_clauses = []
        group_by_clause = ""
        
        for step in self.steps:
            if step.step_type == "join":
                join_clauses.append(step.to_sql_fragment())
            elif step.step_type == "filter" and step.filter_condition:
                filter_clauses.append(step.filter_condition)
            elif step.step_type == "aggregate" and step.group_by_columns:
                group_by_clause = "GROUP BY " + ", ".join(step.group_by_columns)
        
        # Build complete query
        parts = [select_clause, from_clause]
        parts.extend(join_clauses)
        if filter_clauses:
            parts.append("WHERE " + " AND ".join(filter_clauses))
        if group_by_clause:
            parts.append(group_by_clause)
        
        return "\n".join(parts)


class Dataset(BaseModel):
    """A generated dataset from a pipeline."""
    dataset_id: str = Field(..., description="Unique dataset identifier")
    name: str = Field(..., description="Dataset name")
    description: str = Field(..., description="Dataset description")
    source_pipeline: str = Field(..., description="Pipeline that generates this dataset")
    columns: list[ColumnMapping] = Field(default_factory=list, description="Dataset columns")
    row_count_estimate: Optional[int] = Field(None, description="Estimated row count")
    creation_reason: str = Field(..., description="Why this dataset was created")

    def get_column_names(self) -> list[str]:
        """Get list of column names in the dataset."""
        return [c.target_name for c in self.columns]
