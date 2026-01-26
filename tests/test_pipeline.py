"""Unit tests for pipeline models."""

import pytest
from src.models.pipeline import (
    JoinType,
    JoinCondition,
    ColumnMapping,
    PipelineStep,
    Pipeline,
    Dataset,
)


class TestJoinCondition:
    """Tests for JoinCondition model."""
    
    def test_create_join_condition(self):
        cond = JoinCondition(
            left_table="orders",
            left_column="user_id",
            right_table="users",
            right_column="id",
        )
        assert cond.operator == "="
        assert cond.to_sql() == "orders.user_id = users.id"
    
    def test_custom_operator(self):
        cond = JoinCondition(
            left_table="a",
            left_column="x",
            right_table="b",
            right_column="y",
            operator="<>",
        )
        assert cond.to_sql() == "a.x <> b.y"


class TestColumnMapping:
    """Tests for ColumnMapping model."""
    
    def test_simple_mapping(self):
        mapping = ColumnMapping(
            source_table="users",
            source_column="name",
            target_name="name",
        )
        assert mapping.to_sql() == "users.name"
    
    def test_mapping_with_alias(self):
        mapping = ColumnMapping(
            source_table="users",
            source_column="name",
            target_name="user_name",
            alias="user_name",
        )
        assert mapping.to_sql() == "users.name AS user_name"
    
    def test_mapping_with_rename(self):
        mapping = ColumnMapping(
            source_table="users",
            source_column="name",
            target_name="customer_name",
        )
        assert mapping.to_sql() == "users.name AS customer_name"


class TestPipeline:
    """Tests for Pipeline model."""
    
    def test_simple_pipeline(self):
        pipeline = Pipeline(
            pipeline_id="test_pipeline",
            name="Test Pipeline",
            description="A test pipeline",
            source_tables=["users", "orders"],
            steps=[
                PipelineStep(
                    step_id="step_1",
                    step_name="Join Users",
                    step_type="join",
                    description="Join with users",
                    join_type=JoinType.LEFT,
                    join_conditions=[
                        JoinCondition(
                            left_table="orders",
                            left_column="user_id",
                            right_table="users",
                            right_column="id",
                        ),
                    ],
                ),
            ],
            output_columns=[
                ColumnMapping(source_table="orders", source_column="id", target_name="order_id"),
                ColumnMapping(source_table="users", source_column="name", target_name="user_name"),
            ],
        )
        
        sql = pipeline.to_sql()
        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "LEFT JOIN" in sql
        assert "orders.user_id = users.id" in sql


class TestDataset:
    """Tests for Dataset model."""
    
    def test_create_dataset(self):
        dataset = Dataset(
            dataset_id="ds_001",
            name="users_orders",
            description="Joined users and orders",
            source_pipeline="pipeline_001",
            columns=[
                ColumnMapping(source_table="users", source_column="id", target_name="user_id"),
                ColumnMapping(source_table="orders", source_column="id", target_name="order_id"),
            ],
            creation_reason="FK relationship detected",
        )
        
        assert dataset.name == "users_orders"
        assert dataset.get_column_names() == ["user_id", "order_id"]
