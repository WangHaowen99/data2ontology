"""Unit tests for ontology models and generator."""

import pytest
from src.models.ontology import (
    PropertyType,
    ObjectType,
    LinkType,
    Ontology,
    OntologyDataType,
    map_pg_type_to_ontology,
)


class TestOntologyDataTypeMapping:
    """Tests for PostgreSQL to Ontology type mapping."""
    
    def test_integer_types(self):
        assert map_pg_type_to_ontology("integer") == OntologyDataType.INTEGER
        assert map_pg_type_to_ontology("int") == OntologyDataType.INTEGER
        assert map_pg_type_to_ontology("int4") == OntologyDataType.INTEGER
        assert map_pg_type_to_ontology("smallint") == OntologyDataType.INTEGER
    
    def test_long_types(self):
        assert map_pg_type_to_ontology("bigint") == OntologyDataType.LONG
        assert map_pg_type_to_ontology("int8") == OntologyDataType.LONG
    
    def test_string_types(self):
        assert map_pg_type_to_ontology("varchar") == OntologyDataType.STRING
        assert map_pg_type_to_ontology("text") == OntologyDataType.STRING
        assert map_pg_type_to_ontology("character varying") == OntologyDataType.STRING
        assert map_pg_type_to_ontology("varchar(255)") == OntologyDataType.STRING
    
    def test_boolean_types(self):
        assert map_pg_type_to_ontology("boolean") == OntologyDataType.BOOLEAN
        assert map_pg_type_to_ontology("bool") == OntologyDataType.BOOLEAN
    
    def test_timestamp_types(self):
        assert map_pg_type_to_ontology("timestamp") == OntologyDataType.TIMESTAMP
        assert map_pg_type_to_ontology("timestamptz") == OntologyDataType.TIMESTAMP
    
    def test_json_types(self):
        assert map_pg_type_to_ontology("json") == OntologyDataType.OBJECT
        assert map_pg_type_to_ontology("jsonb") == OntologyDataType.OBJECT
    
    def test_array_types(self):
        assert map_pg_type_to_ontology("integer[]") == OntologyDataType.ARRAY
        assert map_pg_type_to_ontology("text[]") == OntologyDataType.ARRAY
    
    def test_unknown_defaults_to_string(self):
        assert map_pg_type_to_ontology("unknown_type") == OntologyDataType.STRING


class TestPropertyType:
    """Tests for PropertyType model."""
    
    def test_create_property(self):
        prop = PropertyType(
            id="Users.id",
            name="Id",
            data_type=OntologyDataType.INTEGER,
            source_table="users",
            source_column="id",
            is_primary_key=True,
            creation_reason="Primary key column",
        )
        assert prop.id == "Users.id"
        assert prop.data_type == OntologyDataType.INTEGER
        assert prop.is_primary_key is True


class TestObjectType:
    """Tests for ObjectType model."""
    
    def test_create_object_type(self):
        obj = ObjectType(
            id="Users",
            name="Users",
            source_table="public.users",
            primary_key=["id"],
            properties=[
                PropertyType(
                    id="Users.id",
                    name="Id",
                    data_type=OntologyDataType.INTEGER,
                    source_table="users",
                    source_column="id",
                    creation_reason="ID column",
                ),
            ],
            creation_reason="Users table",
        )
        assert obj.id == "Users"
        assert len(obj.properties) == 1
    
    def test_get_property(self):
        obj = ObjectType(
            id="Users",
            name="Users",
            source_table="users",
            properties=[
                PropertyType(id="Users.id", name="Id", data_type=OntologyDataType.INTEGER, 
                            source_table="users", source_column="id", creation_reason="ID"),
                PropertyType(id="Users.name", name="Name", data_type=OntologyDataType.STRING,
                            source_table="users", source_column="name", creation_reason="Name"),
            ],
            creation_reason="test",
        )
        prop = obj.get_property("Name")
        assert prop is not None
        assert prop.id == "Users.name"


class TestOntology:
    """Tests for Ontology model."""
    
    def test_create_ontology(self):
        ontology = Ontology(
            name="test_ontology",
            source_database="testdb",
            object_types=[
                ObjectType(id="Users", name="Users", source_table="users", 
                          creation_reason="test", properties=[]),
            ],
            link_types=[
                LinkType(id="Orders_to_Users", name="hasUser", source_object_type="Orders",
                        target_object_type="Users", creation_reason="FK relationship"),
            ],
        )
        assert ontology.name == "test_ontology"
        assert ontology.object_type_count == 1
        assert ontology.link_type_count == 1
    
    def test_to_json(self):
        ontology = Ontology(
            name="test",
            source_database="testdb",
            object_types=[],
            link_types=[],
        )
        json_data = ontology.to_json()
        assert json_data["name"] == "test"
        assert "object_types" in json_data
