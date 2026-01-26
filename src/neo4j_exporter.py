"""Neo4j exporter for Ontology."""

from typing import Optional, Any, List
from neo4j import GraphDatabase, Driver
from sqlalchemy import create_engine, text

from .config import Neo4jConfig, DatabaseConfig
from .models.ontology import Ontology, OntologyDataType


class Neo4jExporter:
    """Exports Ontology definition and data to Neo4j."""

    def __init__(self, config: Neo4jConfig):
        """Initialize the Neo4j exporter.
        
        Args:
            config: Neo4j configuration
        """
        self.config = config
        self._driver: Optional[Driver] = None

    def connect(self) -> Driver:
        """Create and return Neo4j driver."""
        if self._driver is None:
            auth = (self.config.user, self.config.password) if self.config.user else None
            self._driver = GraphDatabase.driver(self.config.uri, auth=auth)
        return self._driver

    def close(self):
        """Close Neo4j connection."""
        if self._driver:
            self._driver.close()
            self._driver = None

    def export_ontology(self, ontology: Ontology) -> dict:
        """Export ontology structure (constraints) to Neo4j.
        
        Args:
            ontology: Ontology definition
            
        Returns:
            Statistics dictionary
        """
        driver = self.connect()
        stats = {"constraints_created": 0}
        
        with driver.session(database=self.config.database) as session:
            # Create constraints for Primary Keys
            for obj_type in ontology.object_types:
                if not obj_type.primary_key:
                    continue
                
                # Use ObjectType ID as Label (e.g., "RawListings")
                label = obj_type.id
                
                # Resolve PK property 
                pk_ref = obj_type.primary_key[0]
                
                # Find the PropertyType to get the clean name
                # e.g. id="RawListings.id" -> key="id"
                pk_prop = next((p for p in obj_type.properties if p.id.endswith(f".{pk_ref}") or p.name == pk_ref), None)
                
                if pk_prop:
                    neo4j_pk_prop = pk_prop.id.split('.')[-1]
                else:
                    neo4j_pk_prop = pk_ref

                constraint_name = f"constraint_{label.lower()}_pk"
                try:
                    # Neo4j 5.x syntax
                    cypher = (
                        f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                        f"FOR (n:`{label}`) REQUIRE n.`{neo4j_pk_prop}` IS UNIQUE"
                    )
                    session.run(cypher)
                    stats["constraints_created"] += 1
                except Exception as e:
                    # Fallback for older Neo4j versions
                    try:
                        cypher = (
                            f"CREATE CONSTRAINT ON (n:`{label}`) ASSERT n.`{neo4j_pk_prop}` IS UNIQUE"
                        )
                        session.run(cypher)
                        stats["constraints_created"] += 1
                    except Exception as e2:
                        print(f"Warning: Failed to create constraint for {label}: {e2}")

        return stats

    def sync_data(self, ontology: Ontology, db_config: DatabaseConfig) -> dict:
        """Sync data from PostgreSQL to Neo4j based on Ontology.
        
        Args:
            ontology: Ontology definition
            db_config: Source database configuration
            
        Returns:
            Statistics dictionary
        """
        stats = {"nodes_created": 0, "relationships_created": 0}
        driver = self.connect()
        pg_engine = create_engine(db_config.connection_string)
        
        with driver.session(database=self.config.database) as session:
            # 1. Sync Nodes (Object Types)
            for obj_type in ontology.object_types:
                # Skip objects without primary keys
                if not obj_type.primary_key:
                    print(f"Skipping {obj_type.name} - no primary key defined")
                    continue
                    
                print(f"Syncing node type: {obj_type.name} ({obj_type.id})...")
                
                # Map Source Column -> Neo4j Property Key
                col_to_prop_key = {}
                for p in obj_type.properties:
                    # Extract property key from ID (e.g. "RawListings.listingDate" -> "listingDate")
                    prop_key = p.id.split('.')[-1]
                    col_to_prop_key[p.source_column] = prop_key
                
                columns = [f'"{col}"' for col in col_to_prop_key.keys()]
                query = f'SELECT {", ".join(columns)} FROM {obj_type.source_table}'
                
                # Identify PK for this ObjectType
                pk_ref = obj_type.primary_key[0] if obj_type.primary_key else "id"
                pk_prop_obj = next((p for p in obj_type.properties if p.id.endswith(f".{pk_ref}")), None)
                neo4j_pk_key = pk_prop_obj.id.split('.')[-1] if pk_prop_obj else "id"

                # Fetch data from Postgres
                with pg_engine.connect() as conn:
                    result = conn.execute(text(query))
                    batch = []
                    batch_size = 1000
                    
                    for row in result:
                        row_dict = {}
                        pk_value = None
                        
                        for i, col_name in enumerate(col_to_prop_key.keys()):
                            val = row[i]
                            # Handle data types
                            if hasattr(val, 'isoformat'):
                                val = val.isoformat()
                            elif hasattr(val, 'quantize') and hasattr(val, 'to_eng_string'): # Decimal
                                val = float(val)
                            
                            neo4j_key = col_to_prop_key[col_name]
                            row_dict[neo4j_key] = val
                            
                            if neo4j_key == neo4j_pk_key:
                                pk_value = val
                        
                        if pk_value is None:
                            continue
                        
                        batch.append(row_dict)
                        
                        if len(batch) >= batch_size:
                            self._write_node_batch_with_pk(session, obj_type.id, neo4j_pk_key, batch)
                            stats["nodes_created"] += len(batch)
                            batch = []
                    
                    if batch:
                        self._write_node_batch_with_pk(session, obj_type.id, neo4j_pk_key, batch)
                        stats["nodes_created"] += len(batch)

            # 2. Sync Relationships (Link Types)
            for link_type in ontology.link_types:
                if link_type.cardinality != "many-to-one":
                    continue
                
                source_obj = ontology.get_object_type(link_type.source_object_type)
                target_obj = ontology.get_object_type(link_type.target_object_type)
                
                if not source_obj or not target_obj:
                    continue
                
                # Skip if either object has no primary key
                if not source_obj.primary_key or not target_obj.primary_key:
                    continue
                
                # 1. Source PK
                src_pk_ref = source_obj.primary_key[0]
                src_pk_prop = next((p for p in source_obj.properties if p.id.endswith(f".{src_pk_ref}")), None)
                if not src_pk_prop: continue
                src_pk_col = src_pk_prop.source_column
                src_neo4j_pk = src_pk_prop.id.split('.')[-1]
                
                # 2. Target PK
                tgt_pk_ref = target_obj.primary_key[0]
                tgt_pk_prop = next((p for p in target_obj.properties if p.id.endswith(f".{tgt_pk_ref}")), None)
                if not tgt_pk_prop: continue
                tgt_neo4j_pk = tgt_pk_prop.id.split('.')[-1]

                # 3. Source FK
                fk_prop_name = link_type.source_property
                fk_prop = next((p for p in source_obj.properties if p.id.endswith(f".{fk_prop_name}") or p.name == fk_prop_name), None)
                if not fk_prop:
                    fk_prop = source_obj.get_property(fk_prop_name)
                    if not fk_prop: continue
                
                fk_col = fk_prop.source_column

                # Query
                query = f'SELECT "{src_pk_col}", "{fk_col}" FROM {source_obj.source_table} WHERE "{fk_col}" IS NOT NULL'
                
                with pg_engine.connect() as conn:
                    result = conn.execute(text(query))
                    batch = []
                    batch_size = 1000
                    
                    for row in result:
                        batch.append({
                            "source_id": row[0],
                            "target_id": row[1]
                        })
                        
                        if len(batch) >= batch_size:
                            self._write_rel_batch(
                                session, 
                                link_type, 
                                source_obj.id,
                                target_obj.id,
                                src_neo4j_pk,
                                tgt_neo4j_pk,
                                batch
                            )
                            stats["relationships_created"] += len(batch)
                            batch = []
                    
                    if batch:
                        self._write_rel_batch(
                            session, 
                            link_type, 
                            source_obj.id,
                            target_obj.id,
                            src_neo4j_pk,
                            tgt_neo4j_pk,
                            batch
                        )
                        stats["relationships_created"] += len(batch)
        
        return stats

    def _write_node_batch(self, session, label: str, batch: List[dict]):
        """Legacy method."""
        pass 
        
    def _write_node_batch_with_pk(self, session, label: str, pk_name: str, batch: List[dict]):
        """Write a batch of nodes using MERGE with explicit PK."""
        cypher = (
            f"UNWIND $batch AS row "
            f"MERGE (n:`{label}` {{ `{pk_name}`: row.`{pk_name}` }}) "
            f"SET n += row"
        )
        session.run(cypher, batch=batch)

    def _write_rel_batch(self, session, link_type, source_label, target_label, source_pk, target_pk, batch: List[dict]):
        """Write a batch of relationships."""
        rel_type = link_type.name
        
        cypher = (
            f"UNWIND $batch AS row "
            f"MATCH (s:`{source_label}` {{ `{source_pk}`: row.source_id }}) "
            f"MATCH (t:`{target_label}` {{ `{target_pk}`: row.target_id }}) "
            f"MERGE (s)-[r:`{rel_type}`]->(t)"
        )
        session.run(cypher, batch=batch)






def export_ontology_to_neo4j(ontology: Ontology, config: Neo4jConfig, db_config: Optional[DatabaseConfig] = None) -> dict:
    """Convenience function to export ontology and data to Neo4j.
    
    Args:
        ontology: Ontology definition
        config: Neo4j configuration
        db_config: Source database configuration (required for data sync)
        
    Returns:
        Statistics dictionary
    """
    exporter = Neo4jExporter(config)
    try:
        stats = exporter.export_ontology(ontology)
        if db_config:
            data_stats = exporter.sync_data(ontology, db_config)
            stats.update(data_stats)
        return stats
    finally:
        exporter.close()
