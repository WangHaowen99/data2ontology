"""Data2Ontology API Server with multi-database support."""

import os
import sys
import shutil
import tempfile
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime

# 加载 .env 配置
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Body, UploadFile, File, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add current directory to path to import src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.storage import get_storage, ConnectionStorage
from src.adapters import get_adapter, DatabaseAdapter
from src.config import AnalysisConfig, Neo4jConfig, OutputConfig
from src.relationship_analyzer import RelationshipAnalyzer
from src.ontology_generator import OntologyGenerator
from src.report_generator import ReportGenerator
from src.neo4j_exporter import export_ontology_to_neo4j
from src.models.metadata import DatabaseMetadata, TableInfo, ColumnInfo
from src.semantic_analyzer import SemanticAnalyzer, LLMConfig, generate_semantic_report, load_prompts_config, save_prompts_config

app = FastAPI(title="Data2Ontology API", description="多数据库本体生成和知识图谱管理")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state for active connections and adapters
_active_adapters: Dict[str, DatabaseAdapter] = {}
_ontologies: Dict[str, Any] = {}
_tasks: Dict[str, Dict[str, Any]] = {}  # 存储后台任务状态
_neo4j_driver = None

# --- Data Models ---

class DBConnectionRequest(BaseModel):
    db_type: str = "postgresql"
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    schema_name: str = "public"
    name: Optional[str] = None

class QueryRequest(BaseModel):
    sql: str
    connection_id: Optional[str] = None

class OntologyGenerateRequest(BaseModel):
    table_names: List[str]
    connection_id: Optional[str] = None

class Neo4jConnectRequest(BaseModel):
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = ""

class Neo4jExportRequest(BaseModel):
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = ""
    ontology_id: Optional[str] = None

class NodeCreateRequest(BaseModel):
    label: str
    properties: Dict[str, Any] = {}

class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str = ""
    progress: float = 0.0
    logs: List[Dict[str, str]] = []  # {time: str, content: str, status: str}
    result: Optional[Dict[str, Any]] = None

class NodeUpdateRequest(BaseModel):
    properties: Dict[str, Any]

class RelationshipCreateRequest(BaseModel):
    source_id: str
    target_id: str
    type: str
    properties: Optional[Dict[str, Any]] = None

# --- Helper Functions ---

def get_active_adapter(connection_id: str = None) -> DatabaseAdapter:
    """Get an active database adapter."""
    if connection_id and connection_id in _active_adapters:
        return _active_adapters[connection_id]
    
    # Return any active adapter if no specific ID
    if _active_adapters:
        return list(_active_adapters.values())[0]
    
    raise HTTPException(status_code=400, detail="没有活动的数据库连接")

def adapter_to_metadata(adapter: DatabaseAdapter, connection_id: str) -> DatabaseMetadata:
    """Convert adapter table info to DatabaseMetadata."""
    tables = adapter.get_tables()
    
    table_metadata = []
    for t in tables:
        columns = [
            ColumnInfo(
                name=c.name,
                data_type=c.data_type,
                nullable=c.nullable,
                is_primary_key=c.is_primary_key,
                comment=c.comment
            )
            for c in t.columns
        ]
        table_metadata.append(TableInfo(
            name=t.name,
            schema_name=t.schema,
            columns=columns,
            row_count_estimate=t.row_count,
            comment=t.comment
        ))
    
    return DatabaseMetadata(
        database_name=adapter.config.get("database", connection_id),
        tables=table_metadata
    )

# --- Database Connection APIs ---

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "message": "服务正常运行"}

@app.get("/api/db/connections")
async def get_connections():
    """获取所有保存的数据库连接."""
    storage = get_storage()
    connections = storage.get_all_connections()
    
    # Update status based on active adapters
    for conn in connections:
        conn["status"] = "connected" if conn["id"] in _active_adapters else "disconnected"
        # Remove encrypted password from response
        conn.pop("password_encrypted", None)
    
    return connections

@app.post("/api/db/connect")
async def connect_db(req: DBConnectionRequest):
    """连接数据库并保存连接信息."""
    try:
        storage = get_storage()
        
        # Create connection config
        config = {
            "db_type": req.db_type,
            "host": req.host,
            "port": req.port,
            "database": req.database,
            "user": req.user,
            "password": req.password,
            "schema_name": req.schema_name,
            "name": req.name or req.database,
        }
        
        # Get adapter and connect
        adapter = get_adapter(req.db_type, config)
        adapter.connect()
        
        # Save connection
        config["status"] = "connected"
        connection_id = storage.save_connection(config)
        
        # Store active adapter
        _active_adapters[connection_id] = adapter
        
        # Get basic metadata
        tables = adapter.get_tables()
        
        return {
            "status": "connected",
            "connection_id": connection_id,
            "message": f"成功连接到 {req.database}",
            "tables_count": len(tables),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/db/disconnect/{connection_id}")
async def disconnect_db(connection_id: str):
    """断开数据库连接."""
    if connection_id in _active_adapters:
        _active_adapters[connection_id].disconnect()
        del _active_adapters[connection_id]
    
    storage = get_storage()
    storage.update_status(connection_id, "disconnected")
    
    return {"status": "disconnected", "message": "已断开连接"}

@app.post("/api/db/reconnect/{connection_id}")
async def reconnect_db(connection_id: str):
    """重新连接数据库."""
    storage = get_storage()
    conn_info = storage.get_connection(connection_id)
    
    if not conn_info:
        raise HTTPException(status_code=404, detail="连接不存在")
    
    try:
        adapter = get_adapter(conn_info["db_type"], conn_info)
        adapter.connect()
        
        _active_adapters[connection_id] = adapter
        storage.update_status(connection_id, "connected")
        
        return {"status": "connected", "message": "重新连接成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/db/connection/{connection_id}")
async def delete_connection(connection_id: str):
    """删除数据库连接."""
    if connection_id in _active_adapters:
        _active_adapters[connection_id].disconnect()
        del _active_adapters[connection_id]
    
    storage = get_storage()
    storage.delete_connection(connection_id)
    
    return {"status": "deleted", "message": "连接已删除"}

@app.get("/api/db/metadata")
async def get_metadata(connection_id: Optional[str] = None):
    """获取数据库元数据."""
    adapter = get_active_adapter(connection_id)
    tables = adapter.get_tables()
    
    return {
        "database_name": adapter.config.get("database", "Unknown"),
        "tables": [
            {
                "name": t.name,
                "schema": t.schema,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "nullable": c.nullable,
                        "is_primary_key": c.is_primary_key,
                        "comment": c.comment
                    }
                    for c in t.columns
                ],
                "row_count_estimate": t.row_count,
                "comment": t.comment
            }
            for t in tables
        ],
        "table_count": len(tables),
        "column_count": sum(len(t.columns) for t in tables)
    }

@app.get("/api/db/tables/{table_name}/sample")
async def get_table_sample(table_name: str, connection_id: Optional[str] = None):
    """获取表的示例数据."""
    adapter = get_active_adapter(connection_id)
    try:
        rows = adapter.get_table_sample(table_name, limit=10)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/db/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """上传 CSV 文件作为数据源."""
    try:
        # Save file temporarily
        temp_dir = Path(tempfile.gettempdir()) / "data2ontology_csv"
        temp_dir.mkdir(exist_ok=True)
        
        file_path = temp_dir / file.filename
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Create CSV adapter
        from src.adapters import CSVAdapter
        config = {
            "db_type": "csv",
            "file_path": str(file_path),
            "name": file.filename.replace(".csv", ""),
            "database": file.filename.replace(".csv", "")
        }
        
        adapter = CSVAdapter(config)
        adapter.connect()
        
        # Save connection
        storage = get_storage()
        config["status"] = "connected"
        connection_id = storage.save_connection(config)
        
        _active_adapters[connection_id] = adapter
        
        return {
            "status": "success",
            "connection_id": connection_id,
            "message": f"CSV 文件 {file.filename} 上传成功",
            "tables_count": len(adapter.get_tables())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Query APIs ---

@app.post("/api/query/sql")
async def execute_sql(req: QueryRequest):
    """执行 SQL 查询."""
    adapter = get_active_adapter(req.connection_id)
    try:
        result = adapter.execute_query(req.sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Prompt Configuration APIs ---

@app.get("/api/config/prompts")
async def get_prompts_config():
    """获取 LLM Prompt 配置."""
    config = load_prompts_config()
    return config

@app.post("/api/config/prompts")
async def update_prompts_config(config: Dict[str, str] = Body(...)):
    """更新 LLM Prompt 配置."""
    try:
        save_prompts_config(config)
        return {"status": "saved", "message": "Prompt 配置已保存"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Ontology APIs ---


# --- Task Progress Helper ---

def update_task(task_id: str, status: Optional[str] = None, message: Optional[str] = None, progress: Optional[float] = None, log_content: Optional[str] = None):
    """Update task status and logs."""
    if task_id not in _tasks:
        return
        
    task = _tasks[task_id]
    if status:
        task["status"] = status
    if message:
        task["message"] = message
    if progress is not None:
        task["progress"] = progress
    
    if log_content:
        log_entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "content": log_content,
            "status": "process" if status == "running" else status or "process"
        }
        task["logs"].append(log_entry)

def process_ontology_generation(task_id: str, req: OntologyGenerateRequest):
    """Background task for ontology generation."""
    try:
        update_task(task_id, "running", "开始获取元数据...", 0.05, "开始从数据库获取元数据...")
        
        adapter = get_active_adapter(req.connection_id)
        connection_id = req.connection_id or list(_active_adapters.keys())[0]
        
        # Get metadata
        metadata = adapter_to_metadata(adapter, connection_id)
        
        # Filter tables
        if req.table_names:
            selected_tables_set = set(req.table_names)
            metadata.tables = [
                t for t in metadata.tables 
                if t.name in selected_tables_set or t.full_name in selected_tables_set
            ]
        
        update_task(task_id, "running", f"已获取 {len(metadata.tables)} 个表的元数据", 0.1, f"获取到 {len(metadata.tables)} 个表的元数据，开始分析...")

        # Relationship analysis
        update_task(task_id, "running", "正在分析表关系...", 0.15, "正在分析表之间的外键和潜在关联...")
        analysis_config = AnalysisConfig(schemas=[adapter.config.get("schema_name", "public")])
        analyzer = RelationshipAnalyzer(analysis_config)
        metadata = analyzer.analyze(metadata)
        
        # Semantic analysis prep
        prompts_config = load_prompts_config()
        llm_config = LLMConfig(
            api_key=os.getenv("OPENAI_API_KEY", ""),
            api_base=os.getenv("OPENAI_API_BASE", ""),
            model=os.getenv("LLM_MODEL", "gpt-4o-mini"),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "2000")),
            table_analysis_prompt=prompts_config.get("table_analysis_prompt", ""),
            relationship_analysis_prompt=prompts_config.get("relationship_analysis_prompt", "")
        )
        semantic_analyzer = SemanticAnalyzer(llm_config)
        
        update_task(task_id, "running", "开始语义分析...", 0.2, "开始使用 LLM 进行语义分析...")
        
        table_analyses = {}
        total_tables = len(metadata.tables)
        
        for idx, table in enumerate(metadata.tables):
            progress = 0.2 + (0.6 * (idx / total_tables))
            update_task(task_id, "running", f"正在分析表: {table.name}", progress, f"正在分析表 '{table.name}' ({idx+1}/{total_tables})...")
            
            # Fetch sample data
            try:
                sample_data = adapter.get_table_sample(table.name, limit=5)
            except:
                sample_data = []
            
            # Build column info
            columns_info = [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "is_primary_key": col.is_primary_key,
                    "comment": col.comment
                }
                for col in table.columns
            ]
            
            # Analyze table
            analysis = semantic_analyzer.analyze_table(
                table_name=table.name,
                columns=columns_info,
                sample_data=sample_data,
                table_comment=table.comment,
                row_count=table.row_count_estimate
            )
            
            if analysis:
                table_analyses[table.name] = analysis
                # Log detailed analysis result
                props_desc = ", ".join([f"{p['column_name']}->{p['business_name']}" for p in analysis.get('properties', [])[:3]])
                update_task(task_id, None, None, None, f"✓ 表 '{table.name}' 分析完成: 识别为 '{analysis.get('entity_name_cn')}'")

        update_task(task_id, "running", "计算图谱结构...", 0.85, "语义分析完成，正在生成本体结构...")

        # Generate ontology
        ont_generator = OntologyGenerator(metadata, analysis_config)
        ontology = ont_generator.generate()
        
        # Store ontology
        ontology_id = f"ont_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        _ontologies[ontology_id] = ontology
        
        update_task(task_id, "running", "生成报告...", 0.95, "正在生成分析报告...")

        # Generate report
        report_content = generate_semantic_report(ontology, table_analyses)
        
        result = {
            "ontology_id": ontology_id,
            "ontology": ontology.dict(),
            "report": report_content,
            "table_analyses": table_analyses
        }
        
        update_task(task_id, "completed", "构建完成", 1.0, "本体构建全部完成！")
        _tasks[task_id]["result"] = result

    except Exception as e:
        import traceback
        traceback.print_exc()
        error_msg = str(e)
        update_task(task_id, "error", f"构建失败: {error_msg}", None, f"错误: {error_msg}")

@app.post("/api/ontology/generate", response_model=TaskResponse)
async def generate_ontology(req: OntologyGenerateRequest, background_tasks: BackgroundTasks):
    """异步生成本体."""
    task_id = f"task_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    
    # Initialize task
    _tasks[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "message": "任务已创建",
        "progress": 0.0,
        "logs": [],
        "result": None
    }
    
    # Start background task
    background_tasks.add_task(process_ontology_generation, task_id, req)
    
    return _tasks[task_id]

@app.get("/api/ontology/task/{task_id}", response_model=TaskResponse)
async def get_task_status(task_id: str):
    """获取任务状态."""
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return _tasks[task_id]

@app.get("/api/ontology/list")
async def list_ontologies():
    """获取已生成的本体列表."""
    return [
        {
            "id": oid,
            "name": ont.name,
            "object_type_count": ont.object_type_count,
            "link_type_count": ont.link_type_count
        }
        for oid, ont in _ontologies.items()
    ]

# --- Neo4j APIs ---

@app.post("/api/neo4j/connect")
async def connect_neo4j(req: Neo4jConnectRequest):
    """连接 Neo4j 数据库."""
    global _neo4j_driver
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(req.uri, auth=(req.user, req.password))
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        
        _neo4j_driver = driver
        
        # Save connection
        storage = get_storage()
        storage.save_neo4j_connection({
            "uri": req.uri,
            "user": req.user,
            "password": req.password,
            "status": "connected"
        })
        
        return {"status": "connected", "message": "Neo4j 连接成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/neo4j/status")
async def get_neo4j_status():
    """获取 Neo4j 连接状态."""
    if _neo4j_driver:
        try:
            with _neo4j_driver.session() as session:
                session.run("RETURN 1")
            return {"status": "connected"}
        except:
            pass
    return {"status": "disconnected"}

@app.post("/api/neo4j/export")
async def export_neo4j(req: Neo4jExportRequest):
    """导出本体到 Neo4j."""
    # Get ontology
    ontology = None
    if req.ontology_id and req.ontology_id in _ontologies:
        ontology = _ontologies[req.ontology_id]
    elif _ontologies:
        ontology = list(_ontologies.values())[-1]
    
    if not ontology:
        raise HTTPException(status_code=400, detail="没有可用的本体数据")
    
    # Get active database connection for data
    if not _active_adapters:
        raise HTTPException(status_code=400, detail="没有活动的数据库连接")
    
    try:
        neo4j_config = Neo4jConfig(uri=req.uri, user=req.user, password=req.password)
        
        # Get first active connection's config
        first_adapter = list(_active_adapters.values())[0]
        from src.config import DatabaseConfig
        db_config = DatabaseConfig(
            host=first_adapter.config.get("host", "localhost"),
            port=first_adapter.config.get("port", 5432),
            database=first_adapter.config.get("database", ""),
            user=first_adapter.config.get("user", ""),
            password=first_adapter.config.get("password", ""),
            schema=first_adapter.config.get("schema_name", "public")
        )
        
        stats = export_ontology_to_neo4j(ontology, neo4j_config, db_config)
        
        return {
            "status": "success",
            "message": "导出成功",
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/neo4j/graph")
async def get_neo4j_graph(uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """获取 Neo4j 图数据用于可视化."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        nodes = []
        edges = []
        
        with driver.session() as session:
            # Fetch nodes
            result = session.run("MATCH (n) RETURN n, labels(n) LIMIT 200")
            for record in result:
                node = record["n"]
                labels = record["labels(n)"]
                node_id = getattr(node, "element_id", str(node.id))
                nodes.append({
                    "id": node_id,
                    "label": labels[0] if labels else "Node",
                    "properties": dict(node)
                })
            
            # Fetch relationships
            result = session.run("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200")
            for record in result:
                source = record["n"]
                target = record["m"]
                rel = record["r"]
                
                source_id = getattr(source, "element_id", str(source.id))
                target_id = getattr(target, "element_id", str(target.id))
                
                edges.append({
                    "source": source_id,
                    "target": target_id,
                    "label": rel.type,
                    "properties": dict(rel)
                })
        
        driver.close()
        
        return {"nodes": nodes, "edges": edges}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Neo4j CRUD APIs ---

@app.post("/api/neo4j/nodes")
async def create_node(req: NodeCreateRequest, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """创建节点."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            props_str = ", ".join([f"{k}: ${k}" for k in req.properties.keys()])
            query = f"CREATE (n:{req.label} {{{props_str}}}) RETURN n"
            result = session.run(query, **req.properties)
            node = result.single()["n"]
            node_id = getattr(node, "element_id", str(node.id))
        driver.close()
        
        return {"status": "created", "node_id": node_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/neo4j/nodes/{node_id}")
async def update_node(node_id: str, req: NodeUpdateRequest, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """更新节点属性."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            set_str = ", ".join([f"n.{k} = ${k}" for k in req.properties.keys()])
            query = f"MATCH (n) WHERE elementId(n) = $node_id SET {set_str} RETURN n"
            session.run(query, node_id=node_id, **req.properties)
        driver.close()
        
        return {"status": "updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/neo4j/nodes/{node_id}")
async def delete_node(node_id: str, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """删除节点."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            session.run("MATCH (n) WHERE elementId(n) = $node_id DETACH DELETE n", node_id=node_id)
        driver.close()
        
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/neo4j/relationships")
async def create_relationship(req: RelationshipCreateRequest, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """创建关系."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            props = req.properties or {}
            props_str = ", ".join([f"{k}: ${k}" for k in props.keys()]) if props else ""
            props_clause = f" {{{props_str}}}" if props_str else ""
            
            query = f"""
                MATCH (a), (b)
                WHERE elementId(a) = $source_id AND elementId(b) = $target_id
                CREATE (a)-[r:{req.type}{props_clause}]->(b)
                RETURN r
            """
            session.run(query, source_id=req.source_id, target_id=req.target_id, **props)
        driver.close()
        
        return {"status": "created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/neo4j/relationships/{rel_id}")
async def delete_relationship(rel_id: str, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """删除关系."""
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            session.run("MATCH ()-[r]->() WHERE elementId(r) = $rel_id DELETE r", rel_id=rel_id)
        driver.close()
        
        return {"status": "deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/neo4j/import-ontology")
async def import_ontology_to_neo4j(ontology_id: str = Body(..., embed=True), uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = ""):
    """从本体定义导入到 Neo4j（仅结构，不含数据）."""
    if ontology_id not in _ontologies:
        raise HTTPException(status_code=404, detail="本体不存在")
    
    ontology = _ontologies[ontology_id]
    
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        with driver.session() as session:
            # Create constraints for each object type
            for obj_type in ontology.object_types:
                try:
                    session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{obj_type.name}) REQUIRE n.id IS UNIQUE")
                except:
                    pass  # Constraint may already exist
        
        driver.close()
        
        return {
            "status": "success",
            "message": f"已导入 {ontology.object_type_count} 个实体类型定义"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Startup ---

@app.on_event("startup")
async def startup_event():
    """应用启动时自动重连已保存的数据库连接."""
    storage = get_storage()
    connections = storage.get_all_connections()
    
    for conn in connections:
        if conn.get("status") == "connected":
            try:
                conn_info = storage.get_connection(conn["id"])
                adapter = get_adapter(conn_info["db_type"], conn_info)
                adapter.connect()
                _active_adapters[conn["id"]] = adapter
                print(f"自动重连成功: {conn_info.get('name', conn_info.get('database'))}")
            except Exception as e:
                print(f"自动重连失败: {conn.get('name')}: {e}")
                storage.update_status(conn["id"], "disconnected")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
