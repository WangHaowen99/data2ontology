"""Configuration management for Auto Pipeline Builder."""

import os
from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    """PostgreSQL database configuration."""
    
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(..., description="Database name")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password")
    schema: str = Field(default="public", description="Default schema to analyze")
    
    class Config:
        env_prefix = "PG_"
        env_file = ".env"
        extra = "ignore"
    
    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def psycopg2_params(self) -> dict:
        """Get psycopg2 connection parameters."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }


class AnalysisConfig(BaseSettings):
    """Configuration for metadata analysis."""
    
    # Naming convention patterns for FK detection
    fk_column_patterns: list[str] = Field(
        default=["_id$", "_fk$", "Id$"],
        description="Regex patterns for foreign key columns"
    )
    
    # Similarity threshold for column name matching
    similarity_threshold: float = Field(
        default=0.8,
        description="Minimum similarity score for column matching (0-1)"
    )
    
    # Whether to include views in analysis
    include_views: bool = Field(
        default=False,
        description="Whether to include views in metadata extraction"
    )
    
    # Schemas to analyze (empty = all schemas)
    schemas: list[str] = Field(
        default=["public"],
        description="Schemas to analyze"
    )
    
    # Tables to exclude from analysis
    exclude_tables: list[str] = Field(
        default=[],
        description="Tables to exclude from analysis"
    )
    
    # Maximum tables to process (0 = unlimited)
    max_tables: int = Field(
        default=0,
        description="Maximum number of tables to process (0 = unlimited)"
    )
    
    class Config:
        env_prefix = "ANALYSIS_"
        extra = "ignore"


class OutputConfig(BaseSettings):
    """Configuration for output generation."""
    
    output_dir: Path = Field(
        default=Path("./output"),
        description="Directory for output files"
    )
    
    # Report formats
    generate_markdown: bool = Field(default=True, description="Generate Markdown reports")
    generate_json: bool = Field(default=True, description="Generate JSON outputs")
    generate_sql: bool = Field(default=True, description="Generate SQL scripts")
    
    # Report filenames
    metadata_report_name: str = Field(default="metadata_report.md")
    ontology_report_name: str = Field(default="ontology_report.md")
    ontology_json_name: str = Field(default="ontology.json")
    pipeline_sql_name: str = Field(default="pipelines.sql")
    
    class Config:
        env_prefix = "OUTPUT_"
        extra = "ignore"
    
    def ensure_output_dir(self):
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)


class Neo4jConfig(BaseSettings):
    """Neo4j database configuration."""
    
    uri: str = Field(default="bolt://localhost:7687", description="Neo4j URI")
    user: str = Field(default="neo4j", description="Neo4j user")
    password: str = Field(default="", description="Neo4j password")
    database: str = Field(default="neo4j", description="Neo4j database name")
    
    class Config:
        env_prefix = "NEO4J_"
        extra = "ignore"


class AppConfig(BaseSettings):
    """Main application configuration."""
    
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    verbose: bool = Field(default=False, description="Verbose output")
    
    class Config:
        env_prefix = "APP_"
        extra = "ignore"

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "AppConfig":
        """Load configuration from environment and .env file."""
        if env_file and Path(env_file).exists():
            from dotenv import load_dotenv
            load_dotenv(env_file)
        
        return cls(
            database=DatabaseConfig(),
            analysis=AnalysisConfig(),
            neo4j=Neo4jConfig(),
            output=OutputConfig(),
        )

    @classmethod
    def from_args(
        cls,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        output_dir: str = "./output",
        neo4j_uri: Optional[str] = None,
        neo4j_user: Optional[str] = None,
        neo4j_password: Optional[str] = None,
        **kwargs
    ) -> "AppConfig":
        """Create configuration from command line arguments."""
        # Only pass arguments that are not None to allow Pydantic to use env vars/defaults
        db_kwargs = {}
        if host is not None: db_kwargs["host"] = host
        if port is not None: db_kwargs["port"] = port
        if database is not None: db_kwargs["database"] = database
        if user is not None: db_kwargs["user"] = user
        if password is not None: db_kwargs["password"] = password
        if schema is not None: db_kwargs["schema"] = schema
        
        neo4j_kwargs = {}
        if neo4j_uri is not None: neo4j_kwargs["uri"] = neo4j_uri
        if neo4j_user is not None: neo4j_kwargs["user"] = neo4j_user
        if neo4j_password is not None: neo4j_kwargs["password"] = neo4j_password
        
        return cls(
            database=DatabaseConfig(**db_kwargs),
            analysis=AnalysisConfig(**kwargs),
            neo4j=Neo4jConfig(**neo4j_kwargs),
            output=OutputConfig(output_dir=Path(output_dir)),
        )
