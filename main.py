"""
Auto Pipeline Builder - 主程序入口

一个类似 Palantir Pipeline Builder 的自动化数据管道构建工具。
"""

import sys
import json
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from dotenv import load_dotenv

from src.config import AppConfig, DatabaseConfig, AnalysisConfig, OutputConfig, Neo4jConfig
from src.metadata_extractor import MetadataExtractor
from src.relationship_analyzer import RelationshipAnalyzer
from src.pipeline_builder import PipelineBuilder
from src.ontology_generator import OntologyGenerator
from src.report_generator import ReportGenerator
from src.neo4j_exporter import export_ontology_to_neo4j

console = Console()


@click.command()
# PostgreSQL - Set defaults to None to allow .env override
@click.option("--host", "-h", default=None, help="PostgreSQL 主机地址 (默认: localhost)")
@click.option("--port", "-p", default=None, type=int, help="PostgreSQL 端口 (默认: 5432)")
@click.option("--database", "-d", required=False, help="数据库名称 (可选，可经由环境变量配置)")
@click.option("--user", "-u", required=False, help="数据库用户名 (可选，可经由环境变量配置)")
@click.option("--password", "-P", required=False, help="数据库密码 (可选，可经由环境变量配置)", hide_input=True)
@click.option("--schema", "-s", default=None, help="要分析的 Schema (默认: public)")
# Output
@click.option("--output", "-o", default="./output", help="输出目录")
# Environment
@click.option("--env-file", "-e", default=".env", help=".env 文件路径")
# Neo4j
@click.option("--neo4j-uri", default=None, help="Neo4j URI (默认: bolt://localhost:7687)")
@click.option("--neo4j-user", default=None, help="Neo4j 用户名 (默认: neo4j)")
@click.option("--neo4j-password", default=None, help="Neo4j 密码 (默认: 空)")
@click.option("--export-neo4j", is_flag=True, help="是否导出到 Neo4j")
# Unstructured Data Analysis  
@click.option("--enable-log-analysis", is_flag=True, help="启用日志分析")
@click.option("--log-paths", multiple=True, help="日志文件路径 (可多次指定)")
@click.option("--enable-code-analysis", is_flag=True, help="启用代码分析")
@click.option("--code-paths", multiple=True, help="代码目录路径 (可多次指定)")
# Misc
@click.option("--verbose", "-v", is_flag=True, help="详细输出")
def main(
    host: Optional[str], 
    port: Optional[int], 
    database: Optional[str], 
    user: Optional[str], 
    password: Optional[str], 
    schema: Optional[str], 
    output: str, 
    env_file: str, 
    neo4j_uri: Optional[str], 
    neo4j_user: Optional[str], 
    neo4j_password: Optional[str], 
    export_neo4j: bool,
    enable_log_analysis: bool,
    log_paths: tuple[str, ...],
    enable_code_analysis: bool,
    code_paths: tuple[str, ...],
    verbose: bool
):
    """
    Auto Pipeline Builder - 自动数据管道构建工具
    
    从 PostgreSQL 数据库读取元数据，自动分析表关系，生成数据管道和 Ontology 原型。
    """
    # Create configuration from args (prioritizing args > env > defaults)
    # We pass the .env file path to from_env inside config if needed, but pydantic-settings handles it.
    # Actually, config.py loads .env if we call from_env via BaseSettings logic or load_dotenv manually.
    # main.py calls load_dotenv first.
    
    if Path(env_file).exists():
        load_dotenv(env_file)
    
    # We construct AppConfig using from_args which carefully merges provided args
    config = AppConfig.from_args(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        schema=schema,
        output_dir=output,
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        enable_log_analysis=enable_log_analysis,
        log_paths=list(log_paths) if log_paths else None,
        enable_code_analysis=enable_code_analysis,
        code_paths=list(code_paths) if code_paths else None,
        # other flags
    )
    
    # Validation: Ensure critical DB config is present (either from args or env)
    if not config.database.database or not config.database.user or not config.database.password:
        console.print("[bold red]错误: 必须提供数据库名称、用户名和密码 (通过参数或 .env)[/bold red]")
        sys.exit(1)

    console.print(Panel.fit(
        "[bold blue]Auto Pipeline Builder[/bold blue]\n"
        "自动数据管道构建工具",
        border_style="blue"
    ))
    
    console.print(f"\n[cyan]连接数据库:[/cyan] {config.database.host}:{config.database.port}/{config.database.database}")
    console.print(f"[cyan]分析 Schema:[/cyan] {config.database.schema}")
    console.print(f"[cyan]输出目录:[/cyan] {config.output.output_dir}")
    if export_neo4j:
         console.print(f"[cyan]Neo4j 导出:[/cyan] {config.neo4j.uri}")
    if config.unstructured.enable_log_analysis:
        console.print(f"[cyan]日志分析:[/cyan] 已启用, {len(config.unstructured.log_paths)} 个路径")
    if config.unstructured.enable_code_analysis:
        console.print(f"[cyan]代码分析:[/cyan] 已启用, {len(config.unstructured.code_paths)} 个路径")
    console.print("")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            
            # Step 1: Extract metadata
            task = progress.add_task("[cyan]提取数据库元数据...", total=None)
            extractor = MetadataExtractor(config.database, config.analysis)
            metadata = extractor.extract_metadata()
            extractor.close()
            progress.update(task, description=f"[green]✓ 发现 {metadata.table_count} 个表，{metadata.column_count} 列")
            
            # Step 2: Analyze unstructured data (optional)
            enhanced_metadata = metadata
            if config.unstructured.enable_log_analysis or config.unstructured.enable_code_analysis:
                from src.unstructured_analyzer import UnstructuredAnalyzer
                
                task = progress.add_task("[cyan]分析非结构化数据...", total=None)
                unstructured_analyzer = UnstructuredAnalyzer()
                
                log_paths = config.unstructured.log_paths if config.unstructured.enable_log_analysis else None
                code_paths = config.unstructured.code_paths if config.unstructured.enable_code_analysis else None
                
                enhanced_metadata = unstructured_analyzer.analyze(
                    metadata,
                    log_paths=log_paths,
                    code_paths=code_paths,
                    log_max_lines=config.unstructured.log_max_lines,
                    code_languages=config.unstructured.code_languages,
                    code_exclude_patterns=config.unstructured.code_exclude_patterns,
                )
                
                insights_summary = []
                if enhanced_metadata.log_insights:
                    insights_summary.append(f"{len(enhanced_metadata.log_insights.entity_references)} 条日志洞察")
                if enhanced_metadata.code_insights:
                    insights_summary.append(f"{len(enhanced_metadata.code_insights.entities)} 个代码实体")
                
                progress.update(task, description=f"[green]✓ " + ", ".join(insights_summary))
            
            # Step 3: Analyze relationships
            task = progress.add_task("[cyan]分析表间关系...", total=None)
            analyzer = RelationshipAnalyzer(config.analysis)
            enhanced_metadata = analyzer.analyze(enhanced_metadata)
            rel_count = len(enhanced_metadata.detected_relationships)
            progress.update(task, description=f"[green]✓ 检测到 {rel_count} 个关系")
            
            # Step 4: Build pipelines
            task = progress.add_task("[cyan]生成数据管道...", total=None)
            builder = PipelineBuilder(enhanced_metadata, config.analysis)
            datasets = builder.generate_datasets()
            pipelines = []
            for ds in datasets:
                try:
                    tables = list(set(c.source_table for c in ds.columns))
                    if len(tables) >= 2:
                        pipeline = builder.create_pipeline(
                            name=ds.name.replace("_dataset", "_pipeline"),
                            source_tables=tables,
                        )
                        pipelines.append(pipeline)
                except Exception:
                    pass
            progress.update(task, description=f"[green]✓ 生成 {len(pipelines)} 个管道，{len(datasets)} 个数据集")
            
            # Step 5: Generate ontology
            task = progress.add_task("[cyan]生成 Ontology 原型...", total=None)
            ont_generator = OntologyGenerator(enhanced_metadata, config.analysis)
            ontology = ont_generator.generate()
            progress.update(task, description=f"[green]✓ 创建 {ontology.object_type_count} 个实体类型，{ontology.link_type_count} 个关系类型")
            
            # Step 6: Export to Neo4j (Optional)
            if export_neo4j:
                task = progress.add_task("[cyan]导出至 Neo4j (含数据同步)...", total=None)
                stats = export_ontology_to_neo4j(ontology, config.neo4j, config.database)
                
                desc = f"[green]✓ Neo4j 导出完成: 创建 {stats.get('constraints_created', 0)} 约束"
                if "nodes_created" in stats:
                    desc += f", {stats['nodes_created']} 节点"
                if "relationships_created" in stats:
                    desc += f", {stats['relationships_created']} 关系"
                progress.update(task, description=desc)

            # Step 7: Generate reports
            task = progress.add_task("[cyan]生成分析报告...", total=None)
            report_generator = ReportGenerator(config.output)
            saved_paths = report_generator.save_all_reports(enhanced_metadata, ontology, pipelines, datasets)
            progress.update(task, description=f"[green]✓ 生成 {len(saved_paths)} 个报告文件")
        
        # Print summary
        console.print("\n")
        console.print(Panel.fit(
            "[bold green]✓ 处理完成！[/bold green]",
            border_style="green"
        ))
        
        # Summary table
        summary_table = Table(title="处理摘要", show_header=True)
        summary_table.add_column("指标", style="cyan")
        summary_table.add_column("数值", style="green")
        
        summary_table.add_row("表总数", str(enhanced_metadata.table_count))
        summary_table.add_row("列总数", str(enhanced_metadata.column_count))
        summary_table.add_row("外键约束", str(enhanced_metadata.foreign_key_count))
        summary_table.add_row("检测到的关系", str(len(enhanced_metadata.detected_relationships)))
        summary_table.add_row("生成的实体类型", str(ontology.object_type_count))
        summary_table.add_row("生成的关系类型", str(ontology.link_type_count))
        summary_table.add_row("生成的管道", str(len(pipelines)))
        summary_table.add_row("生成的数据集", str(len(datasets)))
        
        # Add unstructured data stats if available
        if hasattr(enhanced_metadata, 'entity_insights'):
            summary_table.add_row("实体洞察", str(len(enhanced_metadata.entity_insights)))
        if hasattr(enhanced_metadata, 'log_insights') and enhanced_metadata.log_insights:
            summary_table.add_row("日志文件分析", str(len(enhanced_metadata.log_insights.log_files_analyzed)))
        if hasattr(enhanced_metadata, 'code_insights') and enhanced_metadata.code_insights:
            summary_table.add_row("代码文件分析", str(len(enhanced_metadata.code_insights.code_files_analyzed)))
        
        console.print(summary_table)
        
        # Output files
        console.print("\n[bold cyan]生成的文件:[/bold cyan]")
        for name, path in saved_paths.items():
            console.print(f"  📄 {path}")
        
        # Recommendations
        recommendations = builder.get_join_recommendations()
        if recommendations and verbose:
            console.print("\n[bold cyan]建议:[/bold cyan]")
            for rec in recommendations[:5]:
                if rec["type"] == "hub_table":
                    console.print(f"  ⭐ {rec['description']}")
                elif rec["type"] == "isolated_table":
                    console.print(f"  ⚠️  {rec['description']}")
        
    except Exception as e:
        console.print(f"\n[bold red]错误:[/bold red] {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)


def run_from_config(config: AppConfig):
    """Run the pipeline builder from an AppConfig object.
    
    Args:
        config: Application configuration
    """
    # Extract metadata
    extractor = MetadataExtractor(config.database, config.analysis)
    metadata = extractor.extract_metadata()
    extractor.close()
    
    # Analyze relationships
    analyzer = RelationshipAnalyzer(config.analysis)
    metadata = analyzer.analyze(metadata)
    
    # Build pipelines
    builder = PipelineBuilder(metadata, config.analysis)
    datasets = builder.generate_datasets()
    pipelines = []
    for ds in datasets:
        try:
            tables = list(set(c.source_table for c in ds.columns))
            if len(tables) >= 2:
                pipeline = builder.create_pipeline(
                    name=ds.name.replace("_dataset", "_pipeline"),
                    source_tables=tables,
                )
                pipelines.append(pipeline)
        except Exception:
            pass
    
    # Generate ontology
    ont_generator = OntologyGenerator(metadata, config.analysis)
    ontology = ont_generator.generate()
    
    # Generate reports
    report_generator = ReportGenerator(config.output)
    saved_paths = report_generator.save_all_reports(metadata, ontology, pipelines, datasets)
    
    return {
        "metadata": metadata,
        "ontology": ontology,
        "pipelines": pipelines,
        "datasets": datasets,
        "reports": saved_paths,
    }


if __name__ == "__main__":
    main()
