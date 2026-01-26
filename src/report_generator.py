"""Report generator for metadata analysis and ontology documentation."""

from pathlib import Path
from typing import Optional
from datetime import datetime
from jinja2 import Template

from .config import OutputConfig
from .models.metadata import DatabaseMetadata, RelationshipConfidence
from .models.ontology import Ontology
from .models.pipeline import Pipeline, Dataset


class ReportGenerator:
    """Generates Markdown reports for metadata and ontology analysis."""

    def __init__(self, output_config: Optional[OutputConfig] = None):
        """Initialize the report generator.
        
        Args:
            output_config: Output configuration
        """
        self.config = output_config or OutputConfig()
        self.config.ensure_output_dir()

    def generate_metadata_report(self, metadata: DatabaseMetadata) -> str:
        """Generate a metadata analysis report.
        
        Args:
            metadata: Database metadata
            
        Returns:
            Markdown report content
        """
        template = Template(METADATA_REPORT_TEMPLATE)
        
        # Prepare table details
        table_details = []
        for table in metadata.tables:
            columns_info = []
            for col in table.columns:
                flags = []
                if col.is_primary_key:
                    flags.append("PK")
                if col.is_unique:
                    flags.append("UNIQUE")
                if not col.nullable:
                    flags.append("NOT NULL")
                
                flags_str = f" ({', '.join(flags)})" if flags else ""
                columns_info.append(f"- `{col.name}`: {col.data_type}{flags_str}")
            
            fks_info = []
            for fk in table.foreign_keys:
                fks_info.append(f"- `{fk.column}` → `{fk.references_table}.{fk.references_column}`")
            
            table_details.append({
                "name": table.name,
                "schema": table.schema,
                "row_count": table.row_count_estimate or "未知",
                "columns": columns_info,
                "foreign_keys": fks_info,
                "comment": table.comment or "",
            })
        
        # Prepare relationship summary
        relationships_by_confidence = {
            "high": [],
            "medium": [],
            "low": [],
        }
        for rel in metadata.detected_relationships:
            relationships_by_confidence[rel.confidence.value].append({
                "source": f"{rel.source_table}.{rel.source_column}",
                "target": f"{rel.target_table}.{rel.target_column}",
                "method": rel.detection_method,
                "reason": rel.reason,
            })
        
        content = template.render(
            database_name=metadata.database_name,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            table_count=metadata.table_count,
            column_count=metadata.column_count,
            fk_count=metadata.foreign_key_count,
            relationship_count=len(metadata.detected_relationships),
            tables=table_details,
            high_conf_rels=relationships_by_confidence["high"],
            medium_conf_rels=relationships_by_confidence["medium"],
            low_conf_rels=relationships_by_confidence["low"],
        )
        
        return content

    def generate_ontology_report(self, ontology: Ontology, metadata: DatabaseMetadata) -> str:
        """Generate an ontology creation report.
        
        Args:
            ontology: Generated ontology
            metadata: Source database metadata
            
        Returns:
            Markdown report content
        """
        template = Template(ONTOLOGY_REPORT_TEMPLATE)
        
        # Prepare object types
        object_types = []
        for obj in ontology.object_types:
            props = [{"name": p.name, "type": p.data_type.value, "pk": p.is_primary_key} for p in obj.properties]
            object_types.append({
                "id": obj.id,
                "name": obj.name,
                "source_table": obj.source_table,
                "primary_key": ", ".join(obj.primary_key),
                "properties": props,
                "creation_reason": obj.creation_reason,
            })
        
        # Prepare link types
        link_types = []
        for link in ontology.link_types:
            link_types.append({
                "id": link.id,
                "name": link.name,
                "source": link.source_object_type,
                "target": link.target_object_type,
                "cardinality": link.cardinality,
                "confidence": link.confidence,
                "creation_reason": link.creation_reason,
            })
        
        content = template.render(
            ontology_name=ontology.name,
            database_name=ontology.source_database,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            version=ontology.version,
            object_type_count=ontology.object_type_count,
            property_count=ontology.total_property_count,
            link_type_count=ontology.link_type_count,
            object_types=object_types,
            link_types=link_types,
        )
        
        return content

    def generate_pipeline_report(self, pipelines: list[Pipeline], datasets: list[Dataset]) -> str:
        """Generate a pipeline and dataset report.
        
        Args:
            pipelines: List of generated pipelines
            datasets: List of generated datasets
            
        Returns:
            Markdown report content
        """
        template = Template(PIPELINE_REPORT_TEMPLATE)
        
        pipeline_info = []
        for p in pipelines:
            pipeline_info.append({
                "id": p.pipeline_id,
                "name": p.name,
                "description": p.description,
                "source_tables": ", ".join(p.source_tables),
                "sql": p.to_sql(),
                "step_count": len(p.steps),
                "output_column_count": len(p.output_columns),
            })
        
        dataset_info = []
        for d in datasets:
            dataset_info.append({
                "id": d.dataset_id,
                "name": d.name,
                "description": d.description,
                "columns": ", ".join(d.get_column_names()[:5]) + ("..." if len(d.columns) > 5 else ""),
                "creation_reason": d.creation_reason,
            })
        
        content = template.render(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            pipeline_count=len(pipelines),
            dataset_count=len(datasets),
            pipelines=pipeline_info,
            datasets=dataset_info,
        )
        
        return content

    def save_report(self, content: str, filename: str) -> Path:
        """Save report to file.
        
        Args:
            content: Report content
            filename: Output filename
            
        Returns:
            Path to saved file
        """
        output_path = self.config.output_dir / filename
        output_path.write_text(content, encoding="utf-8")
        return output_path

    def save_all_reports(
        self,
        metadata: DatabaseMetadata,
        ontology: Ontology,
        pipelines: list[Pipeline],
        datasets: list[Dataset],
    ) -> dict[str, Path]:
        """Generate and save all reports.
        
        Args:
            metadata: Database metadata
            ontology: Generated ontology
            pipelines: Generated pipelines
            datasets: Generated datasets
            
        Returns:
            Dict mapping report types to file paths
        """
        paths = {}
        
        # Metadata report
        metadata_content = self.generate_metadata_report(metadata)
        paths["metadata"] = self.save_report(metadata_content, self.config.metadata_report_name)
        
        # Ontology report
        ontology_content = self.generate_ontology_report(ontology, metadata)
        paths["ontology"] = self.save_report(ontology_content, self.config.ontology_report_name)
        
        # Pipeline report
        pipeline_content = self.generate_pipeline_report(pipelines, datasets)
        paths["pipelines"] = self.save_report(pipeline_content, "pipeline_report.md")
        
        # Ontology JSON
        if self.config.generate_json:
            import json
            ontology_json = json.dumps(ontology.to_json(), indent=2, ensure_ascii=False)
            json_path = self.config.output_dir / self.config.ontology_json_name
            json_path.write_text(ontology_json, encoding="utf-8")
            paths["ontology_json"] = json_path
        
        # Pipeline SQL
        if self.config.generate_sql and pipelines:
            sql_content = "\n\n-- " + "-" * 60 + "\n\n".join(
                f"-- Pipeline: {p.name}\n{p.to_sql()};" for p in pipelines
            )
            sql_path = self.config.output_dir / self.config.pipeline_sql_name
            sql_path.write_text(sql_content, encoding="utf-8")
            paths["pipelines_sql"] = sql_path
        
        return paths


# Report Templates

METADATA_REPORT_TEMPLATE = """# 数据库元数据分析报告

**数据库**: {{ database_name }}  
**生成时间**: {{ generated_at }}

---

## 📊 统计摘要

| 指标 | 数值 |
|------|------|
| 表总数 | {{ table_count }} |
| 列总数 | {{ column_count }} |
| 外键约束数 | {{ fk_count }} |
| 检测到的关系数 | {{ relationship_count }} |

---

## 📋 表详情

{% for table in tables %}
### {{ table.name }}

**Schema**: `{{ table.schema }}`  
**预估行数**: {{ table.row_count }}
{% if table.comment %}
**描述**: {{ table.comment }}
{% endif %}

#### 列信息

{% for col in table.columns %}
{{ col }}
{% endfor %}

{% if table.foreign_keys %}
#### 外键约束

{% for fk in table.foreign_keys %}
{{ fk }}
{% endfor %}
{% endif %}

---

{% endfor %}

## 🔗 检测到的关系

### 高置信度关系 (外键约束)

{% if high_conf_rels %}
| 源 | 目标 | 检测方法 |
|---|---|---|
{% for rel in high_conf_rels %}
| `{{ rel.source }}` | `{{ rel.target }}` | {{ rel.method }} |
{% endfor %}
{% else %}
*无*
{% endif %}

### 中置信度关系 (命名规则推断)

{% if medium_conf_rels %}
| 源 | 目标 | 原因 |
|---|---|---|
{% for rel in medium_conf_rels %}
| `{{ rel.source }}` | `{{ rel.target }}` | {{ rel.reason }} |
{% endfor %}
{% else %}
*无*
{% endif %}

### 低置信度关系 (相似度分析)

{% if low_conf_rels %}
| 源 | 目标 | 原因 |
|---|---|---|
{% for rel in low_conf_rels %}
| `{{ rel.source }}` | `{{ rel.target }}` | {{ rel.reason }} |
{% endfor %}
{% else %}
*无*
{% endif %}
"""

ONTOLOGY_REPORT_TEMPLATE = """# Ontology 创建报告

**Ontology 名称**: {{ ontology_name }}  
**来源数据库**: {{ database_name }}  
**版本**: {{ version }}  
**生成时间**: {{ generated_at }}

---

## 📊 统计摘要

| 指标 | 数值 |
|------|------|
| 实体类型 (Object Types) | {{ object_type_count }} |
| 属性类型 (Properties) | {{ property_count }} |
| 关系类型 (Link Types) | {{ link_type_count }} |

---

## 🏷️ 实体类型 (Object Types)

{% for obj in object_types %}
### {{ obj.name }} (`{{ obj.id }}`)

**来源表**: `{{ obj.source_table }}`  
**主键**: `{{ obj.primary_key }}`

**创建原因**: {{ obj.creation_reason }}

#### 属性列表

| 属性名 | 类型 | 主键 |
|--------|------|------|
{% for prop in obj.properties %}
| {{ prop.name }} | {{ prop.type }} | {{ "✓" if prop.pk else "" }} |
{% endfor %}

---

{% endfor %}

## 🔗 关系类型 (Link Types)

{% for link in link_types %}
### {{ link.name }} (`{{ link.id }}`)

**源实体**: `{{ link.source }}`  
**目标实体**: `{{ link.target }}`  
**基数**: {{ link.cardinality }}  
**置信度**: {{ link.confidence }}

**创建原因**: {{ link.creation_reason }}

---

{% endfor %}
"""

PIPELINE_REPORT_TEMPLATE = """# 数据管道报告

**生成时间**: {{ generated_at }}

---

## 📊 统计摘要

| 指标 | 数值 |
|------|------|
| 生成的管道数 | {{ pipeline_count }} |
| 生成的数据集数 | {{ dataset_count }} |

---

## 🔧 数据管道

{% for p in pipelines %}
### {{ p.name }}

**ID**: `{{ p.id }}`  
**描述**: {{ p.description }}  
**源表**: {{ p.source_tables }}  
**步骤数**: {{ p.step_count }}  
**输出列数**: {{ p.output_column_count }}

#### 生成的 SQL

```sql
{{ p.sql }}
```

---

{% endfor %}

## 📦 数据集

{% for d in datasets %}
### {{ d.name }}

**ID**: `{{ d.id }}`  
**描述**: {{ d.description }}  
**主要列**: {{ d.columns }}

**创建原因**: {{ d.creation_reason }}

---

{% endfor %}
"""
