"""LLM-based semantic analysis for ontology generation.

This module provides configurable LLM prompts for analyzing database tables
and columns to infer their business meaning.
"""

import os
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

# 默认 Prompt 模板
DEFAULT_TABLE_ANALYSIS_PROMPT = """你是一个数据分析专家。请分析以下数据库表信息，推断它在业务系统中代表的实体含义。

## 表信息
- 表名: {table_name}
- 表注释: {table_comment}
- 列数: {column_count}
- 预估行数: {row_count}

## 列信息
{columns_info}

## 数据样本
{sample_data}

## 请分析并给出：
1. **业务实体名称**：这个表代表什么业务实体（用中文命名）
2. **实体描述**：用1-2句话描述这个实体在业务中的作用
3. **核心属性分析**：分析每个列的业务含义（中文）

请按以下JSON格式返回：
```json
{{
    "entity_name_cn": "中文实体名称",
    "entity_description": "实体的业务描述",
    "properties": [
        {{
            "column_name": "列名",
            "business_name": "业务名称（中文）",
            "business_description": "业务含义描述"
        }}
    ]
}}
```"""

DEFAULT_RELATIONSHIP_ANALYSIS_PROMPT = """你是一个数据分析专家。请分析以下两个表之间的关系，推断它们在业务中的关联含义。

## 源表: {source_table}
描述: {source_description}

## 目标表: {target_table}
描述: {target_description}

## 关联字段
源表列: {source_column}
目标表列: {target_column}

## 请分析并给出：
1. **关系名称**：这个关系的业务名称（中文动词短语，如"属于"、"包含"、"关联"）
2. **关系描述**：描述这两个实体之间的业务关系

请按以下JSON格式返回：
```json
{{
    "relationship_name_cn": "关系名称（中文）",
    "relationship_description": "关系的业务描述"
}}
```"""


@dataclass
class LLMConfig:
    """LLM configuration."""
    provider: str = "openai"  # openai, azure, local
    api_key: str = ""
    api_base: str = ""
    model: str = "gpt-4o-mini"
    temperature: float = 0.3
    max_tokens: int = 2000
    
    # Prompt 模板
    table_analysis_prompt: str = DEFAULT_TABLE_ANALYSIS_PROMPT
    relationship_analysis_prompt: str = DEFAULT_RELATIONSHIP_ANALYSIS_PROMPT


class SemanticAnalyzer:
    """LLM-based semantic analyzer for database tables."""
    
    def __init__(self, config: LLMConfig = None):
        self.config = config or LLMConfig()
        self._client = None
        
    def _get_client(self):
        """Get or create LLM client."""
        if self._client:
            return self._client
            
        if self.config.provider == "openai":
            try:
                from openai import OpenAI
                api_key = self.config.api_key or os.getenv("OPENAI_API_KEY")
                if not api_key:
                    return None
                self._client = OpenAI(
                    api_key=api_key,
                    base_url=self.config.api_base or os.getenv("OPENAI_API_BASE")
                )
            except ImportError:
                return None
        return self._client
    
    def analyze_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        sample_data: List[Dict[str, Any]] = None,
        table_comment: str = None,
        row_count: int = None
    ) -> Optional[Dict[str, Any]]:
        """Analyze a table to infer its business meaning.
        
        Args:
            table_name: Name of the table
            columns: List of column info dicts
            sample_data: Sample rows from the table
            table_comment: Table comment if any
            row_count: Estimated row count
            
        Returns:
            Analysis result dict or None if LLM not available
        """
        client = self._get_client()
        if not client:
            # 返回基于规则的分析
            return self._rule_based_table_analysis(table_name, columns, sample_data)
        
        # 构建列信息
        columns_info = "\n".join([
            f"- {col['name']} ({col['data_type']})"
            + (f" - PK" if col.get('is_primary_key') else "")
            + (f" - {col.get('comment')}" if col.get('comment') else "")
            for col in columns
        ])
        
        # 构建样本数据
        sample_str = "无样本数据"
        if sample_data and len(sample_data) > 0:
            sample_rows = sample_data[:3]  # 最多3行
            sample_str = json.dumps(sample_rows, ensure_ascii=False, indent=2, default=str)
        
        # 填充 prompt
        prompt = self.config.table_analysis_prompt.format(
            table_name=table_name,
            table_comment=table_comment or "无",
            column_count=len(columns),
            row_count=row_count or "未知",
            columns_info=columns_info,
            sample_data=sample_str
        )
        
        try:
            response = client.chat.completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": "你是一个专业的数据分析师，擅长分析数据库结构并推断业务含义。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens
            )
            
            content = response.choices[0].message.content
            
            # 提取 JSON
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(content[json_start:json_end])
                
        except Exception as e:
            print(f"LLM analysis failed: {e}")
        
        return self._rule_based_table_analysis(table_name, columns, sample_data)
    
    def _rule_based_table_analysis(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        sample_data: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Fallback rule-based analysis when LLM is not available."""
        # 基于表名推断实体名称
        entity_name = self._infer_entity_name(table_name)
        
        # 分析每个列
        properties = []
        for col in columns:
            prop = {
                "column_name": col["name"],
                "business_name": self._infer_column_name(col["name"]),
                "business_description": self._infer_column_description(
                    col["name"], 
                    col["data_type"],
                    col.get("comment"),
                    col.get("is_primary_key", False)
                )
            }
            properties.append(prop)
        
        return {
            "entity_name_cn": entity_name,
            "entity_description": f"{entity_name}信息记录",
            "properties": properties
        }
    
    def _infer_entity_name(self, table_name: str) -> str:
        """Infer Chinese entity name from table name."""
        name_lower = table_name.lower()
        
        # 移除常见前缀
        prefixes = ['raw_', 't_', 'tbl_', 'tb_', 'dim_', 'fact_', 'ods_', 'dwd_', 'dws_', 'ads_']
        for prefix in prefixes:
            if name_lower.startswith(prefix):
                name_lower = name_lower[len(prefix):]
                break
        
        # 常见实体名映射
        entity_mappings = {
            'user': '用户', 'users': '用户', 'account': '账户', 'accounts': '账户',
            'order': '订单', 'orders': '订单', 'product': '产品', 'products': '产品',
            'customer': '客户', 'customers': '客户', 'item': '项目', 'items': '项目',
            'category': '类别', 'categories': '类别', 'department': '部门',
            'employee': '员工', 'employees': '员工', 'staff': '员工',
            'project': '项目', 'projects': '项目', 'task': '任务', 'tasks': '任务',
            'log': '日志', 'logs': '日志', 'record': '记录', 'records': '记录',
            'config': '配置', 'setting': '设置', 'settings': '设置',
            'file': '文件', 'files': '文件', 'document': '文档', 'documents': '文档',
            'message': '消息', 'messages': '消息', 'notification': '通知',
            'payment': '支付', 'payments': '支付', 'transaction': '交易',
            'inventory': '库存', 'stock': '库存', 'warehouse': '仓库',
            'supplier': '供应商', 'vendor': '供应商', 'partner': '合作伙伴',
            'contract': '合同', 'agreement': '协议',
            'equipment': '设备', 'device': '设备', 'machine': '机器',
            'defect': '缺陷', 'defects': '缺陷', 'issue': '问题', 'bug': '缺陷',
            'work_order': '工单', 'workorder': '工单', 'ticket': '工单',
            'listing': '列表项', 'listings': '列表项',
            'district': '区域', 'area': '区域', 'region': '地区',
        }
        
        for eng, chn in entity_mappings.items():
            if eng in name_lower:
                return chn
        
        # 默认使用表名
        return table_name.replace('_', ' ').title()
    
    def _infer_column_name(self, column_name: str) -> str:
        """Infer Chinese column name."""
        name_lower = column_name.lower()
        
        column_mappings = {
            'id': '标识', 'uuid': '唯一标识', 'code': '编码',
            'name': '名称', 'title': '标题', 'label': '标签',
            'description': '描述', 'desc': '描述', 'content': '内容', 'text': '文本',
            'status': '状态', 'state': '状态', 'type': '类型', 'category': '类别',
            'created_at': '创建时间', 'updated_at': '更新时间', 'deleted_at': '删除时间',
            'create_time': '创建时间', 'update_time': '更新时间',
            'start_time': '开始时间', 'end_time': '结束时间',
            'price': '价格', 'amount': '金额', 'cost': '成本', 'total': '总计',
            'quantity': '数量', 'qty': '数量', 'count': '数量',
            'user_id': '用户ID', 'order_id': '订单ID', 'product_id': '产品ID',
            'parent_id': '父级ID', 'level': '层级', 'sort': '排序',
            'is_active': '是否激活', 'is_deleted': '是否删除', 'enabled': '是否启用',
            'email': '邮箱', 'phone': '电话', 'mobile': '手机', 'address': '地址',
            'remark': '备注', 'note': '备注', 'comment': '备注',
            'version': '版本', 'priority': '优先级',
        }
        
        for eng, chn in column_mappings.items():
            if eng == name_lower or name_lower.endswith(f'_{eng}'):
                return chn
        
        return column_name.replace('_', ' ').title()
    
    def _infer_column_description(
        self,
        column_name: str,
        data_type: str,
        comment: str = None,
        is_primary_key: bool = False
    ) -> str:
        """Infer column business description."""
        if comment:
            return comment
        
        name_lower = column_name.lower()
        
        # 基于列名模式推断
        if is_primary_key or name_lower == 'id':
            return "记录的唯一标识符"
        if name_lower.endswith('_id') or name_lower.endswith('id'):
            ref_name = name_lower.replace('_id', '').replace('id', '')
            return f"关联{self._infer_entity_name(ref_name)}的标识"
        if 'time' in name_lower or 'date' in name_lower or 'at' in name_lower:
            return "时间戳记录"
        if 'status' in name_lower or 'state' in name_lower:
            return "当前状态标识"
        if 'name' in name_lower or 'title' in name_lower:
            return "显示名称"
        if 'desc' in name_lower or 'content' in name_lower:
            return "详细描述信息"
        if 'price' in name_lower or 'amount' in name_lower or 'cost' in name_lower:
            return "金额数值"
        if 'count' in name_lower or 'qty' in name_lower or 'quantity' in name_lower:
            return "数量统计"
        if name_lower.startswith('is_') or name_lower.startswith('has_'):
            return "布尔标记"
        
        # 基于数据类型
        type_lower = data_type.lower()
        if 'bool' in type_lower:
            return "是/否标记"
        if 'int' in type_lower or 'numeric' in type_lower:
            return "数值"
        if 'text' in type_lower or 'varchar' in type_lower or 'char' in type_lower:
            return "文本信息"
        if 'time' in type_lower or 'date' in type_lower:
            return "时间记录"
        if 'json' in type_lower:
            return "结构化数据"
        
        return f"{self._infer_column_name(column_name)}字段"


def generate_semantic_report(
    ontology,
    table_analyses: Dict[str, Dict[str, Any]],
    relationship_analyses: Dict[str, Dict[str, Any]] = None
) -> str:
    """Generate a semantic report from analysis results.
    
    Args:
        ontology: The generated ontology
        table_analyses: Dict mapping table names to their analysis results
        relationship_analyses: Optional dict of relationship analyses
        
    Returns:
        Markdown formatted report
    """
    report = "# 本体语义分析报告\n\n"
    
    # 摘要
    report += "## 📊 概览\n\n"
    report += f"| 指标 | 数量 |\n"
    report += f"|------|------|\n"
    report += f"| 业务实体 | {ontology.object_type_count} |\n"
    report += f"| 实体关系 | {ontology.link_type_count} |\n"
    report += f"| 总属性数 | {sum(len(obj.properties) for obj in ontology.object_types)} |\n\n"
    
    # 实体分析
    report += "## 🏢 业务实体\n\n"
    
    for obj in ontology.object_types:
        table_name = obj.source_table.split('.')[-1] if '.' in obj.source_table else obj.source_table
        analysis = table_analyses.get(table_name, {})
        
        entity_name = analysis.get("entity_name_cn", obj.name)
        entity_desc = analysis.get("entity_description", obj.description)
        
        report += f"### {entity_name} ({obj.name})\n\n"
        report += f"**业务描述**: {entity_desc}\n\n"
        report += f"**数据来源**: `{obj.source_table}`\n\n"
        
        # 属性表格
        report += "| 属性 | 业务名称 | 类型 | 说明 |\n"
        report += "|------|---------|------|------|\n"
        
        prop_analyses = {p["column_name"]: p for p in analysis.get("properties", [])}
        
        for prop in obj.properties:
            col_name = prop.source_column or prop.name
            prop_info = prop_analyses.get(col_name, {})
            business_name = prop_info.get("business_name", prop.name)
            business_desc = prop_info.get("business_description", prop.description)
            pk_mark = " 🔑" if prop.is_primary_key else ""
            
            report += f"| {prop.name}{pk_mark} | {business_name} | {prop.data_type} | {business_desc} |\n"
        
        report += "\n"
    
    # 关系分析
    if ontology.link_types:
        report += "## 🔗 实体关系\n\n"
        
        for link in ontology.link_types:
            rel_key = f"{link.source_object_type}-{link.target_object_type}"
            rel_analysis = (relationship_analyses or {}).get(rel_key, {})
            
            rel_name = rel_analysis.get("relationship_name_cn", link.name)
            rel_desc = rel_analysis.get("relationship_description", link.description)
            
            report += f"- **{link.source_object_type}** → *{rel_name}* → **{link.target_object_type}**\n"
            report += f"  - {rel_desc}\n"
        
        report += "\n"
    
    return report


# 配置文件路径
PROMPTS_CONFIG_PATH = None


def get_prompts_config_path() -> str:
    """Get the path to prompts config file."""
    global PROMPTS_CONFIG_PATH
    if PROMPTS_CONFIG_PATH:
        return PROMPTS_CONFIG_PATH
    
    from pathlib import Path
    home = Path.home()
    config_dir = home / ".data2ontology"
    config_dir.mkdir(exist_ok=True)
    return str(config_dir / "prompts_config.json")


def load_prompts_config() -> Dict[str, str]:
    """Load prompts configuration from file."""
    config_path = get_prompts_config_path()
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    
    return {
        "table_analysis_prompt": DEFAULT_TABLE_ANALYSIS_PROMPT,
        "relationship_analysis_prompt": DEFAULT_RELATIONSHIP_ANALYSIS_PROMPT
    }


def save_prompts_config(config: Dict[str, str]):
    """Save prompts configuration to file."""
    config_path = get_prompts_config_path()
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
