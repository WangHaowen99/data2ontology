"""Ontology generator from database metadata."""

from typing import Optional
from datetime import datetime
import uuid
import re

from .config import AnalysisConfig
from .models.metadata import (
    DatabaseMetadata, 
    EnhancedDatabaseMetadata,
    TableInfo, 
    DetectedRelationship, 
    RelationshipConfidence,
    EntityInsight,
    InsightSource,
)
from .models.ontology import (
    PropertyType,
    ObjectType,
    LinkType,
    Ontology,
    OntologyDataType,
    map_pg_type_to_ontology,
)


class OntologyGenerator:
    """Generates Ontology definitions from database metadata."""

    def __init__(self, metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None):
        """Initialize the ontology generator.
        
        Args:
            metadata: Database metadata with detected relationships (can be EnhancedDatabaseMetadata)
            config: Analysis configuration
        """
        self.metadata = metadata
        self.config = config or AnalysisConfig()
        
        # Check if we have enhanced metadata with unstructured insights
        self.is_enhanced = isinstance(metadata, EnhancedDatabaseMetadata)
        if self.is_enhanced:
            self.entity_insights_map = {
                self._normalize_name(ei.table_name or ei.entity_name): ei 
                for ei in metadata.entity_insights
            }

    def generate(self) -> Ontology:
        """Generate complete Ontology from metadata.
        
        Returns:
            Ontology object
        """
        object_types = []
        link_types = []
        
        # Generate object types from tables
        for table in self.metadata.tables:
            obj_type = self._generate_object_type(table)
            object_types.append(obj_type)
        
        # Generate link types from relationships
        for rel in self.metadata.detected_relationships:
            link_type = self._generate_link_type(rel)
            link_types.append(link_type)
        
        return Ontology(
            name=f"{self.metadata.database_name}_ontology",
            description=f"从数据库 '{self.metadata.database_name}' 自动生成的 Ontology",
            version="1.0.0",
            source_database=self.metadata.database_name,
            object_types=object_types,
            link_types=link_types,
            created_at=datetime.now().isoformat(),
        )

    def _generate_object_type(self, table: TableInfo) -> ObjectType:
        """Generate an ObjectType from a table.
        
        Args:
            table: Table information
            
        Returns:
            ObjectType object
        """
        # Generate object type ID and name - 使用 PascalCase 保持与 LinkType 一致
        obj_id = self._to_pascal_case(table.name)
        obj_name = obj_id  # 使用相同的名称确保 link_type 能匹配
        display_name = self._humanize_name(table.name)  # 用于描述
        
        # Generate properties from columns
        properties = []
        for column in table.columns:
            prop = self._generate_property(table.name, column)
            properties.append(prop)
        
        # Determine creation reason
        reason = self._generate_object_type_reason(table)
        
        # Check for insights from unstructured data
        insights_from_code = None
        insights_from_logs = None
        if self.is_enhanced:
            entity_insight = self.entity_insights_map.get(self._normalize_name(table.name))
            if entity_insight:
                if InsightSource.CODE in entity_insight.sources:
                    insights_from_code = entity_insight.description_from_code or \
                        f"代码中发现此实体，相关实体: {', '.join(entity_insight.related_entities[:3])}"
                if InsightSource.LOG in entity_insight.sources:
                    ops = entity_insight.operations_from_logs
                    if ops:
                        insights_from_logs = f"日志中检测到操作: {', '.join(ops)}"
        
        return ObjectType(
            id=obj_id,
            name=obj_name,
            description=table.comment or f"实体类型 '{display_name}'，源自表 '{table.name}'",
            source_table=table.full_name,
            primary_key=[self._to_camel_case(pk) for pk in table.primary_keys],
            properties=properties,
            creation_reason=reason,
            insights_from_code=insights_from_code,
            insights_from_logs=insights_from_logs,
        )

    def _generate_property(self, table_name: str, column) -> PropertyType:
        """Generate a PropertyType from a column.
        
        Args:
            table_name: Parent table name
            column: Column information
            
        Returns:
            PropertyType object
        """
        prop_id = f"{self._to_pascal_case(table_name)}.{self._to_camel_case(column.name)}"
        prop_name = self._humanize_name(column.name)
        
        # Map PostgreSQL type to Ontology type
        ontology_type = map_pg_type_to_ontology(column.data_type)
        
        # 语义化分析列名含义
        description = self._infer_column_semantic(column.name, column.data_type, column.comment)
        
        # Generate creation reason with semantic context
        reason = self._generate_property_reason(table_name, column, description)
        
        return PropertyType(
            id=prop_id,
            name=prop_name,
            data_type=ontology_type,
            description=description,
            source_table=table_name,
            source_column=column.name,
            nullable=column.nullable,
            is_primary_key=column.is_primary_key,
            creation_reason=reason,
        )
    
    def _infer_column_semantic(self, column_name: str, data_type: str, comment: str = None) -> str:
        """Infer semantic meaning from column name and type.
        
        Args:
            column_name: Column name
            data_type: Column data type
            comment: Optional column comment
            
        Returns:
            Semantic description
        """
        if comment:
            return comment
        
        name_lower = column_name.lower()
        
        # 常见命名模式的语义映射
        semantic_patterns = {
            # 标识符
            ('id', '_id'): '唯一标识符',
            ('uuid', 'guid'): '全局唯一标识符',
            # 名称相关
            ('name', '_name'): '名称',
            ('title',): '标题',
            ('label',): '显示标签',
            # 描述相关
            ('desc', 'description'): '详细描述',
            ('comment', 'note', 'remark'): '备注说明',
            ('summary',): '摘要',
            # 时间相关
            ('created_at', 'create_time', 'createtime'): '创建时间',
            ('updated_at', 'update_time', 'updatetime', 'modified_at'): '最后修改时间',
            ('deleted_at', 'delete_time'): '删除时间（软删除）',
            ('start_time', 'begin_time', 'start_date'): '开始时间',
            ('end_time', 'finish_time', 'end_date'): '结束时间',
            ('expired_at', 'expiry_date'): '过期时间',
            # 状态相关
            ('status',): '状态',
            ('state',): '当前状态',
            ('is_active', 'active'): '是否激活',
            ('is_deleted', 'deleted'): '是否已删除',
            ('is_enabled', 'enabled'): '是否启用',
            ('is_valid', 'valid'): '是否有效',
            # 数量相关
            ('count', '_count'): '数量统计',
            ('amount', 'total'): '金额或总量',
            ('quantity', 'qty'): '数量',
            ('price', 'cost'): '价格',
            ('rate', 'ratio'): '比率',
            # 用户相关
            ('user_id', 'userid'): '关联用户标识',
            ('created_by', 'creator'): '创建者',
            ('updated_by', 'modifier'): '修改者',
            ('owner', 'owner_id'): '所有者',
            # 类型/分类
            ('type', '_type'): '类型分类',
            ('category', 'cat'): '类别',
            ('level', 'grade'): '级别或等级',
            ('priority',): '优先级',
            # 位置相关
            ('address',): '地址',
            ('location',): '位置',
            ('lat', 'latitude'): '纬度',
            ('lng', 'lon', 'longitude'): '经度',
            # 联系方式
            ('email',): '电子邮箱',
            ('phone', 'tel', 'mobile'): '电话号码',
            ('url', 'link', 'website'): '链接地址',
            # 其他常见
            ('code',): '编码',
            ('version', 'ver'): '版本号',
            ('order', 'sort', 'seq'): '排序序号',
            ('parent_id',): '父级关联标识',
            ('config', 'settings'): '配置信息',
            ('data', 'content'): '数据内容',
            ('image', 'img', 'photo', 'avatar'): '图片',
            ('file', 'attachment'): '文件附件',
        }
        
        for patterns, meaning in semantic_patterns.items():
            for pattern in patterns:
                if pattern in name_lower or name_lower.endswith(pattern):
                    return meaning
        
        # 基于数据类型的默认描述
        type_descriptions = {
            'boolean': '布尔标志',
            'bool': '布尔标志',
            'timestamp': '时间戳',
            'date': '日期',
            'json': 'JSON 数据',
            'jsonb': 'JSON 数据',
            'text': '文本内容',
            'integer': '整数值',
            'bigint': '大整数值',
            'numeric': '精确数值',
            'decimal': '精确数值',
        }
        
        for type_key, desc in type_descriptions.items():
            if type_key in data_type.lower():
                return f"{self._humanize_name(column_name)}（{desc}）"
        
        return f"属性 {self._humanize_name(column_name)}"
    
    def _generate_property_reason(self, table_name: str, column, description: str) -> str:
        """Generate creation reason for a property.
        
        Args:
            table_name: Parent table name
            column: Column information
            description: Semantic description
            
        Returns:
            Creation reason string
        """
        reasons = [f"表示{description}"]
        
        if column.is_primary_key:
            reasons.append("作为实体主键")
        
        if not column.nullable:
            reasons.append("必填字段")
        
        return "，".join(reasons)

    def _generate_link_type(self, rel: DetectedRelationship) -> LinkType:
        """Generate a LinkType from a relationship.
        
        Args:
            rel: Detected relationship
            
        Returns:
            LinkType object
        """
        # Generate link name
        source_name = self._to_pascal_case(rel.source_table)
        target_name = self._to_pascal_case(rel.target_table)
        
        # Create semantic link name
        link_name = self._generate_link_name(rel)
        link_id = f"{source_name}_to_{target_name}"
        
        # Determine cardinality (usually many-to-one for FK relationships)
        cardinality = "many-to-one"
        
        # Check for insights from unstructured data
        insights_from_code = None
        insights_from_logs = None
        if self.is_enhanced:
            # Look for matching relationship insights
            source_norm = self._normalize_name(rel.source_table)
            target_norm = self._normalize_name(rel.target_table)
            
            for rel_insight in self.metadata.relationship_insights:
                if (self._normalize_name(rel_insight.source_entity) == source_norm and 
                    self._normalize_name(rel_insight.target_entity) == target_norm):
                    
                    if InsightSource.CODE in rel_insight.sources:
                        code_evidence = [e for e in rel_insight.evidence if "代码" in e or "code" in e.lower()]
                        if code_evidence:
                            insights_from_code = "; ".join(code_evidence[:2])
                    
                    if InsightSource.LOG in rel_insight.sources:
                        log_evidence = [e for e in rel_insight.evidence if "日志" in e or "log" in e.lower()]
                        if log_evidence:
                            insights_from_logs = "; ".join(log_evidence[:2])
                    break
        
        return LinkType(
            id=link_id,
            name=link_name,
            description=f"从 {rel.source_table} 到 {rel.target_table} 的关联关系",
            source_object_type=source_name,
            target_object_type=target_name,
            cardinality=cardinality,
            source_property=self._to_camel_case(rel.source_column),
            confidence=rel.confidence.value,
            creation_reason=rel.reason,
            insights_from_code=insights_from_code,
            insights_from_logs=insights_from_logs,
        )

    def _generate_link_name(self, rel: DetectedRelationship) -> str:
        """Generate a semantic link name for a relationship.
        
        Args:
            rel: Detected relationship
            
        Returns:
            Human-readable link name
        """
        source_col = rel.source_column.lower()
        target_table = rel.target_table.lower()
        
        # Common patterns for link names
        if source_col.endswith('_id') or source_col.endswith('id'):
            # user_id -> hasUser
            base = source_col.replace('_id', '').replace('id', '').strip('_')
            if base:
                return f"has{self._to_pascal_case(base)}"
        
        # Default pattern
        return f"relatedTo{self._to_pascal_case(target_table)}"

    def _generate_object_type_reason(self, table: TableInfo) -> str:
        """Generate a reason for creating this object type.
        
        Args:
            table: Table information
            
        Returns:
            Explanation string
        """
        reasons = []
        
        # Basic reason
        reasons.append(f"表 '{table.name}' 代表业务实体")
        
        # Column count
        reasons.append(f"包含 {len(table.columns)} 个属性")
        
        # Primary key
        if table.primary_keys:
            reasons.append(f"使用 '{', '.join(table.primary_keys)}' 作为唯一标识")
        
        # Foreign keys
        if table.foreign_keys:
            fk_targets = [fk.references_table for fk in table.foreign_keys]
            reasons.append(f"与表 {', '.join(fk_targets)} 存在关联")
        
        # Row count
        if table.row_count_estimate:
            reasons.append(f"预估包含 {table.row_count_estimate:,} 条记录")
        
        return "；".join(reasons)

    def _to_pascal_case(self, name: str) -> str:
        """Convert name to PascalCase.
        
        Args:
            name: Snake_case or other format name
            
        Returns:
            PascalCase name
        """
        # Handle snake_case
        parts = name.split('_')
        return ''.join(word.capitalize() for word in parts if word)

    def _to_camel_case(self, name: str) -> str:
        """Convert name to camelCase.
        
        Args:
            name: Snake_case or other format name
            
        Returns:
            camelCase name
        """
        pascal = self._to_pascal_case(name)
        if pascal:
            return pascal[0].lower() + pascal[1:]
        return name

    def _humanize_name(self, name: str) -> str:
        """Convert name to human-readable format.
        
        Args:
            name: Technical name
            
        Returns:
            Human-readable name
        """
        # Split by underscore and capitalize each word
        parts = name.replace('_', ' ').split()
        return ' '.join(word.capitalize() for word in parts)

    def _normalize_name(self, name: str) -> str:
        """Normalize entity name for matching with insights.
        
        Args:
            name: Entity name
            
        Returns:
            Normalized name
        """
        return name.lower().strip().rstrip('s')

    def get_ontology_summary(self, ontology: Ontology) -> dict:
        """Get a summary of the ontology.
        
        Args:
            ontology: Generated ontology
            
        Returns:
            Summary dictionary
        """
        high_conf_links = len([l for l in ontology.link_types if l.confidence == "high"])
        medium_conf_links = len([l for l in ontology.link_types if l.confidence == "medium"])
        low_conf_links = len([l for l in ontology.link_types if l.confidence == "low"])
        
        return {
            "object_types_count": ontology.object_type_count,
            "total_properties_count": ontology.total_property_count,
            "link_types_count": ontology.link_type_count,
            "high_confidence_links": high_conf_links,
            "medium_confidence_links": medium_conf_links,
            "low_confidence_links": low_conf_links,
        }


def generate_ontology(metadata: DatabaseMetadata, config: Optional[AnalysisConfig] = None) -> Ontology:
    """Convenience function to generate ontology.
    
    Args:
        metadata: Database metadata
        config: Analysis configuration
        
    Returns:
        Generated Ontology
    """
    generator = OntologyGenerator(metadata, config)
    return generator.generate()
