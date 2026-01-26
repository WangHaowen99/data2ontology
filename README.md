# Auto Pipeline Builder

一个类似 Palantir Pipeline Builder 的自动化数据管道构建工具，用于从 PostgreSQL 数据库元数据自动生成数据管道和 Ontology 原型。

## 功能特性

- 🔍 **元数据提取**: 自动读取 PostgreSQL 数据库的所有表结构、列信息、约束等
- 🔗 **关系发现**: 智能检测表间关系（外键约束、命名规则、相似度分析）
- 🔧 **管道生成**: 自动生成最优 JOIN 路径和数据转换管道
- 🏷️ **Ontology 生成**: 创建实体类型、属性类型、关系类型的 Ontology 原型
- 📝 **日志分析** (可选): 从应用日志中提取业务实体引用和操作模式
- 💻 **代码分析** (可选): 从源代码中提取数据模型定义和 API 端点
- 🔄 **多源整合**: 将数据库元数据、日志和代码分析结果整合，生成更完善的 Ontology
- 🕸️ **Neo4j 集成**: 即时将 Ontology 结构导出到 Neo4j 图数据库
- 📊 **分析报告**: 输出详细的元数据分析和 Ontology 创建报告

## 安装

```bash
cd auto_pipeline_builder
pip install -r requirements.txt
```

## 快速开始

### 1. 配置数据库连接

复制环境变量示例文件并修改：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填入你的 PostgreSQL 连接信息：

```ini
PG_HOST=localhost
PG_PORT=5439
PG_DATABASE=your_database
PG_USER=your_user
PG_PASSWORD=your_password
PG_SCHEMA=public
```

### 2. 运行程序

```bash
python main.py -d your_database -u your_user -P your_password
```

或使用完整参数：

```bash
python main.py \
    --host localhost \
    --port 5432 \
    --database your_database \
    --user your_user \
    --password your_password \
    --schema public \
    --output ./output \
    --verbose
```

### 4. 导出到 Neo4j

添加 `--export-neo4j` 参数即可将 Ontology 结构同步到 Neo4j：

```bash
python main.py -d your_database -u your_user -P your_password --export-neo4j
```

确保在 `.env` 中配置了 Neo4j 连接信息：
```ini
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=secret
```

### 3. 使用非结构化数据分析 (可选)

#### 启用日志分析

从应用日志中提取业务实体引用和操作模式：

```bash
python main.py -d your_database -u your_user -P your_password \
    --enable-log-analysis \
    --log-paths ./logs/app.log \
    --log-paths ./logs/api.log
```

日志分析器会检测:
- 实体引用（user, order, product 等）
- CRUD 操作模式
- 实体间的共现关系

#### 启用代码分析

从项目源代码中提取数据模型和 API 端点：

```bash
python main.py -d your_database -u your_user -P your_password \
    --enable-code-analysis \
    --code-paths ./src/models \
    --code-paths ./src/api
```

代码分析器支持:
- Python (类定义、ORM 模型、FastAPI/Flask 路由)
- Java (实体类、Spring 注解)
- JavaScript/TypeScript (类、接口、Express 路由)

#### 同时使用多个数据源

结合数据库元数据、日志和代码分析：

```bash
python main.py -d your_database -u your_user -P your_password \
    --enable-log-analysis --log-paths ./logs/*.log \
    --enable-code-analysis --code-paths ./src \
    --output ./output \
    --verbose
```

### 4. 查看输出

运行完成后，在 `output/` 目录下会生成：

- `metadata_report.md` - 数据库元数据分析报告
- `ontology_report.md` - Ontology 创建报告（包含创建原因）
- `pipeline_report.md` - 数据管道报告
- `ontology.json` - Ontology 定义（JSON 格式）
- `pipelines.sql` - 生成的 SQL 查询

## 命令行参数

| 参数 | 简写 | 默认值 | 描述 |
|------|------|--------|------|
| `--host` | `-h` | localhost | PostgreSQL 主机地址 |
| `--port` | `-p` | 5432 | PostgreSQL 端口 |
| `--database` | `-d` | *必填* | 数据库名称 |
| `--user` | `-u` | *必填* | 数据库用户名 |
| `--password` | `-P` | *必填* | 数据库密码 |
| `--schema` | `-s` | public | 要分析的 Schema |
| `--output` | `-o` | ./output | 输出目录 |
| `--env-file` | `-e` | .env | 环境变量文件路径 |
| `--enable-log-analysis` | | false | 启用日志分析 |
| `--log-paths` | | | 日志文件路径（可多次指定） |
| `--enable-code-analysis` | | false | 启用代码分析 |
| `--code-paths` | | | 代码目录路径（可多次指定） |
| `--export-neo4j` | | false | 导出到 Neo4j |
| `--verbose` | `-v` | false | 详细输出 |

## 编程接口

也可以通过 Python 代码调用：

```python
from src.config import DatabaseConfig, AnalysisConfig, OutputConfig, AppConfig
from main import run_from_config

config = AppConfig(
    database=DatabaseConfig(
        host="localhost",
        port=5432,
        database="your_database",
        user="your_user",
        password="your_password",
    ),
    analysis=AnalysisConfig(schemas=["public"]),
    output=OutputConfig(output_dir="./output"),
)

result = run_from_config(config)

# 访问结果
metadata = result["metadata"]
ontology = result["ontology"]
pipelines = result["pipelines"]
datasets = result["datasets"]
```

## 关系检测方法

### 1. 外键约束 (高置信度 - 100%)

直接从数据库约束中提取，最可靠的关系来源。

### 2. 命名规则 (中置信度 - 80%)

检测列名模式，例如：
- `user_id` → `users` 表
- `category_fk` → `categories` 表
- `orderId` → `orders` 表

### 3. 相似度分析 (低置信度 - 60%)

基于列名和数据类型的相似度分析，用于发现潜在关系。

## Ontology 类型映射

| PostgreSQL 类型 | Ontology 类型 |
|----------------|---------------|
| integer, int4, smallint | Integer |
| bigint, int8 | Long |
| numeric, decimal, money | Decimal |
| real, float4, double precision | Double |
| boolean | Boolean |
| varchar, text, char | String |
| timestamp, timestamptz | Timestamp |
| date | Date |
| json, jsonb | Object |
| bytea | Binary |
| point, geometry, geography | GeoLocation |

## 项目结构

```
auto_pipeline_builder/
├── src/
│   ├── __init__.py
│   ├── config.py              # 配置管理
│   ├── metadata_extractor.py  # 元数据提取
│   ├── relationship_analyzer.py # 关系分析
│   ├── pipeline_builder.py    # 管道构建
│   ├── ontology_generator.py  # Ontology 生成
│   ├── report_generator.py    # 报告生成
│   └── models/
│       ├── __init__.py
│       ├── metadata.py        # 元数据模型
│       ├── ontology.py        # Ontology 模型
│       └── pipeline.py        # 管道模型
├── tests/                     # 测试文件
├── output/                    # 输出目录
├── main.py                    # 主入口
├── requirements.txt
├── .env.example
└── README.md
```

## 示例输出

### 元数据分析报告

```markdown
# 数据库元数据分析报告

## 统计摘要
- 表总数: 15
- 列总数: 87
- 外键约束: 12
- 检测到的关系: 18

## 表详情
### users
- id: integer (PK)
- name: varchar(100)
- email: varchar(255) (UNIQUE)
- created_at: timestamp
```

### Ontology 报告

```markdown
# Ontology 创建报告

## Users 实体类型
来源表: public.users
创建原因: 表 'users' 代表业务实体；包含 4 个属性；使用 'id' 作为唯一标识

## Order → User 关系
创建原因: 外键约束 orders.user_id 引用 users.id，表示订单和用户的归属关系
置信度: 100% (外键约束)
```

## 许可证

MIT License
