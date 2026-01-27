import React, { useState, useEffect } from 'react';
import {
    Layout, Tree, Table, Tabs, Input, Button, Empty,
    Tag, Space, Card, Typography, Spin, message, Row, Col, Select, Modal, Form
} from 'antd';
import {
    Database, Table as TableIcon, Code2, MessageSquare,
    Play, Sparkles, Columns, Key, FileJson, Edit, Plus, Trash2, Download
} from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import { dbApi } from '../services/api';

const { Sider, Content } = Layout;
const { Title, Text } = Typography;
const { TextArea } = Input;

const ExplorerPage = () => {
    const [connections, setConnections] = useState([]);
    const [selectedConnection, setSelectedConnection] = useState(null);
    const [metadata, setMetadata] = useState(null);
    const [loading, setLoading] = useState(true);
    const [selectedTable, setSelectedTable] = useState(null);
    const [tableData, setTableData] = useState(null);

    // Query UI State
    const [sql, setSql] = useState('SELECT * FROM public.users LIMIT 10;');
    const [queryResult, setQueryResult] = useState(null);
    const [queryLoading, setQueryLoading] = useState(false);

    // Natural Language UI State
    const [nlQuery, setNlQuery] = useState('');
    const [nlResponse, setNlResponse] = useState(null);
    const [nlLoading, setNlLoading] = useState(false);

    useEffect(() => {
        fetchConnections();
    }, []);

    useEffect(() => {
        if (selectedConnection) {
            fetchMetadata(selectedConnection);
        }
    }, [selectedConnection]);

    useEffect(() => {
        if (selectedTable && selectedConnection) {
            fetchTableData(selectedTable.name);
        }
    }, [selectedTable]);

    const fetchConnections = async () => {
        try {
            const res = await dbApi.getConnections();
            const connectedOnes = (res.data || []).filter(c => c.status === 'connected');
            setConnections(connectedOnes);
            if (connectedOnes.length > 0) {
                setSelectedConnection(connectedOnes[0].id);
            }
        } catch (err) {
            console.error('Failed to fetch connections:', err);
        }
    };

    const fetchMetadata = async (connectionId) => {
        try {
            setLoading(true);
            const res = await dbApi.getMetadata(connectionId);
            const data = res.data;
            setMetadata(data);
            if (data.tables && data.tables.length > 0) {
                setSelectedTable(data.tables[0]);
                setSql(`SELECT * FROM ${data.tables[0].schema}.${data.tables[0].name} LIMIT 10;`);
            }
        } catch (err) {
            message.error('加载元数据失败');
        } finally {
            setLoading(false);
        }
    };

    const fetchTableData = async (tableName) => {
        try {
            const res = await dbApi.getTableSample(tableName, selectedConnection);
            setTableData(res.data);
        } catch (err) {
            setTableData(null);
        }
    };

    const handleTableSelect = (keys) => {
        if (keys.length > 0 && metadata) {
            const tableName = keys[0];
            const table = metadata.tables.find(t => t.name === tableName);
            if (table) {
                setSelectedTable(table);
                setSql(`SELECT * FROM ${table.schema}.${table.name} LIMIT 10;`);
            }
        }
    };

    const executeSql = async () => {
        setQueryLoading(true);
        try {
            const res = await dbApi.executeSql(sql, selectedConnection);
            setQueryResult(res.data);
        } catch (err) {
            setQueryResult({ error: err.response?.data?.detail || '查询失败' });
        } finally {
            setQueryLoading(false);
        }
    };

    const executeNlQuery = async () => {
        if (!nlQuery.trim()) return;
        setNlLoading(true);
        try {
            const res = await dbApi.executeNatural(nlQuery, selectedConnection);
            setNlResponse(res.data);
            if (res.data.sql) {
                setSql(res.data.sql);
            }
        } catch (err) {
            message.error('查询处理失败');
        } finally {
            setNlLoading(false);
        }
    };

    const exportResults = () => {
        if (!queryResult || !queryResult.rows) return;

        const csv = [
            queryResult.columns.join(','),
            ...queryResult.rows.map(row =>
                queryResult.columns.map(col => JSON.stringify(row[col] ?? '')).join(',')
            )
        ].join('\n');

        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'query_result.csv';
        a.click();
    };

    // Convert metadata to TreeData
    const treeData = (metadata && metadata.tables) ? [
        {
            title: `${metadata.database_name} (${metadata.table_count} 表)`,
            key: 'db_root',
            icon: <Database size={16} />,
            children: metadata.tables.map(table => ({
                title: table.name,
                key: table.name,
                icon: <TableIcon size={14} />,
            })),
        },
    ] : [];

    // Column table for selected table
    const columnTableCols = [
        {
            title: '列名', dataIndex: 'name', key: 'name', render: (text, r) => (
                <span>
                    {r.is_primary_key && <Key size={12} style={{ marginRight: 4, color: 'var(--color-warning)' }} />}
                    {text}
                </span>
            )
        },
        { title: '类型', dataIndex: 'data_type', key: 'data_type', render: v => <Tag>{v}</Tag> },
        { title: '可空', dataIndex: 'nullable', key: 'nullable', render: v => v ? '是' : '否' },
        { title: '备注', dataIndex: 'comment', key: 'comment', render: v => v || '-' },
    ];

    // Query result columns
    const resultCols = queryResult && queryResult.columns ? queryResult.columns.map(col => ({
        title: col,
        dataIndex: col,
        key: col,
        ellipsis: true,
        render: v => v === null ? <Text type="secondary">NULL</Text> : String(v)
    })) : [];

    if (loading) {
        return (
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 400 }}>
                <Spin size="large" />
            </div>
        );
    }

    return (
        <div style={{ maxWidth: 1400, margin: '0 auto' }}>
            <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <Title level={2} style={{ margin: 0 }}>数据浏览</Title>
                    <Text type="secondary">浏览表结构、执行查询、修改表定义</Text>
                </div>
                {connections.length > 0 && (
                    <Select
                        value={selectedConnection}
                        onChange={setSelectedConnection}
                        style={{ width: 200 }}
                        placeholder="选择数据源"
                        options={connections.map(c => ({
                            value: c.id,
                            label: (
                                <span>
                                    <Database size={14} style={{ marginRight: 6, verticalAlign: 'text-bottom' }} />
                                    {c.name || c.database}
                                </span>
                            )
                        }))}
                    />
                )}
            </div>

            <Layout style={{ background: 'transparent' }}>
                <Sider width={260} style={{ background: 'transparent', marginRight: 24 }}>
                    <Card title="数据库结构" size="small" styles={{ body: { maxHeight: 'calc(100vh - 220px)', overflowY: 'auto' } }}>
                        {metadata ? (
                            <Tree
                                showIcon
                                defaultExpandAll
                                selectedKeys={selectedTable ? [selectedTable.name] : []}
                                onSelect={handleTableSelect}
                                treeData={treeData}
                            />
                        ) : (
                            <Empty description="暂无数据" />
                        )}
                    </Card>
                </Sider>

                <Content>
                    <Tabs
                        defaultActiveKey="schema"
                        items={[
                            {
                                key: 'schema',
                                label: (<span><Columns size={16} style={{ marginRight: 6 }} />表结构</span>),
                                children: selectedTable ? (
                                    <Card
                                        title={
                                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                                <TableIcon size={18} />
                                                <span>{selectedTable.name}</span>
                                                <Tag color="blue">{selectedTable.columns?.length || 0} 列</Tag>
                                            </div>
                                        }
                                    >
                                        <Table
                                            dataSource={selectedTable.columns}
                                            columns={columnTableCols}
                                            rowKey="name"
                                            size="small"
                                            pagination={false}
                                        />

                                        {tableData && tableData.length > 0 && (
                                            <>
                                                <Title level={5} style={{ marginTop: 24 }}>示例数据</Title>
                                                <Table
                                                    dataSource={tableData}
                                                    columns={Object.keys(tableData[0] || {}).map(k => ({
                                                        title: k,
                                                        dataIndex: k,
                                                        key: k,
                                                        ellipsis: true
                                                    }))}
                                                    size="small"
                                                    scroll={{ x: 'max-content' }}
                                                    pagination={false}
                                                />
                                            </>
                                        )}
                                    </Card>
                                ) : (
                                    <Empty description="请选择一个表" />
                                )
                            },
                            {
                                key: 'sql',
                                label: (<span><Code2 size={16} style={{ marginRight: 6 }} />SQL 查询</span>),
                                children: (
                                    <Card>
                                        <TextArea
                                            value={sql}
                                            onChange={e => setSql(e.target.value)}
                                            rows={4}
                                            style={{ fontFamily: 'monospace', marginBottom: 16 }}
                                            placeholder="输入 SQL 语句..."
                                        />
                                        <Space style={{ marginBottom: 16 }}>
                                            <Button
                                                type="primary"
                                                icon={<Play size={16} />}
                                                onClick={executeSql}
                                                loading={queryLoading}
                                            >
                                                执行查询
                                            </Button>
                                            {queryResult && queryResult.rows && (
                                                <Button icon={<Download size={16} />} onClick={exportResults}>
                                                    导出 CSV
                                                </Button>
                                            )}
                                        </Space>

                                        {queryResult && (
                                            queryResult.error ? (
                                                <div style={{ color: 'var(--color-error)', padding: 16, background: '#fff2f0', borderRadius: 8 }}>
                                                    {queryResult.error}
                                                </div>
                                            ) : (
                                                <>
                                                    <Text type="secondary" style={{ marginBottom: 8, display: 'block' }}>
                                                        返回 {queryResult.rows?.length || 0} 行
                                                    </Text>
                                                    <Table
                                                        dataSource={queryResult.rows}
                                                        columns={resultCols}
                                                        size="small"
                                                        scroll={{ x: 'max-content' }}
                                                        pagination={{ pageSize: 20 }}
                                                    />
                                                </>
                                            )
                                        )}
                                    </Card>
                                )
                            },
                            {
                                key: 'nl',
                                label: (<span><Sparkles size={16} style={{ marginRight: 6 }} />自然语言</span>),
                                children: (
                                    <Card>
                                        <div style={{ marginBottom: 16 }}>
                                            <Text type="secondary">
                                                用自然语言描述您的查询需求，系统将自动生成 SQL 语句
                                            </Text>
                                        </div>
                                        <Input.Search
                                            value={nlQuery}
                                            onChange={e => setNlQuery(e.target.value)}
                                            placeholder="例如：查询销售额最高的前10个产品"
                                            enterButton={<><Sparkles size={14} style={{ marginRight: 4 }} />生成查询</>}
                                            loading={nlLoading}
                                            onSearch={executeNlQuery}
                                            size="large"
                                        />

                                        {nlResponse && (
                                            <div style={{ marginTop: 24 }}>
                                                {nlResponse.sql && (
                                                    <Card title="生成的 SQL" size="small" style={{ marginBottom: 16 }}>
                                                        <pre style={{ margin: 0, fontFamily: 'monospace', background: '#f5f5f5', padding: 12, borderRadius: 4 }}>
                                                            {nlResponse.sql}
                                                        </pre>
                                                        <Button
                                                            type="primary"
                                                            size="small"
                                                            style={{ marginTop: 8 }}
                                                            onClick={() => {
                                                                setSql(nlResponse.sql);
                                                                executeSql();
                                                            }}
                                                        >
                                                            执行此查询
                                                        </Button>
                                                    </Card>
                                                )}
                                                {nlResponse.explanation && (
                                                    <Card title="解释" size="small">
                                                        <ReactMarkdown>{nlResponse.explanation}</ReactMarkdown>
                                                    </Card>
                                                )}
                                            </div>
                                        )}
                                    </Card>
                                )
                            }
                        ]}
                    />
                </Content>
            </Layout>
        </div>
    );
};

export default ExplorerPage;
