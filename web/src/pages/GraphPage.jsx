import React, { useState, useEffect, useRef } from 'react';
import {
    Card, Button, Typography, Space, message, Tabs, Form, Input, Tag,
    Row, Col, Modal, Table, Drawer, Select, Popconfirm, Divider, Empty, Alert
} from 'antd';
import { Graph } from '@antv/g6';
import {
    Share2, RefreshCw, Settings, Edit, Plus, Trash2, Download, Upload, Eye, Save
} from 'lucide-react';
import { neo4jApi, ontologyApi } from '../services/api';

const { Title, Text } = Typography;

// 节点颜色映射
const NODE_COLORS = {
    Person: { fill: '#E3F2FD', stroke: '#1976D2' },
    Organization: { fill: '#E8F5E9', stroke: '#388E3C' },
    Product: { fill: '#FFF3E0', stroke: '#F57C00' },
    Location: { fill: '#F3E5F5', stroke: '#7B1FA2' },
    Event: { fill: '#FFEBEE', stroke: '#D32F2F' },
    default: { fill: '#E7F5FF', stroke: '#228BE6' }
};

const GraphPage = () => {
    const containerRef = useRef(null);
    const graphRef = useRef(null);

    // Connection state
    const [neo4jConfig, setNeo4jConfig] = useState({
        uri: 'bolt://localhost:7687',
        user: 'neo4j',
        password: ''
    });
    const [connectionStatus, setConnectionStatus] = useState('disconnected');
    const [connecting, setConnecting] = useState(false);

    // Graph data
    const [graphData, setGraphData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [exporting, setExporting] = useState(false);

    // Selected node for property panel
    const [selectedNode, setSelectedNode] = useState(null);
    const [selectedEdge, setSelectedEdge] = useState(null);

    // Edit mode
    const [editDrawerOpen, setEditDrawerOpen] = useState(false);
    const [editMode, setEditMode] = useState('node'); // 'node' or 'edge'
    const [editForm] = Form.useForm();

    // Ontology import
    const [ontologies, setOntologies] = useState([]);
    const [selectedOntology, setSelectedOntology] = useState(null);
    const [importModalOpen, setImportModalOpen] = useState(false);

    // Stats
    const [stats, setStats] = useState(null);

    useEffect(() => {
        checkNeo4jStatus();
        fetchOntologies();
    }, []);

    useEffect(() => {
        if (graphData && containerRef.current) {
            renderGraph(graphData);
        }
        return () => {
            if (graphRef.current) {
                graphRef.current.destroy();
                graphRef.current = null;
            }
        };
    }, [graphData]);

    const checkNeo4jStatus = async () => {
        try {
            const res = await neo4jApi.getStatus();
            setConnectionStatus(res.data.status);
        } catch {
            setConnectionStatus('disconnected');
        }
    };

    const fetchOntologies = async () => {
        try {
            const res = await ontologyApi.getOntologies();
            setOntologies(res.data || []);
        } catch {
            setOntologies([]);
        }
    };

    const handleConnect = async () => {
        setConnecting(true);
        try {
            await neo4jApi.connect(neo4jConfig);
            setConnectionStatus('connected');
            message.success('Neo4j 连接成功');
            fetchGraph();
        } catch (err) {
            message.error(err.response?.data?.detail || 'Neo4j 连接失败');
        } finally {
            setConnecting(false);
        }
    };

    const handleExport = async () => {
        setExporting(true);
        try {
            const res = await neo4jApi.export(neo4jConfig, selectedOntology);
            message.success('成功导出到 Neo4j');
            setStats(res.data.stats);
            fetchGraph();
        } catch (err) {
            message.error(err.response?.data?.detail || '导出失败，请检查 Neo4j 连接');
        } finally {
            setExporting(false);
        }
    };

    const fetchGraph = async () => {
        setLoading(true);
        try {
            const res = await neo4jApi.getGraph(neo4jConfig);
            setGraphData(res.data);
            if (res.data.nodes.length === 0) {
                message.info('Neo4j 中暂无数据');
            }
        } catch (err) {
            message.error('获取图数据失败');
        } finally {
            setLoading(false);
        }
    };

    const renderGraph = (data) => {
        if (!containerRef.current) return;

        if (graphRef.current) {
            graphRef.current.destroy();
        }

        const nodes = data.nodes.map(n => ({
            id: n.id,
            data: {
                label: n.properties?.name || n.properties?.id || n.label || 'Node',
                nodeType: n.label,
                ...n.properties
            }
        }));

        const edges = data.edges.map((e, index) => ({
            id: `edge-${index}`,
            source: e.source,
            target: e.target,
            data: {
                label: e.label || '',
                ...e.properties
            }
        }));

        const width = containerRef.current.scrollWidth || 800;
        const height = containerRef.current.scrollHeight || 600;

        try {
            const graph = new Graph({
                container: containerRef.current,
                width,
                height,
                data: { nodes, edges },
                layout: {
                    type: 'force',
                    preventOverlap: true,
                    linkDistance: 150,
                },
                behaviors: ['drag-canvas', 'zoom-canvas', 'drag-element', 'click-select'],
                node: {
                    style: {
                        size: 40,
                        fill: (d) => {
                            const color = NODE_COLORS[d.data?.nodeType] || NODE_COLORS.default;
                            return color.fill;
                        },
                        stroke: (d) => {
                            const color = NODE_COLORS[d.data?.nodeType] || NODE_COLORS.default;
                            return color.stroke;
                        },
                        lineWidth: 2,
                        labelText: (d) => String(d.data?.label || d.id || 'Node'),
                        labelPlacement: 'bottom',
                        labelFontSize: 12,
                        labelFill: '#333',
                    },
                },
                edge: {
                    style: {
                        stroke: '#A5D8FF',
                        endArrow: true,
                        labelText: (d) => String(d.data?.label || ''),
                        labelFontSize: 10,
                        labelFill: '#868E96',
                    },
                },
            });

            // Node click handler
            graph.on('node:click', (evt) => {
                const nodeId = evt.target.id;
                const nodeData = data.nodes.find(n => n.id === nodeId);
                setSelectedNode(nodeData);
                setSelectedEdge(null);
            });

            // Edge click handler
            graph.on('edge:click', (evt) => {
                const edgeIndex = parseInt(evt.target.id?.replace('edge-', ''));
                if (!isNaN(edgeIndex) && data.edges[edgeIndex]) {
                    setSelectedEdge(data.edges[edgeIndex]);
                    setSelectedNode(null);
                }
            });

            // Canvas click to deselect
            graph.on('canvas:click', () => {
                setSelectedNode(null);
                setSelectedEdge(null);
            });

            graph.render();
            graphRef.current = graph;
        } catch (err) {
            console.error('G6 render error:', err);
            message.error('图形渲染失败: ' + err.message);
        }
    };

    const handleCreateNode = () => {
        setEditMode('node');
        editForm.resetFields();
        setEditDrawerOpen(true);
    };

    const handleCreateEdge = () => {
        setEditMode('edge');
        editForm.resetFields();
        setEditDrawerOpen(true);
    };

    const handleEditSubmit = async (values) => {
        try {
            if (editMode === 'node') {
                await neo4jApi.createNode({
                    label: values.label,
                    properties: { name: values.name, ...values.properties }
                }, neo4jConfig);
                message.success('节点创建成功');
            } else {
                await neo4jApi.createRelationship({
                    source_id: values.source_id,
                    target_id: values.target_id,
                    type: values.type,
                    properties: values.properties
                }, neo4jConfig);
                message.success('关系创建成功');
            }
            setEditDrawerOpen(false);
            fetchGraph();
        } catch (err) {
            message.error('操作失败: ' + (err.response?.data?.detail || err.message));
        }
    };

    const handleDeleteNode = async (nodeId) => {
        try {
            await neo4jApi.deleteNode(nodeId, neo4jConfig);
            message.success('节点已删除');
            setSelectedNode(null);
            fetchGraph();
        } catch (err) {
            message.error('删除失败');
        }
    };

    const handleImportOntology = async () => {
        if (!selectedOntology) {
            message.warning('请选择要导入的本体');
            return;
        }
        try {
            const res = await neo4jApi.importFromOntology(selectedOntology, neo4jConfig);
            message.success(res.data.message);
            setImportModalOpen(false);
            fetchGraph();
        } catch (err) {
            message.error('导入失败');
        }
    };

    return (
        <div style={{ maxWidth: 1400, margin: '0 auto' }}>
            <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <Title level={2} style={{ margin: 0 }}>知识图谱</Title>
                    <Text type="secondary">可视化和编辑 Neo4j 中的知识图谱</Text>
                </div>
                <Space>
                    <Tag color={connectionStatus === 'connected' ? 'success' : 'error'}>
                        {connectionStatus === 'connected' ? '已连接' : '未连接'}
                    </Tag>
                    <Button icon={<RefreshCw size={16} />} onClick={fetchGraph} loading={loading}>
                        刷新
                    </Button>
                    <Button type="primary" icon={<Download size={16} />} onClick={handleExport} loading={exporting}>
                        同步到 Neo4j
                    </Button>
                </Space>
            </div>

            <Tabs
                defaultActiveKey="visualize"
                items={[
                    {
                        key: 'connection',
                        label: '连接管理',
                        children: (
                            <Row gutter={24}>
                                <Col span={12}>
                                    <Card title="Neo4j 连接配置">
                                        <Form layout="vertical">
                                            <Form.Item label="URI">
                                                <Input
                                                    value={neo4jConfig.uri}
                                                    onChange={e => setNeo4jConfig({ ...neo4jConfig, uri: e.target.value })}
                                                    placeholder="bolt://localhost:7687"
                                                />
                                            </Form.Item>
                                            <Form.Item label="用户名">
                                                <Input
                                                    value={neo4jConfig.user}
                                                    onChange={e => setNeo4jConfig({ ...neo4jConfig, user: e.target.value })}
                                                    placeholder="neo4j"
                                                />
                                            </Form.Item>
                                            <Form.Item label="密码">
                                                <Input.Password
                                                    value={neo4jConfig.password}
                                                    onChange={e => setNeo4jConfig({ ...neo4jConfig, password: e.target.value })}
                                                    placeholder="输入密码"
                                                />
                                            </Form.Item>
                                            <Button
                                                type="primary"
                                                onClick={handleConnect}
                                                loading={connecting}
                                                block
                                            >
                                                连接
                                            </Button>
                                        </Form>
                                    </Card>
                                </Col>
                                <Col span={12}>
                                    <Card title="本体导入">
                                        <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
                                            从「本体建模」生成的本体导入到 Neo4j
                                        </Text>
                                        {ontologies.length > 0 ? (
                                            <>
                                                <Select
                                                    value={selectedOntology}
                                                    onChange={setSelectedOntology}
                                                    style={{ width: '100%', marginBottom: 16 }}
                                                    placeholder="选择本体"
                                                    options={ontologies.map(o => ({
                                                        value: o.id,
                                                        label: `${o.name} (${o.object_type_count} 实体)`
                                                    }))}
                                                />
                                                <Button
                                                    icon={<Upload size={16} />}
                                                    onClick={handleImportOntology}
                                                    disabled={!selectedOntology}
                                                    block
                                                >
                                                    导入本体结构
                                                </Button>
                                            </>
                                        ) : (
                                            <Empty description="暂无可导入的本体" />
                                        )}
                                    </Card>
                                </Col>
                            </Row>
                        )
                    },
                    {
                        key: 'visualize',
                        label: '图谱可视化',
                        children: (
                            <Row gutter={24}>
                                <Col span={selectedNode || selectedEdge ? 18 : 24}>
                                    <Card
                                        title="知识图谱"
                                        styles={{ body: { padding: 0, height: 550 } }}
                                        extra={
                                            <Space>
                                                <Button size="small" icon={<Plus size={14} />} onClick={handleCreateNode}>
                                                    新建节点
                                                </Button>
                                                <Button size="small" icon={<Share2 size={14} />} onClick={handleCreateEdge}>
                                                    新建关系
                                                </Button>
                                            </Space>
                                        }
                                    >
                                        <div
                                            ref={containerRef}
                                            style={{ width: '100%', height: 550 }}
                                        />
                                    </Card>
                                </Col>

                                {(selectedNode || selectedEdge) && (
                                    <Col span={6}>
                                        <Card
                                            title={selectedNode ? '节点属性' : '关系属性'}
                                            extra={
                                                selectedNode && (
                                                    <Popconfirm
                                                        title="确定删除此节点？"
                                                        onConfirm={() => handleDeleteNode(selectedNode.id)}
                                                    >
                                                        <Button size="small" danger icon={<Trash2 size={14} />} />
                                                    </Popconfirm>
                                                )
                                            }
                                        >
                                            {selectedNode && (
                                                <>
                                                    <div style={{ marginBottom: 12 }}>
                                                        <Tag color="blue">{selectedNode.label}</Tag>
                                                    </div>
                                                    <Table
                                                        dataSource={Object.entries(selectedNode.properties || {}).map(([k, v]) => ({
                                                            key: k,
                                                            property: k,
                                                            value: String(v)
                                                        }))}
                                                        columns={[
                                                            { title: '属性', dataIndex: 'property', key: 'property' },
                                                            { title: '值', dataIndex: 'value', key: 'value', ellipsis: true }
                                                        ]}
                                                        size="small"
                                                        pagination={false}
                                                    />
                                                </>
                                            )}
                                            {selectedEdge && (
                                                <>
                                                    <div style={{ marginBottom: 12 }}>
                                                        <Tag color="green">{selectedEdge.label}</Tag>
                                                    </div>
                                                    <Text type="secondary">
                                                        {selectedEdge.source} → {selectedEdge.target}
                                                    </Text>
                                                    {selectedEdge.properties && Object.keys(selectedEdge.properties).length > 0 && (
                                                        <Table
                                                            style={{ marginTop: 12 }}
                                                            dataSource={Object.entries(selectedEdge.properties).map(([k, v]) => ({
                                                                key: k,
                                                                property: k,
                                                                value: String(v)
                                                            }))}
                                                            columns={[
                                                                { title: '属性', dataIndex: 'property', key: 'property' },
                                                                { title: '值', dataIndex: 'value', key: 'value' }
                                                            ]}
                                                            size="small"
                                                            pagination={false}
                                                        />
                                                    )}
                                                </>
                                            )}
                                        </Card>
                                    </Col>
                                )}
                            </Row>
                        )
                    },
                    {
                        key: 'edit',
                        label: '编辑管理',
                        children: (
                            <Card>
                                <Alert
                                    type="info"
                                    title="编辑说明"
                                    description="在「图谱可视化」Tab 中点击节点或关系，然后在右侧属性面板进行编辑或删除操作。"
                                    showIcon
                                    style={{ marginBottom: 24 }}
                                />

                                <Row gutter={24}>
                                    <Col span={12}>
                                        <Card title="快捷操作" size="small">
                                            <Space direction="vertical" style={{ width: '100%' }}>
                                                <Button icon={<Plus size={16} />} onClick={handleCreateNode} block>
                                                    创建新节点
                                                </Button>
                                                <Button icon={<Share2 size={16} />} onClick={handleCreateEdge} block>
                                                    创建新关系
                                                </Button>
                                                <Divider />
                                                <Button icon={<Download size={16} />} onClick={handleExport} loading={exporting} block>
                                                    同步本体数据到 Neo4j
                                                </Button>
                                            </Space>
                                        </Card>
                                    </Col>
                                    <Col span={12}>
                                        <Card title="统计信息" size="small">
                                            {graphData ? (
                                                <div>
                                                    <p><strong>节点数量:</strong> {graphData.nodes?.length || 0}</p>
                                                    <p><strong>关系数量:</strong> {graphData.edges?.length || 0}</p>
                                                    {stats && (
                                                        <>
                                                            <Divider />
                                                            <p><strong>最近同步:</strong></p>
                                                            <p>- 创建节点: {stats.nodes_created || 0}</p>
                                                            <p>- 创建关系: {stats.relationships_created || 0}</p>
                                                        </>
                                                    )}
                                                </div>
                                            ) : (
                                                <Empty description="暂无数据" />
                                            )}
                                        </Card>
                                    </Col>
                                </Row>
                            </Card>
                        )
                    }
                ]}
            />

            {/* Create/Edit Drawer */}
            <Drawer
                title={editMode === 'node' ? '创建节点' : '创建关系'}
                open={editDrawerOpen}
                onClose={() => setEditDrawerOpen(false)}
                width={400}
            >
                <Form form={editForm} layout="vertical" onFinish={handleEditSubmit}>
                    {editMode === 'node' ? (
                        <>
                            <Form.Item name="label" label="节点类型" rules={[{ required: true }]}>
                                <Input placeholder="Person, Organization, etc." />
                            </Form.Item>
                            <Form.Item name="name" label="名称" rules={[{ required: true }]}>
                                <Input placeholder="节点名称" />
                            </Form.Item>
                        </>
                    ) : (
                        <>
                            <Form.Item name="source_id" label="源节点 ID" rules={[{ required: true }]}>
                                <Input placeholder="源节点 Element ID" />
                            </Form.Item>
                            <Form.Item name="target_id" label="目标节点 ID" rules={[{ required: true }]}>
                                <Input placeholder="目标节点 Element ID" />
                            </Form.Item>
                            <Form.Item name="type" label="关系类型" rules={[{ required: true }]}>
                                <Input placeholder="KNOWS, WORKS_AT, etc." />
                            </Form.Item>
                        </>
                    )}
                    <Button type="primary" htmlType="submit" block>
                        提交
                    </Button>
                </Form>
            </Drawer>
        </div>
    );
};

export default GraphPage;
