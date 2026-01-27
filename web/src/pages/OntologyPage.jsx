import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
    Card, Checkbox, Button, Typography, Row, Col,
    Tabs, Spin, message, Divider, Tag, Empty, Select, Upload,
    Steps, Badge, Flex, Space, ColorPicker, Timeline, Modal, Progress, Input
} from 'antd';
import {
    FileText, Code, Upload as UploadIcon,
    Database, Play, ChevronDown, ChevronUp, Download, Layers, GitBranch, X, Settings
} from 'lucide-react';
import { Graph } from '@antv/g6';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { dbApi, ontologyApi, configApi } from '../services/api';

const { Title, Text } = Typography;

const STORAGE_KEY = 'data2ontology_result';

const DEFAULT_COLORS = [
    '#E3F2FD', '#E8F5E9', '#FFF3E0', '#F3E5F5', '#FFEBEE',
    '#E0F7FA', '#F9FBE7', '#FCE4EC', '#EFEBE9', '#ECEFF1'
];

const STROKE_COLORS = {
    '#E3F2FD': '#1976D2', '#E8F5E9': '#388E3C', '#FFF3E0': '#F57C00', '#F3E5F5': '#7B1FA2', '#FFEBEE': '#D32F2F',
    '#E0F7FA': '#0097A7', '#F9FBE7': '#AFB42B', '#FCE4EC': '#C2185B', '#EFEBE9': '#5D4037', '#ECEFF1': '#455A64'
};

const OntologyPage = () => {
    const [connections, setConnections] = useState([]);
    const [selectedConnection, setSelectedConnection] = useState(null);
    const [connectionStatus, setConnectionStatus] = useState('disconnected');
    const [tables, setTables] = useState([]);
    const [selectedTables, setSelectedTables] = useState([]);
    const [uploadedFiles, setUploadedFiles] = useState([]);

    const [generating, setGenerating] = useState(false);
    const [currentStep, setCurrentStep] = useState(-1);
    const [stepLogs, setStepLogs] = useState([]);
    const [result, setResult] = useState(null);

    const [importing, setImporting] = useState(false);
    const [importProgress, setImportProgress] = useState(0);
    const [importedData, setImportedData] = useState(null);

    const graphContainerRef = useRef(null);
    const graphRef = useRef(null);
    const nodesDataRef = useRef([]); // 保存节点数据用于查找
    const [selectedNode, setSelectedNode] = useState(null);
    const [visualConfig, setVisualConfig] = useState({});
    const [contextMenu, setContextMenu] = useState({ visible: false, x: 0, y: 0, nodeType: null });

    const [configCollapsed, setConfigCollapsed] = useState(false);
    const [graphPanelCollapsed, setGraphPanelCollapsed] = useState(false);
    const [viewMode, setViewMode] = useState('schema');

    const [promptsModalVisible, setPromptsModalVisible] = useState(false);
    const [promptsConfig, setPromptsConfig] = useState({
        table_analysis_prompt: '',
        relationship_analysis_prompt: ''
    });
    const [promptsSaving, setPromptsSaving] = useState(false);

    // 节点/关系编辑
    const [nodeEditModal, setNodeEditModal] = useState({ visible: false, mode: 'add', node: null });
    const [edgeEditModal, setEdgeEditModal] = useState({ visible: false, mode: 'add', edge: null });
    const [nodeForm, setNodeForm] = useState({ name: '', type: '', description: '' });
    const [edgeForm, setEdgeForm] = useState({ source: '', target: '', name: '' });
    const edgesDataRef = useRef([]); // 保存边数据

    // 从 localStorage 恢复
    useEffect(() => {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) {
            try {
                const parsed = JSON.parse(saved);
                setResult(parsed.result);
                setStepLogs(parsed.stepLogs || []);
                setVisualConfig(parsed.visualConfig || {});
                setSelectedTables(parsed.selectedTables || []);
                setImportedData(parsed.importedData || null);
                setViewMode(parsed.viewMode || 'schema');
                setConfigCollapsed(true);
            } catch (e) {
                console.error('Failed to restore:', e);
            }
        }
        fetchConnections();
    }, []);

    const saveState = useCallback(() => {
        if (result) {
            localStorage.setItem(STORAGE_KEY, JSON.stringify({
                result, stepLogs, visualConfig, selectedTables, importedData, viewMode
            }));
        }
    }, [result, stepLogs, visualConfig, selectedTables, importedData, viewMode]);

    useEffect(() => { saveState(); }, [saveState]);

    useEffect(() => {
        if (selectedConnection) {
            fetchTables(selectedConnection);
            setConnectionStatus('connected');
        } else {
            setConnectionStatus('disconnected');
            setTables([]);
        }
    }, [selectedConnection]);

    // 仅在 result/viewMode/importedData 变化时重绘图谱，不包含 selectedNode
    useEffect(() => {
        if (result && graphContainerRef.current) {
            const timer = setTimeout(() => {
                if (viewMode === 'schema') {
                    renderSchemaGraph(result.ontology, visualConfig);
                } else if (importedData) {
                    renderDataGraph(importedData, visualConfig);
                }
            }, 50);
            return () => clearTimeout(timer);
        }
    }, [result, viewMode, importedData, visualConfig]);

    const fetchConnections = async () => {
        try {
            const res = await dbApi.getConnections();
            const connectedOnes = (res.data || []).filter(c => c.status === 'connected');
            setConnections(connectedOnes);
            if (connectedOnes.length > 0 && !selectedConnection) {
                setSelectedConnection(connectedOnes[0].id);
            }
        } catch (err) {
            console.error('Failed to fetch:', err);
        }
    };

    const loadPromptsConfig = async () => {
        try {
            const res = await configApi.getPrompts();
            setPromptsConfig(res.data);
        } catch (err) {
            console.error('Failed to load prompts:', err);
        }
    };

    const savePromptsConfig = async () => {
        setPromptsSaving(true);
        try {
            await configApi.savePrompts(promptsConfig);
            message.success('Prompt 配置已保存');
            setPromptsModalVisible(false);
        } catch (err) {
            message.error('保存失败');
        } finally {
            setPromptsSaving(false);
        }
    };

    const handleOpenPromptsModal = () => {
        loadPromptsConfig();
        setPromptsModalVisible(true);
    };

    const fetchTables = async (connectionId) => {
        try {
            const res = await dbApi.getMetadata(connectionId);
            const tableList = res.data.tables.map(t => ({
                name: t.name,
                columnCount: t.columns?.length || 0,
                columns: t.columns || [],
                rowCount: t.row_count_estimate
            }));
            setTables(tableList);
            if (selectedTables.length === 0) {
                setSelectedTables(tableList.map(t => t.name));
            }
            setConnectionStatus('connected');
        } catch (err) {
            message.error('加载表信息失败');
            setTables([]);
            setConnectionStatus('disconnected');
        }
    };

    const addStepLog = (step, content, status = 'process') => {
        setStepLogs(prev => [...prev, { step, content, status, time: new Date().toLocaleTimeString() }]);
    };

    // 前端推断关系
    const inferRelationshipsFromTables = (objectTypes) => {
        const edges = [];
        const nodeNames = new Set(objectTypes.map(obj => obj.name));

        objectTypes.forEach(obj => {
            const props = obj.properties || [];
            props.forEach(prop => {
                const propName = prop.source_column?.toLowerCase() || prop.name?.toLowerCase() || '';

                if (propName.endsWith('_id') || propName.endsWith('id')) {
                    const potentialTarget = propName.replace(/_id$/, '').replace(/id$/, '');

                    objectTypes.forEach(targetObj => {
                        const targetName = targetObj.name.toLowerCase();
                        const targetTable = (targetObj.source_table || '').toLowerCase().replace(/^public\./, '');

                        if (obj.name !== targetObj.name &&
                            (targetName.includes(potentialTarget) ||
                                targetTable.includes(potentialTarget) ||
                                potentialTarget.includes(targetName.substring(0, 4)))) {

                            if (nodeNames.has(obj.name) && nodeNames.has(targetObj.name)) {
                                edges.push({
                                    id: `inferred-${obj.name}-${targetObj.name}-${propName}`,
                                    source: obj.name,
                                    target: targetObj.name,
                                    data: { label: `has${targetObj.name}` }
                                });
                            }
                        }
                    });
                }
            });
        });

        const uniqueEdges = [];
        const seen = new Set();
        edges.forEach(e => {
            const key = `${e.source}-${e.target}`;
            if (!seen.has(key)) {
                seen.add(key);
                uniqueEdges.push(e);
            }
        });

        return uniqueEdges;
    };

    const handleGenerate = async () => {
        if (selectedTables.length === 0) {
            message.warning('请至少选择一个表');
            return;
        }

        setGenerating(true);
        setResult(null);
        setImportedData(null);
        setViewMode('schema');
        setCurrentStep(0);
        setStepLogs([]);
        setConfigCollapsed(false);
        setSelectedNode(null);

        addStepLog(0, `分析 ${selectedTables.length} 个数据表...`);

        try {
            // 1. 启动任务
            const startRes = await ontologyApi.generate(selectedTables, selectedConnection);
            const taskId = startRes.data.task_id;

            // 2. 轮询状态
            const pollInterval = setInterval(async () => {
                try {
                    const statusRes = await ontologyApi.getTaskStatus(taskId);
                    const task = statusRes.data;

                    // 更新进度条和日志
                    if (task.logs && task.logs.length > 0) {
                        // 只添加新日志
                        setStepLogs(prev => {
                            const existingContents = new Set(prev.map(l => l.content + l.time));
                            const newLogs = task.logs
                                .filter(l => !existingContents.has(l.content + l.time))
                                .map(l => ({ ...l, step: currentStep })); // 保持 step 格式
                            return [...prev, ...newLogs];
                        });
                    }

                    if (task.status === 'completed') {
                        clearInterval(pollInterval);

                        // 处理完成逻辑
                        const ontology = task.result.ontology;

                        let linkCount = ontology.link_types?.length || 0;
                        if (linkCount === 0 && ontology.object_types?.length > 1) {
                            const inferredEdges = inferRelationshipsFromTables(ontology.object_types);
                            if (inferredEdges.length > 0) {
                                ontology.inferred_links = inferredEdges;
                                linkCount = inferredEdges.length;
                                addStepLog(2, `前端推断 ${linkCount} 个潜在关系`, 'finish');
                            }
                        }

                        setCurrentStep(3);
                        addStepLog(3, '渲染 Schema 图谱...', 'process');

                        setResult(task.result);

                        const initialConfig = {};
                        (ontology.object_types || []).forEach((obj, idx) => {
                            initialConfig[obj.name] = { color: DEFAULT_COLORS[idx % DEFAULT_COLORS.length] };
                        });
                        setVisualConfig(initialConfig);

                        setTimeout(() => {
                            addStepLog(3, 'Schema 图谱渲染完成', 'finish');
                            setCurrentStep(4);
                        }, 300);

                        message.success('本体 Schema 生成成功！');
                        setGenerating(false);
                    } else if (task.status === 'error') {
                        clearInterval(pollInterval);
                        addStepLog(currentStep, task.message || '生成失败', 'error');
                        message.error(task.message || '生成失败');
                        setGenerating(false);
                    }
                } catch (err) {
                    console.error("Poll error", err);
                }
            }, 1000);

        } catch (err) {
            addStepLog(currentStep, `错误: ${err.response?.data?.detail || err.message}`, 'error');
            message.error(err.response?.data?.detail || '启动生成失败');
            setGenerating(false);
        }
    };

    const handleImportData = async () => {
        if (!result) return;

        setImporting(true);
        setImportProgress(0);
        setSelectedNode(null);
        addStepLog(4, '导入表数据...', 'process');

        try {
            const objectTypes = result.ontology.object_types || [];
            const allNodes = [];
            const allEdges = [];
            const nodesByType = {};

            for (let i = 0; i < objectTypes.length; i++) {
                const obj = objectTypes[i];
                setImportProgress(Math.round(((i + 1) / objectTypes.length) * 100));
                addStepLog(4, `导入 ${obj.source_table}...`);

                try {
                    const tableName = obj.source_table?.includes('.')
                        ? obj.source_table.split('.').pop()
                        : obj.source_table;
                    const sampleRes = await dbApi.getTableSample(tableName, selectedConnection);
                    const rows = sampleRes.data || [];

                    nodesByType[obj.name] = [];

                    // 查找主键字段名
                    const pkFields = (obj.properties || []).filter(p => p.is_primary_key).map(p => p.source_column || p.name);

                    rows.forEach((row, rowIdx) => {
                        const nodeId = `${obj.name}_${rowIdx}`;

                        // 优先使用主键值作为标签
                        let label = '';
                        for (const pk of pkFields) {
                            if (row[pk] !== undefined && row[pk] !== null) {
                                label = String(row[pk]);
                                break;
                            }
                        }
                        // 如果没有主键值，尝试其他常见字段
                        if (!label) {
                            label = row.name || row.title || row.id?.toString() || `#${rowIdx + 1}`;
                        }

                        const node = {
                            id: nodeId,
                            data: {
                                label: label,
                                type: obj.name,
                                properties: row
                            }
                        };
                        allNodes.push(node);
                        nodesByType[obj.name].push(node);
                    });
                } catch (e) {
                    console.warn(`Failed to load ${obj.source_table}:`, e);
                }

                await new Promise(r => setTimeout(r, 100));
            }

            const linkTypes = result.ontology.link_types || result.ontology.inferred_links || [];
            linkTypes.forEach((link, idx) => {
                const sourceType = link.source_object_type || link.source;
                const targetType = link.target_object_type || link.target;
                const sourceNodes = nodesByType[sourceType] || [];
                const targetNodes = nodesByType[targetType] || [];

                if (sourceNodes.length > 0 && targetNodes.length > 0) {
                    const count = Math.min(sourceNodes.length, targetNodes.length, 5);
                    for (let i = 0; i < count; i++) {
                        allEdges.push({
                            id: `edge_${idx}_${i}`,
                            source: sourceNodes[i].id,
                            target: targetNodes[i % targetNodes.length].id,
                            data: { label: link.name || link.data?.label || '' }
                        });
                    }
                }
            });

            setImportedData({ nodes: allNodes, edges: allEdges });
            setViewMode('data');
            addStepLog(4, `完成：${allNodes.length} 节点, ${allEdges.length} 关系`, 'finish');
            message.success(`已导入 ${allNodes.length} 个实体`);
        } catch (err) {
            addStepLog(4, `导入失败: ${err.message}`, 'error');
            message.error('数据导入失败');
        } finally {
            setImporting(false);
            setImportProgress(0);
        }
    };

    const toggleTable = (tableName) => {
        setSelectedTables(prev =>
            prev.includes(tableName) ? prev.filter(t => t !== tableName) : [...prev, tableName]
        );
    };

    const createGraph = (container, nodes, edges, config, isSchemaView = true) => {
        if (graphRef.current) {
            graphRef.current.destroy();
            graphRef.current = null;
        }

        nodesDataRef.current = nodes; // 保存节点数据
        edgesDataRef.current = edges; // 保存边数据

        const width = container.scrollWidth || 800;
        const height = 500;

        const graph = new Graph({
            container,
            width,
            height,
            data: { nodes, edges },
            animation: false,
            layout: {
                type: isSchemaView ? 'circular' : 'grid',  // 使用 grid 布局避免重叠
                ...(isSchemaView ? {
                    radius: Math.min(width, height) / 2.8,
                } : {
                    cols: Math.ceil(Math.sqrt(nodes.length)),
                    sortBy: 'data.type',  // 按类型分组
                    nodeSize: 60,
                })
            },
            behaviors: [
                'drag-canvas',
                { type: 'zoom-canvas', sensitivity: 0.4 },
                'drag-element',
                { type: 'click-select', multiple: false },
            ],
            node: {
                style: {
                    size: isSchemaView ? 55 : 28,
                    fill: (d) => config[isSchemaView ? d.id : d.data?.type]?.color || '#E3F2FD',
                    stroke: (d) => STROKE_COLORS[config[isSchemaView ? d.id : d.data?.type]?.color] || '#1976D2',
                    lineWidth: 2,
                    labelText: (d) => isSchemaView ? d.id : (d.data?.label?.substring(0, 10) || d.id),
                    labelPlacement: isSchemaView ? 'center' : 'bottom',
                    labelFontSize: isSchemaView ? 10 : 9,
                    labelFill: '#333',
                },
                state: {
                    selected: {
                        stroke: '#1890ff',
                        lineWidth: 3,
                        shadowColor: 'rgba(24,144,255,0.4)',
                        shadowBlur: 8,
                    }
                }
            },
            edge: {
                style: {
                    stroke: '#91caff',
                    lineWidth: 1.5,
                    endArrow: true,
                    labelText: (d) => d.data?.label || '',
                    labelFontSize: 9,
                    labelFill: '#666',
                    labelBackground: true,
                    labelBackgroundFill: '#fff',
                    labelBackgroundOpacity: 0.9,
                }
            }
        });

        graph.on('node:click', (evt) => {
            const nodeId = evt.target.id;
            const nodeData = nodesDataRef.current.find(n => n.id === nodeId);
            setSelectedNode(nodeData);
            setContextMenu({ visible: false, x: 0, y: 0, nodeType: null });
        });

        graph.on('node:contextmenu', (evt) => {
            evt.preventDefault();
            setContextMenu({
                visible: true,
                x: evt.clientX,
                y: evt.clientY,
                nodeType: isSchemaView ? evt.target.id : nodesDataRef.current.find(n => n.id === evt.target.id)?.data?.type
            });
        });

        graph.on('canvas:click', () => {
            setSelectedNode(null);
            setContextMenu({ visible: false, x: 0, y: 0, nodeType: null });
        });

        graph.render();
        graphRef.current = graph;
    };

    const renderSchemaGraph = (ontology, config) => {
        if (!graphContainerRef.current) return;

        const nodes = (ontology.object_types || []).map(obj => ({
            id: obj.name,
            data: { label: obj.name, properties: obj.properties, fullObj: obj }
        }));

        let edges = [];
        const nodeIds = new Set(nodes.map(n => n.id));

        console.log('=== Schema Graph Debug ===');
        console.log('Node IDs:', Array.from(nodeIds));
        console.log('link_types count:', ontology.link_types?.length || 0);

        if (ontology.link_types?.length > 0) {
            ontology.link_types.forEach((link, idx) => {
                const src = link.source_object_type;
                const tgt = link.target_object_type;
                console.log(`Link ${idx}: "${src}" -> "${tgt}" (name: ${link.name})`);
                console.log(`  - src in nodes: ${nodeIds.has(src)}, tgt in nodes: ${nodeIds.has(tgt)}`);

                if (src && tgt && nodeIds.has(src) && nodeIds.has(tgt)) {
                    edges.push({ id: `link-${idx}`, source: src, target: tgt, data: { label: link.name } });
                } else {
                    console.warn(`  - SKIPPED: source or target not found in nodes`);
                }
            });
        }

        if (ontology.inferred_links?.length > 0) {
            console.log('inferred_links count:', ontology.inferred_links.length);
            ontology.inferred_links.forEach(link => {
                if (nodeIds.has(link.source) && nodeIds.has(link.target)) {
                    edges.push(link);
                }
            });
        }

        console.log('Final edges:', edges.length, edges);
        createGraph(graphContainerRef.current, nodes, edges, config, true);
    };

    const renderDataGraph = (data, config) => {
        if (!graphContainerRef.current || !data) return;
        console.log('Data:', data.nodes.length, 'nodes,', data.edges.length, 'edges');
        createGraph(graphContainerRef.current, data.nodes, data.edges, config, false);
    };

    const handleColorChange = (_, hex) => {
        if (contextMenu.nodeType) {
            const newConfig = { ...visualConfig, [contextMenu.nodeType]: { color: hex } };
            setVisualConfig(newConfig);
        }
    };

    const handleClearResult = () => {
        Modal.confirm({
            title: '确认清除',
            content: '清除当前建模结果？',
            onOk: () => {
                setResult(null);
                setImportedData(null);
                setStepLogs([]);
                setViewMode('schema');
                setSelectedNode(null);
                localStorage.removeItem(STORAGE_KEY);
            }
        });
    };

    // 节点编辑功能
    const handleAddNode = () => {
        setNodeForm({ name: '', type: '', description: '' });
        setNodeEditModal({ visible: true, mode: 'add', node: null });
    };

    const handleEditNode = (node) => {
        setNodeForm({
            name: node.id || node.data?.label || '',
            type: node.data?.type || '',
            description: node.data?.fullObj?.description || node.data?.properties?.description || ''
        });
        setNodeEditModal({ visible: true, mode: 'edit', node });
    };

    const handleDeleteNode = (nodeId) => {
        Modal.confirm({
            title: '删除节点',
            content: `确定删除节点 "${nodeId}" 吗？相关的关系也会被删除。`,
            onOk: () => {
                // 从 ontology 中删除
                if (result?.ontology) {
                    const newOntology = {
                        ...result.ontology,
                        object_types: result.ontology.object_types.filter(o => o.name !== nodeId),
                        link_types: result.ontology.link_types?.filter(l =>
                            l.source_object_type !== nodeId && l.target_object_type !== nodeId
                        )
                    };
                    setResult({ ...result, ontology: newOntology });
                }
                setSelectedNode(null);
                message.success('节点已删除');
            }
        });
    };

    const handleSaveNode = () => {
        if (!nodeForm.name.trim()) {
            message.error('请输入节点名称');
            return;
        }

        if (result?.ontology) {
            const newOntology = { ...result.ontology };

            if (nodeEditModal.mode === 'add') {
                // 添加新节点
                const newNode = {
                    id: nodeForm.name,
                    name: nodeForm.name,
                    description: nodeForm.description || `实体 ${nodeForm.name}`,
                    source_table: 'manual',
                    primary_key: [],
                    properties: [],
                    creation_reason: '手动添加'
                };
                newOntology.object_types = [...(newOntology.object_types || []), newNode];
                message.success('节点已添加');
            } else {
                // 编辑现有节点
                newOntology.object_types = newOntology.object_types.map(o =>
                    o.name === nodeEditModal.node.id ? { ...o, name: nodeForm.name, description: nodeForm.description } : o
                );
                message.success('节点已更新');
            }

            setResult({ ...result, ontology: newOntology });
        }

        setNodeEditModal({ visible: false, mode: 'add', node: null });
    };

    // 关系编辑功能
    const handleAddEdge = () => {
        setEdgeForm({ source: '', target: '', name: '' });
        setEdgeEditModal({ visible: true, mode: 'add', edge: null });
    };

    const handleDeleteEdge = (edgeId) => {
        Modal.confirm({
            title: '删除关系',
            content: '确定删除此关系吗？',
            onOk: () => {
                if (result?.ontology) {
                    const newOntology = {
                        ...result.ontology,
                        link_types: result.ontology.link_types?.filter((_, idx) => `link-${idx}` !== edgeId)
                    };
                    setResult({ ...result, ontology: newOntology });
                }
                message.success('关系已删除');
            }
        });
    };

    const handleSaveEdge = () => {
        if (!edgeForm.source || !edgeForm.target || !edgeForm.name.trim()) {
            message.error('请填写完整的关系信息');
            return;
        }

        if (result?.ontology) {
            const newOntology = { ...result.ontology };
            const newEdge = {
                id: `${edgeForm.source}_to_${edgeForm.target}`,
                name: edgeForm.name,
                source_object_type: edgeForm.source,
                target_object_type: edgeForm.target,
                description: `${edgeForm.source} ${edgeForm.name} ${edgeForm.target}`,
                source_property: '',
                target_property: '',
                creation_reason: '手动添加'
            };
            newOntology.link_types = [...(newOntology.link_types || []), newEdge];
            setResult({ ...result, ontology: newOntology });
            message.success('关系已添加');
        }

        setEdgeEditModal({ visible: false, mode: 'add', edge: null });
    };

    // 右键菜单
    const renderContextMenu = () => {
        if (!contextMenu.visible) return null;
        return (
            <div style={{
                position: 'fixed',
                top: contextMenu.y,
                left: contextMenu.x,
                zIndex: 1000,
                background: '#fff',
                boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
                borderRadius: 6,
                padding: 12,
                width: 180
            }}>
                <div style={{ marginBottom: 8, fontWeight: 600, borderBottom: '1px solid #eee', paddingBottom: 6 }}>
                    {contextMenu.nodeType}
                </div>
                <div style={{ marginBottom: 8 }}>
                    <Text style={{ fontSize: 12, color: '#666' }}>节点颜色</Text>
                    <ColorPicker
                        value={visualConfig[contextMenu.nodeType]?.color}
                        onChange={handleColorChange}
                        presets={[{ label: '预设', colors: DEFAULT_COLORS }]}
                        size="small"
                    />
                </div>
                <Button size="small" block onClick={() => setContextMenu({ visible: false, x: 0, y: 0, nodeType: null })}>
                    关闭
                </Button>
            </div>
        );
    };

    // 节点详情浮动面板
    const renderNodeDetail = () => {
        if (!selectedNode) return null;

        const isSchema = viewMode === 'schema';
        const nodeType = isSchema ? selectedNode.id : selectedNode.data?.type;
        const nodeColor = visualConfig[nodeType]?.color || '#E3F2FD';

        return (
            <div style={{
                position: 'absolute',
                top: 10,
                right: 10,
                width: 280,
                maxHeight: 400,
                background: '#fff',
                borderRadius: 8,
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                zIndex: 100,
                overflow: 'hidden'
            }}>
                {/* 标题栏 */}
                <div style={{
                    background: nodeColor,
                    padding: '10px 12px',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <div style={{
                            width: 12,
                            height: 12,
                            borderRadius: '50%',
                            background: STROKE_COLORS[nodeColor] || '#1976D2'
                        }} />
                        <Text strong style={{ fontSize: 13 }}>
                            {isSchema ? selectedNode.id : selectedNode.data?.label}
                        </Text>
                    </div>
                    <Button
                        type="text"
                        size="small"
                        icon={<X size={14} />}
                        onClick={() => setSelectedNode(null)}
                    />
                </div>

                {/* 内容区 */}
                <div style={{ padding: 12, maxHeight: 340, overflowY: 'auto' }}>
                    {isSchema && selectedNode.data.fullObj?.source_table && (
                        <Tag color="blue" style={{ marginBottom: 10 }}>
                            来源: {selectedNode.data.fullObj.source_table}
                        </Tag>
                    )}
                    {!isSchema && selectedNode.data?.type && (
                        <Tag color="green" style={{ marginBottom: 10 }}>
                            类型: {selectedNode.data.type}
                        </Tag>
                    )}

                    <Divider style={{ margin: '8px 0' }}>属性</Divider>

                    {isSchema ? (
                        (selectedNode.data.fullObj?.properties || []).map(p => (
                            <div key={p.name} style={{
                                marginBottom: 6,
                                padding: '6px 8px',
                                background: '#f5f5f5',
                                borderRadius: 4,
                                fontSize: 12
                            }}>
                                <strong>{p.name}</strong>
                                <span style={{ marginLeft: 8, color: '#666' }}>{p.data_type}</span>
                                {p.is_primary_key && (
                                    <Tag color="gold" style={{ marginLeft: 4, fontSize: 10, padding: '0 4px' }}>PK</Tag>
                                )}
                            </div>
                        ))
                    ) : (
                        Object.entries(selectedNode.data?.properties || {}).slice(0, 12).map(([k, v]) => (
                            <div key={k} style={{ marginBottom: 4, fontSize: 12 }}>
                                <strong>{k}:</strong>{' '}
                                <span style={{ color: '#666' }}>{String(v).substring(0, 50)}</span>
                            </div>
                        ))
                    )}

                    {/* 编辑操作按钮 */}
                    {isSchema && (
                        <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
                            <Button size="small" onClick={() => handleEditNode(selectedNode)}>
                                编辑
                            </Button>
                            <Button size="small" danger onClick={() => handleDeleteNode(selectedNode.id)}>
                                删除
                            </Button>
                        </div>
                    )}
                </div>
            </div>
        );
    };

    return (
        <div style={{ maxWidth: 1400, margin: '0 auto', padding: '0 16px' }}
            onClick={() => contextMenu.visible && setContextMenu({ visible: false, x: 0, y: 0, nodeType: null })}>

            {/* 顶部工具栏 */}
            <div style={{
                marginBottom: 16,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: 12
            }}>
                <div>
                    <Title level={3} style={{ margin: 0 }}>本体建模工作台</Title>
                    <Text type="secondary">从数据库 Schema 生成语义本体和知识图谱</Text>
                </div>
                <Space size="middle" wrap>
                    <Space>
                        <Select
                            value={selectedConnection}
                            onChange={setSelectedConnection}
                            style={{ width: 180 }}
                            placeholder="选择数据源"
                            options={connections.map(c => ({
                                value: c.id,
                                label: <span><Database size={14} style={{ marginRight: 4 }} />{c.name || c.database}</span>
                            }))}
                        />
                        <Badge status={connectionStatus === 'connected' ? 'success' : 'warning'} />
                    </Space>
                    <Button
                        type="primary"
                        icon={<Play size={14} />}
                        loading={generating}
                        onClick={handleGenerate}
                        disabled={selectedTables.length === 0}
                    >
                        生成 Schema
                    </Button>
                    {result && (
                        <Button
                            icon={<Download size={14} />}
                            loading={importing}
                            onClick={handleImportData}
                            disabled={viewMode === 'data'}
                        >
                            {viewMode === 'data' ? '已导入' : '导入数据'}
                        </Button>
                    )}
                    {result && (
                        <Button danger size="small" onClick={handleClearResult}>清除</Button>
                    )}
                    <Button
                        icon={<Settings size={14} />}
                        onClick={handleOpenPromptsModal}
                        title="配置 LLM Prompt"
                    />
                </Space>
            </div>

            <Row gutter={16}>
                {/* 左侧配置面板 */}
                <Col xs={24} md={6} lg={5}>
                    <Card
                        title="建模配置"
                        size="small"
                        styles={{ body: { padding: 12 } }}
                    >
                        <div style={{ marginBottom: 8 }}>
                            <Text strong>数据表</Text>
                            <Button type="link" size="small" onClick={() => setSelectedTables(tables.map(t => t.name))}>
                                全选
                            </Button>
                        </div>
                        <div style={{ maxHeight: 180, overflowY: 'auto' }}>
                            {tables.length === 0 ? (
                                <Empty description="连接数据源" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                            ) : (
                                <Flex vertical gap={4}>
                                    {tables.map(t => (
                                        <Checkbox
                                            key={t.name}
                                            checked={selectedTables.includes(t.name)}
                                            onChange={() => toggleTable(t.name)}
                                        >
                                            {t.name}
                                        </Checkbox>
                                    ))}
                                </Flex>
                            )}
                        </div>

                        <Divider style={{ margin: '12px 0' }} />

                        <Text strong>辅助建模（源代码/日志分析）</Text>
                        <Upload.Dragger
                            multiple
                            showUploadList={{ showRemoveIcon: true }}
                            onChange={info => setUploadedFiles(info.fileList)}
                            style={{ marginTop: 8 }}
                        >
                            <p style={{ margin: 0 }}><UploadIcon size={16} /> 上传源代码或日志</p>
                        </Upload.Dragger>
                    </Card>

                    {/* 分析日志 */}
                    <Card title="分析日志" size="small" style={{ marginTop: 16 }} styles={{ body: { maxHeight: 400, overflowY: 'auto', padding: 10 } }}>
                        {stepLogs.length === 0 ? (
                            <Empty description="等待开始" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                        ) : (
                            <Timeline
                                items={stepLogs.map((log, idx) => ({
                                    color: log.status === 'finish' ? 'green' : log.status === 'error' ? 'red' : 'blue',
                                    children: (
                                        <div key={idx} style={{ fontSize: 12 }}>
                                            <Text type="secondary">{log.time}</Text> {log.content}
                                        </div>
                                    )
                                }))}
                            />
                        )}
                    </Card>
                </Col>

                {/* 右侧主区域 */}
                <Col xs={24} md={18} lg={19}>
                    {importing && (
                        <Card style={{ marginBottom: 16 }}>
                            <div style={{ textAlign: 'center', padding: 24 }}>
                                <Spin />
                                <Progress percent={importProgress} style={{ marginTop: 12, maxWidth: 400, margin: '12px auto' }} />
                                <Text style={{ display: 'block', marginTop: 8 }}>导入表数据...</Text>
                            </div>
                        </Card>
                    )}

                    {generating && !result && (
                        <Card>
                            <div style={{ padding: 32, textAlign: 'center' }}>
                                <Steps
                                    current={currentStep}
                                    size="small"
                                    items={[
                                        { title: '元数据分析' },
                                        { title: '关系推断' },
                                        { title: '本体构建' },
                                        { title: '图谱渲染' }
                                    ]}
                                />
                                <div style={{ marginTop: 24 }}>
                                    <Spin />
                                    <Text style={{ display: 'block', marginTop: 12 }}>正在构建本体...</Text>
                                </div>
                            </div>
                        </Card>
                    )}

                    {result && (
                        <>
                            <Card
                                title={
                                    <Space>
                                        <Button
                                            type="text"
                                            size="small"
                                            icon={graphPanelCollapsed ? <ChevronDown size={14} /> : <ChevronUp size={14} />}
                                            onClick={() => setGraphPanelCollapsed(!graphPanelCollapsed)}
                                        />
                                        {viewMode === 'schema' ? <Layers size={16} /> : <GitBranch size={16} />}
                                        <span>{viewMode === 'schema' ? 'Schema 视图' : '知识图谱'}</span>
                                        <Tag color={viewMode === 'schema' ? 'blue' : 'green'}>
                                            {viewMode === 'schema'
                                                ? `${result.ontology.object_types?.length || 0} 类型`
                                                : `${importedData?.nodes?.length || 0} 节点`}
                                        </Tag>
                                    </Space>
                                }
                                size="small"
                                extra={
                                    <Space>
                                        <Button size="small" type="primary" ghost onClick={handleAddNode}>
                                            + 节点
                                        </Button>
                                        <Button size="small" onClick={handleAddEdge}>
                                            + 关系
                                        </Button>
                                        {viewMode === 'data' && (
                                            <Button size="small" onClick={() => { setViewMode('schema'); setSelectedNode(null); }}>
                                                查看 Schema
                                            </Button>
                                        )}
                                        {viewMode === 'schema' && importedData && (
                                            <Button size="small" onClick={() => { setViewMode('data'); setSelectedNode(null); }}>
                                                查看图谱
                                            </Button>
                                        )}
                                        <Text type="secondary" style={{ fontSize: 12 }}>右键改色 / 双击编辑</Text>
                                    </Space>
                                }
                                styles={{ body: { padding: 0, position: 'relative', display: graphPanelCollapsed ? 'none' : 'block' } }}
                            >
                                <div ref={graphContainerRef} style={{ width: '100%', height: 500, background: '#fafafa' }} />
                                {renderNodeDetail()}
                            </Card>

                            <Card style={{ marginTop: 16 }} size="small">
                                <Tabs
                                    size="small"
                                    items={[
                                        {
                                            key: 'report',
                                            label: <span><FileText size={14} /> 分析报告</span>,
                                            children: (
                                                <div className="markdown-body" style={{ maxHeight: 800, overflowY: 'auto' }}>
                                                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{result.report}</ReactMarkdown>
                                                </div>
                                            )
                                        },
                                        {
                                            key: 'json',
                                            label: <span><Code size={14} /> JSON 定义</span>,
                                            children: (
                                                <pre style={{
                                                    maxHeight: 800,
                                                    overflow: 'auto',
                                                    background: '#f5f5f5',
                                                    padding: 12,
                                                    fontSize: 11,
                                                    borderRadius: 4
                                                }}>
                                                    {JSON.stringify(result.ontology, null, 2)}
                                                </pre>
                                            )
                                        }
                                    ]}
                                />
                            </Card>
                        </>
                    )}

                    {!result && !generating && (
                        <Card style={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                            <Empty description="选择数据表后点击「生成 Schema」开始建模" />
                        </Card>
                    )}
                </Col>
            </Row>

            {renderContextMenu()}

            {/* Prompt 配置模态框 */}
            <Modal
                title="LLM Prompt 配置"
                open={promptsModalVisible}
                onCancel={() => setPromptsModalVisible(false)}
                onOk={savePromptsConfig}
                confirmLoading={promptsSaving}
                width={800}
                okText="保存"
                cancelText="取消"
            >
                <div style={{ marginBottom: 16 }}>
                    <Text strong>表分析 Prompt</Text>
                    <Text type="secondary" style={{ display: 'block', marginBottom: 8, fontSize: 12 }}>
                        用于分析数据库表结构，推断业务实体含义。可用变量：{'{table_name}'}, {'{columns_info}'}, {'{sample_data}'}
                    </Text>
                    <Input.TextArea
                        value={promptsConfig.table_analysis_prompt}
                        onChange={(e) => setPromptsConfig({ ...promptsConfig, table_analysis_prompt: e.target.value })}
                        rows={10}
                        placeholder="输入表分析 Prompt 模板..."
                    />
                </div>

                <div>
                    <Text strong>关系分析 Prompt</Text>
                    <Text type="secondary" style={{ display: 'block', marginBottom: 8, fontSize: 12 }}>
                        用于分析表之间的关系含义。可用变量：{'{source_table}'}, {'{target_table}'}, {'{source_column}'}, {'{target_column}'}
                    </Text>
                    <Input.TextArea
                        value={promptsConfig.relationship_analysis_prompt}
                        onChange={(e) => setPromptsConfig({ ...promptsConfig, relationship_analysis_prompt: e.target.value })}
                        rows={8}
                        placeholder="输入关系分析 Prompt 模板..."
                    />
                </div>
            </Modal>

            {/* 节点编辑模态框 */}
            <Modal
                title={nodeEditModal.mode === 'add' ? "添加实体节点" : "编辑实体节点"}
                open={nodeEditModal.visible}
                onOk={handleSaveNode}
                onCancel={() => setNodeEditModal({ visible: false, mode: 'add', node: null })}
            >
                <div style={{ marginBottom: 16 }}>
                    <Text strong style={{ display: 'block', marginBottom: 8 }}>节点名称 (ID)</Text>
                    <Input
                        value={nodeForm.name}
                        onChange={e => setNodeForm({ ...nodeForm, name: e.target.value })}
                        placeholder="例如：User"
                        disabled={nodeEditModal.mode === 'edit'} // ID 通常不允许修改
                    />
                </div>
                <div style={{ marginBottom: 16 }}>
                    <Text strong style={{ display: 'block', marginBottom: 8 }}>描述</Text>
                    <Input.TextArea
                        value={nodeForm.description}
                        onChange={e => setNodeForm({ ...nodeForm, description: e.target.value })}
                        placeholder="节点业务描述"
                        rows={4}
                    />
                </div>
            </Modal>

            {/* 关系编辑模态框 */}
            <Modal
                title="添加/编辑关系"
                open={edgeEditModal.visible}
                onOk={handleSaveEdge}
                onCancel={() => setEdgeEditModal({ visible: false, mode: 'add', edge: null })}
            >
                <div style={{ marginBottom: 16 }}>
                    <Text strong style={{ display: 'block', marginBottom: 8 }}>关系名称</Text>
                    <Input
                        value={edgeForm.name}
                        onChange={e => setEdgeForm({ ...edgeForm, name: e.target.value })}
                        placeholder="例如：HAS_ORDER"
                    />
                </div>
                <div style={{ display: 'flex', gap: 16, marginBottom: 16 }}>
                    <div style={{ flex: 1 }}>
                        <Text strong style={{ display: 'block', marginBottom: 8 }}>源节点</Text>
                        <Select
                            style={{ width: '100%' }}
                            value={edgeForm.source}
                            onChange={v => setEdgeForm({ ...edgeForm, source: v })}
                            options={result?.ontology?.object_types?.map(obj => ({ label: obj.name, value: obj.name })) || []}
                            placeholder="选择源节点"
                        />
                    </div>
                    <div style={{ flex: 1 }}>
                        <Text strong style={{ display: 'block', marginBottom: 8 }}>目标节点</Text>
                        <Select
                            style={{ width: '100%' }}
                            value={edgeForm.target}
                            onChange={v => setEdgeForm({ ...edgeForm, target: v })}
                            options={result?.ontology?.object_types?.map(obj => ({ label: obj.name, value: obj.name })) || []}
                            placeholder="选择目标节点"
                        />
                    </div>
                </div>
            </Modal>
        </div>
    );
};

export default OntologyPage;
