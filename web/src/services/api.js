import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

const api = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export const dbApi = {
    // 连接管理
    connect: (config) => api.post('/db/connect', config),
    disconnect: (connectionId) => api.post(`/db/disconnect/${connectionId}`),
    reconnect: (connectionId) => api.post(`/db/reconnect/${connectionId}`),
    deleteConnection: (connectionId) => api.delete(`/db/connection/${connectionId}`),
    getConnections: () => api.get('/db/connections'),

    // 元数据和查询
    getMetadata: (connectionId) => api.get('/db/metadata', { params: { connection_id: connectionId } }),
    getTableSample: (tableName, connectionId) => api.get(`/db/tables/${tableName}/sample`, { params: { connection_id: connectionId } }),
    executeSql: (sql, connectionId) => api.post('/query/sql', { sql, connection_id: connectionId }),
    executeNatural: (query, connectionId) => api.post('/query/natural', { query, connection_id: connectionId }),

    // CSV 上传
    uploadCsv: (formData) => api.post('/db/upload-csv', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
    }),
};

export const ontologyApi = {
    generate: (tableNames, connectionId) => api.post('/ontology/generate', { table_names: tableNames, connection_id: connectionId }),
    getOntologies: () => api.get('/ontology/list'),
};

export const configApi = {
    getPrompts: () => api.get('/config/prompts'),
    savePrompts: (config) => api.post('/config/prompts', config),
};

export const neo4jApi = {
    // 连接管理
    connect: (config) => api.post('/neo4j/connect', config),
    disconnect: () => api.post('/neo4j/disconnect'),
    getStatus: () => api.get('/neo4j/status'),

    // 数据操作
    export: (config, ontologyId) => api.post('/neo4j/export', { ...config, ontology_id: ontologyId }),
    getGraph: (config) => api.get('/neo4j/graph', { params: config }),

    // CRUD 操作 - 包含 Neo4j 连接参数
    createNode: (node, neo4jConfig) => api.post('/neo4j/nodes', node, { params: neo4jConfig }),
    updateNode: (nodeId, data, neo4jConfig) => api.put(`/neo4j/nodes/${nodeId}`, data, { params: neo4jConfig }),
    deleteNode: (nodeId, neo4jConfig) => api.delete(`/neo4j/nodes/${nodeId}`, { params: neo4jConfig }),

    createRelationship: (rel, neo4jConfig) => api.post('/neo4j/relationships', rel, { params: neo4jConfig }),
    updateRelationship: (relId, data, neo4jConfig) => api.put(`/neo4j/relationships/${relId}`, data, { params: neo4jConfig }),
    deleteRelationship: (relId, neo4jConfig) => api.delete(`/neo4j/relationships/${relId}`, { params: neo4jConfig }),

    // 从 Ontology 导入
    importFromOntology: (ontologyId, neo4jConfig) => api.post('/neo4j/import-ontology', { ontology_id: ontologyId }, { params: neo4jConfig }),
};

export default api;
