import React, { useState, useEffect } from 'react';
import { Card, Form, Input, Button, Typography, Alert, message, Tabs, Select, Upload, Table, Tag, Space, Popconfirm, Modal } from 'antd';
import { Database, ArrowRight, CheckCircle2, Upload as UploadIcon, Trash2, RefreshCw, Plus, Server } from 'lucide-react';
import { dbApi } from '../services/api';
import { useNavigate } from 'react-router-dom';

const { Title, Text, Paragraph } = Typography;
const { Dragger } = Upload;

const DashboardPage = () => {
    const [loading, setLoading] = useState(false);
    const [connections, setConnections] = useState([]);
    const [activeTab, setActiveTab] = useState('list');
    const [dbType, setDbType] = useState('postgresql');
    const [error, setError] = useState(null);
    const navigate = useNavigate();
    const [form] = Form.useForm();

    // 加载已保存的连接列表
    useEffect(() => {
        loadConnections();
    }, []);

    const loadConnections = async () => {
        try {
            const res = await dbApi.getConnections();
            setConnections(res.data || []);
        } catch (err) {
            // 如果 API 不存在，使用空列表
            setConnections([]);
        }
    };

    const onFinish = async (values) => {
        setLoading(true);
        setError(null);
        try {
            const payload = { ...values, db_type: dbType };
            const res = await dbApi.connect(payload);
            message.success('数据库连接成功！');
            window.dispatchEvent(new Event('db-connected'));
            loadConnections();
            setActiveTab('list');
        } catch (err) {
            setError(err.response?.data?.detail || '连接数据库失败');
        } finally {
            setLoading(false);
        }
    };

    const handleDisconnect = async (connectionId) => {
        try {
            await dbApi.disconnect(connectionId);
            message.success('已断开连接');
            loadConnections();
        } catch (err) {
            message.error('断开连接失败');
        }
    };

    const handleReconnect = async (connectionId) => {
        try {
            await dbApi.reconnect(connectionId);
            message.success('重新连接成功');
            loadConnections();
        } catch (err) {
            message.error('重新连接失败');
        }
    };

    const handleDelete = async (connectionId) => {
        try {
            await dbApi.deleteConnection(connectionId);
            message.success('已删除连接');
            loadConnections();
        } catch (err) {
            message.error('删除连接失败');
        }
    };

    const handleCsvUpload = async (file) => {
        const formData = new FormData();
        formData.append('file', file);
        try {
            await dbApi.uploadCsv(formData);
            message.success('CSV 文件上传成功');
            loadConnections();
        } catch (err) {
            message.error('CSV 上传失败');
        }
        return false; // 阻止默认上传行为
    };

    const connectionColumns = [
        {
            title: '名称',
            dataIndex: 'name',
            key: 'name',
            render: (text, record) => (
                <Space>
                    <Database size={16} />
                    <span>{text || record.database}</span>
                </Space>
            )
        },
        {
            title: '类型',
            dataIndex: 'db_type',
            key: 'db_type',
            render: (type) => {
                const colors = {
                    postgresql: 'blue',
                    mysql: 'orange',
                    sqlserver: 'purple',
                    csv: 'green'
                };
                return <Tag color={colors[type] || 'default'}>{type?.toUpperCase()}</Tag>;
            }
        },
        {
            title: '主机',
            dataIndex: 'host',
            key: 'host',
            render: (text, record) => record.db_type === 'csv' ? '-' : `${text}:${record.port}`
        },
        {
            title: '状态',
            dataIndex: 'status',
            key: 'status',
            render: (status) => (
                <Tag color={status === 'connected' ? 'success' : 'error'}>
                    {status === 'connected' ? '已连接' : '已断开'}
                </Tag>
            )
        },
        {
            title: '操作',
            key: 'actions',
            render: (_, record) => (
                <Space>
                    {record.status === 'connected' ? (
                        <Button size="small" onClick={() => handleDisconnect(record.id)}>
                            断开
                        </Button>
                    ) : (
                        <Button size="small" type="primary" onClick={() => handleReconnect(record.id)}>
                            重连
                        </Button>
                    )}
                    <Popconfirm title="确定删除此连接？" onConfirm={() => handleDelete(record.id)}>
                        <Button size="small" danger icon={<Trash2 size={14} />} />
                    </Popconfirm>
                </Space>
            )
        }
    ];

    const getFormFields = () => {
        const commonFields = (
            <>
                <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16 }}>
                    <Form.Item name="host" label="主机地址" rules={[{ required: true, message: '请输入主机地址' }]}>
                        <Input placeholder="localhost" />
                    </Form.Item>
                    <Form.Item name="port" label="端口" rules={[{ required: true, message: '请输入端口' }]}>
                        <Input type="number" placeholder={dbType === 'mysql' ? '3306' : dbType === 'sqlserver' ? '1433' : '5432'} />
                    </Form.Item>
                </div>
                <Form.Item name="database" label="数据库名称" rules={[{ required: true, message: '请输入数据库名称' }]}>
                    <Input placeholder="my_database" />
                </Form.Item>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                    <Form.Item name="user" label="用户名" rules={[{ required: true, message: '请输入用户名' }]}>
                        <Input placeholder="用户名" />
                    </Form.Item>
                    <Form.Item name="password" label="密码" rules={[{ required: true, message: '请输入密码' }]}>
                        <Input.Password placeholder="******" />
                    </Form.Item>
                </div>
            </>
        );

        if (dbType === 'postgresql') {
            return (
                <>
                    {commonFields}
                    <Form.Item name="schema_name" label="Schema">
                        <Input placeholder="public" />
                    </Form.Item>
                </>
            );
        }
        return commonFields;
    };

    return (
        <div style={{ maxWidth: 1200, margin: '0 auto' }}>
            <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <Title level={2} style={{ margin: 0 }}>数据接入管理</Title>
                    <Text type="secondary">管理数据库连接和数据源</Text>
                </div>
                <Button type="primary" icon={<Plus size={16} />} onClick={() => setActiveTab('add')}>
                    新建连接
                </Button>
            </div>

            <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={[
                    {
                        key: 'list',
                        label: '连接列表',
                        children: (
                            <Card>
                                {connections.length === 0 ? (
                                    <div style={{ textAlign: 'center', padding: 40 }}>
                                        <Server size={48} style={{ color: 'var(--color-text-tertiary)', marginBottom: 16 }} />
                                        <Title level={4} style={{ color: 'var(--color-text-secondary)' }}>暂无数据连接</Title>
                                        <Text type="secondary">点击"新建连接"添加数据库或上传 CSV 文件</Text>
                                    </div>
                                ) : (
                                    <Table
                                        dataSource={connections}
                                        columns={connectionColumns}
                                        rowKey="id"
                                        pagination={false}
                                    />
                                )}
                            </Card>
                        )
                    },
                    {
                        key: 'add',
                        label: '新建连接',
                        children: (
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
                                <Card
                                    title={
                                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                            <Database size={20} className="text-primary" />
                                            <span>数据库连接</span>
                                        </div>
                                    }
                                >
                                    {error && (
                                        <Alert
                                            title="连接失败"
                                            description={error}
                                            type="error"
                                            showIcon
                                            style={{ marginBottom: 24 }}
                                        />
                                    )}

                                    <Form.Item label="数据库类型" style={{ marginBottom: 16 }}>
                                        <Select
                                            value={dbType}
                                            onChange={(val) => {
                                                setDbType(val);
                                                form.resetFields();
                                            }}
                                            options={[
                                                { value: 'postgresql', label: 'PostgreSQL' },
                                                { value: 'mysql', label: 'MySQL' },
                                                { value: 'sqlserver', label: 'SQL Server' },
                                            ]}
                                        />
                                    </Form.Item>

                                    <Form
                                        form={form}
                                        layout="vertical"
                                        onFinish={onFinish}
                                        initialValues={{
                                            host: 'localhost',
                                            port: dbType === 'mysql' ? 3306 : dbType === 'sqlserver' ? 1433 : 5432,
                                            schema_name: 'public'
                                        }}
                                    >
                                        {getFormFields()}

                                        <Form.Item style={{ marginBottom: 0, marginTop: 16 }}>
                                            <Button type="primary" htmlType="submit" loading={loading} block size="large" icon={<ArrowRight size={18} />}>
                                                连接数据库
                                            </Button>
                                        </Form.Item>
                                    </Form>
                                </Card>

                                <Card
                                    title={
                                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                            <UploadIcon size={20} className="text-primary" />
                                            <span>上传 CSV 文件</span>
                                        </div>
                                    }
                                >
                                    <Dragger
                                        accept=".csv"
                                        beforeUpload={handleCsvUpload}
                                        showUploadList={false}
                                        style={{ padding: 40 }}
                                    >
                                        <p style={{ marginBottom: 16 }}>
                                            <UploadIcon size={48} style={{ color: 'var(--color-primary)' }} />
                                        </p>
                                        <p style={{ fontSize: 16 }}>点击或拖拽 CSV 文件到此区域</p>
                                        <p style={{ color: 'var(--color-text-secondary)' }}>
                                            支持单个或多个 CSV 文件上传，系统将自动解析列类型
                                        </p>
                                    </Dragger>

                                    <Card style={{ marginTop: 24, background: 'var(--color-primary-light)', border: 'none' }}>
                                        <div style={{ display: 'flex', gap: 12 }}>
                                            <CheckCircle2 color="var(--color-primary)" size={24} />
                                            <div>
                                                <Text strong>安全且本地化</Text>
                                                <Paragraph style={{ margin: 0, marginTop: 4, opacity: 0.8 }}>
                                                    您的数据库凭证和数据仅在本地处理，不会发送到外部服务器。
                                                </Paragraph>
                                            </div>
                                        </div>
                                    </Card>
                                </Card>
                            </div>
                        )
                    }
                ]}
            />
        </div>
    );
};

export default DashboardPage;
