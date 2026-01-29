import React, { useState, useEffect } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { Layout, Menu, Typography, message } from 'antd';
import {
    Database,
    Search,
    Binary,
    Share2,
    LayoutDashboard
} from 'lucide-react';
import { dbApi } from '../services/api';

const { Sider, Content } = Layout;
const { Title } = Typography;

const MainLayout = () => {
    const navigate = useNavigate();
    const location = useLocation();
    const [dbStatus, setDbStatus] = useState('disconnected'); // disconnected, connected

    const menuItems = [
        {
            key: '/dashboard',
            icon: <LayoutDashboard size={18} />,
            label: '数据接入',
        },
        {
            key: '/explorer',
            icon: <Search size={18} />,
            label: '数据浏览',
            disabled: dbStatus === 'disconnected',
        },
        {
            key: '/ontology',
            icon: <Binary size={18} />,
            label: '本体建模',
            disabled: dbStatus === 'disconnected',
        },
        {
            key: '/graph',
            icon: <Share2 size={18} />,
            label: '知识图谱',
            disabled: dbStatus === 'disconnected',
        },
    ];

    // Global check for connection status (simple implementation)
    useEffect(() => {
        const checkConnection = async () => {
            try {
                await dbApi.getMetadata();
                setDbStatus('connected');
            } catch (e) {
                setDbStatus('disconnected');
                if (location.pathname !== '/dashboard') {
                    // Allow staying on current page but status shows disconnected
                    // Or strictly redirect: navigate('/dashboard');
                }
            }
        };
        // Check on mount and maybe periodically
        checkConnection();

        // Listen for custom event for connection success
        const handleConnect = () => setDbStatus('connected');
        window.addEventListener('db-connected', handleConnect);

        return () => window.removeEventListener('db-connected', handleConnect);
    }, [location.pathname]);

    return (
        <Layout style={{ minHeight: '100vh' }}>
            <Sider
                width={240}
                theme="light"
                style={{
                    position: 'fixed',
                    height: '100vh',
                    left: 0,
                    borderRight: '1px solid var(--color-border)',
                    zIndex: 10
                }}
            >
                <div style={{ padding: '24px 24px 8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <div style={{
                        width: 32,
                        height: 32,
                        background: 'var(--color-primary)',
                        borderRadius: 8,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        color: 'white'
                    }}>
                        <Database size={20} />
                    </div>
                    <div>
                        <Title level={5} style={{ margin: 0, fontSize: '1rem', fontWeight: 600 }}>Data2Ontology</Title>
                        <div style={{ fontSize: '0.75rem', color: 'var(--color-text-secondary)', display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{
                                width: 6,
                                height: 6,
                                borderRadius: '50%',
                                background: dbStatus === 'connected' ? 'var(--color-success)' : 'var(--color-text-tertiary)'
                            }} />
                            {dbStatus === 'connected' ? '已连接' : '未连接'}
                        </div>
                    </div>
                </div>

                <Menu
                    mode="inline"
                    selectedKeys={[location.pathname]}
                    style={{ borderRight: 0, marginTop: 16 }}
                    items={menuItems}
                    onClick={({ key }) => navigate(key)}
                />

                <div style={{ position: 'absolute', bottom: 24, left: 24, width: 'calc(100% - 48px)' }}>
                    <div style={{
                        padding: 12,
                        background: 'var(--color-surface)',
                        borderRadius: 8,
                        fontSize: '0.75rem',
                        color: 'var(--color-text-secondary)'
                    }}>
                        v1.1.6
                    </div>
                </div>
            </Sider>

            <Layout style={{ marginLeft: 240, background: 'var(--color-bg)' }}>
                <Content style={{ padding: '24px', maxWidth: 1400, margin: '0 auto', width: '100%' }}>
                    <Outlet />
                </Content>
            </Layout>
        </Layout>
    );
};

export default MainLayout;
