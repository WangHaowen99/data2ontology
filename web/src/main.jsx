import React from 'react';
import ReactDOM from 'react-dom/client';
import { ConfigProvider } from 'antd';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ConfigProvider
      theme={{
        token: {
          fontFamily: 'Inter, sans-serif',
          colorPrimary: '#228be6',
          borderRadius: 8,
          colorBgContainer: '#ffffff',
          colorBorder: '#e9ecef',
        },
      }}
    >
      <App />
    </ConfigProvider>
  </React.StrictMode>,
);
