import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import MainLayout from './layouts/MainLayout';
import DashboardPage from './pages/DashboardPage';
import ExplorerPage from './pages/ExplorerPage';
import OntologyPage from './pages/OntologyPage';
import GraphPage from './pages/GraphPage';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="explorer" element={<ExplorerPage />} />
          <Route path="ontology" element={<OntologyPage />} />
          <Route path="graph" element={<GraphPage />} />
        </Route>
      </Routes>
    </Router>
  );
}

export default App;
