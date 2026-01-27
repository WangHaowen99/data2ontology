"""Persistence storage layer for database connections."""

import json
import sqlite3
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
from cryptography.fernet import Fernet
import base64
import hashlib


class ConnectionStorage:
    """SQLite-based storage for database connections."""
    
    def __init__(self, db_path: str = None):
        """Initialize storage with optional custom path."""
        if db_path is None:
            # Default to user's home directory
            home = Path.home()
            app_dir = home / ".data2ontology"
            app_dir.mkdir(exist_ok=True)
            db_path = str(app_dir / "connections.db")
        
        self.db_path = db_path
        self._init_db()
        self._init_encryption()
    
    def _init_db(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY,
                name TEXT,
                db_type TEXT NOT NULL,
                host TEXT,
                port INTEGER,
                database TEXT,
                username TEXT,
                password_encrypted TEXT,
                schema_name TEXT,
                file_path TEXT,
                status TEXT DEFAULT 'disconnected',
                created_at TEXT,
                updated_at TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS neo4j_connections (
                id TEXT PRIMARY KEY,
                uri TEXT NOT NULL,
                username TEXT,
                password_encrypted TEXT,
                status TEXT DEFAULT 'disconnected',
                created_at TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _init_encryption(self):
        """Initialize encryption key."""
        key_path = Path(self.db_path).parent / ".key"
        if key_path.exists():
            with open(key_path, "rb") as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(key_path, "wb") as f:
                f.write(self.key)
        self.cipher = Fernet(self.key)
    
    def _encrypt(self, text: str) -> str:
        """Encrypt sensitive data."""
        if not text:
            return ""
        return self.cipher.encrypt(text.encode()).decode()
    
    def _decrypt(self, encrypted: str) -> str:
        """Decrypt sensitive data."""
        if not encrypted:
            return ""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    def save_connection(self, connection: Dict[str, Any]) -> str:
        """Save a new connection or update existing."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        connection_id = connection.get("id") or f"conn_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        now = datetime.now().isoformat()
        
        password_encrypted = self._encrypt(connection.get("password", ""))
        
        cursor.execute("""
            INSERT OR REPLACE INTO connections 
            (id, name, db_type, host, port, database, username, password_encrypted, schema_name, file_path, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            connection_id,
            connection.get("name", connection.get("database", "Unnamed")),
            connection.get("db_type", "postgresql"),
            connection.get("host"),
            connection.get("port"),
            connection.get("database"),
            connection.get("user") or connection.get("username"),
            password_encrypted,
            connection.get("schema_name"),
            connection.get("file_path"),
            connection.get("status", "disconnected"),
            connection.get("created_at", now),
            now
        ))
        
        conn.commit()
        conn.close()
        
        return connection_id
    
    def get_all_connections(self) -> List[Dict[str, Any]]:
        """Get all saved connections (without decrypted passwords)."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM connections ORDER BY updated_at DESC")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific connection with decrypted password."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM connections WHERE id = ?", (connection_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            result = dict(row)
            result["password"] = self._decrypt(result.pop("password_encrypted", ""))
            # 将 username 映射回 user 以兼容适配器
            if "username" in result:
                result["user"] = result.pop("username")
            return result
        return None
    
    def update_status(self, connection_id: str, status: str):
        """Update connection status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE connections SET status = ?, updated_at = ? WHERE id = ?",
            (status, datetime.now().isoformat(), connection_id)
        )
        
        conn.commit()
        conn.close()
    
    def delete_connection(self, connection_id: str):
        """Delete a connection."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM connections WHERE id = ?", (connection_id,))
        
        conn.commit()
        conn.close()
    
    # Neo4j connection methods
    def save_neo4j_connection(self, connection: Dict[str, Any]) -> str:
        """Save Neo4j connection."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        connection_id = "neo4j_default"
        now = datetime.now().isoformat()
        
        password_encrypted = self._encrypt(connection.get("password", ""))
        
        cursor.execute("""
            INSERT OR REPLACE INTO neo4j_connections 
            (id, uri, username, password_encrypted, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            connection_id,
            connection.get("uri"),
            connection.get("user") or connection.get("username"),
            password_encrypted,
            connection.get("status", "disconnected"),
            now
        ))
        
        conn.commit()
        conn.close()
        
        return connection_id
    
    def get_neo4j_connection(self) -> Optional[Dict[str, Any]]:
        """Get Neo4j connection with decrypted password."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM neo4j_connections WHERE id = 'neo4j_default'")
        row = cursor.fetchone()
        conn.close()
        
        if row:
            result = dict(row)
            result["password"] = self._decrypt(result.pop("password_encrypted", ""))
            return result
        return None


# Singleton instance
_storage_instance = None

def get_storage() -> ConnectionStorage:
    """Get singleton storage instance."""
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = ConnectionStorage()
    return _storage_instance
