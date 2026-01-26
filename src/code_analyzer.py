"""Code analyzer module for extracting business insights from source code."""

import re
from pathlib import Path
from typing import Optional
from collections import defaultdict

from .models.metadata import (
    CodeInsight,
    CodeEntity,
    ApiEndpoint,
)


class CodeAnalyzer:
    """Analyzes source code to extract entity definitions and API endpoints."""

    def __init__(self, languages: list[str] = None, exclude_patterns: list[str] = None):
        """Initialize the code analyzer.
        
        Args:
            languages: List of languages to analyze (default: python, java, javascript, typescript)
            exclude_patterns: Glob patterns to exclude
        """
        self.languages = languages or ["python", "java", "javascript", "typescript"]
        self.exclude_patterns = exclude_patterns or ["*/node_modules/*", "*/venv/*", "*/.git/*", "*/build/*", "*/dist/*"]
        
        # Python-specific patterns
        self.python_class_pattern = re.compile(r'^class\s+(\w+)(?:\(([^)]*)\))?:', re.MULTILINE)
        self.python_field_pattern = re.compile(r'^\s+(\w+)\s*[:=]\s*(.+?)(?:\s*#.*)?$', re.MULTILINE)
        self.python_method_pattern = re.compile(r'^\s+def\s+(\w+)\s*\(', re.MULTILINE)
        self.python_decorator_pattern = re.compile(r'@(\w+(?:\.\w+)*)')
        
        # Java-specific patterns
        self.java_class_pattern = re.compile(r'(?:public\s+)?class\s+(\w+)(?:\s+extends\s+(\w+))?')
        self.java_field_pattern = re.compile(r'(?:private|protected|public)\s+(\w+(?:<[\w,\s]+>)?)\s+(\w+)\s*;')
        
        # JavaScript/TypeScript patterns
        self.js_class_pattern = re.compile(r'class\s+(\w+)(?:\s+extends\s+(\w+))?')
        self.js_interface_pattern = re.compile(r'interface\s+(\w+)')
        
        # API endpoint patterns
        self.api_patterns = {
            "flask": re.compile(r'@app\.route\(["\']([^"\']+)["\'](?:,\s*methods\s*=\s*\[([^\]]+)\])?'),
            "fastapi": re.compile(r'@app\.(?:get|post|put|delete|patch)\(["\']([^"\']+)["\']'),
            "django": re.compile(r'path\(["\']([^"\']+)["\'],\s*(\w+)'),
            "spring": re.compile(r'@(?:Get|Post|Put|Delete|Request)Mapping\(["\']([^"\']+)["\']'),
            "express": re.compile(r'app\.(?:get|post|put|delete|patch)\(["\']([^"\']+)["\']'),
        }

    def analyze_code(self, code_paths: list[str]) -> CodeInsight:
        """Analyze source code files and extract insights.
        
        Args:
            code_paths: List of code directory or file paths to analyze
            
        Returns:
            CodeInsight object containing extracted information
        """
        entities = []
        api_endpoints = []
        entity_relationships = defaultdict(set)
        files_analyzed = []
        
        for code_path in code_paths:
            path = Path(code_path)
            
            if path.is_file():
                # Analyze single file
                file_entities, file_apis, file_rels = self._analyze_file(path)
                entities.extend(file_entities)
                api_endpoints.extend(file_apis)
                for entity, related in file_rels.items():
                    entity_relationships[entity].update(related)
                if file_entities or file_apis:
                    files_analyzed.append(str(path))
            elif path.is_dir():
                # Recursively analyze directory
                for file_path in self._get_code_files(path):
                    file_entities, file_apis, file_rels = self._analyze_file(file_path)
                    entities.extend(file_entities)
                    api_endpoints.extend(file_apis)
                    for entity, related in file_rels.items():
                        entity_relationships[entity].update(related)
                    if file_entities or file_apis:
                        files_analyzed.append(str(file_path))
        
        # Convert relationship sets to lists
        relationships_dict = {k: list(v) for k, v in entity_relationships.items()}
        
        return CodeInsight(
            entities=entities,
            api_endpoints=api_endpoints,
            entity_relationships=relationships_dict,
            total_files_analyzed=len(files_analyzed),
            code_files_analyzed=files_analyzed
        )

    def _get_code_files(self, directory: Path) -> list[Path]:
        """Get all code files in a directory recursively.
        
        Args:
            directory: Directory to search
            
        Returns:
            List of code file paths
        """
        code_files = []
        extensions = {
            "python": [".py"],
            "java": [".java"],
            "javascript": [".js", ".jsx"],
            "typescript": [".ts", ".tsx"]
        }
        
        valid_extensions = []
        for lang in self.languages:
            valid_extensions.extend(extensions.get(lang, []))
        
        for file_path in directory.rglob("*"):
            if file_path.is_file() and file_path.suffix in valid_extensions:
                # Check exclude patterns
                if not self._is_excluded(file_path):
                    code_files.append(file_path)
        
        return code_files

    def _is_excluded(self, file_path: Path) -> bool:
        """Check if a file should be excluded.
        
        Args:
            file_path: File path to check
            
        Returns:
            True if file should be excluded
        """
        path_str = str(file_path)
        for pattern in self.exclude_patterns:
            # Simple pattern matching (not full glob)
            if pattern.replace("*/", "").replace("/*", "") in path_str:
                return True
        return False

    def _analyze_file(self, file_path: Path) -> tuple[list[CodeEntity], list[ApiEndpoint], dict]:
        """Analyze a single code file.
        
        Args:
            file_path: Path to the code file
            
        Returns:
            Tuple of (entities, api_endpoints, relationships)
        """
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception:
            return [], [], {}
        
        suffix = file_path.suffix
        
        if suffix == ".py":
            return self._analyze_python(content, file_path)
        elif suffix == ".java":
            return self._analyze_java(content, file_path)
        elif suffix in [".js", ".jsx", ".ts", ".tsx"]:
            return self._analyze_javascript(content, file_path)
        
        return [], [], {}

    def _analyze_python(
        self, 
        content: str, 
        file_path: Path
    ) -> tuple[list[CodeEntity], list[ApiEndpoint], dict]:
        """Analyze Python source code.
        
        Args:
            content: File content
            file_path: Path to the file
            
        Returns:
            Tuple of (entities, api_endpoints, relationships)
        """
        entities = []
        api_endpoints = []
        relationships = defaultdict(set)
        
        # Find all classes
        for match in self.python_class_pattern.finditer(content):
            class_name = match.group(1)
            base_classes = match.group(2) if match.group(2) else ""
            
            # Get line number
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract class body (simplified - get next 100 lines or until next class)
            class_start = match.end()
            class_end = content.find('\nclass ', class_start)
            if class_end == -1:
                class_end = len(content)
            class_body = content[class_start:class_end]
            
            # Extract fields
            fields = []
            for field_match in self.python_field_pattern.finditer(class_body):
                field_name = field_match.group(1)
                field_type = field_match.group(2).strip()
                fields.append({"name": field_name, "type": field_type})
                
                # Detect relationships to other entities
                # Look for patterns like: user = models.ForeignKey(User, ...)
                if any(keyword in field_type for keyword in ["ForeignKey", "OneToOne", "ManyToMany"]):
                    related_match = re.search(r'(?:ForeignKey|OneToOne|ManyToMany)\s*\(\s*["\']?(\w+)', field_type)
                    if related_match:
                        relationships[class_name].add(related_match.group(1))
            
            # Extract methods
            methods = [m.group(1) for m in self.python_method_pattern.finditer(class_body)]
            
            # Extract docstring
            docstring_match = re.search(r'^\s*["\'"]{3}(.*?)["\'"]{3}', class_body, re.DOTALL)
            description = docstring_match.group(1).strip() if docstring_match else None
            
            # Determine entity type
            entity_type = "class"
            if "Model" in base_classes or "BaseModel" in base_classes:
                entity_type = "model"
            elif "Schema" in base_classes or any(d in content[:match.start()] for d in ["@dataclass", "@pydantic"]):
                entity_type = "dto"
            
            # Track base class relationships
            if base_classes:
                for base in base_classes.split(','):
                    base = base.strip()
                    if base and base not in ["object", "Model", "BaseModel", "Schema"]:
                        relationships[class_name].add(base)
            
            entity = CodeEntity(
                name=class_name,
                entity_type=entity_type,
                file_path=str(file_path),
                line_number=line_number,
                fields=fields,
                methods=methods,
                relationships=list(relationships[class_name]),
                description=description
            )
            entities.append(entity)
        
        # Find API endpoints
        lines = content.split('\n')
        for i, line in enumerate(lines):
            # Flask/FastAPI route detection
            for framework, pattern in self.api_patterns.items():
                if framework in ["flask", "fastapi", "django"]:
                    match = pattern.search(line)
                    if match:
                        path = match.group(1)
                        method = "GET"  # Default
                        
                        if framework == "flask" and match.group(2):
                            method = match.group(2).strip('"\'')
                        elif framework == "fastapi":
                            # Method is in decorator name
                            method_match = re.search(r'@app\.(\w+)\(', line)
                            if method_match:
                                method = method_match.group(1).upper()
                        
                        # Find handler function (next non-decorator line with def)
                        handler = ""
                        for j in range(i + 1, min(i + 5, len(lines))):
                            if lines[j].strip().startswith('def '):
                                handler_match = re.search(r'def\s+(\w+)', lines[j])
                                if handler_match:
                                    handler = handler_match.group(1)
                                break
                        
                        # Try to detect entities referenced in handler
                        entities_referenced = []
                        # Look ahead a few lines for entity references
                        handler_body = '\n'.join(lines[i:min(i + 20, len(lines))])
                        for entity in entities:
                            if entity.name in handler_body:
                                entities_referenced.append(entity.name)
                        
                        endpoint = ApiEndpoint(
                            path=path,
                            method=method,
                            handler=handler,
                            entities_referenced=entities_referenced,
                            file_path=str(file_path),
                            line_number=i + 1
                        )
                        api_endpoints.append(endpoint)
        
        return entities, api_endpoints, dict(relationships)

    def _analyze_java(
        self, 
        content: str, 
        file_path: Path
    ) -> tuple[list[CodeEntity], list[ApiEndpoint], dict]:
        """Analyze Java source code.
        
        Args:
            content: File content
            file_path: Path to the file
            
        Returns:
            Tuple of (entities, api_endpoints, relationships)
        """
        entities = []
        api_endpoints = []
        relationships = defaultdict(set)
        
        # Find all classes
        for match in self.java_class_pattern.finditer(content):
            class_name = match.group(1)
            base_class = match.group(2)
            
            line_number = content[:match.start()].count('\n') + 1
            
            # Extract class body
            class_start = match.end()
            # Find matching closing brace (simplified)
            brace_count = 1
            class_end = class_start
            for i, char in enumerate(content[class_start:], class_start):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        class_end = i
                        break
            
            class_body = content[class_start:class_end]
            
            # Extract fields
            fields = []
            for field_match in self.java_field_pattern.finditer(class_body):
                field_type = field_match.group(1)
                field_name = field_match.group(2)
                fields.append({"name": field_name, "type": field_type})
                
                # Check for @ManyToOne, @OneToMany annotations
                field_context = class_body[max(0, field_match.start() - 100):field_match.start()]
                if any(ann in field_context for ann in ["@ManyToOne", "@OneToMany", "@ManyToMany", "@OneToOne"]):
                    # Extract the entity type from generics or field type
                    generic_match = re.search(r'<(\w+)>', field_type)
                    if generic_match:
                        relationships[class_name].add(generic_match.group(1))
                    elif field_type != class_name:
                        relationships[class_name].add(field_type)
            
            # Check for @Entity annotation
            entity_type = "class"
            if "@Entity" in content[:match.start()]:
                entity_type = "entity"
            
            if base_class:
                relationships[class_name].add(base_class)
            
            entity = CodeEntity(
                name=class_name,
                entity_type=entity_type,
                file_path=str(file_path),
                line_number=line_number,
                fields=fields,
                methods=[],  # Would need more complex parsing
                relationships=list(relationships[class_name]),
                description=None
            )
            entities.append(entity)
        
        # Find Spring API endpoints
        lines = content.split('\n')
        for i, line in enumerate(lines):
            match = self.api_patterns["spring"].search(line)
            if match:
                path = match.group(1)
                method_match = re.search(r'@(\w+)Mapping', line)
                method = method_match.group(1).upper() if method_match else "GET"
                
                # Find handler method
                handler = ""
                for j in range(i + 1, min(i + 5, len(lines))):
                    method_match = re.search(r'public\s+\w+\s+(\w+)\s*\(', lines[j])
                    if method_match:
                        handler = method_match.group(1)
                        break
                
                endpoint = ApiEndpoint(
                    path=path,
                    method=method,
                    handler=handler,
                    entities_referenced=[],
                    file_path=str(file_path),
                    line_number=i + 1
                )
                api_endpoints.append(endpoint)
        
        return entities, api_endpoints, dict(relationships)

    def _analyze_javascript(
        self, 
        content: str, 
        file_path: Path
    ) -> tuple[list[CodeEntity], list[ApiEndpoint], dict]:
        """Analyze JavaScript/TypeScript source code.
        
        Args:
            content: File content
            file_path: Path to the file
            
        Returns:
            Tuple of (entities, api_endpoints, relationships)
        """
        entities = []
        api_endpoints = []
        relationships = defaultdict(set)
        
        # Find classes
        for match in self.js_class_pattern.finditer(content):
            class_name = match.group(1)
            base_class = match.group(2)
            
            line_number = content[:match.start()].count('\n') + 1
            
            if base_class:
                relationships[class_name].add(base_class)
            
            entity = CodeEntity(
                name=class_name,
                entity_type="class",
                file_path=str(file_path),
                line_number=line_number,
                fields=[],
                methods=[],
                relationships=list(relationships[class_name]),
                description=None
            )
            entities.append(entity)
        
        # Find interfaces (TypeScript)
        for match in self.js_interface_pattern.finditer(content):
            interface_name = match.group(1)
            line_number = content[:match.start()].count('\n') + 1
            
            entity = CodeEntity(
                name=interface_name,
                entity_type="interface",
                file_path=str(file_path),
                line_number=line_number,
                fields=[],
                methods=[],
                relationships=[],
                description=None
            )
            entities.append(entity)
        
        # Find Express.js API endpoints
        lines = content.split('\n')
        for i, line in enumerate(lines):
            match = self.api_patterns["express"].search(line)
            if match:
                path = match.group(1)
                method_match = re.search(r'app\.(\w+)\(', line)
                method = method_match.group(1).upper() if method_match else "GET"
                
                # Handler is usually inline or next parameter
                handler = "inline"
                
                endpoint = ApiEndpoint(
                    path=path,
                    method=method,
                    handler=handler,
                    entities_referenced=[],
                    file_path=str(file_path),
                    line_number=i + 1
                )
                api_endpoints.append(endpoint)
        
        return entities, api_endpoints, dict(relationships)


def analyze_code(
    code_paths: list[str],
    languages: list[str] = None,
    exclude_patterns: list[str] = None
) -> CodeInsight:
    """Convenience function to analyze source code.
    
    Args:
        code_paths: List of code directory or file paths
        languages: Languages to analyze
        exclude_patterns: Patterns to exclude
        
    Returns:
        CodeInsight object
    """
    analyzer = CodeAnalyzer(languages=languages, exclude_patterns=exclude_patterns)
    return analyzer.analyze_code(code_paths)
