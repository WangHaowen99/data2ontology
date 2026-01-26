"""Log analyzer module for extracting business insights from application logs."""

import re
from pathlib import Path
from typing import Optional
from collections import defaultdict, Counter
from datetime import datetime

from .models.metadata import (
    LogInsight,
    EntityReference,
    OperationPattern,
)


class LogAnalyzer:
    """Analyzes application logs to extract business entity references and patterns."""

    def __init__(self, max_lines: int = 10000):
        """Initialize the log analyzer.
        
        Args:
            max_lines: Maximum number of log lines to process per file
        """
        self.max_lines = max_lines
        
        # Common entity patterns
        self.entity_patterns = {
            "user": re.compile(r'\b(?:user|usuario|uid)[_\s]*[:\=]?\s*["\']?(\w+)["\']?', re.IGNORECASE),
            "order": re.compile(r'\b(?:order|pedido|oid)[_\s]*[:\=]?\s*["\']?(\w+)["\']?', re.IGNORECASE),
            "product": re.compile(r'\b(?:product|produto|pid)[_\s]*[:\=]?\s*["\']?(\w+)["\']?', re.IGNORECASE),
            "customer": re.compile(r'\b(?:customer|cliente|cid)[_\s]*[:\=]?\s*["\']?(\w+)["\']?', re.IGNORECASE),
            "id": re.compile(r'\b(\w+)_id[:\=]?\s*["\']?([a-zA-Z0-9\-]+)["\']?', re.IGNORECASE),
        }
        
        # Operation patterns (CRUD)
        self.operation_patterns = {
            "CREATE": re.compile(r'\b(?:create|insert|add|new|created|inserted|added)\b', re.IGNORECASE),
            "READ": re.compile(r'\b(?:read|select|get|fetch|query|retrieved|loaded)\b', re.IGNORECASE),
            "UPDATE": re.compile(r'\b(?:update|modify|change|updated|modified|changed)\b', re.IGNORECASE),
            "DELETE": re.compile(r'\b(?:delete|remove|drop|deleted|removed)\b', re.IGNORECASE),
        }
        
        # Timestamp patterns
        self.timestamp_patterns = [
            re.compile(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}'),  # ISO format
            re.compile(r'\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}'),    # US format
            re.compile(r'\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]'), # Bracketed
        ]

    def analyze_logs(self, log_paths: list[str]) -> LogInsight:
        """Analyze log files and extract insights.
        
        Args:
            log_paths: List of log file paths to analyze
            
        Returns:
            LogInsight object containing extracted information
        """
        entity_refs = []
        operation_patterns = []
        entity_cooccurrences = defaultdict(set)
        total_lines = 0
        files_analyzed = []
        
        # Track entities per line for cooccurrence
        operations_by_type = defaultdict(lambda: {
            "count": 0,
            "entities": set(),
            "samples": [],
            "timestamps": []
        })
        
        for log_path in log_paths:
            path = Path(log_path)
            if not path.exists():
                continue
                
            files_analyzed.append(str(path))
            
            try:
                with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    for i, line in enumerate(f):
                        if i >= self.max_lines:
                            break
                        
                        total_lines += 1
                        line = line.strip()
                        
                        if not line:
                            continue
                        
                        # Extract timestamp
                        timestamp = self._extract_timestamp(line)
                        
                        # Extract entities from this line
                        line_entities = self._extract_entities_from_line(line, str(path), i + 1)
                        entity_refs.extend(line_entities)
                        
                        # Track entity cooccurrence (entities appearing in the same line)
                        entity_names = [e.entity_name for e in line_entities]
                        for entity in entity_names:
                            for other in entity_names:
                                if entity != other:
                                    entity_cooccurrences[entity].add(other)
                        
                        # Detect operations
                        detected_ops = self._detect_operations(line, entity_names)
                        for op_type in detected_ops:
                            operations_by_type[op_type]["count"] += 1
                            operations_by_type[op_type]["entities"].update(entity_names)
                            if len(operations_by_type[op_type]["samples"]) < 3:
                                operations_by_type[op_type]["samples"].append(line[:200])
                            if timestamp:
                                operations_by_type[op_type]["timestamps"].append(timestamp)
                            
            except Exception as e:
                # Skip files that can't be read
                continue
        
        # Convert operation tracking to OperationPattern objects
        for op_type, data in operations_by_type.items():
            timestamps = data["timestamps"]
            time_range = None
            if timestamps:
                time_range = (min(timestamps), max(timestamps))
            
            pattern = OperationPattern(
                operation_type=op_type,
                entities_involved=list(data["entities"]),
                frequency=data["count"],
                timestamp_range=time_range,
                sample_log_lines=data["samples"]
            )
            operation_patterns.append(pattern)
        
        # Convert cooccurrence sets to lists
        cooccurrence_dict = {k: list(v) for k, v in entity_cooccurrences.items()}
        
        return LogInsight(
            entity_references=entity_refs,
            operation_patterns=operation_patterns,
            entity_cooccurrences=cooccurrence_dict,
            total_log_lines_analyzed=total_lines,
            log_files_analyzed=files_analyzed
        )

    def _extract_entities_from_line(
        self, 
        line: str, 
        file_path: str, 
        line_number: int
    ) -> list[EntityReference]:
        """Extract entity references from a log line.
        
        Args:
            line: Log line content
            file_path: Path to the log file
            line_number: Line number in the file
            
        Returns:
            List of EntityReference objects
        """
        entities = []
        
        for entity_type, pattern in self.entity_patterns.items():
            matches = pattern.finditer(line)
            for match in matches:
                entity_id = None
                entity_name = entity_type
                
                if entity_type == "id":
                    # Special handling for generic _id pattern
                    entity_name = match.group(1)  # Get the prefix (e.g., "user" from "user_id")
                    entity_id = match.group(2)
                    confidence = 0.8
                else:
                    entity_id = match.group(1) if match.groups() else None
                    confidence = 0.9
                
                # Get context (surrounding text)
                start = max(0, match.start() - 50)
                end = min(len(line), match.end() + 50)
                context = line[start:end]
                
                entities.append(EntityReference(
                    entity_name=entity_name.lower(),
                    entity_id=entity_id,
                    source_location=f"{file_path}:{line_number}",
                    context=context,
                    confidence=confidence
                ))
        
        return entities

    def _detect_operations(self, line: str, entities: list[str]) -> list[str]:
        """Detect operation types in a log line.
        
        Args:
            line: Log line content
            entities: List of entity names found in this line
            
        Returns:
            List of detected operation types
        """
        operations = []
        
        # Only detect operations if there are entities involved
        if not entities:
            return operations
        
        for op_type, pattern in self.operation_patterns.items():
            if pattern.search(line):
                operations.append(op_type)
        
        return operations

    def _extract_timestamp(self, line: str) -> Optional[str]:
        """Extract timestamp from a log line.
        
        Args:
            line: Log line content
            
        Returns:
            Timestamp string if found, None otherwise
        """
        for pattern in self.timestamp_patterns:
            match = pattern.search(line)
            if match:
                return match.group(0).strip('[]')
        
        return None

    def get_entity_summary(self, insight: LogInsight) -> dict[str, dict]:
        """Get a summary of entities found in logs.
        
        Args:
            insight: LogInsight object
            
        Returns:
            Dictionary mapping entity names to statistics
        """
        entity_stats = defaultdict(lambda: {
            "count": 0,
            "unique_ids": set(),
            "operations": set()
        })
        
        # Count entity references
        for ref in insight.entity_references:
            entity_stats[ref.entity_name]["count"] += 1
            if ref.entity_id:
                entity_stats[ref.entity_name]["unique_ids"].add(ref.entity_id)
        
        # Add operation information
        for pattern in insight.operation_patterns:
            for entity in pattern.entities_involved:
                entity_stats[entity]["operations"].add(pattern.operation_type)
        
        # Convert to regular dict with serializable values
        return {
            entity: {
                "reference_count": stats["count"],
                "unique_id_count": len(stats["unique_ids"]),
                "operations": list(stats["operations"])
            }
            for entity, stats in entity_stats.items()
        }


def analyze_logs(log_paths: list[str], max_lines: int = 10000) -> LogInsight:
    """Convenience function to analyze log files.
    
    Args:
        log_paths: List of log file paths
        max_lines: Maximum lines to process per file
        
    Returns:
        LogInsight object
    """
    analyzer = LogAnalyzer(max_lines=max_lines)
    return analyzer.analyze_logs(log_paths)
