"""
Output writer abstraction for medical pricing data.
Supports multiple output formats: database, JSON, CSV.
"""
import json
import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class OutputWriter(ABC):
    """Abstract base class for output writers."""

    @abstractmethod
    def write_hospital(self, hospital_data: Dict[str, Any]) -> None:
        """Write hospital data to output destination."""
        pass

    @abstractmethod
    def write_operations(self, operations: List[Dict[str, Any]]) -> None:
        """Write medical operations data to output destination."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close any open resources."""
        pass


class DatabaseWriter(OutputWriter):
    """Writes data to Supabase database (maintains current behavior)."""

    def __init__(self):
        """Initialize database writer with Supabase manager."""
        from .database import supabase_manager
        self.supabase_manager = supabase_manager

        # Initialize Supabase if not already done
        if not self.supabase_manager.client:
            self.supabase_manager.initialize()
            logger.info("DatabaseWriter: Supabase client initialized")

    def write_hospital(self, hospital_data: Dict[str, Any]) -> None:
        """Write hospital data to Supabase."""
        try:
            self.supabase_manager.insert_hospital(hospital_data)
            logger.debug(f"DatabaseWriter: Inserted hospital {hospital_data.get('facility_id')}")
        except Exception as e:
            logger.error(f"DatabaseWriter: Failed to write hospital: {e}")
            raise

    def write_operations(self, operations: List[Dict[str, Any]]) -> None:
        """Batch write medical operations to Supabase."""
        try:
            self.supabase_manager.batch_insert_medical_operations(operations)
            logger.debug(f"DatabaseWriter: Inserted {len(operations)} operations")
        except Exception as e:
            logger.error(f"DatabaseWriter: Failed to write operations: {e}")
            raise

    def close(self) -> None:
        """Close database connections."""
        # Supabase client doesn't require explicit closing
        logger.info("DatabaseWriter: Closed")


class JSONWriter(OutputWriter):
    """Writes data to JSON files."""

    def __init__(self, output_dir: Path):
        """
        Initialize JSON writer.

        Args:
            output_dir: Directory where JSON files will be written
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.hospitals = []
        self.operations = []
        self.current_facility_id: Optional[str] = None

        logger.info(f"JSONWriter: Initialized with output_dir={self.output_dir}")

    def write_hospital(self, hospital_data: Dict[str, Any]) -> None:
        """Collect hospital data (written on close)."""
        self.hospitals.append(hospital_data)
        self.current_facility_id = hospital_data.get('facility_id', 'unknown')
        logger.debug(f"JSONWriter: Collected hospital {self.current_facility_id}")

    def write_operations(self, operations: List[Dict[str, Any]]) -> None:
        """Collect operations data (written on close)."""
        self.operations.extend(operations)
        logger.debug(f"JSONWriter: Collected {len(operations)} operations")

    def close(self) -> None:
        """Write collected data to JSON files."""
        try:
            facility_id = self.current_facility_id or 'unknown'

            # Write hospitals file
            if self.hospitals:
                hospitals_file = self.output_dir / f"{facility_id}_hospitals.json"
                with open(hospitals_file, 'w', encoding='utf-8') as f:
                    json.dump(self.hospitals, f, indent=2, default=str)
                logger.info(f"JSONWriter: Wrote {len(self.hospitals)} hospitals to {hospitals_file}")

            # Write operations file
            if self.operations:
                operations_file = self.output_dir / f"{facility_id}_operations.json"
                with open(operations_file, 'w', encoding='utf-8') as f:
                    json.dump(self.operations, f, indent=2, default=str)
                logger.info(f"JSONWriter: Wrote {len(self.operations)} operations to {operations_file}")

            logger.info("JSONWriter: Closed")

        except Exception as e:
            logger.error(f"JSONWriter: Failed to write files: {e}")
            raise


class CSVWriter(OutputWriter):
    """Writes data to CSV files."""

    def __init__(self, output_dir: Path):
        """
        Initialize CSV writer.

        Args:
            output_dir: Directory where CSV files will be written
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.hospitals = []
        self.operations = []
        self.current_facility_id: Optional[str] = None

        logger.info(f"CSVWriter: Initialized with output_dir={self.output_dir}")

    def write_hospital(self, hospital_data: Dict[str, Any]) -> None:
        """Collect hospital data (written on close)."""
        self.hospitals.append(hospital_data)
        self.current_facility_id = hospital_data.get('facility_id', 'unknown')
        logger.debug(f"CSVWriter: Collected hospital {self.current_facility_id}")

    def write_operations(self, operations: List[Dict[str, Any]]) -> None:
        """Collect operations data (written on close)."""
        self.operations.extend(operations)
        logger.debug(f"CSVWriter: Collected {len(operations)} operations")

    def close(self) -> None:
        """Write collected data to CSV files."""
        try:
            facility_id = self.current_facility_id or 'unknown'

            # Write hospitals file
            if self.hospitals:
                hospitals_file = self.output_dir / f"{facility_id}_hospitals.csv"
                fieldnames = list(self.hospitals[0].keys())

                with open(hospitals_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(self.hospitals)

                logger.info(f"CSVWriter: Wrote {len(self.hospitals)} hospitals to {hospitals_file}")

            # Write operations file
            if self.operations:
                operations_file = self.output_dir / f"{facility_id}_operations.csv"

                # Flatten codes dict to string for CSV
                flattened_ops = []
                for op in self.operations:
                    flat_op = op.copy()
                    if 'codes' in flat_op and isinstance(flat_op['codes'], dict):
                        flat_op['codes'] = json.dumps(flat_op['codes'])
                    flattened_ops.append(flat_op)

                fieldnames = list(flattened_ops[0].keys())

                with open(operations_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(flattened_ops)

                logger.info(f"CSVWriter: Wrote {len(self.operations)} operations to {operations_file}")

            logger.info("CSVWriter: Closed")

        except Exception as e:
            logger.error(f"CSVWriter: Failed to write files: {e}")
            raise
