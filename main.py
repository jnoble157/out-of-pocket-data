#!/usr/bin/env python3
"""
Main entry point for the medical pricing data processing application.
"""
import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

# Import and run CLI
from src.cli import cli

if __name__ == '__main__':
    cli()
