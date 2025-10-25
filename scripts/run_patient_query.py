#!/usr/bin/env python3
"""
Launcher script for the Patient Query CLI.
Run this script to start the interactive medical procedure query system.
"""
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import and run the CLI
from patient_query.cli import main

if __name__ == "__main__":
    main()
