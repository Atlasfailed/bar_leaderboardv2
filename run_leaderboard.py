#!/usr/bin/env python3
"""Entry point for the leaderboard calculation pipeline."""

import sys
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from run_pipelinev2 import main

if __name__ == "__main__":
    main()
