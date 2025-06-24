#!/usr/bin/env python3
"""
Root-level Flask app entry point for BAR Nation Leaderboard.
This script sets up the Python path and imports the actual Flask app.
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Import the Flask app using absolute import
from flask_app import app

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BAR Leaderboard Flask App")
    parser.add_argument('--port', type=int, default=5000, help='Port to run the Flask app on (default: 5000)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to run the Flask app on (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true', help='Run Flask in debug mode')
    args = parser.parse_args()
    app.run(debug=args.debug, host=args.host, port=args.port)