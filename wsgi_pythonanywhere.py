#!/usr/bin/python3
"""
WSGI configuration for BAR Leaderboard on PythonAnywhere
"""

import sys
import os

# Add your project directory to the Python path
path = '/home/roark2120'  # Your current PythonAnywhere directory
if path not in sys.path:
    sys.path.insert(0, path)

# Import your Flask application
from static.app import app as application

if __name__ == "__main__":
    application.run()
