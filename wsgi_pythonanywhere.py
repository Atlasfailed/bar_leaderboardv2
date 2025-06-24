#!/usr/bin/python3
"""
WSGI configuration for BAR Leaderboard on PythonAnywhere
"""

import sys
import os

# Add your project directory to the Python path
path = '/home/yourusername/mysite'  # Change this to your actual path
if path not in sys.path:
    sys.path.insert(0, path)

# Import your Flask application
from flask_app import app as application

if __name__ == "__main__":
    application.run()
