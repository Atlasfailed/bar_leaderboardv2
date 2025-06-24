"""
WSGI config for PythonAnywhere deployment.
This file contains the WSGI configuration required to serve the Flask app on PythonAnywhere.
"""

import sys
import os

# Add your project directory to Python path
path = '/home/roark2120/nation_leaderboard'  # Replace 'yourusername' with your actual PythonAnywhere username
if path not in sys.path:
    sys.path.insert(0, path)

# Set environment variables
os.environ['FLASK_ENV'] = 'production'

from .app import app as application

if __name__ == "__main__":
    application.run()
