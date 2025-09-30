#!/usr/bin/env python3
"""
Upload ONLY essential generated files to PythonAnywhere /home/roark2120/data directory.
This script excludes large external datamart files to work within free tier limits.
"""

import os
import sys
import requests
import json
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PythonAnywhereUploader:
    def __init__(self, username, api_token):
        self.username = username
        self.api_token = api_token
        self.base_url = f"https://www.pythonanywhere.com/api/v0/user/{username}/files"
        self.headers = {
            'Authorization': f'Token {api_token}'
        }
    
    def upload_file(self, local_path, remote_path):
        """Upload a single file to PythonAnywhere."""
        try:
            # Read the file content
            with open(local_path, 'rb') as f:
                content = f.read()
            
            # Check file size (PythonAnywhere free tier has limits)
            file_size_mb = len(content) / (1024 * 1024)
            if file_size_mb > 100:  # 100MB limit
                logger.error(f"❌ File {local_path} is too large ({file_size_mb:.1f}MB). Skipping.")
                return False
            
            # Prepare the API request
            url = f"{self.base_url}/path{remote_path}"
            headers = {
                'Authorization': f'Token {self.api_token}'
            }
            
            # Use files parameter for file upload
            files = {'content': (os.path.basename(local_path), content)}
            
            logger.info(f"Uploading {local_path} ({file_size_mb:.1f}MB) to {remote_path}...")
            response = requests.post(url, headers=headers, files=files)
            
            if response.status_code == 201:
                logger.info(f"✅ Successfully uploaded {local_path}")
                return True
            elif response.status_code == 200:
                logger.info(f"✅ Successfully updated {local_path}")
                return True
            else:
                logger.error(f"❌ Failed to upload {local_path}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error uploading {local_path}: {str(e)}")
            return False
    
    def create_directory(self, remote_path):
        """Create a directory on PythonAnywhere if it doesn't exist."""
        try:
            url = f"{self.base_url}/path{remote_path}"
            response = requests.post(url, headers=self.headers, json={'type': 'directory'})
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Directory {remote_path} created")
                return True
            elif response.status_code == 409:
                logger.info(f"✅ Directory {remote_path} already exists")
                return True
            else:
                logger.warning(f"⚠️  Could not create directory {remote_path}: {response.status_code}")
                return True
        except Exception as e:
            logger.warning(f"⚠️  Error creating directory {remote_path}: {str(e)}")
            return True

def main():
    """Main upload function."""
    # Get credentials from environment
    username = os.getenv('PYTHONANYWHERE_USERNAME')
    api_token = os.getenv('PYTHONANYWHERE_API_TOKEN')
    
    if not username or not api_token:
        logger.error("❌ Missing PythonAnywhere credentials. Please set PYTHONANYWHERE_USERNAME and PYTHONANYWHERE_API_TOKEN environment variables.")
        sys.exit(1)
    
    # Initialize uploader
    uploader = PythonAnywhereUploader(username, api_token)
    
    # Fixed data directory on PythonAnywhere
    data_dir = Path("data")
    remote_data_dir = "/home/roark2120/data"
    
    # Create remote data directory
    uploader.create_directory(remote_data_dir)
    
    # ONLY upload small essential files - NO EXTERNAL DATAMART FILES
    essential_files = [
        # Core generated leaderboard files (small, essential)
        "final_leaderboard.parquet",
        "nation_rankings.parquet", 
        "player_contributions.parquet",
        "team_rosters.parquet",
        
        # Static reference files (tiny)
        "iso_country.csv",
        "efficiency_vs_speed_analysis_with_names.csv",
        
        # Season 1 files (if they exist)
        "season_1_final_leaderboard.parquet",
        "season_1_nation_rankings.parquet", 
        "season_1_player_contributions.parquet",
        
        # Analysis results
        "roster_analysis_results.json",
    ]
    
    logger.info("🚀 UPLOADING TO: /home/roark2120/data")
    logger.info("🔍 EXCLUDING large external files:")
    logger.info("   ❌ matches.parquet (EXTERNAL - too large)")
    logger.info("   ❌ match_players.parquet (EXTERNAL - too large)")  
    logger.info("   ❌ players.parquet (EXTERNAL - can be re-downloaded)")
    logger.info(f"📋 Will upload {len(essential_files)} essential files only")
    
    # Upload only the essential files
    success_count = 0
    total_files = 0
    
    for filename in essential_files:
        local_path = data_dir / filename
        remote_path = f"{remote_data_dir}/{filename}"
        
        if local_path.exists():
            total_files += 1
            if uploader.upload_file(local_path, remote_path):
                success_count += 1
        else:
            logger.info(f"⚠️  Optional file not found: {filename}")
    
    # Upload latest performance report if exists
    performance_reports = list(data_dir.glob("performance_report_*.json"))
    if performance_reports:
        latest_report = max(performance_reports, key=lambda p: p.stat().st_mtime)
        total_files += 1
        if uploader.upload_file(latest_report, f"{remote_data_dir}/{latest_report.name}"):
            success_count += 1
    
    # Summary
    logger.info(f"📊 Upload Summary: {success_count}/{total_files} files uploaded successfully")
    logger.info(f"🎯 Target directory: {remote_data_dir}")
    
    if success_count == total_files and total_files > 0:
        logger.info("🎉 All files uploaded successfully!")
        return 0
    elif success_count > 0 and total_files > 0:
        # Success if we uploaded most files
        success_rate = success_count / total_files
        if success_rate >= 0.7:  # 70% success rate is acceptable
            logger.info(f"✅ Upload completed with {success_rate:.1%} success rate")
            return 0
        else:
            logger.error("⚠️  Too many files failed to upload")
            return 1
    elif total_files == 0:
        logger.warning("⚠️  No files found to upload")
        return 1
    else:
        logger.error("⚠️  Upload failed completely")
        return 1

if __name__ == "__main__":
    sys.exit(main())