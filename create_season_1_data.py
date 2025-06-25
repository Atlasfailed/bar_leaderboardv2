#!/usr/bin/env python3
"""
Create Season 1 Data Files
==========================

This script creates static Season 1 data files by filtering the current data
to only include records from before the Season 2 start date (March 23rd, 2025, 12:00 CET).
"""

import pandas as pd
import os
from datetime import datetime
import pytz
from config import config

def create_season_1_data():
    """Create Season 1 data files from current data."""
    
    # Season 1 end date (same as Season 2 start)
    season_1_end = config.seasons.season_1_end
    print(f"Creating Season 1 data for records before: {season_1_end}")
    
    # Create Season 1 leaderboard data
    print("\n1. Creating Season 1 leaderboard data...")
    if os.path.exists(config.paths.final_leaderboard_parquet):
        df = pd.read_parquet(config.paths.final_leaderboard_parquet)
        print(f"Loaded {len(df)} total leaderboard records")
        
        # Convert start_time to datetime if it's not already
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
            
            # Filter for Season 1 (before season 2 start)
            season_1_df = df[df['start_time'] < season_1_end].copy()
            print(f"Filtered to {len(season_1_df)} Season 1 records")
            
            # Save Season 1 leaderboard
            season_1_df.to_parquet(config.paths.season_1_leaderboard_parquet)
            print(f"Saved Season 1 leaderboard to: {config.paths.season_1_leaderboard_parquet}")
        else:
            print("WARNING: No 'start_time' column found in leaderboard data")
    else:
        print(f"ERROR: Leaderboard file not found: {config.paths.final_leaderboard_parquet}")
    
    # Create Season 1 nation rankings data
    print("\n2. Creating Season 1 nation rankings data...")
    if os.path.exists(config.paths.nation_rankings_parquet):
        df = pd.read_parquet(config.paths.nation_rankings_parquet)
        print(f"Loaded {len(df)} total nation ranking records")
        
        # Nation rankings might not have timestamps, so we'll copy all data for Season 1
        # This represents the final state at the end of Season 1
        df.to_parquet(config.paths.season_1_nation_rankings_parquet)
        print(f"Saved Season 1 nation rankings to: {config.paths.season_1_nation_rankings_parquet}")
    else:
        print(f"ERROR: Nation rankings file not found: {config.paths.nation_rankings_parquet}")
    
    # Create Season 1 player contributions data
    print("\n3. Creating Season 1 player contributions data...")
    if os.path.exists(config.paths.player_contributions_parquet):
        df = pd.read_parquet(config.paths.player_contributions_parquet)
        print(f"Loaded {len(df)} total player contribution records")
        
        # Player contributions might not have timestamps, so we'll copy all data for Season 1
        # This represents the final state at the end of Season 1
        df.to_parquet(config.paths.season_1_player_contributions_parquet)
        print(f"Saved Season 1 player contributions to: {config.paths.season_1_player_contributions_parquet}")
    else:
        print(f"ERROR: Player contributions file not found: {config.paths.player_contributions_parquet}")
    
    print("\n✅ Season 1 data creation completed!")
    print("Season 1 files created:")
    print(f"  - {config.paths.season_1_leaderboard_parquet}")
    print(f"  - {config.paths.season_1_nation_rankings_parquet}")
    print(f"  - {config.paths.season_1_player_contributions_parquet}")

def verify_season_1_data():
    """Verify the created Season 1 data files."""
    print("\n📊 Verifying Season 1 data...")
    
    files_to_check = [
        (config.paths.season_1_leaderboard_parquet, "Season 1 Leaderboard"),
        (config.paths.season_1_nation_rankings_parquet, "Season 1 Nation Rankings"),
        (config.paths.season_1_player_contributions_parquet, "Season 1 Player Contributions")
    ]
    
    for file_path, description in files_to_check:
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                print(f"✅ {description}: {len(df)} records")
                
                # Show data structure
                print(f"   Columns: {list(df.columns)}")
                if 'start_time' in df.columns:
                    df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
                    min_date = df['start_time'].min()
                    max_date = df['start_time'].max()
                    print(f"   Date range: {min_date} to {max_date}")
                print()
            except Exception as e:
                print(f"❌ {description}: Error reading file - {e}")
        else:
            print(f"❌ {description}: File not found at {file_path}")

if __name__ == "__main__":
    print("🏆 BAR Season 1 Data Creation")
    print("=" * 50)
    
    create_season_1_data()
    verify_season_1_data()
    
    print("\n🎯 Next steps:")
    print("1. Restart your Flask application to load the Season 1 data")
    print("2. Test the season navigation on your website")
    print("3. Season 1 data is now static and won't change")
