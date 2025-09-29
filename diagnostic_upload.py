#!/usr/bin/env python3
"""
TEST: Simple script to verify what files exist and what would be uploaded.
This is a diagnostic script to debug the upload issue.
"""

import os
from pathlib import Path

def main():
    print("🔍 DIAGNOSTIC: Checking what files exist in data directory")
    
    data_dir = Path("data")
    if not data_dir.exists():
        print("❌ Data directory does not exist!")
        return
    
    all_files = list(data_dir.glob("*.parquet")) + list(data_dir.glob("*.csv")) + list(data_dir.glob("*.json"))
    
    print(f"📁 Found {len(all_files)} total files in data directory:")
    for file in sorted(all_files):
        size_mb = file.stat().st_size / (1024*1024)
        print(f"   📄 {file.name} ({size_mb:.1f} MB)")
    
    print("\n🎯 FILES THAT SHOULD BE UPLOADED (generated/static):")
    essential_files = [
        "final_leaderboard.parquet",
        "nation_rankings.parquet", 
        "player_contributions.parquet",
        "team_rosters.parquet",
        "iso_country.csv",
        "efficiency_vs_speed_analysis_with_names.csv",
        "roster_analysis_results.json",
    ]
    
    for filename in essential_files:
        file_path = data_dir / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024*1024)
            print(f"   ✅ {filename} ({size_mb:.1f} MB)")
        else:
            print(f"   ❌ {filename} (NOT FOUND)")
    
    print("\n🚫 FILES THAT SHOULD NOT BE UPLOADED (external datamart):")
    external_files = [
        "matches.parquet",
        "match_players.parquet", 
        "players.parquet",
    ]
    
    for filename in external_files:
        file_path = data_dir / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024*1024)
            print(f"   🚨 {filename} ({size_mb:.1f} MB) - SHOULD BE EXCLUDED")
        else:
            print(f"   ✅ {filename} (not present)")

if __name__ == "__main__":
    main()