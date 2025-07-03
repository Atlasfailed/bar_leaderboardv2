#!/usr/bin/env python3
"""
Search for player "Atlasfailed" in Season 1 data
"""

import pandas as pd
import sys
from pathlib import Path

def search_player_in_season_1():
    """Search for Atlasfailed in Season 1 data files."""
    
    data_dir = Path("data")
    player_name = "Atlasfailed"
    
    print(f"🔍 Searching for player '{player_name}' in Season 1 data...")
    print("=" * 60)
    
    # Check Season 1 leaderboard
    print("\n1. Season 1 Leaderboard:")
    leaderboard_file = data_dir / "season_1_final_leaderboard.parquet"
    if leaderboard_file.exists():
        df = pd.read_parquet(leaderboard_file)
        print(f"   Total entries: {len(df):,}")
        print(f"   Game types: {df['game_type'].value_counts().to_dict()}")
        
        # Search for the player (case-insensitive)
        player_matches = df[df['name'].str.contains(player_name, case=False, na=False)]
        if not player_matches.empty:
            print(f"\n   ✅ Found {len(player_matches)} entries for '{player_name}':")
            for _, row in player_matches.iterrows():
                print(f"      Game Type: {row['game_type']}, Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
        else:
            print(f"   ❌ No entries found for '{player_name}' in Season 1 leaderboard")
            
        # Check for Large Team specifically
        large_team_data = df[df['game_type'] == 'Large Team']
        print(f"\n   Large Team entries: {len(large_team_data):,}")
        large_team_matches = large_team_data[large_team_data['name'].str.contains(player_name, case=False, na=False)]
        if not large_team_matches.empty:
            print(f"   ✅ Found in Large Team rankings:")
            for _, row in large_team_matches.iterrows():
                print(f"      Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Rank: {row.get('rank', 'N/A')}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
        else:
            print(f"   ❌ Not found in Large Team rankings")
    else:
        print("   ❌ Season 1 leaderboard file not found")
    
    # Check current leaderboard for comparison
    print("\n2. Current Leaderboard (for comparison):")
    current_leaderboard_file = data_dir / "final_leaderboard.parquet"
    if current_leaderboard_file.exists():
        df_current = pd.read_parquet(current_leaderboard_file)
        print(f"   Total entries: {len(df_current):,}")
        
        # Search for the player in current data
        current_matches = df_current[df_current['name'].str.contains(player_name, case=False, na=False)]
        if not current_matches.empty:
            print(f"\n   ✅ Found {len(current_matches)} entries for '{player_name}' in current data:")
            for _, row in current_matches.iterrows():
                print(f"      Game Type: {row['game_type']}, Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
                
            # Check for Large Team specifically in current data
            large_team_current = current_matches[current_matches['game_type'] == 'Large Team']
            if not large_team_current.empty:
                print(f"\n   ✅ Found in current Large Team rankings:")
                for _, row in large_team_current.iterrows():
                    print(f"      Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Rank: {row.get('rank', 'N/A')}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
        else:
            print(f"   ❌ No entries found for '{player_name}' in current leaderboard")
    else:
        print("   ❌ Current leaderboard file not found")
    
    # Check raw match data to see if player exists at all
    print("\n3. Raw Match Data Check:")
    match_players_file = data_dir / "match_players.parquet"
    if match_players_file.exists():
        df_matches = pd.read_parquet(match_players_file)
        
        # Check if we have players data to get names
        players_file = data_dir / "players.parquet"
        if players_file.exists():
            df_players = pd.read_parquet(players_file)
            
            # Search for player in players data
            player_in_db = df_players[df_players['name'].str.contains(player_name, case=False, na=False)]
            if not player_in_db.empty:
                print(f"   ✅ Found player '{player_name}' in players database:")
                for _, player in player_in_db.iterrows():
                    user_id = player['user_id']
                    print(f"      User ID: {user_id}, Name: {player['name']}")
                    
                    # Check their match history
                    player_matches_raw = df_matches[df_matches['user_id'] == user_id]
                    print(f"      Total matches: {len(player_matches_raw):,}")
                    
                    if len(player_matches_raw) > 0:
                        # Load matches data to get game types
                        matches_file = data_dir / "matches.parquet"
                        if matches_file.exists():
                            df_matches_info = pd.read_parquet(matches_file)
                            player_matches_with_info = player_matches_raw.merge(df_matches_info, on='match_id')
                            
                            # Check game type distribution
                            game_type_counts = player_matches_with_info['game_type'].value_counts()
                            print(f"      Game type distribution: {game_type_counts.to_dict()}")
                            
                            # Check Large Team and Team games specifically
                            large_team_matches = len(player_matches_with_info[player_matches_with_info['game_type'] == 'Large Team'])
                            team_matches = len(player_matches_with_info[player_matches_with_info['game_type'] == 'Team'])
                            print(f"      Large Team games: {large_team_matches}")
                            print(f"      Legacy Team games: {team_matches}")
                            print(f"      Combined team games: {large_team_matches + team_matches}")
                            
                            # Check if they meet the threshold
                            from config import config
                            threshold = config.analysis.min_player_games_threshold
                            print(f"      Minimum threshold: {threshold}")
                            print(f"      Qualifies for Large Team: {(large_team_matches + team_matches) >= threshold}")
            else:
                print(f"   ❌ Player '{player_name}' not found in players database")
        else:
            print("   ❌ Players file not found")
    else:
        print("   ❌ Match players file not found")

    # Check for similar names
    print(f"\n4. Similar Names Check:")
    if 'df' in locals() and not df.empty:
        # Look for names that contain "atlas" (case-insensitive)
        similar_names = df[df['name'].str.contains('atlas', case=False, na=False)]['name'].unique()
        if len(similar_names) > 0:
            print(f"   Found names containing 'atlas': {list(similar_names)}")
        else:
            print("   No names containing 'atlas' found")

if __name__ == "__main__":
    search_player_in_season_1()
