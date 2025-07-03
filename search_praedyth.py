#!/usr/bin/env python3
"""
Search for player "Praedyth" to investigate ranking discrepancy
"""

import pandas as pd
import sys
from pathlib import Path

def search_praedyth():
    """Search for Praedyth in all data sources to understand the ranking issue."""
    
    data_dir = Path("data")
    player_name = "Praedyth"
    
    print(f"🔍 Investigating player '{player_name}' ranking discrepancy...")
    print("=" * 70)
    
    # Check current leaderboard
    print("\n1. Current Leaderboard Data:")
    current_leaderboard_file = data_dir / "final_leaderboard.parquet"
    if current_leaderboard_file.exists():
        df_current = pd.read_parquet(current_leaderboard_file)
        
        # Search for the player (case-insensitive)
        player_matches = df_current[df_current['name'].str.contains(player_name, case=False, na=False)]
        if not player_matches.empty:
            print(f"\n   ✅ Found {len(player_matches)} entries for '{player_name}' in current data:")
            for _, row in player_matches.iterrows():
                print(f"      Game Type: {row['game_type']}, Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Leaderboard: {row.get('leaderboard_id', 'N/A')}, Rank: {row.get('rank', 'N/A')}")
                
            # Check Large Team specifically
            large_team_current = player_matches[player_matches['game_type'] == 'Large Team']
            if not large_team_current.empty:
                print(f"\n   ✅ Current Large Team rankings for {player_name}:")
                for _, row in large_team_current.iterrows():
                    print(f"      Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Rank: {row.get('rank', 'N/A')}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
        else:
            print(f"   ❌ No entries found for '{player_name}' in current leaderboard")
    else:
        print("   ❌ Current leaderboard file not found")
    
    # Check Season 1 leaderboard
    print("\n2. Season 1 Leaderboard Data:")
    season1_leaderboard_file = data_dir / "season_1_final_leaderboard.parquet"
    if season1_leaderboard_file.exists():
        df_season1 = pd.read_parquet(season1_leaderboard_file)
        
        # Search for the player (case-insensitive)
        player_matches_s1 = df_season1[df_season1['name'].str.contains(player_name, case=False, na=False)]
        if not player_matches_s1.empty:
            print(f"\n   ✅ Found {len(player_matches_s1)} entries for '{player_name}' in Season 1 data:")
            for _, row in player_matches_s1.iterrows():
                print(f"      Game Type: {row['game_type']}, Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Leaderboard: {row.get('leaderboard_id', 'N/A')}, Rank: {row.get('rank', 'N/A')}")
                
            # Check Large Team specifically
            large_team_s1 = player_matches_s1[player_matches_s1['game_type'] == 'Large Team']
            if not large_team_s1.empty:
                print(f"\n   ✅ Season 1 Large Team rankings for {player_name}:")
                for _, row in large_team_s1.iterrows():
                    print(f"      Rating: {row.get('leaderboard_rating', 'N/A'):.2f}, Rank: {row.get('rank', 'N/A')}, Leaderboard: {row.get('leaderboard_id', 'N/A')}")
        else:
            print(f"   ❌ No entries found for '{player_name}' in Season 1 leaderboard")
    else:
        print("   ❌ Season 1 leaderboard file not found")
    
    # Check raw match data to see player's game history
    print("\n3. Raw Match Data Analysis:")
    match_players_file = data_dir / "match_players.parquet"
    players_file = data_dir / "players.parquet"
    matches_file = data_dir / "matches.parquet"
    
    if all(f.exists() for f in [match_players_file, players_file, matches_file]):
        df_players = pd.read_parquet(players_file)
        df_match_players = pd.read_parquet(match_players_file)
        df_matches = pd.read_parquet(matches_file)
        
        # Find the player in players database
        player_in_db = df_players[df_players['name'].str.contains(player_name, case=False, na=False)]
        if not player_in_db.empty:
            print(f"\n   ✅ Found player '{player_name}' in players database:")
            for _, player in player_in_db.iterrows():
                user_id = player['user_id']
                actual_name = player['name']
                print(f"      User ID: {user_id}, Name: {actual_name}")
                
                # Get their match history
                player_matches_raw = df_match_players[df_match_players['user_id'] == user_id]
                print(f"      Total matches: {len(player_matches_raw):,}")
                
                if len(player_matches_raw) > 0:
                    # Merge with match details to get game types
                    player_matches_with_info = player_matches_raw.merge(df_matches, on='match_id')
                    
                    # Check game type distribution
                    game_type_counts = player_matches_with_info['game_type'].value_counts()
                    print(f"      Game type distribution: {game_type_counts.to_dict()}")
                    
                    # Check specific game modes
                    large_team_games = len(player_matches_with_info[player_matches_with_info['game_type'] == 'Large Team'])
                    team_games = len(player_matches_with_info[player_matches_with_info['game_type'] == 'Team'])
                    small_team_games = len(player_matches_with_info[player_matches_with_info['game_type'] == 'Small Team'])
                    
                    print(f"      Large Team games: {large_team_games}")
                    print(f"      Legacy Team games: {team_games}")
                    print(f"      Small Team games: {small_team_games}")
                    print(f"      Combined Large+Legacy Team games: {large_team_games + team_games}")
                    print(f"      Combined Small+Legacy Team games: {small_team_games + team_games}")
                    
                    # Check if they meet thresholds
                    from config import config
                    threshold = config.analysis.min_player_games_threshold
                    print(f"      Minimum threshold: {threshold}")
                    print(f"      Qualifies for Large Team (with legacy): {(large_team_games + team_games) >= threshold}")
                    print(f"      Qualifies for Large Team (without legacy): {large_team_games >= threshold}")
                    print(f"      Qualifies for Small Team (with legacy): {(small_team_games + team_games) >= threshold}")
                    print(f"      Qualifies for Small Team (without legacy): {small_team_games >= threshold}")
                    
                    # Check if they have ranking data (skill/uncertainty)
                    player_with_rankings = player_matches_with_info.dropna(subset=['new_skill', 'new_uncertainty'])
                    if not player_with_rankings.empty:
                        print(f"      Has ranking data: {len(player_with_rankings):,} matches with skill/uncertainty")
                        
                        # Show latest ratings by game type
                        print(f"      Latest ratings by game type:")
                        for game_type in ['Large Team', 'Small Team', 'Team', 'Duel']:
                            game_data = player_with_rankings[player_with_rankings['game_type'] == game_type]
                            if not game_data.empty:
                                latest = game_data.sort_values('start_time').iloc[-1]
                                rating = latest['new_skill'] - latest['new_uncertainty']
                                print(f"        {game_type}: {rating:.2f} (skill: {latest['new_skill']:.2f}, uncertainty: {latest['new_uncertainty']:.2f})")
                    else:
                        print(f"      ❌ No ranking data found (no skill/uncertainty values)")
                        
        else:
            print(f"   ❌ Player '{player_name}' not found in players database")
    else:
        print("   ❌ Raw match data files not found")

    # Check if there are similar names that might be causing confusion
    print(f"\n4. Similar Names Check:")
    if 'df_current' in locals() and not df_current.empty:
        # Look for names that contain "praedyth" or similar (case-insensitive)
        similar_names = df_current[df_current['name'].str.contains('praedyth', case=False, na=False)]['name'].unique()
        if len(similar_names) > 0:
            print(f"   Found names containing 'praedyth': {list(similar_names)}")
        else:
            print("   No names containing 'praedyth' found")
            
        # Also check for names that might be variations
        variations = ['prady', 'prae', 'dyth']
        for variation in variations:
            variant_names = df_current[df_current['name'].str.contains(variation, case=False, na=False)]['name'].unique()
            if len(variant_names) > 0:
                print(f"   Found names containing '{variation}': {list(variant_names)}")

if __name__ == "__main__":
    search_praedyth()
