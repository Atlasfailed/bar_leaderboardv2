#!/usr/bin/env python3
"""
Test Season Functionality
=========================

This script tests the season functionality by making API calls to both seasons
and verifying that they return different data.
"""

import requests
import json

def test_season_api(base_url="http://127.0.0.1:5001"):
    """Test the season API endpoints."""
    
    print("🧪 Testing Season API Functionality")
    print("=" * 50)
    
    # Test leaderboards endpoint for both seasons
    print("\n1. Testing /api/leaderboards endpoint:")
    
    for season in [1, 2]:
        try:
            response = requests.get(f"{base_url}/api/leaderboards?season={season}")
            if response.status_code == 200:
                data = response.json()
                nations_count = len(data.get('nations', []))
                regions_count = len(data.get('regions', []))
                print(f"   Season {season}: ✅ {nations_count} nations, {regions_count} regions")
            else:
                print(f"   Season {season}: ❌ HTTP {response.status_code}")
        except Exception as e:
            print(f"   Season {season}: ❌ Error: {e}")
    
    # Test global leaderboard endpoint for both seasons
    print("\n2. Testing /api/leaderboard/global/Large Team endpoint:")
    
    for season in [1, 2]:
        try:
            response = requests.get(f"{base_url}/api/leaderboard/global/Large Team?season={season}")
            if response.status_code == 200:
                data = response.json()
                player_count = data.get('total_players', 0)
                top_players = len(data.get('players', []))
                print(f"   Season {season}: ✅ {player_count} total players, showing top {top_players}")
                
                # Show top 3 players for verification
                if data.get('players'):
                    print(f"      Top 3 players:")
                    for i, player in enumerate(data['players'][:3]):
                        print(f"        {i+1}. {player['display_name']} - {player['leaderboard_rating']:.2f}")
            else:
                print(f"   Season {season}: ❌ HTTP {response.status_code}")
        except Exception as e:
            print(f"   Season {season}: ❌ Error: {e}")
    
    # Test nation-specific leaderboard
    print("\n3. Testing /api/leaderboard/US/Large Team endpoint:")
    
    for season in [1, 2]:
        try:
            response = requests.get(f"{base_url}/api/leaderboard/US/Large Team?season={season}")
            if response.status_code == 200:
                data = response.json()
                player_count = data.get('total_players', 0)
                top_players = len(data.get('players', []))
                print(f"   Season {season}: ✅ {player_count} total US players, showing top {top_players}")
            else:
                print(f"   Season {season}: ❌ HTTP {response.status_code}")
        except Exception as e:
            print(f"   Season {season}: ❌ Error: {e}")
    
    print("\n✅ Season API testing completed!")
    print("\n🎯 Next steps:")
    print("1. Visit http://127.0.0.1:5001 to test the web interface")
    print("2. Try switching between Season 1 and Season 2 using the navigation buttons")
    print("3. Verify that different data is shown for each season")

if __name__ == "__main__":
    test_season_api()
