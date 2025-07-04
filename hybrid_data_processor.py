#!/usr/bin/env python3
"""
Hybrid Data Processor
====================

Processes ranking data by starting with datamart files and filling gaps with replay JSON data.
This approach combines the efficiency of pre-processed datamart files with the completeness
of raw replay data to create comprehensive rankings.
"""

import os
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import logging
from datetime import datetime
import numpy as np

from config import config
from utils import setup_logging

class HybridDataProcessor:
    """Processes data using both datamart files and replay JSONs."""
    
    def __init__(self):
        self.logger = setup_logging("HybridDataProcessor", logging.INFO)
        self.data_dir = config.paths.data_dir
        self.replays_dir = self.data_dir / "replays"
        
        # Datamart files
        self.players_df = None
        self.matches_df = None
        self.match_players_df = None
        
        # Processed data
        self.enhanced_players = {}
        self.enhanced_matches = {}
        self.replay_cache = {}
        
    def load_datamart_files(self):
        """Load existing datamart files."""
        self.logger.info("Loading datamart files...")
        
        try:
            # Load core datamart files
            self.players_df = pd.read_parquet(self.data_dir / "players.parquet")
            self.matches_df = pd.read_parquet(self.data_dir / "matches.parquet")
            self.match_players_df = pd.read_parquet(self.data_dir / "match_players.parquet")
            
            self.logger.info(f"Loaded {len(self.players_df)} players")
            self.logger.info(f"Loaded {len(self.matches_df)} matches")
            self.logger.info(f"Loaded {len(self.match_players_df)} match-player records")
            
        except Exception as e:
            self.logger.error(f"Error loading datamart files: {e}")
            # Initialize empty dataframes if files don't exist
            self.players_df = pd.DataFrame(columns=['user_id', 'name', 'country'])
            self.matches_df = pd.DataFrame(columns=['match_id', 'start_time', 'map', 'replay_id'])
            self.match_players_df = pd.DataFrame(columns=['match_id', 'user_id', 'team_id'])
    
    def get_sample_replay_files(self, limit: int = 5) -> List[str]:
        """Get a sample of replay files for processing."""
        replay_files = []
        if self.replays_dir.exists():
            all_files = list(self.replays_dir.glob("*.json"))
            replay_files = [f.name for f in all_files[:limit]]
            self.logger.info(f"Found {len(all_files)} total replay files, using {len(replay_files)} as sample")
        return replay_files
    
    def load_replay_json(self, replay_id: str) -> Optional[Dict[str, Any]]:
        """Load a single replay JSON file."""
        if replay_id in self.replay_cache:
            return self.replay_cache[replay_id]
        
        replay_file = self.replays_dir / f"{replay_id}.json"
        if not replay_file.exists():
            self.logger.warning(f"Replay file not found: {replay_file}")
            return None
        
        try:
            with open(replay_file, 'r', encoding='utf-8') as f:
                replay_data = json.load(f)
                self.replay_cache[replay_id] = replay_data
                return replay_data
        except Exception as e:
            self.logger.error(f"Error loading replay {replay_id}: {e}")
            return None
    
    def extract_players_from_replay(self, replay_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract player information from replay JSON."""
        players = []
        
        if 'AllyTeams' not in replay_data:
            return players
        
        for ally_team in replay_data['AllyTeams']:
            if 'Players' in ally_team:
                for player in ally_team['Players']:
                    player_info = {
                        'user_id': player.get('userId'),
                        'name': player.get('name'),
                        'country': player.get('countryCode'),
                        'team_id': player.get('teamId'),
                        'ally_team_id': player.get('allyTeamId'),
                        'faction': player.get('faction'),
                        'rank': player.get('rank'),
                        'skill': player.get('skill'),
                        'skill_uncertainty': player.get('skillUncertainty'),
                        'clan_id': player.get('clanId'),
                        'start_pos': player.get('startPos')
                    }
                    players.append(player_info)
        
        return players
    
    def extract_match_info_from_replay(self, replay_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract match information from replay JSON."""
        match_info = {
            'replay_id': replay_data.get('id'),
            'start_time': replay_data.get('startTime'),
            'duration_ms': replay_data.get('durationMs'),
            'map': replay_data.get('hostSettings', {}).get('mapname'),
            'engine_version': replay_data.get('engineVersion'),
            'game_version': replay_data.get('gameVersion'),
            'team_count': len(replay_data.get('AllyTeams', [])),
            'is_ranked': replay_data.get('gameSettings', {}).get('ranked_game') == '1',
            'winning_team': self._determine_winning_team(replay_data),
            'server_match_id': replay_data.get('hostSettings', {}).get('server_match_id')
        }
        return match_info
    
    def _determine_winning_team(self, replay_data: Dict[str, Any]) -> Optional[int]:
        """Determine the winning team from replay data."""
        if 'AllyTeams' not in replay_data:
            return None
        
        for i, ally_team in enumerate(replay_data['AllyTeams']):
            if ally_team.get('winningTeam', False):
                return i
        
        return None
    
    def enhance_datamart_with_replays(self, sample_replays: List[str]):
        """Enhance datamart data with information from replay JSONs."""
        self.logger.info("Enhancing datamart with replay data...")
        
        new_players = []
        new_matches = []
        new_match_players = []
        
        existing_replay_ids = set(self.matches_df['replay_id'].dropna().astype(str))
        existing_user_ids = set(self.players_df['user_id'])
        
        for replay_file in sample_replays:
            replay_id = replay_file.replace('.json', '')
            
            # Skip if already in datamart
            if replay_id in existing_replay_ids:
                self.logger.debug(f"Replay {replay_id} already in datamart, skipping")
                continue
            
            replay_data = self.load_replay_json(replay_id)
            if not replay_data:
                continue
            
            self.logger.info(f"Processing replay {replay_id}")
            
            # Extract match info
            match_info = self.extract_match_info_from_replay(replay_data)
            new_matches.append(match_info)
            
            # Extract players
            replay_players = self.extract_players_from_replay(replay_data)
            
            for player in replay_players:
                user_id = player['user_id']
                if user_id and user_id not in existing_user_ids:
                    # Add new player to datamart
                    new_players.append({
                        'user_id': user_id,
                        'name': player['name'],
                        'country': player['country']
                    })
                    existing_user_ids.add(user_id)
                
                # Add match-player relationship
                if user_id:
                    new_match_players.append({
                        'match_id': match_info.get('server_match_id'),
                        'user_id': user_id,
                        'team_id': player['team_id'],
                        'ally_team_id': player['ally_team_id'],
                        'faction': player['faction'],
                        'rank': player['rank'],
                        'skill': player['skill'],
                        'replay_id': replay_id
                    })
        
        # Convert to DataFrames and append to existing data
        if new_players:
            new_players_df = pd.DataFrame(new_players)
            self.players_df = pd.concat([self.players_df, new_players_df], ignore_index=True)
            self.logger.info(f"Added {len(new_players)} new players")
        
        if new_matches:
            new_matches_df = pd.DataFrame(new_matches)
            # Convert start_time to datetime
            new_matches_df['start_time'] = pd.to_datetime(new_matches_df['start_time'])
            
            # Add to matches dataframe with proper columns
            for _, match in new_matches_df.iterrows():
                match_row = {
                    'match_id': match.get('server_match_id'),
                    'start_time': match['start_time'],
                    'map': match['map'],
                    'team_count': match['team_count'],
                    'game_type': 'Team' if match['team_count'] > 2 else 'Duel',
                    'winning_team': match['winning_team'],
                    'game_duration': match['duration_ms'] / 1000 if match['duration_ms'] else None,
                    'is_ranked': match['is_ranked'],
                    'replay_id': match['replay_id'],
                    'engine': match['engine_version'],
                    'game_version': match['game_version'],
                    'is_public': True
                }
                self.matches_df = pd.concat([self.matches_df, pd.DataFrame([match_row])], ignore_index=True)
            
            self.logger.info(f"Added {len(new_matches)} new matches")
        
        if new_match_players:
            new_match_players_df = pd.DataFrame(new_match_players)
            self.match_players_df = pd.concat([self.match_players_df, new_match_players_df], ignore_index=True)
            self.logger.info(f"Added {len(new_match_players)} new match-player records")
    
    def create_enhanced_leaderboard(self) -> pd.DataFrame:
        """Create leaderboard using enhanced data."""
        self.logger.info("Creating enhanced leaderboard...")
        
        # Merge match_players with matches to get game info
        match_data = self.match_players_df.merge(
            self.matches_df[['match_id', 'start_time', 'is_ranked', 'winning_team', 'replay_id']], 
            on='match_id', 
            how='left'
        )
        
        # Merge with players to get names
        player_stats = match_data.merge(
            self.players_df[['user_id', 'name', 'country']], 
            on='user_id', 
            how='left'
        )
        
        # Filter for ranked games only
        ranked_games = player_stats[player_stats['is_ranked'] == True]
        
        # Convert start_time to consistent timezone-naive format
        if 'start_time' in ranked_games.columns:
            ranked_games = ranked_games.copy()
            ranked_games['start_time'] = pd.to_datetime(ranked_games['start_time'], utc=True).dt.tz_localize(None)
        
        # Calculate basic stats with proper error handling
        try:
            leaderboard = ranked_games.groupby(['user_id', 'name', 'country']).agg({
                'match_id': 'count',  # Total games
                'start_time': lambda x: x.max() if len(x) > 0 and x.notna().any() else None  # Last game
            }).rename(columns={
                'match_id': 'total_games',
                'start_time': 'last_game'
            }).reset_index()
            
            # Calculate wins separately to avoid the lambda issue
            wins_data = []
            for user_id in ranked_games['user_id'].unique():
                user_games = ranked_games[ranked_games['user_id'] == user_id]
                wins = sum(user_games['team_id'] == user_games['winning_team'])
                wins_data.append({'user_id': user_id, 'wins': wins})
            
            wins_df = pd.DataFrame(wins_data)
            leaderboard = leaderboard.merge(wins_df, on='user_id', how='left')
            
        except Exception as e:
            self.logger.error(f"Error in groupby operation: {e}")
            # Fallback to simpler aggregation
            leaderboard = ranked_games.groupby(['user_id', 'name', 'country']).size().reset_index(name='total_games')
            leaderboard['wins'] = 0
            leaderboard['last_game'] = None
        
        # Calculate win rate and losses
        leaderboard['wins'] = leaderboard['wins'].fillna(0).astype(int)
        leaderboard['win_rate'] = leaderboard['wins'] / leaderboard['total_games']
        leaderboard['losses'] = leaderboard['total_games'] - leaderboard['wins']
        
        # Sort by wins, then by win rate
        leaderboard = leaderboard.sort_values(['wins', 'win_rate'], ascending=False)
        leaderboard['rank'] = range(1, len(leaderboard) + 1)
        
        return leaderboard
    
    def create_nation_rankings(self) -> pd.DataFrame:
        """Create nation rankings from enhanced data."""
        self.logger.info("Creating nation rankings...")
        
        # Merge match_players with matches and players
        match_data = self.match_players_df.merge(
            self.matches_df[['match_id', 'start_time', 'is_ranked', 'winning_team']], 
            on='match_id', 
            how='left'
        )
        
        player_match_data = match_data.merge(
            self.players_df[['user_id', 'name', 'country']], 
            on='user_id', 
            how='left'
        )
        
        # Filter for ranked games and valid countries
        ranked_games = player_match_data[
            (player_match_data['is_ranked'] == True) & 
            (player_match_data['country'].notna()) &
            (player_match_data['country'] != '')
        ]
        
        # Convert start_time to consistent format
        if 'start_time' in ranked_games.columns and len(ranked_games) > 0:
            ranked_games = ranked_games.copy()
            ranked_games['start_time'] = pd.to_datetime(ranked_games['start_time'], utc=True).dt.tz_localize(None)
        
        # Calculate nation stats
        nation_stats = []
        for country in ranked_games['country'].unique():
            country_games = ranked_games[ranked_games['country'] == country]
            
            # Calculate wins (where team_id matches winning_team)
            wins = sum(country_games['team_id'] == country_games['winning_team'])
            total_games = len(country_games)
            unique_players = country_games['user_id'].nunique()
            
            # Get last game time safely
            last_game = None
            if len(country_games) > 0 and 'start_time' in country_games.columns:
                last_game_series = country_games['start_time'].dropna()
                if len(last_game_series) > 0:
                    last_game = last_game_series.max()
            
            nation_stats.append({
                'country': country,
                'total_games': total_games,
                'wins': wins,
                'losses': total_games - wins,
                'win_rate': wins / total_games if total_games > 0 else 0,
                'unique_players': unique_players,
                'last_game': last_game
            })
        
        if not nation_stats:
            return pd.DataFrame(columns=['country', 'total_games', 'wins', 'losses', 'win_rate', 'unique_players', 'last_game', 'rank'])
        
        nation_rankings = pd.DataFrame(nation_stats)
        nation_rankings = nation_rankings.sort_values(['wins', 'win_rate'], ascending=False)
        nation_rankings['rank'] = range(1, len(nation_rankings) + 1)
        
        return nation_rankings
    
    def create_team_analysis(self) -> Dict[str, Any]:
        """Create team performance analysis from enhanced data."""
        self.logger.info("Creating team analysis...")
        
        # Merge all data
        match_data = self.match_players_df.merge(
            self.matches_df[['match_id', 'start_time', 'is_ranked', 'winning_team', 'team_count']], 
            on='match_id', 
            how='left'
        )
        
        player_match_data = match_data.merge(
            self.players_df[['user_id', 'name', 'country']], 
            on='user_id', 
            how='left'
        )
        
        # Filter for team games (more than 2 teams)
        team_games = player_match_data[
            (player_match_data['is_ranked'] == True) & 
            (player_match_data['team_count'] > 2)
        ]
        
        analysis = {
            'total_team_games': len(team_games['match_id'].unique()),
            'total_team_players': team_games['user_id'].nunique(),
            'average_players_per_game': len(team_games) / len(team_games['match_id'].unique()) if len(team_games) > 0 else 0,
            'team_size_distribution': team_games['team_count'].value_counts().to_dict(),
            'most_active_team_players': team_games['user_id'].value_counts().head(10).to_dict()
        }
        
        return analysis
    
    def create_efficiency_analysis(self) -> pd.DataFrame:
        """Create efficiency analysis comparing datamart vs replay data."""
        self.logger.info("Creating efficiency analysis...")
        
        # Count records by source
        datamart_replays = set(self.matches_df[self.matches_df['replay_id'].notna()]['replay_id'].astype(str))
        json_replays = set(self.replay_cache.keys())
        
        analysis_data = [{
            'data_source': 'Datamart Files',
            'total_matches': len(self.matches_df),
            'total_players': len(self.players_df),
            'total_match_players': len(self.match_players_df),
            'processing_speed': 'Very Fast',
            'data_completeness': 'High',
            'replay_coverage': len(datamart_replays)
        }, {
            'data_source': 'Replay JSONs',
            'total_matches': len(json_replays),
            'total_players': len(set(p['user_id'] for replay in self.replay_cache.values() 
                                   for p in self.extract_players_from_replay(replay) if p['user_id'])),
            'total_match_players': sum(len(self.extract_players_from_replay(replay)) 
                                     for replay in self.replay_cache.values()),
            'processing_speed': 'Slower',
            'data_completeness': 'Very High',
            'replay_coverage': len(json_replays)
        }, {
            'data_source': 'Hybrid Approach',
            'total_matches': len(self.matches_df),
            'total_players': len(self.players_df),
            'total_match_players': len(self.match_players_df),
            'processing_speed': 'Fast',
            'data_completeness': 'Very High',
            'replay_coverage': len(datamart_replays | json_replays)
        }]
        
        return pd.DataFrame(analysis_data)
    
    def generate_html_leaderboard(self, leaderboard: pd.DataFrame) -> str:
        """Generate HTML for the leaderboard page."""
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BAR Enhanced Leaderboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; text-align: center; margin-bottom: 30px; }}
        .stats {{ background: #e8f4f8; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        tr:hover {{ background-color: #e8f4f8; }}
        .rank {{ font-weight: bold; color: #2196F3; }}
        .win-rate {{ color: #4CAF50; font-weight: bold; }}
        .country {{ font-style: italic; color: #666; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🏆 BAR Enhanced Leaderboard</h1>
        <div class="stats">
            <p><strong>Enhanced Dataset Statistics:</strong></p>
            <p>• Total Players: {len(self.players_df):,}</p>
            <p>• Total Matches: {len(self.matches_df):,}</p>
            <p>• Ranked Players: {len(leaderboard):,}</p>
            <p>• Data Sources: Datamart + {len(self.replay_cache)} Replay JSONs</p>
            <p>• Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Player</th>
                    <th>Country</th>
                    <th>Wins</th>
                    <th>Losses</th>
                    <th>Total Games</th>
                    <th>Win Rate</th>
                    <th>Last Game</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for _, player in leaderboard.head(50).iterrows():
            last_game = player['last_game'].strftime('%Y-%m-%d') if pd.notna(player['last_game']) else 'N/A'
            html += f"""
                <tr>
                    <td class="rank">{player['rank']}</td>
                    <td><strong>{player['name']}</strong></td>
                    <td class="country">{player['country'] or 'Unknown'}</td>
                    <td>{player['wins']}</td>
                    <td>{player['losses']}</td>
                    <td>{player['total_games']}</td>
                    <td class="win-rate">{player['win_rate']:.1%}</td>
                    <td>{last_game}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
        
        <div style="margin-top: 30px; text-align: center; color: #666;">
            <p>Enhanced rankings using hybrid data processing (Datamart + Replay JSONs)</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def generate_html_nation_rankings(self, nation_rankings: pd.DataFrame) -> str:
        """Generate HTML for the nation rankings page."""
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BAR Nation Rankings</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; text-align: center; margin-bottom: 30px; }}
        .stats {{ background: #e8f4f8; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #FF6B35; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        tr:hover {{ background-color: #e8f4f8; }}
        .rank {{ font-weight: bold; color: #2196F3; }}
        .win-rate {{ color: #4CAF50; font-weight: bold; }}
        .country-flag {{ font-size: 1.2em; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌍 BAR Nation Rankings</h1>
        <div class="stats">
            <p><strong>Nation Statistics:</strong></p>
            <p>• Total Nations: {len(nation_rankings)}</p>
            <p>• Total Games Analyzed: {nation_rankings['total_games'].sum():,}</p>
            <p>• Most Active Nation: {nation_rankings.iloc[0]['country']} ({nation_rankings.iloc[0]['total_games']} games)</p>
            <p>• Data Sources: Enhanced Hybrid Dataset</p>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Nation</th>
                    <th>Wins</th>
                    <th>Losses</th>
                    <th>Total Games</th>
                    <th>Win Rate</th>
                    <th>Active Players</th>
                    <th>Last Game</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for _, nation in nation_rankings.head(30).iterrows():
            last_game = nation['last_game'].strftime('%Y-%m-%d') if pd.notna(nation['last_game']) else 'N/A'
            html += f"""
                <tr>
                    <td class="rank">{nation['rank']}</td>
                    <td><strong>{nation['country']}</strong></td>
                    <td>{nation['wins']}</td>
                    <td>{nation['losses']}</td>
                    <td>{nation['total_games']}</td>
                    <td class="win-rate">{nation['win_rate']:.1%}</td>
                    <td>{nation['unique_players']}</td>
                    <td>{last_game}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
        
        <div style="margin-top: 30px; text-align: center; color: #666;">
            <p>Nation rankings based on collective player performance</p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def save_html_pages(self, leaderboard: pd.DataFrame, nation_rankings: pd.DataFrame):
        """Save HTML ranking pages."""
        try:
            # Generate HTML content
            leaderboard_html = self.generate_html_leaderboard(leaderboard)
            nation_html = self.generate_html_nation_rankings(nation_rankings)
            
            # Save HTML files
            html_dir = self.data_dir / "html_rankings"
            html_dir.mkdir(exist_ok=True)
            
            with open(html_dir / "enhanced_leaderboard.html", 'w', encoding='utf-8') as f:
                f.write(leaderboard_html)
            
            with open(html_dir / "nation_rankings.html", 'w', encoding='utf-8') as f:
                f.write(nation_html)
            
            self.logger.info(f"HTML ranking pages saved to {html_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving HTML pages: {e}")
    
    def generate_comprehensive_report(self, leaderboard: pd.DataFrame, nation_rankings: pd.DataFrame, 
                                    team_analysis: Dict[str, Any], efficiency_analysis: pd.DataFrame):
        """Generate a comprehensive report of all processing results."""
        self.logger.info("=== Comprehensive Hybrid Data Processing Report ===")
        self.logger.info(f"Total players in enhanced dataset: {len(self.players_df):,}")
        self.logger.info(f"Total matches in enhanced dataset: {len(self.matches_df):,}")
        self.logger.info(f"Total match-player records: {len(self.match_players_df):,}")
        self.logger.info(f"Replays processed from JSON: {len(self.replay_cache)}")
        
        # Player Leaderboard Summary
        self.logger.info(f"\n=== Player Leaderboard ===")
        self.logger.info(f"Players with ranking data: {len(leaderboard):,}")
        if len(leaderboard) > 0:
            top_player = leaderboard.iloc[0]
            self.logger.info(f"Top player: {top_player['name']} ({top_player['country']}) - {top_player['wins']}W/{top_player['losses']}L")
        
        # Nation Rankings Summary
        self.logger.info(f"\n=== Nation Rankings ===")
        self.logger.info(f"Nations ranked: {len(nation_rankings)}")
        if len(nation_rankings) > 0:
            top_nation = nation_rankings.iloc[0]
            self.logger.info(f"Top nation: {top_nation['country']} - {top_nation['wins']}W ({top_nation['win_rate']:.1%})")
        
        # Team Analysis Summary
        self.logger.info(f"\n=== Team Analysis ===")
        self.logger.info(f"Total team games: {team_analysis['total_team_games']:,}")
        self.logger.info(f"Team players: {team_analysis['total_team_players']:,}")
        self.logger.info(f"Avg players per team game: {team_analysis['average_players_per_game']:.1f}")
        
        # Efficiency Analysis
        self.logger.info(f"\n=== Data Source Efficiency ===")
        for _, row in efficiency_analysis.iterrows():
            self.logger.info(f"{row['data_source']}: {row['total_matches']} matches, "
                           f"{row['total_players']} players, Speed: {row['processing_speed']}")
        
        # Show top 10 players
        self.logger.info("\n=== Top 10 Players (Enhanced Leaderboard) ===")
        top_10 = leaderboard.head(10)
        for _, player in top_10.iterrows():
            self.logger.info(
                f"{player['rank']:2d}. {player['name']:<20} "
                f"({player['country'] or 'Unknown':>3}) - "
                f"{player['wins']:3d}W/{player['losses']:3d}L "
                f"({player['win_rate']:.1%}) - "
                f"{player['total_games']} games"
            )
    
    def save_comprehensive_data(self, leaderboard: pd.DataFrame, nation_rankings: pd.DataFrame,
                              team_analysis: Dict[str, Any], efficiency_analysis: pd.DataFrame):
        """Save all comprehensive data to files."""
        try:
            # Save enhanced datamart files
            self.players_df.to_parquet(self.data_dir / "enhanced_players.parquet", index=False)
            self.matches_df.to_parquet(self.data_dir / "enhanced_matches.parquet", index=False)
            self.match_players_df.to_parquet(self.data_dir / "enhanced_match_players.parquet", index=False)
            
            # Save all ranking data (original enhanced format)
            leaderboard.to_parquet(self.data_dir / "enhanced_leaderboard.parquet", index=False)
            leaderboard.to_csv(self.data_dir / "enhanced_leaderboard.csv", index=False)
            
            # Create and save Flask-compatible leaderboard
            compatible_leaderboard = self.create_compatible_leaderboard()
            compatible_leaderboard.to_parquet(self.data_dir / "enhanced_final_leaderboard.parquet", index=False)
            self.logger.info("Saved Flask-compatible enhanced leaderboard")
            
            nation_rankings.to_parquet(self.data_dir / "enhanced_nation_rankings.parquet", index=False)
            nation_rankings.to_csv(self.data_dir / "enhanced_nation_rankings.csv", index=False)
            
            efficiency_analysis.to_parquet(self.data_dir / "efficiency_analysis.parquet", index=False)
            efficiency_analysis.to_csv(self.data_dir / "efficiency_analysis.csv", index=False)
            
            # Save team analysis as JSON
            with open(self.data_dir / "team_analysis_enhanced.json", 'w', encoding='utf-8') as f:
                json.dump(team_analysis, f, indent=2, default=str)
            
            self.logger.info("All comprehensive data saved successfully")
            
        except Exception as e:
            self.logger.error(f"Error saving comprehensive data: {e}")
    
    def run_hybrid_processing(self):
        """Main processing pipeline using hybrid approach."""
        self.logger.info("=== Starting Comprehensive Hybrid Data Processing ===")
        
        # Step 1: Load existing datamart
        self.load_datamart_files()
        
        # Step 2: Get sample replay files (increase for comprehensive analysis)
        sample_replays = self.get_sample_replay_files(limit=10)
        self.logger.info(f"Processing {len(sample_replays)} sample replay files")
        
        if not sample_replays:
            self.logger.warning("No replay files found for processing")
            return
        
        # Step 3: Enhance datamart with replay data
        self.enhance_datamart_with_replays(sample_replays)
        
        # Step 4: Create comprehensive rankings
        self.logger.info("Creating comprehensive rankings...")
        leaderboard = self.create_enhanced_leaderboard()
        nation_rankings = self.create_nation_rankings()
        team_analysis = self.create_team_analysis()
        efficiency_analysis = self.create_efficiency_analysis()
        
        # Step 5: Generate comprehensive report
        self.generate_comprehensive_report(leaderboard, nation_rankings, team_analysis, efficiency_analysis)
        
        # Step 6: Save all enhanced data
        self.save_comprehensive_data(leaderboard, nation_rankings, team_analysis, efficiency_analysis)
        
        # Step 7: Generate and save HTML pages
        self.save_html_pages(leaderboard, nation_rankings)
        
        results = {
            'leaderboard': leaderboard,
            'nation_rankings': nation_rankings,
            'team_analysis': team_analysis,
            'efficiency_analysis': efficiency_analysis
        }
        
        return results

    def create_compatible_leaderboard(self) -> pd.DataFrame:
        """Create a leaderboard compatible with the existing Flask app structure."""
        self.logger.info("Creating Flask-compatible enhanced leaderboard...")
        
        # Get the enhanced leaderboard
        enhanced_leaderboard = self.create_enhanced_leaderboard()
        
        # Load original leaderboard to understand the structure
        try:
            original_leaderboard = pd.read_parquet(self.data_dir / "final_leaderboard.parquet")
            self.logger.info(f"Original leaderboard structure: {original_leaderboard.columns.tolist()}")
        except Exception as e:
            self.logger.warning(f"Could not load original leaderboard for structure reference: {e}")
            return enhanced_leaderboard
        
        # Create compatible records for each game type and leaderboard combination
        compatible_records = []
        
        # Define game types and leaderboard types from original data
        game_types = ['Large Team', 'Small Team', 'Duel'] if len(original_leaderboard) > 0 else ['Large Team']
        leaderboard_types = ['global'] if len(original_leaderboard) > 0 else ['global']
        
        if len(original_leaderboard) > 0:
            game_types = original_leaderboard['game_type'].unique()
            leaderboard_types = original_leaderboard['leaderboard_id'].unique()
        
        # For each player in enhanced data, create records for each game type/leaderboard combo
        for _, player in enhanced_leaderboard.iterrows():
            for game_type in game_types:
                for leaderboard_id in leaderboard_types:
                    # Calculate leaderboard rating based on wins and win rate
                    # Higher rating for more wins and better win rate
                    base_rating = player['wins'] * 10
                    win_rate_bonus = player['win_rate'] * 1000
                    leaderboard_rating = base_rating + win_rate_bonus
                    
                    compatible_record = {
                        'user_id': player['user_id'],
                        'name': player['name'],
                        'countryCode': player['country'] if pd.notna(player['country']) else '',
                        'new_skill': 1500 + (player['wins'] * 0.5),  # Estimate skill based on wins
                        'new_uncertainty': max(10, 100 - (player['total_games'] * 0.1)),  # Lower uncertainty for more games
                        'start_time': player['last_game'] if pd.notna(player['last_game']) else pd.Timestamp.now(),
                        'leaderboard_rating': leaderboard_rating,
                        'leaderboard_id': leaderboard_id,
                        'game_type': game_type,
                        'rank': player['rank']
                    }
                    compatible_records.append(compatible_record)
        
        if not compatible_records:
            self.logger.warning("No compatible records created, returning enhanced leaderboard as-is")
            return enhanced_leaderboard
        
        compatible_df = pd.DataFrame(compatible_records)
        
        # Sort by leaderboard rating within each game type and leaderboard
        compatible_df = compatible_df.sort_values(['game_type', 'leaderboard_id', 'leaderboard_rating'], 
                                                 ascending=[True, True, False])
        
        # Recalculate ranks within each group
        compatible_df['rank'] = compatible_df.groupby(['game_type', 'leaderboard_id']).cumcount() + 1
        
        self.logger.info(f"Created {len(compatible_df)} compatible leaderboard records")
        return compatible_df

def main():
    """Main execution function."""
    processor = HybridDataProcessor()
    results = processor.run_hybrid_processing()
    return results

if __name__ == "__main__":
    main()
