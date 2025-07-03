#!/usr/bin/env python3
"""
Replay Gap Filler
==================

Specifically processes replay JSON files to find and fill gaps in the datamart.
This script identifies replays that exist as JSON files but are missing from 
the datamart, then processes them to enhance the dataset.
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

class ReplayGapFiller:
    """Fills gaps in datamart using replay JSON files."""
    
    def __init__(self):
        self.logger = setup_logging("ReplayGapFiller", logging.INFO)
        self.data_dir = config.paths.data_dir
        self.replays_dir = self.data_dir / "replays"
        
    def find_missing_replays(self, max_check: int = 100) -> List[str]:
        """Find replay files that aren't in the datamart."""
        self.logger.info("Scanning for missing replays...")
        
        # Load existing datamart
        try:
            matches_df = pd.read_parquet(self.data_dir / "matches.parquet")
            existing_replay_ids = set(matches_df['replay_id'].dropna().astype(str))
            self.logger.info(f"Found {len(existing_replay_ids)} replays in datamart")
        except Exception as e:
            self.logger.warning(f"Could not load matches datamart: {e}")
            existing_replay_ids = set()
        
        # Check replay files
        if not self.replays_dir.exists():
            self.logger.error("Replays directory not found!")
            return []
        
        all_replay_files = list(self.replays_dir.glob("*.json"))
        self.logger.info(f"Found {len(all_replay_files)} total replay JSON files")
        
        # Find missing ones (check first max_check files for demo)
        missing_replays = []
        checked_count = 0
        
        for replay_file in all_replay_files:
            if checked_count >= max_check:
                break
                
            replay_id = replay_file.stem  # filename without .json
            if replay_id not in existing_replay_ids:
                missing_replays.append(replay_id + ".json")
            
            checked_count += 1
        
        self.logger.info(f"Found {len(missing_replays)} missing replays in first {checked_count} files checked")
        return missing_replays[:10]  # Return first 10 for demo
    
    def process_missing_replay(self, replay_id: str) -> Dict[str, Any]:
        """Process a single missing replay file."""
        replay_file = self.replays_dir / replay_id
        
        if not replay_file.exists():
            self.logger.error(f"Replay file not found: {replay_file}")
            return {}
        
        try:
            with open(replay_file, 'r', encoding='utf-8') as f:
                replay_data = json.load(f)
            
            # Extract key information
            processed_data = {
                'replay_id': replay_data.get('id'),
                'file_name': replay_data.get('fileName'),
                'start_time': replay_data.get('startTime'),
                'duration_ms': replay_data.get('durationMs'),
                'engine_version': replay_data.get('engineVersion'),
                'game_version': replay_data.get('gameVersion'),
                'map_name': replay_data.get('hostSettings', {}).get('mapname'),
                'server_match_id': replay_data.get('hostSettings', {}).get('server_match_id'),
                'is_ranked': replay_data.get('gameSettings', {}).get('ranked_game') == '1',
                'players': [],
                'ally_teams': len(replay_data.get('AllyTeams', [])),
                'spectators': len(replay_data.get('Spectators', []))
            }
            
            # Extract player information
            if 'AllyTeams' in replay_data:
                for ally_team_idx, ally_team in enumerate(replay_data['AllyTeams']):
                    is_winning_team = ally_team.get('winningTeam', False)
                    
                    if 'Players' in ally_team:
                        for player in ally_team['Players']:
                            player_info = {
                                'user_id': player.get('userId'),
                                'name': player.get('name'),
                                'country': player.get('countryCode'),
                                'team_id': player.get('teamId'),
                                'ally_team_id': ally_team_idx,
                                'faction': player.get('faction'),
                                'rank': player.get('rank'),
                                'skill': player.get('skill'),
                                'skill_uncertainty': player.get('skillUncertainty'),
                                'won_game': is_winning_team,
                                'start_pos': player.get('startPos'),
                                'clan_id': player.get('clanId')
                            }
                            processed_data['players'].append(player_info)
            
            # Extract awards/statistics if available
            if 'awards' in replay_data:
                processed_data['awards'] = {
                    'econ_destroyed': replay_data['awards'].get('econDestroyed', []),
                    'fighting_units_destroyed': replay_data['awards'].get('fightingUnitsDestroyed', []),
                    'resource_efficiency': replay_data['awards'].get('resourceEfficiency', []),
                    'most_resources_produced': replay_data['awards'].get('mostResourcesProduced', {}),
                    'most_damage_taken': replay_data['awards'].get('mostDamageTaken', {})
                }
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Error processing replay {replay_id}: {e}")
            return {}
    
    def create_gap_filling_report(self, missing_replays: List[str]) -> pd.DataFrame:
        """Create a report of what data we can extract from missing replays."""
        report_data = []
        
        self.logger.info(f"Processing {len(missing_replays)} missing replay files...")
        
        for replay_file in missing_replays:
            replay_id = replay_file.replace('.json', '')
            self.logger.info(f"Processing {replay_id}...")
            
            processed = self.process_missing_replay(replay_file)
            
            if processed:
                # Create summary row
                summary = {
                    'replay_id': processed['replay_id'],
                    'file_name': processed['file_name'],
                    'start_time': processed['start_time'],
                    'duration_minutes': processed['duration_ms'] / 60000 if processed['duration_ms'] else None,
                    'map_name': processed['map_name'],
                    'is_ranked': processed['is_ranked'],
                    'player_count': len(processed['players']),
                    'team_count': processed['ally_teams'],
                    'spectator_count': processed['spectators'],
                    'engine_version': processed['engine_version'],
                    'game_version': processed['game_version']
                }
                
                # Add player information
                if processed['players']:
                    player_names = [p['name'] for p in processed['players'] if p['name']]
                    summary['players_list'] = ', '.join(player_names[:4]) + ('...' if len(player_names) > 4 else '')
                    
                    countries = list(set([p['country'] for p in processed['players'] if p['country']]))
                    summary['countries'] = ', '.join(countries)
                    
                    # Determine winner
                    winners = [p['name'] for p in processed['players'] if p.get('won_game')]
                    summary['winners'] = ', '.join(winners) if winners else 'Unknown'
                
                report_data.append(summary)
        
        if report_data:
            return pd.DataFrame(report_data)
        else:
            return pd.DataFrame()
    
    def demonstrate_gap_filling(self):
        """Demonstrate the gap-filling capability."""
        self.logger.info("=== Replay Gap Filling Demonstration ===")
        
        # Find missing replays
        missing_replays = self.find_missing_replays(max_check=50)
        
        if not missing_replays:
            self.logger.info("No missing replays found in sample check!")
            return
        
        # Create report
        report_df = self.create_gap_filling_report(missing_replays)
        
        if report_df.empty:
            self.logger.warning("No data extracted from missing replays")
            return
        
        # Display results
        self.logger.info(f"\n=== Gap Filling Results ===")
        self.logger.info(f"Successfully processed {len(report_df)} missing replay files")
        
        # Show some examples
        self.logger.info(f"\n=== Sample Missing Replay Data ===")
        for idx, row in report_df.head(5).iterrows():
            self.logger.info(f"\nReplay: {row['replay_id']}")
            self.logger.info(f"  Date: {row['start_time']}")
            self.logger.info(f"  Map: {row['map_name']}")
            self.logger.info(f"  Duration: {row['duration_minutes']:.1f} min" if row['duration_minutes'] else "  Duration: Unknown")
            self.logger.info(f"  Players ({row['player_count']}): {row.get('players_list', 'Unknown')}")
            self.logger.info(f"  Countries: {row.get('countries', 'Unknown')}")
            self.logger.info(f"  Winner(s): {row.get('winners', 'Unknown')}")
            self.logger.info(f"  Ranked: {'Yes' if row['is_ranked'] else 'No'}")
        
        # Save report
        try:
            report_df.to_csv(self.data_dir / "gap_filling_report.csv", index=False)
            self.logger.info(f"\nGap filling report saved to: {self.data_dir / 'gap_filling_report.csv'}")
        except Exception as e:
            self.logger.error(f"Error saving report: {e}")
        
        # Statistics
        ranked_games = report_df[report_df['is_ranked'] == True]
        self.logger.info(f"\n=== Statistics from Missing Replays ===")
        self.logger.info(f"Total games found: {len(report_df)}")
        self.logger.info(f"Ranked games: {len(ranked_games)}")
        self.logger.info(f"Unranked games: {len(report_df) - len(ranked_games)}")
        
        if len(report_df) > 0:
            avg_duration = report_df['duration_minutes'].mean()
            self.logger.info(f"Average game duration: {avg_duration:.1f} minutes")
            
            total_players = report_df['player_count'].sum()
            self.logger.info(f"Total player-game records: {total_players}")
        
        return report_df


def main():
    """Main execution function."""
    filler = ReplayGapFiller()
    report = filler.demonstrate_gap_filling()
    return report

if __name__ == "__main__":
    main()
