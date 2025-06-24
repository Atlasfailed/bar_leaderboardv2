#!/usr/bin/env python3
"""
BAR Leaderboard Data Validation
===============================

Comprehensive data validation utilities for all pipeline inputs and outputs.
Ensures data quality and consistency across the entire system.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import logging
from datetime import datetime, timedelta

from config import config
from utils import setup_logging

class DataValidator:
    """Comprehensive data validation for BAR leaderboard pipelines."""
    
    def __init__(self):
        self.logger = setup_logging(self.__class__.__name__)
        self.validation_results: List[Dict[str, Any]] = []
    
    def validate_datamart_inputs(self, raw_data: Dict[str, pd.DataFrame]) -> bool:
        """Validate raw datamart inputs."""
        self.logger.info("🔍 Validating datamart inputs...")
        
        all_valid = True
        
        # Validate matches data
        if not self._validate_matches_data(raw_data.get('matches')):
            all_valid = False
        
        # Validate match_players data
        if not self._validate_match_players_data(raw_data.get('match_players')):
            all_valid = False
        
        # Validate players data
        if not self._validate_players_data(raw_data.get('players')):
            all_valid = False
        
        # Cross-validation between datasets
        if not self._validate_data_consistency(raw_data):
            all_valid = False
        
        return all_valid
    
    def validate_pipeline_output(self, data: pd.DataFrame, pipeline_type: str) -> bool:
        """Validate pipeline output data."""
        self.logger.info(f"🔍 Validating {pipeline_type} pipeline output...")
        
        if pipeline_type == 'leaderboard':
            return self._validate_leaderboard_output(data)
        elif pipeline_type == 'nation_rankings':
            return self._validate_nation_rankings_output(data)
        elif pipeline_type == 'team_analysis':
            return self._validate_team_analysis_output(data)
        else:
            self.logger.error(f"Unknown pipeline type: {pipeline_type}")
            return False
    
    def _validate_matches_data(self, matches_df: Optional[pd.DataFrame]) -> bool:
        """Validate matches dataframe."""
        if matches_df is None:
            self._add_error("matches", "DataFrame is None")
            return False
        
        required_columns = ['match_id', 'start_time', 'game_type', 'is_ranked', 'winning_team']
        missing_columns = [col for col in required_columns if col not in matches_df.columns]
        
        if missing_columns:
            self._add_error("matches", f"Missing columns: {missing_columns}")
            return False
        
        # Check for reasonable data ranges
        if matches_df['match_id'].duplicated().any():
            self._add_error("matches", "Duplicate match IDs found")
            return False
        
        # Validate date range (should be within last 5 years)
        if 'start_time' in matches_df.columns:
            matches_df['start_time'] = pd.to_datetime(matches_df['start_time'])
            min_date = datetime.now() - timedelta(days=5*365)
            old_matches = matches_df[matches_df['start_time'] < min_date]
            if len(old_matches) / len(matches_df) > 0.5:
                self._add_warning("matches", f"Many matches older than 5 years: {len(old_matches):,}")
        
        # Validate game types
        valid_game_types = ['Duel', 'Small Team', 'Large Team', 'Team', 'FFA', 'Team FFA', 'Coop', 'Custom']
        invalid_types = matches_df[~matches_df['game_type'].isin(valid_game_types)]['game_type'].unique()
        if len(invalid_types) > 0:
            self._add_warning("matches", f"Unknown game types: {list(invalid_types)}")
        
        self._add_success("matches", f"Validated {len(matches_df):,} match records")
        return True
    
    def _validate_match_players_data(self, match_players_df: Optional[pd.DataFrame]) -> bool:
        """Validate match_players dataframe."""
        if match_players_df is None:
            self._add_error("match_players", "DataFrame is None")
            return False
        
        required_columns = ['match_id', 'user_id', 'team_id']
        missing_columns = [col for col in required_columns if col not in match_players_df.columns]
        
        if missing_columns:
            self._add_error("match_players", f"Missing columns: {missing_columns}")
            return False
        
        # Check for null values in critical columns
        null_counts = match_players_df[required_columns].isnull().sum()
        for col, null_count in null_counts.items():
            if null_count > 0:
                self._add_warning("match_players", f"Null values in {col}: {null_count:,}")
        
        # Validate team IDs (should be 0 or 1 for most game types)
        invalid_teams = match_players_df[~match_players_df['team_id'].isin([0, 1, 2, 3, 4, 5])]
        if len(invalid_teams) > 0:
            self._add_warning("match_players", f"Unusual team IDs found: {invalid_teams['team_id'].unique()}")
        
        self._add_success("match_players", f"Validated {len(match_players_df):,} player-match records")
        return True
    
    def _validate_players_data(self, players_df: Optional[pd.DataFrame]) -> bool:
        """Validate players dataframe."""
        if players_df is None:
            self._add_error("players", "DataFrame is None")
            return False
        
        required_columns = ['user_id']
        missing_columns = [col for col in required_columns if col not in players_df.columns]
        
        if missing_columns:
            self._add_error("players", f"Missing columns: {missing_columns}")
            return False
        
        # Check for duplicate user IDs
        if players_df['user_id'].duplicated().any():
            self._add_error("players", "Duplicate user IDs found")
            return False
        
        # Validate country codes if present
        if 'countryCode' in players_df.columns:
            country_codes = players_df['countryCode'].dropna()
            invalid_codes = country_codes[country_codes.str.len() != 2]
            if len(invalid_codes) > 0:
                self._add_warning("players", f"Invalid country codes: {len(invalid_codes):,} entries")
        
        self._add_success("players", f"Validated {len(players_df):,} player records")
        return True
    
    def _validate_data_consistency(self, raw_data: Dict[str, pd.DataFrame]) -> bool:
        """Validate consistency between datasets."""
        matches_df = raw_data.get('matches')
        match_players_df = raw_data.get('match_players')
        players_df = raw_data.get('players')
        
        if not all([matches_df is not None, match_players_df is not None, players_df is not None]):
            return True  # Skip if any dataset is missing
        
        # Check match ID consistency
        match_ids_matches = set(matches_df['match_id'])
        match_ids_players = set(match_players_df['match_id'])
        
        orphaned_player_matches = match_ids_players - match_ids_matches
        if orphaned_player_matches:
            self._add_warning("consistency", f"Match IDs in match_players but not in matches: {len(orphaned_player_matches):,}")
        
        # Check user ID consistency
        user_ids_players = set(players_df['user_id'])
        user_ids_matches = set(match_players_df['user_id'])
        
        orphaned_match_players = user_ids_matches - user_ids_players
        if orphaned_match_players:
            self._add_warning("consistency", f"User IDs in match_players but not in players: {len(orphaned_match_players):,}")
        
        self._add_success("consistency", "Cross-dataset validation completed")
        return True
    
    def _validate_leaderboard_output(self, leaderboard_df: pd.DataFrame) -> bool:
        """Validate leaderboard pipeline output."""
        required_columns = ['user_id', 'name', 'countryCode', 'leaderboard_rating', 'game_type', 'rank']
        missing_columns = [col for col in required_columns if col not in leaderboard_df.columns]
        
        if missing_columns:
            self._add_error("leaderboard_output", f"Missing columns: {missing_columns}")
            return False
        
        # Validate ratings are reasonable
        if 'leaderboard_rating' in leaderboard_df.columns:
            ratings = leaderboard_df['leaderboard_rating']
            if ratings.min() < 0 or ratings.max() > 5000:
                self._add_warning("leaderboard_output", f"Unusual rating range: {ratings.min():.0f} to {ratings.max():.0f}")
        
        # Validate ranks are sequential
        for game_type in leaderboard_df['game_type'].unique():
            game_data = leaderboard_df[leaderboard_df['game_type'] == game_type]
            for leaderboard_id in game_data['leaderboard_id'].unique():
                lb_data = game_data[game_data['leaderboard_id'] == leaderboard_id]
                ranks = sorted(lb_data['rank'].unique())
                if ranks != list(range(1, len(ranks) + 1)):
                    self._add_warning("leaderboard_output", f"Non-sequential ranks in {game_type}/{leaderboard_id}")
        
        self._add_success("leaderboard_output", f"Validated {len(leaderboard_df):,} leaderboard entries")
        return True
    
    def _validate_nation_rankings_output(self, rankings_df: pd.DataFrame) -> bool:
        """Validate nation rankings pipeline output."""
        required_columns = ['countryCode', 'country_name', 'game_type', 'rank', 'total_score']
        missing_columns = [col for col in required_columns if col not in rankings_df.columns]
        
        if missing_columns:
            self._add_error("nation_rankings_output", f"Missing columns: {missing_columns}")
            return False
        
        # Validate scores are reasonable
        if 'total_score' in rankings_df.columns:
            scores = rankings_df['total_score']
            if scores.min() < -10000 or scores.max() > 10000:
                self._add_warning("nation_rankings_output", f"Unusual score range: {scores.min():.0f} to {scores.max():.0f}")
        
        self._add_success("nation_rankings_output", f"Validated {len(rankings_df):,} nation ranking entries")
        return True
    
    def _validate_team_analysis_output(self, teams_df: pd.DataFrame) -> bool:
        """Validate team analysis pipeline output."""
        # Team analysis output structure may vary, so basic validation
        if len(teams_df) == 0:
            self._add_warning("team_analysis_output", "No team analysis results generated")
            return True
        
        self._add_success("team_analysis_output", f"Validated {len(teams_df):,} team analysis entries")
        return True
    
    def _add_error(self, component: str, message: str) -> None:
        """Add an error to validation results."""
        self.validation_results.append({
            'level': 'ERROR',
            'component': component,
            'message': message,
            'timestamp': datetime.now()
        })
        self.logger.error(f"❌ {component}: {message}")
    
    def _add_warning(self, component: str, message: str) -> None:
        """Add a warning to validation results."""
        self.validation_results.append({
            'level': 'WARNING',
            'component': component,
            'message': message,
            'timestamp': datetime.now()
        })
        self.logger.warning(f"⚠️  {component}: {message}")
    
    def _add_success(self, component: str, message: str) -> None:
        """Add a success message to validation results."""
        self.validation_results.append({
            'level': 'SUCCESS',
            'component': component,
            'message': message,
            'timestamp': datetime.now()
        })
        self.logger.info(f"✅ {component}: {message}")
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get a summary of all validation results."""
        errors = [r for r in self.validation_results if r['level'] == 'ERROR']
        warnings = [r for r in self.validation_results if r['level'] == 'WARNING']
        successes = [r for r in self.validation_results if r['level'] == 'SUCCESS']
        
        return {
            'total_checks': len(self.validation_results),
            'errors': len(errors),
            'warnings': len(warnings),
            'successes': len(successes),
            'is_valid': len(errors) == 0,
            'details': self.validation_results
        }
    
    def print_validation_summary(self) -> None:
        """Print a formatted validation summary."""
        summary = self.get_validation_summary()
        
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 VALIDATION SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"✅ Successes: {summary['successes']}")
        self.logger.info(f"⚠️  Warnings: {summary['warnings']}")
        self.logger.info(f"❌ Errors: {summary['errors']}")
        self.logger.info(f"🎯 Overall Status: {'VALID' if summary['is_valid'] else 'INVALID'}")
        
        if summary['errors'] > 0:
            self.logger.info("\n❌ ERRORS:")
            for result in self.validation_results:
                if result['level'] == 'ERROR':
                    self.logger.info(f"  • {result['component']}: {result['message']}")
        
        if summary['warnings'] > 0:
            self.logger.info("\n⚠️  WARNINGS:")
            for result in self.validation_results:
                if result['level'] == 'WARNING':
                    self.logger.info(f"  • {result['component']}: {result['message']}")

# Global validator instance
data_validator = DataValidator()
