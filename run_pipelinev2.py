#!/usr/bin/env python3
"""
BAR Leaderboard Pipeline v2
============================

Modernized pipeline for calculating player leaderboards from BAR data marts.
This script:
1. Downloads data from official BAR data marts
2. Processes leaderboard ratings for various game types and regions
3. Generates the final leaderboard file for the web application

Usage:
    python run_pipelinev2.py [--config CONFIG_FILE] [--environment ENV]
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging
import argparse

from config import config
from utils import setup_logging, data_loader, merge_player_data, filter_ranked_matches, safe_file_write

# Import optional enhancements (with fallbacks)
try:
    from performance_monitoring import performance_monitor
    PERFORMANCE_MONITORING_AVAILABLE = True
except ImportError:
    PERFORMANCE_MONITORING_AVAILABLE = False

try:
    from data_validation import data_validator
    DATA_VALIDATION_AVAILABLE = True
except ImportError:
    DATA_VALIDATION_AVAILABLE = False

try:
    from ..core.config_manager import config_manager
    CONFIG_MANAGER_AVAILABLE = True
except ImportError:
    CONFIG_MANAGER_AVAILABLE = False

# ==============================================================================
# --- Configuration ---
# ==============================================================================

# Game types to include in leaderboards
SUPPORTED_GAME_TYPES = [
    'Large Team', 'Small Team', 'Duel'
]

# Minimum games required for inclusion in leaderboards
MIN_GAMES_THRESHOLD = 5

# ==============================================================================
# --- Leaderboard Calculation ---
# ==============================================================================

class LeaderboardCalculator:
    """Handles leaderboard calculation and processing."""
    
    def __init__(self, min_games_threshold: int = MIN_GAMES_THRESHOLD, 
                 enable_monitoring: bool = True, enable_validation: bool = True):
        self.logger = setup_logging(self.__class__.__name__)
        config.setup_ssl()
        self.min_games_threshold = min_games_threshold
        self.enable_monitoring = enable_monitoring and PERFORMANCE_MONITORING_AVAILABLE
        self.enable_validation = enable_validation and DATA_VALIDATION_AVAILABLE
    
    def calculate_leaderboards(self) -> pd.DataFrame:
        """Main method to calculate all leaderboards."""
        if self.enable_monitoring:
            with performance_monitor.monitor_operation("Full Leaderboard Calculation"):
                return self._calculate_leaderboards_impl()
        else:
            return self._calculate_leaderboards_impl()
    
    def _calculate_leaderboards_impl(self) -> pd.DataFrame:
        """Implementation of the leaderboard calculation logic."""
        self.logger.info("Starting leaderboard calculation pipeline")
        
        # Load data
        data = self._load_and_prepare_data()
        
        # Calculate leaderboards
        leaderboards = []
        
        # Global leaderboards by game type
        for game_type in SUPPORTED_GAME_TYPES:
            self.logger.info(f"Calculating global leaderboard for: {game_type}")
            global_lb = self._calculate_global_leaderboard(data, game_type)
            if not global_lb.empty:
                leaderboards.append(global_lb)
        
        # Country-specific leaderboards
        countries = data['country'].dropna().unique()
        
        self.logger.info(f"Evaluating country leaderboards for {len(countries)} countries")
        
        country_leaderboard_count = 0
        for i, country in enumerate(countries, 1):
            if pd.isna(country) or len(str(country)) != 2:
                continue
            
            country_data = data[data['country'] == country]
            total_players = country_data['user_id'].nunique()
            
            # Skip countries with too few total players
            if total_players < 15:
                continue
            
            self.logger.info(f"Processing country {i}/{len(countries)}: {country} ({total_players} total players)")
            
            country_has_leaderboards = False
            for game_type in SUPPORTED_GAME_TYPES:
                # Check if this country-game type combination has enough qualified players
                game_data = country_data[country_data['game_type'] == game_type]
                if game_data.empty:
                    continue
                
                # Count qualified players for this game type
                player_game_counts = game_data['user_id'].value_counts()
                qualified_players = player_game_counts[player_game_counts >= self.min_games_threshold]
                
                if len(qualified_players) >= 15:  # Ensure 15+ qualified players per game mode
                    country_lb = self._calculate_country_leaderboard_optimized(country_data, country, game_type)
                    if not country_lb.empty:
                        leaderboards.append(country_lb)
                        country_has_leaderboards = True
            
            if country_has_leaderboards:
                country_leaderboard_count += 1
        
        self.logger.info(f"Generated country leaderboards for {country_leaderboard_count} countries")
        
        # Regional leaderboards based on sub-regions
        if 'sub_region' in data.columns and data['sub_region'].notna().any():
            regions = data['sub_region'].dropna().unique()
            # Filter to regions with sufficient players
            region_player_counts = data.groupby('sub_region')['user_id'].nunique()
            significant_regions = region_player_counts[region_player_counts >= 15].index
            
            self.logger.info(f"Calculating regional leaderboards for {len(significant_regions)} regions (of {len(regions)} total)")
            
            for i, region in enumerate(significant_regions, 1):
                if pd.isna(region) or region == '':
                    continue
                
                self.logger.info(f"Processing region {i}/{len(significant_regions)}: {region}")
                region_data = data[data['sub_region'] == region]
                
                for game_type in SUPPORTED_GAME_TYPES:
                    region_lb = self._calculate_regional_leaderboard(region_data, region, game_type)
                    if not region_lb.empty:
                        leaderboards.append(region_lb)
        
        # Combine all leaderboards
        if leaderboards:
            final_leaderboard = pd.concat(leaderboards, ignore_index=True)
            self.logger.info(f"Generated {len(final_leaderboard):,} leaderboard entries")
            return final_leaderboard
        else:
            self.logger.warning("No leaderboard data generated")
            return pd.DataFrame()
    
    def _load_and_prepare_data(self) -> pd.DataFrame:
        """Load and prepare data for leaderboard calculation."""
        
        if self.enable_monitoring:
            with performance_monitor.monitor_operation("Data Loading"):
                return self._load_and_prepare_data_impl()
        else:
            return self._load_and_prepare_data_impl()
    
    def _load_and_prepare_data_impl(self) -> pd.DataFrame:
        """Implementation of data loading and preparation."""
        self.logger.info("Loading BAR data marts...")
        
        # Load all datamart files
        raw_data = data_loader.load_datamart_data()
        
        # Validate input data if enabled
        if self.enable_validation:
            if not data_validator.validate_datamart_inputs(raw_data):
                self.logger.warning("Data validation found issues, but continuing...")
        
        self.logger.info("Manually merging data marts to ensure all columns are available for leaderboard calculation.")
        
        # Perform a full merge to get all necessary data in one frame, replacing the problematic generic utility.
        data = pd.merge(raw_data['match_players'], raw_data['matches'], on='match_id', how='left')
        data = pd.merge(data, raw_data['players'], on='user_id', how='left')

        # Load ISO country data for regional mapping
        iso_country_path = config.paths.data_dir / 'iso_country.csv'
        if iso_country_path.exists():
            self.logger.info("Loading ISO country data for regional mapping...")
            iso_countries = pd.read_csv(iso_country_path)
            # Map country codes to sub-regions
            country_region_map = iso_countries.set_index('alpha-2')['sub-region'].to_dict()
            data['sub_region'] = data['country'].map(country_region_map)
            self.logger.info(f"Mapped {data['sub_region'].notna().sum():,} records to sub-regions")
        else:
            self.logger.warning("ISO country data not found, regional rankings will be skipped")
            data['sub_region'] = None

        # Debug: Log available columns
        self.logger.info(f"Available columns after merge: {list(data.columns)}")
        
        # Filter for ranked matches
        self.logger.info("Filtering for ranked matches...")
        data = filter_ranked_matches(data, SUPPORTED_GAME_TYPES)
        
        self.logger.info(f"Prepared {len(data):,} match records for analysis")
        return data
    
    def _calculate_global_leaderboard(self, data: pd.DataFrame, game_type: str) -> pd.DataFrame:
        """Calculate global leaderboard for a specific game type."""
        game_data = data[data['game_type'] == game_type].copy()
        
        if game_data.empty:
            return pd.DataFrame()
        
        # Get latest rating for each player
        latest_ratings = (game_data
                         .sort_values('start_time')
                         .groupby('user_id')
                         .tail(1)
                         .copy())
        
        # Filter players with minimum games
        player_game_counts = game_data['user_id'].value_counts()
        qualified_players = player_game_counts[player_game_counts >= self.min_games_threshold].index
        
        latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
        
        if latest_ratings.empty:
            return pd.DataFrame()
        
        # Prepare leaderboard data
        leaderboard = latest_ratings[[
            'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
        ]].copy()
        
        # Calculate leaderboard rating as skill - uncertainty
        leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
        
        # Rename columns for consistency
        leaderboard = leaderboard.rename(columns={
            'country': 'countryCode'
        })
        
        leaderboard['leaderboard_id'] = 'global'
        leaderboard['game_type'] = game_type
        leaderboard['rank'] = leaderboard['leaderboard_rating'].rank(method='dense', ascending=False)
        
        return leaderboard
    
    def _calculate_country_leaderboard(self, data: pd.DataFrame, country: str, game_type: str) -> pd.DataFrame:
        """Calculate country-specific leaderboard for a game type."""
        country_data = data[
            (data['country'] == country) & 
            (data['game_type'] == game_type)
        ].copy()
        
        if country_data.empty:
            return pd.DataFrame()
        
        # Get latest rating for each player in this country
        latest_ratings = (country_data
                         .sort_values('start_time')
                         .groupby('user_id')
                         .tail(1)
                         .copy())
        
        # Filter players with minimum games
        player_game_counts = country_data['user_id'].value_counts()
        qualified_players = player_game_counts[player_game_counts >= self.min_games_threshold].index
        
        latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
        
        if len(latest_ratings) < 15:  # Need at least 15 qualified players for a country leaderboard
            return pd.DataFrame()
        
        # Prepare leaderboard data
        leaderboard = latest_ratings[[
            'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
        ]].copy()
        
        # Calculate leaderboard rating as skill - uncertainty
        leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
        
        # Rename columns for consistency
        leaderboard = leaderboard.rename(columns={
            'country': 'countryCode'
        })
        
        leaderboard['leaderboard_id'] = country
        leaderboard['game_type'] = game_type
        leaderboard['rank'] = leaderboard['leaderboard_rating'].rank(method='dense', ascending=False)
        
        return leaderboard
    
    def _calculate_country_leaderboard_optimized(self, country_data: pd.DataFrame, country: str, game_type: str) -> pd.DataFrame:
        """Calculate country-specific leaderboard for a game type (optimized version with pre-filtered data)."""
        game_data = country_data[country_data['game_type'] == game_type].copy()
        
        if game_data.empty:
            return pd.DataFrame()
        
        # Quick check: do we have enough data to bother?
        unique_players = game_data['user_id'].nunique()
        if unique_players < 3:
            return pd.DataFrame()
        
        # Get latest rating for each player in this country
        latest_ratings = (game_data
                         .sort_values('start_time')
                         .groupby('user_id')
                         .tail(1)
                         .copy())
        
        # Filter players with minimum games
        player_game_counts = game_data['user_id'].value_counts()
        qualified_players = player_game_counts[player_game_counts >= self.min_games_threshold].index
        
        latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
        
        if len(latest_ratings) < 15:  # Need at least 15 qualified players for a country leaderboard
            return pd.DataFrame()
        
        # Prepare leaderboard data
        leaderboard = latest_ratings[[
            'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
        ]].copy()
        
        # Calculate leaderboard rating as skill - uncertainty
        leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
        
        # Rename columns for consistency
        leaderboard = leaderboard.rename(columns={
            'country': 'countryCode'
        })
        
        leaderboard['leaderboard_id'] = country
        leaderboard['game_type'] = game_type
        leaderboard['rank'] = leaderboard['leaderboard_rating'].rank(method='dense', ascending=False)
        
        return leaderboard

    def _calculate_regional_leaderboard(self, region_data: pd.DataFrame, region: str, game_type: str) -> pd.DataFrame:
        """Calculate regional leaderboard for a game type based on sub-region."""
        game_data = region_data[region_data['game_type'] == game_type].copy()
        
        if game_data.empty:
            return pd.DataFrame()
        
        # Quick check: do we have enough data to bother?
        unique_players = game_data['user_id'].nunique()
        if unique_players < 5:  # Need at least 5 players for a regional leaderboard
            return pd.DataFrame()
        
        # Get latest rating for each player in this region
        latest_ratings = (game_data
                         .sort_values('start_time')
                         .groupby('user_id')
                         .tail(1)
                         .copy())
        
        # Filter players with minimum games
        player_game_counts = game_data['user_id'].value_counts()
        qualified_players = player_game_counts[player_game_counts >= self.min_games_threshold].index
        
        latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
        
        if len(latest_ratings) < 5:  # Need at least 5 qualified players for a regional leaderboard
            return pd.DataFrame()
        
        # Prepare leaderboard data
        leaderboard = latest_ratings[[
            'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
        ]].copy()
        
        # Calculate leaderboard rating as skill - uncertainty
        leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
        
        # Rename columns for consistency
        leaderboard = leaderboard.rename(columns={
            'country': 'countryCode'
        })
        
        # Use region name as leaderboard_id (without prefix to keep it clean)
        leaderboard['leaderboard_id'] = region
        leaderboard['game_type'] = game_type
        leaderboard['rank'] = leaderboard['leaderboard_rating'].rank(method='dense', ascending=False)
        
        return leaderboard

    def save_leaderboard(self, leaderboard_df: pd.DataFrame) -> None:
        """Save the final leaderboard to file."""
        if leaderboard_df.empty:
            self.logger.error("Cannot save empty leaderboard")
            return
        
        # Validate output data if enabled
        if self.enable_validation:
            data_validator.validate_pipeline_output(leaderboard_df, 'leaderboard')
        
        self.logger.info(f"Saving leaderboard with {len(leaderboard_df):,} entries...")
        safe_file_write(leaderboard_df, config.paths.final_leaderboard_parquet)
        self.logger.info(f"Leaderboard saved to: {config.paths.final_leaderboard_parquet}")
        
        # Log summary statistics
        summary = {
            'total_entries': len(leaderboard_df),
            'unique_players': leaderboard_df['user_id'].nunique(),
            'game_types': leaderboard_df['game_type'].nunique(),
            'countries': leaderboard_df['countryCode'].nunique()
        }
        
        self.logger.info(f"Leaderboard summary: {summary}")

# ==============================================================================
# --- Main Execution ---
# ==============================================================================

def main():
    """Main pipeline execution."""
    parser = argparse.ArgumentParser(description="BAR Leaderboard Pipeline v2")
    parser.add_argument(
        "--min-games",
        type=int,
        default=MIN_GAMES_THRESHOLD,
        help=f"Minimum games required for leaderboard inclusion (default: {MIN_GAMES_THRESHOLD})"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file (YAML or JSON)"
    )
    parser.add_argument(
        "--environment",
        type=str,
        default="development",
        help="Configuration environment (default: development)"
    )
    parser.add_argument(
        "--no-monitoring",
        action="store_true",
        help="Disable performance monitoring"
    )
    parser.add_argument(
        "--no-validation",
        action="store_true",
        help="Disable data validation"
    )
    args = parser.parse_args()

    # Load configuration if available
    if CONFIG_MANAGER_AVAILABLE and (args.config or args.environment != "development"):
        env_config = config_manager.load_configuration(args.config, args.environment)
        min_games = args.min_games or env_config.min_games_threshold
        enable_monitoring = not args.no_monitoring and env_config.enable_monitoring
        enable_validation = not args.no_validation and env_config.enable_validation
    else:
        min_games = args.min_games
        enable_monitoring = not args.no_monitoring
        enable_validation = not args.no_validation

    calculator = LeaderboardCalculator(
        min_games_threshold=min_games,
        enable_monitoring=enable_monitoring,
        enable_validation=enable_validation
    )
    
    try:
        # Calculate leaderboards
        leaderboard_df = calculator.calculate_leaderboards()
        
        # Save results
        calculator.save_leaderboard(leaderboard_df)
        
        # Show performance summary if monitoring was enabled
        if enable_monitoring and PERFORMANCE_MONITORING_AVAILABLE:
            performance_monitor.print_performance_summary()
            performance_monitor.save_performance_report()
        
        # Show validation summary if validation was enabled
        if enable_validation and DATA_VALIDATION_AVAILABLE:
            data_validator.print_validation_summary()
        
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        calculator.logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
