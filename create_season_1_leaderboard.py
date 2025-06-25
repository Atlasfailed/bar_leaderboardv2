#!/usr/bin/env python3
"""
Create Season 1 Leaderboard
===========================

This script creates a proper Season 1 leaderboard by running the pipeline
using only Season 1 data (before March 23rd, 2025, 12:00 CET).

This is different from create_season_1_data.py which just filters the 
existing leaderboard. This script recalculates leaderboards using only
Season 1 match data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pathlib import Path

from config import config
from utils import setup_logging, data_loader, filter_ranked_matches, safe_file_write

# Setup logging
logger = setup_logging('Season1Leaderboard')

# Constants
SUPPORTED_GAME_TYPES = ['Large Team', 'Small Team', 'Duel']
MIN_GAMES_THRESHOLD = config.analysis.min_player_games_threshold  # Use config value for player leaderboards

def create_season_1_leaderboard():
    """Create Season 1 leaderboard using only Season 1 data."""
    
    logger.info("Creating Season 1 leaderboard from Season 1 data only")
    
    # Load and merge data
    logger.info("Loading datamart files...")
    raw_data = data_loader.load_datamart_data()
    
    # Merge data like the main pipeline
    logger.info("Merging datamart files...")
    data = pd.merge(raw_data['match_players'], raw_data['matches'], on='match_id', how='left')
    data = pd.merge(data, raw_data['players'], on='user_id', how='left')
    
    logger.info(f"Total merged records: {len(data):,}")
    
    # Filter for Season 1 only (before March 23, 2025 12:00 CET)
    logger.info("Filtering for Season 1 data...")
    data['start_time'] = pd.to_datetime(data['start_time'], utc=True)
    season_1_end = config.seasons.season_1_end
    season_1_data = data[data['start_time'] < season_1_end].copy()
    
    logger.info(f"Season 1 records: {len(season_1_data):,} (filtered from {len(data):,})")
    
    # Filter for ranked matches in supported game types
    logger.info("Filtering for ranked matches...")
    season_1_data = filter_ranked_matches(season_1_data, SUPPORTED_GAME_TYPES)
    
    logger.info(f"Season 1 ranked records: {len(season_1_data):,}")
    
    # Add ISO country mapping
    iso_country_path = config.paths.data_dir / 'iso_country.csv'
    if iso_country_path.exists():
        logger.info("Adding regional mapping...")
        iso_countries = pd.read_csv(iso_country_path)
        country_region_map = iso_countries.set_index('alpha-2')['sub-region'].to_dict()
        season_1_data['sub_region'] = season_1_data['country'].map(country_region_map)
    else:
        season_1_data['sub_region'] = None
    
    # Calculate leaderboards
    leaderboards = []
    
    # Global leaderboards by game type
    for game_type in SUPPORTED_GAME_TYPES:
        logger.info(f"Calculating Season 1 global leaderboard for: {game_type}")
        global_lb = calculate_global_leaderboard(season_1_data, game_type)
        if not global_lb.empty:
            leaderboards.append(global_lb)
    
    # Country-specific leaderboards
    countries = season_1_data['country'].dropna().unique()
    logger.info(f"Evaluating Season 1 country leaderboards for {len(countries)} countries")
    
    country_leaderboard_count = 0
    for i, country in enumerate(countries, 1):
        if pd.isna(country) or len(str(country)) != 2:
            continue
        
        country_data = season_1_data[season_1_data['country'] == country]
        total_players = country_data['user_id'].nunique()
        
        # Skip countries with too few total players
        if total_players < 15:
            continue
        
        logger.info(f"Processing country {i}/{len(countries)}: {country} ({total_players} total players)")
        
        # Check if country qualifies by having 15+ qualified players in at least one game mode
        country_qualifies = False
        for game_type in SUPPORTED_GAME_TYPES:
            game_data = country_data[country_data['game_type'] == game_type]
            if game_data.empty:
                continue
            
            player_game_counts = game_data['user_id'].value_counts()
            qualified_players = player_game_counts[player_game_counts >= MIN_GAMES_THRESHOLD]
            
            if len(qualified_players) >= 15:
                country_qualifies = True
                break
        
        # If country qualifies, create leaderboards for all game modes
        if country_qualifies:
            country_has_leaderboards = False
            for game_type in SUPPORTED_GAME_TYPES:
                country_lb = calculate_country_leaderboard(country_data, country, game_type)
                if not country_lb.empty:
                    leaderboards.append(country_lb)
                    country_has_leaderboards = True
            
            if country_has_leaderboards:
                country_leaderboard_count += 1
    
    logger.info(f"Generated Season 1 country leaderboards for {country_leaderboard_count} countries")
    
    # Regional leaderboards
    if season_1_data['sub_region'].notna().any():
        regions = season_1_data['sub_region'].dropna().unique()
        region_player_counts = season_1_data.groupby('sub_region')['user_id'].nunique()
        significant_regions = region_player_counts[region_player_counts >= 15].index
        
        logger.info(f"Calculating Season 1 regional leaderboards for {len(significant_regions)} regions")
        
        for i, region in enumerate(significant_regions, 1):
            if pd.isna(region) or region == '':
                continue
            
            logger.info(f"Processing region {i}/{len(significant_regions)}: {region}")
            
            region_data = season_1_data[season_1_data['sub_region'] == region]
            
            for game_type in SUPPORTED_GAME_TYPES:
                game_data = region_data[region_data['game_type'] == game_type]
                if game_data.empty:
                    continue
                
                player_game_counts = game_data['user_id'].value_counts()
                qualified_players = player_game_counts[player_game_counts >= MIN_GAMES_THRESHOLD]
                
                if len(qualified_players) >= 15:
                    regional_lb = calculate_regional_leaderboard(region_data, region, game_type)
                    if not regional_lb.empty:
                        leaderboards.append(regional_lb)
    
    # Combine all leaderboards
    if leaderboards:
        logger.info(f"Combining {len(leaderboards)} leaderboard sections")
        final_leaderboard = pd.concat(leaderboards, ignore_index=True)
        
        # Add ranks
        final_leaderboard = add_ranks_to_leaderboard(final_leaderboard)
        
        logger.info(f"Final Season 1 leaderboard: {len(final_leaderboard):,} entries")
        
        # Save Season 1 leaderboard
        output_path = config.paths.season_1_leaderboard_parquet
        safe_file_write(final_leaderboard, output_path)
        logger.info(f"Saved Season 1 leaderboard to: {output_path}")
        
        # Check for AtlasFailed specifically
        atlas_records = final_leaderboard[final_leaderboard['user_id'] == 134300]
        logger.info(f"AtlasFailed records in Season 1 leaderboard: {len(atlas_records)}")
        if len(atlas_records) > 0:
            logger.info("AtlasFailed game types in Season 1:")
            for game_type in atlas_records['game_type'].unique():
                count = len(atlas_records[atlas_records['game_type'] == game_type])
                logger.info(f"  {game_type}: {count} entries")
        
        return final_leaderboard
    else:
        logger.error("No leaderboards generated!")
        return pd.DataFrame()

def calculate_global_leaderboard(data: pd.DataFrame, game_type: str) -> pd.DataFrame:
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
    qualified_players = player_game_counts[player_game_counts >= MIN_GAMES_THRESHOLD].index
    
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
    
    # Add metadata
    leaderboard['game_type'] = game_type
    leaderboard['leaderboard_id'] = 'global'
    
    return leaderboard

def calculate_country_leaderboard(data: pd.DataFrame, country: str, game_type: str) -> pd.DataFrame:
    """Calculate country leaderboard for a specific game type."""
    country_game_data = data[(data['country'] == country) & (data['game_type'] == game_type)].copy()
    
    if country_game_data.empty:
        return pd.DataFrame()
    
    # Get latest rating for each player in this country
    latest_ratings = (country_game_data
                     .sort_values('start_time')
                     .groupby('user_id')
                     .tail(1)
                     .copy())
    
    # Filter players with minimum games
    player_game_counts = country_game_data['user_id'].value_counts()
    qualified_players = player_game_counts[player_game_counts >= MIN_GAMES_THRESHOLD].index
    
    latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
    
    if latest_ratings.empty:
        return pd.DataFrame()
    
    # Prepare leaderboard data
    leaderboard = latest_ratings[[
        'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
    ]].copy()
    
    # Calculate leaderboard rating
    leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
    
    # Rename columns for consistency
    leaderboard = leaderboard.rename(columns={
        'country': 'countryCode'
    })
    
    # Add metadata
    leaderboard['game_type'] = game_type
    leaderboard['leaderboard_id'] = f'country_{country}'
    
    return leaderboard

def calculate_regional_leaderboard(data: pd.DataFrame, region: str, game_type: str) -> pd.DataFrame:
    """Calculate regional leaderboard for a specific game type."""
    regional_game_data = data[(data['sub_region'] == region) & (data['game_type'] == game_type)].copy()
    
    if regional_game_data.empty:
        return pd.DataFrame()
    
    # Get latest rating for each player in this region
    latest_ratings = (regional_game_data
                     .sort_values('start_time')
                     .groupby('user_id')
                     .tail(1)
                     .copy())
    
    # Filter players with minimum games
    player_game_counts = regional_game_data['user_id'].value_counts()
    qualified_players = player_game_counts[player_game_counts >= MIN_GAMES_THRESHOLD].index
    
    latest_ratings = latest_ratings[latest_ratings['user_id'].isin(qualified_players)]
    
    if len(latest_ratings) < 15:  # Need at least 15 qualified players
        return pd.DataFrame()
    
    # Prepare leaderboard data
    leaderboard = latest_ratings[[
        'user_id', 'name', 'country', 'new_skill', 'new_uncertainty', 'start_time'
    ]].copy()
    
    # Calculate leaderboard rating
    leaderboard['leaderboard_rating'] = leaderboard['new_skill'] - leaderboard['new_uncertainty']
    
    # Rename columns for consistency
    leaderboard = leaderboard.rename(columns={
        'country': 'countryCode'
    })
    
    # Add metadata
    leaderboard['game_type'] = game_type
    leaderboard['leaderboard_id'] = f'region_{region.replace(" ", "_")}'
    
    return leaderboard

def add_ranks_to_leaderboard(leaderboard: pd.DataFrame) -> pd.DataFrame:
    """Add ranking to leaderboard data."""
    leaderboard = leaderboard.copy()
    
    # Sort by rating descending and add ranks within each leaderboard/game type
    leaderboard['rank'] = (leaderboard
                          .groupby(['leaderboard_id', 'game_type'])['leaderboard_rating']
                          .rank(method='min', ascending=False))
    
    return leaderboard

if __name__ == "__main__":
    try:
        result = create_season_1_leaderboard()
        if not result.empty:
            print("✅ Season 1 leaderboard created successfully!")
            print(f"Total entries: {len(result):,}")
            
            # Show some stats
            print(f"Game types: {result['game_type'].value_counts().to_dict()}")
            print(f"Countries: {result['countryCode'].nunique()}")
            print(f"Players: {result['user_id'].nunique()}")
        else:
            print("❌ No leaderboard data generated")
    except Exception as e:
        logger.error(f"Failed to create Season 1 leaderboard: {e}")
        raise
