#!/usr/bin/env python3
"""
BAR Nation Ranking Pipeline
============================

Calculates nation rankings using confidence factor algorithm and generates
player contributions data for the web application.

This script:
1. Downloads BAR data mart files
2. Calculates nation scores with confidence factors
3. Generates nation rankings and player contributions
4. Outputs results for the web application

Usage:
    python nation_ranking_pipeline.py [--min-games N]
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
import logging
import argparse
from datetime import datetime, timedelta

from config import config
from utils import setup_logging, data_loader, merge_player_data, filter_ranked_matches, safe_file_write

# ==============================================================================
# --- Nation Ranking Calculation ---
# ==============================================================================

class NationRankingCalculator:
    """Handles nation ranking calculation and player contribution analysis."""
    
    def __init__(self, min_games_threshold: int):
        self.logger = setup_logging(self.__class__.__name__)
        config.setup_ssl()
        self.min_games_threshold = min_games_threshold
    
    def calculate_nation_rankings(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Main method to calculate nation rankings and player contributions."""
        self.logger.info("Starting nation ranking calculation pipeline")
        
        # Load and prepare data
        data = self._load_and_prepare_data()
        
        # Calculate rankings for each game type
        all_rankings = []
        all_contributions = []
        
        game_types = data['game_type'].unique()
        self.logger.info(f"Calculating rankings for {len(game_types)} game types")
        
        for game_type in game_types:
            self.logger.info(f"Processing game type: {game_type}")
            
            game_data = data[data['game_type'] == game_type].copy()
            if game_data.empty:
                continue
            
            # Calculate nation rankings for this game type
            rankings, contributions = self._calculate_game_type_rankings(game_data, game_type)
            
            if not rankings.empty:
                all_rankings.append(rankings)
            if not contributions.empty:
                all_contributions.append(contributions)
        
        # Combine results
        final_rankings = pd.concat(all_rankings, ignore_index=True) if all_rankings else pd.DataFrame()
        final_contributions = pd.concat(all_contributions, ignore_index=True) if all_contributions else pd.DataFrame()
        
        self.logger.info(f"Generated {len(final_rankings)} nation rankings")
        self.logger.info(f"Generated {len(final_contributions)} player contributions")
        
        return final_rankings, final_contributions
    
    def _load_and_prepare_data(self) -> pd.DataFrame:
        """Load and prepare data for nation ranking calculation."""
        self.logger.info("Loading BAR data marts...")
        
        # Load all datamart files
        raw_data = data_loader.load_datamart_data()
        
        # Load ISO country data
        iso_df = pd.read_csv(config.paths.iso_countries_csv)
        
        # Filter matches to last 7 days only
        one_week_ago = datetime.now() - timedelta(days=7)
        recent_matches = raw_data['matches'][raw_data['matches']['start_time'] >= one_week_ago].copy()
        
        self.logger.info(f"Filtering to matches from last 7 days: {len(recent_matches):,} matches (from {one_week_ago.strftime('%Y-%m-%d %H:%M')})")
        
        if recent_matches.empty:
            self.logger.warning("No matches found in the last 7 days!")
            return pd.DataFrame()
        
        # Merge data (same approach as leaderboard pipeline)
        self.logger.info("Merging player and match data...")
        data = pd.merge(raw_data['match_players'], recent_matches, on='match_id', how='inner')
        data = pd.merge(data, raw_data['players'], on='user_id', how='left')
        
        # Filter for ranked matches
        supported_game_types = ['Duel', 'Small Team', 'Large Team', 'Team', 'FFA']
        data = filter_ranked_matches(data, supported_game_types)
        
        # Filter for valid countries
        valid_countries = set(iso_df['alpha-2'].str.strip())
        data = data[data['country'].isin(valid_countries)].copy()
        
        # Add country names
        country_mapping = dict(zip(iso_df['alpha-2'], iso_df['name']))
        data['country_name'] = data['country'].map(country_mapping)
        
        self.logger.info(f"Prepared {len(data):,} match records for weekly analysis (last 7 days)")
        return data
    
    def _calculate_game_type_rankings(self, game_data: pd.DataFrame, game_type: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate nation rankings for a specific game type."""
        
        # Calculate player contributions (wins/losses by country)
        player_contributions = self._calculate_player_contributions(game_data, game_type)
        
        if player_contributions.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        # Aggregate by country
        country_stats = self._aggregate_country_stats(player_contributions)
        
        if country_stats.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        # Calculate confidence factors and adjusted scores
        nation_rankings = self._calculate_confidence_scores(country_stats, game_type)
        
        # Add top contributors
        nation_rankings = self._add_top_contributors(nation_rankings, player_contributions)
        
        return nation_rankings, player_contributions
    
    def _calculate_player_contributions(self, game_data: pd.DataFrame, game_type: str) -> pd.DataFrame:
        """Calculate individual player contributions (wins/losses)."""
        
        # For each player-match, determine if they won or lost
        game_data['won'] = (game_data['team_id'] == game_data['winning_team']).astype(int)
        game_data['score'] = game_data['won'] * 2 - 1  # +1 for win, -1 for loss
        
        # Aggregate by player and country  
        player_stats = game_data.groupby(['user_id', 'name', 'country', 'country_name']).agg({
            'score': 'sum',
            'won': 'sum',
            'match_id': 'count'
        }).reset_index()
        
        player_stats.rename(columns={'match_id': 'total_games', 'won': 'wins'}, inplace=True)
        player_stats['losses'] = player_stats['total_games'] - player_stats['wins']
        player_stats['game_type'] = game_type
        
        # Filter players with minimum games
        min_games = self.min_games_threshold // 10  # Lower threshold for individual players
        player_stats = player_stats[player_stats['total_games'] >= min_games].copy()
        
        return player_stats
    
    def _aggregate_country_stats(self, player_contributions: pd.DataFrame) -> pd.DataFrame:
        """Aggregate player contributions by country."""
        
        country_stats = player_contributions.groupby(['country', 'country_name']).agg({
            'score': 'sum',
            'wins': 'sum', 
            'losses': 'sum',
            'total_games': 'sum',
            'user_id': 'count'
        }).reset_index()
        
        country_stats.rename(columns={'user_id': 'player_count'}, inplace=True)
        
        # Don't filter here - we'll filter after calculating the confidence factor
        # based on the dynamic minimum games threshold (k/4)
        
        return country_stats
    
    def _calculate_confidence_scores(self, country_stats: pd.DataFrame, game_type: str) -> pd.DataFrame:
        """Calculate confidence factors and adjusted scores using the specified formula."""
        
        # Calculate k-factor per the specification:
        # k = (Average Games per Nation) ÷ 2 for this game mode
        average_games_per_nation = country_stats['total_games'].mean()
        k_value = average_games_per_nation / 2
        confidence_factor = 2 * k_value  # Confidence Factor = 2k
        min_games_required = k_value / 4  # Minimum activity threshold
        
        self.logger.info(f"Game mode {game_type}: avg_games={average_games_per_nation:.1f}, k={k_value:.1f}, CF={confidence_factor:.1f}, min_games={min_games_required:.1f}")
        
        # Apply minimum games filter based on calculated threshold
        country_stats = country_stats[country_stats['total_games'] >= min_games_required].copy()
        
        if country_stats.empty:
            self.logger.warning(f"No nations meet minimum games requirement ({min_games_required:.1f}) for {game_type}")
            return country_stats
        
        # Add calculated values as columns (same for all nations in this game mode)
        country_stats['k_value'] = k_value
        country_stats['confidence_factor'] = confidence_factor
        country_stats['min_games_required'] = min_games_required
        
        # Calculate adjusted score with confidence factor
        # Formula: (Wins - Losses) / (Total Games + Confidence Factor) × 10000
        denominator = country_stats['total_games'] + confidence_factor
        country_stats['total_score'] = (country_stats['score'] / denominator * 10000).round().astype(int)
        
        # Calculate raw score for comparison
        country_stats['raw_score'] = (country_stats['score'] / country_stats['total_games'] * 10000).round().astype(int)
        
        # Sort by adjusted score and add ranks
        country_stats = country_stats.sort_values('total_score', ascending=False).reset_index(drop=True)
        country_stats['rank'] = range(1, len(country_stats) + 1)
        
        # Add game type
        country_stats['game_type'] = game_type
        
        return country_stats
    
    def _add_top_contributors(self, nation_rankings: pd.DataFrame, player_contributions: pd.DataFrame) -> pd.DataFrame:
        """Add top contributors for each nation."""
        
        def get_top_contributors(country_code: str) -> List[Dict[str, Any]]:
            country_players = player_contributions[
                player_contributions['country'] == country_code
            ].nlargest(5, 'score')
            
            contributors = []
            for _, player in country_players.iterrows():
                if player['score'] > 0:  # Only include positive contributors
                    contributors.append({
                        'name': player['name'],
                        'score': int(player['score'])
                    })
            
            return contributors
        
        nation_rankings['top_contributors'] = nation_rankings['country'].apply(get_top_contributors)
        
        return nation_rankings
    
    def save_results(self, nation_rankings: pd.DataFrame, player_contributions: pd.DataFrame) -> None:
        """Save nation rankings and player contributions to files."""
        
        if not nation_rankings.empty:
            self.logger.info("Saving nation rankings...")
            safe_file_write(nation_rankings, config.paths.nation_rankings_parquet)
            self.logger.info(f"Nation rankings saved to: {config.paths.nation_rankings_parquet}")
        
        if not player_contributions.empty:
            self.logger.info("Saving player contributions...")
            safe_file_write(player_contributions, config.paths.player_contributions_parquet)
            self.logger.info(f"Player contributions saved to: {config.paths.player_contributions_parquet}")
        
        # Log summary statistics
        if not nation_rankings.empty:
            summary = {
                'total_nations': len(nation_rankings),
                'game_types': nation_rankings['game_type'].nunique(),
                'total_players': len(player_contributions) if not player_contributions.empty else 0,
                'top_nation': nation_rankings.iloc[0]['country_name'] if len(nation_rankings) > 0 else 'N/A'
            }
            self.logger.info(f"Rankings summary: {summary}")

# ==============================================================================
# --- Main Execution ---
# ==============================================================================

def main():
    """Main pipeline execution."""
    parser = argparse.ArgumentParser(description="BAR Nation Ranking Pipeline")
    parser.add_argument(
        "--min-games",
        type=int,
        default=10, # A reasonable default
        help="Minimum games required for nation leaderboard inclusion."
    )
    args = parser.parse_args()

    calculator = NationRankingCalculator(min_games_threshold=args.min_games)
    
    try:
        # Calculate rankings
        nation_rankings, player_contributions = calculator.calculate_nation_rankings()
        
        # Save results
        calculator.save_results(nation_rankings, player_contributions)
        
        print("✅ Nation ranking pipeline completed successfully!")
        
    except Exception as e:
        calculator.logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
