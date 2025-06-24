#!/usr/bin/env python3
"""
BAR Leaderboard Configuration Management
========================================

Centralized configuration for all BAR leaderboard analysis scripts.
This module provides unified settings, file paths, and shared utilities.
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path

# ==============================================================================
# --- Base Configuration ---
# ==============================================================================

@dataclass
class NetworkConfig:
    """Network and SSL configuration for API requests."""
    proxies: Optional[Dict[str, str]] = None
    headers: Dict[str, str] = None
    ssl_verify: bool = False
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

@dataclass
class DataMartConfig:
    """Configuration for BAR data mart URLs and caching."""
    matches_url: str = 'https://data-marts.beyondallreason.dev/matches.parquet'
    match_players_url: str = 'https://data-marts.beyondallreason.dev/match_players.parquet'
    players_url: str = 'https://data-marts.beyondallreason.dev/players.parquet'
    cache_duration_hours: int = 24

@dataclass
class AnalysisConfig:
    """Configuration for various analysis parameters."""
    # Nation ranking parameters
    min_games_threshold: int = 50
    k_factor_base: float = 400.0
    
    # Team analysis parameters
    min_matches_for_connection: int = 5
    min_team_matches: int = 10
    min_roster_size: int = 2
    max_roster_size: int = 10
    
    # Performance parameters
    max_workers: int = 4
    chunk_size: int = 10000

@dataclass
class FilePaths:
    """Centralized file path management."""
    data_dir: Path
    
    def __post_init__(self):
        self.data_dir = Path(self.data_dir)
        self.data_dir.mkdir(exist_ok=True)
    
    @property
    def matches_parquet(self) -> Path:
        return self.data_dir / "matches.parquet"
    
    @property
    def match_players_parquet(self) -> Path:
        return self.data_dir / "match_players.parquet"
    
    @property
    def players_parquet(self) -> Path:
        return self.data_dir / "players.parquet"
    
    @property
    def final_leaderboard_parquet(self) -> Path:
        return self.data_dir / "final_leaderboard.parquet"
    
    @property
    def nation_rankings_parquet(self) -> Path:
        return self.data_dir / "nation_rankings.parquet"
    
    @property
    def player_contributions_parquet(self) -> Path:
        return self.data_dir / "player_contributions.parquet"
    
    @property
    def iso_countries_csv(self) -> Path:
        return self.data_dir / "iso_country.csv"
    
    @property
    def efficiency_analysis_csv(self) -> Path:
        return self.data_dir / "efficiency_vs_speed_analysis_with_names.csv"
    
    @property
    def team_analysis_json(self) -> Path:
        return self.data_dir / "roster_analysis_results.json"
    
    @property
    def team_rosters_parquet(self) -> Path:
        return self.data_dir / "team_rosters.parquet"
    
    @property
    def team_communities_parquet(self) -> Path:
        return self.data_dir / "team_communities.parquet"

class Config:
    """Main configuration class that brings together all configuration components."""
    
    def __init__(self, base_dir: Optional[str] = None):
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            # In deployment, config.py is in root directory
            self.base_dir = Path(__file__).parent
        self.paths = FilePaths(data_dir=self.base_dir / "data")
        self.network = NetworkConfig()
        self.datamart = DataMartConfig()
        self.analysis = AnalysisConfig()
        # Add easy access to team analysis config
        self.team_analysis = self.analysis
    
    def setup_ssl(self):
        """Configure SSL settings for data mart access."""
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context

# Global configuration instance
config = Config()
