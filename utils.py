#!/usr/bin/env python3
"""
BAR Leaderboard Shared Utilities
=================================

Common utilities and helper functions used across all analysis scripts.
"""

import os
import time
import requests
import pandas as pd
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime

from .config import config

# ==============================================================================
# --- Logging Configuration ---
# ==============================================================================

def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """Set up consistent logging across all scripts."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

# ==============================================================================
# --- Network Utilities ---
# ==============================================================================

def make_request(url: str, timeout: int = 30) -> requests.Response:
    """Make a HTTP request with proper configuration and error handling."""
    try:
        response = requests.get(
            url, 
            headers=config.network.headers,
            proxies=config.network.proxies,
            timeout=timeout,
            verify=config.network.ssl_verify
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch data from {url}: {e}")

# ==============================================================================
# --- Data Loading Utilities ---
# ==============================================================================

class DataLoader:
    """Centralized data loading with caching and error handling."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or setup_logging(self.__class__.__name__)
    
    def download_parquet(self, url: str, local_path: Optional[Path] = None) -> pd.DataFrame:
        """Download a parquet file from URL and optionally cache it locally."""
        self.logger.info(f"Downloading parquet from: {url}")
        
        try:
            response = make_request(url)
            df = pd.read_parquet(BytesIO(response.content))
            self.logger.info(f"Successfully loaded {len(df):,} rows")
            
            # Cache locally if path provided
            if local_path:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(local_path, index=False)
                self.logger.info(f"Cached data to {local_path}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to download parquet from {url}: {e}")
            raise
    
    def load_with_cache(self, url: str, local_path: Path, 
                       cache_hours: int = None) -> pd.DataFrame:
        """Load data with local caching based on file age."""
        cache_hours = cache_hours or config.datamart.cache_duration_hours
        
        # Check if cached file exists and is recent enough
        if local_path.exists():
            file_age_hours = (time.time() - local_path.stat().st_mtime) / 3600
            if file_age_hours < cache_hours:
                self.logger.info(f"Loading from cache: {local_path} (age: {file_age_hours:.1f}h)")
                return pd.read_parquet(local_path)
        
        # Download fresh data
        self.logger.info(f"Cache miss or expired, downloading fresh data")
        return self.download_parquet(url, local_path)
    
    def load_datamart_data(self) -> Dict[str, pd.DataFrame]:
        """Load all standard datamart files with caching."""
        data = {}
        
        try:
            self.logger.info("Loading BAR datamart files...")
            
            # Load matches
            data['matches'] = self.load_with_cache(
                config.datamart.matches_url,
                config.paths.matches_parquet
            )
            
            # Load match players
            data['match_players'] = self.load_with_cache(
                config.datamart.match_players_url,
                config.paths.match_players_parquet
            )
            
            # Load players
            data['players'] = self.load_with_cache(
                config.datamart.players_url,
                config.paths.players_parquet
            )
            
            # Load ISO countries if available
            if config.paths.iso_countries_csv.exists():
                data['iso_countries'] = pd.read_csv(config.paths.iso_countries_csv)
                self.logger.info(f"Loaded {len(data['iso_countries'])} ISO country mappings")
            else:
                self.logger.warning("ISO countries file not found")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load datamart data: {e}")
            raise

# ==============================================================================
# --- Data Processing Utilities ---
# ==============================================================================

def merge_player_data(match_players_df: pd.DataFrame, 
                     players_df: pd.DataFrame,
                     matches_df: pd.DataFrame) -> pd.DataFrame:
    """Merge match players with player info and match details."""
    
    # Ensure players have country codes
    if 'country' in players_df.columns and 'countryCode' not in players_df.columns:
        players_df = players_df.copy()
        players_df['countryCode'] = players_df['country']
    
    # Merge match details
    data = match_players_df.merge(
        matches_df[['match_id', 'winning_team', 'game_type', 'is_ranked']],
        on='match_id',
        how='left'
    )
    
    # Merge player details
    data = data.merge(
        players_df[['user_id', 'name', 'countryCode']],
        on='user_id',
        how='left'
    )
    
    # Fill missing names
    data['name'] = data['name'].fillna(
        data['user_id'].apply(lambda x: f"Player_{x}")
    )
    
    return data

def filter_ranked_matches(data: pd.DataFrame, 
                         game_types: Optional[List[str]] = None) -> pd.DataFrame:
    """Filter data for ranked matches in specified game types."""
    filtered = data[data['is_ranked'] == True].copy()
    
    if game_types:
        filtered = filtered[filtered['game_type'].isin(game_types)]
    
    return filtered

# ==============================================================================
# --- File Management Utilities ---
# ==============================================================================

def ensure_directory_exists(path: Path) -> None:
    """Ensure a directory exists, creating it if necessary."""
    path.mkdir(parents=True, exist_ok=True)

def safe_file_write(data: Any, filepath: Path, format: str = 'auto') -> None:
    """Safely write data to file with automatic format detection."""
    ensure_directory_exists(filepath.parent)
    
    if format == 'auto':
        format = filepath.suffix.lower()
    
    if format in ['.parquet', 'parquet']:
        if hasattr(data, 'to_parquet'):
            data.to_parquet(filepath, index=False)
        else:
            raise ValueError("Data must be a pandas DataFrame for parquet format")
    
    elif format in ['.csv', 'csv']:
        if hasattr(data, 'to_csv'):
            data.to_csv(filepath, index=False)
        else:
            raise ValueError("Data must be a pandas DataFrame for CSV format")
    
    elif format in ['.json', 'json']:
        import json
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    else:
        raise ValueError(f"Unsupported format: {format}")

# ==============================================================================
# --- Analysis Utilities ---
# ==============================================================================

def calculate_win_rate(wins: int, losses: int) -> float:
    """Calculate win rate with safe division."""
    total = wins + losses
    return wins / total if total > 0 else 0.0

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Perform safe division with default value for zero denominator."""
    return numerator / denominator if denominator != 0 else default

def format_percentage(value: float, decimals: int = 1) -> str:
    """Format a decimal as a percentage string."""
    return f"{value * 100:.{decimals}f}%"

# ==============================================================================
# --- Progress Tracking ---
# ==============================================================================

class ProgressTracker:
    """Simple progress tracking for long-running operations."""
    
    def __init__(self, total: int, name: str = "Progress", 
                 logger: Optional[logging.Logger] = None):
        self.total = total
        self.name = name
        self.current = 0
        self.logger = logger or setup_logging("ProgressTracker")
        self.start_time = time.time()
    
    def update(self, increment: int = 1) -> None:
        """Update progress by specified increment."""
        self.current += increment
        
        if self.current % max(1, self.total // 20) == 0 or self.current >= self.total:
            self._log_progress()
    
    def _log_progress(self) -> None:
        """Log current progress."""
        percentage = (self.current / self.total) * 100
        elapsed = time.time() - self.start_time
        
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
            self.logger.info(f"{self.name}: {self.current:,}/{self.total:,} ({percentage:.1f}%) - ETA: {eta:.0f}s")
        else:
            self.logger.info(f"{self.name}: {self.current:,}/{self.total:,} ({percentage:.1f}%)")

# Global data loader instance
data_loader = DataLoader()
