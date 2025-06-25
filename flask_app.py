from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np 
import math
import json
from typing import Optional, Dict, Any, List, Tuple

from config import config

# --- Configuration ---
app = Flask(__name__)
CORS(app)

# --- Production/Development Environment Detection ---
PRODUCTION = os.environ.get('FLASK_ENV') == 'production'


class DataManager:
    """Centralized data management for loading and caching all data files."""
    
    def __init__(self):
        self.leaderboard_df: Optional[pd.DataFrame] = None
        self.country_name_map: Dict[str, str] = {}
        self.nation_rankings_df: Optional[pd.DataFrame] = None
        self.player_contributions_df: Optional[pd.DataFrame] = None
        self.processed_leaderboards_cache: Dict[str, Any] = {}
        self.last_updated_time: str = "N/A"
        
        # Season 1 data
        self.season_1_leaderboard_df: Optional[pd.DataFrame] = None
        self.season_1_nation_rankings_df: Optional[pd.DataFrame] = None
        self.season_1_player_contributions_df: Optional[pd.DataFrame] = None
        self.season_1_processed_cache: Dict[str, Any] = {}
    
    @staticmethod
    def safe_json_convert(value: Any) -> Any:
        """Convert numpy types and other non-JSON-serializable types to JSON-compatible types."""
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif hasattr(value, 'tolist'):
            return value.tolist()
        elif isinstance(value, (list, tuple)):
            return [DataManager.safe_json_convert(item) for item in value]
        elif isinstance(value, dict):
            return {k: DataManager.safe_json_convert(v) for k, v in value.items()}
        return value
    
    @staticmethod
    def get_last_datamart_update() -> datetime:
        """Calculate the last datamart update time based on cron schedule."""
        now = datetime.now()
        
        # Find the most recent Tuesday or Saturday at 10:00 AM
        days_back = 0
        while days_back <= 7:
            check_date = now - timedelta(days=days_back)
            
            if check_date.weekday() in [1, 5]:  # Tuesday=1, Saturday=5
                update_time = check_date.replace(hour=10, minute=0, second=0, microsecond=0)
                
                if update_time <= now:
                    return update_time
            
            days_back += 1
        
        # Fallback: return a time from a week ago
        fallback = now - timedelta(days=7)
        return fallback.replace(hour=10, minute=0, second=0, microsecond=0)
    
    def load_leaderboard_data(self) -> bool:
        """Load and preprocess leaderboard data."""
        if not os.path.exists(config.paths.final_leaderboard_parquet):
            print(f"ERROR: Leaderboard file not found at: {config.paths.final_leaderboard_parquet}")
            return False
        
        try:
            self.leaderboard_df = pd.read_parquet(config.paths.final_leaderboard_parquet)
            
            # Calculate last updated time
            last_datamart_update = self.get_last_datamart_update()
            self.last_updated_time = last_datamart_update.strftime('%B %d, %Y at %H:%M UTC')
            
            print(f"Loaded {len(self.leaderboard_df)} player leaderboard records.")
            print(f"Data represents datamart update from: {self.last_updated_time}")
            
            # Pre-process for faster API responses
            print("Pre-processing leaderboard data for faster access...")
            self.processed_leaderboards_cache = self._preprocess_leaderboard_data()
            print(f"Pre-processed {len(self.processed_leaderboards_cache)} leaderboard combinations.")
            
            return True
        except Exception as e:
            print(f"ERROR loading player leaderboard data: {e}")
            return False
    
    def load_country_data(self) -> bool:
        """Load country mapping data."""
        if not os.path.exists(config.paths.iso_countries_csv):
            print(f"ERROR: Country file not found at: {config.paths.iso_countries_csv}")
            return False
        
        try:
            countries_df = pd.read_csv(config.paths.iso_countries_csv)
            self.country_name_map = pd.Series(
                countries_df.name.values, 
                index=countries_df['alpha-2'].str.strip()
            ).to_dict()
            print(f"Loaded {len(self.country_name_map)} country mappings.")
            return True
        except Exception as e:
            print(f"ERROR loading country data: {e}")
            return False
    
    def load_nation_rankings(self) -> bool:
        """Load nation rankings data."""
        if not os.path.exists(config.paths.nation_rankings_parquet):
            print(f"INFO: Nation ranking file not found at: {config.paths.nation_rankings_parquet}")
            return False
        
        try:
            self.nation_rankings_df = pd.read_parquet(config.paths.nation_rankings_parquet)
            print(f"Loaded {len(self.nation_rankings_df)} nation ranking records.")
            return True
        except Exception as e:
            print(f"ERROR loading nation ranking data: {e}")
            return False
    
    def load_player_contributions(self) -> bool:
        """Load player contributions data."""
        if not os.path.exists(config.paths.player_contributions_parquet):
            print(f"INFO: Player contributions file not found at: {config.paths.player_contributions_parquet}")
            return False
        
        try:
            self.player_contributions_df = pd.read_parquet(config.paths.player_contributions_parquet)
            print(f"Loaded {len(self.player_contributions_df)} player contribution records.")
            return True
        except Exception as e:
            print(f"ERROR loading player contributions data: {e}")
            return False
    
    def load_season_1_data(self) -> bool:
        """Load and preprocess season 1 data."""
        try:
            # Load season 1 leaderboard data
            if os.path.exists(config.paths.season_1_leaderboard_parquet):
                self.season_1_leaderboard_df = pd.read_parquet(config.paths.season_1_leaderboard_parquet)
                print(f"Loaded {len(self.season_1_leaderboard_df)} season 1 leaderboard records.")
                
                # Pre-process season 1 data for faster API responses
                print("Pre-processing season 1 leaderboard data...")
                self.season_1_processed_cache = self._preprocess_leaderboard_data_for_season(self.season_1_leaderboard_df)
                print(f"Pre-processed {len(self.season_1_processed_cache)} season 1 leaderboard combinations.")
            else:
                print(f"WARNING: Season 1 leaderboard file not found at: {config.paths.season_1_leaderboard_parquet}")
            
            # Load season 1 nation rankings data
            if os.path.exists(config.paths.season_1_nation_rankings_parquet):
                self.season_1_nation_rankings_df = pd.read_parquet(config.paths.season_1_nation_rankings_parquet)
                print(f"Loaded {len(self.season_1_nation_rankings_df)} season 1 nation ranking records.")
            else:
                print(f"WARNING: Season 1 nation ranking file not found at: {config.paths.season_1_nation_rankings_parquet}")
            
            # Load season 1 player contributions data
            if os.path.exists(config.paths.season_1_player_contributions_parquet):
                self.season_1_player_contributions_df = pd.read_parquet(config.paths.season_1_player_contributions_parquet)
                print(f"Loaded {len(self.season_1_player_contributions_df)} season 1 player contribution records.")
            else:
                print(f"WARNING: Season 1 player contributions file not found at: {config.paths.season_1_player_contributions_parquet}")
            
            return True
        except Exception as e:
            print(f"ERROR loading season 1 data: {e}")
            return False
    
    def load_all_data(self) -> bool:
        """Load all data files and return success status."""
        success = True
        success &= self.load_leaderboard_data()
        success &= self.load_country_data()
        success &= self.load_nation_rankings()
        success &= self.load_player_contributions()
        success &= self.load_season_1_data()
        return success
    
    def _preprocess_leaderboard_data(self) -> Dict[str, Any]:
        """Pre-process leaderboard data to create fast lookup tables."""
        if self.leaderboard_df is None:
            return {}
        
        processed = {}
        
        # Get unique combinations of leaderboard_id and game_type
        combinations = self.leaderboard_df[['leaderboard_id', 'game_type']].drop_duplicates()
        
        for _, row in combinations.iterrows():
            leaderboard_id = row['leaderboard_id']
            game_type = row['game_type']
            key = f"{leaderboard_id}_{game_type}"
            
            # Filter data for this combination
            filtered_df = self.leaderboard_df[
                (self.leaderboard_df['leaderboard_id'] == leaderboard_id) & 
                (self.leaderboard_df['game_type'] == game_type)
            ]
            
            if not filtered_df.empty:
                processed[key] = self._process_player_data(filtered_df)
        
        # Also create global leaderboards for each game type
        game_types = self.leaderboard_df['game_type'].unique()
        for game_type in game_types:
            key = f"global_{game_type}"
            filtered_df = self.leaderboard_df[self.leaderboard_df['game_type'] == game_type]
            
            if not filtered_df.empty:
                processed[key] = self._process_player_data(filtered_df)
        
        return processed
    
    def _preprocess_leaderboard_data_for_season(self, leaderboard_df: pd.DataFrame) -> Dict[str, Any]:
        """Pre-process leaderboard data for a specific season to create fast lookup tables."""
        if leaderboard_df is None or leaderboard_df.empty:
            return {}
        
        processed = {}
        
        # Get unique combinations of leaderboard_id and game_type
        combinations = leaderboard_df[['leaderboard_id', 'game_type']].drop_duplicates()
        
        for _, row in combinations.iterrows():
            leaderboard_id = row['leaderboard_id']
            game_type = row['game_type']
            key = f"{leaderboard_id}_{game_type}"
            
            # Filter data for this combination
            filtered_df = leaderboard_df[
                (leaderboard_df['leaderboard_id'] == leaderboard_id) & 
                (leaderboard_df['game_type'] == game_type)
            ]
            
            if not filtered_df.empty:
                processed[key] = self._process_player_data(filtered_df)
        
        # Also create global leaderboards for each game type
        game_types = leaderboard_df['game_type'].unique()
        for game_type in game_types:
            key = f"global_{game_type}"
            filtered_df = leaderboard_df[leaderboard_df['game_type'] == game_type]
            
            if not filtered_df.empty:
                processed[key] = self._process_player_data(filtered_df)
        
        return processed
    
    def _process_player_data(self, filtered_df: pd.DataFrame) -> Dict[str, Any]:
        """Process player data for a specific leaderboard/game type combination."""
        # Get latest entry for each player
        latest_entries = filtered_df.sort_values('start_time').groupby('name').tail(1)
        
        # Determine if this is a country-specific leaderboard
        is_country_leaderboard = False
        if not filtered_df.empty:
            sample_leaderboard_id = filtered_df['leaderboard_id'].iloc[0]
            is_country_leaderboard = sample_leaderboard_id.startswith('country_')
        
        # Convert to list of player dictionaries
        players = []
        for _, player in latest_entries.iterrows():
            country_code = player.get('countryCode', '')
            player_data = {
                "name": player['name'],
                "display_name": player['name'],
                "leaderboard_rating": float(player['leaderboard_rating']),
                "countryCode": country_code,
                "user_id": int(player['user_id'])
            }
            
            # Add flag information for global and regional leaderboards (not country-specific)
            if not is_country_leaderboard and country_code:
                player_data["flag"] = f"https://flagcdn.com/w20/{country_code.lower()}.png"
            
            players.append(player_data)
        
        # Sort by rating (descending) and add ranks
        players.sort(key=lambda x: x['leaderboard_rating'], reverse=True)
        for i, player in enumerate(players):
            player['rank'] = i + 1
        
        return {
            "players": players,
            "total_players": len(players)
        }

# Global data manager instance
data_manager = DataManager()


# Initialize data manager and load all data on startup
print("Initializing BAR Leaderboard application...")
success = data_manager.load_all_data()
if not success:
    print("WARNING: Some data files could not be loaded. Some features may not work properly.")
print("Application initialization complete.")


# --- API Endpoints ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/nation-rankings')
def nation_rankings_page():
    return render_template('nation_rankings.html')

@app.route('/efficiency-analysis')
def efficiency_analysis_page():
    return render_template('efficiency_analysis.html')

@app.route('/team-rankings')
def team_rankings_page():
    return render_template('team_rankings.html')


@app.route('/api/player-contributions/<game_type>')
def get_player_contributions(game_type):
    """Provides the full list of player contributions for a game type."""
    if data_manager.player_contributions_df is None:
        return jsonify({"error": "Player contributions data not available. Please run 'nation_ranking_pipeline.py' again to generate it."}), 500

    game_type_str = game_type.replace('%20', ' ')
    filtered_df = data_manager.player_contributions_df[data_manager.player_contributions_df['game_type'] == game_type_str]
    
    cleaned_records = []
    for record in filtered_df.to_dict(orient='records'):
        cleaned_records.append({
            'name': record.get('name'),
            'country_name': record.get('country_name', record.get('countryCode')),
            'score': int(record.get('score', 0))
        })
        
    return jsonify(cleaned_records)


@app.route('/api/nation-rankings/<game_type>')
def get_nation_rankings(game_type):
    """Provides the weekly nation rankings for a specific game type."""
    if data_manager.nation_rankings_df is None:
        return jsonify({"error": "Nation ranking data not available. Please run 'nation_ranking_pipeline.py' successfully first."}), 500

    game_type_str = game_type.replace('%20', ' ')
    filtered_df = data_manager.nation_rankings_df[data_manager.nation_rankings_df['game_type'] == game_type_str].copy()

    if filtered_df.empty:
        return jsonify([])

    records = filtered_df.to_dict(orient='records')
    
    # Clean records for JSON serialization using the data manager's converter
    cleaned_records = []
    for record in records:
        new_record = {
            'game_type': record.get('game_type'),
            'countryCode': record.get('country'),  # Use 'country' column, expose as 'countryCode' for JS
            'total_score': int(record.get('total_score', 0)),
            'country_name': record.get('country_name'),
            'rank': int(record.get('rank', 0))
        }
        
        # Handle top_contributors with safe JSON conversion
        top_contributors = record.get('top_contributors')
        contributors_list = []
        
        if top_contributors is not None:
            # Use data manager's safe conversion
            top_contributors = data_manager.safe_json_convert(top_contributors)
            
            if isinstance(top_contributors, list):
                for contributor in top_contributors:
                    if isinstance(contributor, dict):
                        contributors_list.append({
                            'name': contributor.get('name'),
                            'score': int(contributor.get('score', 0))
                        })
        
        new_record['top_contributors'] = contributors_list
        cleaned_records.append(new_record)

    return jsonify(cleaned_records)


@app.route('/api/search-player/<game_type>/<player_name>')
def search_player_contributions(game_type, player_name):
    """Search for a player's contributions across all nations for a specific game type."""
    if data_manager.player_contributions_df is None:
        return jsonify({"error": "Player contributions data not available. Please run 'nation_ranking_pipeline.py' again to generate it."}), 500

    game_type_str = game_type.replace('%20', ' ')
    player_name_lower = player_name.lower()
    
    # Filter by game type first
    game_data = data_manager.player_contributions_df[data_manager.player_contributions_df['game_type'] == game_type_str]
    
    # Search for players whose names contain the search term
    matching_players = game_data[
        game_data['name'].str.lower().str.contains(player_name_lower, na=False)
    ]
    
    if matching_players.empty:
        return jsonify([])
    
    # Group by player name and aggregate their contributions
    results = []
    for player_name_exact in matching_players['name'].unique():
        player_data = matching_players[matching_players['name'] == player_name_exact]
        
        contributions = []
        total_score = 0
        
        for _, row in player_data.iterrows():
            contribution = {
                'country_name': row.get('country_name', row.get('countryCode', 'Unknown')),
                'countryCode': row.get('countryCode', ''),
                'score': int(row.get('score', 0))
            }
            contributions.append(contribution)
            total_score += contribution['score']
        
        # Sort contributions by score (highest first)
        contributions.sort(key=lambda x: x['score'], reverse=True)
        
        results.append({
            'player_name': player_name_exact,
            'total_score': total_score,
            'contributions': contributions,
            'game_type': game_type_str
        })
    
    # Sort results by total score (highest first)
    results.sort(key=lambda x: x['total_score'], reverse=True)
    
    return jsonify(results)


@app.route('/api/nation-score-breakdown/<country_code>/<game_type>')
def get_nation_score_breakdown(country_code, game_type):
    """Provides detailed breakdown of how a nation's score is calculated."""
    if data_manager.player_contributions_df is None or data_manager.nation_rankings_df is None:
        return jsonify({"error": "Data not available."}), 500

    country_code = country_code.upper()
    game_type_str = game_type.replace('%20', ' ')
    
    # Get nation ranking info for k value and other stats
    nation_info = data_manager.nation_rankings_df[
        (data_manager.nation_rankings_df['country'] == country_code) & 
        (data_manager.nation_rankings_df['game_type'] == game_type_str)
    ]
    
    if nation_info.empty:
        return jsonify({"error": f"No ranking data found for {country_code} in {game_type_str}"}), 404
    
    nation_row = nation_info.iloc[0]
    
    # Get all players for this country and game type
    country_players = data_manager.player_contributions_df[
        (data_manager.player_contributions_df['country'] == country_code) & 
        (data_manager.player_contributions_df['game_type'] == game_type_str)
    ].copy()

    if country_players.empty:
        return jsonify({"error": f"No player data found for {country_code} in {game_type_str}"}), 404

    # Calculate statistics
    total_players = len(country_players)
    raw_score = int(country_players['score'].sum())
    total_games = int(nation_row.get('total_games', country_players['score'].abs().sum()))
    
    # Get k value and other scoring info from nation ranking
    k_value = float(nation_row.get('k_value', 0))
    confidence_factor = float(nation_row.get('confidence_factor', k_value * 2))
    adjusted_score = float(nation_row.get('total_score', 0))
    min_games_required = float(nation_row.get('min_games_required', k_value / 4))
    
    # Calculate win/loss breakdown
    positive_players = len(country_players[country_players['score'] > 0])
    negative_players = len(country_players[country_players['score'] < 0])
    zero_players = len(country_players[country_players['score'] == 0])
    
    positive_score_sum = int(country_players[country_players['score'] > 0]['score'].sum())
    negative_score_sum = int(country_players[country_players['score'] < 0]['score'].sum())
    
    # Get top contributors (positive only)
    top_positive = country_players[country_players['score'] > 0].nlargest(10, 'score')
    
    # Convert to clean records
    top_positive_list = []
    for _, player in top_positive.iterrows():
        top_positive_list.append({
            'name': player['name'],
            'score': int(player['score']),
            'wins': int(player['score']),  # Since wins = +1 each
            'losses': 0  # If score is positive, no net losses
        })

    # Get country name
    country_name = country_players['country_name'].iloc[0] if not country_players.empty else country_code

    # Calculate what the score would be without the confidence factor
    raw_percentage = (raw_score / total_games) * 10000 if total_games > 0 else 0
    
    breakdown = {
        'country_code': country_code,
        'country_name': country_name,
        'game_type': game_type_str,
        'adjusted_score': adjusted_score,
        'raw_score': raw_score,
        'raw_percentage': round(raw_percentage, 2),
        'total_games': total_games,
        'total_players': total_players,
        'handicap_info': {
            'k_value': round(k_value, 1),
            'confidence_factor': round(confidence_factor, 1),
            'min_games_required': round(min_games_required, 1),
            'meets_minimum': total_games >= min_games_required,
            'explanation': f'Confidence Factor of {confidence_factor:.1f} games added to denominator. Minimum {min_games_required:.1f} games required for leaderboard.'
        },
        'player_distribution': {
            'positive_players': positive_players,
            'negative_players': negative_players,
            'zero_players': zero_players
        },
        'score_breakdown': {
            'total_wins': positive_score_sum,
            'total_losses': abs(negative_score_sum),
            'net_score': raw_score,
            'win_rate_percentage': round((positive_score_sum / total_games) * 100, 1) if total_games > 0 else 0
        },
        'explanation': {
            'scoring_system': 'Adjusted Score = (Wins - Losses) / (Total Games + Confidence Factor) × 10000',
            'calculation': f'({positive_score_sum} - {abs(negative_score_sum)}) / ({total_games} + {confidence_factor:.1f}) × 10000 = {adjusted_score:.0f}',
            'interpretation': 'The Confidence Factor is added to the denominator, making it larger. This means nations with fewer games get lower scores (bigger denominator = smaller result), while nations with many games are barely affected. This prevents small samples from dominating the leaderboard.',
            'how_it_works': f'Without Confidence Factor, this nation would score {raw_percentage:.0f}. With the factor of {confidence_factor:.1f} added to the denominator, the score becomes {adjusted_score:.0f}. The more games played, the less impact the factor has.',
            'comparison': f'Without Confidence Factor: {raw_percentage:.0f}, With Confidence Factor: {adjusted_score:.0f}'
        },
        'top_contributors': top_positive_list
    }

    return jsonify(breakdown)


@app.route('/api/status')
def get_status():
    """Provides status information for the leaderboard."""
    return jsonify({
        "status": "online",
        "last_updated": data_manager.last_updated_time,
        "message": "BAR Leaderboard is operational"
    })

@app.route('/api/leaderboards')
def get_leaderboards():
    """Provides list of available leaderboards."""
    from flask import request
    season = int(request.args.get('season', config.seasons.default_season))
    
    # Select appropriate dataset based on season
    if season == 1 and hasattr(data_manager, 'season_1_leaderboard_df') and data_manager.season_1_leaderboard_df is not None:
        leaderboard_df = data_manager.season_1_leaderboard_df
    elif season == 2 and data_manager.leaderboard_df is not None:
        leaderboard_df = data_manager.leaderboard_df
    else:
        return jsonify({"error": "Leaderboard data not available for requested season"}), 500
    
    # Get unique leaderboard types from the data
    nations = []
    regions = []
    
    for leaderboard_id in leaderboard_df['leaderboard_id'].unique():
        if leaderboard_id == 'global':
            # Skip global leaderboard in the dropdown
            continue
        elif leaderboard_id.startswith('country_'):
            # Extract country code from country_XX format (Season 1 style)
            country_code = leaderboard_id.replace('country_', '')
            country_name = data_manager.country_name_map.get(country_code, country_code)
            nations.append({
                "id": leaderboard_id,
                "name": country_name,
                "code": country_code,
                "flag": f"https://flagcdn.com/w20/{country_code.lower()}.png"
            })
        elif leaderboard_id.startswith('region_'):
            # Format region name (replace underscores with spaces) (Season 1 style)
            region_name = leaderboard_id.replace('region_', '').replace('_', ' ')
            regions.append({
                "id": leaderboard_id,
                "name": region_name,
                "code": leaderboard_id
            })
        elif len(leaderboard_id) == 2 and leaderboard_id.isupper():
            # Bare country code format (Season 2 style)
            country_code = leaderboard_id
            country_name = data_manager.country_name_map.get(country_code, country_code)
            nations.append({
                "id": leaderboard_id,
                "name": country_name,
                "code": country_code,
                "flag": f"https://flagcdn.com/w20/{country_code.lower()}.png"
            })
        else:
            # Regional leaderboard (Season 2 style with descriptive names)
            regions.append({
                "id": leaderboard_id,
                "name": leaderboard_id,
                "code": leaderboard_id
            })
    
    # Sort by country name
    nations.sort(key=lambda x: x['name'])
    regions.sort(key=lambda x: x['name'])
    
    return jsonify({
        "nations": nations,
        "regions": regions
    })

@app.route('/api/leaderboard/<leaderboard_id>/<game_type>')
def get_leaderboard(leaderboard_id, game_type):
    """Get leaderboard data for a specific leaderboard and game type."""
    from flask import request
    season = int(request.args.get('season', config.seasons.default_season))
    
    # Select appropriate cache based on season
    if season == 1 and hasattr(data_manager, 'season_1_processed_cache'):
        cache = data_manager.season_1_processed_cache
    elif season == 2:
        cache = data_manager.processed_leaderboards_cache
    else:
        return jsonify({"error": "Leaderboard data not available for requested season"}), 500
    
    if not cache:
        return jsonify({"error": "Leaderboard data not available"}), 500
    
    game_type_str = game_type.replace('%20', ' ')
    cache_key = f"{leaderboard_id}_{game_type_str}"
    
    # Use preprocessed data for fast lookup
    if cache_key in cache:
        return jsonify(cache[cache_key])
    else:
        return jsonify({"players": [], "total_players": 0})

@app.route('/api/leaderboard/global/<game_type>')
def get_global_leaderboard(game_type):
    """Get global leaderboard data for a specific game type."""
    from flask import request
    season = int(request.args.get('season', config.seasons.default_season))
    
    # Select appropriate cache based on season
    if season == 1 and hasattr(data_manager, 'season_1_processed_cache'):
        cache = data_manager.season_1_processed_cache
    elif season == 2:
        cache = data_manager.processed_leaderboards_cache
    else:
        return jsonify({"error": "Leaderboard data not available for requested season"}), 500
    
    if not cache:
        return jsonify({"error": "Leaderboard data not available"}), 500
    
    game_type_str = game_type.replace('%20', ' ')
    cache_key = f"global_{game_type_str}"
    
    # Use preprocessed data for fast lookup
    if cache_key in cache:
        return jsonify(cache[cache_key])
    else:
        return jsonify({"players": [], "total_players": 0})


@app.route('/api/efficiency-data')
def get_efficiency_data():
    """Provides efficiency vs speed analysis data for plotting."""
    if not os.path.exists(config.paths.efficiency_analysis_csv):
        return jsonify({"error": "Efficiency analysis data not available"}), 500
    
    try:
        df = pd.read_csv(config.paths.efficiency_analysis_csv)
        
        # Convert to more intuitive units
        df['metal_efficiency_per_100'] = df['metal_efficiency'] * 100  # Energy per 100 metal
        df['time_efficiency_per_1000'] = df['time_efficiency'] * 1000  # Energy per 1000 build time
        
        # Include all energy buildings (both variable and fixed)
        # Group by faction for separate datasets
        arm_data = df[df['faction'] == 'ARM']
        cor_data = df[df['faction'] == 'COR']
        
        return jsonify({
            'arm_data': arm_data.to_dict('records'),
            'cor_data': cor_data.to_dict('records')
        })
        
    except Exception as e:
        return jsonify({"error": f"Error loading efficiency data: {str(e)}"}), 500

@app.route('/api/team-rankings')
def get_team_rankings():
    """Get team rankings data."""
    try:
        if not os.path.exists(config.paths.team_analysis_json):
            return jsonify({"error": "Team analysis data not available. Please run team analysis first."}), 404
        
        with open(config.paths.team_analysis_json, 'r') as f:
            roster_data = json.load(f)
        
        # Transform the roster data to the expected frontend format
        team_data = {
            "analysis_date": roster_data.get("analysis_date"),
            "config": roster_data.get("config"),
            "summary": roster_data.get("summary"),
            "party_teams": roster_data.get("rosters", []),
            "network_teams": roster_data.get("communities", []),
            "frequent_pairs": []  # Empty since we're only doing party-based analysis
        }
        
        return jsonify(team_data)
    except Exception as e:
        return jsonify({"error": f"Error loading team data: {str(e)}"}), 500

@app.route('/api/team-rankings/<team_type>')
def get_team_rankings_by_type(team_type):
    """Get team rankings data by type (party_teams, network_teams, frequent_teammates)."""
    try:
        if not os.path.exists(config.paths.team_analysis_json):
            return jsonify({"error": "Team analysis data not available. Please run team analysis first."}), 404
        
        with open(config.paths.team_analysis_json, 'r') as f:
            roster_data = json.load(f)
        
        # Transform the roster data to the expected frontend format
        if team_type == 'party_teams':
            return jsonify({team_type: roster_data.get("rosters", [])})
        elif team_type == 'network_teams' or team_type == 'communities':
            return jsonify({team_type: roster_data.get("communities", [])})
        elif team_type == 'frequent_teammates' or team_type == 'frequent_pairs':
            return jsonify({team_type: []})  # Empty since we're only doing party-based analysis
        else:
            return jsonify({"error": f"Team type '{team_type}' not found"}), 404
        
    except Exception as e:
        return jsonify({"error": f"Error loading team data: {str(e)}"}), 500

@app.route('/api/player-suggestions/<partial_name>')
def get_player_suggestions(partial_name):
    """Get player name suggestions for autocomplete."""
    try:
        if not os.path.exists(config.paths.team_analysis_json):
            return jsonify({"error": "Team analysis data not available."}), 404
        
        with open(config.paths.team_analysis_json, 'r') as f:
            roster_data = json.load(f)
        
        partial_name_lower = partial_name.lower()
        suggestions = set()
        
        # Collect all unique player names from roster data (party teams)
        for team in roster_data.get('rosters', []):
            for player in team.get('roster', []):
                name = player.get('name', '')
                if name and partial_name_lower in name.lower():
                    suggestions.add(name)
        
        # Also collect from communities data
        for community in roster_data.get('communities', []):
            for player in community.get('roster', []):
                name = player.get('name', '')
                if name and partial_name_lower in name.lower():
                    suggestions.add(name)
        
        # Return sorted suggestions, limited to 10
        sorted_suggestions = sorted(list(suggestions))[:10]
        
        return jsonify({
            'suggestions': sorted_suggestions,
            'query': partial_name
        })
        
    except Exception as e:
        return jsonify({"error": f"Error getting suggestions: {str(e)}"}), 500

@app.route('/api/search-teams/<player_name>')
@app.route('/api/search-teams/<player_name>/<search_type>')
def search_teams_by_player(player_name, search_type='party_teams'):
    """Search for teams or communities that contain a specific player."""
    try:
        if not os.path.exists(config.paths.team_analysis_json):
            return jsonify({"error": "Team analysis data not available. Please run team analysis first."}), 404
        
        with open(config.paths.team_analysis_json, 'r') as f:
            roster_data = json.load(f)
        
        player_name_lower = player_name.lower()
        player_items = []
        
        if search_type == 'network_teams' or search_type == 'communities':
            # Search through communities
            for community in roster_data.get('communities', []):
                for player in community.get('roster', []):
                    if player_name_lower in player['name'].lower():
                        player_items.append({
                            **community,
                            'team_type': 'community',
                            'search_match': player
                        })
                        break
            
            # Sort communities by size (largest first) and exact matches
            def sort_key(item):
                exact_match = player_name_lower == item['search_match']['name'].lower()
                size = item.get('player_count', 0)
                return (-int(exact_match), -size)
                
        else:
            # Search through party teams (default behavior)
            for team in roster_data.get('rosters', []):
                for player in team.get('roster', []):
                    if player_name_lower in player['name'].lower():
                        # Add player_details field for frontend compatibility
                        team_copy = team.copy()
                        team_copy['player_details'] = team_copy.get('roster', [])
                        player_items.append({
                            **team_copy,
                            'team_type': 'party',
                            'search_match': player
                        })
                        break
            
            # Sort teams by relevance (exact matches first, then by team activity)
            def sort_key(item):
                exact_match = player_name_lower == item['search_match']['name'].lower()
                activity = item.get('stats_overall', {}).get('matches', 0)
                return (-int(exact_match), -activity)
        
        player_items.sort(key=sort_key)
        
        return jsonify({
            'player_name': player_name,
            'search_type': search_type,
            'items_found': len(player_items),
            'teams': player_items[:20]  # Limit to top 20 results
        })
        
    except Exception as e:
        return jsonify({"error": f"Error searching for player teams: {str(e)}"}), 500

@app.route('/api/network-data')
def get_network_data():
    """Get the full network data for visualization."""
    try:
        # Generate network data from the existing team analysis
        network_data = generate_network_data()
        return jsonify(network_data)
    except Exception as e:
        print(f"Error generating network data: {e}")
        return jsonify({"error": "Failed to generate network data"}), 500

@app.route('/api/player-network/<player_name>')
def get_player_network(player_name):
    """Get network data focused on a specific player and their connections."""
    try:
        network_data = generate_player_network(player_name)
        return jsonify(network_data)
    except Exception as e:
        print(f"Error generating player network for {player_name}: {e}")
        return jsonify({"error": f"Failed to generate network data for player {player_name}"}), 500

@app.route('/api/network-search/<query>')
def search_network_players(query):
    """Search for players in the network."""
    try:
        players = search_players_in_network(query)
        return jsonify(players)
    except Exception as e:
        print(f"Error searching players: {e}")
        return jsonify({"error": "Failed to search players"}), 500

def generate_network_data():
    """Generate network graph data from team analysis results."""
    if not os.path.exists(config.paths.team_analysis_json):
        raise Exception("Team analysis file not found")
    
    with open(config.paths.team_analysis_json, 'r') as f:
        team_data = json.load(f)
    
    nodes = []
    edges = []
    node_ids = set()
    
    # Extract nodes and edges from communities (more comprehensive than just teams)
    for community in team_data.get('communities', []):
        community_players = community.get('roster', [])
        community_size = len(community_players)
        
        # Add nodes
        for player in community_players:
            player_id = player['user_id']
            if player_id not in node_ids:
                nodes.append({
                    'id': player_id,
                    'name': player['name'],
                    'country': player.get('country', 'Unknown'),
                    'group': len(nodes) % 10,  # Color grouping
                    'community_size': community_size,
                    'community_id': community.get('community_id')
                })
                node_ids.add(player_id)
        
        # Add edges between players in the same community
        for i, player1 in enumerate(community_players):
            for player2 in community_players[i+1:]:
                # Calculate edge weight based on community size (smaller = stronger connection)
                weight = max(1, 50 - community_size)
                edges.append({
                    'source': player1['user_id'],
                    'target': player2['user_id'],
                    'weight': weight,
                    'community_id': community.get('community_id')
                })
    
    # Limit the network size for performance (show top 1000 most connected players)
    if len(nodes) > 1000:
        # Count connections per player
        connection_counts = {}
        for edge in edges:
            connection_counts[edge['source']] = connection_counts.get(edge['source'], 0) + 1
            connection_counts[edge['target']] = connection_counts.get(edge['target'], 0) + 1
        
        # Sort players by connection count and take top 1000
        top_players = sorted(connection_counts.items(), key=lambda x: x[1], reverse=True)[:1000]
        top_player_ids = {player_id for player_id, count in top_players}
        
        # Filter nodes and edges
        nodes = [node for node in nodes if node['id'] in top_player_ids]
        edges = [edge for edge in edges if edge['source'] in top_player_ids and edge['target'] in top_player_ids]
    
    return {
        'nodes': nodes,
        'edges': edges,
        'stats': {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'communities': len(team_data.get('communities', []))
        }
    }

def generate_player_network(player_name):
    """Generate network data focused on a specific player."""
    if not os.path.exists(config.paths.team_analysis_json):
        raise Exception("Team analysis file not found")
    
    with open(config.paths.team_analysis_json, 'r') as f:
        team_data = json.load(f)
    
    player_name_lower = player_name.lower()
    target_player = None
    player_communities = []
    
    # Find the player and their communities
    for community in team_data.get('communities', []):
        community_players = community.get('roster', [])
        player_in_community = None
        
        for player in community_players:
            if player['name'].lower() == player_name_lower:
                target_player = player
                player_in_community = player
                break
        
        if player_in_community:
            player_communities.append(community)
    
    if not target_player:
        return {'nodes': [], 'edges': [], 'message': f'Player "{player_name}" not found in network'}
    
    nodes = []
    edges = []
    node_ids = set()
    
    # Add the target player
    nodes.append({
        'id': target_player['user_id'],
        'name': target_player['name'],
        'country': target_player.get('country', 'Unknown'),
        'group': 0,  # Special group for target player
        'is_target': True
    })
    node_ids.add(target_player['user_id'])
    
    # Add nodes and edges from all communities the player belongs to
    for i, community in enumerate(player_communities):
        community_players = community.get('roster', [])
        group_id = i + 1
        
        for player in community_players:
            player_id = player['user_id']
            if player_id not in node_ids:
                nodes.append({
                    'id': player_id,
                    'name': player['name'],
                    'country': player.get('country', 'Unknown'),
                    'group': group_id,
                    'community_size': len(community_players),
                    'community_id': community.get('community_id')
                })
                node_ids.add(player_id)
            
            # Add edge to target player
            if player_id != target_player['user_id']:
                edges.append({
                    'source': target_player['user_id'],
                    'target': player_id,
                    'weight': 10,
                    'community_id': community.get('community_id')
                })
        
        # Add edges between other players in the same community (lighter connections)
        for j, player1 in enumerate(community_players):
            for player2 in community_players[j+1:]:
                if player1['user_id'] != target_player['user_id'] and player2['user_id'] != target_player['user_id']:
                    edges.append({
                        'source': player1['user_id'],
                        'target': player2['user_id'],
                        'weight': 2,
                        'community_id': community.get('community_id')
                    })
    
    return {
        'nodes': nodes,
        'edges': edges,
        'target_player': target_player,
        'communities': len(player_communities),
        'stats': {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'player_communities': len(player_communities)
        }
    }

def search_players_in_network(query):
    """Search for players in the network data."""
    if not os.path.exists(config.paths.team_analysis_json):
        raise Exception("Team analysis file not found")
    
    with open(config.paths.team_analysis_json, 'r') as f:
        team_data = json.load(f)
    
    query_lower = query.lower()
    matching_players = []
    seen_players = set()
    
    # Search through all communities
    for community in team_data.get('communities', []):
        for player in community.get('roster', []):
            player_id = player['user_id']
            player_name = player['name']
            
            if player_id not in seen_players and query_lower in player_name.lower():
                matching_players.append({
                    'id': player_id,
                    'name': player_name,
                    'country': player.get('country', 'Unknown')
                })
                seen_players.add(player_id)
                
                # Limit results for performance
                if len(matching_players) >= 50:
                    break
        
        if len(matching_players) >= 50:
            break
    
    return matching_players

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="BAR Leaderboard Flask App")
    parser.add_argument('--port', type=int, default=5000, help='Port to run the Flask app on (default: 5000)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to run the Flask app on (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true', help='Run Flask in debug mode')
    args = parser.parse_args()
    app.run(debug=args.debug, host=args.host, port=args.port)

