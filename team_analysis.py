#!/usr/bin/env python3
"""
Core Roster Analysis Script for Nation Leaderboard
===================================================

This script identifies stable, premade teams ("core rosters") by analyzing
player collaboration patterns. It uses a hybrid network approach:

1.  A relationship graph is built ONLY from players who have explicitly
    queued together in a party. This uses the `party_id` as a "proof of
    connection" for a single match, eliminating noise from random matchmaking.

2.  A community detection algorithm is run on this high-quality graph to
    find dense clusters of players who frequently play together, even if
    not everyone is available for every game. These clusters are the
    "core rosters".

3.  Each roster is analyzed in detail to calculate win rates, player attendance,
    and other stats, now broken down by game mode for greater accuracy.

Usage:
    python team_analysis.py [--min-matches N] [--min-team-matches N]
"""

import pandas as pd
import networkx as nx
from collections import defaultdict, Counter
import json
from itertools import combinations
import argparse
import logging

from config import config
from utils import setup_logging, data_loader, safe_file_write, merge_player_data

class TeamAnalyzer:
    """Handles the entire team roster analysis pipeline."""

    def __init__(self, min_matches_connection, min_team_matches, min_roster_size, max_roster_size):
        self.logger = setup_logging(self.__class__.__name__)
        config.setup_ssl()
        self.min_matches_connection = min_matches_connection
        self.min_team_matches = min_team_matches
        self.min_roster_size = min_roster_size
        self.max_roster_size = max_roster_size

    def run_analysis(self):
        """Main entry point to run the full analysis pipeline."""
        self.logger.info("Starting team analysis pipeline...")
        
        # Load data
        data, players_data = self._load_and_prepare_data()
        if data is None:
            return

        # Build network
        roster_network, party_info = self._build_roster_network(data)
        if roster_network is None:
            return

        # Detect and analyze rosters
        analyzed_rosters, unrestricted_communities = self._detect_and_analyze_rosters(
            roster_network, party_info, players_data, data
        )

        # Save results
        self._save_results(analyzed_rosters, unrestricted_communities)
        self.logger.info("Team analysis pipeline completed successfully.")

    def _load_and_prepare_data(self):
        """Loads and prepares data using shared utilities."""
        self.logger.info("Loading BAR data marts...")
        raw_data = data_loader.load_datamart_data()
        
        if not all(k in raw_data for k in ['match_players', 'matches', 'players']):
            self.logger.error("One or more required data marts are missing.")
            return None, None

        self.logger.info("Merging player and match data...")
        data = merge_player_data(
            raw_data['match_players'],
            raw_data['players'],
            raw_data['matches']
        )
        
        # Keep a separate, clean players dataframe for lookups
        players_data = raw_data['players'].copy()
        players_data['name'] = players_data['name'].fillna(players_data['user_id'].apply(lambda x: f"Player_{x}"))

        self.logger.info(f"Prepared {len(data):,} match records for analysis.")
        return data, players_data

    def _build_roster_network(self, data):
        self.logger.info("Building 'Pre-Made Only' relationship graph...")
        party_data = data.loc[data['party_id'].notnull(), ['match_id', 'party_id', 'user_id', 'team_id', 'game_type', 'is_ranked']].copy()
        if party_data.empty:
            self.logger.warning("No party data found. Cannot build roster network.")
            return None, None

        grouped_parties = party_data.groupby(['match_id', 'party_id'])
        party_info_list = [{
            'match_id': name[0],
            'players': sorted(group['user_id'].tolist()),
            'team_id': group['team_id'].iloc[0],
            'game_type': group['game_type'].iloc[0],
            'is_ranked': group['is_ranked'].iloc[0]
        } for name, group in grouped_parties]
        self.logger.info(f"Found {len(party_info_list):,} unique party instances.")

        edge_weights = Counter(
            pair
            for party in party_info_list
            if len(party['players']) > 1
            for pair in combinations(party['players'], 2)
        )

        G = nx.Graph()
        for pair, weight in edge_weights.items():
            if weight >= self.min_matches_connection:
                G.add_edge(pair[0], pair[1], weight=weight)

        self.logger.info(f"Roster network built: {G.number_of_nodes():,} players, {G.number_of_edges():,} connections.")
        return G, party_info_list

    def _detect_and_analyze_rosters(self, G, party_info_list, players_data, data):
        self.logger.info("Detecting and analyzing core rosters...")
        if G is None or G.number_of_nodes() == 0:
            self.logger.warning("Graph is empty. Skipping roster detection.")
            return [], []

        player_to_party_map = defaultdict(list)
        for i, party_info in enumerate(party_info_list):
            for player_id in party_info['players']:
                player_to_party_map[player_id].append(i)

        match_win_lookup = data.drop_duplicates('match_id').set_index('match_id')['winning_team'].to_dict()
        player_lookup = players_data.set_index('user_id').to_dict('index')

        # Using a simple community detection for this refactored version
        communities = list(nx.community.louvain_communities(G, weight='weight'))
        self.logger.info(f"Detected {len(communities)} communities.")

        analyzed_rosters = []
        for i, roster_ids in enumerate(communities):
            if not (self.min_roster_size <= len(roster_ids) <= self.max_roster_size):
                continue

            stats = self._calculate_roster_stats(list(roster_ids), party_info_list, player_to_party_map, match_win_lookup)
            if not stats or stats['total_matches_as_team'] < self.min_team_matches:
                continue

            roster_details = self._get_roster_details(roster_ids, stats, player_lookup)
            subgraph = G.subgraph(roster_ids)
            overall_stats = self._summarize_overall_stats(stats)

            analyzed_rosters.append({
                'roster_id': f"roster_{i+1}",
                'team_name': f"{roster_details[0]['name']}'s Squad",
                'player_count': len(roster_ids),
                'roster': roster_details,
                'stats_overall': overall_stats,
                'stats_by_mode': stats['stats_by_mode'],
                'avg_connection_strength': round(subgraph.size(weight='weight') / subgraph.number_of_edges(), 2) if subgraph.number_of_edges() > 0 else 0,
                'most_common_lineups': [
                    {'lineup_names': [player_lookup.get(pid, {}).get('name', f"Player_{pid}") for pid in lineup], 'count': count}
                    for lineup, count in stats['most_common_lineups']
                ]
            })

        analyzed_rosters.sort(key=lambda x: x['stats_overall']['matches'], reverse=True)
        self.logger.info(f"Successfully analyzed {len(analyzed_rosters)} final rosters.")
        
        # For simplicity, this refactored version doesn't produce the separate "unrestricted_communities"
        return analyzed_rosters, []

    def _calculate_roster_stats(self, roster_ids, party_info_list, player_to_party_map, match_win_lookup):
        relevant_party_indices = {idx for pid in roster_ids for idx in player_to_party_map.get(pid, [])}
        team_match_info = [party_info_list[idx] for idx in relevant_party_indices if len(set(roster_ids).intersection(party_info_list[idx]['players'])) >= 2]

        if not team_match_info:
            return None

        stats_by_mode = defaultdict(lambda: {'wins': 0, 'losses': 0, 'matches': 0})
        for match in team_match_info:
            mode = match['game_type'] if pd.notna(match['game_type']) else 'Unknown'
            stats_by_mode[mode]['matches'] += 1
            winning_team = match_win_lookup.get(match['match_id'], -1)
            if pd.notna(winning_team) and winning_team >= 0:
                if match['team_id'] == winning_team:
                    stats_by_mode[mode]['wins'] += 1
                else:
                    stats_by_mode[mode]['losses'] += 1
        
        for mode, stats in stats_by_mode.items():
            decided = stats['wins'] + stats['losses']
            stats['win_rate'] = stats['wins'] / decided if decided > 0 else 0.0

        team_match_lineups = [tuple(sorted(info['players'])) for info in team_match_info]
        player_attendance = Counter(pid for lineup in team_match_lineups for pid in lineup if pid in roster_ids)
        lineup_counts = Counter(team_match_lineups)

        return {
            'total_matches_as_team': len(team_match_info),
            'stats_by_mode': dict(stats_by_mode),
            'player_attendance': dict(player_attendance),
            'most_common_lineups': lineup_counts.most_common(5)
        }

    def _get_roster_details(self, roster_ids, stats, player_lookup):
        total_team_matches = stats['total_matches_as_team']
        roster_details = []
        for pid in roster_ids:
            player_info = player_lookup.get(pid, {})
            matches_played = stats['player_attendance'].get(pid, 0)
            roster_details.append({
                'user_id': pid,
                'name': player_info.get('name', f"Player_{pid}"),
                'country': player_info.get('countryCode', 'Unknown'),
                'matches_played_with_team': matches_played,
                'attendance_percent': round((matches_played / total_team_matches) * 100, 1) if total_team_matches > 0 else 0
            })
        roster_details.sort(key=lambda x: x['matches_played_with_team'], reverse=True)
        return roster_details

    def _summarize_overall_stats(self, stats):
        overall_stats = Counter()
        for mode, mode_stats in stats['stats_by_mode'].items():
            overall_stats['wins'] += mode_stats['wins']
            overall_stats['losses'] += mode_stats['losses']
            overall_stats['matches'] += mode_stats['matches']
        
        total_decided = overall_stats['wins'] + overall_stats['losses']
        overall_stats['win_rate'] = overall_stats['wins'] / total_decided if total_decided > 0 else 0.0
        return dict(overall_stats)

    def _save_results(self, analyzed_rosters, unrestricted_communities):
        """Saves the analysis results to parquet files."""
        if analyzed_rosters:
            rosters_df = pd.DataFrame(analyzed_rosters)
            safe_file_write(rosters_df, config.paths.team_rosters_parquet)
            self.logger.info(f"Saved {len(rosters_df)} team rosters to {config.paths.team_rosters_parquet}")
        else:
            self.logger.warning("No team rosters were generated to save.")

        # This part is kept for compatibility, though the refactored version doesn't produce it.
        if unrestricted_communities:
            communities_df = pd.DataFrame(unrestricted_communities)
            safe_file_write(communities_df, config.paths.team_communities_parquet)
            self.logger.info(f"Saved {len(communities_df)} communities to {config.paths.team_communities_parquet}")

def main():
    """Main execution block."""
    parser = argparse.ArgumentParser(description="BAR Team Roster Analysis Pipeline")
    parser.add_argument("--min-matches", type=int, default=config.team_analysis.min_matches_for_connection, help="Min matches for a connection.")
    parser.add_argument("--min-team-matches", type=int, default=config.team_analysis.min_team_matches, help="Min matches for a team to be included.")
    parser.add_argument("--min-roster-size", type=int, default=config.team_analysis.min_roster_size, help="Min players in a roster.")
    parser.add_argument("--max-roster-size", type=int, default=config.team_analysis.max_roster_size, help="Max players in a roster.")
    args = parser.parse_args()

    analyzer = TeamAnalyzer(
        min_matches_connection=args.min_matches,
        min_team_matches=args.min_team_matches,
        min_roster_size=args.min_roster_size,
        max_roster_size=args.max_roster_size
    )
    
    try:
        analyzer.run_analysis()
        print("✅ Team analysis pipeline completed successfully!")
    except Exception as e:
        logging.getLogger(__name__).error(f"Pipeline failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
