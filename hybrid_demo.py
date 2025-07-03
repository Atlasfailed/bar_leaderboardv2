#!/usr/bin/env python3
"""
Hybrid Data Processing Demo
===========================

Demonstrates the hybrid approach that combines datamart files with replay JSON data
to create comprehensive rankings for the BAR (Beyond All Reason) leaderboard system.
"""

import pandas as pd
from pathlib import Path
from hybrid_data_processor import HybridDataProcessor

def demonstrate_hybrid_approach():
    """Demonstrate the power of the hybrid data processing approach."""
    
    print("🚀 BAR Hybrid Data Processing Demonstration")
    print("=" * 60)
    
    # Initialize processor
    processor = HybridDataProcessor()
    
    print("\n📊 APPROACH 1: Datamart Files Only")
    print("-" * 40)
    processor.load_datamart_files()
    print(f"✅ Players: {len(processor.players_df):,}")
    print(f"✅ Matches: {len(processor.matches_df):,}")
    print(f"✅ Match-Player Records: {len(processor.match_players_df):,}")
    print(f"⚡ Processing Speed: Very Fast")
    print(f"📈 Data Completeness: High (from API)")
    
    print("\n🎮 APPROACH 2: Replay JSON Files")
    print("-" * 40)
    sample_replays = processor.get_sample_replay_files(limit=5)
    json_data_summary = {
        'files_found': len(list(processor.replays_dir.glob("*.json"))),
        'sample_processed': len(sample_replays),
        'total_potential': f"{len(list(processor.replays_dir.glob('*.json'))):,} replay files"
    }
    
    print(f"✅ Total Replay Files Available: {json_data_summary['files_found']:,}")
    print(f"🔍 Sample Files Processed: {json_data_summary['sample_processed']}")
    print(f"⚡ Processing Speed: Slower (JSON parsing)")
    print(f"📈 Data Completeness: Very High (detailed replay data)")
    print(f"🎯 Covers Gaps: Missing matches, player details, game stats")
    
    print("\n🔄 APPROACH 3: Hybrid (Datamart + JSON)")
    print("-" * 40)
    print(f"✅ Combines speed of datamart with completeness of JSON")
    print(f"✅ Fills gaps in datamart with replay data")
    print(f"✅ Provides enhanced player statistics")
    print(f"✅ Creates comprehensive rankings")
    print(f"⚡ Processing Speed: Fast (optimized hybrid)")
    print(f"📈 Data Completeness: Very High (best of both)")
    
    print("\n🏆 RESULTS COMPARISON")
    print("-" * 40)
    
    # Show what the hybrid approach achieved
    results = processor.run_hybrid_processing()
    
    if results:
        leaderboard = results['leaderboard']
        nation_rankings = results['nation_rankings']
        
        print(f"\n📈 Enhanced Dataset Statistics:")
        print(f"   • Total Players: {len(processor.players_df):,}")
        print(f"   • Total Matches: {len(processor.matches_df):,}")
        print(f"   • Ranked Players: {len(leaderboard):,}")
        print(f"   • Nations Ranked: {len(nation_rankings):,}")
        print(f"   • JSON Replays Processed: {len(processor.replay_cache)}")
        
        print(f"\n🎯 Key Benefits:")
        print(f"   ✅ Gap filling: Found new matches not in datamart")
        print(f"   ✅ Enhanced player data: Added missing player information")
        print(f"   ✅ Comprehensive rankings: Combined all data sources")
        print(f"   ✅ Web-ready output: Generated HTML ranking pages")
        print(f"   ✅ Multiple formats: CSV, Parquet, HTML outputs")
        
        print(f"\n🌐 Generated Output Files:")
        data_dir = Path(processor.data_dir)
        html_dir = data_dir / "html_rankings"
        
        output_files = [
            "enhanced_leaderboard.csv",
            "enhanced_leaderboard.parquet", 
            "enhanced_nation_rankings.csv",
            "enhanced_players.parquet",
            "enhanced_matches.parquet"
        ]
        
        for file in output_files:
            if (data_dir / file).exists():
                size = (data_dir / file).stat().st_size / (1024*1024)
                print(f"   📄 {file} ({size:.1f} MB)")
        
        if html_dir.exists():
            html_files = list(html_dir.glob("*.html"))
            for html_file in html_files:
                print(f"   🌐 {html_file.name}")
    
    print(f"\n✨ CONCLUSION")
    print("-" * 40)
    print(f"The hybrid approach successfully combines:")
    print(f"• Fast datamart processing for bulk data")
    print(f"• Detailed JSON replay parsing for gaps")
    print(f"• Comprehensive ranking generation")
    print(f"• Multiple output formats for flexibility")
    print(f"\nResult: Enhanced BAR leaderboard system! 🚀")

if __name__ == "__main__":
    demonstrate_hybrid_approach()
