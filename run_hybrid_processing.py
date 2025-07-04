#!/usr/bin/env python3
"""
Run Hybrid Data Processing Pipeline
===================================

Entry point for running the hybrid data processor as part of the pipeline system.
This script processes datamart files and enhances them with replay JSON data.
"""

import sys
import logging
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from hybrid_data_processor import HybridDataProcessor

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the hybrid data processing pipeline."""
    try:
        logger.info("🔄 Starting Hybrid Data Processing Pipeline...")
        
        # Initialize and run the hybrid processor
        processor = HybridDataProcessor()
        results = processor.run_hybrid_processing()
        
        if results:
            leaderboard = results['leaderboard']
            nation_rankings = results['nation_rankings']
            
            logger.info("✅ Hybrid data processing completed successfully!")
            logger.info(f"📊 Generated enhanced leaderboard with {len(leaderboard):,} players")
            logger.info(f"🌍 Generated enhanced nation rankings with {len(nation_rankings):,} nations")
            logger.info("🌐 HTML ranking pages generated and saved")
            
            # Summary of what was enhanced
            enhanced_files = [
                "enhanced_leaderboard.parquet",
                "enhanced_leaderboard.csv", 
                "enhanced_nation_rankings.parquet",
                "enhanced_nation_rankings.csv",
                "enhanced_players.parquet",
                "enhanced_matches.parquet",
                "enhanced_match_players.parquet"
            ]
            
            logger.info("📄 Enhanced data files created:")
            for file in enhanced_files:
                logger.info(f"   ✓ {file}")
            
            logger.info("🚀 Flask app will now use enhanced data automatically!")
            
        else:
            logger.warning("⚠️ Hybrid processing completed but no results returned")
            
    except Exception as e:
        logger.error(f"❌ Hybrid data processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
