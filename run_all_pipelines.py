#!/usr/bin/env python3
"""Entry point for running all pipelines."""

import sys
import subprocess
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pipeline(script_name):
    """Run a pipeline script and log the results."""
    try:
        logger.info(f"Starting {script_name}...")
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, text=True, check=True)
        logger.info(f"✅ {script_name} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} failed with error code {e.returncode}")
        if e.stdout:
            logger.error(f"Stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"Stderr: {e.stderr}")
        return False

def main():
    """Run all pipeline scripts in sequence."""
    logger.info("🚀 Starting all BAR pipelines...")
    
    # Define pipelines to run in order
    pipelines = [
        "run_pipelinev2.py",
        "run_hybrid_processing.py",  # Add hybrid processing after main pipeline
        "run_nation_rankings.py", 
        "run_team_analysis.py"
    ]
    
    success_count = 0
    for pipeline in pipelines:
        if run_pipeline(pipeline):
            success_count += 1
    
    logger.info(f"📊 Pipeline Summary: {success_count}/{len(pipelines)} completed successfully")
    
    if success_count == len(pipelines):
        logger.info("🎉 All pipelines completed successfully!")
        return 0
    else:
        logger.error("⚠️  Some pipelines failed. Check logs above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())