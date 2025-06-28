#!/bin/bash
# BAR Deployment Cleanup Script
echo "🧹 Cleaning up development files..."

# Remove development/testing files
echo "Removing development test files..."
rm -f verify_slovenia.py
rm -f compare_parquet_files.py
rm -f test_seasons.py
rm -rf test/

# Remove redundant files
echo "Removing redundant files..."
rm -f flask_app.py
rm -f run_leaderboard.py
rm -f requirements_minimal.txt

# Remove cache files
echo "Removing cache files..."
rm -rf __pycache__/
rm -f .DS_Store

# Remove empty directories
echo "Removing empty directories..."
rmdir extra/ 2>/dev/null || true

# Optional: Remove one-time scripts (uncomment if you're done with them)
# rm -f convert_to_csv.py
# rm -f create_season_1_data.py
# rm -f create_season_1_leaderboard.py

echo "✅ Cleanup complete!"
echo "📊 Essential files preserved:"
echo "   - Core app files (app.py, config.py, etc.)"
echo "   - Pipeline scripts"
echo "   - Deployment files"
echo "   - Data and static assets"
