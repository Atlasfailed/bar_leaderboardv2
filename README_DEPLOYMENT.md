# BAR Leaderboard - PythonAnywhere Deployment Package

## 📦 This Package Contains:
- All Python files (flattened structure)
- Static files (CSS, JS, images)
- Templates (HTML files)
- Configuration files
- Data files (essential and pipeline-generated)

## 🚀 Deployment Instructions:

### 1. Upload Files to PythonAnywhere
1. Upload all files to your PythonAnywhere account (e.g., `/home/yourusername/mysite/`)
2. Keep the folder structure:
   ```
   mysite/
   ├── flask_app.py          # Main Flask application
   ├── config.py             # Configuration
   ├── utils.py              # Utilities
   ├── *.py                  # Other Python files
   ├── static/               # CSS, JS, images
   ├── templates/            # HTML templates
   ├── data/                 # Data files (some generated by pipelines)
   │   ├── iso_country.csv   # Country mappings (essential)
   │   ├── *.parquet         # Pipeline-generated data
   │   └── *.json            # Analysis results
   ├── requirements.txt      # Dependencies
   └── wsgi_pythonanywhere.py # WSGI config
   ```

### 2. Install Dependencies
In PythonAnywhere console:
```bash
cd ~/mysite
pip3.10 install --user -r requirements.txt
```

### 3. Configure WSGI
1. Edit `wsgi_pythonanywhere.py`
2. Change the path to your actual directory
3. Copy content to your PythonAnywhere WSGI configuration

### 4. Set Up Data Pipeline (CRITICAL)
Run the data pipeline to generate all required data files:
```bash
cd ~/mysite
python3.10 run_all_pipelines.py
```

**Note:** The web application requires data files generated by the pipeline. If these files don't exist, the app will show errors. Always run the pipeline after deployment and before starting the web app.

### 5. Configure Web App
1. Go to PythonAnywhere Web tab
2. Set source code directory to `/home/yourusername/mysite`
3. Set WSGI configuration file to your modified wsgi file
4. Set static files mapping: `/static/` -> `/home/yourusername/mysite/static/`

## 📊 Data Updates
To update rankings data:
```bash
python3.10 run_all_pipelines.py
```

## 📋 Data Files Included:
- **Essential:** `iso_country.csv` (always included)
- **Pipeline-generated:** These files are created by `run_all_pipelines.py`:
  - `final_leaderboard.parquet` - Player leaderboards
  - `nation_rankings.parquet` - Nation rankings (last 7 days)
  - `player_contributions.parquet` - Player contributions analysis
  - `roster_analysis_results.json` - Team analysis
  - `team_rosters.parquet` - Team roster data
  - `efficiency_vs_speed_analysis_with_names.csv` - Efficiency analysis

## 🔧 Troubleshooting
- Check error logs in PythonAnywhere
- Ensure all file paths are correct
- Verify Python version compatibility (3.10 recommended)
- **If web app shows "file not found" errors:** Run `python3.10 run_all_pipelines.py`

## ⚠️ Important Notes
- This package uses a flat file structure for easier deployment
- All imports have been adjusted for the flat structure
- **Critical:** Data files need to be populated by running the pipelines before the web app will work
- Nation rankings are based on last 7 days of match data
- Some data files may not be included if they haven't been generated yet
