# Automated BAR Leaderboard Pipeline

This document explains how to set up automated execution of the BAR leaderboard pipeline using GitHub Actions and automatic file upload to PythonAnywhere.

## Overview

The automation runs:
- **Schedule**: Tuesdays and Saturdays at 5 PM CET
- **Tasks**: 
  1. Execute `run_all_pipelines.py`
  2. Upload generated data files to PythonAnywhere
  3. Notify completion status

## Setup Instructions

### 1. GitHub Repository Secrets

You need to configure the following secrets in your GitHub repository:

1. Go to your GitHub repository
2. Click on **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** and add:

#### Required Secrets:

**PYTHONANYWHERE_USERNAME**
- Value: Your PythonAnywhere username (e.g., `yourusername`)

**PYTHONANYWHERE_API_TOKEN**
- Value: Your PythonAnywhere API token
- To get this:
  1. Log into PythonAnywhere
  2. Go to **Account** → **API Token**
  3. Create a new token or copy existing one

### 2. PythonAnywhere Setup

#### API Token Generation:
1. Log into your PythonAnywhere account
2. Go to the **Account** tab
3. Scroll down to **API Token** section
4. Click **Create new API token** if you don't have one
5. Copy the token and use it as the `PYTHONANYWHERE_API_TOKEN` secret

#### File Structure:
The automation assumes your PythonAnywhere files are organized as:
```
/home/yourusername/mysite/data/
├── final_leaderboard.parquet
├── nation_rankings.parquet
├── player_contributions.parquet
├── matches.parquet
├── match_players.parquet
├── players.parquet
├── team_rosters.parquet
├── iso_country.csv
├── efficiency_vs_speed_analysis_with_names.csv
└── performance_report_*.json
```

### 3. Schedule Configuration

The workflow is configured to run at:
- **Tuesdays at 15:00 UTC** (5 PM CET during winter)
- **Saturdays at 15:00 UTC** (5 PM CET during winter)

#### Daylight Saving Time (DST) Adjustment:
- During CET (UTC+1): 15:00 UTC = 16:00 CET = 4 PM CET
- During CEST (UTC+2): 15:00 UTC = 17:00 CEST = 5 PM CEST

If you need to adjust for DST, you can:
1. Use 14:00 UTC during winter (15:00 CET)
2. Use 15:00 UTC during summer (17:00 CEST)

Or modify the cron schedule in `.github/workflows/automated-pipeline.yml`:
```yaml
schedule:
  - cron: '0 14 * * 2'  # Tuesday at 14:00 UTC (15:00 CET)
  - cron: '0 14 * * 6'  # Saturday at 14:00 UTC (15:00 CET)
```

### 4. Manual Testing

You can manually trigger the workflow for testing:
1. Go to your GitHub repository
2. Click **Actions** tab
3. Select **Automated BAR Leaderboard Pipeline**
4. Click **Run workflow** button

### 5. Monitoring

#### GitHub Actions Logs:
- Go to **Actions** tab in your repository
- Click on any workflow run to see detailed logs
- Check for any errors in the pipeline execution or file upload

#### PythonAnywhere Files:
- Log into PythonAnywhere
- Check the **Files** tab to verify uploads
- Look in `/home/yourusername/mysite/data/` for updated files

## Troubleshooting

### Common Issues:

1. **Pipeline Execution Fails**:
   - Check the GitHub Actions logs
   - Verify all dependencies are in `requirements.txt`
   - Check if any of your scripts have changed and need updates

2. **File Upload Fails**:
   - Verify API token is correct and not expired
   - Check PythonAnywhere username is correct
   - Ensure you have enough storage space (free tier has 512MB limit)

3. **Schedule Not Working**:
   - GitHub Actions may have delays during high usage
   - Free tier repositories might have limitations
   - Check the **Actions** tab for any failed runs

### Free Tier Limitations:

**GitHub Actions (Free Tier)**:
- 2,000 minutes/month for private repos
- Unlimited for public repos
- Your workflow should use ~5-10 minutes per run

**PythonAnywhere (Free Tier)**:
- 512MB total storage
- Files API access included
- No scheduled tasks (but GitHub handles scheduling)

### File Size Management:

If you hit storage limits, consider:
1. Cleaning up old performance reports
2. Compressing files before upload
3. Only uploading essential files

## Customization

### Adding More Files:
Edit `.github/scripts/upload_to_pythonanywhere.py` and modify the `files_to_upload` list.

### Changing Schedule:
Edit `.github/workflows/automated-pipeline.yml` and modify the cron expressions.

### Different Upload Location:
Modify the `remote_data_dir` variable in the upload script.

## Security Notes

- API tokens are stored securely as GitHub secrets
- Never commit API tokens to your repository
- Regularly rotate your PythonAnywhere API token
- The upload script only has access to your PythonAnywhere files, not the full system