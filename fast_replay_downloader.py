#!/usr/bin/env python3
"""
BAR Fast Replay Downloader
===========================

High-performance replay downloader for BAR (Beyond All Reason) replays.
Downloads replay JSONs concurrently with proper rate limiting and error handling.
"""

import os
import json
import time
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from typing import Set, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime

from config import config
from utils import setup_logging

# ==============================================================================
# --- Configuration ---
# ==============================================================================

# Replay API Configuration
BASE_REPLAY_URL = "https://api.bar-rts.com/replays/"
REPLAYS_LIST_URL = "https://api.bar-rts.com/replays"

# Download Configuration
MAX_CONCURRENT_DOWNLOADS = 50  # High concurrency since owner approved fast downloads
MAX_PAGES_TO_CHECK = 10000  # Check MANY more pages for comprehensive historical coverage
BATCH_SIZE = 100  # Process replays in batches
REQUEST_TIMEOUT = 30  # Timeout per request
MAX_RETRIES = 3  # Retry failed downloads

# File Paths
REPLAYS_FOLDER = config.paths.data_dir / "replays"
PROCESSED_IDS_FILE = config.paths.data_dir / "processed_replay_ids.txt"
DOWNLOAD_LOG_FILE = config.paths.data_dir / "replay_download.log"
FAILED_DOWNLOADS_FILE = config.paths.data_dir / "failed_replay_downloads.txt"

# ==============================================================================
# --- Fast Replay Downloader Class ---
# ==============================================================================

class FastReplayDownloader:
    """High-performance replay downloader with concurrent processing."""
    
    def __init__(self):
        self.logger = setup_logging("FastReplayDownloader", logging.INFO)
        self.session = None
        self.processed_ids: Set[str] = set()
        self.failed_downloads: Set[str] = set()
        self.download_stats = {
            'total_found': 0,
            'already_processed': 0,
            'newly_downloaded': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Setup SSL configuration like in config
        config.setup_ssl()
        
    async def __aenter__(self):
        """Async context manager entry."""
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_DOWNLOADS + 10,
            limit_per_host=MAX_CONCURRENT_DOWNLOADS,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            ssl=False  # Disable SSL verification like in config
        )
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=config.network.headers
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    def setup_directories(self):
        """Create necessary directories."""
        REPLAYS_FOLDER.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Created/verified replays directory: {REPLAYS_FOLDER}")
    
    def load_processed_ids(self):
        """Load previously processed replay IDs."""
        if PROCESSED_IDS_FILE.exists():
            try:
                with open(PROCESSED_IDS_FILE, 'r', encoding='utf-8') as f:
                    self.processed_ids = {line.strip() for line in f if line.strip()}
                self.logger.info(f"Loaded {len(self.processed_ids)} previously processed replay IDs")
            except Exception as e:
                self.logger.error(f"Error loading processed IDs: {e}")
        
        if FAILED_DOWNLOADS_FILE.exists():
            try:
                with open(FAILED_DOWNLOADS_FILE, 'r', encoding='utf-8') as f:
                    self.failed_downloads = {line.strip() for line in f if line.strip()}
                self.logger.info(f"Loaded {len(self.failed_downloads)} previously failed downloads")
            except Exception as e:
                self.logger.error(f"Error loading failed downloads: {e}")
    
    async def fetch_page_replay_ids(self, page_num: int) -> List[str]:
        """Fetch replay IDs from a specific page."""
        url = f"{REPLAYS_LIST_URL}?page={page_num}"
        
        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        replays = data.get('data', [])
                        replay_ids = [str(replay.get('id')) for replay in replays if replay.get('id')]
                        
                        if not replay_ids:  # Empty page means we've reached the end
                            return []
                            
                        self.logger.debug(f"Page {page_num}: Found {len(replay_ids)} replays")
                        return replay_ids
                    elif response.status == 404:
                        # Page doesn't exist, we've reached the end
                        return []
                    else:
                        self.logger.warning(f"Page {page_num} returned status {response.status}")
                        
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for page {page_num}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    
        self.logger.error(f"Failed to fetch page {page_num} after {MAX_RETRIES} attempts")
        return []
    
    async def collect_all_replay_ids(self) -> List[str]:
        """Collect all replay IDs from all pages concurrently."""
        self.logger.info("Starting to collect replay IDs from all pages...")
        
        # First, determine how many pages we need to check
        all_replay_ids = []
        
        # Process pages in batches to avoid overwhelming the server
        page_batch_size = 20
        current_page = 1
        
        while current_page <= MAX_PAGES_TO_CHECK:
            # Create batch of page numbers
            page_batch = list(range(current_page, min(current_page + page_batch_size, MAX_PAGES_TO_CHECK + 1)))
            
            # Fetch all pages in this batch concurrently
            tasks = [self.fetch_page_replay_ids(page) for page in page_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            empty_pages = 0
            batch_ids = []
            
            for page_num, result in zip(page_batch, results):
                if isinstance(result, Exception):
                    self.logger.error(f"Exception for page {page_num}: {result}")
                    continue
                    
                if not result:  # Empty result
                    empty_pages += 1
                else:
                    batch_ids.extend(result)
            
            all_replay_ids.extend(batch_ids)
            
            # If we got multiple empty pages in a row, we've probably reached the end
            # But be more conservative - only stop if we get many consecutive empty pages
            if empty_pages >= min(len(page_batch), 15):  # Need at least 15 empty pages to stop
                self.logger.info(f"Found {empty_pages} empty pages around page {current_page}, stopping collection")
                break
                
            current_page += page_batch_size
            self.logger.info(f"Processed pages {page_batch[0]}-{page_batch[-1]}: "
                           f"Found {len(batch_ids)} replays, Total: {len(all_replay_ids)}")
            
            # Even smaller delay between batches to go faster through history
            await asyncio.sleep(0.05)
        
        # Remove duplicates while preserving order
        unique_ids = []
        seen = set()
        for replay_id in all_replay_ids:
            if replay_id not in seen:
                unique_ids.append(replay_id)
                seen.add(replay_id)
        
        self.logger.info(f"Collected {len(unique_ids)} unique replay IDs from all pages")
        return unique_ids
    
    async def download_replay_json(self, replay_id: str) -> bool:
        """Download a single replay JSON file."""
        if replay_id in self.processed_ids:
            return True  # Already processed
            
        url = f"{BASE_REPLAY_URL}{replay_id}"
        file_path = REPLAYS_FOLDER / f"{replay_id}.json"
        
        # Skip if file already exists and is valid
        if file_path.exists():
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    json.loads(content)  # Validate JSON
                await self.mark_as_processed(replay_id)
                return True
            except:
                # File is corrupted, re-download
                file_path.unlink(missing_ok=True)
        
        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        replay_data = await response.json()
                        
                        # Save to file
                        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                            await f.write(json.dumps(replay_data, indent=2))
                        
                        await self.mark_as_processed(replay_id)
                        return True
                    else:
                        self.logger.warning(f"Replay {replay_id} returned status {response.status}")
                        
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for replay {replay_id}: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))
        
        # Mark as failed
        await self.mark_as_failed(replay_id)
        return False
    
    async def mark_as_processed(self, replay_id: str):
        """Mark a replay ID as successfully processed."""
        self.processed_ids.add(replay_id)
        async with aiofiles.open(PROCESSED_IDS_FILE, 'a', encoding='utf-8') as f:
            await f.write(f"{replay_id}\n")
    
    async def mark_as_failed(self, replay_id: str):
        """Mark a replay ID as failed to download."""
        self.failed_downloads.add(replay_id)
        async with aiofiles.open(FAILED_DOWNLOADS_FILE, 'a', encoding='utf-8') as f:
            await f.write(f"{replay_id}\n")
    
    async def download_replays_batch(self, replay_ids: List[str]) -> Dict[str, int]:
        """Download a batch of replays concurrently."""
        # Filter out already processed replays
        new_replay_ids = [rid for rid in replay_ids if rid not in self.processed_ids]
        
        if not new_replay_ids:
            return {'success': 0, 'failed': 0, 'skipped': len(replay_ids)}
        
        self.logger.info(f"Downloading batch of {len(new_replay_ids)} replays...")
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        
        async def download_with_semaphore(replay_id):
            async with semaphore:
                return await self.download_replay_json(replay_id)
        
        # Execute downloads concurrently
        tasks = [download_with_semaphore(replay_id) for replay_id in new_replay_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count results
        success_count = sum(1 for r in results if r is True)
        failed_count = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        return {
            'success': success_count,
            'failed': failed_count,
            'skipped': len(replay_ids) - len(new_replay_ids)
        }
    
    async def run_download_pipeline(self):
        """Main download pipeline."""
        self.download_stats['start_time'] = datetime.now()
        self.logger.info("=== Starting Fast Replay Download Pipeline ===")
        
        # Setup
        self.setup_directories()
        self.load_processed_ids()
        
        # Step 1: Collect all replay IDs
        self.logger.info("Step 1: Collecting all replay IDs...")
        all_replay_ids = await self.collect_all_replay_ids()
        self.download_stats['total_found'] = len(all_replay_ids)
        
        if not all_replay_ids:
            self.logger.warning("No replay IDs found!")
            return
        
        # Step 2: Filter new replays
        new_replay_ids = [rid for rid in all_replay_ids if rid not in self.processed_ids]
        self.download_stats['already_processed'] = len(all_replay_ids) - len(new_replay_ids)
        
        self.logger.info(f"Found {len(all_replay_ids)} total replays")
        self.logger.info(f"Already processed: {len(all_replay_ids) - len(new_replay_ids)}")
        self.logger.info(f"New to download: {len(new_replay_ids)}")
        
        if not new_replay_ids:
            self.logger.info("All replays already downloaded!")
            return
        
        # Step 3: Download in batches
        self.logger.info("Step 2: Starting batch downloads...")
        total_downloaded = 0
        total_failed = 0
        
        for i in range(0, len(new_replay_ids), BATCH_SIZE):
            batch = new_replay_ids[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(new_replay_ids) + BATCH_SIZE - 1) // BATCH_SIZE
            
            self.logger.info(f"Processing batch {batch_num}/{total_batches} "
                           f"({len(batch)} replays)...")
            
            batch_results = await self.download_replays_batch(batch)
            total_downloaded += batch_results['success']
            total_failed += batch_results['failed']
            
            self.logger.info(f"Batch {batch_num} complete: "
                           f"{batch_results['success']} downloaded, "
                           f"{batch_results['failed']} failed, "
                           f"{batch_results['skipped']} skipped")
        
        # Update stats
        self.download_stats['newly_downloaded'] = total_downloaded
        self.download_stats['failed'] = total_failed
        self.download_stats['end_time'] = datetime.now()
        
        # Final report
        self.print_final_report()
    
    def print_final_report(self):
        """Print final download statistics."""
        duration = self.download_stats['end_time'] - self.download_stats['start_time']
        
        self.logger.info("=== Download Complete ===")
        self.logger.info(f"Total replays found: {self.download_stats['total_found']:,}")
        self.logger.info(f"Already processed: {self.download_stats['already_processed']:,}")
        self.logger.info(f"Newly downloaded: {self.download_stats['newly_downloaded']:,}")
        self.logger.info(f"Failed downloads: {self.download_stats['failed']:,}")
        self.logger.info(f"Total duration: {duration}")
        
        if self.download_stats['newly_downloaded'] > 0:
            rate = self.download_stats['newly_downloaded'] / duration.total_seconds()
            self.logger.info(f"Download rate: {rate:.2f} replays/second")
        
        self.logger.info(f"Replays saved to: {REPLAYS_FOLDER}")
        self.logger.info(f"Processed IDs logged to: {PROCESSED_IDS_FILE}")
        
        if self.download_stats['failed'] > 0:
            self.logger.info(f"Failed downloads logged to: {FAILED_DOWNLOADS_FILE}")


# ==============================================================================
# --- Main Execution ---
# ==============================================================================

async def main():
    """Main execution function."""
    async with FastReplayDownloader() as downloader:
        await downloader.run_download_pipeline()

def run_fast_replay_downloader():
    """Synchronous wrapper for the async downloader."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
    except Exception as e:
        print(f"Error during download: {e}")
        raise

if __name__ == "__main__":
    run_fast_replay_downloader()
