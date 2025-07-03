#!/usr/bin/env python3
"""
Demo script for the Fast Replay Downloader
===========================================

This script demonstrates how to use the fast replay downloader.
Run this to start downloading BAR replays at high speed.
"""

from fast_replay_downloader import run_fast_replay_downloader

if __name__ == "__main__":
    print("🚀 Starting Fast BAR Replay Downloader")
    print("=" * 50)
    print("This will download replay JSONs from the BAR API as fast as possible.")
    print("The website owner has approved high-speed downloading.")
    print("=" * 50)
    
    try:
        run_fast_replay_downloader()
        print("\n✅ Download completed successfully!")
    except KeyboardInterrupt:
        print("\n⚠️ Download interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during download: {e}")
