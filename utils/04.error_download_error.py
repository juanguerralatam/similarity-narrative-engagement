#!/usr/bin/env python3
"""
Error Analysis and Cleanup Utility

Analyzes download errors from our conversations and provides cleanup functions:
1. SSL/VPN connection errors
2. Race condition corrupted entries
3. Archive inconsistencies
4. Failed file verification
5. Cleanup of problematic files
"""
import csv
import logging
import shutil
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Setup paths
DOWNLOADS_DIR = Path.home() / "Downloads" / "YouTube"
CSV_FILE = Path("output/download.csv")
ARCHIVE_FILE = Path("output/download_archive.txt")
BACKUP_DIR = Path("output/backups")

# Setup logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

class ErrorAnalyzer:
    """Analyze and fix the most important download errors from our sessions"""
    
    def __init__(self):
        self.error_stats = {
            'ssl_errors': 0,
            'corrupted_entries': 0,
            'missing_files': 0,
            'archive_mismatches': 0,
            'failed_downloads': 0
        }
    
    def analyze_all_errors(self) -> Dict[str, int]:
        """Run comprehensive error analysis"""
        log.info("🔍 Starting comprehensive error analysis...")
        
        # 1. Check for SSL retry entries (from VPN issues)
        self.error_stats['ssl_errors'] = self._check_ssl_errors()
        
        # 2. Check for corrupted CSV entries (race conditions)
        self.error_stats['corrupted_entries'] = self._check_corrupted_entries()
        
        # 3. Check for missing files (done but no file)
        self.error_stats['missing_files'] = self._check_missing_files()
        
        # 4. Check archive inconsistencies
        self.error_stats['archive_mismatches'] = self._check_archive_inconsistencies()
        
        # 5. Check failed downloads
        self.error_stats['failed_downloads'] = self._check_failed_downloads()
        
        return self.error_stats
    
    def _check_ssl_errors(self) -> int:
        """Check for SSL/VPN related errors"""
        ssl_count = 0
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    status = row.get('status', '').strip()
                    if status == 'ssl_retry':
                        ssl_count += 1
            log.info(f"Found {ssl_count} videos with SSL/VPN errors")
        except Exception as e:
            log.error(f"Error checking SSL errors: {e}")
        return ssl_count
    
    def _check_corrupted_entries(self) -> int:
        """Check for corrupted CSV entries (invalid video IDs)"""
        corrupted_count = 0
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    video_id = row.get('videoId', '').strip()
                    # Invalid video ID patterns from race conditions
                    if (not video_id or 
                        len(video_id) != 11 or 
                        ' ' in video_id or 
                        ',' in video_id or
                        video_id.endswith('_VweaEx1j62do_vQ')):
                        corrupted_count += 1
            log.info(f"Found {corrupted_count} corrupted CSV entries")
        except Exception as e:
            log.error(f"Error checking corrupted entries: {e}")
        return corrupted_count
    
    def _check_missing_files(self) -> int:
        """Check for videos marked done but files missing"""
        missing_count = 0
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('status', '').strip() == 'done':
                        video_id = row.get('videoId', '').strip()
                        if not self._file_exists(video_id):
                            missing_count += 1
            log.info(f"Found {missing_count} missing files (marked done but no file)")
        except Exception as e:
            log.error(f"Error checking missing files: {e}")
        return missing_count
    
    def _check_archive_inconsistencies(self) -> int:
        """Check for archive/CSV mismatches"""
        mismatch_count = 0
        try:
            # Get CSV done videos
            csv_done = set()
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('status', '').strip() == 'done':
                        csv_done.add(row.get('videoId', '').strip())
            
            # Get archive videos
            archive_videos = set()
            if ARCHIVE_FILE.exists():
                with ARCHIVE_FILE.open('r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip().startswith('youtube '):
                            archive_videos.add(line.strip().split(' ', 1)[1])
            
            mismatch_count = len(csv_done.symmetric_difference(archive_videos))
            log.info(f"Found {mismatch_count} CSV/Archive mismatches")
        except Exception as e:
            log.error(f"Error checking archive inconsistencies: {e}")
        return mismatch_count
    
    def _check_failed_downloads(self) -> int:
        """Check for permanently failed downloads"""
        failed_count = 0
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    status = row.get('status', '').strip()
                    if status in ['failed', 'unavailable']:
                        failed_count += 1
            log.info(f"Found {failed_count} permanently failed downloads")
        except Exception as e:
            log.error(f"Error checking failed downloads: {e}")
        return failed_count
    
    def _file_exists(self, video_id: str) -> bool:
        """Check if video file exists"""
        extensions = ['.mp4', '.webm', '.mkv', '.m4a']
        for ext in extensions:
            file_path = DOWNLOADS_DIR / f"{video_id}{ext}"
            if file_path.exists() and file_path.stat().st_size > 1024:
                return True
        return False

class ErrorCleaner:
    """Clean up problematic files and entries"""
    
    def __init__(self):
        self.backup_dir = BACKUP_DIR
        self.backup_dir.mkdir(exist_ok=True)
    
    def cleanup_all_errors(self, analyzer_stats: Dict[str, int]) -> None:
        """Clean up all identified error types"""
        log.info("🧹 Starting comprehensive error cleanup...")
        
        # Create backup first
        self._create_backup()
        
        # 1. Reset SSL errors for retry
        if analyzer_stats['ssl_errors'] > 0:
            self._reset_ssl_errors()
        
        # 2. Remove corrupted entries
        if analyzer_stats['corrupted_entries'] > 0:
            self._remove_corrupted_entries()
        
        # 3. Fix missing files (reset status)
        if analyzer_stats['missing_files'] > 0:
            self._fix_missing_files()
        
        # 4. Synchronize archive
        if analyzer_stats['archive_mismatches'] > 0:
            self._synchronize_archive()
        
        # 5. Clean failed download files
        self._cleanup_failed_files()
        
        log.info("✅ Error cleanup completed!")
    
    def _create_backup(self) -> None:
        """Create timestamped backup"""
        timestamp = int(time.time())
        if CSV_FILE.exists():
            backup_csv = self.backup_dir / f"download_error_cleanup_{timestamp}.csv"
            shutil.copy2(CSV_FILE, backup_csv)
            log.info(f"Backed up CSV to {backup_csv}")
        
        if ARCHIVE_FILE.exists():
            backup_archive = self.backup_dir / f"archive_error_cleanup_{timestamp}.txt"
            shutil.copy2(ARCHIVE_FILE, backup_archive)
            log.info(f"Backed up archive to {backup_archive}")
    
    def _reset_ssl_errors(self) -> None:
        """Reset SSL retry entries to pending for retry"""
        updated_rows = []
        reset_count = 0
        
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                for row in reader:
                    if row.get('status', '').strip() == 'ssl_retry':
                        row['status'] = ''  # Reset to pending
                        reset_count += 1
                        log.debug(f"Reset SSL error: {row.get('videoId')}")
                    updated_rows.append(row)
            
            # Write updated CSV
            with CSV_FILE.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(updated_rows)
            
            log.info(f"Reset {reset_count} SSL errors for retry")
        except Exception as e:
            log.error(f"Error resetting SSL errors: {e}")
    
    def _remove_corrupted_entries(self) -> None:
        """Remove corrupted CSV entries"""
        valid_rows = []
        removed_count = 0
        
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                for row in reader:
                    video_id = row.get('videoId', '').strip()
                    # Check if valid video ID
                    if (video_id and 
                        len(video_id) == 11 and 
                        ' ' not in video_id and 
                        ',' not in video_id and
                        not video_id.endswith('_VweaEx1j62do_vQ')):
                        valid_rows.append(row)
                    else:
                        removed_count += 1
                        log.debug(f"Removed corrupted entry: {video_id}")
            
            # Write cleaned CSV
            with CSV_FILE.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(valid_rows)
            
            log.info(f"Removed {removed_count} corrupted entries")
        except Exception as e:
            log.error(f"Error removing corrupted entries: {e}")
    
    def _fix_missing_files(self) -> None:
        """Reset status for videos marked done but missing files"""
        updated_rows = []
        reset_count = 0
        
        try:
            with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                
                for row in reader:
                    if row.get('status', '').strip() == 'done':
                        video_id = row.get('videoId', '').strip()
                        if not self._file_exists(video_id):
                            row['status'] = ''  # Reset to pending
                            reset_count += 1
                            log.debug(f"Reset missing file: {video_id}")
                    updated_rows.append(row)
            
            # Write updated CSV
            with CSV_FILE.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(updated_rows)
            
            log.info(f"Reset {reset_count} entries with missing files")
        except Exception as e:
            log.error(f"Error fixing missing files: {e}")
    
    def _synchronize_archive(self) -> None:
        """Rebuild archive from actual files"""
        try:
            actual_files = set()
            extensions = ['.mp4', '.webm', '.mkv', '.m4a']
            
            for ext in extensions:
                for file_path in DOWNLOADS_DIR.glob(f"*{ext}"):
                    video_id = file_path.stem
                    if len(video_id) >= 10:
                        actual_files.add(video_id)
            
            # Write new archive
            with ARCHIVE_FILE.open('w', encoding='utf-8') as f:
                for video_id in sorted(actual_files):
                    f.write(f"youtube {video_id}\n")
            
            log.info(f"Synchronized archive with {len(actual_files)} verified files")
        except Exception as e:
            log.error(f"Error synchronizing archive: {e}")
    
    def _cleanup_failed_files(self) -> None:
        """Clean up any partial files from failed downloads"""
        cleaned_count = 0
        try:
            # Look for small files (likely incomplete)
            for file_path in DOWNLOADS_DIR.glob("*"):
                if file_path.is_file() and file_path.stat().st_size < 1024:  # Less than 1KB
                    file_path.unlink()
                    cleaned_count += 1
                    log.debug(f"Removed small file: {file_path.name}")
            
            log.info(f"Cleaned up {cleaned_count} small/incomplete files")
        except Exception as e:
            log.error(f"Error cleaning up files: {e}")
    
    def _file_exists(self, video_id: str) -> bool:
        """Check if video file exists"""
        extensions = ['.mp4', '.webm', '.mkv', '.m4a']
        for ext in extensions:
            file_path = DOWNLOADS_DIR / f"{video_id}{ext}"
            if file_path.exists() and file_path.stat().st_size > 1024:
                return True
        return False

def check_sync_status():
    """Quick synchronization check"""
    actual_files = set()
    extensions = ['.mp4', '.webm', '.mkv', '.m4a']
    
    for ext in extensions:
        for file_path in DOWNLOADS_DIR.glob(f"*{ext}"):
            video_id = file_path.stem
            if len(video_id) >= 10:  
                actual_files.add(video_id)
    
    csv_done = set()
    with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('status', '').strip() == 'done':
                video_id = row.get('videoId', '').strip()
                if video_id:
                    csv_done.add(video_id)
    
    archive_videos = set()
    if ARCHIVE_FILE.exists():
        with ARCHIVE_FILE.open('r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('youtube '):
                    video_id = line.split(' ', 1)[1]
                    archive_videos.add(video_id)
    
    print(f"Files: {len(actual_files)}, CSV: {len(csv_done)}, Archive: {len(archive_videos)}")
    
    if len(actual_files) == len(csv_done) == len(archive_videos) and actual_files == csv_done == archive_videos:
        print("✅ SYNCHRONIZED")
        return True
    else:
        print("❌ NOT SYNCHRONIZED")
        return False

def sync_all_sources():
    """Synchronize all sources based on actual files"""
    actual_files = set()
    extensions = ['.mp4', '.webm', '.mkv', '.m4a']
    
    for ext in extensions:
        for file_path in DOWNLOADS_DIR.glob(f"*{ext}"):
            video_id = file_path.stem
            if len(video_id) >= 10:  
                actual_files.add(video_id)
    
    # Update CSV
    rows = []
    with CSV_FILE.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            video_id = row.get('videoId', '').strip()
            if video_id in actual_files:
                row['status'] = 'done'
            elif row.get('status', '').strip() == 'done':
                row['status'] = ''
            rows.append(row)
    
    with CSV_FILE.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    # Rebuild archive
    with ARCHIVE_FILE.open('w', encoding='utf-8') as f:
        for video_id in sorted(actual_files):
            f.write(f"youtube {video_id}\n")
    
    print(f"✅ Synchronized {len(actual_files)} files")

def main():
    """Main error analysis and cleanup function"""
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "check":
            check_sync_status()
            return
        elif sys.argv[1] == "sync":
            sync_all_sources()
            return
    
    print("🚀 Download Error Analysis & Cleanup Tool")
    print("=" * 50)
    
    # Initialize analyzer and cleaner
    analyzer = ErrorAnalyzer()
    cleaner = ErrorCleaner()
    
    # Analyze errors
    error_stats = analyzer.analyze_all_errors()
    
    # Show summary
    print("\n📊 ERROR ANALYSIS SUMMARY:")
    print(f"  SSL/VPN errors: {error_stats['ssl_errors']}")
    print(f"  Corrupted entries: {error_stats['corrupted_entries']}")
    print(f"  Missing files: {error_stats['missing_files']}")
    print(f"  Archive mismatches: {error_stats['archive_mismatches']}")
    print(f"  Failed downloads: {error_stats['failed_downloads']}")
    
    total_errors = sum(error_stats.values())
    if total_errors == 0:
        print("\n✅ No errors found! Everything looks good.")
        print("\nUsage: python utils/04.error_download_error.py [check|sync]")
        return
    
    print(f"\n🔧 Total errors to fix: {total_errors}")
    
    # Ask for confirmation
    response = input("\nProceed with cleanup? (y/N): ").lower().strip()
    if response == 'y' or response == 'yes':
        cleaner.cleanup_all_errors(error_stats)
        print("\n🎉 Cleanup completed!")
    else:
        print("\n⏸️ Cleanup cancelled.")

if __name__ == "__main__":
    main()