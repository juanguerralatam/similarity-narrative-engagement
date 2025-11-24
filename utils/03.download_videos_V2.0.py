import os
import sys
import csv
import time
import random
import logging
import argparse
import threading
import fcntl  # For multi-process file locking
import uuid
import numpy as np
from pathlib import Path
from typing import List, Tuple, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
import yt_dlp
from dataclasses import dataclass

# Configure proxy
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7897'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7897'
os.environ['NO_PROXY'] = 'localhost,127.0.0.1'

class VideoStatus(str, Enum):
    """Video download status enumeration"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"
    CAPTCHA_CHALLENGE = "captcha_challenge"
    SSL_RETRY = "ssl_retry"  # For VPN/SSL issues that should be retried

@dataclass
class Config:
    CSV_FILE: Path = Path("output/download.csv")  # FIXED: Correct path
    OUTPUT_DIR: Path = Path.home() / "Downloads" / "YouTube"
    CONCURRENT_FRAGMENTS: int = 8  # Reduced for stability
    MIN_BATCH_SIZE: int = 4        # Smaller batches
    MAX_BATCH_SIZE: int = 8        # More human-like
    cookies_dir: Path = Path("input/Cookies")
    ARCHIVE_FILE: Path = Path("output/download_archive.txt")
    SLEEP_MIN: float = 3.0
    SLEEP_MAX: float = 12.0         # Increased range for more human-like
    MAX_WORKERS: int = 2           # Reduced workers
    FRAGMENT_RETRIES: int = 10     # More retries
    RETRIES: int = 5               # More retries
    BATCH_PAUSE_MEAN: float = 30.0 # Increased mean pause between batches
    BATCH_PAUSE_STD: float = 15.0   # Increased standard deviation

config = Config()

config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

csv_lock = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("output/download_log.txt", mode='a'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
]
ACCEPT_LANGUAGES = ["en-US,en;q=0.9", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
                    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"]
YOUTUBE_CLIENTS = ["web", "mweb", "android", "ios"]  # More stable clients

@contextmanager
def atomic_csv_update(csv_file: Path):
    """Thread-safe atomic CSV update with unique temp file naming"""
    temp_file = csv_file.parent / f'{csv_file.stem}_temp_{uuid.uuid4().hex[:8]}{csv_file.suffix}'
    try:
        yield temp_file
        temp_file.replace(csv_file)
    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise

def verify_downloaded_file(video_id: str, additional_dirs: List[Path] = None) -> bool:
    """Verify that a video file was actually downloaded successfully"""
    possible_extensions = ['.mp4', '.webm', '.mkv', '.m4a', '.mp3']
    
    # Check main download directory
    search_dirs = [config.OUTPUT_DIR]
    
    # Add additional directories if provided
    if additional_dirs:
        search_dirs.extend(additional_dirs)
    
    for search_dir in search_dirs:
        for ext in possible_extensions:
            file_path = search_dir / f"{video_id}{ext}"
            if file_path.exists() and file_path.stat().st_size > 1024:  # At least 1KB
                log.debug(f"Verified downloaded file: {file_path}")
                return True
    return False

def extract_video_id(url: str) -> str:
    return url.split('v=')[-1].split('&')[0]

def human_sleep(base_time: float = 5.0, variation: float = 3.0, min_time: float = 2.0) -> float:
    """Generate human-like sleep time using Gaussian distribution with occasional longer breaks"""
    # 10% chance of much longer break (simulating user distraction)
    if random.random() < 0.10:
        sleep_time = np.random.normal(45.0, 15.0)  # Long break like user switching tasks
        sleep_time = max(10.0, sleep_time)
        log.debug(f"Taking long human break: {sleep_time:.1f} seconds")
    else:
        sleep_time = np.random.normal(base_time, variation)
        sleep_time = max(min_time, sleep_time)  # Ensure minimum
    
    # Add small random jitter
    jitter = random.uniform(-0.5, 0.5)
    sleep_time += jitter
    sleep_time = max(min_time, sleep_time)
    
    log.debug(f"Sleeping for {sleep_time:.1f} seconds")
    time.sleep(sleep_time)
    return sleep_time

def batch_pause() -> float:
    """Generate pause between batches with occasional longer breaks and human-like patterns"""
    # 20% chance of longer break (human-like browsing behavior)
    if random.random() < 0.20:
        pause_time = np.random.normal(120, 45)  # ~2 min break (user checking other sites)
        pause_time = max(45, pause_time)
        log.info(f"Taking extended browsing break: {pause_time:.0f}s")
    # 5% chance of very long break (user taking a call, eating, etc.)
    elif random.random() < 0.05:
        pause_time = np.random.normal(300, 120)  # ~5 min break
        pause_time = max(120, pause_time)
        log.info(f"Taking very long break: {pause_time:.0f}s")
    else:
        pause_time = np.random.normal(config.BATCH_PAUSE_MEAN, config.BATCH_PAUSE_STD)
        pause_time = max(10, pause_time)
    
    # Add random jitter
    jitter = random.uniform(-5, 5)
    pause_time += jitter
    pause_time = max(5, pause_time)
    
    time.sleep(pause_time)
    return pause_time

def progress_hook(d):
    if d['status'] == 'finished':
        log.info(f"[DOWNLOAD FINISHED] {d['filename']}")
    elif d['status'] == 'error':
        log.error(f"[DOWNLOAD ERROR] {d.get('filename', 'unknown')}: {d.get('error', 'unknown error')}")

def build_yt_dlp_opts(disable_proxy: bool = False) -> dict:
    opts = {
        "outtmpl": str(config.OUTPUT_DIR / "%(id)s.%(ext)s"),
        "format_sort": ["+size", "+br", "+res", "+fps"],
        "concurrent_fragments": min(config.CONCURRENT_FRAGMENTS, 4),  # Reduced for stability
        "sleep_interval": random.uniform(config.SLEEP_MIN, config.SLEEP_MAX) + random.uniform(0.5, 2.0),
        "max_sleep_interval": random.uniform(config.SLEEP_MIN * 1.5, config.SLEEP_MAX * 1.5) + random.uniform(1.0, 4.0),
        "retries": 10,  # Increased for SSL issues
        "fragment_retries": 15,  # Increased for SSL issues
        "retry_sleep": 5.0,  # Wait 5 seconds between retries
        "retry_sleep_functions": {"http": lambda n: min(5 + n * 2, 30)},  # Progressive backoff
        "socket_timeout": 30,  # Longer socket timeout for VPN
        "quiet": True,
        "no_warnings": True,
        # Removed download_archive to prevent overwriting existing entries
        "progress_hooks": [progress_hook],
        "user_agent": random.choice(USER_AGENTS),
        "nocheckcertificate": True,  # Disable SSL certificate verification to avoid SSL errors
        "http_headers": {
            "Accept-Language": random.choice(ACCEPT_LANGUAGES),
            "Referer": "https://www.youtube.com/",
            "Connection": "keep-alive",
            "Keep-Alive": "timeout=30, max=100",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",  # Do Not Track header
            "Upgrade-Insecure-Requests": "1"
        },
        "extractor_retries": config.RETRIES,
        "skip_unavailable_fragments": True,
        "keep_fragments": False,
        "ignoreerrors": True,
        "no_check_certificate": False,  # Revert to default for stability
        "prefer_insecure": False,  # Revert to default for stability
        "http_chunk_size": 1024*1024,  # 1MB chunks
        "extractor_args": {
            "youtube": {
                "innertube_client": random.choice(YOUTUBE_CLIENTS),
                # Removed aggressive comment disabling that might trigger detection
            }
        }
    }
    
    # Add proxy only if not disabled
    if not disable_proxy:
        opts["proxy"] = "http://127.0.0.1:7897"
    
    log.info(f"Using '{opts['extractor_args']['youtube']['innertube_client']}' client for this batch.")
    opts.update(get_cookies_config())
    
    if "cookiefile" not in opts and "cookiesfrombrowser" not in opts:
        log.warning("No authentication method available. Bot detection likely increased.")
    
    return opts

def is_cookie_file_valid(cookie_file: Path) -> bool:
    """Enhanced cookie validation with expiration check"""
    if not cookie_file.exists() or cookie_file.stat().st_size < 10:
        return False
    
    current_time = int(time.time())
    
    try:
        with open(cookie_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(1024)
            youtube_domains = ['youtube.com', '.google.com']
            has_youtube = any(domain in content for domain in youtube_domains)
            
            # Basic validation for YouTube domains
            if not has_youtube:
                return False
                
            return True
            
    except Exception as e:
        log.warning(f"Error validating cookie file {cookie_file}: {e}")
        return False

def get_cookies_files() -> List[Path]:
    if not config.cookies_dir.exists():
        return []
    return [f for f in config.cookies_dir.glob("*.txt") if f.is_file()]

def rotate_cookies() -> Optional[str]:
    valid_files = [f for f in get_cookies_files() if is_cookie_file_valid(f)]
    if not valid_files:
        log.info("No valid cookie files found")
        return None
    
    valid_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    weights = [0.5, 0.3] + [0.2 / max(1, len(valid_files) - 2)] * (len(valid_files) - 2) if len(valid_files) > 2 else ([0.67, 0.33] if len(valid_files) == 2 else [1.0])
    selected = random.choices(valid_files, weights=weights[:len(valid_files)])[0]
    log.info(f"Selected cookie file: {selected.name} (one of {len(valid_files)} valid files)")
    return str(selected)

def get_cookies_config() -> dict:
    cookie_file = rotate_cookies()
    if cookie_file:
        log.info(f"Using cookies from file: {cookie_file}")
        return {"cookiefile": cookie_file}
    log.warning("No valid cookie sources available. Bot detection likely.")
    return {}

def modify_csv_rows(modifier_func: Callable[[dict], dict]) -> int:
    """Thread-safe CSV modification with improved error handling"""
    if not config.CSV_FILE.exists():
        return 0
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with csv_lock, atomic_csv_update(config.CSV_FILE) as temp_file:
                modified_count = 0
                with config.CSV_FILE.open('r', newline='', encoding='utf-8') as infile, \
                     temp_file.open('w', newline='', encoding='utf-8') as outfile:
                    reader = csv.DictReader(infile)
                    fieldnames = reader.fieldnames
                    if fieldnames and 'status' not in fieldnames:
                        fieldnames.append('status')
                    
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row in reader:
                        original_row = row.copy()
                        modified_row = modifier_func(row)
                        if modified_row != original_row:
                            modified_count += 1
                        writer.writerow(modified_row)
                        
                return modified_count
        except (PermissionError, OSError) as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 0.5
                log.warning(f"CSV file busy, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                log.error(f"Failed to modify CSV after {max_retries} attempts: {e}")
        except Exception as e:
            log.error(f"Unexpected error modifying CSV: {e}")
            break
    return 0

def update_csv_status(video_id: str, status: str):
    """Update CSV status with proper file locking for multi-process safety"""
    if not config.CSV_FILE.exists():
        return

    # Use a more unique temp file name to avoid conflicts between processes
    import os
    pid = os.getpid()
    timestamp = int(time.time() * 1000000)  # microsecond precision
    temp_file = config.CSV_FILE.parent / f'download_temp_{pid}_{timestamp}.csv'
    
    max_retries = 5
    retry_delay = 0.1
    
    for attempt in range(max_retries):
        try:
            # Use file locking to prevent race conditions between processes
            with config.CSV_FILE.open('r+', newline='', encoding='utf-8') as csvfile:
                # Lock the file for exclusive access
                fcntl.flock(csvfile.fileno(), fcntl.LOCK_EX)
                
                try:
                    csvfile.seek(0)
                    reader = csv.DictReader(csvfile)
                    fieldnames = reader.fieldnames
                    if fieldnames and 'status' not in fieldnames:
                        fieldnames.append('status')

                    rows = []
                    found = False
                    
                    for row in reader:
                        if row.get('videoId') == video_id:
                            row['status'] = status
                            found = True
                        rows.append(row)
                    
                    if found:
                        # Write to temporary file first
                        with temp_file.open('w', newline='', encoding='utf-8') as tempfile:
                            writer = csv.DictWriter(tempfile, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(rows)
                        
                        # Atomically replace the original file
                        temp_file.replace(config.CSV_FILE)
                        log.debug(f"Updated status for {video_id}: {status}")
                        break
                    else:
                        log.warning(f"Video ID {video_id} not found in CSV")
                        break
                        
                finally:
                    # Unlock the file
                    fcntl.flock(csvfile.fileno(), fcntl.LOCK_UN)
                    
        except (IOError, OSError) as e:
            if attempt < max_retries - 1:
                log.warning(f"CSV update attempt {attempt + 1} failed for {video_id}, retrying in {retry_delay}s: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                log.error(f"Failed to update CSV status after {max_retries} attempts for {video_id}: {e}")
        except Exception as e:
            log.error(f"Unexpected error updating CSV status for {video_id}: {e}")
            break
        finally:
            # Clean up temp file if it exists
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass

def add_to_archive(video_id: str):
    """Add video ID to download archive after successful verification"""
    try:
        with config.ARCHIVE_FILE.open('a', encoding='utf-8') as f:
            f.write(f"youtube {video_id}\n")
        log.debug(f"Added {video_id} to archive")
    except Exception as e:
        log.error(f"Error adding {video_id} to archive: {e}")

def remove_from_archive(video_id: str):
    """Remove video ID from download archive if download failed"""
    if not config.ARCHIVE_FILE.exists():
        return
    
    try:
        with config.ARCHIVE_FILE.open('r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Filter out the failed video
        updated_lines = [line for line in lines if not line.strip().endswith(video_id)]
        
        if len(updated_lines) < len(lines):
            with config.ARCHIVE_FILE.open('w', encoding='utf-8') as f:
                f.writelines(updated_lines)
            log.debug(f"Removed {video_id} from archive")
    except Exception as e:
        log.error(f"Error removing {video_id} from archive: {e}")

def generate_archive_from_csv():
    """Generate archive file from CSV done entries - used for initial setup only"""
    if not config.CSV_FILE.exists():
        return
    
    # Only regenerate archive if it doesn't exist
    if config.ARCHIVE_FILE.exists():
        log.debug("Archive file already exists, skipping regeneration")
        return

    done_ids = []
    try:
        with config.CSV_FILE.open('r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('status') == VideoStatus.DONE and row.get('videoId'):
                    # Only add to archive if file actually exists
                    if verify_downloaded_file(row['videoId']):
                        done_ids.append(f"youtube {row['videoId']}")
                    else:
                        log.warning(f"CSV marked as done but file missing, not adding to archive: {row['videoId']}")
    except Exception as e:
        log.error(f"Error reading CSV for archive: {e}")
        return

    try:
        # Only write if we have valid entries
        if done_ids:
            with config.ARCHIVE_FILE.open('w', encoding='utf-8') as f:
                for line in done_ids:
                    f.write(line + '\n')
            log.info(f"Generated archive with {len(done_ids)} verified videos")
        else:
            # Create empty archive file
            config.ARCHIVE_FILE.touch()
            log.info("Created empty archive file")
    except Exception as e:
        log.error(f"Error writing archive: {e}")

def rebuild_archive_from_files(additional_dirs: List[str] = None, preserve_existing: bool = True):
    """Rebuild archive based on actual downloaded files in directories"""
    if not config.OUTPUT_DIR.exists():
        log.warning(f"Output directory does not exist: {config.OUTPUT_DIR}")
        return
    
    try:
        # Prepare search directories
        search_dirs = [config.OUTPUT_DIR]
        if additional_dirs:
            for dir_str in additional_dirs:
                dir_path = Path(dir_str)
                if dir_path.exists():
                    search_dirs.append(dir_path)
                else:
                    log.warning(f"Additional directory not found: {dir_path}")
        
        # Load existing archive entries if preserving
        existing_entries = set()
        if preserve_existing and config.ARCHIVE_FILE.exists():
            with config.ARCHIVE_FILE.open('r', encoding='utf-8') as f:
                existing_entries = {line.strip() for line in f if line.strip()}
        
        # Find all video files in all directories
        video_extensions = ['.mp4', '.webm', '.mkv', '.m4a', '.mp3']
        downloaded_files = set(existing_entries)  # Start with existing entries
        
        for search_dir in search_dirs:
            log.info(f"Searching for video files in: {search_dir}")
            for ext in video_extensions:
                for file_path in search_dir.glob(f"*{ext}"):
                    if file_path.is_file() and file_path.stat().st_size > 1024:
                        video_id = file_path.stem
                        downloaded_files.add(f"youtube {video_id}")
        
        # Sort for consistent output
        downloaded_files = sorted(list(downloaded_files))
        
        # Write to archive file
        with config.ARCHIVE_FILE.open('w', encoding='utf-8') as f:
            for entry in downloaded_files:
                f.write(entry + '\n')
        
        total_new = len(downloaded_files) - len(existing_entries)
        log.info(f"Archive updated: {len(downloaded_files)} total entries ({total_new} new)")
        
        # Update CSV status for these files
        video_ids_set = {entry.split(' ', 1)[1] for entry in downloaded_files if ' ' in entry}
        
        def update_downloaded_status(row: dict) -> dict:
            if row.get('videoId') in video_ids_set and row.get('status') != VideoStatus.DONE:
                row['status'] = VideoStatus.DONE
            return row
        
        updated = modify_csv_rows(update_downloaded_status)
        if updated > 0:
            log.info(f"Updated {updated} CSV entries to '{VideoStatus.DONE}' status")
            
    except Exception as e:
        log.error(f"Error rebuilding archive from files: {e}")

def update_csv_from_archive():
    if not config.ARCHIVE_FILE.exists() or not config.CSV_FILE.exists():
        return
    try:
        with config.ARCHIVE_FILE.open('r', encoding='utf-8') as f:
            archived_ids = {line.strip().split(' ', 1)[1] for line in f if line.strip().startswith('youtube ')}
        
        def modifier(row: dict) -> dict:
            vid = row.get('videoId')
            if vid in archived_ids and row.get('status') != VideoStatus.DONE:
                row['status'] = VideoStatus.DONE
            return row
        
        updated = modify_csv_rows(modifier)
        if updated > 0:
            log.info(f"Updated {updated} videos to '{VideoStatus.DONE}' from archive")
    except Exception as e:
        log.error(f"Error updating CSV from archive: {e}")

def categorize_download_error(error_message: str) -> VideoStatus:
    """Categorize download error into appropriate status"""
    error_lower = error_message.lower()
    
    if "captcha" in error_lower or "challenge" in error_lower:
        return VideoStatus.CAPTCHA_CHALLENGE
    elif "unavailable" in error_lower:
        return VideoStatus.UNAVAILABLE
    else:
        return VideoStatus.FAILED

def download_batch(urls: List[str], disable_proxy: bool = False) -> Tuple[int, int, List[str]]:
    """
    Download a batch of YouTube videos with proper verification.
    
    Returns:
        Tuple of (success_count, fail_count, captcha_challenged_urls)
    """
    if not urls:
        return 0, 0, []

    ydl_opts = build_yt_dlp_opts(disable_proxy)
    captcha_challenged_urls = []
    success = 0
    
    log.info(f"Batch processing: {len(urls)} videos")

    for idx, url in enumerate(urls):
        video_id = extract_video_id(url)
        update_csv_status(video_id, VideoStatus.IN_PROGRESS)
        
        # Check if file already exists and is verified
        if verify_downloaded_file(video_id):
            log.info(f"Video {video_id} already downloaded and verified, skipping")
            update_csv_status(video_id, VideoStatus.DONE)
            success += 1
            continue
            
        try:
            # Human-like delay between downloads
            if idx > 0:
                human_sleep(base_time=5.0, variation=3.0, min_time=2.0)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
                
                # Verify the download actually succeeded
                if verify_downloaded_file(video_id):
                    success += 1
                    update_csv_status(video_id, VideoStatus.DONE)
                    add_to_archive(video_id)  # Only add to archive after verification
                    log.info(f"Successfully downloaded and verified: {video_id}")
                else:
                    log.warning(f"Download reported success but file not found: {video_id}")
                    update_csv_status(video_id, VideoStatus.FAILED)
                    remove_from_archive(video_id)
                    
        except yt_dlp.utils.DownloadError as e:
            error_message = str(e)
            
            if "captcha" in error_message.lower() or "challenge" in error_message.lower():
                log.warning(f"Captcha challenge detected for {video_id}: {error_message}")
                captcha_challenged_urls.append(url)
                update_csv_status(video_id, VideoStatus.CAPTCHA_CHALLENGE)
                remove_from_archive(video_id)
            elif "ssl" in error_message.lower() or "eof" in error_message.lower() or "connection" in error_message.lower():
                log.warning(f"SSL/Connection error for {video_id} (VPN related): {error_message}")
                log.info(f"Retrying {video_id} in 10 seconds due to VPN connection issue...")
                time.sleep(10)  # Wait longer for VPN to stabilize
                
                # Retry once with fresh connection
                try:
                    with yt_dlp.YoutubeDL(build_yt_dlp_opts(disable_proxy)) as retry_ydl:
                        retry_ydl.download([url])
                        if verify_downloaded_file(video_id):
                            success += 1
                            update_csv_status(video_id, VideoStatus.DONE)
                            add_to_archive(video_id)
                            log.info(f"Successfully downloaded on retry: {video_id}")
                        else:
                            log.warning(f"Retry failed for {video_id}, will try again later")
                            update_csv_status(video_id, VideoStatus.SSL_RETRY)  # Special status for SSL issues
                            remove_from_archive(video_id)
                except Exception as retry_e:
                    log.warning(f"Retry also failed for {video_id}: {str(retry_e)}")
                    update_csv_status(video_id, VideoStatus.SSL_RETRY)  # Mark for later retry instead of failed
                    remove_from_archive(video_id)
            elif "unavailable" in error_message.lower():
                log.warning(f"Video {video_id} is unavailable: {error_message}")
                update_csv_status(video_id, VideoStatus.UNAVAILABLE)
                remove_from_archive(video_id)
            else:
                log.error(f"Error downloading {video_id}: {error_message}")
                update_csv_status(video_id, VideoStatus.FAILED)
                remove_from_archive(video_id)
        except Exception as e:
            log.error(f"Unexpected error downloading {video_id}: {str(e)}")
            update_csv_status(video_id, VideoStatus.FAILED)
            remove_from_archive(video_id)
    
    fail = len(urls) - success
    if fail > 0:
        log.warning(f"Batch finished with {fail} failure(s), {len(captcha_challenged_urls)} captcha challenges")
    else:
        log.info("Batch completed successfully with no failures")
    return success, fail, captcha_challenged_urls

def load_videos_from_csv(channel_id: Optional[str] = None) -> List[str]:
    """Load video URLs from CSV using videoId (FIXED: No longer depends on video_url column)"""
    if not config.CSV_FILE.exists():
        log.error(f"CSV file not found: {config.CSV_FILE}")
        sys.exit(1)
    try:
        with config.CSV_FILE.open(newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader if row.get('videoId')]  # FIXED: Use videoId instead of video_url
            if channel_id:
                rows = [row for row in rows if row.get('channelId') == channel_id]
            video_ids = [row['videoId'] for row in rows if row.get('status') not in [VideoStatus.DONE, VideoStatus.UNAVAILABLE]]
            urls = [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids]  # FIXED: Construct URLs
            random.shuffle(urls)
            log.info(f"Found {len(urls)} videos to download" + (f" for channel {channel_id}" if channel_id else ""))
            return urls
    except Exception as e:
        log.error(f"CSV read error: {e}")
        sys.exit(1)

def main(channel_id: Optional[str] = None, disable_proxy: bool = False):
    # First, rebuild archive from actual downloaded files while preserving existing entries
    log.info("Rebuilding archive from actual downloaded files (preserving existing entries)...")
    rebuild_archive_from_files(preserve_existing=True)
    
    urls = load_videos_from_csv(channel_id)
    if not urls:
        log.info("No new videos to download" + (f" for channel {channel_id}" if channel_id else ""))
        return

    start_time = time.time()
    total_success = total_fail = 0

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = []
        batch_num = 1
        i = 0
        while i < len(urls):
            batch_size = random.randint(config.MIN_BATCH_SIZE, config.MAX_BATCH_SIZE)
            batch_urls = urls[i:i + batch_size]
            if not batch_urls:
                break
            log.info(f"--- Submitting batch {batch_num} ({len(batch_urls)} videos) ---")
            futures.append(executor.submit(download_batch, batch_urls, disable_proxy))
            i += len(batch_urls)
            batch_num += 1
            
            # Human-like pause between batch submissions
            if i < len(urls):
                batch_pause()
        
        for future in as_completed(futures):
            success, fail, captcha_challenged_urls = future.result()
            total_success += success
            total_fail += fail
            log.info(f"Batch completed: {success} downloaded, {fail} failed.")

    update_csv_from_archive()
    runtime = (time.time() - start_time) / 60
    log.info(f"=== Download Complete: {total_success} success, {total_fail} failed in {runtime:.1f} minutes ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download YouTube videos from CSV with multi-threaded batch processing.")
    parser.add_argument('--channel-id', type=str, help='Optional channel ID to filter videos by')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy for testing (default: use proxy)')
    parser.add_argument('--rebuild-archive', action='store_true', help='Rebuild archive from actual downloaded files and exit')
    parser.add_argument('--additional-dirs', nargs='+', help='Additional directories to search for downloaded files')
    args = parser.parse_args()
    
    if args.rebuild_archive:
        log.info("Rebuilding archive from downloaded files...")
        rebuild_archive_from_files(additional_dirs=args.additional_dirs, preserve_existing=True)
        sys.exit(0)
    
    log.info(f"Download configuration: Output={config.OUTPUT_DIR}, Workers={config.MAX_WORKERS}")
    log.info(f"Batch size range: {config.MIN_BATCH_SIZE}-{config.MAX_BATCH_SIZE}, Using proxy: {not args.no_proxy}")
    log.info(f"Human-like timing: Gaussian sleep patterns enabled")
    try:
        main(channel_id=args.channel_id, disable_proxy=args.no_proxy)
    except KeyboardInterrupt:
        log.info("Download process interrupted by user")
    except Exception as e:
        log.error(f"Unhandled exception: {e}")
    finally:
        log.info("Download process completed or terminated")