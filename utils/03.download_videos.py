import os
import sys
import csv
import time
import random
import logging
import argparse
import threading
from pathlib import Path
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import yt_dlp
from dataclasses import dataclass, field

@dataclass
class Config:
    CSV_FILE: Path = Path("output/download.csv")
    OUTPUT_DIR: Path = Path.home() / "Downloads" / "YouTube"
    CONCURRENT_FRAGMENTS: int = random.randint(16, 32)
    BATCH_DOWNLOAD_SIZE: int = random.randint(8, 16)
    cookies_dir: Path = Path("input/Cookies")
    ARCHIVE_FILE: Path = Path("output/download_archive.txt")
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

def progress_hook(d):
    if d['status'] == 'finished':
        log.info(f"[DOWNLOAD FINISHED] {d['filename']}")
    elif d['status'] == 'error':
        log.error(f"[DOWNLOAD ERROR] {d.get('filename', 'unknown')}: {d.get('error', 'unknown error')}")

def build_yt_dlp_opts() -> dict:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"
    ]
    
    user_agent = random.choice(user_agents)
    
    languages = ["en-US,en;q=0.9", 
                 "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7", "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
                 "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"]
    accept_language = random.choice(languages)
    
    opts = {
        "outtmpl": str(config.OUTPUT_DIR / "%(id)s.%(ext)s"),
        "format_sort": ["+size", "+br", "+res", "+fps"],
        "concurrent_fragments": min(config.CONCURRENT_FRAGMENTS, 8),
        "sleep_interval": random.uniform(2.0, 5.0) + random.uniform(0.1, 0.5),
        "max_sleep_interval": random.uniform(2.0, 5.0) + random.uniform(0.5, 1.5),
        "retries": 3,
        "fragment_retries": 8,
        "quiet": True,
        "no_warnings": True,
        "download_archive": str(config.ARCHIVE_FILE),
        "progress_hooks": [progress_hook],
        "user_agent": user_agent,
        "http_headers": {
            "Accept-Language": accept_language,
            "Referer": "https://www.youtube.com/",
            "Connection": "keep-alive",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        },
        "extractor_retries": 3,
        "skip_unavailable_fragments": True,
        "keep_fragments": False,
        "ignoreerrors": True,
    }
    
    client_to_emulate = random.choice(["web", "android", "mweb", "ios", "tv", "web_creator", "web_safari"])
    log.info(f"Using '{client_to_emulate}' client for this batch.")

    opts["extractor_args"] = {
        "youtube": {
            "innertube_client": client_to_emulate,
        }
    }

    cookies_config = get_cookies_config()
    opts.update(cookies_config)
    
    if "cookiefile" not in opts and "cookiesfrombrowser" not in opts:
        log.warning("No authentication method available. Bot detection likely increased.")
        
    opts["no_check_certificate"] = False
    opts["prefer_insecure"] = False

    return opts

def is_cookie_file_valid(cookie_file: Path) -> bool:
    if not cookie_file.exists():
        return False
    
    try:
        if cookie_file.stat().st_size < 10:
            return False
        
        with open(cookie_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(1024)
            youtube_domains = ['youtube.com', '.google.com']
            return any(domain in content for domain in youtube_domains)
    except Exception as e:
        log.warning(f"Error validating cookie file {cookie_file}: {e}")
        return False

def get_cookies_files() -> List[Path]:
    if not config.cookies_dir.exists():
        return []
    return [f for f in config.cookies_dir.glob("*.txt") if f.is_file()]

def rotate_cookies() -> Optional[str]:
    cookies_files = get_cookies_files()
    valid_files = [(f, f.stat().st_mtime) for f in cookies_files if is_cookie_file_valid(f)]
    
    if not valid_files:
        log.info("No valid cookie files found")
        return None
    
    valid_files.sort(key=lambda x: x[1], reverse=True)
    valid_file_paths = [str(f[0]) for f in valid_files]
    
    weighted_files = []
    file_count = len(valid_file_paths)
    for i, file_path in enumerate(valid_file_paths):
        if file_count >= 3:
            weight = max(1, 4 - i)
        elif file_count == 2:
            weight = 2 if i == 0 else 1
        else:
            weight = 1
        weighted_files.extend([file_path] * weight)
    
    selected_file = random.choice(weighted_files)
    log.info(f"Selected cookie file: {selected_file} (one of {len(valid_file_paths)} valid files)")
    return selected_file

def get_cookies_config() -> dict:
    cookies_config = {}
    
    cookie_file = rotate_cookies()
    if cookie_file:
        cookies_config["cookiefile"] = cookie_file
        log.info(f"Using cookies from file: {cookie_file}")
        return cookies_config
    
    log.warning("No valid cookie sources available. Bot detection likely.")
    return cookies_config

def update_csv_status(video_id: str, status: str):
    with csv_lock:
        if not config.CSV_FILE.exists():
            return

        temp_file = config.CSV_FILE.parent / 'download_temp.csv'
        found = False
        
        try:
            with config.CSV_FILE.open('r', newline='', encoding='utf-8') as csvfile, \
                 temp_file.open('w', newline='', encoding='utf-8') as tempfile:
                reader = csv.DictReader(csvfile)
                fieldnames = reader.fieldnames
                if fieldnames and 'status' not in fieldnames:
                    fieldnames.append('status')

                writer = csv.DictWriter(tempfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    if row.get('videoId') == video_id:
                        row['status'] = status
                        found = True
                    writer.writerow(row)
                    
            if found:
                temp_file.replace(config.CSV_FILE)
            else:
                temp_file.unlink()
        except Exception as e:
            log.error(f"Failed to update CSV status: {e}")
            if temp_file.exists():
                temp_file.unlink()

def generate_archive_from_csv():
    if not config.CSV_FILE.exists():
        return

    done_ids = []
    try:
        with config.CSV_FILE.open('r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('status') == 'done' and row.get('videoId'):
                    done_ids.append(f"youtube {row['videoId']}")
    except Exception as e:
        log.error(f"Error reading CSV for archive: {e}")
        return

    try:
        with config.ARCHIVE_FILE.open('w', encoding='utf-8') as f:
            for line in done_ids:
                f.write(line + '\n')
        log.info(f"Generated archive with {len(done_ids)} done videos")
    except Exception as e:
        log.error(f"Error writing archive: {e}")

def update_csv_from_archive():
    with csv_lock:
        if not config.ARCHIVE_FILE.exists() or not config.CSV_FILE.exists():
            return

        archived_ids = set()
        try:
            with config.ARCHIVE_FILE.open('r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('youtube '):
                        vid = line.split(' ', 1)[1]
                        archived_ids.add(vid)
        except Exception as e:
            log.error(f"Error reading archive: {e}")
            return

        temp_file = config.CSV_FILE.parent / 'download_temp.csv'
        updated = 0
        try:
            with config.CSV_FILE.open('r', newline='', encoding='utf-8') as csvfile, \
                 temp_file.open('w', newline='', encoding='utf-8') as tempfile:
                reader = csv.DictReader(csvfile)
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(tempfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    vid = row.get('videoId')
                    if vid in archived_ids and row.get('status') != 'done':
                        row['status'] = 'done'
                        updated += 1
                    writer.writerow(row)
                    
            temp_file.replace(config.CSV_FILE)
            if updated > 0:
                log.info(f"Updated {updated} videos to 'done' from archive")
        except Exception as e:
            log.error(f"Error updating CSV from archive: {e}")
            if temp_file.exists():
                temp_file.unlink()

def download_batch(urls: List[str]) -> Tuple[int, int, List[str]]:
    if not urls:
        return 0, 0, []

    ydl_opts = build_yt_dlp_opts()
    captcha_challenged_urls = []
    
    log.info(f"Batch processing: {len(urls)} videos")

    success = 0
    
    for url in urls:
        video_id = url.split('v=')[-1].split('&')[0]
            
        update_csv_status(video_id, "in_progress")
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                
                if urls.index(url) > 0:
                    random_delay = random.uniform(1.0, 3.0)
                    log.debug(f"Random delay of {random_delay:.1f}s before downloading next URL")
                    time.sleep(random_delay)
                
                ydl.download([url])
                success += 1
                update_csv_status(video_id, "done")
                log.info(f"Successfully downloaded: {video_id}")
                
        except yt_dlp.utils.DownloadError as e:
            error_message = str(e)
            if "captcha" in error_message.lower() or "challenge" in error_message.lower():
                log.warning(f"Captcha challenge detected for {video_id}: {error_message}")
                captcha_challenged_urls.append(url)
                update_csv_status(video_id, "captcha_challenge")
            elif "unavailable" in error_message.lower():
                log.warning(f"Video {video_id} is unavailable: {error_message}")
                update_csv_status(video_id, "unavailable")
            else:
                log.error(f"Error downloading {video_id}: {error_message}")
                update_csv_status(video_id, "failed")
        except Exception as e:
            log.error(f"Unexpected error downloading {video_id}: {str(e)}")
            update_csv_status(video_id, "failed")
    
    fail = len(urls) - success
    
    if fail > 0:
        log.warning(f"Batch finished with {fail} failure(s), {len(captcha_challenged_urls)} captcha challenges")
    else:
        log.info("Batch completed successfully with no failures")

    return success, fail, captcha_challenged_urls

def main(channel_id: Optional[str] = None):
    if not config.CSV_FILE.exists():
        log.error(f"CSV file not found: {config.CSV_FILE}")
        sys.exit(1)

    generate_archive_from_csv()

    try:
        with config.CSV_FILE.open(newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader if row.get('video_url')]
            if channel_id:
                rows = [row for row in rows if row.get('channelId') == channel_id]
            urls = [row['video_url'] for row in rows if row.get('status') != 'done']
            random.shuffle(urls)
            log.info(f"Found {len(urls)} videos to download." + (f" for channel {channel_id}" if channel_id else ""))
    except Exception as e:
        log.error(f"CSV read error: {e}")
        sys.exit(1)

    if not urls:
        log.info("No new videos to download." + (f" for channel {channel_id}" if channel_id else ""))
        return

    start_time = time.time()
    total_success = total_fail = 0

    with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
        futures = []
        batch_count = (len(urls) + config.BATCH_DOWNLOAD_SIZE - 1) // config.BATCH_DOWNLOAD_SIZE
        for i in range(0, len(urls), config.BATCH_DOWNLOAD_SIZE):
            batch_urls = urls[i:i + config.BATCH_DOWNLOAD_SIZE]
            batch_num = i // config.BATCH_DOWNLOAD_SIZE + 1
            log.info(f"--- Submitting batch {batch_num}/{batch_count} ({len(batch_urls)} videos) ---")
            futures.append(executor.submit(download_batch, batch_urls))
        
        for future in as_completed(futures):
            success, fail, captcha_challenged_urls = future.result()
            total_success += success
            total_fail += fail
            log.info(f"Batch completed: {success} downloaded, {fail} failed.")

    update_csv_from_archive()

    runtime = (time.time() - start_time) / 60
    log.info(f"=== Download Complete: {total_success} success, {total_fail} failed in {runtime:.1f} minutes ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download YouTube videos from CSV.")
    parser.add_argument('--channel-id', type=str, help='Optional channel ID to filter videos by.')
    args = parser.parse_args()    
    
    log.info(f"Download configuration: Output directory={config.OUTPUT_DIR}")
    log.info(f"Batch size: {config.BATCH_DOWNLOAD_SIZE}")
    
    try:
        main(channel_id=args.channel_id)
    except KeyboardInterrupt:
        log.info("Download process interrupted by user")
    except Exception as e:
        log.error(f"Unhandled exception: {e}")
    finally:
        log.info("Download process completed or terminated")