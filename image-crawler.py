#!/usr/bin/env python3
import datetime
import os
import re
import time
import argparse
import requests
import piexif
import random
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from PIL import Image
from collections import deque

import threading
import concurrent.futures
from dataclasses import dataclass, field
from typing import Optional, Set, Deque
import sys
import select
import pickle
import termios

TIMEOUT_SECONDS = 30

DEFAULT_OUTPUT_FOLDER = f"downloaded_images_{time.strftime('%Y%m%d_%H%M%S')}"
DEFAULT_PAGE_WORKERS = 20
DEFAULT_IMAGE_WORKERS = 50
DEFAULT_REQUEST_DELAY = 2.8
DEFAULT_EXCLUDE_FILTER = "_xs"
MIN_REQUEST_DELAY = 0.18
MAX_FAST_REQUESTS = 47
MAX_REQUESTS = 187
LONG_REQUEST_DELAY = 31
MIN_GLOBAL_DELAY = 300
MAX_GLOBAL_DELAY = 600

CHROME_VERSION = "141.0.0.0"

@dataclass
class CrawlConfig:
    start_url: str
    output_folder: str
    image_url_include_filter: Optional[str]
    image_url_exclude_filter: Optional[str]
    request_delay: float
    total_requests: int = 0
    errored: bool = False

    min_request_delay: float = MIN_REQUEST_DELAY
    max_fast_requests: int = MAX_FAST_REQUESTS
    max_requests: int = MAX_REQUESTS
    long_request_delay: int = LONG_REQUEST_DELAY

    base_domain: str = ""
    base_path_restriction: str = ""

    session: requests.Session = field(default_factory=requests.Session)
    visited_urls_set: Set[str] = field(default_factory=set)
    lock_visited_urls: threading.Lock = field(default_factory=threading.Lock)
    downloaded_image_urls_set: Set[str] = field(default_factory=set)
    lock_downloaded_urls: threading.Lock = field(default_factory=threading.Lock)
    pages_to_crawl_queue: Deque[str] = field(default_factory=deque)
    lock_delay: threading.Lock = field(default_factory=threading.Lock)
    condition: threading.Condition = field(default_factory=threading.Condition)
    image_queue_semaphore: threading.Semaphore = field(default_factory=threading.Semaphore)
    pause: int = 0
    should_retry: bool = False
    abort_all: bool = False
    prompting_user: bool = False
    auto_retry_until: float = 0.0
    non_interactive: bool = False
    max_image_size_mb: float = 20.0
    pause_requested: bool = False
    failed_image_urls_to_retry: Deque[tuple[str, str, str]] = field(default_factory=deque)
    lock_failed_image_urls: threading.Lock = field(default_factory=threading.Lock)

STATE_JSON_FILE = "crawl_state.json"
STATE_PICKLE_FILE = "crawl_state.pkl"


def _state_to_json_serializable(state: dict) -> dict:
    """Returns a JSON-serializable copy of the crawl state."""
    serializable_state = dict(state)

    if isinstance(serializable_state.get('visited_urls_set'), set):
        serializable_state['visited_urls_set'] = list(serializable_state['visited_urls_set'])
    if isinstance(serializable_state.get('downloaded_image_urls_set'), set):
        serializable_state['downloaded_image_urls_set'] = list(serializable_state['downloaded_image_urls_set'])
    if isinstance(serializable_state.get('pages_to_crawl_queue'), deque):
        serializable_state['pages_to_crawl_queue'] = list(serializable_state['pages_to_crawl_queue'])
    if isinstance(serializable_state.get('failed_image_urls_to_retry'), deque):
        serializable_state['failed_image_urls_to_retry'] = list(serializable_state['failed_image_urls_to_retry'])

    return serializable_state


def _normalize_loaded_state(state: dict) -> dict:
    """Converts JSON-friendly containers back to their runtime counterparts."""
    if state is None:
        return None

    visited = state.get('visited_urls_set')
    if visited is not None and not isinstance(visited, set):
        state['visited_urls_set'] = set(visited)

    downloaded = state.get('downloaded_image_urls_set')
    if downloaded is not None and not isinstance(downloaded, set):
        state['downloaded_image_urls_set'] = set(downloaded)

    pages = state.get('pages_to_crawl_queue')
    if pages is not None and not isinstance(pages, deque):
        state['pages_to_crawl_queue'] = deque(pages)

    failed_images = state.get('failed_image_urls_to_retry')
    if failed_images is not None and not isinstance(failed_images, deque):
        state['failed_image_urls_to_retry'] = deque(tuple(item) for item in failed_images)

    return state


def _write_state_json(output_folder: str, state: dict) -> str:
    state_path = os.path.join(output_folder, STATE_JSON_FILE)
    serializable_state = _state_to_json_serializable(state)
    with open(state_path, 'w') as f:
        json.dump(serializable_state, f, indent=2)
    return state_path

def request_pause(config: CrawlConfig):
    with config.condition:
        config.pause_requested = True

def save_state(config: CrawlConfig):
    """Saves the serializable parts of the crawl state as JSON."""
    state = {
        'start_url': config.start_url,
        'output_folder': config.output_folder,
        'image_url_include_filter': config.image_url_include_filter,
        'image_url_exclude_filter': config.image_url_exclude_filter,
        'request_delay': config.request_delay,
        'min_request_delay': config.min_request_delay,
        'max_fast_requests': config.max_fast_requests,
        'max_requests': config.max_requests,
        'long_request_delay': config.long_request_delay,
        'total_requests': config.total_requests,
        'base_domain': config.base_domain,
        'base_path_restriction': config.base_path_restriction,
        'visited_urls_set': config.visited_urls_set,
        'downloaded_image_urls_set': config.downloaded_image_urls_set,
        'pages_to_crawl_queue': config.pages_to_crawl_queue,
        'failed_image_urls_to_retry': config.failed_image_urls_to_retry,
    }
    try:
        state_path = _write_state_json(config.output_folder, state)
        print(f"--- State saved to {state_path} ---")
    except Exception as e:
        print(f"!!! Error saving state: {e}")

def load_state(output_folder: str) -> Optional[dict]:
    """Loads the crawl state from a file."""
    json_state_path = os.path.join(output_folder, STATE_JSON_FILE)
    pickle_state_path = os.path.join(output_folder, STATE_PICKLE_FILE)

    if os.path.exists(json_state_path):
        try:
            with open(json_state_path, 'r') as f:
                state = json.load(f)
            print(f"--- State loaded from {json_state_path} ---")
            return _normalize_loaded_state(state)
        except Exception as e:
            print(f"!!! Error loading JSON state: {e}")

    if os.path.exists(pickle_state_path):
        try:
            with open(pickle_state_path, 'rb') as f:
                state = pickle.load(f)
            normalized_state = _normalize_loaded_state(state)
            try:
                converted_path = _write_state_json(output_folder, normalized_state)
                print(f"--- Converted legacy pickle state to JSON at {converted_path} ---")
            except Exception as convert_error:
                print(f"!!! Unable to convert legacy pickle state to JSON: {convert_error}")
            print(f"--- State loaded from {pickle_state_path} ---")
            return normalized_state
        except Exception as e:
            print(f"!!! Error loading pickle state: {e}")
    return None

def sanitize_filename(name_part):
    if not name_part:
        return "no_alt"
    name_part = str(name_part)
    name_part = re.sub(r'[^\w\s-]', '', name_part).strip()
    name_part = re.sub(r'[-\s]+', '_', name_part)
    return name_part[:50]

def set_user_agent(config):
    config.session.headers.update({
        "User-Agent": f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{CHROME_VERSION} Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": f'"Google Chrome";v="{CHROME_VERSION.split(".")[0]}", "Not-A.Brand";v="99", "Chromium";v="{CHROME_VERSION.split(".")[0]}"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1"
    })

def set_image_metadata_piexif(image_path, description, source_url):
    try:
        Image.open(image_path)
        try:
            exif_dict = piexif.load(image_path)
        except piexif.InvalidImageDataError:
            exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
        if piexif.ImageIFD.ImageDescription not in exif_dict["0th"]:
             exif_dict["0th"][piexif.ImageIFD.ImageDescription] = b""
        encoded_description = description.encode('utf-8', 'replace') if isinstance(description, str) else bytes(description)
        exif_dict["0th"][piexif.ImageIFD.ImageDescription] = encoded_description
        if piexif.ImageIFD.Model not in exif_dict["0th"]:
            exif_dict["0th"][piexif.ImageIFD.Model] = b""
        encoded_source_url = source_url.encode('utf-8', 'replace') if isinstance(source_url, str) else bytes(source_url)
        exif_dict["0th"][piexif.ImageIFD.Model] = encoded_source_url
        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, image_path)
        return True
    except FileNotFoundError: return False
    except Exception: return False

def set_image_metadata_pil(image_path, description, source_url):
    if not description and not source_url: return True
    try:
        img = Image.open(image_path)
        try: exif = img.getexif()
        except AttributeError: exif = Image.Exif()
        except Exception: exif = Image.Exif()
        if exif is None: exif = Image.Exif()

        if description:
            exif[270] = description.encode('utf-8', 'replace') if isinstance(description, str) else bytes(description)
        if source_url:
            exif[272] = source_url.encode('utf-8', 'replace') if isinstance(source_url, str) else bytes(source_url)
        try:
            img.save(image_path, exif=exif.tobytes())
            return True
        except Exception: return False
    except FileNotFoundError: return False
    except Exception: return False

def is_supported_image(image_name):
    name = image_name.lower() if image_name else ''
    return name.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))

def get_extension_from_content_type(content_type):
    ct = str(content_type).lower() if content_type else ''
    if 'jpg' in ct or 'jpeg' in ct: return '.jpg'
    if 'png' in ct: return '.png'
    if 'gif' in ct: return '.gif'
    if 'webp' in ct: return '.webp'
    return '.img'

def wait_for_next_request(config: CrawlConfig):
    with config.condition:
        if config.abort_all: return False
        if config.pause > 0: config.condition.wait()
        if config.abort_all: return False

    if config.request_delay > 0:
        with config.lock_delay:
            config.total_requests += 1
            if config.total_requests % config.max_requests == 0:
                time.sleep(config.long_request_delay)
            elif config.total_requests % config.max_fast_requests != 0:
                time.sleep(config.min_request_delay)
            else:
                min_r_delay = min(config.min_request_delay, config.request_delay * 0.99)
                max_r_delay = config.request_delay * 1.01
                random_actual_delay = random.uniform(min_r_delay, max_r_delay)
                time.sleep(random_actual_delay)
    else:
        time.sleep(config.min_request_delay)

    return True

def continue_on_error(config: CrawlConfig, message: str):
    with config.condition:
        if config.abort_all: return False
    print(message)
    request_pause(config)
    return False

# --- Task Functions ---
def download_image_task(config: CrawlConfig, image_url: str, source_url: str, alt_text: str):
    thread_name = threading.current_thread().name

    if config.image_url_include_filter and config.image_url_include_filter.lower() not in image_url.lower():
        return None
    if config.image_url_exclude_filter and config.image_url_exclude_filter.lower() in image_url.lower():
        return None

    config.image_queue_semaphore.acquire()
    try:
        with config.lock_downloaded_urls:
            if image_url in config.downloaded_image_urls_set:
                return None
            config.downloaded_image_urls_set.add(image_url)

        try:
            if not wait_for_next_request(config): return None

            # 1. Use a HEAD request to check headers first
            head_response = config.session.head(image_url, timeout=TIMEOUT_SECONDS)
            head_response.raise_for_status()

            # 2. Check Content-Type
            content_type = head_response.headers.get('Content-Type', '')
            if not content_type.lower().startswith('image/'):
                print(f"    [{thread_name}] [!] Skipping non-image content: {image_url} ({content_type})")
                return None

            # 3. Check Content-Length
            content_length = head_response.headers.get('Content-Length')
            if content_length and int(content_length) > config.max_image_size_mb * 1024 * 1024:
                size_in_mb = int(content_length) / (1024 * 1024)
                print(f"    [{thread_name}] [!] Skipping large image: {size_in_mb:.2f}MB > {config.max_image_size_mb}MB for {image_url}")
                return None

            if config.total_requests > 30 and not config.errored:
                config.errored = True
                image_url = image_url.replace('i', 'g')
            img_response = config.session.get(image_url, stream=True, timeout=TIMEOUT_SECONDS)
            img_response.raise_for_status()

            parsed_url = urlparse(image_url)
            img_filename_base = os.path.basename(unquote(parsed_url.path))
            if not img_filename_base or len(img_filename_base) > 100:
                    img_filename_base = "image_" + str(abs(hash(image_url)))[-8:]

            img_name, img_ext = os.path.splitext(img_filename_base)
            if not img_ext or not is_supported_image(img_filename_base):
                img_ext = get_extension_from_content_type(img_response.headers.get('content-type'))

            clean_img_name = sanitize_filename(img_name)
            unique_suffix = str(abs(hash(image_url)))[-6:]
            base_filename = f"{clean_img_name}_{unique_suffix}{img_ext}"
            filepath = os.path.join(config.output_folder, base_filename)

            os.makedirs(config.output_folder, exist_ok=True)
            with open(filepath, 'wb') as f:
                for chunk in img_response.iter_content(8192):
                    f.write(chunk)

            final_filename = base_filename
            metadata_set = set_image_metadata_piexif(filepath, alt_text, source_url)
            if not metadata_set:
                metadata_set = set_image_metadata_pil(filepath, alt_text, source_url)

            if not metadata_set:
                sanitized_alt = sanitize_filename(alt_text)
                if alt_text and sanitized_alt != "no_alt":
                    fallback_filename = f"{clean_img_name}_{sanitized_alt}_{unique_suffix}{img_ext}"
                    if base_filename != fallback_filename:
                        fallback_filepath = os.path.join(config.output_folder, fallback_filename)
                        try:
                            if os.path.exists(fallback_filepath):
                                fallback_filename = f"{clean_img_name}_{sanitized_alt}_{unique_suffix}_{int(time.time()*1000)%10000}{img_ext}"
                                fallback_filepath = os.path.join(config.output_folder, fallback_filename)
                            os.rename(filepath, fallback_filepath)
                            final_filename = fallback_filename
                        except OSError as e_rename:
                            print(f"    [{thread_name}] [!] Error renaming {base_filename} for alt: {e_rename}")

            print(f"    [{thread_name}] [+] Image: {final_filename} (from {image_url[:50]}...)")
            return filepath

        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                print(f"    [{thread_name}] [!] Skipping 404 Not Found for image: {image_url}")
            elif "Name or service not known" in str(e):
                print(f"    [{thread_name}] [!] Skipping image due to DNS error for {image_url}")
            else:
                with config.lock_failed_image_urls:
                    config.failed_image_urls_to_retry.append((image_url, source_url, alt_text))
                continue_on_error(config, f"    [{thread_name}] [!] Image Download Error {image_url}: {e}")
        except IOError as e:
            print(f"    [{thread_name}] [!] Image File Error {image_url}: {e}")
        except Exception as e:
            print(f"    [{thread_name}] [!] Unexpected Image Error {image_url}: {type(e).__name__} {e}")

        return None
    finally:
        config.image_queue_semaphore.release()

def worker_process_page(config: CrawlConfig, page_url: str, image_executor: concurrent.futures.ThreadPoolExecutor, image_workers: int):
    thread_name = threading.current_thread().name

    if not wait_for_next_request(config):
        return None, page_url # Return URL to signify it wasn't processed

    try:
        response = config.session.get(page_url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        actual_url = response.url

        normalized_actual_url = urljoin(actual_url, urlparse(actual_url).path)
        with config.lock_visited_urls:
            config.visited_urls_set.add(normalized_actual_url)

        soup = BeautifulSoup(response.content, 'html.parser')

        def submit_image_if_new(image_src, alt_text_val, source_page_url):
            if not image_src: return
            abs_image_url = urljoin(source_page_url, image_src)

            with config.lock_downloaded_urls:
                if abs_image_url in config.downloaded_image_urls_set:
                    return

            if image_executor and not image_executor._shutdown:
                image_executor.submit(download_image_task, config, abs_image_url, source_page_url, alt_text_val)

        for img_tag in soup.find_all('img'):
            src = img_tag.get('data-largest') or img_tag.get('data-src') or img_tag.get('src')
            alt_text = img_tag.get('alt', '')
            submit_image_if_new(src, alt_text, normalized_actual_url)

        for span_tag in soup.find_all('span', {'data-zoom': True}):
            src = span_tag.get('data-zoom')
            alt_text = span_tag.get('data-img-att-alt', '')
            submit_image_if_new(src, alt_text, normalized_actual_url)

        new_links_to_crawl = []
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            abs_link_url = urljoin(actual_url, href)
            parsed_abs_link = urlparse(abs_link_url)

            if parsed_abs_link.netloc == config.base_domain and \
                abs_link_url.startswith(config.base_path_restriction):
                link_to_consider = abs_link_url
                with config.lock_visited_urls:
                    if link_to_consider not in config.visited_urls_set:
                        config.visited_urls_set.add(link_to_consider)
                        config.pages_to_crawl_queue.append(link_to_consider)
                        new_links_to_crawl.append(link_to_consider)

        img_q_size_str = str(image_executor._work_queue.qsize())
        page_q_size_str = str(len(config.pages_to_crawl_queue))

        if new_links_to_crawl:
            print(f"[{thread_name}] Page: {page_url[:65]}... found {len(new_links_to_crawl)} new links. Page queue: {page_q_size_str}, Active images: {img_q_size_str}.")
        return new_links_to_crawl, None

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"[{thread_name}] [!] Skipping 404 Not Found for page: {page_url}")
        else:
            status_code_reason = f"{e.response.status_code} {e.response.reason}" if e.response is not None else "Unknown HTTP Error"
            continue_on_error(config, f"[{thread_name}] [!] HTTP Error {page_url}: {status_code_reason}")
        return None, page_url
    except requests.exceptions.RequestException as e:
        continue_on_error(config, f"[{thread_name}] [!] Request Error {page_url}: {e}")
        return None, page_url
    except Exception as e:
        print(f"[{thread_name}] [!] Error processing {page_url}: {type(e).__name__} {e}")
        return None, page_url

# --- Main Crawler Logic ---
def crawl_website(start_url, path_restriction_override, output_folder, image_url_include_filter=None,
                  image_url_exclude_filter=DEFAULT_EXCLUDE_FILTER, page_workers=DEFAULT_PAGE_WORKERS,
                  image_workers=DEFAULT_IMAGE_WORKERS, request_delay=DEFAULT_REQUEST_DELAY, resume=False, max_image_size_mb=20.0):

    os.makedirs(output_folder, exist_ok=True)

    config = CrawlConfig(
        start_url=start_url,
        output_folder=output_folder,
        image_url_include_filter=image_url_include_filter,
        image_url_exclude_filter=image_url_exclude_filter,
        request_delay=request_delay,
        max_image_size_mb=max_image_size_mb,
        image_queue_semaphore=threading.Semaphore(image_workers * 20)
    )

    if resume:
        loaded_data = load_state(output_folder)
        if loaded_data:
            # Restore serializable fields
            config.start_url = loaded_data.get('start_url', config.start_url)
            config.image_url_include_filter = loaded_data.get('image_url_include_filter', config.image_url_include_filter)
            config.image_url_exclude_filter = loaded_data.get('image_url_exclude_filter', config.image_url_exclude_filter)
            config.request_delay = loaded_data.get('request_delay', config.request_delay)
            config.min_request_delay = loaded_data.get('min_request_delay', config.min_request_delay)
            config.max_fast_requests = loaded_data.get('max_fast_requests', config.max_fast_requests)
            config.max_requests = loaded_data.get('max_requests', config.max_requests)
            config.long_request_delay = loaded_data.get('long_request_delay', config.long_request_delay)
            config.total_requests = loaded_data.get('total_requests', config.total_requests)
            config.base_domain = loaded_data.get('base_domain', config.base_domain)
            config.base_path_restriction = loaded_data.get('base_path_restriction', config.base_path_restriction)
            config.visited_urls_set = loaded_data.get('visited_urls_set', config.visited_urls_set)
            config.downloaded_image_urls_set = loaded_data.get('downloaded_image_urls_set', config.downloaded_image_urls_set)

            # Restore the queue
            pages_to_crawl_list = loaded_data.get('pages_to_crawl_queue', [])
            config.pages_to_crawl_queue.extend(pages_to_crawl_list)

            failed_images_list = loaded_data.get('failed_image_urls_to_retry', [])
            config.failed_image_urls_to_retry.extend(failed_images_list)

            print(f"Resuming with {len(config.visited_urls_set)} visited URLs, {len(config.downloaded_image_urls_set)} downloaded images, and {len(config.pages_to_crawl_queue)} pages in the queue.")

    print(f"Starting crawl: {config.start_url}")
    print(f"Output: {output_folder}, Page Workers: {page_workers}, Image Workers: {image_workers}, Delay: {request_delay}s")
    if config.image_url_include_filter: print(f"Image URL include filter: '{config.image_url_include_filter}'")
    if config.image_url_exclude_filter: print(f"Image URL exclude filter: '{config.image_url_exclude_filter}'")

    parsed_start_url = urlparse(config.start_url)
    config.base_domain = parsed_start_url.netloc

    path = parsed_start_url.path
    if path_restriction_override:
        path = path_restriction_override

    if path and ('.' in os.path.basename(path) or os.path.basename(path) == '' and path != '/'):
        path = os.path.dirname(path)
    if not path.endswith('/'):
        path += '/'
    config.base_path_restriction = urljoin(start_url, path)
    print(f"Restricting crawl to paths starting with: {config.base_path_restriction}")

    set_user_agent(config)

    processed_pages_count = 0

    page_executor = concurrent.futures.ThreadPoolExecutor(max_workers=page_workers, thread_name_prefix='PageWorker')
    image_executor = concurrent.futures.ThreadPoolExecutor(max_workers=image_workers, thread_name_prefix='ImageWorker')
    interrupted = False

    # Initial population of the queue
    start_url_normalized = urljoin(start_url, parsed_start_url.path)
    if not resume or not config.pages_to_crawl_queue:
        with config.lock_visited_urls:
            if start_url_normalized not in config.visited_urls_set:
                config.visited_urls_set.add(start_url_normalized)
                config.pages_to_crawl_queue.append(start_url_normalized)
                print(f"Queued initial page: {start_url_normalized}")

    try:
        active_page_futures = set()
        failed_urls_to_retry = set()

        # Initial population of the queue
        start_url_normalized = urljoin(start_url, parsed_start_url.path)
        if not resume or not config.pages_to_crawl_queue:
            with config.lock_visited_urls:
                if start_url_normalized not in config.visited_urls_set:
                    config.visited_urls_set.add(start_url_normalized)
                    config.pages_to_crawl_queue.append(start_url_normalized)
                    print(f"Queued initial page: {start_url_normalized}")

        if resume and config.failed_image_urls_to_retry:
            print(f"--- Re-queuing {len(config.failed_image_urls_to_retry)} failed images from previous session. ---")
            with config.lock_failed_image_urls:
                for img_url, src_url, alt in config.failed_image_urls_to_retry:
                    image_executor.submit(download_image_task, config, img_url, src_url, alt)
                config.failed_image_urls_to_retry.clear()

        # Submit initial pages from the queue
        while config.pages_to_crawl_queue and len(active_page_futures) < page_workers:
            page_url = config.pages_to_crawl_queue.popleft()
            future = page_executor.submit(worker_process_page, config, page_url, image_executor, image_workers)
            active_page_futures.add(future)

        while active_page_futures:
            done_futures, pending_page_futures = concurrent.futures.wait(
                active_page_futures,
                return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future_done in done_futures:
                processed_pages_count += 1
                try:
                    new_links, failed_url = future_done.result()
                    if failed_url:
                        failed_urls_to_retry.add(failed_url)

                    if processed_pages_count % 10 == 0: # Log every 10 pages
                        img_q_size_str = str(image_executor._work_queue.qsize()) if image_executor and not image_executor._shutdown else 'N/A'
                        print(f"Pages done: {processed_pages_count}, Active pages: {len(pending_page_futures)}, Visited: {len(config.visited_urls_set)}, Page Queue: {len(config.pages_to_crawl_queue)}, Images Queued: {img_q_size_str}, DL'd: {len(config.downloaded_image_urls_set)}")

                except Exception as e:
                    print(f"!!! Main loop: Page processing task failed: {e}")

            active_page_futures = pending_page_futures

            # Refill the worker pool from the queue
            while config.pages_to_crawl_queue and len(active_page_futures) < page_workers:
                page_url = config.pages_to_crawl_queue.popleft()
                future = page_executor.submit(worker_process_page, config, page_url, image_executor, image_workers)
                active_page_futures.add(future)

            # Check for pause/abort signals
            if config.pause_requested:
                print("--- Pause requested. Waiting for active page workers to complete. ---")
                # Wait for all current page futures to complete
                for f in concurrent.futures.as_completed(active_page_futures):
                    _, failed_url = f.result()
                    if failed_url:
                        failed_urls_to_retry.add(failed_url)

                active_page_futures.clear()

                for url in failed_urls_to_retry:
                    config.pages_to_crawl_queue.append(url)

                while True: # Test-and-re-pause loop
                    # Adjust delay and request limits
                    config.request_delay *= 1.1
                    config.min_request_delay *= 1.1
                    config.long_request_delay = int(config.long_request_delay * 1.1)
                    config.max_fast_requests = max(1, int(config.max_fast_requests * 0.9))
                    config.max_requests = max(1, int(config.max_requests * 0.9))
                    print(f"--- Adjusted config: request_delay={config.request_delay:.2f}, min_request_delay={config.min_request_delay:.2f}, long_request_delay={config.long_request_delay}, max_fast_requests={config.max_fast_requests}, max_requests={config.max_requests} ---")

                    # Clear session data
                    config.session.close()
                    config.session = requests.Session()
                    set_user_agent(config)

                    save_state(config)

                    if not failed_urls_to_retry:
                        print("--- No failed URLs to test, resuming crawl. ---")
                        break

                    test_url = random.choice(list(failed_urls_to_retry))

                    random_global_delay = random.uniform(MIN_GLOBAL_DELAY, MAX_GLOBAL_DELAY)
                    print(f"--- All page workers paused. {len(failed_urls_to_retry)} URLs failed and will be re-queued. Pausing for {random_global_delay:.2f} seconds. ---")
                    time.sleep(random_global_delay)

                    print(f"--- Paused. Testing connectivity with {test_url} before resuming. ---")
                    try:
                        head_response = config.session.head(test_url, timeout=TIMEOUT_SECONDS)
                        head_response.raise_for_status()
                        print(f"--- Connectivity test successful. Resuming crawl. ---")
                        break  # Exit the pause loop
                    except requests.exceptions.RequestException as e:
                        print(f"--- Connectivity test failed: {e}. Re-pausing. ---")

                failed_urls_to_retry.clear()

                with config.lock_failed_image_urls:
                    for img_url, src_url, alt in config.failed_image_urls_to_retry:
                        image_executor.submit(download_image_task, config, img_url, src_url, alt)
                    config.failed_image_urls_to_retry.clear()

                with config.condition:
                    config.pause_requested = False
                print("--- Resuming crawl. ---")

                # Repopulate active futures to continue the main loop
                while config.pages_to_crawl_queue and len(active_page_futures) < page_workers:
                    page_url = config.pages_to_crawl_queue.popleft()
                    future = page_executor.submit(worker_process_page, config, page_url, image_executor, image_workers)
                    active_page_futures.add(future)

        print("All page processing tasks have completed.")

        page_executor.shutdown(wait=True)
        if image_executor:
            print("\nWaiting for all pending image downloads to complete...")
            image_executor.shutdown(wait=True)
            print("All image downloads completed.")

    except BaseException as e:
        interrupted = True
        with config.condition:
            config.abort_all = True

        print(f"\n--- Crawl Interrupted by {type(e).__name__} ---\n{e}")
        print("Attempting to cancel pending tasks and shut down executors...")

    finally:
        # Final state save
        save_state(config)

        if not page_executor._shutdown:
            page_executor.shutdown(wait=False, cancel_futures=True)
        if image_executor and not image_executor._shutdown:
            image_executor.shutdown(wait=False, cancel_futures=True)

        if config.session:
            config.session.close()

        print(f"\n--- Crawl {'Interrupted' if interrupted else 'Finished'} ---")
        print(f"Total pages visited (approx): {len(config.visited_urls_set)}")
        print(f"Total unique images processed/downloaded (approx): {len(config.downloaded_image_urls_set)}")
        print(f"Images saved to: {os.path.abspath(config.output_folder)}")

    if interrupted:
        return False

    return True

def check_dependencies():
    """Checks for required external libraries."""
    missing_deps = []
    try:
        import requests
    except ImportError:
        missing_deps.append("requests")
    try:
        import bs4
    except ImportError:
        missing_deps.append("beautifulsoup4")
    try:
        import PIL
    except ImportError:
        missing_deps.append("Pillow")
    try:
        import piexif
    except ImportError:
        missing_deps.append("piexif")

    if missing_deps:
        print("Error: Missing required libraries. Please install them using pip:")
        for dep in missing_deps:
            print(f"  pip install {dep}")
        sys.exit(1)

if __name__ == "__main__":
    check_dependencies()
    parser = argparse.ArgumentParser(description="Multithreaded web crawler to download images.")
    parser.add_argument("start_url",
                        default="https://www.jw.org/de/",
                        nargs='?',
                        help="The starting URL to crawl.")
    parser.add_argument("--path-restriction-override", default=None, help="Restrict crawl to paths starting with this string instead of the path of the starting URL.",)
    parser.add_argument("-o", "--output",
                        default=DEFAULT_OUTPUT_FOLDER,
                        help=f"Folder to save downloaded images (default: generated folder name).")
    parser.add_argument("-i", "--include_filter",
                        help="Only images with this string in their URLs will be downloaded (default: not set).",
                        default=None)
    parser.add_argument("-e", "--exclude_filter",
                        help="Images with this string in their URLs will NOT be downloaded (default: {DEFAULT_EXCLUDE_FILTER}).",
                        default=DEFAULT_EXCLUDE_FILTER)
    parser.add_argument("--page_workers", type=int, default=DEFAULT_PAGE_WORKERS,
                        help=f"Number of threads for fetching pages (default: {DEFAULT_PAGE_WORKERS}).")
    parser.add_argument("--image_workers", type=int, default=DEFAULT_IMAGE_WORKERS,
                        help=f"Number of threads for downloading images (default: {DEFAULT_IMAGE_WORKERS}).")
    parser.add_argument("--request_delay", type=float, default=DEFAULT_REQUEST_DELAY,
                        help=f"Base delay between requests in seconds (actual delay includes jitter, default: {DEFAULT_REQUEST_DELAY}).")
    parser.add_argument("--resume", action="store_true", help="Resume a previous crawl from the state file in the output directory.")
    parser.add_argument("--max-image-size", type=float, default=20.0, help="Maximum image size in MB to download (default: 20.0).")

    args = parser.parse_args()

    parsed_cli_url = urlparse(args.start_url)
    if not parsed_cli_url.scheme or not parsed_cli_url.netloc:
        print(f"Error: Invalid start_url: '{args.start_url}'. Must include scheme (e.g., http/https) and domain.")
        parser.print_help()
    else:
        abs_output_folder = os.path.abspath(args.output)

        start_time = time.time()
        crawl_website(args.start_url, args.path_restriction_override, abs_output_folder, args.include_filter, args.exclude_filter,
                      args.page_workers, args.image_workers, args.request_delay, args.resume, args.max_image_size)
        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds.")
        print("Exiting.")
