#!/usr/bin/env python3
"""
Asynchronous website image crawler.

Crawls pages within a single site (restricted to a base domain + path prefix),
extracts image URLs, downloads them concurrently, and embeds each image's alt
text and source-page URL as metadata (EXIF for JPEG, text chunks for PNG).

Rewrite goals:
  * Polite, ban-resistant crawling - one adaptive rate limiter paces every
    request with jitter and backs off (honouring ``Retry-After``) on 429/5xx,
    tightening the global interval and pausing all workers when throttled.
  * Robust resume - state is checkpointed atomically; on resume every URL that
    was *discovered but not finished* is re-queued, so an interrupted run loses
    no work. Filenames are content-stable (sha1), so resumes never re-download.
  * Crash/hang-free - cooperative shutdown on SIGINT/SIGTERM, queue draining
    that can always be interrupted, and guaranteed client + state cleanup.

The crawler is single-threaded asyncio; there are no locks because all shared
state is mutated only from the event loop.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import random
import re
import signal
import sys
import tempfile
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional
from urllib.parse import (urldefrag, urljoin, urlparse, unquote,
                          parse_qsl, urlencode, urlunparse)

try:
    import httpx
except ImportError:
    sys.exit("Missing dependency 'httpx'. Install with:  pip install -r requirements.txt")

from bs4 import BeautifulSoup

try:
    import piexif
except ImportError:
    piexif = None

try:
    from PIL import Image, PngImagePlugin
except ImportError:  # pragma: no cover - metadata becomes best-effort/no-op
    Image = None
    PngImagePlugin = None


# --------------------------------------------------------------------------- #
# Defaults / constants
# --------------------------------------------------------------------------- #
DEFAULT_START_URL = "https://www.jw.org/de/"
DEFAULT_OUTPUT = f"downloaded_images_{time.strftime('%Y%m%d_%H%M%S')}"
DEFAULT_EXCLUDE = "_xs"
DEFAULT_PAGE_CONCURRENCY = 8
DEFAULT_IMAGE_CONCURRENCY = 16
DEFAULT_DELAY = 1.0          # base seconds between request *starts* (global)
DEFAULT_JITTER = 0.3         # +/- fraction applied to the interval
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_IMAGE_MB = 20.0
DEFAULT_DOMAIN_STORE = str(Path.home() / ".image-crawler" / "domains.json")
MAX_INTERVAL = 60.0          # ceiling for the adaptive interval
INTERVAL_FLOOR = 0.5         # min interval a penalty assumes (bites at --delay 0)
BACKOFF_FACTOR = 1.5         # interval growth per *distinct* throttle burst
RECOVER_AFTER = 8            # consecutive successes before one AIMD relax step
RECOVER_FRACTION = 0.1       # relax step = max(RECOVER_MIN, fraction * interval)
RECOVER_MIN = 0.5            # smallest relax step, seconds
SAFE_MARGIN = 1.15           # operate this far above the worst throttling interval
RESUME_PROBE_FRACTION = 0.5  # on resume, re-probe down to this fraction of the
                             # learned limit (the server's rate window may have
                             # reset since) instead of pinning SAFE_MARGIN above
                             # it; the first fresh throttle relocks the cautious floor
LONG_WAIT_AFTER = 3          # consecutive throttle bursts -> one long cooldown
LONG_COOLDOWN = 90.0         # one-shot pause to reset a server's rate-limit window
MAX_RETRY_AFTER = 300.0      # honour a server Retry-After up to this many seconds
CHROME_VERSION = "141.0.0.0"

STATE_FILE = "crawl_state.json"
LOCK_FILE = "crawl.lock"     # single-instance guard inside the output dir
STATE_VERSION = 2
CHECKPOINT_INTERVAL = 15.0   # seconds between background state saves

RETRY_STATUS = {429, 500, 502, 503, 504}
SUPPORTED_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".avif")
CHUNK = 65536

# Query params that identify a *request* rather than a *page*; dropped from the
# dedupe key so session/tracking params don't explode the crawl frontier.
VOLATILE_QUERY = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "gclsrc", "dclid", "mc_cid", "mc_eid", "_ga",
    "sessionid", "session_id", "sid", "phpsessid", "jsessionid", "aspsessionid",
}


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
class Logger:
    """Tiny stdout logger with a rewritable single-line status."""

    def __init__(self, verbose: bool = False):
        self.verbose_enabled = verbose
        self._status_active = False
        self._status_len = 0

    def _clear_status(self):
        if self._status_active:
            print()
            self._status_active = False
            self._status_len = 0

    def info(self, message: str):
        self._clear_status()
        print(message, flush=True)

    def verbose(self, message: str):
        if self.verbose_enabled:
            self.info(message)

    def status(self, message: str):
        pad = ""
        if self._status_active and self._status_len > len(message):
            pad = " " * (self._status_len - len(message))
        prefix = "\r" if self._status_active else ""
        print(f"{prefix}{message}{pad}", end="", flush=True)
        self._status_active = True
        self._status_len = len(message)

    def flush(self):
        self._clear_status()


# --------------------------------------------------------------------------- #
# Adaptive rate limiter
# --------------------------------------------------------------------------- #
class AdaptiveLimiter:
    """
    Paces the *start* of every request to roughly one per ``min_interval``
    seconds (with jitter), regardless of how many coroutines are in flight, so
    the effective request rate is bounded.

    The interval follows AIMD (the TCP-congestion-control rule), which probes
    for and converges on the server's actual tolerance:

      * **Multiplicative increase** on throttling - ``min_interval`` widens by
        ``BACKOFF_FACTOR`` *once per distinct burst*. Concurrent failures from
        the same overload event are coalesced (they only extend the shared
        pause), so N in-flight 429s can't blow the interval up N times.
      * **Additive decrease** on sustained success - the interval relaxes one
        small step at a time, but never below ``SAFE_MARGIN`` above the widest
        interval the server *still* throttled. That floor is the limiter's
        reverse-engineered estimate of the domain's rate limit; it is persisted
        so a resume starts at the learned delay instead of re-probing from base.
        On *resume* that floor is loosened to ``RESUME_PROBE_FRACTION`` of the
        learned limit, so a resumed run starts cautious but speeds back up if the
        server now tolerates it; the first fresh throttle relocks the full floor.
      * Repeated bursts (>= ``LONG_WAIT_AFTER``) trigger one ``LONG_COOLDOWN``
        pause to ride out a fixed/sliding rate-limit window before resuming.
    """

    def __init__(self, base_delay: float, jitter: float):
        self.base = max(0.0, base_delay)
        self.jitter = min(max(jitter, 0.0), 0.95)
        self.min_interval = self.base
        self._next = 0.0          # monotonic time the next request may start
        self._pause_until = 0.0   # monotonic time the global pause ends
        self._lock = asyncio.Lock()
        self._consecutive_ok = 0
        self._penalty_streak = 0  # consecutive throttle *bursts* (reset on success)
        # Set on resume: loosen the recovery floor so this run can probe back
        # below the learned limit. Cleared by the first fresh throttle (relock).
        self._probe_floor = False
        # The widest request interval that STILL drew a throttle: the learned
        # rate-limit estimate. Safe operation sits SAFE_MARGIN above it. 0 = the
        # domain has never throttled us, so run at base (full speed).
        self.throttle_interval = 0.0

    async def acquire(self):
        # Reserve a paced slot, then sleep to it. If a penalty extends the global
        # pause past our reserved slot while we wait, discard the reservation and
        # reserve a *fresh* paced slot against the new pause. This re-spaces every
        # parked caller by min_interval instead of releasing them all in one burst
        # the instant the pause ends - which is exactly what re-triggers bans.
        while True:
            async with self._lock:
                now = time.monotonic()
                start = max(now, self._next, self._pause_until)
                spread = self.min_interval * random.uniform(1.0 - self.jitter, 1.0 + self.jitter)
                self._next = start + max(0.0, spread)
            delay = start - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            # Our slot was reserved against _pause_until as seen at lock time. If
            # that pause is still in force, a later penalty extended it -> loop and
            # re-reserve so we stay spaced; otherwise we are clear to proceed.
            if time.monotonic() >= self._pause_until:
                return

    def penalize(self, retry_after: Optional[float] = None):
        now = time.monotonic()
        # A "burst" is the first failure of a fresh throttle event: we are not
        # already inside a pause one of its siblings opened. Only the burst leader
        # widens the interval and counts toward the streak; the rest just pile
        # onto the shared pause. This is what stops a fan-out of concurrent 429s
        # from multiplying the interval up to the ceiling in one shot.
        burst = now >= self._pause_until
        if burst:
            # Record the interval that drew this throttle (widest seen) BEFORE
            # widening - that is the learned limit we must stay above.
            self.throttle_interval = max(self.throttle_interval, self.min_interval, INTERVAL_FLOOR)
            if self._probe_floor:
                # Probing below the learned limit drew a throttle: snap back up to
                # the cautious floor (we may be below it), stop re-probing, and
                # respect the full floor for the rest of this run.
                self.min_interval = max(self.min_interval, self.throttle_interval * SAFE_MARGIN)
                self._probe_floor = False
            self._penalty_streak += 1
            self._consecutive_ok = 0
            widened = max(self.min_interval, self.base, INTERVAL_FLOOR) * BACKOFF_FACTOR
            self.min_interval = min(MAX_INTERVAL, widened)
        # Choose the pause. A server's Retry-After always wins (and still extends
        # the shared pause even for coalesced failures). After repeated bursts,
        # take one long cooldown to let a windowed limiter reset.
        if retry_after is not None:
            pause = min(retry_after, MAX_RETRY_AFTER)
        elif self._penalty_streak >= LONG_WAIT_AFTER:
            pause = LONG_COOLDOWN
        else:
            pause = max(self.min_interval * 2.0, 5.0)
        self._pause_until = max(self._pause_until, now + pause)

    def reward(self):
        self._consecutive_ok += 1
        self._penalty_streak = 0
        # Never relax below a margin above the worst interval the server threw a
        # throttle at; that floor is the learned rate limit.
        floor = self.base
        if self.throttle_interval:
            # On resume probe down toward RESUME_PROBE_FRACTION of the learned
            # limit; otherwise hold the cautious SAFE_MARGIN floor.
            margin = RESUME_PROBE_FRACTION if self._probe_floor else SAFE_MARGIN
            floor = max(self.base, self.throttle_interval * margin)
        if self._consecutive_ok < RECOVER_AFTER or self.min_interval <= floor:
            return
        self._consecutive_ok = 0
        # AIMD additive decrease: a proportional step (faster relaxation when far
        # out, RECOVER_MIN floor when near base) toward the learned safe point.
        step = max(RECOVER_MIN, self.min_interval * RECOVER_FRACTION)
        self.min_interval = max(floor, self.min_interval - step)

    def restore(self, throttle_interval: Optional[float]):
        """Resume at a domain's previously discovered safe delay. Given the
        widest interval that was throttled last run, start SAFE_MARGIN above it
        rather than re-probing the limit from ``base`` (and re-eating the bans).

        The start stays cautious, but the recovery floor is loosened to
        ``RESUME_PROBE_FRACTION`` of the learned limit so a resumed run speeds
        back up if the server's rate window has reset; a fresh throttle relocks
        the full floor (see ``penalize``)."""
        self.throttle_interval = max(0.0, throttle_interval or 0.0)
        if self.throttle_interval:
            self.min_interval = min(
                MAX_INTERVAL, max(self.base, self.throttle_interval * SAFE_MARGIN))
            self._probe_floor = True


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def sanitize_filename(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"[^\w\s-]", "", str(name)).strip()
    name = re.sub(r"[-\s]+", "_", name)
    return name[:50]


def is_supported_ext(ext: str) -> bool:
    return bool(ext) and ext.lower() in SUPPORTED_EXTS


def ext_from_content_type(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "jpeg" in ct or "jpg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "gif" in ct:
        return ".gif"
    if "webp" in ct:
        return ".webp"
    if "avif" in ct:
        return ".avif"
    if "bmp" in ct:
        return ".bmp"
    return ".img"


def build_filename(url: str, content_type: str) -> str:
    """Deterministic, collision-resistant filename derived from the URL."""
    parsed = urlparse(url)
    base = os.path.basename(unquote(parsed.path))
    name, ext = os.path.splitext(base)
    if not is_supported_ext(ext):
        ext = ext_from_content_type(content_type)
    name = sanitize_filename(name) or "image"
    digest = hashlib.sha1(url.encode("utf-8", "replace")).hexdigest()[:10]
    return f"{name}_{digest}{ext}"


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
        if when is None:
            return None
        delta = when.timestamp() - time.time()
        return max(0.0, delta)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_srcset(srcset: str):
    """Yield (url, descriptor) per the HTML srcset grammar. A URL is a run of
    non-whitespace (so commas *inside* a URL - common with image CDNs - are
    preserved); candidates are separated by a comma after the descriptor, or by
    a comma directly terminating a URL."""
    pos, n = 0, len(srcset)
    while pos < n:
        while pos < n and (srcset[pos].isspace() or srcset[pos] == ","):
            pos += 1
        if pos >= n:
            break
        start = pos
        while pos < n and not srcset[pos].isspace():
            pos += 1
        url = srcset[start:pos]
        if url.endswith(","):                 # comma-terminated URL, no descriptor
            yield url.rstrip(","), None
            continue
        while pos < n and srcset[pos].isspace():
            pos += 1
        dstart = pos
        while pos < n and srcset[pos] != ",":
            pos += 1
        descriptor = srcset[dstart:pos].strip()
        if pos < n:
            pos += 1                          # consume the separating comma
        yield url, (descriptor or None)


def largest_from_srcset(srcset: str) -> Optional[str]:
    """Pick the highest-resolution candidate from a srcset attribute. On a tie
    (equal or absent descriptors) the first-seen candidate wins."""
    if not srcset:
        return None
    best_url, best_score = None, None
    for url, descriptor in _parse_srcset(srcset):
        if not url:
            continue
        score = 0.0
        if descriptor:
            d = descriptor.lower()
            try:
                if d.endswith("w"):
                    score = float(d[:-1])
                elif d.endswith("x"):
                    score = float(d[:-1]) * 1000.0
            except ValueError:
                score = 0.0
        if best_score is None or score > best_score:
            best_url, best_score = url, score
    return best_url


def embed_metadata(path: Path, ext: str, alt: str, source: str):
    """Embed alt text + source URL. Best-effort: never raises, never fails the
    download over metadata. ``ext`` is the real image extension (``path`` is the
    ``.part`` temp, so we embed *before* the atomic rename and an interrupted
    write can never leave a corrupt final image)."""
    if (not alt and not source) or Image is None:
        return
    ext = (ext or "").lower()
    try:
        if ext in (".jpg", ".jpeg") and piexif is not None:
            try:
                exif = piexif.load(str(path))
            except Exception:
                exif = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
            if alt:
                exif["0th"][piexif.ImageIFD.ImageDescription] = alt.encode("utf-8", "replace")
            if source:
                exif["0th"][piexif.ImageIFD.Model] = source.encode("utf-8", "replace")
            piexif.insert(piexif.dump(exif), str(path))
        elif ext == ".png" and PngImagePlugin is not None:
            with Image.open(path) as img:
                img.load()
                meta = PngImagePlugin.PngInfo()
                # Preserve any pre-existing text chunks before adding ours.
                for key, value in (img.info or {}).items():
                    if isinstance(value, str) and key not in ("Description", "Source"):
                        with contextlib.suppress(Exception):
                            meta.add_text(key, value)
                if alt:
                    meta.add_text("Description", alt)
                if source:
                    meta.add_text("Source", source)
                # Pass format explicitly: we operate on the ".part" temp, whose
                # extension PIL can't map to a format.
                img.save(path, format="PNG", pnginfo=meta)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Crawler
# --------------------------------------------------------------------------- #
class Crawler:
    def __init__(self, args, logger: Logger):
        self.log = logger
        self.start_url = args.start_url
        self.output = Path(args.output)
        self.include = args.include_filter
        self.exclude = args.exclude_filter
        self.page_conc = max(1, args.page_concurrency)
        self.image_conc = max(1, args.image_concurrency)
        self.max_retries = max(0, args.max_retries)
        self.timeout = args.timeout
        self.max_bytes = int(args.max_image_size * 1024 * 1024)
        self.max_pages = max(0, args.max_pages)
        self.user_agent = args.user_agent
        self.resume = args.resume
        self.path_override = args.path_restriction
        # Shared cross-run delay cache (keyed by domain), independent of -o dir.
        self.domain_store = None if args.no_domain_store else args.domain_store

        self.limiter = AdaptiveLimiter(args.delay, args.jitter)

        # Crawl bounds (filled in setup_bounds()).
        self.base_domain = ""          # lowercased netloc
        self.base_path = ""            # path prefix, always ends with '/'
        self.base_path_restriction = ""  # human-readable full-URL form, for logs

        # State: pages discovered vs. fully handled (200 or definitive 404).
        self.seen_pages: set[str] = set()
        self.processed_pages: set[str] = set()
        # Images discovered (url -> (source_page, alt)) vs. finished.
        self.image_seen: dict[str, tuple[str, str]] = {}
        self.image_done: set[str] = set()

        # Bounded image queue gives natural backpressure; page queue is
        # unbounded so a producing worker can never deadlock on put().
        self.page_queue: asyncio.Queue[str] = asyncio.Queue()
        self.image_queue: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue(
            maxsize=max(100, self.image_conc * 20)
        )

        self.stop_event = asyncio.Event()
        self.client: Optional[httpx.AsyncClient] = None
        self.requests = 0
        self.images_saved = 0
        self._signals_installed: list[int] = []
        self._lock_path: Optional[Path] = None

    # -- HTTP ------------------------------------------------------------- #
    def _headers(self) -> dict:
        major = CHROME_VERSION.split(".")[0]
        ua = self.user_agent or (
            f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{CHROME_VERSION} Safari/537.36"
        )
        return {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": f'"Google Chrome";v="{major}", "Not-A.Brand";v="99", "Chromium";v="{major}"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"',
            "Upgrade-Insecure-Requests": "1",
        }

    def _backoff(self, attempt: int) -> float:
        base = max(self.limiter.base, 0.5)
        return min(MAX_INTERVAL, base * (2 ** attempt)) * random.uniform(0.8, 1.2)

    async def _fetch_page(self, url: str) -> Optional[httpx.Response]:
        """GET a page with retry/backoff. Raises HTTPStatusError for non-retry
        4xx (e.g. 404). Returns None only if shutdown was requested."""
        for attempt in range(self.max_retries + 1):
            await self.limiter.acquire()
            if self.stop_event.is_set():
                return None
            try:
                resp = await self.client.get(url)
                self.requests += 1
            except (httpx.TransportError, httpx.TimeoutException):
                if attempt >= self.max_retries:
                    raise
                self.limiter.penalize()
                await asyncio.sleep(self._backoff(attempt))
                continue
            if resp.status_code in RETRY_STATUS:
                if attempt >= self.max_retries:
                    resp.raise_for_status()
                retry_after = parse_retry_after(resp.headers.get("retry-after"))
                self.limiter.penalize(retry_after)
                await asyncio.sleep(retry_after if retry_after is not None else self._backoff(attempt))
                continue
            resp.raise_for_status()  # other 4xx -> caller handles
            self.limiter.reward()
            return resp
        return None

    # -- Extraction ------------------------------------------------------- #
    def _extract_images(self, soup: BeautifulSoup, page_url: str):
        def resolve(src):
            if not src or not src.strip():
                return None
            abs_url = urljoin(page_url, src.strip())
            # An empty/whitespace attr resolves back to the page itself - skip it
            # so the HTML page isn't queued and fetched as a bogus image.
            return abs_url if abs_url != page_url else None

        for img in soup.find_all("img"):
            src = img.get("data-largest") or img.get("data-src") or img.get("src")
            if not src and img.get("srcset"):
                src = largest_from_srcset(img.get("srcset"))
            url = resolve(src)
            if url:
                yield url, img.get("alt", "") or ""
        for source in soup.find_all("source", srcset=True):
            url = resolve(largest_from_srcset(source.get("srcset")))
            if url:
                yield url, ""
        for span in soup.find_all("span", attrs={"data-zoom": True}):
            url = resolve(span.get("data-zoom"))
            if url:
                yield url, span.get("data-img-att-alt", "") or ""

    def _extract_links(self, soup: BeautifulSoup, page_url: str):
        for tag in soup.find_all("a", href=True):
            yield urljoin(page_url, tag["href"])

    # -- Queue management ------------------------------------------------- #
    @staticmethod
    def _page_key(url: str) -> str:
        """Canonical dedupe key: drop the #fragment and volatile tracking query
        params, then sort the rest, so one logical page maps to one key (keeps
        session/utm params from exploding the frontier)."""
        url = urldefrag(url).url
        parsed = urlparse(url)
        if parsed.query:
            kept = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                    if k.lower() not in VOLATILE_QUERY]
            kept.sort()
            parsed = parsed._replace(query=urlencode(kept))
            url = urlunparse(parsed)
        return url

    def _enqueue_page(self, url: str):
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return
        if parsed.netloc.lower() != self.base_domain:
            return
        # Restrict on the parsed path (not raw-URL startswith) so sibling-prefix
        # false positives ('/de' matching '/department') and scheme/host string
        # differences can't slip through. base_path always ends with '/'.
        if not parsed.path.startswith(self.base_path):
            return
        key = self._page_key(url)
        if key in self.seen_pages:
            return
        if self.max_pages and len(self.seen_pages) >= self.max_pages:
            return
        self.seen_pages.add(key)
        self.page_queue.put_nowait(key)

    async def _enqueue_image(self, url: str, source: str, alt: str):
        low = url.lower()
        if self.include and self.include.lower() not in low:
            return
        if self.exclude and self.exclude.lower() in low:
            return
        if url in self.image_seen:
            return
        self.image_seen[url] = (source, alt)
        await self.image_queue.put((url, source, alt))

    # -- Workers ---------------------------------------------------------- #
    async def _process_page(self, url: str):
        try:
            resp = await self._fetch_page(url)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            # Permanent client errors (4xx except 429) won't change on retry, so
            # mark them processed - otherwise they'd be re-fetched on every resume.
            if 400 <= code < 500 and code != 429:
                self.processed_pages.add(url)
                self.log.verbose(f"[!] HTTP {code} page (giving up): {url}")
            else:
                self.log.verbose(f"[!] HTTP {code} page (will retry on resume): {url}")
            return
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            self.log.verbose(f"[!] network fail page (will retry on resume): {url} ({exc})")
            return
        if resp is None:
            return  # shutting down

        final_url = self._page_key(str(resp.url))
        self.processed_pages.add(url)
        if final_url != url:
            self.seen_pages.add(final_url)       # keep the processed ⊆ seen invariant
            self.processed_pages.add(final_url)

        try:
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            self.log.verbose(f"[!] parse error {url}: {exc}")
            return

        for image_url, alt in self._extract_images(soup, final_url):
            if self.stop_event.is_set():
                return
            await self._enqueue_image(image_url, final_url, alt)

        new_links = 0
        for link in self._extract_links(soup, final_url):
            before = len(self.seen_pages)
            self._enqueue_page(link)
            new_links += len(self.seen_pages) - before

        if new_links:
            self.log.verbose(f"Page {url[:70]} -> {new_links} new links "
                             f"(page queue {self.page_queue.qsize()})")

    async def _download_image(self, url: str, source: str, alt: str):
        if url in self.image_done:
            return
        for attempt in range(self.max_retries + 1):
            await self.limiter.acquire()
            if self.stop_event.is_set():
                return
            tmp: Optional[Path] = None
            try:
                async with self.client.stream("GET", url) as resp:
                    self.requests += 1
                    if resp.status_code == 404:
                        self.image_done.add(url)
                        return
                    if resp.status_code in RETRY_STATUS:
                        if attempt >= self.max_retries:
                            self.log.verbose(f"[!] give up image {resp.status_code}: {url}")
                            return
                        retry_after = parse_retry_after(resp.headers.get("retry-after"))
                        self.limiter.penalize(retry_after)
                        await asyncio.sleep(retry_after if retry_after is not None else self._backoff(attempt))
                        continue
                    resp.raise_for_status()

                    ctype = resp.headers.get("content-type", "").split(";")[0].strip().lower()
                    if not self._looks_like_image(url, ctype):
                        self.log.verbose(f"[!] skip non-image {url} ({ctype})")
                        self.image_done.add(url)
                        return
                    clen = resp.headers.get("content-length")
                    if clen and clen.isdigit() and int(clen) > self.max_bytes:
                        self.log.verbose(f"[!] skip large {int(clen)/1048576:.1f}MB: {url}")
                        self.image_done.add(url)
                        return

                    filename = build_filename(url, ctype)
                    dest = self.output / filename
                    ext = dest.suffix
                    tmp = dest.with_name(dest.name + ".part")
                    size = 0
                    oversized = False
                    with open(tmp, "wb") as fh:
                        async for chunk in resp.aiter_bytes(CHUNK):
                            size += len(chunk)
                            if size > self.max_bytes:
                                oversized = True
                                break
                            fh.write(chunk)
                    if oversized:
                        with contextlib.suppress(OSError):
                            tmp.unlink()
                        tmp = None
                        self.log.verbose(f"[!] skip oversized stream: {url}")
                        self.image_done.add(url)
                        return

                # Embed metadata into the temp file, *then* atomically swap it in,
                # so a crash mid-metadata can never corrupt the final image.
                embed_metadata(tmp, ext, alt, source)
                os.replace(tmp, dest)
                tmp = None
                self.image_done.add(url)
                self.images_saved += 1
                self.limiter.reward()
                self.log.verbose(f"[+] {filename}  (<- {url[:60]})")
                return

            except httpx.HTTPStatusError as exc:
                self.log.verbose(f"[!] skip image {exc.response.status_code}: {url}")
                self.image_done.add(url)
                return
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt >= self.max_retries:
                    self.log.verbose(f"[!] image network give-up (retry on resume): {url} ({exc})")
                    return
                self.limiter.penalize()
                await asyncio.sleep(self._backoff(attempt))
            except OSError as exc:
                self.log.info(f"[!] file error for {url}: {exc}")
                return
            except Exception as exc:
                self.log.info(f"[!] unexpected image error {url}: {type(exc).__name__}: {exc}")
                return
            finally:
                if tmp is not None:
                    with contextlib.suppress(OSError):
                        tmp.unlink()

    @staticmethod
    def _looks_like_image(url: str, content_type: str) -> bool:
        if content_type.startswith("image/"):
            return True
        ext = os.path.splitext(urlparse(url).path)[1]
        return is_supported_ext(ext)

    async def _page_worker(self):
        while True:
            url = await self.page_queue.get()
            try:
                if not self.stop_event.is_set():
                    await self._process_page(url)
            except Exception as exc:  # never let a worker die silently
                self.log.info(f"[!] page worker error {url}: {type(exc).__name__}: {exc}")
            finally:
                self.page_queue.task_done()

    async def _image_worker(self):
        while True:
            url, source, alt = await self.image_queue.get()
            try:
                if not self.stop_event.is_set():
                    await self._download_image(url, source, alt)
            except Exception as exc:
                self.log.info(f"[!] image worker error {url}: {type(exc).__name__}: {exc}")
            finally:
                self.image_queue.task_done()

    async def _progress_loop(self):
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(1.0)
                interval = f"interval {self.limiter.min_interval:.2f}s"
                if self.limiter.throttle_interval:
                    interval += f" (limit~{self.limiter.throttle_interval:.2f}s)"
                self.log.status(
                    f"pages {len(self.processed_pages)}/{len(self.seen_pages)} "
                    f"(q{self.page_queue.qsize()}) | "
                    f"images {self.images_saved}/{len(self.image_seen)} "
                    f"(q{self.image_queue.qsize()}) | "
                    f"reqs {self.requests} | {interval}"
                )
        except asyncio.CancelledError:
            pass

    async def _checkpoint_loop(self):
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(CHECKPOINT_INTERVAL)
                self.save_state(quiet=True)
        except asyncio.CancelledError:
            pass

    # -- State ------------------------------------------------------------ #
    def save_state(self, quiet: bool = False):
        state = {
            "version": STATE_VERSION,
            "start_url": self.start_url,
            "base_domain": self.base_domain,
            "base_path": self.base_path,
            "include_filter": self.include,
            "exclude_filter": self.exclude,
            "seen_pages": sorted(self.seen_pages),
            "processed_pages": sorted(self.processed_pages),
            "image_seen": [[u, s, a] for u, (s, a) in self.image_seen.items()],
            "image_done": sorted(self.image_done),
            "requests": self.requests,
            "min_interval": self.limiter.min_interval,
            "throttle_interval": self.limiter.throttle_interval,
            "saved_at": time.time(),
        }
        try:
            self.output.mkdir(parents=True, exist_ok=True)
            # Unique temp name per write: a fixed "<state>.tmp" is racy when two
            # writers (e.g. an accidental second instance) share the dir - both
            # rename the same path and the loser hits ENOENT. mkstemp gives each
            # write its own file; the final os.replace is still atomic.
            fd, tmp_name = tempfile.mkstemp(
                dir=self.output, prefix=STATE_FILE + ".", suffix=".tmp")
            tmp = Path(tmp_name)
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(state, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, self.output / STATE_FILE)
            except BaseException:
                with contextlib.suppress(OSError):
                    tmp.unlink()
                raise
            if not quiet:
                self.log.info(f"--- State saved to {self.output / STATE_FILE} ---")
        except Exception as exc:
            self.log.info(f"!!! Error saving state: {exc}")
        # Independent of local-state success: keep the shared per-domain cache warm.
        self._save_domain_store()

    def load_state(self) -> bool:
        path = self.output / STATE_FILE
        if not path.exists():
            self.log.info(f"--- No state file at {path}; starting fresh. ---")
            return False
        try:
            with open(path) as fh:
                state = json.load(fh)
        except Exception as exc:
            self.log.info(f"!!! Error loading state ({exc}); starting fresh. ---")
            return False

        self.base_domain = state.get("base_domain", "")
        self.base_path = state.get("base_path", "")
        if state.get("include_filter") is not None:
            self.include = state["include_filter"]
        if state.get("exclude_filter") is not None:
            self.exclude = state["exclude_filter"]
        self.seen_pages = set(state.get("seen_pages", []))
        self.processed_pages = set(state.get("processed_pages", []))
        self.image_seen = {row[0]: (row[1], row[2]) for row in state.get("image_seen", []) if row}
        self.image_done = set(state.get("image_done", []))
        self.requests = state.get("requests", 0)
        # Restore the *learned limit* (not the raw min_interval, which may be a
        # transient penalty spike): resume SAFE_MARGIN above the widest interval
        # the server throttled last run, so we keep the discovered delay instead
        # of re-probing - and re-eating the bans - from base every resume.
        self.limiter.restore(state.get("throttle_interval", 0.0))
        self.log.info(
            f"--- Resumed: {len(self.processed_pages)} pages done, "
            f"{len(self.seen_pages) - len(self.processed_pages)} pending, "
            f"{len(self.image_done)} images done, "
            f"{len(self.image_seen) - len(self.image_done)} images pending. ---"
        )
        if self.limiter.throttle_interval:
            self.log.info(
                f"--- Learned delay for {self.base_domain or 'domain'}: "
                f"~{self.limiter.throttle_interval:.2f}s throttle limit -> "
                f"starting at {self.limiter.min_interval:.2f}s interval, "
                f"probing down to ~{max(self.limiter.base, self.limiter.throttle_interval * RESUME_PROBE_FRACTION):.2f}s. ---")
        return True

    # -- Shared per-domain delay cache ------------------------------------ #
    @staticmethod
    def _store_limit(entry) -> float:
        """Read a domain's learned throttle interval from a store entry, tolerating
        both the dict form and a bare number (older/hand-edited files)."""
        if isinstance(entry, dict):
            try:
                return float(entry.get("throttle_interval", 0.0) or 0.0)
            except (TypeError, ValueError):
                return 0.0
        try:
            return float(entry or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _load_domain_store(self):
        """Seed the limiter from a delay learned for this domain on a *previous*
        run - even one that used a different -o dir. Merges (max) with whatever
        the local resume state already restored, so the most cautious value wins."""
        if not self.domain_store or not self.base_domain:
            return
        try:
            with open(self.domain_store) as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            return
        if not isinstance(data, dict):
            return
        learned = self._store_limit(data.get(self.base_domain))
        if learned > self.limiter.throttle_interval:
            self.limiter.restore(learned)
            self.log.info(
                f"--- Shared delay memory: {self.base_domain} limit "
                f"~{self.limiter.throttle_interval:.2f}s -> starting at "
                f"{self.limiter.min_interval:.2f}s interval. ---")

    def _save_domain_store(self):
        """Persist this domain's learned limit back to the shared cache. Reads,
        merges, and atomically rewrites so a concurrent crawler on *another*
        domain isn't clobbered, and the stored limit only ratchets up (cautious)."""
        if not self.domain_store or not self.base_domain:
            return
        if self.limiter.throttle_interval <= 0:
            return  # nothing learned this run; don't write a meaningless 0
        store_path = Path(self.domain_store)
        try:
            store_path.parent.mkdir(parents=True, exist_ok=True)
            data = {}
            try:
                with open(store_path) as fh:
                    loaded = json.load(fh)
                if isinstance(loaded, dict):
                    data = loaded
            except (OSError, ValueError):
                data = {}
            merged = max(self._store_limit(data.get(self.base_domain)),
                         self.limiter.throttle_interval)
            data[self.base_domain] = {
                "throttle_interval": round(merged, 3),
                "updated_at": time.time(),
            }
            fd, tmp_name = tempfile.mkstemp(
                dir=store_path.parent, prefix=store_path.name + ".", suffix=".tmp")
            tmp = Path(tmp_name)
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(data, fh, indent=2, sort_keys=True)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, store_path)
            except BaseException:
                with contextlib.suppress(OSError):
                    tmp.unlink()
                raise
        except Exception as exc:
            self.log.verbose(f"[!] could not update domain store {store_path}: {exc}")

    # -- Setup / run ------------------------------------------------------ #
    def setup_bounds(self):
        parsed = urlparse(self.start_url)
        if not self.base_domain:
            self.base_domain = parsed.netloc.lower()
        if not self.base_path:
            if self.path_override:
                # An override is a directory prefix; just normalise the slashes.
                base = self.path_override
            else:
                # Restrict to the directory *containing* the start resource, so
                # the start page and its siblings stay in scope. A trailing-slash
                # URL is itself a directory; otherwise drop the final segment.
                path = parsed.path or "/"
                base = path if path.endswith("/") else path.rsplit("/", 1)[0] + "/"
            if not base.startswith("/"):
                base = "/" + base
            if not base.endswith("/"):
                base += "/"
            self.base_path = base
        scheme = parsed.scheme or "https"
        self.base_path_restriction = f"{scheme}://{self.base_domain}{self.base_path}"

    def _install_signals(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
                self._signals_installed.append(sig)
            except (NotImplementedError, RuntimeError):
                pass  # e.g. Windows / non-main thread

    def _on_signal(self, sig):
        if not self.stop_event.is_set():
            self.log.info(f"\n--- Signal {signal.Signals(sig).name} received; "
                          f"finishing in-flight work and saving state "
                          f"(press Ctrl-C again to force-quit). ---")
            self.stop_event.set()
        else:
            # Second signal: restore default handlers so another Ctrl-C aborts
            # immediately even if graceful shutdown is somehow stuck.
            self._remove_signals()

    def _remove_signals(self):
        loop = asyncio.get_running_loop()
        for sig in self._signals_installed:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)

    # -- Single-instance lock --------------------------------------------- #
    @staticmethod
    def _read_lock_pid(path: Path) -> Optional[int]:
        try:
            text = path.read_text().strip()
            return int(text) if text else None
        except (OSError, ValueError):
            return None

    @staticmethod
    def _pid_alive(pid: Optional[int]) -> bool:
        if not pid or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True  # exists, just owned by another user
        return True

    def _acquire_lock(self) -> bool:
        """Refuse to run if another live instance owns this output dir.

        Two crawlers sharing one -o dir clobber each other's state file and
        double the request rate (server throttling). The lock holds our PID;
        a lock left by a dead process is treated as stale and reclaimed.
        """
        self.output.mkdir(parents=True, exist_ok=True)
        path = self.output / LOCK_FILE
        for _ in range(2):  # at most one stale-lock reclaim, then give up
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                other = self._read_lock_pid(path)
                if self._pid_alive(other):
                    self.log.info(
                        f"!!! Another crawler (pid {other}) is using "
                        f"{self.output}; refusing to start a second instance. "
                        f"Stop it first, or use a different -o output dir.")
                    return False
                self.log.info(
                    f"--- Reclaiming stale lock {path} (pid {other} not "
                    f"running). ---")
                with contextlib.suppress(OSError):
                    path.unlink()
                continue
            with os.fdopen(fd, "w") as fh:
                fh.write(str(os.getpid()))
            self._lock_path = path
            return True
        self.log.info(f"!!! Could not acquire lock {path}.")
        return False

    def _release_lock(self):
        if self._lock_path is None:
            return
        # Only remove the lock if it is still ours (don't delete a lock a
        # reclaiming instance may have rewritten).
        if self._read_lock_pid(self._lock_path) == os.getpid():
            with contextlib.suppress(OSError):
                self._lock_path.unlink()
        self._lock_path = None

    async def _seed(self):
        if self.resume:
            self.load_state()
        self.setup_bounds()
        self._load_domain_store()
        self.log.info(f"Crawl: {self.start_url}")
        self.log.info(f"Output: {self.output}  |  pages x{self.page_conc}  images x{self.image_conc}  "
                      f"delay {self.limiter.base}s (+/-{int(self.limiter.jitter*100)}%)")
        self.log.info(f"Restricting to: {self.base_path_restriction}")
        if self.include:
            self.log.info(f"Include filter: '{self.include}'")
        if self.exclude:
            self.log.info(f"Exclude filter: '{self.exclude}'")

        # Re-queue everything discovered-but-unfinished (covers in-flight loss).
        pending_pages = [p for p in self.seen_pages if p not in self.processed_pages]
        for page in pending_pages:
            self.page_queue.put_nowait(page)
        if not self.seen_pages:
            self._enqueue_page(self.start_url)
            if not self.seen_pages:  # start_url outside its own restriction (shouldn't happen)
                key = self._page_key(self.start_url)
                self.seen_pages.add(key)
                self.page_queue.put_nowait(key)
        if pending_pages:
            self.log.info(f"Re-queued {len(pending_pages)} pending pages.")

        pending_images = [(u, s, a) for u, (s, a) in self.image_seen.items() if u not in self.image_done]
        for job in pending_images:
            await self.image_queue.put(job)
        if pending_images:
            self.log.info(f"Re-queued {len(pending_images)} pending images.")

    async def _join_or_stop(self, queue: asyncio.Queue):
        join_task = asyncio.ensure_future(queue.join())
        stop_task = asyncio.ensure_future(self.stop_event.wait())
        try:
            await asyncio.wait({join_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (join_task, stop_task):
                if not task.done():
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def run(self) -> bool:
        self.output.mkdir(parents=True, exist_ok=True)
        if not self._acquire_lock():
            return False
        self._install_signals()
        limits = httpx.Limits(
            max_connections=self.page_conc + self.image_conc + 10,
            max_keepalive_connections=20,
        )
        kwargs = dict(headers=self._headers(), follow_redirects=True, limits=limits,
                      timeout=httpx.Timeout(self.timeout))
        try:
            self.client = httpx.AsyncClient(http2=True, **kwargs)
        except ImportError:
            self.client = httpx.AsyncClient(http2=False, **kwargs)

        workers: list[asyncio.Task] = []
        interrupted = False
        try:
            async with self.client:
                workers = [asyncio.create_task(self._page_worker()) for _ in range(self.page_conc)]
                workers += [asyncio.create_task(self._image_worker()) for _ in range(self.image_conc)]
                aux = [asyncio.create_task(self._progress_loop()),
                       asyncio.create_task(self._checkpoint_loop())]
                workers += aux

                await self._seed()
                # Phase 1: crawl all pages (children + image jobs enqueued here).
                await self._join_or_stop(self.page_queue)
                # Phase 2: finish image downloads (skipped on a stop request).
                if not self.stop_event.is_set():
                    await self._join_or_stop(self.image_queue)

                interrupted = self.stop_event.is_set()
        except asyncio.CancelledError:
            interrupted = True
            raise
        finally:
            self.stop_event.set()
            for task in workers:
                task.cancel()
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
            self._remove_signals()
            self.log.flush()
            self.save_state(quiet=False)
            self._release_lock()
            self.log.info(f"\n--- Crawl {'interrupted' if interrupted else 'finished'} ---")
            self.log.info(f"Pages processed: {len(self.processed_pages)} "
                          f"(discovered {len(self.seen_pages)})")
            self.log.info(f"Images saved this run: {self.images_saved} "
                          f"(done total {len(self.image_done)}/{len(self.image_seen)})")
            self.log.info(f"Total requests: {self.requests}")
            self.log.info(f"Output: {self.output.resolve()}")
            if interrupted:
                self.log.info("Re-run with --resume to continue.")
        return not interrupted


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Asynchronous website image crawler.")
    p.add_argument("start_url", nargs="?", default=DEFAULT_START_URL,
                   help=f"Starting URL (default: {DEFAULT_START_URL}).")
    p.add_argument("--path-restriction", "--path-restriction-override", dest="path_restriction",
                   default=None,
                   help="Restrict the crawl to paths starting with this string "
                        "(default: the directory of start_url's path).")
    p.add_argument("-o", "--output", default=DEFAULT_OUTPUT,
                   help="Folder for downloaded images (default: timestamped folder).")
    p.add_argument("-i", "--include_filter", default=None,
                   help="Only download images whose URL contains this string.")
    p.add_argument("-e", "--exclude_filter", default=DEFAULT_EXCLUDE,
                   help=f"Skip images whose URL contains this string (default: '{DEFAULT_EXCLUDE}').")
    p.add_argument("--page-concurrency", "--page_workers", dest="page_concurrency",
                   type=int, default=DEFAULT_PAGE_CONCURRENCY,
                   help=f"Concurrent page fetches (default: {DEFAULT_PAGE_CONCURRENCY}).")
    p.add_argument("--image-concurrency", "--image_workers", dest="image_concurrency",
                   type=int, default=DEFAULT_IMAGE_CONCURRENCY,
                   help=f"Concurrent image downloads (default: {DEFAULT_IMAGE_CONCURRENCY}).")
    p.add_argument("--delay", "--request_delay", dest="delay", type=float, default=DEFAULT_DELAY,
                   help=f"Base seconds between request starts, globally (default: {DEFAULT_DELAY}).")
    p.add_argument("--jitter", type=float, default=DEFAULT_JITTER,
                   help=f"Random +/- fraction applied to the delay (default: {DEFAULT_JITTER}).")
    p.add_argument("--max-pages", type=int, default=0,
                   help="Stop after discovering this many pages (0 = unlimited).")
    p.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                   help=f"Retries per request on 429/5xx/network errors (default: {DEFAULT_MAX_RETRIES}).")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                   help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT}).")
    p.add_argument("--max-image-size", "--max_image_size", dest="max_image_size",
                   type=float, default=DEFAULT_MAX_IMAGE_MB,
                   help=f"Max image size in MB (default: {DEFAULT_MAX_IMAGE_MB}).")
    p.add_argument("--user-agent", default=None, help="Override the User-Agent header.")
    p.add_argument("--resume", action="store_true",
                   help="Resume from crawl_state.json in the output folder.")
    p.add_argument("--domain-store", default=DEFAULT_DOMAIN_STORE,
                   help="JSON file caching the learned per-domain delay across runs "
                        f"and output dirs (default: {DEFAULT_DOMAIN_STORE}).")
    p.add_argument("--no-domain-store", action="store_true",
                   help="Don't read or write the shared per-domain delay cache.")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Per-page / per-image logging.")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    parsed = urlparse(args.start_url)
    if not parsed.scheme or not parsed.netloc:
        print(f"Error: invalid start_url '{args.start_url}'. "
              f"Include a scheme and domain, e.g. https://example.com/path/")
        return 2

    logger = Logger(verbose=args.verbose)
    crawler = Crawler(args, logger)

    start = time.time()
    try:
        completed = asyncio.run(crawler.run())
    except KeyboardInterrupt:
        # Fallback if a signal slipped past the handler.
        completed = False
    elapsed = time.time() - start
    print(f"Total execution time: {elapsed:.2f}s.")
    return 0 if completed else 1


if __name__ == "__main__":
    sys.exit(main())
