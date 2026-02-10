"""
GDELT Async Pipeline with R2 Upload
=====================================
Downloads, decompresses, merges, and filters GDELT data (Events, Mentions, GKG),
then uploads to Cloudflare R2 and deletes local files to conserve disk space.

3-Stage Async Pipeline:
  Stage 1: Download  - fetches zip files from GDELT servers
  Stage 2: Process   - decompresses, merges, filters via DuckDB
  Stage 3: Upload    - uploads processed CSV to R2, deletes local file

Stages are connected via asyncio.Queue so they don't block each other.

Supports both GDELT 1.0 (2004-2015, daily) and GDELT 2.0 (2015+, 15-minute updates).

Features:
- Async download with configurable workers
- DuckDB-based decompression and merging
- Cloudflare R2 upload with delete-after-upload
- Rich live terminal dashboard
- Graceful shutdown (Ctrl+C)
- Resume capability via state file
- Error logging
- Filtering for Events, Mentions, and GKG

Usage:
    python gdelt_pipeline_r2.py --download-workers 5
    python gdelt_pipeline_r2.py --gdelt-version 1 --start-date 20040101 --end-date 20150218
    python gdelt_pipeline_r2.py --gdelt-version 2 --start-date 20150219

Environment Variables (for R2):
    R2_ACCESS_KEY_ID      - R2 access key
    R2_SECRET_ACCESS_KEY  - R2 secret key
    R2_ENDPOINT_URL       - R2 endpoint (default: EU endpoint)
    R2_BUCKET_NAME        - R2 bucket name (default: europe)
"""

import asyncio
import aiohttp
import aiofiles
import boto3
import duckdb
import json
import logging
import os
import re
import shutil
import signal
import sys
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import argparse
import hashlib

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
from rich import box

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    # Directories (Linux paths for RunPod)
    gdelt_base_dir: Path = Path("/workspace/gdelt")
    temp_dir: Path = Path("/workspace/gdelt/tmp")

    # GDELT URLs - Version 2 (2015+, 15-minute updates)
    masterlist_v2_url: str = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    base_download_v2_url: str = "http://data.gdeltproject.org/gdeltv2/"

    # GDELT URLs - Version 1 (2004-2015, daily updates)
    masterlist_v1_url: str = "http://data.gdeltproject.org/events/filesizes"
    base_download_v1_url: str = "http://data.gdeltproject.org/events/"

    # GDELT version to use (1 or 2)
    gdelt_version: int = 2

    # Worker configuration
    download_workers: int = 5
    process_workers: int = 2
    upload_workers: int = 3
    duckdb_memory_limit: str = "4GB"

    # File patterns
    file_types: List[str] = field(default_factory=lambda: ["export", "mentions", "gkg"])

    # State and logging
    state_file: Path = field(default_factory=lambda: Path("/workspace/gdelt/pipeline_state.json"))
    log_file: Path = field(default_factory=lambda: Path("/workspace/gdelt/pipeline.log"))
    error_log_file: Path = field(default_factory=lambda: Path("/workspace/gdelt/pipeline_errors.log"))

    # Download settings
    download_timeout: int = 300  # 5 minutes
    max_retries: int = 3
    retry_delay: int = 5

    # Date range (None means all available)
    start_date: Optional[str] = None  # Format: YYYYMMDD
    end_date: Optional[str] = None    # Format: YYYYMMDD

    # R2 Configuration
    r2_bucket_name: str = "europe"
    r2_endpoint_url: str = "https://2a139e9393f803634546ad9d541d37b9.r2.cloudflarestorage.com"
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""

    # Queue sizes (limits memory usage)
    download_queue_size: int = 20
    upload_queue_size: int = 10

    # Minimum free disk space (GB) - pause downloads when free space drops below this
    min_free_disk_gb: float = 5.0


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(config: Config) -> logging.Logger:
    """Setup logging with both file and console handlers."""
    logger = logging.getLogger("gdelt_pipeline")
    logger.setLevel(logging.DEBUG)

    # File handler
    file_handler = logging.FileHandler(config.log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    file_handler.setFormatter(file_format)

    # Error file handler
    error_handler = logging.FileHandler(config.error_log_file, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_format)

    logger.addHandler(file_handler)
    logger.addHandler(error_handler)

    # No console handler - the Rich dashboard handles console output
    return logger


# =============================================================================
# DASHBOARD / PROGRESS TRACKING
# =============================================================================

class PipelineStats:
    """Thread-safe stats for the live dashboard."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.start_time = time.time()

        # Download stats
        self.files_to_download: int = 0
        self.files_downloaded: int = 0
        self.files_download_skipped: int = 0
        self.files_download_failed: int = 0
        self.bytes_downloaded: int = 0
        self.current_download: str = ""

        # Process stats
        self.days_to_process: int = 0
        self.days_processed: int = 0
        self.days_process_failed: int = 0
        self.rows_processed: int = 0
        self.current_process: str = ""

        # Upload stats
        self.files_to_upload: int = 0
        self.files_uploaded: int = 0
        self.files_upload_failed: int = 0
        self.bytes_uploaded: int = 0
        self.files_deleted_after_upload: int = 0
        self.current_upload: str = ""

        # Queue depths
        self.download_queue_depth: int = 0
        self.upload_queue_depth: int = 0

        # Stage status
        self.download_stage_done: bool = False
        self.process_stage_done: bool = False
        self.upload_stage_done: bool = False

        # Recent log lines for the dashboard
        self.recent_logs: List[str] = []
        self.max_recent_logs: int = 12

    async def add_log(self, msg: str):
        async with self._lock:
            ts = datetime.now().strftime("%H:%M:%S")
            self.recent_logs.append(f"[dim]{ts}[/dim] {msg}")
            if len(self.recent_logs) > self.max_recent_logs:
                self.recent_logs.pop(0)

    def get_disk_usage(self, path: Path) -> str:
        """Get disk usage of a path."""
        try:
            total = 0
            if path.exists():
                for f in path.rglob("*"):
                    if f.is_file():
                        total += f.stat().st_size
            return self._fmt_bytes(total)
        except Exception:
            return "N/A"

    def get_free_disk(self) -> str:
        """Get free disk space on /workspace."""
        try:
            st = os.statvfs("/workspace")
            free = st.f_bavail * st.f_frsize
            return self._fmt_bytes(free)
        except Exception:
            try:
                usage = shutil.disk_usage("/workspace")
                return self._fmt_bytes(usage.free)
            except Exception:
                return "N/A"

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} PB"

    def elapsed(self) -> str:
        s = int(time.time() - self.start_time)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"


def build_dashboard(stats: PipelineStats, config: Config) -> Layout:
    """Build the Rich layout for the live dashboard."""
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="logs", size=min(stats.max_recent_logs + 2, 14)),
    )
    layout["body"].split_row(
        Layout(name="download", ratio=1),
        Layout(name="process", ratio=1),
        Layout(name="upload", ratio=1),
        Layout(name="disk", size=28),
    )

    # Header
    header_text = Text()
    header_text.append("  GDELT Pipeline ", style="bold white on blue")
    header_text.append(f"  v{config.gdelt_version}  ", style="bold yellow")
    header_text.append(f"  Elapsed: {stats.elapsed()}  ", style="bold green")
    status = "RUNNING"
    if stats.upload_stage_done:
        status = "COMPLETE"
    header_text.append(f"  [{status}]", style="bold cyan")
    layout["header"].update(Panel(header_text, style="bold"))

    # Download panel
    dl_total = stats.files_to_download or 1
    dl_done = stats.files_downloaded + stats.files_download_skipped + stats.files_download_failed
    dl_pct = min(100, dl_done / dl_total * 100) if dl_total else 0
    dl_status = "[green]DONE[/green]" if stats.download_stage_done else "[yellow]ACTIVE[/yellow]"

    dl_table = Table(show_header=False, box=None, padding=(0, 1))
    dl_table.add_column("key", style="dim", width=12)
    dl_table.add_column("val")
    dl_table.add_row("Status", dl_status)
    dl_table.add_row("Progress", f"[bold]{dl_done}[/bold] / {dl_total} ({dl_pct:.0f}%)")
    dl_table.add_row("Downloaded", f"[green]{stats.files_downloaded}[/green]")
    dl_table.add_row("Skipped", f"[yellow]{stats.files_download_skipped}[/yellow]")
    dl_table.add_row("Failed", f"[red]{stats.files_download_failed}[/red]")
    dl_table.add_row("Data", stats._fmt_bytes(stats.bytes_downloaded))
    dl_table.add_row("Queue", f"{stats.download_queue_depth}")
    if stats.current_download:
        dl_table.add_row("Current", f"[dim]{stats.current_download[:30]}[/dim]")
    layout["download"].update(Panel(dl_table, title="⬇ Download", border_style="blue"))

    # Process panel
    pr_total = stats.days_to_process or 1
    pr_done = stats.days_processed + stats.days_process_failed
    pr_pct = min(100, pr_done / pr_total * 100) if pr_total else 0
    pr_status = "[green]DONE[/green]" if stats.process_stage_done else "[yellow]ACTIVE[/yellow]"

    pr_table = Table(show_header=False, box=None, padding=(0, 1))
    pr_table.add_column("key", style="dim", width=12)
    pr_table.add_column("val")
    pr_table.add_row("Status", pr_status)
    pr_table.add_row("Progress", f"[bold]{pr_done}[/bold] / {pr_total} ({pr_pct:.0f}%)")
    pr_table.add_row("Processed", f"[green]{stats.days_processed}[/green]")
    pr_table.add_row("Failed", f"[red]{stats.days_process_failed}[/red]")
    pr_table.add_row("Rows", f"{stats.rows_processed:,}")
    if stats.current_process:
        pr_table.add_row("Current", f"[dim]{stats.current_process}[/dim]")
    layout["process"].update(Panel(pr_table, title="⚙ Process", border_style="yellow"))

    # Upload panel
    up_total = stats.files_to_upload or 1
    up_done = stats.files_uploaded + stats.files_upload_failed
    up_pct = min(100, up_done / up_total * 100) if up_total else 0
    up_status = "[green]DONE[/green]" if stats.upload_stage_done else "[yellow]ACTIVE[/yellow]"

    up_table = Table(show_header=False, box=None, padding=(0, 1))
    up_table.add_column("key", style="dim", width=12)
    up_table.add_column("val")
    up_table.add_row("Status", up_status)
    up_table.add_row("Progress", f"[bold]{up_done}[/bold] / {up_total} ({up_pct:.0f}%)")
    up_table.add_row("Uploaded", f"[green]{stats.files_uploaded}[/green]")
    up_table.add_row("Failed", f"[red]{stats.files_upload_failed}[/red]")
    up_table.add_row("Data", stats._fmt_bytes(stats.bytes_uploaded))
    up_table.add_row("Deleted", f"[cyan]{stats.files_deleted_after_upload}[/cyan]")
    up_table.add_row("Queue", f"{stats.upload_queue_depth}")
    if stats.current_upload:
        up_table.add_row("Current", f"[dim]{stats.current_upload[:30]}[/dim]")
    layout["upload"].update(Panel(up_table, title="☁ Upload R2", border_style="green"))

    # Disk panel
    disk_table = Table(show_header=False, box=None, padding=(0, 1))
    disk_table.add_column("key", style="dim", width=10)
    disk_table.add_column("val")
    disk_table.add_row("Free", f"[bold]{stats.get_free_disk()}[/bold]")
    disk_table.add_row("Temp", stats.get_disk_usage(config.temp_dir))
    disk_table.add_row("Output", stats.get_disk_usage(config.gdelt_base_dir))
    layout["disk"].update(Panel(disk_table, title="💾 Disk", border_style="magenta"))

    # Logs panel
    log_text = "\n".join(stats.recent_logs) if stats.recent_logs else "[dim]Waiting for events...[/dim]"
    layout["logs"].update(Panel(log_text, title="📋 Recent Activity", border_style="dim"))

    return layout


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

@dataclass
class PipelineState:
    """Tracks pipeline progress for resume capability."""
    downloaded_files: Set[str] = field(default_factory=set)
    processed_days: Set[str] = field(default_factory=set)  # Format: YYYYMMDD_type
    uploaded_files: Set[str] = field(default_factory=set)   # R2 keys that were uploaded
    failed_downloads: Dict[str, int] = field(default_factory=dict)  # url -> retry count
    failed_processing: Dict[str, str] = field(default_factory=dict)  # day_type -> error
    failed_uploads: Dict[str, str] = field(default_factory=dict)     # key -> error
    last_masterlist_update: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "downloaded_files": list(self.downloaded_files),
            "processed_days": list(self.processed_days),
            "uploaded_files": list(self.uploaded_files),
            "failed_downloads": self.failed_downloads,
            "failed_processing": self.failed_processing,
            "failed_uploads": self.failed_uploads,
            "last_masterlist_update": self.last_masterlist_update
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState":
        return cls(
            downloaded_files=set(data.get("downloaded_files", [])),
            processed_days=set(data.get("processed_days", [])),
            uploaded_files=set(data.get("uploaded_files", [])),
            failed_downloads=data.get("failed_downloads", {}),
            failed_processing=data.get("failed_processing", {}),
            failed_uploads=data.get("failed_uploads", {}),
            last_masterlist_update=data.get("last_masterlist_update")
        )


class StateManager:
    """Manages pipeline state persistence."""

    def __init__(self, state_file: Path, logger: logging.Logger):
        self.state_file = state_file
        self.logger = logger
        self.state = self._load_state()
        self._lock = asyncio.Lock()

    def _load_state(self) -> PipelineState:
        """Load state from file or backup, or create new."""
        for path in [self.state_file, self.state_file.with_suffix('.bak')]:
            if path.exists():
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    source = "backup" if path.suffix == '.bak' else "state"
                    self.logger.info(f"Loaded {source}: {len(data.get('downloaded_files', []))} downloaded, "
                                   f"{len(data.get('processed_days', []))} processed, "
                                   f"{len(data.get('uploaded_files', []))} uploaded")
                    return PipelineState.from_dict(data)
                except Exception as e:
                    self.logger.warning(f"Failed to load {path.name}: {e}")
        self.logger.info("No valid state found. Starting fresh.")
        return PipelineState()

    async def save_state(self):
        """Save current state to file with backup for crash safety."""
        async with self._lock:
            try:
                backup_file = self.state_file.with_suffix('.bak')
                if self.state_file.exists():
                    try:
                        shutil.copy2(self.state_file, backup_file)
                    except Exception:
                        pass
                state_json = json.dumps(self.state.to_dict(), indent=2)
                async with aiofiles.open(self.state_file, 'w') as f:
                    await f.write(state_json)
            except Exception as e:
                self.logger.error(f"Failed to save state: {e}")

    async def mark_downloaded(self, filename: str):
        self.state.downloaded_files.add(filename)

    async def mark_processed(self, day_type: str):
        self.state.processed_days.add(day_type)
        await self.save_state()

    async def mark_uploaded(self, r2_key: str):
        self.state.uploaded_files.add(r2_key)
        await self.save_state()

    async def mark_download_failed(self, url: str):
        self.state.failed_downloads[url] = self.state.failed_downloads.get(url, 0) + 1

    async def mark_processing_failed(self, day_type: str, error: str):
        self.state.failed_processing[day_type] = error

    async def mark_upload_failed(self, r2_key: str, error: str):
        self.state.failed_uploads[r2_key] = error
        await self.save_state()


# =============================================================================
# GDELT FILE UTILITIES
# =============================================================================

def parse_gdelt_filename(filename: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse GDELT filename to extract date, time, and type.

    GDELT 2.0 Examples (15-minute updates, 2015+):
        20240101000000.export.CSV.zip -> (20240101, 000000, export)
        20240101000000.mentions.CSV.zip -> (20240101, 000000, mentions)
        20240101000000.gkg.csv.zip -> (20240101, 000000, gkg)

    GDELT 1.0 Examples (daily updates, 2004-2015):
        20140101.export.CSV.zip -> (20140101, 000000, export)
        2013.export.CSV.zip -> (full year file, skipped)
    """
    # GDELT 2.0 patterns (15-minute updates)
    v2_patterns = [
        r'^(\d{8})(\d{6})\.export\.CSV\.zip$',
        r'^(\d{8})(\d{6})\.mentions\.CSV\.zip$',
        r'^(\d{8})(\d{6})\.gkg\.csv\.zip$',
    ]

    for pattern in v2_patterns:
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            date, time_str = match.groups()
            if 'export' in filename.lower():
                return (date, time_str, 'export')
            elif 'mentions' in filename.lower():
                return (date, time_str, 'mentions')
            elif 'gkg' in filename.lower():
                return (date, time_str, 'gkg')

    # GDELT 1.0 patterns (daily updates) - only has export (events)
    v1_patterns = [
        (r'^(\d{8})\.export\.CSV\.zip$', 'daily'),
        (r'^(\d{8})\.zip$', 'daily'),
        (r'^(\d{6})\.zip$', 'monthly'),
        (r'^(\d{4})\.zip$', 'yearly'),
    ]

    for pattern, period_type in v1_patterns:
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            date_part = match.group(1)
            if period_type == 'daily':
                return (date_part, '000000', 'export')
            elif period_type == 'monthly':
                return (date_part + '01', '000000', 'export')
            elif period_type == 'yearly':
                return (date_part + '0101', '000000', 'export')

    return None


def get_year_from_date(date_str: str) -> str:
    """Extract year from YYYYMMDD format."""
    return date_str[:4]


def get_output_path(config: Config, date_str: str, file_type: str) -> Path:
    """Get the output CSV path for a given date and file type."""
    year = get_year_from_date(date_str)
    type_dir = config.gdelt_base_dir / year / file_type
    type_dir.mkdir(parents=True, exist_ok=True)
    return type_dir / f"{date_str}.{file_type}.csv"


def get_r2_key(date_str: str, file_type: str) -> str:
    """Get the R2 object key for a given date and file type."""
    year = get_year_from_date(date_str)
    return f"{year}/{file_type}/{date_str}.{file_type}.csv"


# =============================================================================
# MASTERLIST MANAGEMENT
# =============================================================================

async def fetch_masterlist_v2(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """Fetch and parse the GDELT 2.0 masterlist."""
    logger.info("Fetching GDELT 2.0 masterlist...")
    async with aiohttp.ClientSession() as session:
        async with session.get(config.masterlist_v2_url) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch masterlist: HTTP {response.status}")
            content = await response.text()
            lines = content.strip().split('\n')
            entries = []
            for line in lines:
                parts = line.strip().split()
                if len(parts) >= 3:
                    size, hash_val, url = parts[0], parts[1], parts[2]
                    entries.append((size, hash_val, url))
            logger.info(f"Fetched GDELT 2.0 masterlist with {len(entries)} entries")
            return entries


async def fetch_masterlist_v1(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """Fetch and parse the GDELT 1.0 file list."""
    logger.info("Fetching GDELT 1.0 file list...")
    async with aiohttp.ClientSession() as session:
        async with session.get(config.masterlist_v1_url) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch GDELT 1.0 file list: HTTP {response.status}")
            content = await response.text()
            lines = content.strip().split('\n')
            entries = []
            for line in lines:
                parts = line.strip().split()
                if len(parts) >= 2:
                    size, filename = parts[0], parts[1]
                    url = config.base_download_v1_url + filename
                    entries.append((size, '', url))
            logger.info(f"Fetched GDELT 1.0 file list with {len(entries)} entries")
            return entries


async def fetch_masterlist(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """Fetch masterlist based on configured version."""
    if config.gdelt_version == 1:
        return await fetch_masterlist_v1(config, logger)
    else:
        return await fetch_masterlist_v2(config, logger)


def filter_masterlist_by_date(
    entries: List[Tuple[str, str, str]],
    start_date: Optional[str],
    end_date: Optional[str],
    file_types: List[str]
) -> List[Tuple[str, str, str]]:
    """Filter masterlist entries by date range and file types."""
    filtered = []
    for size, hash_val, url in entries:
        filename = url.split('/')[-1]
        parsed = parse_gdelt_filename(filename)
        if not parsed:
            continue
        date, time_str, ftype = parsed
        if ftype not in file_types:
            continue
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        filtered.append((size, hash_val, url))
    return filtered


def group_files_by_day(entries: List[Tuple[str, str, str]]) -> Dict[str, List[Tuple[str, str, str]]]:
    """Group masterlist entries by day and type (YYYYMMDD_type)."""
    grouped = defaultdict(list)
    for size, hash_val, url in entries:
        filename = url.split('/')[-1]
        parsed = parse_gdelt_filename(filename)
        if parsed:
            date, time_str, ftype = parsed
            key = f"{date}_{ftype}"
            grouped[key].append((size, hash_val, url))
    return dict(grouped)


# =============================================================================
# DATA FILTERING
# =============================================================================

# Event codes to remove (generic noise)
REMOVE_EVENT_CODES = ['010', '011', '016', '018']

# GKG source collection identifiers to remove
REMOVE_SOURCE_COLLECTIONS = [3, 5, 6]

# GKG theme patterns to remove (categories 6, 7, 9)
REMOVE_THEME_PATTERNS = [
    # Category 6: Humanitarian/Development Organizations (600+ NGOs)
    r'TAX_FNCACT_',       # Functional actor taxonomy
    r'CRISISLEX_',        # Crisis lexicon
    r'UNGP_',             # UN Guiding Principles
    r'WB_\d+_',           # World Bank categories
    r'HUMANITARIAN',      # Humanitarian themes
    r'NGO',               # NGO-related
    r'REFUGEES',          # Refugee-related
    r'DISPLACED',         # Displacement themes

    # Category 7: Demographics & Identity
    r'TAX_ETHNICITY_',    # Ethnic groups
    r'TAX_RELIGION_',     # Religious groups
    r'LANGUAGE_',         # Languages
    r'ETHNIC',            # Ethnic-related
    r'MINORITY',          # Minority groups
    r'INDIGENOUS',        # Indigenous peoples

    # Category 9: Nature & Environment (non-market relevant)
    r'ENV_',              # Environmental themes
    r'TAX_DISEASE_',      # Disease taxonomy
    r'SPECIES_',          # Species taxonomy
    r'POACHING',          # Poaching
    r'WILDLIFE',          # Wildlife
    r'ANIMAL',            # Animal-related
    r'PLANT',             # Plant-related
    r'BIODIVERSITY',      # Biodiversity
    r'CONSERVATION',      # Conservation (non-market)
    r'ECOSYSTEM',         # Ecosystem themes
    r'HABITAT',           # Habitat themes
    r'MIGRATION_ANIMAL',  # Animal migration
]

# Compiled regex for faster matching
REMOVE_THEME_REGEX = re.compile('|'.join(REMOVE_THEME_PATTERNS), re.IGNORECASE)


def get_events_filter_sql() -> str:
    """SQL filter for Events table."""
    codes = "', '".join(REMOVE_EVENT_CODES)
    return f"\"EventCode\" NOT IN ('{codes}')"


def get_mentions_filter_sql() -> str:
    """SQL filter for Mentions table."""
    return "TRY_CAST(\"Confidence\" AS INTEGER) >= 70 AND TRY_CAST(\"InRawText\" AS INTEGER) = 1"


def get_gkg_filter_sql() -> str:
    """SQL filter for GKG table - source collection filter."""
    collections = ", ".join(map(str, REMOVE_SOURCE_COLLECTIONS))
    return f"TRY_CAST(\"V2SourceCollectionIdentifier\" AS INTEGER) NOT IN ({collections}) OR \"V2SourceCollectionIdentifier\" IS NULL"


def filter_gkg_themes(themes_str: str) -> str:
    """Filter out irrelevant themes from GKG V2Themes field."""
    if not themes_str or themes_str == '' or themes_str is None:
        return themes_str
    themes = themes_str.split(';')
    filtered_themes = []
    for theme in themes:
        theme = theme.strip()
        if not theme:
            continue
        if not REMOVE_THEME_REGEX.search(theme):
            filtered_themes.append(theme)
    return ';'.join(filtered_themes)


def build_gkg_theme_filter_sql() -> str:
    """Build SQL CASE expression to filter themes in DuckDB."""
    patterns = '|'.join([f'[^;]*{p}[^;]*;?' for p in REMOVE_THEME_PATTERNS])
    return f"regexp_replace(V1Themes, '{patterns}', '', 'gi')"


# =============================================================================
# DOWNLOAD MANAGER
# =============================================================================

def get_free_disk_gb(path: Path) -> float:
    """Get free disk space in GB (instant, no scanning)."""
    try:
        usage = shutil.disk_usage(str(path))
        return usage.free / (1024 ** 3)
    except Exception:
        return 999.0  # assume plenty if check fails


class DownloadManager:
    """Manages async file downloads."""

    def __init__(self, config: Config, state_manager: StateManager,
                 logger: logging.Logger, stats: PipelineStats):
        self.config = config
        self.state = state_manager
        self.logger = logger
        self.stats = stats
        self.shutdown_event = asyncio.Event()
        self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Get or create a shared aiohttp session."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=self.config.download_workers, limit_per_host=50, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=self.config.download_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self):
        """Close the shared session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def download_file(self, url: str, dest_path: Path, semaphore: asyncio.Semaphore, expected_hash: Optional[str] = None) -> Optional[Path]:
        """Download a single file with retry logic. Returns dest_path on success, None on failure."""
        filename = url.split('/')[-1]

        # Skip if already downloaded
        if filename in self.state.state.downloaded_files:
            self.stats.files_download_skipped += 1
            return dest_path if dest_path.exists() else None

        # Check if file exists locally
        if dest_path.exists():
            await self.state.mark_downloaded(filename)
            self.stats.files_download_skipped += 1
            return dest_path

        # Wait for free disk space BEFORE acquiring semaphore
        while not self.shutdown_event.is_set():
            free_gb = get_free_disk_gb(self.config.gdelt_base_dir)
            if free_gb >= self.config.min_free_disk_gb:
                break
            await asyncio.sleep(5)
        if self.shutdown_event.is_set():
            return None

        async with semaphore:
            self.stats.current_download = filename
            session = await self._ensure_session()

            for attempt in range(self.config.max_retries):
                if self.shutdown_event.is_set():
                    return None

                try:
                    async with session.get(url) as response:
                        if response.status == 404:
                            self.logger.debug(f"File not found (404), skipping: {filename}")
                            await self.state.mark_download_failed(url)
                            self.stats.files_download_failed += 1
                            return None

                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}")

                        temp_path = dest_path.with_suffix('.tmp')
                        file_size = 0
                        async with aiofiles.open(temp_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                if self.shutdown_event.is_set():
                                    temp_path.unlink(missing_ok=True)
                                    return None
                                await f.write(chunk)
                                file_size += len(chunk)

                        temp_path.rename(dest_path)

                    await self.state.mark_downloaded(filename)
                    self.stats.files_downloaded += 1
                    self.stats.bytes_downloaded += file_size
                    self.logger.debug(f"Downloaded: {filename}")
                    return dest_path

                except Exception as e:
                    self.logger.warning(f"Download attempt {attempt + 1}/{self.config.max_retries} failed for {filename}: {e}")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(self.config.retry_delay * (attempt + 1))

            await self.state.mark_download_failed(url)
            self.stats.files_download_failed += 1
            self.logger.warning(f"Failed to download after {self.config.max_retries} attempts: {filename}")
            return None

    async def download_day(self, day_key: str, entries: List[Tuple[str, str, str]], semaphore: asyncio.Semaphore) -> List[Path]:
        """Download all files for a specific day concurrently."""
        day_temp_dir = self.config.temp_dir / f"{day_key}"
        day_temp_dir.mkdir(parents=True, exist_ok=True)

        tasks = []
        for size, hash_val, url in entries:
            filename = url.split('/')[-1]
            dest_path = day_temp_dir / filename
            tasks.append(self.download_file(url, dest_path, semaphore, hash_val))

        results = await asyncio.gather(*tasks)
        return [p for p in results if p is not None]


# =============================================================================
# DATA PROCESSING WITH DUCKDB
# =============================================================================

class DataProcessor:
    """Processes GDELT files using DuckDB."""

    # Column definitions for each file type
    EVENTS_COLUMNS = [
        "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
        "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
        "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
        "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
        "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
        "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
        "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
        "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
        "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
        "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
        "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
        "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
        "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
        "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
        "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
        "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
        "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"
    ]

    MENTIONS_COLUMNS = [
        "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
        "MentionSourceName", "MentionIdentifier", "SentenceID", "Actor1CharOffset",
        "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence",
        "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras"
    ]

    GKG_COLUMNS = [
        "GKGRECORDID", "V21DATE", "V2SourceCollectionIdentifier", "V2SourceCommonName",
        "V2DocumentIdentifier", "V1Counts", "V21Counts", "V1Themes", "V2EnhancedThemes",
        "V1Locations", "V2EnhancedLocations", "V1Persons", "V2EnhancedPersons",
        "V1Organizations", "V2EnhancedOrganizations", "V15Tone", "V21EnhancedDates",
        "V2GCAM", "V21SharingImage", "V21RelatedImages", "V21SocialImageEmbeds",
        "V21SocialVideoEmbeds", "V21Quotations", "V21AllNames", "V21Amounts",
        "V21TranslationInfo", "V2ExtrasXML"
    ]

    def __init__(self, config: Config, state_manager: StateManager,
                 logger: logging.Logger, stats: PipelineStats):
        self.config = config
        self.state = state_manager
        self.logger = logger
        self.stats = stats
        self.shutdown_event = asyncio.Event()
        self.executor = ThreadPoolExecutor(max_workers=config.process_workers)

    def _get_column_names(self, file_type: str) -> List[str]:
        if file_type == 'export':
            return self.EVENTS_COLUMNS
        elif file_type == 'mentions':
            return self.MENTIONS_COLUMNS
        elif file_type == 'gkg':
            return self.GKG_COLUMNS
        return []

    def _process_day_sync(self, day_key: str, zip_files: List[Path]) -> Optional[Tuple[Path, int]]:
        """Synchronous processing of a day's files using DuckDB. Returns (output_path, row_count) or None."""
        date, ftype = day_key.split('_')

        if not zip_files:
            self.logger.warning(f"No files to process for {day_key}")
            return None

        try:
            con = duckdb.connect(':memory:')
            con.execute(f"SET memory_limit='{self.config.duckdb_memory_limit}'")
            con.execute("SET threads=4")

            columns = self._get_column_names(ftype)
            if not columns:
                self.logger.error(f"Unknown file type: {ftype}")
                return None

            col_defs_sql = ", ".join([f'"{col}" VARCHAR' for col in columns])
            con.execute(f"CREATE TABLE raw_data ({col_defs_sql})")

            files_processed = 0
            for zip_path in zip_files:
                try:
                    if not zip_path.exists():
                        continue

                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        csv_names = [n for n in zf.namelist() if n.endswith('.csv') or n.endswith('.CSV')]
                        if not csv_names:
                            continue

                        csv_name = csv_names[0]
                        csv_content = zf.read(csv_name).decode('utf-8', errors='replace')

                        tid = threading.get_ident()
                        temp_csv = self.config.temp_dir / f"temp_{tid}_{day_key}_{zip_path.stem}.csv"
                        with open(temp_csv, 'w', encoding='utf-8') as f:
                            f.write(csv_content)

                        temp_csv_path = str(temp_csv)

                        try:
                            con.execute(f"""
                                CREATE TEMP TABLE file_data AS
                                SELECT * FROM read_csv('{temp_csv_path}',
                                    delim='\t',
                                    header=false,
                                    quote='',
                                    escape='',
                                    null_padding=true,
                                    ignore_errors=true,
                                    auto_detect=true,
                                    all_varchar=true
                                )
                            """)

                            file_cols = con.execute("SELECT * FROM file_data LIMIT 0").description
                            file_col_names = [col[0] for col in file_cols]

                            num_cols = min(len(columns), len(file_col_names))
                            select_cols = ", ".join([f'"{file_col_names[i]}"' for i in range(num_cols)])

                            if num_cols < len(columns):
                                select_cols += ", " + ", ".join(["NULL"] * (len(columns) - num_cols))

                            con.execute(f"INSERT INTO raw_data SELECT {select_cols} FROM file_data")
                            con.execute("DROP TABLE file_data")
                            files_processed += 1

                        except Exception as e:
                            self.logger.debug(f"Failed to process file {zip_path.name}: {e}")
                            try:
                                con.execute("DROP TABLE IF EXISTS file_data")
                            except:
                                pass
                        finally:
                            try:
                                temp_csv.unlink()
                            except:
                                pass

                except Exception as e:
                    self.logger.debug(f"Failed to extract {zip_path}: {e}")
                    continue

            if files_processed == 0:
                self.logger.warning(f"No valid data extracted for {day_key}")
                con.close()
                return None

            # Apply filters
            if ftype == 'export':
                filter_sql = get_events_filter_sql()
                con.execute(f"CREATE TABLE filtered_data AS SELECT * FROM raw_data WHERE {filter_sql}")
            elif ftype == 'mentions':
                filter_sql = get_mentions_filter_sql()
                con.execute(f"CREATE TABLE filtered_data AS SELECT * FROM raw_data WHERE {filter_sql}")
            elif ftype == 'gkg':
                filter_sql = get_gkg_filter_sql()
                con.execute(f"CREATE TABLE filtered_data AS SELECT * FROM raw_data WHERE {filter_sql}")
            else:
                con.execute("CREATE TABLE filtered_data AS SELECT * FROM raw_data")

            output_path = get_output_path(self.config, date, ftype)
            output_path_str = str(output_path)

            con.execute(f"""
                COPY filtered_data TO '{output_path_str}' (HEADER, DELIMITER '\t')
            """)

            row_count = con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
            con.close()

            self.logger.info(f"Processed {day_key}: {row_count} rows -> {output_path.name}")
            return (output_path, row_count)

        except Exception as e:
            self.logger.error(f"Failed to process {day_key}: {e}")
            return None

    async def process_day(self, day_key: str, zip_files: List[Path]) -> Optional[Tuple[Path, int]]:
        """Async wrapper for processing a day's files."""
        if self.shutdown_event.is_set():
            return None

        # Check if already processed AND uploaded (skip entirely)
        date, ftype = day_key.split('_')
        r2_key = get_r2_key(date, ftype)
        if day_key in self.state.state.processed_days and r2_key in self.state.state.uploaded_files:
            self.logger.debug(f"Already processed and uploaded: {day_key}")
            return None

        self.stats.current_process = day_key

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                self.executor,
                self._process_day_sync,
                day_key,
                zip_files
            )

            if result:
                output_path, row_count = result
                await self.state.mark_processed(day_key)
                self.stats.days_processed += 1
                self.stats.rows_processed += row_count

                # Delete source zip files after successful processing
                for zip_file in zip_files:
                    try:
                        zip_file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Failed to delete zip {zip_file}: {e}")

                # Clean up the day temp directory
                day_temp_dir = self.config.temp_dir / day_key
                try:
                    if day_temp_dir.exists():
                        shutil.rmtree(day_temp_dir, ignore_errors=True)
                except Exception:
                    pass

                return result
            else:
                self.stats.days_process_failed += 1
                return None

        except Exception as e:
            await self.state.mark_processing_failed(day_key, str(e))
            self.stats.days_process_failed += 1
            self.logger.error(f"Processing failed for {day_key}: {e}")
            return None


# =============================================================================
# R2 UPLOAD MANAGER
# =============================================================================

class R2UploadManager:
    """Manages uploads to Cloudflare R2 and deletes local files after."""

    def __init__(self, config: Config, state_manager: StateManager,
                 logger: logging.Logger, stats: PipelineStats):
        self.config = config
        self.state = state_manager
        self.logger = logger
        self.stats = stats
        self.shutdown_event = asyncio.Event()
        self.executor = ThreadPoolExecutor(max_workers=config.upload_workers)

        # Initialize S3 client for R2
        self.s3_client = boto3.client(
            's3',
            endpoint_url=config.r2_endpoint_url,
            aws_access_key_id=config.r2_access_key_id,
            aws_secret_access_key=config.r2_secret_access_key,
            region_name='auto',
        )

    def _upload_file_sync(self, local_path: Path, r2_key: str) -> bool:
        """Synchronous upload of a file to R2."""
        try:
            file_size = local_path.stat().st_size
            self.s3_client.upload_file(
                str(local_path),
                self.config.r2_bucket_name,
                r2_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            self.logger.info(f"Uploaded to R2: {r2_key} ({PipelineStats._fmt_bytes(file_size)})")
            return True
        except Exception as e:
            self.logger.error(f"Failed to upload {r2_key}: {e}")
            return False

    async def upload_and_delete(self, local_path: Path, r2_key: str) -> bool:
        """Upload file to R2, then delete local copy on success."""
        if self.shutdown_event.is_set():
            return False

        # Skip if already uploaded
        if r2_key in self.state.state.uploaded_files:
            self.logger.debug(f"Already uploaded: {r2_key}")
            # Still delete local file if it exists
            if local_path.exists():
                try:
                    local_path.unlink()
                    self.stats.files_deleted_after_upload += 1
                except Exception:
                    pass
            return True

        self.stats.current_upload = r2_key

        loop = asyncio.get_event_loop()

        for attempt in range(self.config.max_retries):
            if self.shutdown_event.is_set():
                return False

            try:
                file_size = local_path.stat().st_size if local_path.exists() else 0
                success = await loop.run_in_executor(
                    self.executor,
                    self._upload_file_sync,
                    local_path,
                    r2_key
                )

                if success:
                    await self.state.mark_uploaded(r2_key)
                    self.stats.files_uploaded += 1
                    self.stats.bytes_uploaded += file_size

                    # DELETE local file after confirmed upload
                    try:
                        local_path.unlink()
                        self.stats.files_deleted_after_upload += 1
                        self.logger.info(f"Deleted local file: {local_path.name}")
                    except Exception as e:
                        self.logger.warning(f"Failed to delete local file {local_path}: {e}")

                    # Also try to clean up empty parent directories
                    try:
                        parent = local_path.parent
                        if parent.exists() and not any(parent.iterdir()):
                            parent.rmdir()
                    except Exception:
                        pass

                    return True

            except Exception as e:
                self.logger.warning(f"Upload attempt {attempt + 1}/{self.config.max_retries} failed for {r2_key}: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))

        await self.state.mark_upload_failed(r2_key, "Max retries exceeded")
        self.stats.files_upload_failed += 1
        self.logger.error(f"Failed to upload after {self.config.max_retries} attempts: {r2_key}")
        return False


# =============================================================================
# MAIN PIPELINE (3-STAGE ASYNC)
# =============================================================================

class GDELTPipeline:
    """Main pipeline orchestrator with 3 async stages connected by queues."""

    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config)
        self.stats = PipelineStats()
        self.state_manager = StateManager(config.state_file, self.logger)
        self.download_manager = DownloadManager(config, self.state_manager, self.logger, self.stats)
        self.processor = DataProcessor(config, self.state_manager, self.logger, self.stats)
        self.uploader = R2UploadManager(config, self.state_manager, self.logger, self.stats)
        self.shutdown_requested = False

        # Queues connecting stages
        # download_queue: items are (day_key, List[Path]) - downloaded zip files ready for processing
        self.download_queue: asyncio.Queue = asyncio.Queue(maxsize=config.download_queue_size)
        # upload_queue: items are (local_path, r2_key) - processed CSVs ready for upload
        self.upload_queue: asyncio.Queue = asyncio.Queue(maxsize=config.upload_queue_size)

    def request_shutdown(self):
        """Request graceful shutdown."""
        self.logger.info("Shutdown requested. Finishing current tasks...")
        self.shutdown_requested = True
        self.download_manager.shutdown_event.set()
        self.processor.shutdown_event.set()
        self.uploader.shutdown_event.set()

    async def _wait_for_disk_space(self):
        """Wait until free disk space is above min_free_disk_gb."""
        while not self.shutdown_requested:
            free_gb = get_free_disk_gb(self.config.gdelt_base_dir)
            if free_gb >= self.config.min_free_disk_gb:
                return
            await self.stats.add_log(f"[red]Free disk {free_gb:.1f}GB < {self.config.min_free_disk_gb}GB minimum. Pausing downloads...[/red]")
            await asyncio.sleep(10)

    async def _stage_download(self, grouped: Dict[str, List[Tuple[str, str, str]]]):
        """Stage 1: Download files and put them on the download_queue for processing."""
        sorted_items = sorted(grouped.items())
        total = len(sorted_items)
        self.stats.days_to_process = total
        self.stats.files_to_upload = total
        self.stats.files_to_download = sum(len(v) for v in grouped.values())

        semaphore = asyncio.Semaphore(self.config.download_workers)

        for day_key, day_entries in sorted_items:
            if self.shutdown_requested:
                break

            # Check if already fully done (processed + uploaded)
            date, ftype = day_key.split('_')
            r2_key = get_r2_key(date, ftype)
            if day_key in self.state_manager.state.processed_days and r2_key in self.state_manager.state.uploaded_files:
                self.stats.files_download_skipped += len(day_entries)
                self.stats.days_processed += 1
                self.stats.files_uploaded += 1
                await self.stats.add_log(f"[dim]Skip (done): {day_key}[/dim]")
                continue

            # Wait if disk usage is too high
            await self._wait_for_disk_space()

            if self.shutdown_requested:
                break

            downloaded = await self.download_manager.download_day(day_key, day_entries, semaphore)

            if downloaded and not self.shutdown_requested:
                await self.download_queue.put((day_key, downloaded))
                self.stats.download_queue_depth = self.download_queue.qsize()
                await self.stats.add_log(f"[blue]Downloaded:[/blue] {day_key} ({len(downloaded)} files)")

        # Close the shared download session
        await self.download_manager.close()

        # Signal end of downloads
        await self.download_queue.put(None)
        self.stats.download_stage_done = True
        await self.stats.add_log("[green]Download stage complete[/green]")

    async def _process_worker(self, worker_id: int):
        """Single process worker that consumes from download_queue."""
        while True:
            if self.shutdown_requested:
                break

            item = await self.download_queue.get()
            if item is None:
                # Put sentinel back for other workers, then exit
                await self.download_queue.put(None)
                break

            day_key, zip_files = item
            self.stats.download_queue_depth = self.download_queue.qsize()

            result = await self.processor.process_day(day_key, zip_files)

            if result:
                output_path, row_count = result
                date, ftype = day_key.split('_')
                r2_key = get_r2_key(date, ftype)
                await self.upload_queue.put((output_path, r2_key))
                self.stats.upload_queue_depth = self.upload_queue.qsize()
                await self.stats.add_log(f"[yellow]Processed:[/yellow] {day_key} ({row_count:,} rows)")

    async def _stage_process(self):
        """Stage 2: Launch multiple process workers to consume from download_queue."""
        workers = [self._process_worker(i) for i in range(self.config.process_workers)]
        await asyncio.gather(*workers)

        # Signal end of processing (one sentinel per upload worker)
        for _ in range(self.config.upload_workers):
            await self.upload_queue.put(None)
        self.stats.process_stage_done = True
        await self.stats.add_log("[green]Process stage complete[/green]")

    async def _upload_worker(self, worker_id: int):
        """Single upload worker that consumes from upload_queue."""
        while True:
            if self.shutdown_requested:
                break

            item = await self.upload_queue.get()
            if item is None:
                # Put sentinel back for other workers, then exit
                await self.upload_queue.put(None)
                break

            local_path, r2_key = item
            self.stats.upload_queue_depth = self.upload_queue.qsize()

            success = await self.uploader.upload_and_delete(local_path, r2_key)
            if success:
                await self.stats.add_log(f"[green]Uploaded + deleted:[/green] {r2_key}")
            else:
                await self.stats.add_log(f"[red]Upload failed:[/red] {r2_key}")

    async def _stage_upload(self):
        """Stage 3: Launch multiple upload workers to consume from upload_queue."""
        workers = [self._upload_worker(i) for i in range(self.config.upload_workers)]
        await asyncio.gather(*workers)

        self.stats.upload_stage_done = True
        await self.stats.add_log("[green]Upload stage complete[/green]")

    async def run(self, file_types: Optional[List[str]] = None):
        """Run the full 3-stage pipeline with live dashboard."""
        self.logger.info("Starting GDELT pipeline...")
        await self.stats.add_log("Fetching masterlist...")

        # Fetch and filter masterlist
        entries = await fetch_masterlist(self.config, self.logger)
        types_to_fetch = file_types or self.config.file_types
        filtered = filter_masterlist_by_date(
            entries,
            self.config.start_date,
            self.config.end_date,
            types_to_fetch
        )

        self.logger.info(f"Filtered to {len(filtered)} files for types: {types_to_fetch}")
        await self.stats.add_log(f"Found {len(filtered)} files to process")

        grouped = group_files_by_day(filtered)
        total_days = len(grouped)
        self.stats.days_to_process = total_days
        self.stats.files_to_upload = total_days
        self.stats.files_to_download = sum(len(v) for v in grouped.values())

        await self.stats.add_log(f"{total_days} day-type combinations, {self.stats.files_to_download} total files")

        console = Console()

        # Run all 3 stages concurrently with live dashboard
        with Live(build_dashboard(self.stats, self.config), console=console, refresh_per_second=2) as live:

            async def refresh_dashboard():
                while not self.stats.upload_stage_done and not self.shutdown_requested:
                    live.update(build_dashboard(self.stats, self.config))
                    await asyncio.sleep(0.5)
                # Final refresh
                live.update(build_dashboard(self.stats, self.config))

            # Launch all stages + dashboard refresh concurrently
            await asyncio.gather(
                self._stage_download(grouped),
                self._stage_process(),
                self._stage_upload(),
                refresh_dashboard(),
                return_exceptions=True,
            )

        # Final state save
        await self.state_manager.save_state()

        # Print summary
        console.print()
        summary = Table(title="Pipeline Summary", box=box.ROUNDED)
        summary.add_column("Metric", style="bold")
        summary.add_column("Value", justify="right")
        summary.add_row("Elapsed", self.stats.elapsed())
        summary.add_row("Files Downloaded", str(self.stats.files_downloaded))
        summary.add_row("Files Skipped", str(self.stats.files_download_skipped))
        summary.add_row("Days Processed", str(self.stats.days_processed))
        summary.add_row("Total Rows", f"{self.stats.rows_processed:,}")
        summary.add_row("Files Uploaded to R2", str(self.stats.files_uploaded))
        summary.add_row("Local Files Deleted", str(self.stats.files_deleted_after_upload))
        summary.add_row("Data Downloaded", PipelineStats._fmt_bytes(self.stats.bytes_downloaded))
        summary.add_row("Data Uploaded", PipelineStats._fmt_bytes(self.stats.bytes_uploaded))
        summary.add_row("Download Failures", str(self.stats.files_download_failed))
        summary.add_row("Process Failures", str(self.stats.days_process_failed))
        summary.add_row("Upload Failures", str(self.stats.files_upload_failed))
        console.print(summary)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="GDELT Pipeline - Download, process, and upload to R2"
    )

    parser.add_argument(
        '--download-workers',
        type=int,
        default=5,
        help='Number of concurrent download workers (default: 5)'
    )

    parser.add_argument(
        '--upload-workers',
        type=int,
        default=3,
        help='Number of concurrent upload workers (default: 3)'
    )

    parser.add_argument(
        '--process-workers',
        type=int,
        default=2,
        help='Number of concurrent DuckDB process workers (default: 2)'
    )

    parser.add_argument(
        '--gdelt-version',
        type=int,
        choices=[1, 2],
        default=2,
        help='GDELT version: 1 (2004-2015, daily) or 2 (2015+, 15-min updates). Default: 2'
    )

    parser.add_argument(
        '--types',
        nargs='+',
        choices=['export', 'mentions', 'gkg'],
        default=['export', 'mentions', 'gkg'],
        help='File types to download and process (default: all)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYYMMDD format'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYYMMDD format'
    )

    parser.add_argument(
        '--memory-limit',
        type=str,
        default='4GB',
        help='DuckDB memory limit per worker (default: 4GB)'
    )

    parser.add_argument(
        '--base-dir',
        type=str,
        default='/workspace/gdelt',
        help='Base directory for GDELT data (default: /workspace/gdelt)'
    )

    parser.add_argument(
        '--r2-bucket',
        type=str,
        default='europe',
        help='R2 bucket name (default: europe)'
    )

    parser.add_argument(
        '--min-free-disk-gb',
        type=float,
        default=5.0,
        help='Minimum free disk space in GB before pausing downloads (default: 5)'
    )

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_args()

    # For GDELT 1.0, only 'export' type is available
    file_types = args.types
    if args.gdelt_version == 1:
        file_types = ['export']
        if args.types != ['export', 'mentions', 'gkg'] and 'export' not in args.types:
            print("Warning: GDELT 1.0 only has 'export' (events) data. Using 'export' only.")

    base_dir = Path(args.base_dir)

    # R2 credentials
    r2_access_key = 'fdfa18bf64b18c61bbee64fda98ca20b'
    r2_secret_key = '394c88a7aaf0027feabe74ae20da9b2f743ab861336518a09972bc39534596d8'
    r2_endpoint = 'https://2a139e9393f803634546ad9d541d37b9.r2.cloudflarestorage.com'
    r2_bucket = args.r2_bucket

    config = Config(
        gdelt_base_dir=base_dir,
        temp_dir=base_dir / "tmp",
        state_file=base_dir / "pipeline_state.json",
        log_file=base_dir / "pipeline.log",
        error_log_file=base_dir / "pipeline_errors.log",
        download_workers=args.download_workers,
        process_workers=args.process_workers,
        upload_workers=args.upload_workers,
        min_free_disk_gb=args.min_free_disk_gb,
        duckdb_memory_limit=args.memory_limit,
        file_types=file_types,
        start_date=args.start_date,
        end_date=args.end_date,
        gdelt_version=args.gdelt_version,
        r2_bucket_name=r2_bucket,
        r2_endpoint_url=r2_endpoint,
        r2_access_key_id=r2_access_key,
        r2_secret_access_key=r2_secret_key,
    )

    # Ensure directories exist
    config.gdelt_base_dir.mkdir(parents=True, exist_ok=True)

    # Clean temp dir from previous runs to free disk space
    if config.temp_dir.exists():
        shutil.rmtree(config.temp_dir, ignore_errors=True)
    config.temp_dir.mkdir(parents=True, exist_ok=True)

    # Create pipeline
    pipeline = GDELTPipeline(config)

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        pipeline.request_shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run pipeline
    try:
        await pipeline.run(file_types)
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        await pipeline.state_manager.save_state()
        pipeline.logger.info("Pipeline finished. State saved.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        import traceback
        print(f"\n\nFATAL ERROR: {e}")
        traceback.print_exc()
        with open("pipeline_crash.log", "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"FATAL CRASH: {e}\n")
            traceback.print_exc(file=f)
