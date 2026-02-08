"""
GDELT Async Pipeline
=====================
Downloads, decompresses, merges, and filters GDELT data (Events, Mentions, GKG).

Supports both GDELT 1.0 (2004-2015, daily) and GDELT 2.0 (2015+, 15-minute updates).

Features:
- Async download with configurable workers
- DuckDB-based decompression and merging
- Graceful shutdown (Ctrl+C)
- Resume capability via state file
- Error logging
- Filtering for Events, Mentions, and GKG
- Resilient to 404 errors (continues processing)

Usage:
    python gdelt_pipeline.py --download-workers 5  # GDELT 2.0 (default)
    python gdelt_pipeline.py --gdelt-version 1 --start-date 20040101 --end-date 20150218
    python gdelt_pipeline.py --gdelt-version 2 --start-date 20150219

Author: Auto-generated
"""

import asyncio
import aiohttp
import aiofiles
import duckdb
import json
import logging
import os
import re
import signal
import sys
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import argparse
import hashlib

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    # Directories
    gdelt_base_dir: Path = Path(r"E:\GDELT")
    temp_dir: Path = Path(r"C:\Users\daoud\Downloads\temp")
    
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
    process_workers: int = 10
    duckdb_memory_limit: str = "10GB"
    
    # File patterns
    file_types: List[str] = field(default_factory=lambda: ["export", "mentions", "gkg"])
    
    # State and logging
    state_file: Path = field(default_factory=lambda: Path(r"E:\GDELT\pipeline_state.json"))
    log_file: Path = field(default_factory=lambda: Path(r"E:\GDELT\pipeline.log"))
    error_log_file: Path = field(default_factory=lambda: Path(r"E:\GDELT\pipeline_errors.log"))
    
    # Download settings
    download_timeout: int = 300  # 5 minutes
    max_retries: int = 3
    retry_delay: int = 5
    
    # Date range (None means all available)
    start_date: Optional[str] = None  # Format: YYYYMMDD
    end_date: Optional[str] = None    # Format: YYYYMMDD


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(config: Config) -> logging.Logger:
    """Setup logging with both file and console handlers."""
    logger = logging.getLogger("gdelt_pipeline")
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S')
    console_handler.setFormatter(console_format)
    
    # File handler
    file_handler = logging.FileHandler(config.log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    file_handler.setFormatter(file_format)
    
    # Error file handler
    error_handler = logging.FileHandler(config.error_log_file, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

@dataclass
class PipelineState:
    """Tracks pipeline progress for resume capability."""
    downloaded_files: Set[str] = field(default_factory=set)
    processed_days: Set[str] = field(default_factory=set)  # Format: YYYYMMDD_type
    failed_downloads: Dict[str, int] = field(default_factory=dict)  # url -> retry count
    failed_processing: Dict[str, str] = field(default_factory=dict)  # day_type -> error
    last_masterlist_update: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            "downloaded_files": list(self.downloaded_files),
            "processed_days": list(self.processed_days),
            "failed_downloads": self.failed_downloads,
            "failed_processing": self.failed_processing,
            "last_masterlist_update": self.last_masterlist_update
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState":
        return cls(
            downloaded_files=set(data.get("downloaded_files", [])),
            processed_days=set(data.get("processed_days", [])),
            failed_downloads=data.get("failed_downloads", {}),
            failed_processing=data.get("failed_processing", {}),
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
                                   f"{len(data.get('processed_days', []))} processed")
                    return PipelineState.from_dict(data)
                except Exception as e:
                    self.logger.warning(f"Failed to load {path.name}: {e}")
        self.logger.info("No valid state found. Starting fresh.")
        return PipelineState()
    
    async def save_state(self):
        """Save current state to file with backup for crash safety."""
        async with self._lock:
            try:
                # Write backup first, then overwrite main file
                backup_file = self.state_file.with_suffix('.bak')
                if self.state_file.exists():
                    try:
                        import shutil
                        shutil.copy2(self.state_file, backup_file)
                    except Exception:
                        pass
                state_json = json.dumps(self.state.to_dict(), indent=2)
                async with aiofiles.open(self.state_file, 'w') as f:
                    await f.write(state_json)
            except Exception as e:
                self.logger.error(f"Failed to save state: {e}")
    
    async def mark_downloaded(self, filename: str):
        """Mark a file as downloaded (state saved at batch boundaries, not per-file)."""
        self.state.downloaded_files.add(filename)
    
    async def mark_processed(self, day_type: str):
        """Mark a day+type as processed."""
        self.state.processed_days.add(day_type)
        await self.save_state()
    
    async def mark_download_failed(self, url: str):
        """Track failed download (state saved at batch boundaries, not per-file)."""
        self.state.failed_downloads[url] = self.state.failed_downloads.get(url, 0) + 1
    
    async def mark_processing_failed(self, day_type: str, error: str):
        """Track failed processing (state saved at batch boundaries, not per-file)."""
        self.state.failed_processing[day_type] = error


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
            date, time = match.groups()
            if 'export' in filename.lower():
                return (date, time, 'export')
            elif 'mentions' in filename.lower():
                return (date, time, 'mentions')
            elif 'gkg' in filename.lower():
                return (date, time, 'gkg')
    
    # GDELT 1.0 patterns (daily updates) - only has export (events)
    v1_patterns = [
        (r'^(\d{8})\.export\.CSV\.zip$', 'daily'),   # Daily file: 20140101.export.CSV.zip
        (r'^(\d{8})\.zip$', 'daily'),                 # Early daily format: 20130401.zip
        (r'^(\d{6})\.zip$', 'monthly'),               # Monthly file: 200601.zip (2006-March 2013)
        (r'^(\d{4})\.zip$', 'yearly'),                # Yearly file: 2004.zip (1979-2005)
    ]
    
    for pattern, period_type in v1_patterns:
        match = re.match(pattern, filename, re.IGNORECASE)
        if match:
            date_part = match.group(1)
            if period_type == 'daily':
                return (date_part, '000000', 'export')
            elif period_type == 'monthly':
                # Convert YYYYMM to YYYYMM01 for consistency
                return (date_part + '01', '000000', 'export')
            elif period_type == 'yearly':
                # Convert YYYY to YYYY0101 for consistency
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


# =============================================================================
# MASTERLIST MANAGEMENT
# =============================================================================

async def fetch_masterlist_v2(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """
    Fetch and parse the GDELT 2.0 masterlist.
    Returns list of (size, hash, url) tuples.
    """
    logger.info("Fetching GDELT 2.0 masterlist...")
    
    async with aiohttp.ClientSession() as session:
        try:
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
                
        except Exception as e:
            logger.error(f"Failed to fetch masterlist: {e}")
            raise


async def fetch_masterlist_v1(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """
    Fetch and parse the GDELT 1.0 file list.
    GDELT 1.0 uses a different format - just filename and size.
    Returns list of (size, hash, url) tuples (hash is empty for v1).
    """
    logger.info("Fetching GDELT 1.0 file list...")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(config.masterlist_v1_url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch GDELT 1.0 file list: HTTP {response.status}")
                
                content = await response.text()
                lines = content.strip().split('\n')
                
                entries = []
                for line in lines:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        # Format: size filename (e.g., "12345678 20140101.export.CSV.zip")
                        size, filename = parts[0], parts[1]
                        url = config.base_download_v1_url + filename
                        entries.append((size, '', url))
                
                logger.info(f"Fetched GDELT 1.0 file list with {len(entries)} entries")
                return entries
                
        except Exception as e:
            logger.error(f"Failed to fetch GDELT 1.0 file list: {e}")
            raise


async def fetch_masterlist(config: Config, logger: logging.Logger) -> List[Tuple[str, str, str]]:
    """
    Fetch and parse the GDELT masterlist based on configured version.
    Returns list of (size, hash, url) tuples.
    """
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
            
        date, time, ftype = parsed
        
        # Filter by file type
        if ftype not in file_types:
            continue
        
        # Filter by date range
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
            date, time, ftype = parsed
            key = f"{date}_{ftype}"
            grouped[key].append((size, hash_val, url))
    
    return dict(grouped)


# =============================================================================
# DOWNLOAD MANAGER
# =============================================================================

class DownloadManager:
    """Manages async file downloads."""
    
    def __init__(self, config: Config, state_manager: StateManager, logger: logging.Logger):
        self.config = config
        self.state = state_manager
        self.logger = logger
        self.shutdown_event = asyncio.Event()
        self.active_downloads = 0
        self._lock = asyncio.Lock()
    
    async def download_file(self, url: str, dest_path: Path, expected_hash: Optional[str] = None) -> bool:
        """Download a single file with retry logic. Returns False for missing files (404) without blocking."""
        filename = url.split('/')[-1]
        
        # Skip if already downloaded
        if filename in self.state.state.downloaded_files:
            return True
        
        # Check if file exists locally
        if dest_path.exists():
            self.logger.debug(f"File exists: {filename}")
            await self.state.mark_downloaded(filename)
            return True
        
        for attempt in range(self.config.max_retries):
            if self.shutdown_event.is_set():
                return False
                
            try:
                async with self._lock:
                    self.active_downloads += 1
                
                timeout = aiohttp.ClientTimeout(total=self.config.download_timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        # Handle 404 - file doesn't exist on server, skip without retry
                        if response.status == 404:
                            self.logger.debug(f"File not found (404), skipping: {filename}")
                            await self.state.mark_download_failed(url)
                            return False
                        
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}")
                        
                        # Download to temp file first
                        temp_path = dest_path.with_suffix('.tmp')
                        async with aiofiles.open(temp_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                if self.shutdown_event.is_set():
                                    temp_path.unlink(missing_ok=True)
                                    return False
                                await f.write(chunk)
                        
                        # Move to final location
                        temp_path.rename(dest_path)
                        
                await self.state.mark_downloaded(filename)
                self.logger.debug(f"Downloaded: {filename}")
                return True
                
            except Exception as e:
                self.logger.warning(f"Download attempt {attempt + 1}/{self.config.max_retries} failed for {filename}: {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
            finally:
                async with self._lock:
                    self.active_downloads -= 1
        
        await self.state.mark_download_failed(url)
        self.logger.warning(f"Failed to download after {self.config.max_retries} attempts (skipping): {filename}")
        return False
    
    async def download_day(self, day_key: str, entries: List[Tuple[str, str, str]]) -> List[Path]:
        """Download all files for a specific day."""
        date, ftype = day_key.split('_')
        year = get_year_from_date(date)
        
        # Create temp directory for this day
        day_temp_dir = self.config.temp_dir / f"{day_key}"
        day_temp_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        
        for size, hash_val, url in entries:
            if self.shutdown_event.is_set():
                break
                
            filename = url.split('/')[-1]
            dest_path = day_temp_dir / filename
            
            if await self.download_file(url, dest_path, hash_val):
                downloaded_files.append(dest_path)
        
        return downloaded_files


# =============================================================================
# DATA FILTERING
# =============================================================================

# Event codes to remove (generic noise)
REMOVE_EVENT_CODES = ['010', '011', '016', '018']

# GKG source collection identifiers to remove
REMOVE_SOURCE_COLLECTIONS = [3, 5, 6]

# GKG theme patterns to remove (categories 6, 7, 9)
# These are used for post-processing theme fields
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
            
        # Check if theme matches any removal pattern using compiled regex
        if not REMOVE_THEME_REGEX.search(theme):
            filtered_themes.append(theme)
    
    return ';'.join(filtered_themes)


def build_gkg_theme_filter_sql() -> str:
    """Build SQL CASE expression to filter themes in DuckDB."""
    # DuckDB doesn't support Python UDFs easily, so we use regex_replace
    # to remove unwanted themes from the semicolon-delimited string
    patterns = '|'.join([f'[^;]*{p}[^;]*;?' for p in REMOVE_THEME_PATTERNS])
    return f"regexp_replace(V1Themes, '{patterns}', '', 'gi')"


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
    
    def __init__(self, config: Config, state_manager: StateManager, logger: logging.Logger):
        self.config = config
        self.state = state_manager
        self.logger = logger
        self.shutdown_event = asyncio.Event()
        self.executor = ThreadPoolExecutor(max_workers=config.process_workers)
    
    def _get_column_names(self, file_type: str) -> List[str]:
        """Get column names for file type."""
        if file_type == 'export':
            return self.EVENTS_COLUMNS
        elif file_type == 'mentions':
            return self.MENTIONS_COLUMNS
        elif file_type == 'gkg':
            return self.GKG_COLUMNS
        return []
    
    def _process_day_sync(self, day_key: str, zip_files: List[Path]) -> Optional[Path]:
        """Synchronous processing of a day's files using DuckDB."""
        date, ftype = day_key.split('_')
        
        if not zip_files:
            self.logger.warning(f"No files to process for {day_key}")
            return None
        
        try:
            # Create DuckDB connection with memory limit
            con = duckdb.connect(':memory:')
            con.execute(f"SET memory_limit='{self.config.duckdb_memory_limit}'")
            con.execute("SET threads=4")
            
            # Get column names
            columns = self._get_column_names(ftype)
            if not columns:
                self.logger.error(f"Unknown file type: {ftype}")
                return None
            
            # Create the target table with proper column names
            col_defs_sql = ", ".join([f'"{col}" VARCHAR' for col in columns])
            con.execute(f"CREATE TABLE raw_data ({col_defs_sql})")
            
            # Process each zip file individually and insert into table
            files_processed = 0
            for zip_path in zip_files:
                try:
                    if not zip_path.exists():
                        continue
                        
                    # Extract CSV from zip
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        csv_names = [n for n in zf.namelist() if n.endswith('.csv') or n.endswith('.CSV')]
                        if not csv_names:
                            continue
                        
                        csv_name = csv_names[0]
                        csv_content = zf.read(csv_name).decode('utf-8', errors='replace')
                        
                        # Write to temp file for DuckDB
                        temp_csv = self.config.temp_dir / f"temp_{day_key}_{zip_path.stem}.csv"
                        with open(temp_csv, 'w', encoding='utf-8') as f:
                            f.write(csv_content)
                        
                        temp_csv_path = str(temp_csv).replace('\\', '/')
                        
                        # Read file and get its columns
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
                            
                            # Get actual column names from this file
                            file_cols = con.execute("SELECT * FROM file_data LIMIT 0").description
                            file_col_names = [col[0] for col in file_cols]
                            
                            # Build insert with column mapping
                            num_cols = min(len(columns), len(file_col_names))
                            select_cols = ", ".join([f'"{file_col_names[i]}"' for i in range(num_cols)])
                            
                            # Pad with NULLs if file has fewer columns
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
                            # Clean up temp file
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
            
            # Apply filters based on file type
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
            
            # Get output path
            output_path = get_output_path(self.config, date, ftype)
            output_path_str = str(output_path).replace('\\', '/')
            
            # Export to CSV
            con.execute(f"""
                COPY filtered_data TO '{output_path_str}' (HEADER, DELIMITER '\t')
            """)
            
            # Get row count
            row_count = con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
            
            con.close()
            
            self.logger.info(f"Processed {day_key}: {row_count} rows -> {output_path.name}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Failed to process {day_key}: {e}")
            return None
    
    async def process_day(self, day_key: str, zip_files: List[Path]) -> Optional[Path]:
        """Async wrapper for processing a day's files."""
        if self.shutdown_event.is_set():
            return None
        
        # Check if already processed
        if day_key in self.state.state.processed_days:
            self.logger.debug(f"Already processed: {day_key}")
            return None
        
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                self.executor, 
                self._process_day_sync, 
                day_key, 
                zip_files
            )
            
            if result:
                await self.state.mark_processed(day_key)
                
                # Delete source zip files after successful processing
                for zip_file in zip_files:
                    try:
                        zip_file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {zip_file}: {e}")
            
            return result
            
        except Exception as e:
            await self.state.mark_processing_failed(day_key, str(e))
            self.logger.error(f"Processing failed for {day_key}: {e}")
            return None


# =============================================================================
# MAIN PIPELINE
# =============================================================================

class GDELTPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config)
        self.state_manager = StateManager(config.state_file, self.logger)
        self.download_manager = DownloadManager(config, self.state_manager, self.logger)
        self.processor = DataProcessor(config, self.state_manager, self.logger)
        self.shutdown_requested = False
    
    def request_shutdown(self):
        """Request graceful shutdown."""
        self.logger.info("Shutdown requested. Finishing current tasks...")
        self.shutdown_requested = True
        self.download_manager.shutdown_event.set()
        self.processor.shutdown_event.set()
    
    async def run_download_mode(self, file_types: Optional[List[str]] = None):
        """Run in download mode - fetch missing files from GDELT."""
        self.logger.info("Starting download mode...")
        
        # Fetch masterlist
        entries = await fetch_masterlist(self.config, self.logger)
        
        # Filter by date and type
        types_to_fetch = file_types or self.config.file_types
        filtered = filter_masterlist_by_date(
            entries, 
            self.config.start_date, 
            self.config.end_date,
            types_to_fetch
        )
        
        self.logger.info(f"Filtered to {len(filtered)} files for types: {types_to_fetch}")
        
        # Group by day
        grouped = group_files_by_day(filtered)
        
        # Process each day
        semaphore = asyncio.Semaphore(self.config.download_workers)
        
        async def download_and_process(day_key: str, day_entries: List[Tuple[str, str, str]]):
            if self.shutdown_requested:
                return
            
            try:
                # Check if output file already exists
                date, ftype = day_key.split('_')
                output_path = get_output_path(self.config, date, ftype)
                if output_path.exists():
                    self.logger.debug(f"Output already exists, skipping: {output_path.name}")
                    return
                
                async with semaphore:
                    # Download files
                    downloaded = await self.download_manager.download_day(day_key, day_entries)
                    
                    if downloaded and not self.shutdown_requested:
                        # Process immediately
                        await self.processor.process_day(day_key, downloaded)
            except Exception as e:
                self.logger.error(f"Error in download_and_process for {day_key}: {e}", exc_info=True)
        
        sorted_items = sorted(grouped.items())
        total = len(sorted_items)
        self.logger.info(f"Processing {total} day-type combinations...")
        
        # Process in batches to avoid memory issues with too many coroutines
        batch_size = self.config.download_workers * 10
        processed = 0
        
        for i in range(0, total, batch_size):
            if self.shutdown_requested:
                break
            
            batch = sorted_items[i:i + batch_size]
            tasks = [download_and_process(day_key, day_entries) for day_key, day_entries in batch]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed with exception: {result}", exc_info=result)
            
            processed += len(batch)
            self.logger.info(f"Progress: {processed}/{total} day-type combinations")
            
            # Save state between batches
            await self.state_manager.save_state()
        
        self.logger.info("Download mode completed")
    


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="GDELT Download Pipeline - Download and process GDELT data from masterlist"
    )
    
    parser.add_argument(
        '--download-workers',
        type=int,
        default=5,
        help='Number of concurrent download workers (default: 5)'
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
        help='File types to download and process (default: all). Note: GDELT 1.0 only has export (events).'
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
        default='10GB',
        help='DuckDB memory limit per worker (default: 10GB)'
    )
    
    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_args()
    
    # Create config
    # For GDELT 1.0, only 'export' type is available
    file_types = args.types
    if args.gdelt_version == 1:
        file_types = ['export']
        if args.types != ['export', 'mentions', 'gkg'] and 'export' not in args.types:
            print("Warning: GDELT 1.0 only has 'export' (events) data. Using 'export' only.")
    
    config = Config(
        download_workers=args.download_workers,
        process_workers=args.download_workers,  # Use same as download workers
        duckdb_memory_limit=args.memory_limit,
        file_types=file_types,
        start_date=args.start_date,
        end_date=args.end_date,
        gdelt_version=args.gdelt_version
    )
    
    # Ensure directories exist
    config.temp_dir.mkdir(parents=True, exist_ok=True)
    config.gdelt_base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create pipeline
    pipeline = GDELTPipeline(config)
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        pipeline.request_shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run pipeline - download and process
    try:
        await pipeline.run_download_mode(args.types)
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        # Save final state
        await pipeline.state_manager.save_state()
        pipeline.logger.info("Pipeline finished. State saved.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        import traceback
        print(f"\n\nFATAL ERROR: {e}")
        traceback.print_exc()
        # Also write to log file directly
        with open("pipeline_crash.log", "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"FATAL CRASH: {e}\n")
            traceback.print_exc(file=f)
