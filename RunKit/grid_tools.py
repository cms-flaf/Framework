"""
Grid tools with LFN->PFN caching to handle Rucio downtime gracefully.

This module implements a caching mechanism for LFN to PFN mappings
to smooth over temporary Rucio service glitches.
"""

import os
import time
import json
import tempfile
from typing import Dict, Optional, Tuple
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Cache configuration
CACHE_FILE = os.path.join(tempfile.gettempdir(), "flaf_lfn_pfn_cache.json")
CACHE_EXPIRY_HOURS = 24  # Cache entries expire after 24 hours
MAX_RETRIES = 3
RETRY_DELAYS = [0.25, 0.5, 1.0]  # Exponential backoff delays in seconds


class LfnPfnCache:
    """Local cache for LFN->PFN mappings to handle Rucio downtime."""
    
    def __init__(self, cache_file: str = CACHE_FILE):
        self.cache_file = cache_file
        self._cache = None
        self._load_cache()
    
    def _load_cache(self):
        """Load cache from disk."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    self._cache = json.load(f)
            else:
                self._cache = {}
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load cache from {self.cache_file}: {e}")
            self._cache = {}
    
    def _save_cache(self):
        """Save cache to disk."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self._cache, f, indent=2)
        except IOError as e:
            logger.warning(f"Failed to save cache to {self.cache_file}: {e}")
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if a cache entry is expired."""
        return time.time() - timestamp > CACHE_EXPIRY_HOURS * 3600
    
    def _clean_expired(self):
        """Remove expired entries from cache."""
        current_time = time.time()
        expired_keys = [
            key for key, entry in self._cache.items()
            if self._is_expired(entry.get('timestamp', 0))
        ]
        for key in expired_keys:
            del self._cache[key]
        
        if expired_keys:
            logger.info(f"Cleaned {len(expired_keys)} expired cache entries")
            self._save_cache()
    
    def get(self, server: str, lfn: str) -> Optional[str]:
        """Get PFN from cache if available and not expired."""
        self._clean_expired()
        key = f"{server}:{lfn}"
        entry = self._cache.get(key)
        if entry and not self._is_expired(entry.get('timestamp', 0)):
            logger.debug(f"Cache hit for {key}")
            return entry['pfn']
        return None
    
    def set(self, server: str, lfn: str, pfn: str):
        """Store LFN->PFN mapping in cache."""
        key = f"{server}:{lfn}"
        self._cache[key] = {
            'pfn': pfn,
            'timestamp': time.time()
        }
        logger.debug(f"Cached mapping: {key} -> {pfn}")
        self._save_cache()


# Global cache instance
_cache = LfnPfnCache()


def lfn_to_pfn(server: str, lfn: str) -> str:
    """
    Convert LFN to PFN with caching and retry logic.
    
    Args:
        server: Rucio server name
        lfn: Logical file name
        
    Returns:
        Physical file name (PFN)
        
    Raises:
        RuntimeError: If all retries failed and no cached value available
    """
    # First check cache
    cached_pfn = _cache.get(server, lfn)
    
    # Try to get fresh value from Rucio with retries
    for attempt in range(MAX_RETRIES):
        try:
            # Import here to avoid import errors if rucio is not available
            from rucio.client.rseclient import RSEClient
            
            client = RSEClient()
            result = client.lfns2pfns(server, [lfn])
            
            if result and lfn in result:
                pfn = result[lfn]
                # Cache the successful result
                _cache.set(server, lfn, pfn)
                logger.debug(f"Successfully converted {lfn} to {pfn} via Rucio")
                return pfn
            else:
                raise RuntimeError(f"No PFN returned for LFN {lfn}")
                
        except Exception as e:
            logger.warning(f"Rucio call attempt {attempt + 1} failed: {e}")
            
            # If this is not the last attempt, wait before retrying
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                logger.info(f"Waiting {delay}s before retry...")
                time.sleep(delay)
    
    # All Rucio attempts failed, try to use cached value
    if cached_pfn:
        logger.warning(f"Rucio failed, using cached PFN for {lfn}: {cached_pfn}")
        return cached_pfn
    
    # No cached value available, raise error
    raise RuntimeError(
        f"Failed to convert LFN {lfn} to PFN: Rucio unavailable and no cached value"
    )


def path_to_pfn(path: str) -> str:
    """
    Convert a path to PFN, handling both LFNs and regular paths.
    
    Args:
        path: Input path (could be LFN or regular path)
        
    Returns:
        Physical file name/path
    """
    # Simple heuristic: if path starts with /store, treat as LFN
    if path.startswith('/store'):
        # For LFNs, we need to determine the appropriate server
        # This is a simplified approach - in practice you might need
        # more sophisticated server selection logic
        server = 'T2_CH_CERN'  # Default server, should be configurable
        return lfn_to_pfn(server, path)
    else:
        # Regular path, return as-is
        return path


def gfal_ls(path: str) -> list:
    """
    List files using gfal-ls command.
    
    This is a placeholder implementation. The actual implementation
    would use gfal commands to list files.
    
    Args:
        path: Path to list
        
    Returns:
        List of files/directories
    """
    import subprocess
    
    try:
        result = subprocess.run(['gfal-ls', path], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip().split('\n') if result.stdout.strip() else []
        else:
            logger.warning(f"gfal-ls failed for {path}: {result.stderr}")
            return []
    except FileNotFoundError:
        logger.warning("gfal-ls command not found, returning empty list")
        return []
    except Exception as e:
        logger.warning(f"gfal-ls error for {path}: {e}")
        return []