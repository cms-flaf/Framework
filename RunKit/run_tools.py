"""
Runtime tools and utilities.
"""

import subprocess
import re
import logging

logger = logging.getLogger(__name__)


def natural_sort(l):
    """
    Sort a list in natural order (e.g., ['item1', 'item2', 'item10'] instead of ['item1', 'item10', 'item2']).
    
    Args:
        l: List to sort
        
    Returns:
        Sorted list
    """
    def convert(text):
        return int(text) if text.isdigit() else text.lower()
    
    def alphanum_key(key):
        return [convert(c) for c in re.split('([0-9]+)', str(key))]
    
    return sorted(l, key=alphanum_key)


def ps_call(cmd, *args, **kwargs):
    """
    Call a subprocess with error handling.
    
    Args:
        cmd: Command to execute
        *args: Additional arguments for subprocess
        **kwargs: Additional keyword arguments for subprocess
        
    Returns:
        Result of subprocess call
    """
    try:
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd, *args, **kwargs)
        return result
    except Exception as e:
        logger.error(f"Command failed: {cmd}, error: {e}")
        raise