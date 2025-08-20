"""
Environment to JSON conversion utilities.
"""

import os
import json
import subprocess
import logging

logger = logging.getLogger(__name__)


def get_cmsenv(cmssw_path=None):
    """
    Get CMSSW environment variables as a dictionary.
    
    Args:
        cmssw_path: Path to CMSSW installation
        
    Returns:
        Dictionary of environment variables
    """
    env_dict = {}
    
    if cmssw_path is None:
        # Use current environment
        env_dict = dict(os.environ)
        logger.debug("Using current environment")
    else:
        try:
            # Try to source CMSSW environment
            cmd = f"cd {cmssw_path} && eval `scramv1 runtime -sh` && env"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        env_dict[key] = value
                logger.debug(f"Loaded CMSSW environment from {cmssw_path}")
            else:
                logger.warning(f"Failed to load CMSSW environment from {cmssw_path}")
                # Fallback to current environment
                env_dict = dict(os.environ)
                
        except Exception as e:
            logger.warning(f"Error loading CMSSW environment: {e}")
            # Fallback to current environment
            env_dict = dict(os.environ)
    
    # Ensure some essential variables are set
    essential_vars = {
        'HOME': os.path.expanduser('~'),
        'USER': os.getenv('USER', 'unknown'),
        'SHELL': os.getenv('SHELL', '/bin/bash'),
        'PATH': os.getenv('PATH', '/usr/bin:/bin'),
    }
    
    for var, default in essential_vars.items():
        if var not in env_dict:
            env_dict[var] = default
            logger.debug(f"Set default {var}={default}")
    
    return env_dict