"""
WLCG file system and target classes for law with resilient Rucio handling.

This module provides WLCG (Worldwide LHC Computing Grid) file system support
with robust error handling for Rucio service outages.
"""

import os
import logging
from typing import Union, List

try:
    import law
    LAW_AVAILABLE = True
except ImportError:
    LAW_AVAILABLE = False
    # Create a minimal mock for law.FileSystemTarget
    class MockFileSystemTarget:
        def __init__(self, path):
            self.path = path

from .grid_tools import path_to_pfn

logger = logging.getLogger(__name__)


class WLCGFileSystem:
    """
    WLCG file system with resilient initialization that handles Rucio downtime.
    """
    
    def __init__(self, base_paths: Union[str, List[str]]):
        """
        Initialize WLCG file system with base paths.
        
        Args:
            base_paths: Single path or list of base paths
        """
        if isinstance(base_paths, str):
            base_paths = [base_paths]
        
        self.base_paths = base_paths
        self.base_pfns = []
        
        # Convert each base path to PFN with error handling
        for base_path in base_paths:
            try:
                pfn = path_to_pfn(base_path)
                self.base_pfns.append(pfn)
                logger.debug(f"Successfully converted base path {base_path} to {pfn}")
            except Exception as e:
                # Log the error but continue with other paths
                logger.error(f"Failed to convert base path {base_path} to PFN: {e}")
                # Use the original path as fallback
                self.base_pfns.append(base_path)
                logger.warning(f"Using original path {base_path} as fallback")
        
        if not self.base_pfns:
            raise RuntimeError("No valid base paths could be initialized")
        
        logger.info(f"Initialized WLCGFileSystem with {len(self.base_pfns)} base paths")
    
    def exists(self, path: str) -> bool:
        """
        Check if a file/path exists in the WLCG file system.
        
        Args:
            path: Path to check
            
        Returns:
            True if path exists, False otherwise
        """
        # This is a simplified implementation
        # In practice, you would use appropriate WLCG tools
        try:
            # Try to convert to PFN first
            pfn = path_to_pfn(path)
            # Simple existence check - this would need proper WLCG implementation
            return os.path.exists(pfn) if pfn.startswith('/') else False
        except Exception as e:
            logger.warning(f"Error checking existence of {path}: {e}")
            return False
    
    def remove(self, path: str) -> bool:
        """
        Remove a file from the WLCG file system.
        
        Args:
            path: Path to remove
            
        Returns:
            True if successful, False otherwise
        """
        try:
            pfn = path_to_pfn(path)
            if pfn.startswith('/') and os.path.exists(pfn):
                os.remove(pfn)
                return True
            return False
        except Exception as e:
            logger.warning(f"Error removing {path}: {e}")
            return False
    
    def copy(self, src: str, dst: str) -> bool:
        """
        Copy a file within the WLCG file system.
        
        Args:
            src: Source path
            dst: Destination path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            src_pfn = path_to_pfn(src)
            dst_pfn = path_to_pfn(dst)
            
            # Simplified copy - would need proper WLCG implementation
            import shutil
            if src_pfn.startswith('/') and os.path.exists(src_pfn):
                os.makedirs(os.path.dirname(dst_pfn), exist_ok=True)
                shutil.copy2(src_pfn, dst_pfn)
                return True
            return False
        except Exception as e:
            logger.warning(f"Error copying {src} to {dst}: {e}")
            return False


class WLCGFileTarget:
    """
    WLCG file target that uses WLCGFileSystem.
    """
    
    def __init__(self, path: str, fs: WLCGFileSystem):
        """
        Initialize WLCG file target.
        
        Args:
            path: File path
            fs: WLCG file system instance
        """
        self.path = path
        self.fs = fs
        # Initialize parent class if law is available
        if LAW_AVAILABLE:
            law.FileSystemTarget.__init__(self, path)
        else:
            MockFileSystemTarget.__init__(self, path)
    
    def exists(self) -> bool:
        """Check if the target exists."""
        return self.fs.exists(self.path)
    
    def remove(self) -> bool:
        """Remove the target."""
        return self.fs.remove(self.path)
    
    def copy_to_local(self, local_path: str) -> bool:
        """
        Copy target to local file system.
        
        Args:
            local_path: Local destination path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            src_pfn = path_to_pfn(self.path)
            if src_pfn.startswith('/') and os.path.exists(src_pfn):
                import shutil
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                shutil.copy2(src_pfn, local_path)
                return True
            return False
        except Exception as e:
            logger.warning(f"Error copying {self.path} to {local_path}: {e}")
            return False
    
    def copy_from_local(self, local_path: str) -> bool:
        """
        Copy from local file system to target.
        
        Args:
            local_path: Local source path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            dst_pfn = path_to_pfn(self.path)
            if os.path.exists(local_path):
                import shutil
                os.makedirs(os.path.dirname(dst_pfn), exist_ok=True)
                shutil.copy2(local_path, dst_pfn)
                return True
            return False
        except Exception as e:
            logger.warning(f"Error copying {local_path} to {self.path}: {e}")
            return False