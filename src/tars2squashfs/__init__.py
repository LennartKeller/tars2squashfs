"""
tars2squashfs - Convert tar.gz archives to SquashFS filesystem

A tool for converting multiple tar.gz archives into a single SquashFS filesystem
with optimized memory usage and minimal inode consumption during processing.
"""

__version__ = "0.1.0"
__author__ = "Lennart Keller"
__email__ = "lennartkeller@gmail.com"

from .main import main, SquashFSBuilder

__all__ = ["main", "SquashFSBuilder"]