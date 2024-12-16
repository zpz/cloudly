"""
Utilities for cloud computing.
"""

__version__ = '0.0.4'

from . import util

try:
    from . import gcp
except ImportError:
    pass
