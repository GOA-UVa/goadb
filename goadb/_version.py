"""
goadb Version Handler Module
"""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("goadb")
except PackageNotFoundError:
    __version__ = "0.0.0"
