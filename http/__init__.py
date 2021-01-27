"""
http
"""
from sh_util.http.request_common import extract_parameters
from sh_util.http.wget import wget

__all__ = [
    'extract_parameters',
    'wget',
]
