"""
Memcache lazy initialization.
"""
# encoding: utf-8
__author__ = 'Jay Taylor [@jtaylor]'

import os as _os
import logging
import threading as _threading
import settings as _settings
import pylibmc as _pylibmc


def get_memcache_client(new_connection=False):
    """
    Lazily access a memcache client instance.

    @param new_connection boolean Defaults to False.  When True, will force a new
        client to be created.
    """
    pid = _os.getpid()
    tid = _threading.current_thread().ident
    key = '{0}-{1}'.format(pid, tid)

    if _settings.MEMCACHE_CLIENTS.get(key, None) is None or \
            new_connection is True:
        logging.info('[MEMCACHE] pid:%d threadid:%d Getting new memcache client connection',
                     pid, tid)
        _settings.MEMCACHE_CLIENTS[key] = _pylibmc.Client(
            _settings.MEMCACHE_SERVERS.split(','),
            binary=True,
            username=_settings.MEMCACHE_USERNAME,
            password=_settings.MEMCACHE_PASSWORD
        )
        _settings.MEMCACHE_CLIENTS[key].behaviors = {
            'tcp_nodelay': True,
            'ketama': True,
        }

    return _settings.MEMCACHE_CLIENTS[key]


def attempt_memcache_flush():
    """Attempt to flush the memcache server."""
    try:
        logging.info('[MEMCACHE] Attempting to flush all')
        get_memcache_client().flush_all()
        logging.info('[MEMCACHE] Flush completed')
    except Exception as err:
        logging.error('[MEMCACHE] Flush failed, %s', str(err))


__all__ = ['get_memcache_client', 'attempt_memcache_flush']
