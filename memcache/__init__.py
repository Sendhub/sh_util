# encoding: utf-8

"""
Memcache lazy initialization.
"""

__author__ = 'Jay Taylor [@jtaylor]'

import settings as _settings
import pylibmc as _pylibmc
import os as _os, threading as _threading
import logging


def getMemcacheClient(newConnection=False):
    """
    Lazily access a memcache client instance.

    @param newConnection boolean Defaults to False.  When True, will force a new
        client to be created.
    """
    pid = _os.getpid()
    tid = _threading.current_thread().ident
    key = '{0}-{1}'.format(pid, tid)

    if _settings.MEMCACHE_CLIENTS.get(key, None) is None or \
        newConnection is True:
        logging.info(
            '[MEMCACHE] pid:{0} threadid:{1} Getting new memcache client ' \
            'connection'.format(pid, tid)
        )
        _settings.MEMCACHE_CLIENTS[key] = _pylibmc.Client(
            _settings.MEMCACHE_SERVERS.split(','),
            binary=True,
            username=_settings.MEMCACHE_USERNAME,
            password=_settings.MEMCACHE_PASSWORD
        )
        _settings.MEMCACHE_CLIENTS[key].behaviors = {
            'tcp_nodelay': True,
            'ketama': True,
            #'remove_failed': False,
        }

    return _settings.MEMCACHE_CLIENTS[key]


def attemptMemcacheFlush():
    """Attempt to flush the memcache server."""
    try:
        logging.info(u'[MEMCACHE] Attempting to flush all')
        getMemcacheClient().flush_all()
        logging.info(u'[MEMCACHE] Flush completed')
    except Exception, e:
        logging.error(u'[MEMCACHE] Flush failed, {0}/{1}'.format(type(e), e))


__all__ = ['getMemcacheClient', 'attemptMemcacheFlush']

