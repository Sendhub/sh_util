# encoding: utf-8

"""
Translate an input string to replace any ec2 hostname with the bare ip.

e.g.: If the input is
    "postgres://*:*@ec2-107-22-243-182.compute-1.amazonaws.com:5432/dbname",
    the output will be "postgres://*:*@107.22.243.182:5432/d67shu8760iutg"

>>> ec2HostnameToIp(
...     'postgres://*:*@ec2-107-22-243-182.compute-1.amazonaws.com:5432/dbname'
... )
'postgres://*:*@107.22.243.182:5432/dbname'
>>> ec2HostnameToIp('This should come back unchanged')
'This should come back unchanged'
"""

__author__ = 'Jay Taylor [@jtaylor]'

import re as _re

_ec2HostnameRe = _re.compile(
    r'''
        ^(?P<start>.*)
        ec2-(?P<ip>(?:\d+-?){4,4})\.compute-\d\.amazonaws\.com
        (?P<end>.*)$
    ''',
    _re.X
)


def ec2HostnameToIp(s):
    """
    Translate an input string to replace any ec2 hostname with the bare ip.

    e.g. If the input is
    "postgres://*:*@ec2-107-22-243-182.compute-1.amazonaws.com:5432/dbname",
    the output will be "postgres://*:*@107.22.243.182:5432/d67shu8760iutg"
    """
    m = _ec2HostnameRe.match(s)
    while m is not None:
        ip = m.group('ip').replace('-', '.')
        s = '{0}{1}{2}'.format(m.group('start'), ip, m.group('end'))
        m = _ec2HostnameRe.match(s)

    return s
