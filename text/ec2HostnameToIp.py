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

__author__ = "Jay Taylor [@jtaylor]"

import re as _re

_ec2HostnameRe = _re.compile(
    r"""
        ^(?P<start>.*)
        ec2-(?P<ip>(?:\d+-?){4,4})\.compute-\d\.amazonaws\.com
        (?P<end>.*)$
    """,
    _re.X,
)


def ec2HostnameToIp(s):
    """
    Translating an input string to replace any EC2 hostname with the bare IP.

    Example:
    If the input is
    "postgres://*:*@ec2-107-22-243-182.compute-1.amazonaws.com:5432/dbname",
    the output will be "postgres://*:*@107.22.243.182:5432/dbname"
    """

    # Attempting to match an EC2 hostname in the input string
    m = _ec2HostnameRe.match(s)
    while m is not None:
        # Replacing the matched EC2 hostname with the bare IP
        ip = m.group("ip").replace("-", ".")
        s = f"{m.group('start')}{ip}{m.group('end')}"
        # Continuing to search for additional EC2 hostnames
        m = _ec2HostnameRe.match(s)

    return s
