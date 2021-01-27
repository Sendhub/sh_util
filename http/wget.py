"""
@author Jay Taylor [@jtaylor]
@date 2010-11-01

Copyright Jay Taylor 2010
"""
import socket
import logging

# For G-Zip decompression.
import gzip
import io
import re
import urllib.parse
import urllib.request
import urllib.error

socket.setdefaulttimeout(30)

#  USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; ' \
#    'rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10)'
USER_AGENT = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; ' \
    'rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15'


class WgetError(Exception):
    """WgetError class."""
    pass


_urlRe = re.compile(
    r'^https?://(?P<host>[^/:]+(?P<port>[1-9][0-9]*)?)(?P<path>/.*)?$'
)


def normalize_url(url):
    """
    Normalize a url to be properly url-encoded.

    @see http://stackoverflow.com/a/120959/293064 and
        http://docs.python.org/library/urlparse.html for more info.
    """
    parts = urllib.parse.urlparse(url)
    path = urllib.parse.quote_plus(parts.path, safe='&=/.')
    params = urllib.parse.quote_plus(parts.params, safe='&=/.')
    query = urllib.parse.quote_plus(parts.query, safe='&=/.')
    fragment = urllib.parse.quote_plus(parts.fragment, safe='&=/.')
    result = urllib.parse.urlunparse((
        parts.scheme,
        parts.netloc,
        path,
        params,
        query,
        fragment
    ))
    return result


def wget_opener(referer='http://www.google.com/GOBBLEGOBBLEGOBBLE'):
    """Custom opener."""
    opener = urllib.request.build_opener()
    opener.addheaders = [
        ('User-agent', USER_AGENT),
        ('Referer', referer),
    ]
    return opener


def wget(
    url,
    request_type='GET',
    body=None,
    referer=None,
    num_tries=1,
    accept_encoding=None,
    user_agent=USER_AGENT,
    headers=None,
    timeout=None,
    as_dict=False
):
    """
    Execute an HTTP request.  This is called 'wget' but it is really more like
    curl..
    """
    timeout = timeout if timeout is not None else socket.getdefaulttimeout()

    if num_tries <= 0:
        raise WgetError('Not able to be opened in 0 tries left')

    if headers is None:
        headers = {}

    if accept_encoding is not None:
        headers['Accept-Encoding'] = accept_encoding
    if user_agent is not None:
        headers['User-Agent'] = user_agent
    if referer is not None:
        headers['Referer'] = referer

    opener = urllib.request.build_opener()
    opener.addheaders = [(header, value) for header, value in list(headers.items())]

    try:
        url = normalize_url(url)
        logging.info('w\'%sting %s' % (request_type.lower(), url))
        if request_type is 'GET':
            res = opener.open(url, timeout=timeout)

            if as_dict:
                received_data = dict(body=res.read(), code=res.code,
                                     headers=res.info(), url=res.geturl())
            else:
                received_data = res.read()
        else:
            if as_dict:
                # this is just because I haven't implemented this functionality
                # yet for calls that don't use urllib
                raise WgetError('as_dict can only be True for GETs')

            import http.client
            parsed = _urlRe.match(url)
            if not parsed:
                raise WgetError('Invalid hostname: {0}'.format(url))

            maybe_port = parsed.group('port')
            port = int(maybe_port) if maybe_port is not None else \
                (443 if url.startswith('https') else 80)

            if port == 443:
                conn = http.client.HTTPSConnection(
                    parsed.group('host'),
                    port=port,
                    timeout=timeout
                )
            else:
                conn = http.client.HTTPConnection(
                    parsed.group('host'),
                    port=port,
                    timeout=timeout
                )

            conn.request(request_type, parsed.group('path'), body, headers)
            resp = conn.getresponse()
            received_data = resp.read()
        try:
            compressedstream = io.StringIO(received_data)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            received_data = gzipper.read()
        except IOError:
            pass

        return received_data

    except urllib.error.URLError as e:
        if num_tries > 1:
            return wget(
                url=url,
                referer=referer,
                headers=headers,
                num_tries=num_tries - 1
            )
        raise WgetError(url + b' failed, ' + str(e))
