
"""
@author Jay Taylor [@jtaylor]
@date 2010-11-01

Copyright Jay Taylor 2010
"""

#import socks
import socket
import logging
#socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9050)
#socket.socket = socks.socksocket

# For G-Zip decompression.
import gzip, StringIO, re, urlparse, urllib, urllib2

socket.setdefaulttimeout(30)

#USER_AGENT = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; ' \
#    'rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10)'
USER_AGENT = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; ' \
    'rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15'

class WgetError(Exception):
    """WgetError class."""
    pass


_urlRe = re.compile(
    r'^https?:\/\/(?P<host>[^\/:]+(?P<port>[1-9][0-9]*)?)((?P<path>\/.*))?$'
)


def normalizeUrl(url):
    """
    Normalize a url to be properly url-encoded.

    @see http://stackoverflow.com/a/120959/293064 and
        http://docs.python.org/library/urlparse.html for more info.
    """
    parts = urlparse.urlparse(url)
    path = urllib.quote_plus(parts.path, safe='&=/.')
    params = urllib.quote_plus(parts.params, safe='&=/.')
    query = urllib.quote_plus(parts.query, safe='&=/.')
    fragment = urllib.quote_plus(parts.fragment, safe='&=/.')
    result = urlparse.urlunparse((
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
    opener = urllib2.build_opener()
    opener.addheaders = [
        ('User-agent', USER_AGENT),
        ('Referer', referer),
    ]
    return opener


def wget(
    url,
    requestType='GET',
    body=None,
    referer=None,
    numTries=1,
    acceptEncoding=None,
    userAgent=USER_AGENT,
    headers=None,
    timeout=None,
    asDict=False
):
    """
    Execute an HTTP request.  This is called 'wget' but it is really more like
    curl..
    """
    timeout = timeout if timeout is not None else socket.getdefaulttimeout()

    if numTries <= 0:
        raise WgetError('Not able to be opened in 0 tries left')

    if headers is None:
        headers = {}

    if acceptEncoding is not None:
        headers['Accept-Encoding'] = acceptEncoding
    if userAgent is not None:
        headers['User-Agent'] = userAgent
    if referer is not None:
        headers['Referer'] = referer

    opener = urllib2.build_opener()
    opener.addheaders = [(header, value) for header, value in headers.items()]

    try:
        url = normalizeUrl(url)
        logging.info('w\'%sting %s' % (requestType.lower(), url))
        if requestType is 'GET':
            res = opener.open(url, timeout=timeout)

            if asDict:
                receivedData = dict(body=res.read(), code=res.code,
                    headers=res.info(), url=res.geturl())
            else:
                receivedData = res.read()
        else:
            if asDict:
                # this is just because I haven't implemented this functionality
                # yet for calls that don't use urllib
                raise WgetError('asDict can only be True for GETs')

            import httplib
            parsed = _urlRe.match(url)
            if not parsed:
                raise WgetError('Invalid hostname: {0}'.format(url))

            maybePort = parsed.group('port')
            port = int(maybePort) if maybePort is not None else \
                (443 if url.startswith('https') else 80)


            if port == 443:
                conn = httplib.HTTPSConnection(
                    parsed.group('host'),
                    port=port,
                    timeout=timeout
                )
            else:
                conn = httplib.HTTPConnection(
                    parsed.group('host'),
                    port=port,
                    timeout=timeout
                )

            conn.request(requestType, parsed.group('path'), body, headers)
            resp = conn.getresponse()
            receivedData = resp.read()
        try:
            compressedstream = StringIO.StringIO(receivedData)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            receivedData = gzipper.read()
        except IOError:
            pass
        #print 'received=%s' % receivedData

        return receivedData

    except urllib2.URLError, e:
        if numTries > 1:
            return wget(
                url=url,
                referer=referer,
                headers=headers,
                numTries=numTries - 1
            )
        raise WgetError(url + ' failed, ' + str(e))

