import settings
import logging
import simplejson as json
from sh_util.retry import retry


SIFTSCIENCE_CHOICES = (
    ('$spam', 'spam'),
    ('$spam', 'nigeria'),
    ('$spam', 'high block rate'),
    ('$spam', '2k first 24 hours'),
    ('$duplicate_account', 'duplicate'),
    ('$chargeback', 'chargeback'),
    ('$chargeback', 'suspicious payment'),
)

def isBadReason(reason):
    """
    Confirms that the passed in reason (which will be stored in SH db,
    maps to a siftscience label we would consider a bad user
    """

    try:
        SIFTSCIENCE_CHOICES[map(lambda (x, y): y, SIFTSCIENCE_CHOICES).index(reason)][0]
        return True
    except ValueError:
        pass
    return False

def mapReasonToSiftScienceLabel(reason):
    """
    Fetches the sift science label for the given suspension reason.
    There are multiple types of spam reasons (nigerian, high_block_rate)
    that we want to track but siftscience only needs to label them as spam.
    """

    return SIFTSCIENCE_CHOICES[map(lambda (x, y): y, SIFTSCIENCE_CHOICES).index(reason)][0]

def labelUser(userId, isBad, reason):
    """
    Send the sift science label to sift science
    """
    from sh_util.http.wget import wget

    SIFTSCIENCE_203_API_URL = 'https://api.siftscience.com/v203/'

    if settings.SIFTSCIENCE_ENABLED != '1':
        logging.warning('Siftscience disabled. Exiting.')
        return

    if isBad:
        label = mapReasonToSiftScienceLabel(reason)
    else:
        label = 'n/a'

    logging.info('Labelling user {} as bad=={} label =={} because of reason=={}'.format(userId, isBad, label, reason))

    assert (isBad is False) or (isBad is True and isBadReason(reason)), '{} is not a valid reason to label as bad'.format(reason)

    postData = {
        '$is_bad': isBad,
        '$api_key': settings.SIFTSCIENCE_API_KEY
    }

    # only add the reasons if the user is bad
    if isBad:
        postData['$reasons'] = [label]

    postData = json.dumps(postData)

    @retry(3, desiredOutcome=lambda x: x is not None)
    def doLabelWithRetry():
        """POST labeled user to SiftScience."""
        try:
            wget('{0}users/{1}/labels'.format(SIFTSCIENCE_203_API_URL, userId), requestType='POST', body=postData)
            return True

        except Exception, e:
            logging.error(u'Caught exception: {0}, returning False'.format(e))
            return None

    doLabelWithRetry()
