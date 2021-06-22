"""
siftscience
"""
# pylint: disable=E0611,E0401,E1101,W0703
from sh_util.http.wget import wget
import logging
import settings
import simplejson as json
from sh_util.retry import retry


SIFTSCIENCE_CHOICES = (
    ('$spam', 'spam'),
    ('$spam', 'nigeria'),
    ('$spam', 'high block rate'),
    ('$spam', '1k first 24 hours'),
    ('$duplicate_account', 'duplicate'),
    ('$chargeback', 'chargeback'),
    ('$chargeback', 'suspicious payment'),
)


def is_bad_reason(reason):
    """
    Confirms that the passed in reason (which will be stored in SH db,
    maps to a siftscience label we would consider a bad user
    """

    try:
        if SIFTSCIENCE_CHOICES[SIFTSCIENCE_CHOICES.index(reason)][0]:
            return True
    except ValueError:
        pass
    return False


def map_reason_to_sift_science_label(reason):
    """
    Fetches the sift science label for the given suspension reason.
    There are multiple types of spam reasons (nigerian, high_block_rate)
    that we want to track but siftscience only needs to label them as spam.
    """

    return SIFTSCIENCE_CHOICES[SIFTSCIENCE_CHOICES.index(reason)][0]


def label_user(user_id, is_bad, reason):
    """
    Send the sift science label to sift science
    """

    siftscience_203_api_url = 'https://api.siftscience.com/v203/'

    if settings.SIFTSCIENCE_ENABLED != '1':
        logging.warning('Siftscience disabled. Exiting.')
        return

    if is_bad:
        label = map_reason_to_sift_science_label(reason)
    else:
        label = 'n/a'

    logging.info('Labelling user %i as bad==%r label ==%r '
                 'because of reason==%s', user_id, is_bad, label, reason)

    assert (is_bad is False) or (is_bad is True and is_bad_reason(reason)), \
        '{} is not a valid reason to label as bad'.format(reason)

    post_data = {
        '$is_bad': is_bad,
        '$api_key': settings.SIFTSCIENCE_API_KEY
    }

    # only add the reasons if the user is bad
    if is_bad:
        post_data['$reasons'] = [label]

    post_data = json.dumps(post_data)

    @retry(3, desiredOutcome=lambda x: x is not None)
    def do_label_with_retry():
        """POST labeled user to SiftScience."""
        try:
            wget('{0}users/{1}/labels'.format(siftscience_203_api_url, user_id),  # noqa
                 request_type='POST', body=post_data)
            return True

        except Exception as err:
            logging.error('Caught exception: %s, returning False', str(err))
            return None

    do_label_with_retry()
