"""
siftscience module.

Providing utilities for mapping suspension reasons and sending user labels
to the Sift Science API.

"""

import logging

import simplejson as json

import settings

from ..retry import retry
from ..sh_http.wget import wget

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
    Confirming whether the provided reason is recognized as a bad reason.

    Args:
        reason: The reason entry from `SIFTSCIENCE_CHOICES` to validate.

    Returns:
        True if `reason` is a valid bad reason choice, otherwise False.

    """

    try:
        if SIFTSCIENCE_CHOICES[SIFTSCIENCE_CHOICES.index(reason)][0]:
            return True
    except ValueError:
        pass
    return False


def map_reason_to_sift_science_label(reason):
    """
    Fetching the Sift Science label for the given suspension reason.

    There are multiple internal spam reason variants (for example, 'nigerian' or
    'high_block_rate') that are tracked separately but are mapped to the same
    Sift Science label (for example, '$spam').

    Args:
        reason: The reason entry from `SIFTSCIENCE_CHOICES` to map.

    Returns:
        The corresponding Sift Science label string.

    """

    return SIFTSCIENCE_CHOICES[SIFTSCIENCE_CHOICES.index(reason)][0]


def label_user(user_id, is_bad, reason):
    """
    Sending the label for a user to the Sift Science API.

    This function is building the payload for Sift Science and POSTing the
    label for the specified user. When `is_bad` is True, the `reason` is being
    mapped to the appropriate Sift Science label and included in the payload.

    Args:
        user_id: The numeric identifier for the user.
        is_bad: Boolean indicating whether the user is being labeled as bad.
        reason: The reason entry from `SIFTSCIENCE_CHOICES` when `is_bad` is True.

    """

    siftscience_203_api_url = 'https://api.siftscience.com/v203/'

    if settings.SIFTSCIENCE_ENABLED != '1':
        logging.warning(f'Siftscience disabled. Exiting.')
        return

    if is_bad:
        label = map_reason_to_sift_science_label(reason)
    else:
        label = 'n/a'

    logging.info(f'Labelling user {user_id} as bad=={is_bad} label =={label} because of reason=={reason}')

    assert (is_bad is False) or (is_bad is True and is_bad_reason(reason)),  f'{reason} is not a valid reason to label as bad'

    post_data = {
        '$is_bad': is_bad,
        '$api_key': settings.SIFTSCIENCE_API_KEY
    }

    # Adding the reasons only when the user is bad
    if is_bad:
        post_data['$reasons'] = [label]

    post_data = json.dumps(post_data)

    @retry(3, desired_outcome=lambda x: x is not None)
    def do_label_with_retry():
        """
        Posting the labeled user to the Sift Science API.

        Returns:
            True on success, or None on failure to trigger a retry.

        """
        try:
            wget(f'{siftscience_203_api_url}users/{user_id}/labels', request_type='POST', body=post_data)
            return True

        except Exception as err:
            logging.error(f'Caught exception: {err}, returning False')
            return None

    do_label_with_retry()
