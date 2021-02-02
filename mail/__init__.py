"""E-mail utilities."""

# encoding: utf-8

import sys
import traceback
import logging
from django.conf import settings
from django.core.mail import send_mail
from django.views.debug import get_exception_reporter_filter
from smtp import send_html_email


def send_email(subject, body, from_address, to_address):
    """
    Sends an email.
    """
    logging.info('Sending email, subject=%s, body=%s, from_address=%s, to_address=%s',
                 subject, body, from_address, to_address)
    if isinstance(to_address, str):
        to_address = (to_address,)
    if settings.REALLY_SEND_EMAIL is True:
        send_mail(subject, body, from_address, to_address, fail_silently=False)
    else:
        logging.info('NOTICE: Didn\'t really send e-mail, '
                     'disabled by configuration')


def send_error_email(error, request=None):
    """
    send error email
    """
    info = {'error': str(error.message)}
    if hasattr(settings, 'DEBUG') and settings.DEBUG:
        # Development:
        info['stacktrace'] = [s.strip() for s in traceback.format_exception(*sys.exc_info())]

    if request is not None:
        try:
            filtr = get_exception_reporter_filter(request)
            request_repr = filtr.get_request_repr(request)
        except Exception as err:
            logging.warning("found exception while processing : %s", str(err))
            request_repr = "Request repr() unavailable."
    else:
        request_repr = "Request repr() unavailable."

    stacktrace = [s.strip() for s in traceback.format_exception(*sys.exc_info())]

    message = "%s\n\n%s" % (stacktrace, request_repr)

    settings.DEBUG = False

    if not hasattr(settings, 'DEBUG') or \
            (hasattr(settings, 'DEBUG') and not settings.DEBUG):
        from django.core.mail import mail_admins

        if request is not None:
            subject = 'SendHub Exception (%s IP): %s' % \
                      ((request.META.get('REMOTE_ADDR') in
                        settings.INTERNAL_IPS and 'internal' or
                        'EXTERNAL'), request.path)
        else:
            subject = 'SendHub Exception Report'

        mail_admins(subject, message, fail_silently=True)


__all__ = [
    'send_email',
    'send_html_email',
    'send_error_email'
]
