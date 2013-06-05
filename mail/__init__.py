# encoding: utf-8

"""E-mail utilities."""

from django.conf import settings
from django.core.mail import send_mail
from django.views.debug import get_exception_reporter_filter
from .smtp import sendHtmlEmail
import logging
import sys, traceback

def sendEmail(subject, body, fromAddress, toAddress):
    """Sends an email."""
    logging.info('Sending email, subject={subject}, body={body}, ' \
        'fromAddress={fromAddress}, toAddress={toAddress}'.format(
        subject=subject,
        body=body,
        fromAddress=fromAddress,
        toAddress=toAddress
    ))
    if type(toAddress) is str:
        toAddress = (toAddress,)
    if settings.REALLY_SEND_EMAIL is True:
        send_mail(subject, body, fromAddress, toAddress, fail_silently=False)
    else:
        logging.info('NOTICE: Didn\'t really send e-mail, disabled by configuration')


def sendErrorEmail(e, request=None):

    info = {'error': str(e.message)}

    if hasattr(settings, 'DEBUG') and settings.DEBUG:
        # Development:
        info['stacktrace'] = map(
            lambda s: s.strip(),
            traceback.format_exception(*sys.exc_info())
        )

    if request is not None:
        try:
            filter = get_exception_reporter_filter(request)
            request_repr = filter.get_request_repr(request)
        except Exception:
            request_repr = "Request repr() unavailable."
    else:
        request_repr = "Request repr() unavailable."

    stacktrace = map(
        lambda s: s.strip(),
        traceback.format_exception(*sys.exc_info())
    )

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
    'sendEmail',
    'sendHtmlEmail',
    'sendErrorEmail'
]

