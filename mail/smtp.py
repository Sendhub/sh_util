"""
send email methods
"""

__author__ = 'brock'

from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.template import Context
from django.utils.html import strip_tags


def send_html_email(from_email, to_emails, subject, template, template_vars):
    """
    Send email
    """
    html_content = render_to_string(template, template_vars, Context({}))
    text_content = strip_tags(html_content)

    if isinstance(to_emails, list):
        recipient = to_emails
    else:
        recipient = [to_emails]
        # create the email, and attach the HTML version as well.
    msg = EmailMultiAlternatives(subject, text_content, from_email,
                                 to=recipient)
    msg.attach_alternative(html_content, "text/html")
    msg.send()
