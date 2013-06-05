__author__ = 'brock'

from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.template import Context
from django.utils.html import strip_tags

def sendHtmlEmail(fromEmail, toEmails, subject, template, templateVars):
    html_content = render_to_string(template, templateVars, Context({}))
    text_content = strip_tags(html_content)

    if type(toEmails) == list:
        to = toEmails
    else:
        to = [toEmails]
        # create the email, and attach the HTML version as well.
    msg = EmailMultiAlternatives(subject, text_content, fromEmail,
        to=to)
    msg.attach_alternative(html_content, "text/html")
    msg.send()
