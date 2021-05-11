"""
Module to implement exception catching class.
"""

__author__ = 'brock'

import simplejson as json


class ErrorResponse():
    """
    Error Response class implementation
    """
    def __init__(self, message, dev_message='', code='', more_info=''):
        self.message = message
        self.dev_message = dev_message
        self.code = code
        self.more_info = more_info

    def __unicode__(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return json.dumps(self.__dict__)
