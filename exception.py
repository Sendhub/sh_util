__author__ = 'brock'

import simplejson as json

class ErrorResponse(object):
    def __init__(self, message, devMessage = '', code = '', moreInfo = ''):
        self.message = message
        self.devMessage = devMessage
        self.code = code
        self.moreInfo = moreInfo

    def __unicode__(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return json.dumps(self.__dict__)