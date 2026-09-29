"""
This module implements the exception catching class.
"""

__author__ = "brock"

import simplejson as json


class ErrorResponse:
    """
    Implements the Error Response class.

    Attributes:
        message (str): The error message.
        dev_message (str): The developer-specific error message.
        code (str): The error code.
        more_info (str): Additional information about the error.
    """

    def __init__(self, message, dev_message="", code="", more_info=""):
        """
        Initializes the ErrorResponse instance.

        Args:
            message (str): The error message.
            dev_message (str, optional): The developer-specific error message. Defaults to an empty string.
            code (str, optional): The error code. Defaults to an empty string.
            more_info (str, optional): Additional information about the error. Defaults to an empty string.
        """
        self.message = message
        self.dev_message = dev_message
        self.code = code
        self.more_info = more_info

    def __unicode__(self):
        """
        Returns the string representation of the error response in JSON format.

        Returns:
            str: The JSON string representation of the error response.
        """
        return json.dumps(self.__dict__)

    def __str__(self):
        """
        Returns the string representation of the error response in JSON format.

        Returns:
            str: The JSON string representation of the error response.
        """
        return json.dumps(self.__dict__)
