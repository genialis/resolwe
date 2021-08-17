"""Custom exceptions used in billing app."""
from rest_framework.exceptions import APIException


class ValidationError(APIException):
    """Validation error."""
