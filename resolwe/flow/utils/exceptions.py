""".. Ignore pydocstyle D400.

========================
Resolwe Exceptions Utils
========================

Utils functions for working with exceptions.

"""
from django.core.exceptions import ValidationError

from rest_framework.response import Response
from rest_framework.views import exception_handler


def resolwe_exception_handler(exc, context):
    """Handle exceptions raised in API and make them nicer.

    To enable this, you have to add it to the settings:

        .. code:: python

            REST_FRAMEWORK = {
                'EXCEPTION_HANDLER': 'resolwe.flow.utils.exceptions.resolwe_exception_handler',
            }

    """
    response = exception_handler(exc, context)

    if isinstance(exc, ValidationError):
        if response is None:
            response = Response({})
        response.status_code = 400
        response.data["error"] = exc.message

    return response
