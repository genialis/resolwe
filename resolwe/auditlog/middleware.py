"""Resolwe custom middleware class."""

import logging
import uuid

import resolwe.auditlog.auditmanager
from resolwe.permissions.models import get_anonymous_user

logger = logging.getLogger(__name__)


class ResolweAuditMiddleware:
    """Register custom audit manager.

    The manager looks for object request (read/write/create) by modifying base
    object and registering with the audit manager in appropriate moments.

    The manager is registered at the beginning of the request and storage into
    thread local storage (so one instance per request exists).

    At the end of the request the gathered data is logged (using signals sent
    by Django) using custom logger.
    """

    def __init__(self, get_response):
        """Initialize middleware."""
        self.get_response = get_response

    def _resolve_user(self, user):
        """Resolve user instance from request."""
        return get_anonymous_user() if user.is_anonymous else user

    def __call__(self, request):
        """Process the request and store it.

        Also set the request id.
        """
        audit_manager = resolwe.auditlog.auditmanager.AuditManager.global_instance()
        audit_manager.reset()

        user = self._resolve_user(request.user)
        # Extract request id from the request headers or generate a random one.
        request_id_headers = ("HTTP_X_REQUEST_ID", "HTTP_X-AMZ-CF-ID")
        request_ids = [
            request.META.get(header)
            for header in request_id_headers
            if header in request.META
        ]
        request_id = request_ids[0] if request_ids else str(uuid.uuid4())
        request.META["RESOLWE_AUDIT_MANAGER_REQUEST_ID"] = request_id
        servername = getattr(request.META, "HTTP_HOST", "unknown")

        try:
            response = self.get_response(request)
            message = (
                "Request finished: "
                f"METHOD={request.method}; STATUS={response.status_code}; "
                f"USER={user.username}({user.id}); "
                f"URL={request.build_absolute_uri()}; "
                f"SERVERNAME={servername}"
            )

            audit_manager.log_message(message)
            audit_manager.emit(request, response)
        except Exception:
            logger.exception("Error processing request.")
        finally:
            return response
