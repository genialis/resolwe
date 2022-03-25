"""Audit logger."""

import logging
from typing import Optional

from rest_framework.request import Request
from rest_framework.response import Response

from resolwe.auditlog.auditmanager import AccessType, AuditManager, ContentType, Fields

audit_log = logging.getLogger("auditlog")


class AuditLogger:
    """Main audit log class."""

    @property
    def manager(self):
        """Get the global audit manager."""
        return AuditManager.global_instance()

    def info(self, message: str, *args, **kwargs):
        """Log custom message at info level.

        :attr message: the logger message.
        :attr args: arguments to pass to the logger.
        :attr kwargs: keyword arguments to pass to the logger. When 'request'
            or 'response' keys are present it is expected their correspondent
            values are request and response object. The information is
            extracted from them and added to the kwargs.
        """
        extra_data = kwargs.setdefault("extra", dict())
        extra_data.update(
            self._extract_information(kwargs.pop("request"), kwargs.pop("response"))
        )
        audit_log.info(message, *args, **kwargs)

    def _extract_from_request(self, request: Request) -> dict:
        """Extract the information from the request."""
        return {
            "url": request.get_full_path(),
            "server_name": getattr(request.META, "HTTP_HOST", "unknown"),
            "session_id": request.session.session_key
            if hasattr(request, "session")
            else None,
            "request_id": request.META["RESOLWE_AUDIT_MANAGER_REQUEST_ID"],
        }

    def _extract_from_response(self, response: Response) -> dict:
        """Extract the information from the response."""
        return {"status_code": response.status_code}

    def _extract_information(
        self, request: Optional[Request], response: Optional[Response]
    ) -> dict:
        """Extract the information from the request and the response."""
        info = dict()
        if request is not None:
            info.update(self._extract_from_request(request))
        if response is not None:
            info.update(self._extract_from_response(response))
        return info

    def log_object_access(
        self,
        request,
        response,
        content_type: ContentType,
        object_id: int,
        access_type: AccessType,
        fields: Fields,
        level=logging.INFO,
    ):
        """Create an audit log entry.

        :args message: the string to log.
        :args content_type: content_type for audit log. If not given it is
            determined from the queryset.
        :args access_type: C,R,U, or D.
        """
        audit_log_extra_data = {
            "content_type": content_type,
            "accessed_ids": object_id,
            "crud": access_type,
            "fields": ",".join(sorted(fields)),
        }
        audit_log_extra_data.update(self._extract_information(request, response))
        message = (
            "Object accessed: {content_type} {accessed_ids} {fields} {crud}".format(
                **audit_log_extra_data
            )
        )
        audit_log.log(level, message, extra=audit_log_extra_data)


logger = AuditLogger()
