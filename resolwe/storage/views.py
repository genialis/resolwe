"""Storage application views."""
import re
from datetime import datetime
from distutils.util import strtobool
from pathlib import Path
from typing import List, Optional, Tuple, Union

import pytz

from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.utils import timezone
from django.views import View

from resolwe.flow.models import Data
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.storage.models import FileStorage, ReferencedPath


class DataBrowseView(View):
    """Emulate nginx JSON directory view of the data directory.

    Use information in database to construct JSON response that emulates nginx
    autoindex module with autoindex_format set to json. See
    http://nginx.org/en/docs/http/ngx_http_autoindex_module.html
    for details.
    """

    def _get_mtime(self, entry: datetime):
        """Convert datetime entry to nginx autoindex compatible string.

        The timezone returned by autoindex module was set to GMT so we convert
        from our timetone to GMT.
        """
        gmt = timezone.localtime(entry, pytz.timezone("GMT"))
        return gmt.strftime("%a, %d %b %Y %H:%M:%S %Z")

    def _path_to_dict(self, referenced_path: ReferencedPath, base_path: Path):
        """Convert ReferencedPath to dictionary emulating nginx response."""
        data = {
            "name": Path(referenced_path.path).relative_to(base_path).as_posix(),
            "type": "file",
            "mtime": self._get_mtime(referenced_path.file_storage.created),
            "size": referenced_path.size,
        }
        if referenced_path.path.endswith("/"):
            data.pop("size")
            data["type"] = "directory"
        return data

    def _get_datum(self, data_id: int) -> Data:
        """Get datum with given id and check it's permissions."""
        # Check permissions and load requested data object.
        data = get_objects_for_user(
            self.request.user, "view_data", Data.objects.filter(pk=data_id)
        )
        # Send response with status 403 when requested data object does not exists.
        if not data.exists():
            raise PermissionDenied()

        return data.get()

    def _resolve_dir(self, relative_path: Path, file_storage: FileStorage) -> List:
        """Resolve directory."""
        # Empty path evaluates to "."
        regex_path = relative_path.as_posix() + "/" if relative_path != Path() else ""
        # Show only entries in this directory.
        regex = "^{}[^/]+/?$".format(regex_path)
        return [
            self._path_to_dict(path, regex_path)
            for path in file_storage.files.filter(path__regex=regex)
        ]

    def _resolve_file(self, relative_path: Path, file_storage: FileStorage) -> str:
        """Resolve file URI and return signed url."""
        # Redirect to the resource.
        force_download = strtobool(self.request.GET.get("force_download", "false"))
        return file_storage.default_storage_location.connector.presigned_url(
            file_storage.subpath / relative_path, force_download=force_download
        )

    def _get_response(
        self, datum: Data, relative_path: Path
    ) -> Optional[Tuple[str, bool]]:
        """Return a (response, is_file) tuple."""
        file_storage: FileStorage = datum.location

        if (
            relative_path == Path()
            or file_storage.files.filter(path=relative_path.as_posix() + "/").exists()
        ):
            # If directory
            return (self._resolve_dir(relative_path, file_storage), False)
        elif file_storage.files.filter(path=relative_path.as_posix()).exists():
            # If file
            return (self._resolve_file(relative_path, file_storage), True)
        else:
            return None, False

    def get(
        self, request: HttpRequest, *args: Union[str, int], **kwargs: Union[str, int]
    ) -> HttpResponse:
        """Giver URL either redirect to resource or serve a list of resources.

        If URL points to a file then redirect to a default storage location.
        If URL points to a directory then send a JSON encoded list of entires
        in this directory emulating nginx autoindex module.

        :param request: the given request. When the user requests URL
            http://platform-centos.genialis.local/data/1/..., then the value of
            parameter should be 1/... .
        """

        data_id = kwargs.get("data_id")
        relative_path = Path(kwargs.get("uri", ""))

        datum = self._get_datum(data_id)
        response, is_file = self._get_response(datum, relative_path)
        if is_file:
            return redirect(response)
        elif response:
            return JsonResponse(response, safe=False)
        else:
            raise PermissionDenied()


class UriResolverView(DataBrowseView):
    """Get resolved and signed URLs for a given list of URIs."""

    def get(
        self, request: HttpRequest, *args: Union[str, int], **kwargs: Union[str, int]
    ) -> HttpResponse:
        """Get resolved and signed URLs for a given list of URIs."""

        response_data = {}

        for uri in request.GET.getlist("uri", []):
            match = re.match(r"(\d+)/(.+)", uri)
            if not match:
                response_data[uri] = ""
                continue
            data_id = int(match.group(1))
            relative_path = Path(match.group(2))
            datum = self._get_datum(data_id)

            response = self._get_response(datum, relative_path)[0]
            if response:
                response_data[uri] = response
            else:
                raise PermissionDenied()

        return JsonResponse(response_data)
