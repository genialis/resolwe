"""Storage application views."""

from datetime import datetime
from distutils.util import strtobool
from pathlib import Path
from typing import Union

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

        # Check permissions and load requested data object.
        data_qset = get_objects_for_user(
            self.request.user, "view_data", Data.objects.filter(pk=data_id)
        )
        # Send response with status 403 when requested data object does not exists.
        if not data_qset.exists():
            raise PermissionDenied()

        file_storage: FileStorage = data_qset.get().location
        is_dir = (
            relative_path == Path()
            or file_storage.files.filter(path=relative_path.as_posix() + "/").exists()
        )
        is_file = file_storage.files.filter(path=relative_path.as_posix()).exists()
        if is_dir:
            # Empty path evaluates to "."
            regex_path = (
                relative_path.as_posix() + "/" if relative_path != Path() else ""
            )
            # Show only entries in this directory.
            regex = "^{}[^/]+/?$".format(regex_path)
            data = [
                self._path_to_dict(path, regex_path)
                for path in file_storage.files.filter(path__regex=regex)
            ]
            return JsonResponse(data, safe=False)
        elif is_file:
            # Redirect to the resource.
            force_download = strtobool(request.GET.get("force_download", "false"))
            redirect_url = file_storage.default_storage_location.connector.presigned_url(
                file_storage.subpath / relative_path, force_download=force_download
            )
            return redirect(redirect_url)
        else:
            raise PermissionDenied()
