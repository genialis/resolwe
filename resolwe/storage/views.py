"""Storage application views."""

import json
import logging
import re
import tempfile
from datetime import datetime
from distutils.util import strtobool
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple, Union
from urllib.parse import urlencode, urlparse

import pytz
from bs4 import BeautifulSoup
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from rest_framework import serializers
from rest_framework.response import Response
from rest_framework.views import APIView

from resolwe.flow.models import Data
from resolwe.flow.models.collection import Collection
from resolwe.permissions.utils import get_user
from resolwe.storage.connectors import ConnectorType, connectors
from resolwe.storage.connectors.baseconnector import BaseStorageConnector
from resolwe.storage.models import FileStorage, ReferencedPath
from resolwe.test.utils import ignore_in_tests

logger = logging.getLogger(__name__)


class UploadConfigSerializer(serializers.Serializer):
    """Serializer for upload configuration."""

    type = serializers.CharField()
    config = serializers.DictField()


class RFC3339DateTimeField(serializers.DateTimeField):
    """DateTime field that serializes to RFC3339 format."""

    format = "%Y-%m-%dT%H:%M:%SZ"


class UploadCredentialsSerializer(serializers.Serializer):
    """Serializer for upload credentials."""

    AccessKeyId = serializers.CharField()
    SecretAccessKey = serializers.CharField()
    Token = serializers.CharField()
    Expiration = RFC3339DateTimeField()


class UploadCredentials(APIView):
    """Get the upload credentials."""

    # Used to generate the API schema.
    serializer_class = UploadCredentialsSerializer

    def get(self, request, *args, **kwargs):
        """Return the upload credentials."""
        try:
            upload_connector = connectors.for_storage("upload")[0]
        except Exception:
            message = "Upload connector could not be determined."
            logger.exception(message)
            raise ImproperlyConfigured(message)

        if upload_connector.CONNECTOR_TYPE != ConnectorType.S3:
            raise ImproperlyConfigured(
                "Credentials endpoint only supports S3 connector."
            )
        prefix = str(get_user(request.user).id)
        credentials = upload_connector.temporary_credentials(prefix)["credentials"]
        # The credentials are returned in the format expected by the AWS SDK, except
        # the key "SessionToken", which must be transformed to "Token".
        credentials["Token"] = credentials.pop("SessionToken")
        return Response(self.serializer_class(credentials).data)


class UploadConfig(APIView):
    """Get the upload configuration."""

    serializer_class = UploadConfigSerializer

    def get(self, request, *args, **kwargs):
        """Return the JSON representing the upload configuration.

        The returning object is JSON representation of the dictionary with the
        following fields:

        - type: the type of upload connector. Currently we
          support 'LOCAL', and 'S3' connector types. Upload through server is
          always supported.

        - credentials: the dictionary representing the set of credentials that
          are used to upload data. This dictionary is specific to connector
          type and may be empty.
        """
        try:
            upload_connector = connectors.for_storage("upload")[0]
            prefix = str(get_user(request.user).id)
            response = {
                "type": upload_connector.CONNECTOR_TYPE.name,
                "config": upload_connector.temporary_credentials(prefix),
            }
        except Exception:
            message = "Upload connector could not be determined."
            logger.exception(message)
            raise ImproperlyConfigured(message)

        serializer = self.serializer_class(response)
        return Response(serializer.data)


class DataBrowseView(View):
    """Emulate nginx JSON directory view of the data directory.

    Use information in database to construct JSON response that emulates nginx
    autoindex module with autoindex_format set to json. See
    http://nginx.org/en/docs/http/ngx_http_autoindex_module.html
    for details.
    """

    def _get_mtime(self, entry: datetime) -> str:
        """Convert datetime entry to nginx autoindex compatible string.

        The timezone returned by autoindex module was set to GMT so we convert
        from our timetone to GMT.
        """
        gmt = timezone.localtime(entry, pytz.timezone("GMT"))
        return gmt.strftime("%a, %d %b %Y %H:%M:%S %Z")

    def _path_to_dict(
        self, referenced_path: ReferencedPath, base_path: Union[str, Path]
    ) -> Dict[str, Union[str, int]]:
        """Convert ReferencedPath to dictionary emulating nginx response."""
        assert referenced_path.file_storage is not None
        data = {
            "name": Path(referenced_path.path).relative_to(base_path).as_posix(),
            "type": "file",
            "mtime": self._get_mtime(referenced_path.file_storage.created),
            "size": referenced_path.size,
            "md5": referenced_path.md5,
        }
        if referenced_path.path.endswith("/"):
            data.pop("size")
            data["type"] = "directory"
        return data

    def _get_datum(self, data_id: int) -> Data:
        """Get datum with given id and check it's permissions."""
        # Check permissions and load requested data object.
        data = Data.objects.filter(pk=data_id).filter_for_user(self.request.user)
        # Send response with status 403 when requested data object does not exists.
        if not data.exists():
            raise PermissionDenied()

        return data.get()

    def _resolve_dir(
        self, relative_path: Path, file_storage: FileStorage
    ) -> List[Dict[str, Union[str, int]]]:
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
        """Resolve file URI and return signed url.

        :raises PermissionDenied: if presigned URL could not be generated.
        """
        # Redirect to the resource.
        force_download = strtobool(self.request.GET.get("force_download", "false"))
        default_storage_location = file_storage.default_storage_location
        assert default_storage_location is not None
        presigned_url = default_storage_location.connector.presigned_url(
            file_storage.subpath / relative_path, force_download=force_download
        )
        if presigned_url is None:
            raise PermissionDenied()

        return presigned_url

    def _get_response(
        self, datum: Data, relative_path: Path
    ) -> Tuple[Union[str, list], bool]:
        """Return a (response, is_file) tuple.

        :raises PermissionDenied: when path can not be resolved.
        """
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

        raise PermissionDenied()

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

        data_id = int(kwargs["data_id"])
        relative_path = str(kwargs.get("uri", ""))
        if Path(urlparse(relative_path).path).suffix in [".html", ".htm"]:
            redirect_url = reverse("HTMLPreprocessorView", args=args, kwargs=kwargs)
            arguments = urlencode(request.GET)
            if arguments:
                redirect_url = f"{redirect_url}?{arguments}"
            return redirect(redirect_url)

        datum = self._get_datum(data_id)
        response, is_file = self._get_response(datum, Path(relative_path))
        if is_file:
            return redirect(response)
        elif response:
            return JsonResponse(response, safe=False)
        else:
            raise PermissionDenied()


class HTMLPreprocessorView(DataBrowseView):
    """Serve HTML files.

    Pull the file to the server, pre-sign all the links in the file (including
    CSS and Javascript) and serve the modified file.
    """

    def get(
        self, request: HttpRequest, *args: Union[str, int], **kwargs: Union[str, int]
    ) -> HttpResponse:
        """Serve transformed HTML."""

        def _process_url(
            url: str, from_connector: BaseStorageConnector, subpath: Path
        ) -> str:
            """Presign URL if necessary."""
            parsed_url = urlparse(url)
            # We have to process link when it is relative (no scheme and no
            # netloc is given). Is should also not be anchor link (no path).
            if parsed_url.scheme or parsed_url.netloc or not parsed_url.path:
                return url

            # HTML files should be redirected to HTMLProcessor view.
            if Path(parsed_url.path).suffix in [".htm", ".html"]:
                return reverse("HTMLPreprocessorView", args=args, kwargs=kwargs)
            else:
                presigned_url = from_connector.presigned_url(
                    subpath / url, expiration=3600
                )
                if presigned_url is None:
                    raise RuntimeError(
                        "Unable to presign URL {url} for data object with id {subpath}."
                    )
                return presigned_url

        data_id = int(kwargs["data_id"])
        relative_path = Path(str(kwargs.get("uri", "")))
        assert relative_path.suffix in [
            ".html",
            ".htm",
        ], f"Only HTML files can be served, {relative_path} requested."
        datum = self._get_datum(data_id)
        subpath = Path(datum.location.subpath)
        from_connector = datum.location.default_storage_location.connector
        # Temporary file is opened with mode 'w+b' by default.
        with tempfile.SpooledTemporaryFile() as stream:
            from_connector.get(subpath / relative_path, stream)
            stream.seek(0)
            soup = BeautifulSoup(stream)
            tags_attributes = {
                "a": "href",
                "link": "href",
                "script": "src",
                "img": "src",
            }
            for tag in soup.findAll(list(tags_attributes.keys())):
                attribute = tags_attributes[tag.name]
                if attribute in tag.attrs:
                    tag[attribute] = _process_url(
                        tag[attribute], from_connector, subpath
                    )
            result = str(soup)
        response = HttpResponse(result)
        if strtobool(self.request.GET.get("force_download", "false")):
            response["Content-Disposition"] = "attachment"
        return response


@method_decorator(csrf_exempt, name="dispatch")
class UriResolverView(DataBrowseView):
    """Get resolved and signed URLs for a given list of URIs."""

    def get(
        self, request: HttpRequest, *args: Union[str, int], **kwargs: Union[str, int]
    ) -> HttpResponse:
        """
        Return empty response.

        GET Method should not be disabled, but since it is implemented in
        superclass, it is overwritten.
        """
        return HttpResponse("")

    def post(
        self, request: HttpRequest, *args: Union[str, int], **kwargs: Union[str, int]
    ) -> HttpResponse:
        """Get resolved and signed URLs for a given list of URIs."""

        content = {}
        try:
            content = json.loads(request.read())
        except json.decoder.JSONDecodeError:
            pass

        response_data = {}
        for uri in content.get("uris", []):
            match = re.match(r"(\d+)/(.+)", uri)
            if not match:
                response_data[uri] = ""
                continue
            data_id = int(match.group(1))
            relative_path = Path(match.group(2))
            datum = self._get_datum(data_id)
            try:
                response_data[uri] = self._get_response(datum, relative_path)[0]
            except PermissionDenied:
                response_data[uri] = ""

        return JsonResponse(response_data)

    def _get_datum(self, data_id: int) -> Data:
        """
        Get datum with given id and check it's permissions.

        Speedup: Since checking permissions if quite slow, this method
        can become a real bottleneck if there are > 100 Data object to
        check. However, if they are from the same collection it is easy
        to check, just confirm that the user has view permission on
        collection. That can be checked only once and than cached.
        Except in tests, where colletion ids may be reused.
        """

        @ignore_in_tests(lru_cache(maxsize=1))
        def has_view_permission(collection_id: int) -> bool:
            collection = Collection.objects.filter(pk=collection_id).filter_for_user(
                self.request.user
            )
            return collection.exists()

        datum = Data.objects.get(pk=data_id)

        if datum.collection and has_view_permission(datum.collection.id):
            return datum

        return super()._get_datum(data_id)
