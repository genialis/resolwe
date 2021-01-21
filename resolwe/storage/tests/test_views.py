"""Test resolwe.storage.views."""
import json
from unittest.mock import MagicMock, patch

from django.test import TestCase

from rest_framework import status

from resolwe.storage.views import UriResolverView


class UriResolverViewTest(TestCase):
    """Test UriResolverView."""

    @patch("resolwe.storage.views.DataBrowseView._get_datum")
    @patch("resolwe.storage.views.DataBrowseView._get_response")
    def test_get(self, get_response_mock, get_datum_mock):
        """Test get method."""
        get_datum_mock.return_value = MagicMock()
        get_response_mock.side_effect = [
            ("signed_url1", True),
            ("dir_structure", False),
            ("signed_url2", True),
        ]

        request = MagicMock()
        request.GET.getlist.return_value = [
            "123/file1.txt",
            "456/dir",
            "789/dir/file2.txt",
        ]

        response = UriResolverView().get(request)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            json.loads(response.content.decode("utf-8")),
            {
                "123/file1.txt": "signed_url1",
                "456/dir": "dir_structure",
                "789/dir/file2.txt": "signed_url2",
            },
        )
