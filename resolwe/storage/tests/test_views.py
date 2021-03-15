"""Test resolwe.storage.views."""
import json
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, TestCase

from rest_framework import status

from resolwe.storage.views import UriResolverView


class UriResolverViewTest(TestCase):
    """Test UriResolverView."""

    def setUp(self):
        self.factory = RequestFactory()

    @patch("resolwe.storage.views.UriResolverView._get_datum")
    @patch("resolwe.storage.views.DataBrowseView._get_response")
    def test_post(self, get_response_mock, get_datum_mock):
        """Test post method."""
        get_datum_mock.return_value = MagicMock()
        get_response_mock.side_effect = [
            ("signed_url1", True),
            ("dir_structure", False),
            ("signed_url2", True),
        ]

        uris = [
            "123/file1.txt",
            "456/dir",
            "789/dir/file2.txt",
        ]
        request = self.factory.post("", {"uris": uris}, content_type="application/json")

        response = UriResolverView().post(request)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            json.loads(response.content.decode("utf-8")),
            {
                "123/file1.txt": "signed_url1",
                "456/dir": "dir_structure",
                "789/dir/file2.txt": "signed_url2",
            },
        )

    def test_get(self):
        """Test get method."""
        request = self.factory.get("")
        response = UriResolverView().get(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.content.decode("utf-8"), "")
