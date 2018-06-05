# pylint: disable=missing-docstring
from resolwe.elastic.composer import composer
from resolwe.test import TestCase


class MyExtendableType:
    pass


class ComposerTest(TestCase):
    def test_add_extension(self):
        # Add some extensions.
        composer.add_extension('resolwe.elastic.tests.test_composer.MyExtendableType', 42)
        composer.add_extension('resolwe.elastic.tests.test_composer.MyExtendableType', 'foo')

        # Check that they are all here.
        extensions = composer.get_extensions(MyExtendableType)
        self.assertEqual(extensions, [42, 'foo'])

        extensions = composer.get_extensions('resolwe.elastic.tests.test_composer.MyExtendableType')
        self.assertEqual(extensions, [42, 'foo'])

        extensions = composer.get_extensions(MyExtendableType())
        self.assertEqual(extensions, [42, 'foo'])
