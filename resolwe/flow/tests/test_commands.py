import os

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.utils.six import StringIO


class ToolRegisterTest(TestCase):

    def setUp(self):
        u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')

    def test_tool_register_all(self):
        out, err = StringIO(), StringIO()

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), stdout=out, stder=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertEqual('', err.getvalue())

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), stdout=out, stder=err)
        self.assertTrue('Skip processor test-min: same version installed' in out.getvalue())
        self.assertEqual('', err.getvalue())

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), force=True, stdout=out, stder=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertEqual('', err.getvalue())

    def test_tool_register_filter(self):
        out, err = StringIO(), StringIO()

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), schema='test-min', stdout=out, stder=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertEqual('', err.getvalue())

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), schema='test-min', stdout=out, stder=err)
        self.assertTrue('Skip processor test-min: same version installed' in out.getvalue())
        self.assertEqual('', err.getvalue())

        call_command('tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), schema='test-min', force=True, stdout=out, stder=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertEqual('', err.getvalue())


class ToolRegisterTestNoAdmin(TestCase):

    def test_tool_register_no_admin(self):
        err = StringIO()
        self.assertRaises(SystemExit, call_command, 'tool_register', path=os.path.join(os.path.dirname(__file__), 'tools'), stderr=err)
        self.assertEqual('Admin does not exist: create a superuser\n', err.getvalue())
