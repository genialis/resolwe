# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.managers import manager
from resolwe.flow.models import Process, Data
from resolwe.flow.expression_engines import EvaluationError


class ProcessFieldsTagsTest(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.contributor = user_model.objects.create_user('test_user', 'test_pwd')

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_templatetags(self):
        input_process = Process.objects.create(
            name='Input process',
            contributor=self.contributor,
            type='data:test:inputobject:',
        )
        input_data = Data.objects.create(
            name='Input Data object',
            contributor=self.contributor,
            process=input_process,
        )

        process = Process.objects.create(
            name='Test template tags',
            requirements={'expression-engine': 'jinja'},
            contributor=self.contributor,
            type='test:data:templatetags:',
            input_schema=[
                {'name': 'input_data', 'type': 'data:test:inputobject:'},
            ],
            output_schema=[
                {'name': 'name', 'type': 'basic:string:'},
                {'name': 'id', 'type': 'basic:integer:'},
                {'name': 'type', 'type': 'basic:string:'},
                {'name': 'basename', 'type': 'basic:string:'},
                {'name': 'subtype', 'type': 'basic:string:'},
                {'name': 'yesno', 'type': 'basic:string:'},
                {'name': 'datalookup', 'type': 'basic:integer:'},
            ],
            run={
                'language': 'bash',
                'program': """
re-save name "{{ input_data | name }}"
re-save id {{ input_data | id }}
re-save type {{ input_data | type }}
re-save basename "{{ '/foo/bar/moo' | basename }}"
re-save subtype "{{ 'data:test:inputobject:' | subtype('data:') }}"
re-save yesno "{{ true | yesno('yes', 'no') }}"
re-save datalookup "{{ 'input-data-object' | data_by_slug }}"
"""
            }

        )
        data = Data.objects.create(
            name='Data object',
            contributor=self.contributor,
            process=process,
            input={'input_data': input_data.pk},
        )

        manager.communicate(verbosity=0)

        # update output
        data = Data.objects.get(pk=data.pk)

        self.assertEqual(data.output['name'], input_data.name)
        self.assertEqual(data.output['id'], input_data.pk)
        self.assertEqual(data.output['type'], input_process.type)
        self.assertEqual(data.output['basename'], 'moo')
        self.assertEqual(data.output['subtype'], 'True')
        self.assertEqual(data.output['yesno'], 'yes')
        self.assertEqual(data.output['datalookup'], input_data.pk)


class ExpressionEngineTest(TestCase):
    def test_jinja_engine(self):
        engine = manager.get_expression_engine('jinja')
        block = engine.evaluate_block('Hello {{ world }}', {'world': 'cruel world'})
        self.assertEqual(block, 'Hello cruel world')
        block = engine.evaluate_block('Hello {% if world %}world{% endif %}', {'world': True})
        self.assertEqual(block, 'Hello world')

        with self.assertRaises(EvaluationError):
            engine.evaluate_block('Hello {% bar')

        expression = engine.evaluate_inline('world', {'world': 'cruel world'})
        self.assertEqual(expression, 'cruel world')
        expression = engine.evaluate_inline('world', {'world': True})
        self.assertEqual(expression, True)
        expression = engine.evaluate_inline('world | yesno("yes", "no")', {'world': False})
        self.assertEqual(expression, 'no')
        expression = engine.evaluate_inline('[1, 2, 3, world]', {'world': 4})
        self.assertEqual(expression, [1, 2, 3, 4])
        expression = engine.evaluate_inline('a.b.c.d', {})
        self.assertEqual(expression, None)
        expression = engine.evaluate_inline('a.b.c().d', {})
        self.assertEqual(expression, None)
        expression = engine.evaluate_inline('a.b.0.d', {'a': {'b': [{'d': 'Hello world'}]}})
        self.assertEqual(expression, 'Hello world')
        expression = engine.evaluate_inline('a.b.0.d', {})
        self.assertEqual(expression, None)

        # Test that propagation of undefined values works.
        expression = engine.evaluate_inline('foo.bar | name | default("bar")', {})
        self.assertEqual(expression, 'bar')
        expression = engine.evaluate_inline('foo.bar | id | default("bar")', {})
        self.assertEqual(expression, 'bar')
        expression = engine.evaluate_inline('foo.bar | type | default("bar")', {})
        self.assertEqual(expression, 'bar')
        expression = engine.evaluate_inline('foo.bar | basename | default("bar")', {})
        self.assertEqual(expression, 'bar')

        # Ensure that filter decorations are correctly copied when decorating filters to
        # automatically propagate undefined values on exceptions.
        expression = engine.evaluate_inline('foo | join(" ")', {'foo': ['a', 'b', 'c']})
        self.assertEqual(expression, 'a b c')
