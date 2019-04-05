""".. Ignore pydocstyle D400.

============================
Documentation from Processes
============================

Sphinx extension for semi-automatic documentation of Resolwe processes.

This module introduces a new directive that enables user to document a
set of processes. The directive can be used in three variations::

    .. autoprocess::

    .. autoprocess:: category_name

    .. autoprocess:: process_slug

The first option documents *all* processes.

The second option documents just the ones that have *category* equal to
``category_name``. This means that subcategories (e.g. ``analyses:alignment``,
``analyses:variants``) of a category (e.g. ``analyses``) will not be documented.

Third option documents only one process: the one with *slug* equal to
``process_slug``.

"""

import fnmatch
import os
import re
from itertools import groupby
from operator import itemgetter

import yaml
from docutils import nodes
from docutils.parsers.rst import Directive
from sphinx import addnodes

from resolwe.flow.utils import iterate_schema

PROCESS_CACHE = None


def get_process_definition_start(fname, slug):
    """Find the first line of process definition.

    The first line of process definition is the line with a slug.

    :param str fname: Path to filename with processes
    :param string slug: process slug
    :return: line where the process definiton starts
    :rtype: int

    """
    with open(fname) as file_:
        for i, line in enumerate(file_):
            if re.search(r'slug:\s*{}'.format(slug), line):
                return i + 1
    # In case starting line is not found just return first line
    return 1


def get_processes(process_dir, base_source_uri):
    """Find processes in path.

    :param str process_dir: Path to the directory where to search for processes
    :param str base_source_uri: Base URL of the source code repository with process definitions
    :return: Dictionary of processes where keys are URLs pointing to processes'
    source code and values are processes' definitions parsed from YAML files
    :rtype: dict
    :raises: ValueError: if multiple processes with the same slug are found

    """
    global PROCESS_CACHE  # pylint: disable=global-statement
    if PROCESS_CACHE is not None:
        return PROCESS_CACHE

    all_process_files = []
    process_file_extensions = ['*.yaml', '*.yml']
    for root, _, filenames in os.walk(process_dir):
        for extension in process_file_extensions:
            for filename in fnmatch.filter(filenames, extension):
                all_process_files.append(os.path.join(root, filename))

    def read_yaml_file(fname):
        """Read the yaml file."""
        with open(fname) as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    processes = []
    for process_file in all_process_files:
        processes_in_file = read_yaml_file(process_file)
        for process in processes_in_file:
            # This section finds the line in file where the
            # defintion of the process starts. (there are
            # multiple process definition in some files).
            startline = get_process_definition_start(process_file, process['slug'])

            # Put together URL to starting line of process definition.
            process['source_uri'] = base_source_uri + process_file[len(process_dir) + 1:] + '#L' + str(startline)

            if 'category' not in process:
                process['category'] = 'uncategorized'

            processes.append(process)

    PROCESS_CACHE = processes
    return processes


class AutoProcessDirective(Directive):
    """Automatically document Resolwe processes."""

    has_content = True
    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = None

    def make_field(self, field_name, field_body):
        """Fill content into nodes.

        :param string field_name: Field name of the field
        :param field_name: Field body if the field
        :type field_name: str or instance of docutils.nodes
        :return: field instance filled with given name and body
        :rtype: nodes.field

        """
        name = nodes.field_name()
        name += nodes.Text(field_name)

        paragraph = nodes.paragraph()
        if isinstance(field_body, str):
            # This is the case when field_body is just a string:
            paragraph += nodes.Text(field_body)
        else:
            # This is the case when field_body is a complex node:
            # useful when constructing nested field lists
            paragraph += field_body

        body = nodes.field_body()
        body += paragraph

        field = nodes.field()
        field.extend([name, body])
        return field

    def make_properties_list(self, field):
        """Fill the ``field`` into a properties list and return it.

        :param dict field: the content of the property list to make
        :return: field_list instance filled with given field
        :rtype: nodes.field_list

        """
        properties_list = nodes.field_list()

        # changing the order of elements in this list affects
        # the order in which they are displayed
        property_names = ['label', 'type', 'description', 'required',
                          'disabled', 'hidden', 'default', 'placeholder',
                          'validate_regex', 'choices', 'collapse', 'group']

        for name in property_names:
            if name not in field:
                continue

            value = field[name]

            # Value should be formatted in code-style (=literal) mode
            if name in ['type', 'default', 'placeholder', 'validate_regex']:
                literal_node = nodes.literal(str(value), str(value))
                properties_list += self.make_field(name, literal_node)

            # Special formating of ``value`` is needed if name == 'choices'
            elif name == 'choices':
                bullet_list = nodes.bullet_list()
                for choice in value:
                    label = nodes.Text(choice['label'] + ': ')
                    val = nodes.literal(choice['value'], choice['value'])

                    paragraph = nodes.paragraph()
                    paragraph += label
                    paragraph += val
                    list_item = nodes.list_item()
                    list_item += paragraph
                    bullet_list += list_item

                properties_list += self.make_field(name, bullet_list)

            else:
                properties_list += self.make_field(name, str(value))

        return properties_list

    def make_process_header(self, slug, typ, version, source_uri, description, inputs):
        """Generate a process definition header.

        :param str slug: process' slug
        :param str typ: process' type
        :param str version:  process' version
        :param str source_uri: url to the process definition
        :param str description: process' description
        :param dict inputs: process' inputs

        """
        node = addnodes.desc()
        signode = addnodes.desc_signature(slug, '')
        node.append(signode)

        node['objtype'] = node['desctype'] = typ

        signode += addnodes.desc_annotation(typ, typ, classes=['process-type'])
        signode += addnodes.desc_addname('', '')
        signode += addnodes.desc_name(slug + ' ', slug + ' ')

        paramlist = addnodes.desc_parameterlist()

        for field_schema, _, _ in iterate_schema({}, inputs, ''):
            field_type = field_schema['type']
            field_name = field_schema['name']

            field_default = field_schema.get('default', None)
            field_default = '' if field_default is None else '={}'.format(field_default)

            param = addnodes.desc_parameter('', '', noemph=True)
            param += nodes.emphasis(field_type, field_type, classes=['process-type'])
            # separate by non-breaking space in the output
            param += nodes.strong(text='\xa0\xa0' + field_name)

            paramlist += param

        signode += paramlist
        signode += nodes.reference('', nodes.Text('[Source: v{}]'.format(version)),
                                   refuri=source_uri, classes=['viewcode-link'])

        desc = nodes.paragraph()
        desc += nodes.Text(description, description)

        return [node, desc]

    def make_process_node(self, process):
        """Fill the content of process definiton node.

        :param dict process: process data as given from yaml.load function
        :return: process node

        """
        name = process['name']
        slug = process['slug']
        typ = process['type']
        version = process['version']
        description = process.get('description', '')
        source_uri = process['source_uri']
        inputs = process.get('input', [])
        outputs = process.get('output', [])

        # Make process name a section title:
        section = nodes.section(ids=['process-' + slug])
        section += nodes.title(name, name)

        # Make process header:
        section += self.make_process_header(slug, typ, version, source_uri, description, inputs)

        # Make inputs section:
        container_node = nodes.container(classes=['toggle'])
        container_header = nodes.paragraph(classes=['header'])
        container_header += nodes.strong(text='Input arguments')
        container_node += container_header

        container_body = nodes.container()
        for field_schema, _, path in iterate_schema({}, inputs, ''):
            container_body += nodes.strong(text=path)
            container_body += self.make_properties_list(field_schema)

        container_node += container_body
        section += container_node

        # Make outputs section:
        container_node = nodes.container(classes=['toggle'])
        container_header = nodes.paragraph(classes=['header'])
        container_header += nodes.strong(text='Output results')
        container_node += container_header

        container_body = nodes.container()
        for field_schema, _, path in iterate_schema({}, outputs, ''):
            container_body += nodes.strong(text=path)
            container_body += self.make_properties_list(field_schema)

        container_node += container_body
        section += container_node

        return [section, addnodes.index(entries=[('single', name, 'process-' + slug, '', None)])]

    def run(self):
        """Create a list of process definitions."""
        config = self.state.document.settings.env.config

        # Get all processes:
        processes = get_processes(config.autoprocess_process_dir, config.autoprocess_source_base_url)
        process_nodes = []

        for process in sorted(processes, key=itemgetter('name')):
            process_nodes.extend(self.make_process_node(process))

        return process_nodes


class AutoProcessCategoryDirective(Directive):
    """Automatically document process categories."""

    has_content = True
    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = None

    def run(self):
        """Create a category tree."""
        config = self.state.document.settings.env.config

        # Group processes by category
        processes = get_processes(config.autoprocess_process_dir, config.autoprocess_source_base_url)
        processes.sort(key=itemgetter('category'))
        categorized_processes = {k: list(g) for k, g in groupby(processes, itemgetter('category'))}

        # Build category tree
        category_sections = {'': nodes.container(ids=['categories'])}
        top_categories = []

        for category in sorted(categorized_processes.keys()):
            category_path = ''

            for category_node in category.split(':'):
                parent_category_path = category_path
                category_path += '{}:'.format(category_node)

                if category_path in category_sections:
                    continue

                category_name = category_node.capitalize()

                section = nodes.section(ids=['category-' + category_node])
                section += nodes.title(category_name, category_name)

                # Add process list
                category_key = category_path[:-1]
                if category_key in categorized_processes:
                    listnode = nodes.bullet_list()
                    section += listnode

                    for process in categorized_processes[category_key]:
                        par = nodes.paragraph()

                        node = nodes.reference('', process['name'], internal=True)
                        node['refuri'] = config.autoprocess_definitions_uri + '#process-' + process['slug']
                        node['reftitle'] = process['name']

                        par += node
                        listnode += nodes.list_item('', par)

                category_sections[parent_category_path] += section
                category_sections[category_path] = section

                if parent_category_path == '':
                    top_categories.append(section)

        # Return top sections only
        return top_categories


class AutoProcessTypesDirective(Directive):
    """Automatically document process types."""

    has_content = True
    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = None

    def run(self):
        """Create a type list."""
        config = self.state.document.settings.env.config

        # Group processes by category
        processes = get_processes(config.autoprocess_process_dir, config.autoprocess_source_base_url)
        processes.sort(key=itemgetter('type'))
        processes_by_types = {k: list(g) for k, g in groupby(processes, itemgetter('type'))}

        listnode = nodes.bullet_list()

        for typ in sorted(processes_by_types.keys()):
            par = nodes.paragraph()
            par += nodes.literal(typ, typ)
            par += nodes.Text(' - ')

            processes = sorted(processes_by_types[typ], key=itemgetter('name'))
            last_process = processes[-1]
            for process in processes:
                node = nodes.reference('', process['name'], internal=True)
                node['refuri'] = config.autoprocess_definitions_uri + '#process-' + process['slug']
                node['reftitle'] = process['name']
                par += node
                if process != last_process:
                    par += nodes.Text(', ')

            listnode += nodes.list_item('', par)

        return [listnode]


def setup(app):
    """Register directives.

    When sphinx loads the extension (= imports the extension module) it
    also executes the setup() function. Setup is the way extension
    informs Sphinx about everything that the extension enables: which
    config_values are introduced, which custom nodes/directives/roles
    and which events are defined in extension.

    In this case, only one new directive is created. All used nodes are
    constructed from already existing nodes in docutils.nodes package.

    """
    app.add_config_value('autoprocess_process_dir', '', 'env')
    app.add_config_value('autoprocess_source_base_url', '', 'env')
    app.add_config_value('autoprocess_definitions_uri', '', 'env')

    app.add_directive('autoprocess', AutoProcessDirective)
    app.add_directive('autoprocesscategory', AutoProcessCategoryDirective)
    app.add_directive('autoprocesstype', AutoProcessTypesDirective)

    # The setup() function can return a dictionary. This is treated by
    # Sphinx as metadata of the extension:
    return {'version': '0.2'}
