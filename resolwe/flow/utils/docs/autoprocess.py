"""
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

import os
import re
import fnmatch

import six
import yaml

from docutils import nodes
from docutils.parsers.rst import Directive


class AutoProcessDirective(Directive):
    """
    Directive used to automatically document a Resolwe process.
    """
    has_content = True
    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = None

    def filter_processes(self, content, categories, processes_dict):
        """
        Filter processes_dict by the content of the directive

        Content can be:

            * None - if there is no content, all processes are returned.
            * Category - if the content of the directive is a string matching
              an element in categories list, only processes with this
              category will be returned.
            * Process slug - if the content of the directive is a valid
              process slug, only the corresponding process is returned
              (packed in a list)

        :param list content: content of the directive
        :param set categories: set of all categories
        :param dict processes_dict: dict of all source_base_url: processes pairs
        :return: filtered processes_dict
        :rtype: dict
        :raises: NotImplementedError: if more than one argument is passed
        :raises: ValueError: if the given argument is ivalid
        """
        if not content:
            return processes_dict
        elif len(content) > 1:
            raise NotImplementedError("Only one argument can be passed to autoprocess.")
        content = content[0]
        # Make a filtered dict of all process that have slug == content
        filtered_by_slug = {url: proc for url, proc in six.iteritems(processes_dict) if proc['slug'] == content}
        if len(filtered_by_slug) == 1:
            return filtered_by_slug

        elif content in categories:
            filtered_by_category = {
                url: proc for url, proc in six.iteritems(processes_dict)
                if proc.get('category', 'uncategorized') == content}
            return filtered_by_category

        raise ValueError("Invalid argument given to autoprocess directive.")

    def get_process_definition_start(self, fname, slug):
        """
        Find the starting line of process definition - where slug is defined

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

    def get_all_processes(self, process_dir, source_base_url):
        """
        :param str process_dir: Path to the directory where to search for processes
        :param str source_base_url: Base URL of the source code repository with process definitions
        :return: Dictionary of processes where keys are URLs pointing to processes'
        source code and values are processes' definitions parsed from YAML files
        :rtype: dict
        :raises: ValueError: if multiple processes with the same slug are found
        """
        all_process_files = []
        process_file_extensions = ['*.yaml', '*.yml']
        for root, _, filenames in os.walk(process_dir):
            for extension in process_file_extensions:
                for filename in fnmatch.filter(filenames, extension):
                    all_process_files.append(os.path.join(root, filename))

        def read_yaml_file(fname):
            """Read yaml file"""
            with open(fname) as file_:
                return yaml.load(file_)

        all_processes = {}
        for process_file in all_process_files:
            processes_in_file = read_yaml_file(process_file)
            for process in processes_in_file:

                # This section finds the line in file where the
                # defintion of the process starts. (there are
                # multiple process definition in some files).
                startline = self.get_process_definition_start(process_file, process['slug'])

                # Put together URL to starting line of process definition.
                source_url = source_base_url + process_file[len(process_dir) + 1:] + '#L' + str(startline)
                chosen_process = [proc for proc in processes_in_file if proc['slug'] == process['slug']]
                # The scenarion with len(chosen_process) == 0 is not possible
                if len(chosen_process) > 1:
                    raise ValueError("More than one process with same slug: {}".format(process['slug']))

                all_processes[source_url] = chosen_process[0]

        return all_processes

    def get_all_categories(self, process_list):
        """
        Get list of all categories in given process list.

        For processes with undefined category field (yes, there are some!),
        there is "uncategorized" category.

        :param list process_list: list of all processes
        :return: list of all categories in given process list
        :rtype: list
        """
        categories = set()
        for process in process_list:
            categories.add(process.get('category', 'uncategorized'))
        return list(categories)

    def make_field(self, field_name, field_body):
        """
        Handy function for filling content into nodes.field

        :param string field_name: Field name of the field
        :param field_name: Field body if the field
        :type field_name: str or instance of docutils.nodes
        :return: field instance filled with given name and body
        :rtype: nodes.field
        """

        name = nodes.field_name()
        name += nodes.Text(field_name)

        paragraph = nodes.paragraph()
        if isinstance(field_body, six.string_types):
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

    def make_properties_list(self, data):
        """
        Fill the ``data`` into a properties list and return it.

        :param dict data: the content of the property list to make
        :return: field_list instance filled with given data
        :rtype: nodes.field_list
        """
        # Make a copy so the function parameter is not modified.
        data = data.copy()

        properties_list = nodes.field_list()

        # Remove the name (it is already used in field_name...)
        data.pop('name')

        # changing the order of elements in this list affects
        # the order in which they are displayed
        property_names = [
            'label',
            'type',
            'description',
            'required',
            'disabled',
            'hidden',
            'default',
            'placeholder',
            'validate_regex',
            'choices',
            'collapse',
            'group']

        for name in property_names:
            if name in data:
                value = data.pop(name)

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

                # Make a nested list of properties:
                elif name == 'group':
                    list123 = nodes.field_list()
                    for sub_param in value:
                        sub_param_properties_list = self.make_properties_list(sub_param)
                        list123 += self.make_field(sub_param['name'], sub_param_properties_list)

                    properties_list += self.make_field(name, list123)

                else:
                    properties_list += self.make_field(name, str(value))

        return properties_list

    def make_process_node(self, source_base_url, process):
        """
        Fill the content of process definiton node

        :param str source_base_url: url to process definition
        :param dict process: process data as given from yaml.load function
        :return: section instance filled with given content
        :rtype: nodes.field
        """

        name = process['name']

        # Make process name as section title:
        section = nodes.section()
        section['ids'] = [process['slug']]
        title = nodes.subtitle()
        title_text = nodes.Text(name)
        title += title_text
        section += title

        top_field_list = nodes.field_list()

        slug = process['slug']
        version = process['version']
        description = process.get('description', '')
        inputs = process.get('input', [])
        outputs = process.get('output', [])

        # Make hyperlink to process definition:
        para = nodes.paragraph()
        source_reference = nodes.reference('Link', 'Link')
        source_reference['refdocname'] = 'some_random_string'
        source_reference['refuri'] = source_base_url
        para += source_reference

        # Make slug, varsion, description and source_code link:
        names = ['Slug', 'Version', 'Description', 'Source code']
        values = [slug, version, description, para]
        for name, value in zip(names, values):
            top_field_list += self.make_field(name, value)

        # Make inputs section:
        inputs_list = nodes.field_list()
        for input_ in inputs:
            input_properties_list = self.make_properties_list(input_)
            inputs_list += self.make_field(input_['name'], input_properties_list)
        top_field_list += self.make_field('Inputs', inputs_list)

        # Make outputs section:
        outputs_list = nodes.field_list()
        for output in outputs:
            output_properties_list = self.make_properties_list(output)
            outputs_list += self.make_field(output['name'], output_properties_list)
        top_field_list += self.make_field('Outputs', outputs_list)

        section += top_field_list
        return section

    def run(self):
        """
        Return a list of nodes that should be parsed.

        Classes derived from Directive class must implemenent run() method,
        which is the most important method of this class: it returns a list
        of nodes. Nodes are building blocks of the document and are the
        basic internal structure that is later formatted into HTML/LaTeX...

        Several things happen in run method:

            * the content of the directive will be parsed: this will
              determine which processes to document and extract the
              content of *.yaml files into dictionary like object.
              This is done by method ``filter_processes``
            * This dictionary-like object is traversed and desired
              information is extracted: name, slug, id, inputs, outputs...
            * With this information we cunstruct a list of nodes. Nodes are
              representing the hierarchical structure of the document.

        """
        config = self.state.document.settings.env.config

        # Get all processes:
        all_processes_dict = self.get_all_processes(config.autoprocess_process_dir, config.autoprocess_source_base_url)
        all_processes = [val for val in all_processes_dict.values()]

        # Extract all the categories:
        categories = self.get_all_categories(all_processes)

        # This is the list of processes thet need to be documented
        processes_to_document = self.filter_processes(self.content, categories, all_processes_dict)

        if len(processes_to_document) == 1:
            # Only one process:
            source_url, process = list(processes_to_document.items())[0]
            top_node = self.make_process_node(source_url, process)
        elif len(processes_to_document) > 1:
            # A category of processes, make a process category section:
            process1 = list(processes_to_document.items())[0][1]
            category = process1.get('category', 'uncategorized')
            title = nodes.title()
            title += nodes.Text("Category: " + str(category))
            top_node = nodes.section()
            top_node['ids'] = [category]
            top_node += title

            # Make sorting:
            pairs = sorted(processes_to_document.items(), key=lambda pair: pair[1]['name'])
            for source_url, process in pairs:
                process_node = self.make_process_node(source_url, process)
                top_node += process_node
        else:
            raise ValueError("No processes to documement.")

        return [top_node]


def setup(app):
    """
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

    app.add_directive('autoprocess', AutoProcessDirective)

    # The setup() function can return a dictionary. This is treated by
    # Sphinx as metadata of the extension:
    return {'version': '0.1'}
