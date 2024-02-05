"""Resolwe models reference utils."""

from resolwe.flow.utils import iterate_fields


def referenced_schema_files(fields, schema):
    """Get the list of files and directories references by fields.

    :return: tuple of lists, first list containing  files and
        directories refereced in data.output.
    :rtype: Tuple[List[str], List[str]]
    """
    refs = []
    for field_schema, fields in iterate_fields(fields, schema):
        if "type" in field_schema:
            field_type = field_schema["type"]
            field_name = field_schema["name"]

            # Add basic:file: entries
            if field_type.startswith("basic:file:"):
                refs.append(fields[field_name]["file"])
                refs += fields[field_name].get("refs", [])

            # Add list:basic:file: entries
            elif field_type.startswith("list:basic:file:"):
                for field in fields[field_name]:
                    refs.append(field["file"])
                    refs += field.get("refs", [])

            # Add basic:dir: entries
            elif field_type.startswith("basic:dir:"):
                refs.append(fields[field_name]["dir"])
                refs += fields[field_name].get("refs", [])

            # Add list:basic:dir: entries
            elif field_type.startswith("list:basic:dir:"):
                for field in fields[field_name]:
                    refs.append(field["dir"])
                    refs += field.get("refs", [])
    return refs


def referenced_files(data, include_descriptor=True):
    """Get the list of files and directories referenced by the data object.

    :param data: given data object.
    :type data: resolwe.flow.models.Data

    :param include_descriptor: include files referenced in descriptor schema.
    :type include_descriptor: bool

    :return: referenced files and directories.
    :rtype: List[str]
    """
    special_files = ["jsonout.txt", "stdout.txt"]

    output = data.output
    output_schema = data.process.output_schema

    descriptor = data.descriptor
    descriptor_schema = getattr(data.descriptor_schema, "schema", [])

    output_files = referenced_schema_files(output, output_schema)

    descriptor_files = (
        referenced_schema_files(descriptor, descriptor_schema)
        if include_descriptor
        else []
    )

    return special_files + output_files + descriptor_files
