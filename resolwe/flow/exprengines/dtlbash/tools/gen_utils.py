"""
Library of utility functions for writing Genesis' processors.
"""

import json


def gen_save(key, value):
    """Convert the given parameters to a JSON object.

    JSON object is of the form:
    { key: value },
    where value can represent any JSON object.

    """
    value = value.replace("\n", " ")
    try:
        value_json = json.loads(value)
    except ValueError:
        # try putting the value into a string
        value_json = json.loads('"{}"'.format(value))
    return json.dumps({key: value_json})


def gen_save_file(key, file_name, *refs):
    """Convert the given parameters to a special JSON object.

    JSON object is of the form:
    { key: {"file": file_name}}, or
    { key: {"file": file_name, "refs": [refs[0], refs[1], ... ]}} if refs are
    given.

    """
    result = {key: {"file": file_name}}
    if refs:
        result[key]["refs"] = refs
    return json.dumps(result)


def gen_warning(value):
    """Call ``gen_save`` function with "proc.warning" as key."""
    return gen_save('proc.warning', value)


def gen_error(value):
    """Call ``gen_save`` function with "proc.error" as key."""
    return gen_save('proc.error', value)


def gen_info(value):
    """Call ``gen_save`` function with "proc.info" as key."""
    return gen_save('proc.info', value)


def gen_progress(progress):
    """Convert given progress to a JSON object.

    Check that progress can be represented as float between 0 and 1 and
    return it in JSON of the form:

        {"proc.progress": progress}

    """
    if isinstance(progress, str):
        progress = json.loads(progress)
    if not isinstance(progress, float) and not isinstance(progress, int):
        raise ValueError('Progress must be float.')
    if not 0 <= float(progress) <= 1:
        raise ValueError('Progress must be float between 0 and 1.')
    return json.dumps({'proc.progress': progress})


def gen_checkrc(rc, *args):
    """Check if ``rc`` (return code) meets requirements.

    Check if ``rc`` is 0 or is in ``args`` list that contains
    acceptable return codes.
    Last argument of ``args`` can optionally be error message that
    is printed if ``rc`` doesn't meet requirements.

    Output is JOSN of the form:

        {"proc.rc": <rc>,
         "proc.error": "<error_msg>"},

    where "proc.error" entry is omitted if empty.

    """
    rc = int(rc)
    acceptable_rcs = []
    error_msg = ""

    if len(args):
        for code in args[:-1]:
            try:
                acceptable_rcs.append(int(code))
            except ValueError:
                ValueError('Return codes must be integers.')

        try:
            acceptable_rcs.append(int(args[-1]))
        except ValueError:
            error_msg = args[-1]

    if rc in acceptable_rcs:
        rc = 0

    ret = {'proc.rc': rc}
    if rc and error_msg:
        ret['proc.error'] = error_msg

    return json.dumps(ret)
