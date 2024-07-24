"""Storage Utils."""


def iso8601_to_rfc3339(iso_string):
    """Convert ISO8601 to RFC3339 format."""
    if "+" in iso_string or "-" in iso_string:
        offset_sign = "+" if "+" in iso_string else "-"
        date_time, offset = iso_string.split(offset_sign)
        offset = offset[:2] + ":" + offset[2:]
        return date_time + offset_sign + offset

    return iso_string
