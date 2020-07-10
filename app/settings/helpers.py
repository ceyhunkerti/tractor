
def parse_boolean(val):
    """Takes a string and returns the equivalent as a boolean value."""
    val = val.strip().lower()
    if val in ("yes", "true", "on", "1"):
        return True

    if val in ("no", "false", "off", "0", "none"):
        return False

    raise ValueError("Invalid boolean value %r" % val)

def array_from_string(val):
    array = val.split(",")
    if "" in array:
        array.remove("")

    return array
