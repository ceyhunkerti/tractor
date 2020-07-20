# -*- coding: utf-8 -*-
import sys
import re
import unicodedata

required = lambda v: v is not None and v != ""


def slugify(string):
    if sys.version_info[0] >= 3:
        unicode = str
    return re.sub(
        r"[-\s]+",
        "-",
        unicode(
            re.sub(
                r"[^\w\s-]",
                "",
                unicodedata.normalize("NFKD", string).encode("ascii", "ignore").decode(),
            )
            .strip()
            .lower()
        ),
    )


def to_delimiter(delim):
    return delim.replace("\\r", "\r").replace("\\n", "\n").replace("\\t", "\t")
