"""
I/O UTILITIES
"""

import re


def stack_string(a):
    """For generating LaTeX tables"""
    l = re.split(r"[ -]+", a)
    if len(l) == 1:
        return l[0]
    else:
        return f"\\makecell{{{l[0]} \\\\ {l[1]}}}"


### END ###
