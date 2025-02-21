""" Compatible with python high and low version import """

try:
    from numpy import NaN
except ImportError:
    from numpy import nan as NaN

try:
    from collections import Iterator
except ImportError:
    from collections.abc import Iterator
