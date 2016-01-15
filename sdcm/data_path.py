import os
import sys

_BASE_DIR = os.path.join(sys.modules[__name__].__file__, "..", "..")
_BASE_DIR = os.path.abspath(_BASE_DIR)


def get_data_path(*args):
    return os.path.join(_BASE_DIR, "data_dir", *args)
