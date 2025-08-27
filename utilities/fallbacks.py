"""
Shared fallback implementations for optional dependencies
"""

from contextlib import contextmanager
import sys
from io import StringIO


@contextmanager
def capture_output():
    """Fallback implementation of IPython's capture_output"""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout = StringIO()
    stderr = StringIO()
    try:
        sys.stdout = stdout
        sys.stderr = stderr
        class CaptureResult:
            def __init__(self):
                self.stdout = stdout.getvalue()
                self.stderr = stderr.getvalue()
        yield CaptureResult()
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


def tqdm(iterable, *args, **kwargs):
    """Fallback tqdm that just passes through the iterable"""
    return iterable
