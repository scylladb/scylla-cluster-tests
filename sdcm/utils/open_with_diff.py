from difflib import unified_diff
from io import StringIO
from pathlib import Path

import click


class OpenWithDiff(StringIO):
    """
    A StringIO subclass that verifies the existing contents of a file and outputs the diff if there are changes.

    Original open() or StringIO do not have mechanism to report back,
     the error_carrier parameters is used to carry "error" state when the file changes.
    """

    def __init__(self, path, *args, error_carrier=None, **kwargs):
        super().__init__("")
        self.path = path
        self.error_carrier = error_carrier

    def close(self):
        new = self.getvalue()
        super().close()
        old_path = Path(self.path)
        if old_path.exists():
            old = old_path.read_text()
        else:
            old = ""
        diff = unified_diff(old.splitlines(keepends=True), new.splitlines(keepends=True), fromfile=str(self.path))
        output = "".join(diff)
        if len(output) > 0:
            click.secho(output, fg="red")
            if self.error_carrier:
                self.error_carrier.error = True
            with open(self.path, "w") as file:
                file.write(new)


class ErrorCarrier:
    """Object which carries error state and is mutable"""

    def __init__(self):
        super().__init__()
        self.error = False

    def __bool__(self):
        return self.error
