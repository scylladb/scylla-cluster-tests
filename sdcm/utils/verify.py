from difflib import unified_diff
from io import StringIO
from pathlib import Path

import click


class Fix(StringIO):
    ERROR = False

    def __init__(self, path, *args, **kwargs):
        super().__init__("")
        self.path = path

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
            self.__class__.ERROR = True
            with open(self.path, "w") as file:
                file.write(new)
