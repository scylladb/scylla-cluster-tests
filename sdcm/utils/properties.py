from typing import Union, TextIO


class PropertiesDict(dict):
    """
    Class that represents properties file of the following format:
        <key>=<value>
        #Some comment
        <key>=<value>
    """

    all_items = dict.items
    all_keys = dict.keys
    all_values = dict.values

    def items(self):
        for key, value in super().items():
            if key.lstrip()[0] != "#":
                yield key, value

    def keys(self):
        for key in super().keys():
            if key.lstrip()[0] != "#":
                yield key

    def values(self):
        for _, value in self.items():
            yield value


def serialize(data: Union[dict, PropertiesDict]) -> str:
    output = []
    items = data.all_items() if isinstance(data, PropertiesDict) else data.items()
    for key, value in items:
        if value is None:
            output.append(key)
            continue
        if " " in value:
            output.append(f'{key} = "{value}"')
        else:
            output.append(f"{key} = {value}")
    return "\n".join(output)


def deserialize(data: Union[str, TextIO]) -> PropertiesDict:
    if not isinstance(data, str):
        data = data.read()
    output = PropertiesDict()
    for line in data.splitlines():
        if not line.strip() or line.lstrip()[0] == "#":
            output[line] = None
            continue
        line_splitted = line.split("=", 1)
        if len(line_splitted) == 2:
            value = line_splitted[1]
            comment_pos = value.find("#")
            if comment_pos >= 0:
                value = value[0:value]
            output[line_splitted[0].strip()] = value.strip().strip('"').strip("'")
    return output
