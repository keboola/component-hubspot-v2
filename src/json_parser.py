class FlattenJsonParser:
    def __init__(self, child_separator: str = '_'):
        self.child_separator = child_separator

    def parse_data(self, data):
        for i, row in enumerate(data):
            data[i] = self._flatten_row(row)
        return data

    def parse_row(self, row: dict):
        return self._flatten_row(row)

    @staticmethod
    def _construct_key(parent_key, separator, child_key):
        return "".join([parent_key, separator, child_key]) if parent_key else child_key

    def _flatten_row(self, nested_dict):
        if len(nested_dict) == 0:
            return {}
        flattened_dict = {}

        def _flatten(dict_object, name_with_parent=''):
            if isinstance(dict_object, dict):
                for key in dict_object:
                    new_parent_name = self._construct_key(name_with_parent, self.child_separator, key)
                    _flatten(dict_object[key], name_with_parent=new_parent_name)
            else:
                flattened_dict[name_with_parent] = dict_object

        _flatten(nested_dict)
        return flattened_dict
