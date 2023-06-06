class FlattenJsonParser:
    """
            Parser for parsing nested dictionaries. Initialize the parser with optional parameters. And use
            either parse_row to parse a single Dict, or parse_data to parse a list of dicts.

            by default, the parser will parse:

                 [{"nesting_0": "0",
                  "nesting_1": {"nesting_1": "1"},
                  "nesting_2": {"nesting_2": {"nesting_2": "2"}},
                  "nesting_3": {"nesting_3": {"nesting_3": {"nesting_3": "3"}}},
                  "nesting_4": {"nesting_4": {"nesting_4": {"nesting_4": {"nesting_4": "4"}}}}
                  }]

            as:

                 [{"nesting_0": "0",
                 "nesting_1_nesting_1": "1",
                 "nesting_2_nesting_2_nesting_2": "2",
                 "nesting_3_nesting_3_nesting_3": {"nesting_3": "3"},
                 "nesting_4_nesting_4_nesting_4": {"nesting_4": {"nesting_4": "4"}}}]


            Args:
                child_separator: The character that will be used to indicate the parsing of nested dictionaries.
                                 e.g. "address": {"house_number": "1"}  with child_separator set to "#" would be parsed
                                 as "address#house_number": "1"

                max_parsing_depth: The max depth indicates how deep the parser will parse nested dictionaries. After
                                   the max depth is reached, the rest of the nested dict will not be parsed and will
                                   be saved as is. The depth starts as 0, so an input dict of {"name" : "Tom"} is depth
                                   0.  {"address": {"house_number": "1"} } would be depth 1, and so on.

    """

    def __init__(self, child_separator: str = '_', max_parsing_depth=2):
        self.child_separator = child_separator
        self.max_parsing_depth = max_parsing_depth

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

        def _flatten(dict_object, name_with_parent='', current_depth=0):
            if isinstance(dict_object, dict) and current_depth <= self.max_parsing_depth:
                for key in dict_object:
                    new_parent_name = self._construct_key(name_with_parent, self.child_separator, key)
                    new_depth = current_depth + 1
                    _flatten(dict_object[key], name_with_parent=new_parent_name, current_depth=new_depth)
            else:
                flattened_dict[name_with_parent] = dict_object

        _flatten(nested_dict, current_depth=0)
        return flattened_dict
