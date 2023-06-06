'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from freezegun import freeze_time

from json_parser import FlattenJsonParser


class TestParser(unittest.TestCase):

    def test_nesting(self):
        users = [
            {
                'name': 'John Doe',
                "nesting_0": "0",
                "nesting_1": {"nesting_1": "1"},
                "nesting_2": {"nesting_2": {"nesting_2": "2"}},
                "nesting_3": {"nesting_3": {"nesting_3": {"nesting_3": "3"}}},
                "nesting_4": {"nesting_4": {"nesting_4": {"nesting_4": {"nesting_4": "4"}}}},
                'address': {
                    'street': '123 Main St',
                    'city': 'Anytown',
                    'state': 'CA',
                    'zip': '12345'
                },
                'preferences': {
                    'color': 'blue',
                    'food': 'pizza',
                    'hobby': 'reading',
                    'email_preferences': {
                        'notify_on': ['new_message', 'newsletter']
                    }}
            }
        ]
        expected_parsed_data = [
            {"name": "John Doe",
             "nesting_0": "0",
             "nesting_1_nesting_1": "1",
             "nesting_2_nesting_2_nesting_2": "2",
             "nesting_3_nesting_3_nesting_3_nesting_3": "3",
             "nesting_4_nesting_4_nesting_4_nesting_4": {"nesting_4": "4"},
             "address_street": "123 Main St",
             "address_city": "Anytown",
             "address_state": "CA",
             "address_zip": "12345",
             "preferences_color": "blue",
             "preferences_food": "pizza",
             "preferences_hobby": "reading",
             "preferences_email_preferences_notify_on": ["new_message", "newsletter"]}]
        parser = FlattenJsonParser(max_parsing_depth=3)

        parsed_data = parser.parse_data(users)
        self.assertEqual(parsed_data, expected_parsed_data)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
