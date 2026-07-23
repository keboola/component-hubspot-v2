'''
Created on 12. 11. 2018

@author: esner
'''
import json
import os
import tempfile
import unittest

import mock
from freezegun import freeze_time
from keboola.component.exceptions import UserException

from component import Component


def _make_datadir(parameters: dict) -> str:
    """Create a minimal Keboola data dir with the given config parameters."""
    data_dir = tempfile.mkdtemp()
    os.makedirs(os.path.join(data_dir, "in", "tables"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, "out", "tables"), exist_ok=True)
    with open(os.path.join(data_dir, "config.json"), "w") as config_file:
        json.dump({"parameters": parameters}, config_file)
    return data_dir


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_invalid_association_object_raises_user_exception(self):
        # Regression: an association referencing an unsupported HubSpot object (e.g. a custom object
        # type id like "2-138922103") used to crash config parsing with a bare ValueError, which the
        # platform surfaced as an opaque internal error (exit 2). It must now be a UserException
        # (exit 1) whose message names the offending value.
        parameters = {
            "#private_app_token": "secret",
            "endpoints": {"contact": True, "custom_object": True},
            "additional_properties": {"custom_object_types": ["2-138922103"]},
            "associations": [{"from_object": "contact", "to_object": "2-138922103"}],
            "fetch_settings": {},
            "destination_settings": {},
        }
        with mock.patch.dict(os.environ, {'KBC_DATADIR': _make_datadir(parameters)}):
            comp = Component()
            with self.assertRaises(UserException) as ctx:
                comp._init_configuration()
        self.assertIn("2-138922103", str(ctx.exception))

    def test_valid_configuration_loads_unchanged(self):
        # Happy path: a configuration with only supported enum values must still load successfully
        # (no behaviour change introduced by the defensive ValueError handling).
        parameters = {
            "#private_app_token": "secret",
            "endpoints": {"contact": True, "company": True},
            "additional_properties": {},
            "associations": [{"from_object": "contact", "to_object": "company"}],
            "fetch_settings": {},
            "destination_settings": {},
        }
        with mock.patch.dict(os.environ, {'KBC_DATADIR': _make_datadir(parameters)}):
            comp = Component()
            comp._init_configuration()
        association = comp._configuration.associations[0]
        self.assertEqual("contact", association.from_object.value)
        self.assertEqual("company", association.to_object.value)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
