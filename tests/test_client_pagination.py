"""
Tests for HubspotClient pagination functionality.
Specifically tests the _get_paged_result_pages method with both:
- Default convention (limit/hasMore) used by campaigns, email events, etc.
- Contact Lists convention (count/has-more) used by /contacts/v1/lists
"""
import unittest
from unittest import mock

from client.client import HubspotClient


class MockResponse:
    """Mock requests.Response for testing."""
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code
        self.reason = "OK"

    def json(self):
        return self._json_data


class TestGetPagedResultPages(unittest.TestCase):
    """Tests for _get_paged_result_pages method."""

    def setUp(self):
        """Set up test fixtures."""
        with mock.patch.object(HubspotClient, '__init__', lambda x, y: None):
            self.client = HubspotClient('fake_token')

    def test_default_convention_single_page(self):
        """Test default convention (limit/hasMore) with single page of results."""
        mock_response = MockResponse({
            'campaigns': [{'id': 1}, {'id': 2}],
            'hasMore': False,
            'offset': 0
        })

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'email/public/v1/campaigns/by-id', {}, 'campaigns'
            ))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], [{'id': 1}, {'id': 2}])
        mock_get.assert_called_once()
        call_params = mock_get.call_args[1]['params']
        self.assertEqual(call_params['limit'], 1000)
        self.assertNotIn('offset', call_params)

    def test_default_convention_multiple_pages(self):
        """Test default convention (limit/hasMore) with multiple pages."""
        captured_params = []

        def capture_params(*args, **kwargs):
            captured_params.append(kwargs['params'].copy())
            if len(captured_params) == 1:
                return MockResponse({
                    'campaigns': [{'id': 1}, {'id': 2}],
                    'hasMore': True,
                    'offset': 'abc123'
                })
            else:
                return MockResponse({
                    'campaigns': [{'id': 3}, {'id': 4}],
                    'hasMore': False,
                    'offset': 'def456'
                })

        with mock.patch.object(self.client, 'get_raw', side_effect=capture_params) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'email/public/v1/campaigns/by-id', {}, 'campaigns'
            ))

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], [{'id': 1}, {'id': 2}])
        self.assertEqual(results[1], [{'id': 3}, {'id': 4}])
        self.assertEqual(mock_get.call_count, 2)

        self.assertEqual(captured_params[0]['limit'], 1000)
        self.assertNotIn('offset', captured_params[0])

        self.assertEqual(captured_params[1]['limit'], 1000)
        self.assertEqual(captured_params[1]['offset'], 'abc123')

    def test_contact_lists_convention_single_page(self):
        """Test Contact Lists convention (count/has-more) with single page."""
        mock_response = MockResponse({
            'lists': [{'listId': 1}, {'listId': 2}],
            'has-more': False,
            'offset': 0
        })

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'contacts/v1/lists/', {}, 'lists',
                limit_param='count', limit=250, has_more_field='has-more'
            ))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], [{'listId': 1}, {'listId': 2}])
        mock_get.assert_called_once()
        call_params = mock_get.call_args[1]['params']
        self.assertEqual(call_params['count'], 250)
        self.assertNotIn('limit', call_params)
        self.assertNotIn('offset', call_params)

    def test_contact_lists_convention_multiple_pages(self):
        """Test Contact Lists convention (count/has-more) with multiple pages."""
        captured_params = []

        def capture_params(*args, **kwargs):
            captured_params.append(kwargs['params'].copy())
            if len(captured_params) == 1:
                return MockResponse({
                    'lists': [{'listId': i} for i in range(1, 251)],
                    'has-more': True,
                    'offset': 250
                })
            elif len(captured_params) == 2:
                return MockResponse({
                    'lists': [{'listId': i} for i in range(251, 501)],
                    'has-more': True,
                    'offset': 500
                })
            else:
                return MockResponse({
                    'lists': [{'listId': i} for i in range(501, 550)],
                    'has-more': False,
                    'offset': 549
                })

        with mock.patch.object(self.client, 'get_raw', side_effect=capture_params) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'contacts/v1/lists/', {}, 'lists',
                limit_param='count', limit=250, has_more_field='has-more'
            ))

        self.assertEqual(len(results), 3)
        self.assertEqual(len(results[0]), 250)
        self.assertEqual(len(results[1]), 250)
        self.assertEqual(len(results[2]), 49)
        self.assertEqual(mock_get.call_count, 3)

        self.assertEqual(captured_params[0]['count'], 250)
        self.assertNotIn('offset', captured_params[0])

        self.assertEqual(captured_params[1]['count'], 250)
        self.assertEqual(captured_params[1]['offset'], 250)

        self.assertEqual(captured_params[2]['count'], 250)
        self.assertEqual(captured_params[2]['offset'], 500)

    def test_preserves_existing_parameters(self):
        """Test that existing parameters in the dict are preserved."""
        mock_response = MockResponse({
            'events': [{'id': 1}],
            'hasMore': False,
            'offset': 0
        })

        initial_params = {'eventType': 'CLICK'}

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'email/public/v1/events', initial_params, 'events'
            ))

        self.assertEqual(len(results), 1)
        call_params = mock_get.call_args[1]['params']
        self.assertEqual(call_params['eventType'], 'CLICK')
        self.assertEqual(call_params['limit'], 1000)

    def test_empty_response(self):
        """Test handling of empty response."""
        mock_response = MockResponse({
            'hasMore': False,
            'offset': 0
        })

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response):
            results = list(self.client._get_paged_result_pages(
                'email/public/v1/campaigns/by-id', {}, 'campaigns'
            ))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], [])

    def test_has_more_false_stops_pagination(self):
        """Test that hasMore=False stops pagination even with offset present."""
        mock_response = MockResponse({
            'campaigns': [{'id': 1}],
            'hasMore': False,
            'offset': 'some_offset'
        })

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'email/public/v1/campaigns/by-id', {}, 'campaigns'
            ))

        self.assertEqual(len(results), 1)
        mock_get.assert_called_once()

    def test_has_more_hyphenated_false_stops_pagination(self):
        """Test that has-more=False stops pagination for contact lists."""
        mock_response = MockResponse({
            'lists': [{'listId': 1}],
            'has-more': False,
            'offset': 'some_offset'
        })

        with mock.patch.object(self.client, 'get_raw', return_value=mock_response) as mock_get:
            results = list(self.client._get_paged_result_pages(
                'contacts/v1/lists/', {}, 'lists',
                limit_param='count', limit=250, has_more_field='has-more'
            ))

        self.assertEqual(len(results), 1)
        mock_get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
