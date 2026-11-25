import os
import unittest
from unittest.mock import patch, MagicMock
import datetime
import pytz

os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8081"
os.environ["TELEGRAM_TOKEN"] = "test_token"

import datastore
import rating_api
import handlers
import main


class TestPrewarmingAndBatching(unittest.TestCase):
    def setUp(self):
        self.mock_store = {}
        self.mock_client = MagicMock()

        def mock_key(kind, name):
            m = MagicMock()
            m.kind = kind
            m.name = str(name)
            return m

        def mock_get(key):
            k_str = f"{key.kind}:{key.name}"
            return self.mock_store.get(k_str)

        def mock_get_multi(keys):
            return [self.mock_store.get(f"{k.kind}:{k.name}") for k in keys]

        def mock_put(entity):
            k_str = f"{entity.key.kind}:{entity.key.name}"
            self.mock_store[k_str] = dict(entity)

        self.mock_client.key.side_effect = mock_key
        self.mock_client.get.side_effect = mock_get
        self.mock_client.get_multi.side_effect = mock_get_multi
        self.mock_client.put.side_effect = mock_put

    @patch("datastore.get_datastore_client")
    def test_warmup_endpoint(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client
        client = main.app.test_client()
        resp = client.get("/_ah/warmup")
        self.assertEqual(resp.status_code, 200)

    @patch("datastore.get_datastore_client")
    def test_get_cached_venues_multi(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        # Seed cached venue
        v1 = MagicMock()
        v1.key = self.mock_client.key("CachedVenue", "101")
        v1.get.side_effect = lambda k, default=None: {
            "venue_id": 101,
            "name": "Площадка 101",
            "town": "Москва",
            "cached_at": datetime.datetime.now(pytz.utc)
        }.get(k, default)
        self.mock_store["CachedVenue:101"] = v1

        res = datastore.get_cached_venues_multi([101, 102])
        self.assertIn("101", res)
        self.assertNotIn("102", res)
        self.assertEqual(res["101"]["name"], "Площадка 101")

    @patch("datastore.get_datastore_client")
    def test_get_chat_configs_multi(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        c1 = MagicMock()
        c1.key = self.mock_client.key("ChatConfig", "-1001")
        c1.get.side_effect = lambda k, default=None: {
            "chat_id": -1001,
            "chat_title": "Супер Чат",
            "venues": ["55"]
        }.get(k, default)
        self.mock_store["ChatConfig:-1001"] = c1

        res = datastore.get_chat_configs_multi([-1001, -1002])
        self.assertIn("-1001", res)
        self.assertNotIn("-1002", res)
        self.assertEqual(res["-1001"]["chat_title"], "Супер Чат")

    @patch("datastore.get_datastore_client")
    @patch("rating_api.get_venue_by_id")
    @patch("helpers.trigger_venue_sync")
    def test_prewarm_miniapp_caches(self, mock_sync, mock_get_venue, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        monitored = {"501": ["-100111"]}
        active_regs = [
            {
                "sync_req_id": "s1",
                "chat_id": -100111,
                "venue_id": None,  # Will be resolved
                "tourn_name": "Тест"
            }
        ]

        mock_get_venue.return_value = {"id": 501, "name": "Тестовая", "town": "Город"}

        handlers.prewarm_miniapp_caches(monitored, active_regs)

        # Verified venue_id is backfilled
        self.assertEqual(active_regs[0]["venue_id"], 501)
        # Verified get_venue_by_id was called to ensure cache freshness
        mock_get_venue.assert_called_with(501)
        # Verified trigger_venue_sync was called because VenueData wasn't pre-existing
        mock_sync.assert_called_with(501)


if __name__ == "__main__":
    unittest.main()
