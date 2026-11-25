import os
import unittest
from unittest.mock import patch, MagicMock
import datetime
import pytz

# Configure test environment
os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8081"
os.environ["TELEGRAM_TOKEN"] = "test_token"

import datastore
import rating_api
import main


class TestVenueDisplayAndCaching(unittest.TestCase):
    def setUp(self):
        # In-memory store for mocked Datastore entities
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

        def mock_put(entity):
            k_str = f"{entity.key.kind}:{entity.key.name}"
            self.mock_store[k_str] = dict(entity)

        self.mock_client.key.side_effect = mock_key
        self.mock_client.get.side_effect = mock_get
        self.mock_client.put.side_effect = mock_put

    @patch("datastore.get_datastore_client")
    def test_cache_and_get_venue(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        # 1. Cache empty or non-existent
        self.assertIsNone(datastore.get_cached_venue(1234))

        # 2. Cache a venue
        sample_venue_data = {
            "id": 1234,
            "name": "Берлинский клуб ЧГК",
            "town": {"id": 100, "name": "Берлин"},
        }
        datastore.cache_venue(1234, sample_venue_data)

        # 3. Retrieve cached venue
        cached = datastore.get_cached_venue(1234)
        self.assertIsNotNone(cached)
        self.assertEqual(cached.get("name"), "Берлинский клуб ЧГК")
        self.assertEqual(cached.get("town"), "Берлин")

        # 4. Cache a venue with large payload (> 1500 bytes)
        large_venue_data = {
            "id": 3053,
            "name": "Большой клуб",
            "town": "Город",
            "description": "x" * 3000
        }
        datastore.cache_venue(3053, large_venue_data)
        cached_large = datastore.get_cached_venue(3053)
        self.assertIsNotNone(cached_large)
        self.assertEqual(cached_large.get("name"), "Большой клуб")

    @patch("datastore.get_cached_venue")
    @patch("datastore.cache_venue")
    @patch("requests.get")
    def test_rating_api_get_venue_by_id(self, mock_requests_get, mock_cache_venue, mock_get_cached):
        # Case A: Cache hit
        mock_get_cached.return_value = {
            "venue_id": 555,
            "name": "Мюнхен Паб",
            "town": "Мюнхен",
        }
        res = rating_api.get_venue_by_id(555)
        self.assertEqual(res["name"], "Мюнхен Паб")
        self.assertEqual(res["town"], "Мюнхен")
        mock_requests_get.assert_not_called()

        # Case B: Cache miss -> API call
        mock_get_cached.return_value = None
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "id": 777,
            "name": "Франкфурт ЧГК",
            "town": {"id": 200, "name": "Франкфурт-на-Майне"},
            "address": "Mainzer Landstr. 10"
        }
        mock_requests_get.return_value = mock_resp

        res2 = rating_api.get_venue_by_id(777)
        self.assertEqual(res2["id"], 777)
        self.assertEqual(res2["name"], "Франкфурт ЧГК")
        self.assertEqual(res2["town"], "Франкфурт-на-Майне")
        mock_cache_venue.assert_called_once()

        # Case C: Non-numeric venue ID (e.g. "RU") -> skips requests.get entirely
        mock_requests_get.reset_mock()
        res3 = rating_api.get_venue_by_id("RU")
        self.assertIsNone(res3)
        mock_requests_get.assert_not_called()

    @patch("datastore.get_datastore_client")
    def test_add_team_registration_saves_venue_id(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        reg = datastore.add_team_registration(
            chat_id=-1001234567,
            thread_id=None,
            sync_req_id="sync_999",
            message_id=42,
            tourn_name="Кубок Городов",
            venue_id=1234
        )
        self.assertEqual(reg["venue_id"], 1234)
        saved = self.mock_store.get("TeamRegistration:-1001234567_sync_999")
        self.assertIsNotNone(saved)
        self.assertEqual(saved.get("venue_id"), 1234)

    @patch("main.get_authenticated_user")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.get_chat_config")
    @patch("datastore.get_chat_title")
    @patch("rating_api.get_venue_by_id")
    @patch("datastore.mark_user_bot_started")
    def test_miniapp_init_includes_venue_and_chat_title(
        self,
        mock_bot_started,
        mock_get_venue_by_id,
        mock_get_chat_title,
        mock_get_chat_config,
        mock_get_monitored_venues,
        mock_get_all_active,
        mock_auth_user
    ):
        mock_auth_user.return_value = ({"username": "admin_user"}, 111111, True)
        mock_get_all_active.return_value = [
            {
                "sync_req_id": "req_1",
                "chat_id": -100111,
                "venue_id": 501,
                "tourn_name": "Берлинский синхрон",
                "teams": []
            },
            {
                "sync_req_id": "req_2",
                "chat_id": -100222,
                "venue_id": None,  # Will be resolved via monitored_venues
                "tourn_name": "Франкфуртский синхрон",
                "teams": []
            }
        ]
        mock_get_monitored_venues.return_value = {
            "602": ["-100222"]
        }
        mock_get_chat_config.return_value = {"venues": ["602"], "collect_rosters": True}
        mock_get_chat_title.side_effect = lambda cid: "ЧГК Берлин" if cid == -100111 else "ЧГК Франкфурт"

        def fake_venue(vid):
            if vid == 501:
                return {"id": 501, "name": "Берлин", "town": "Берлин"}
            if vid == 602:
                return {"id": 602, "name": "Франкфурт", "town": "Франкфурт"}
            return None

        mock_get_venue_by_id.side_effect = fake_venue

        client = main.app.test_client()
        resp = client.post("/api/miniapp/init", json={"initData": "dummy"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()

        tournaments = data.get("tournaments", [])
        self.assertEqual(len(tournaments), 2)

        # First tournament (Berlin)
        t1 = tournaments[0]
        self.assertEqual(t1["venue_id"], 501)
        self.assertIsNotNone(t1["venue"])
        self.assertEqual(t1["venue"]["name"], "Берлин")
        self.assertEqual(t1["chat_title"], "ЧГК Берлин")

        # Second tournament (Frankfurt resolved via monitored_venues)
        t2 = tournaments[1]
        self.assertEqual(t2["venue_id"], 602)
        self.assertIsNotNone(t2["venue"])
        self.assertEqual(t2["venue"]["name"], "Франкфурт")
        self.assertEqual(t2["chat_title"], "ЧГК Франкфурт")

    @patch("main.get_authenticated_user")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.get_chat_config")
    @patch("datastore.get_chat_title")
    @patch("rating_api.get_sync_request_details")
    @patch("rating_api.get_venue_by_id")
    @patch("datastore.mark_user_bot_started")
    def test_legacy_registration_resolved_from_rating_api_and_backfilled(
        self,
        mock_bot_started,
        mock_get_venue_by_id,
        mock_get_sync_req_details,
        mock_get_chat_title,
        mock_get_chat_config,
        mock_get_monitored_venues,
        mock_get_all_active,
        mock_auth_user
    ):
        mock_auth_user.return_value = ({"username": "rep_user"}, 222222, True)
        mock_get_all_active.return_value = [
            {
                "sync_req_id": "78901",
                "chat_id": -100333,
                "venue_id": None,  # Old record without venue_id
                "tourn_name": "Старый турнир",
                "teams": []
            }
        ]
        mock_get_monitored_venues.return_value = {}
        mock_get_chat_config.return_value = {"collect_rosters": True}
        mock_get_chat_title.return_value = "Гамбург ЧГК"
        mock_get_sync_req_details.return_value = {
            "id": "78901",
            "venue_id": 888,
        }
        mock_get_venue_by_id.return_value = {
            "id": 888,
            "name": "Гамбург",
            "town": "Гамбург"
        }

        client = main.app.test_client()
        resp = client.post("/api/miniapp/init", json={"initData": "dummy"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()

        tournaments = data.get("tournaments", [])
        self.assertEqual(len(tournaments), 1)
        t = tournaments[0]
        self.assertEqual(t["venue_id"], 888)
        self.assertEqual(t["venue"]["name"], "Гамбург")
        self.assertEqual(t["chat_title"], "Гамбург ЧГК")

    @patch("datastore.get_datastore_client")
    def test_non_numeric_venue_handling_in_datastore(self, mock_get_ds):
        mock_get_ds.return_value = self.mock_client

        # None of these should raise ValueError: invalid literal for int()
        self.assertIsNone(datastore.get_venue_sync_state("RU"))
        self.assertIsNone(datastore.get_cached_venue("RU"))
        datastore.save_venue_sync_state("RU", datetime.datetime.now(pytz.utc), ["req1"])
        datastore.cache_venue("RU", {"name": "Invalid"})
        self.assertEqual(datastore.get_venue_data("RU"), {"teams": []})
        self.assertEqual(datastore.get_venue_team_roster("RU", 123), [])
        self.assertEqual(datastore.get_venues_team_rosters(["RU", "ABC"], 123), [])
        datastore.update_venue_data_incremental("RU", [{"team_id": 1, "name": "T"}], {})


if __name__ == "__main__":
    unittest.main()
