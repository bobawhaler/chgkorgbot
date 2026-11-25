import os
import unittest
from unittest.mock import patch, MagicMock

os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8081"
os.environ["TELEGRAM_TOKEN"] = "test_token"

import main
import datastore


class TestRoleFilteringInMiniApp(unittest.TestCase):
    def setUp(self):
        self.mock_regs = [
            # Tournament 1: Chat 100, collect_rosters=True, Rep is user 555
            {
                "sync_req_id": "sync_1",
                "chat_id": 100,
                "venue_id": 10,
                "tourn_name": "Турнир 1 (Сбор ВКЛ)",
                "representative_text": "Представитель: Илья (ID 555)",
                "created_by_user_id": 555,
                "teams": [
                    {"team_name": "Команда Альфа", "user_id": 111, "roster": []},
                    {"team_name": "Команда Бета", "user_id": 222, "roster": []},
                ]
            },
            # Tournament 2: Chat 200, collect_rosters=False, Rep is user 555
            {
                "sync_req_id": "sync_2",
                "chat_id": 200,
                "venue_id": 20,
                "tourn_name": "Турнир 2 (Сбор ВЫКЛ)",
                "representative_text": "Представитель: Илья (ID 555)",
                "created_by_user_id": 555,
                "teams": [
                    {"team_name": "Команда Гамма", "user_id": 111, "roster": []},
                ]
            },
            # Tournament 3: Chat 300, collect_rosters=True, Rep is user 777
            {
                "sync_req_id": "sync_3",
                "chat_id": 300,
                "venue_id": 30,
                "tourn_name": "Турнир 3 (Сбор ВКЛ)",
                "representative_text": "Представитель: Ольга (ID 777)",
                "created_by_user_id": 777,
                "teams": [
                    {"team_name": "Команда Дельта", "user_id": 111, "roster": []},
                    {"team_name": "Команда Эпсилон", "user_id": 333, "roster": []},
                ]
            }
        ]

        self.chat_configs = {
            "100": {"collect_rosters": True, "chat_title": "Чат 100", "venues": [10]},
            "200": {"collect_rosters": False, "chat_title": "Чат 200", "venues": [20]},
            "300": {"collect_rosters": True, "chat_title": "Чат 300", "venues": [30]},
        }

    @patch("main.get_authenticated_user")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_configs_multi")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.get_cached_venues_multi")
    @patch("datastore.mark_user_bot_started")
    def test_admin_sees_only_venues_with_collect_rosters(
        self,
        mock_bot_started,
        mock_cached_venues,
        mock_monitored,
        mock_chat_configs,
        mock_all_active,
        mock_auth_user
    ):
        mock_auth_user.return_value = ({"username": "admin"}, 999999, True)
        mock_all_active.return_value = self.mock_regs
        mock_chat_configs.return_value = self.chat_configs
        mock_monitored.return_value = {}
        mock_cached_venues.return_value = {}

        client = main.app.test_client()
        resp = client.post("/api/miniapp/init", json={"initData": "dummy", "test_role": "admin"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()

        tournaments = data.get("tournaments", [])
        tourn_ids = [t["sync_req_id"] for t in tournaments]
        
        # sync_1 and sync_3 have collect_rosters=True -> must be included
        self.assertIn("sync_1", tourn_ids)
        self.assertIn("sync_3", tourn_ids)
        # sync_2 has collect_rosters=False -> must NOT be included for admin
        self.assertNotIn("sync_2", tourn_ids)

    @patch("main.get_authenticated_user")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_configs_multi")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.get_cached_venues_multi")
    @patch("datastore.mark_user_bot_started")
    def test_representative_sees_only_own_venues(
        self,
        mock_bot_started,
        mock_cached_venues,
        mock_monitored,
        mock_chat_configs,
        mock_all_active,
        mock_auth_user
    ):
        # User 555 is rep for sync_1 (chat 100) and sync_2 (chat 200)
        mock_auth_user.return_value = ({"username": "rep_ilya"}, 555, False)
        mock_all_active.return_value = self.mock_regs
        mock_chat_configs.return_value = self.chat_configs
        mock_monitored.return_value = {}
        mock_cached_venues.return_value = {}

        client = main.app.test_client()
        resp = client.post("/api/miniapp/init", json={"initData": "dummy"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()

        tournaments = data.get("tournaments", [])
        tourn_ids = [t["sync_req_id"] for t in tournaments]

        # Only sync_1 should be visible: 555 is representative, and collect_rosters is True
        self.assertIn("sync_1", tourn_ids)
        # sync_2 has collect_rosters=False -> not visible
        self.assertNotIn("sync_2", tourn_ids)
        # sync_3 has rep 777, not 555 -> 555 is NOT rep for sync_3
        # In sync_3, user 555 has no teams either, so sync_3 must NOT be visible
        self.assertNotIn("sync_3", tourn_ids)

        # In sync_1, rep sees all teams of that tournament
        t1 = [t for t in tournaments if t["sync_req_id"] == "sync_1"][0]
        self.assertTrue(t1["is_representative"])
        self.assertEqual(len(t1["teams"]), 2)

    @patch("main.get_authenticated_user")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_configs_multi")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.get_cached_venues_multi")
    @patch("datastore.mark_user_bot_started")
    def test_player_sees_only_own_teams(
        self,
        mock_bot_started,
        mock_cached_venues,
        mock_monitored,
        mock_chat_configs,
        mock_all_active,
        mock_auth_user
    ):
        # User 111 registered "Команда Альфа" in sync_1, "Команда Гамма" in sync_2, "Команда Дельта" in sync_3
        mock_auth_user.return_value = ({"username": "player_111"}, 111, False)
        mock_all_active.return_value = self.mock_regs
        mock_chat_configs.return_value = self.chat_configs
        mock_monitored.return_value = {}
        mock_cached_venues.return_value = {}

        client = main.app.test_client()
        resp = client.post("/api/miniapp/init", json={"initData": "dummy"})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()

        tournaments = data.get("tournaments", [])
        tourn_ids = [t["sync_req_id"] for t in tournaments]

        # sync_1 and sync_3 have collect_rosters=True and user 111 has teams -> visible
        self.assertIn("sync_1", tourn_ids)
        self.assertIn("sync_3", tourn_ids)
        # sync_2 has collect_rosters=False -> not visible
        self.assertNotIn("sync_2", tourn_ids)

        # In sync_1, user 111 only sees "Команда Альфа" (his own team), NOT "Команда Бета" (user 222)
        t1 = [t for t in tournaments if t["sync_req_id"] == "sync_1"][0]
        self.assertFalse(t1["is_representative"])
        self.assertEqual(len(t1["teams"]), 1)
        self.assertEqual(t1["teams"][0]["team_name"], "Команда Альфа")

        # In sync_3, user 111 only sees "Команда Дельта" (his own team), NOT "Команда Эпсилон" (user 333)
        t3 = [t for t in tournaments if t["sync_req_id"] == "sync_3"][0]
        self.assertFalse(t3["is_representative"])
        self.assertEqual(len(t3["teams"]), 1)
        self.assertEqual(t3["teams"][0]["team_name"], "Команда Дельта")


if __name__ == "__main__":
    unittest.main()
