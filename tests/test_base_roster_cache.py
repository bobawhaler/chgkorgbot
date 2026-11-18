import unittest
from unittest.mock import patch, MagicMock
import datetime
import pytz
import datastore
import rating_api
import helpers


class TestBaseRosterCache(unittest.TestCase):
    def test_get_cached_team_base_roster_fresh(self):
        mock_client = MagicMock()
        mock_entity = {
            "team_id": 100,
            "base_pids": [10, 20, 30],
            "captain_id": 10,
            "cached_at": datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=2)
        }
        mock_client.get.return_value = mock_entity

        with patch("datastore.get_datastore_client", return_value=mock_client):
            res = datastore.get_cached_team_base_roster(100, max_age_hours=12)
            self.assertIsNotNone(res)
            base_pids, captain_id = res
            self.assertEqual(base_pids, {10, 20, 30})
            self.assertEqual(captain_id, 10)

    def test_get_cached_team_base_roster_expired(self):
        mock_client = MagicMock()
        mock_entity = {
            "team_id": 100,
            "base_pids": [10, 20, 30],
            "captain_id": 10,
            "cached_at": datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=25)
        }
        mock_client.get.return_value = mock_entity

        with patch("datastore.get_datastore_client", return_value=mock_client):
            # Expired with max_age_hours=12
            res = datastore.get_cached_team_base_roster(100, max_age_hours=12)
            self.assertIsNone(res)

            # But accessible if max_age_hours=None (stale fallback)
            res_stale = datastore.get_cached_team_base_roster(100, max_age_hours=None)
            self.assertIsNotNone(res_stale)
            self.assertEqual(res_stale[0], {10, 20, 30})

    def test_get_cached_team_base_roster_missing_cached_at(self):
        mock_client = MagicMock()
        mock_entity = {
            "team_id": 100,
            "base_pids": [10, 20, 30],
            "captain_id": 10,
            "cached_at": None
        }
        mock_client.get.return_value = mock_entity

        with patch("datastore.get_datastore_client", return_value=mock_client):
            res = datastore.get_cached_team_base_roster(100, max_age_hours=12)
            self.assertIsNone(res)

    def test_get_cached_team_players_fresh_and_expired(self):
        mock_client = MagicMock()
        mock_entity_fresh = {
            "team_id": 100,
            "players": '[{"id": 10, "name": "Test"}]',
            "cached_at": datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=5)
        }
        mock_client.get.return_value = mock_entity_fresh

        with patch("datastore.get_datastore_client", return_value=mock_client):
            res = datastore.get_cached_team_players(100, max_age_hours=24)
            self.assertIsNotNone(res)
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0]["id"], 10)

        mock_entity_expired = {
            "team_id": 100,
            "players": '[{"id": 10, "name": "Test"}]',
            "cached_at": datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=48)
        }
        mock_client.get.return_value = mock_entity_expired
        with patch("datastore.get_datastore_client", return_value=mock_client):
            res_exp = datastore.get_cached_team_players(100, max_age_hours=24)
            self.assertIsNone(res_exp)

    @patch("requests.get")
    @patch("datastore.cache_team_base_roster")
    @patch("datastore.get_cached_team_base_roster")
    def test_get_team_base_players_api_fetch(self, mock_get_cached, mock_cache, mock_req):
        mock_get_cached.return_value = None

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = [
            {"idplayer": 1, "idseason": 58, "captain": False, "playerNumber": 2, "dateRemoved": None},
            {"idplayer": 2, "idseason": 59, "captain": True, "playerNumber": 1, "dateRemoved": None},
            {"idplayer": 3, "idseason": 59, "captain": False, "playerNumber": 2, "dateRemoved": None},
            {"idplayer": 4, "idseason": 59, "captain": False, "playerNumber": 3, "dateRemoved": "2026-01-01"},
        ]
        mock_req.return_value = mock_resp

        base_pids, captain_id = rating_api.get_team_base_players(500)
        self.assertEqual(base_pids, {2, 3})
        self.assertEqual(captain_id, 2)
        mock_cache.assert_called_once_with(500, {2, 3}, 2)

    @patch("requests.get")
    @patch("datastore.get_cached_team_base_roster")
    def test_get_team_base_players_force_refresh(self, mock_get_cached, mock_req):
        mock_get_cached.return_value = ({99}, 99)

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = [
            {"idplayer": 101, "idseason": 60, "captain": True, "dateRemoved": None}
        ]
        mock_req.return_value = mock_resp

        with patch("datastore.cache_team_base_roster"):
            base_pids, cap_id = rating_api.get_team_base_players(500, force_refresh=True)
            self.assertEqual(base_pids, {101})
            self.assertEqual(cap_id, 101)
            mock_req.assert_called_once()

    @patch("requests.get")
    @patch("datastore.get_cached_team_base_roster")
    def test_get_team_base_players_stale_fallback_on_api_error(self, mock_get_cached, mock_req):
        # First call with max_age_hours=12 returns None (expired)
        # Second call with max_age_hours=None returns stale cached data
        mock_get_cached.side_effect = [None, ({77}, 77)]
        mock_req.side_effect = Exception("Network timeout")

        base_pids, cap_id = rating_api.get_team_base_players(500)
        self.assertEqual(base_pids, {77})
        self.assertEqual(cap_id, 77)

    @patch("rating_api.get_player_by_id")
    @patch("rating_api.get_team_players")
    @patch("rating_api.get_team_base_players")
    @patch("datastore.get_user_history")
    @patch("datastore.get_user_mapping")
    def test_get_roster_candidates_base_players_included(self, mock_mapping, mock_hist, mock_base, mock_tp, mock_pbyid):
        mock_mapping.return_value = None
        mock_hist.return_value = {"players": []}
        mock_base.return_value = ({10, 20}, 10)
        mock_tp.return_value = []
        mock_pbyid.side_effect = lambda pid: {
            "id": pid,
            "name": f"Name{pid}",
            "surname": f"Surname{pid}",
            "patronymic": "",
            "town": "Town",
        }

        candidates = helpers.get_roster_candidates(user_id=123, rating_team_id=500)
        self.assertTrue(any(c["pid"] == 10 and c["default_status"] == "K" and c["is_captain"] for c in candidates))
        self.assertTrue(any(c["pid"] == 20 and c["default_status"] == "B" and c["is_base"] for c in candidates))

    @patch("rating_api.search_players")
    @patch("datastore.search_cached_players")
    @patch("rating_api.get_team_base_players")
    @patch("datastore.get_user_history")
    @patch("datastore.get_user_mapping")
    def test_search_players_tiered_status_assignment(self, mock_mapping, mock_hist, mock_base, mock_cache_search, mock_search):
        mock_mapping.return_value = None
        mock_hist.return_value = {"players": []}
        mock_base.return_value = ({10, 20}, 10)
        mock_cache_search.return_value = []
        mock_search.return_value = [
            {"id": 10, "name": "Cap", "surname": "Team"},
            {"id": 20, "name": "Base", "surname": "Player"},
            {"id": 30, "name": "Legion", "surname": "Player"},
        ]

        results = helpers.search_players_tiered("player", rating_team_id=500)
        p10 = next(r for r in results if r["id"] == 10)
        p20 = next(r for r in results if r["id"] == 20)
        p30 = next(r for r in results if r["id"] == 30)

        self.assertEqual(p10["default_status"], "K")
        self.assertEqual(p20["default_status"], "B")
        self.assertEqual(p30["default_status"], "L")


if __name__ == "__main__":
    unittest.main()
