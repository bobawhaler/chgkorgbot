#!/usr/bin/env python3
"""
Test suite to verify that all rating API endpoints used by chgkorgbot
function as expected.

Run as:
    python3 tests/test_rating_api.py
or:
    python3 -m unittest tests/test_rating_api.py
"""

import unittest
import datetime
import requests

import os
import sys

API_URL = "https://api.rating.chgk.info"
HEADERS = {"Accept": "application/json"}
LD_HEADERS = {"Accept": "application/ld+json"}
TIMEOUT = 10


class TestRatingApiHealth(unittest.TestCase):
    """Checks the health and compatibility of rating.chgk.info API endpoints."""

    @classmethod
    def setUpClass(cls):
        # Live network tests are intended for manual diagnostics only.
        # Skip during automated test discovery (pytest / unittest discover)
        # unless executed directly or explicitly enabled with RUN_LIVE_API_TESTS=1.
        is_direct = (
            os.environ.get("RUN_LIVE_API_TESTS") == "1"
            or any("test_rating_api.py" in arg for arg in sys.argv)
        )
        if not is_direct:
            raise unittest.SkipTest(
                "Live rating API tests are for manual verification only. "
                "Run manually: python3 tests/test_rating_api.py"
            )

    def test_01_api_entrypoint_and_docs(self):
        """Verifies entrypoint and documentation accessibility."""
        # Entrypoint with application/ld+json
        r = requests.get(f"{API_URL}/", headers=LD_HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200, f"Entrypoint returned {r.status_code}")
        data = r.json()
        self.assertIn("tournament", data)
        self.assertIn("team", data)
        self.assertIn("player", data)

        # Docs endpoint
        r_docs = requests.get(f"{API_URL}/docs.jsonld", headers=LD_HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_docs.status_code, 200, f"docs.jsonld returned {r_docs.status_code}")

    def test_02_tournament_by_id(self):
        """Verifies fetching a single tournament by ID and expected fields."""
        tourn_id = 5000
        r = requests.get(f"{API_URL}/tournaments/{tourn_id}", headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200, f"Tournament {tourn_id} returned {r.status_code}")
        t = r.json()
        self.assertEqual(t.get("id"), tourn_id)
        self.assertIn("name", t)
        self.assertIn("type", t)
        self.assertIn("editors", t)
        self.assertIn("dateStart", t)
        self.assertIn("dateEnd", t)
        self.assertIn("questionQty", t)
        # difficultyForecast is present in recent tournaments, optional in older
        if "difficultyForecast" in t and t["difficultyForecast"] is not None:
            self.assertIsInstance(t["difficultyForecast"], (int, float))

    def test_03_tournaments_list_and_filters(self):
        """Verifies tournament listing with date and type filters as used in get_tourns."""
        now = datetime.datetime.now(datetime.timezone.utc)
        from_date = (now - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
        to_date = now.strftime("%Y-%m-%d")

        params = {
            "dateStart[before]": f"{to_date} 23:59",
            "dateStart[after]": from_date,
            "dateEnd[after]": f"{to_date} 23:59",
            "type[]": [3, 6, 8],
            "page": 1,
            "itemsPerPage": 20,
        }
        r = requests.get(f"{API_URL}/tournaments", params=params, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200, f"/tournaments returned {r.status_code}")
        items = r.json()
        self.assertIsInstance(items, list)
        self.assertGreater(len(items), 0, "No tournaments returned for recent date range")

        first = items[0]
        self.assertIn("id", first)
        self.assertIn("name", first)
        self.assertIn("type", first)
        self.assertIn(first["type"]["id"], (3, 6, 8), f"Unexpected tournament type: {first['type']}")

    def test_04_venue_requests(self):
        """Verifies venue sync requests listing with dateStart and issuedAt filters."""
        # Venue 3172 is Novorossiysk, a longstanding active venue
        venue_id = 3172
        params = {
            "dateStart[after]": "2024-01-01",
            "page": 1,
            "itemsPerPage": 10,
        }
        r = requests.get(f"{API_URL}/venues/{venue_id}/requests", params=params, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200, f"/venues/{venue_id}/requests returned {r.status_code}")
        items = r.json()
        self.assertIsInstance(items, list)
        self.assertGreater(len(items), 0, "Expected requests for venue 3172")

        req0 = items[0]
        self.assertIn("id", req0)
        self.assertIn("status", req0)
        self.assertIn("tournamentId", req0)
        self.assertIn("dateStart", req0)
        self.assertIn("representative", req0)

        # Check issuedAt filter
        issued_params = {
            "issuedAt[after]": "2024-01-01",
            "issuedAt[before]": "2024-06-01",
            "itemsPerPage": 5,
        }
        r_issued = requests.get(f"{API_URL}/venues/{venue_id}/requests", params=issued_params, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_issued.status_code, 200)

    def test_05_tournament_synch_request_detail(self):
        """Verifies fetching details of a specific sync request."""
        # Request 588 is a known valid request
        s_id = 588
        r = requests.get(f"{API_URL}/tournament_synch_requests/{s_id}", headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200, f"Request {s_id} returned {r.status_code}")
        data = r.json()
        self.assertEqual(data.get("id"), s_id)
        self.assertIn("tournamentId", data)
        self.assertIn("issuedAt", data)
        self.assertIn("dateStart", data)
        self.assertIn("representative", data)
        self.assertIn("narrator", data)

    def test_06_team_endpoints(self):
        """Verifies team by ID, team search, team seasons, and team tournaments."""
        team_id = 1
        # Team by ID
        r = requests.get(f"{API_URL}/teams/{team_id}", headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200)
        team = r.json()
        self.assertEqual(team.get("id"), team_id)
        self.assertEqual(team.get("name"), "Неспроста")
        self.assertIn("town", team)

        # Search teams by name
        r_search = requests.get(f"{API_URL}/teams", params={"name": "Афина", "itemsPerPage": 5}, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_search.status_code, 200)
        teams = r_search.json()
        self.assertIsInstance(teams, list)
        self.assertGreater(len(teams), 0)

        # Team seasons
        r_seasons = requests.get(f"{API_URL}/teams/{team_id}/seasons", params={"itemsPerPage": 10}, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_seasons.status_code, 200)
        seasons = r_seasons.json()
        self.assertIsInstance(seasons, list)
        self.assertGreater(len(seasons), 0)
        self.assertIn("idplayer", seasons[0])

        # Team tournaments
        r_tourns = requests.get(f"{API_URL}/teams/{team_id}/tournaments", headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_tourns.status_code, 200)
        tourns = r_tourns.json()
        self.assertIsInstance(tourns, list)
        self.assertGreater(len(tourns), 0)
        self.assertIn("idtournament", tourns[0])

    def test_07_player_endpoints(self):
        """Verifies player by ID, player seasons, and search by surname/name."""
        player_id = 1000
        # Player by ID
        r = requests.get(f"{API_URL}/players/{player_id}", headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r.status_code, 200)
        player = r.json()
        self.assertEqual(player.get("id"), player_id)
        self.assertIn("name", player)
        self.assertIn("surname", player)
        self.assertIn("patronymic", player)

        # Player seasons
        r_seasons = requests.get(f"{API_URL}/players/{player_id}/seasons", params={"itemsPerPage": 10}, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_seasons.status_code, 200)
        seasons = r_seasons.json()
        self.assertIsInstance(seasons, list)
        self.assertGreater(len(seasons), 0)
        self.assertIn("idteam", seasons[0])

        # Search by surname
        r_surname = requests.get(f"{API_URL}/players", params={"surname": "Иванов", "itemsPerPage": 5}, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_surname.status_code, 200)
        players = r_surname.json()
        self.assertIsInstance(players, list)
        self.assertGreater(len(players), 0)

        # Search by name and surname
        r_both = requests.get(f"{API_URL}/players", params={"surname": "Иванов", "name": "Александр", "itemsPerPage": 5}, headers=HEADERS, timeout=TIMEOUT)
        self.assertEqual(r_both.status_code, 200)
        both_players = r_both.json()
        self.assertIsInstance(both_players, list)
        self.assertGreater(len(both_players), 0)

    def test_08_tournament_results_with_venue_and_members(self):
        """Verifies tournament results with venue filter and includeTeamMembers."""
        # Tournament 8998 has results with synchRequest and teamMembers
        tid = 8998
        venue_id = 3036

        # Filter by venue
        r = requests.get(
            f"{API_URL}/tournaments/{tid}/results",
            params={"venue": venue_id, "includeTeamMembers": 1},
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        self.assertEqual(r.status_code, 200, f"Results returned {r.status_code}")
        results = r.json()
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0, "Expected results for tournament 8998 and venue 3036")

        first_res = results[0]
        self.assertIn("team", first_res)
        self.assertIn("questionsTotal", first_res)
        self.assertIn("synchRequest", first_res)
        self.assertIn("teamMembers", first_res)
        self.assertGreater(len(first_res["teamMembers"]), 0)

        # Check venue matches
        res_vid = first_res["synchRequest"]["venue"]["id"]
        self.assertEqual(res_vid, venue_id, f"Venue mismatch: got {res_vid}, expected {venue_id}")

    def test_09_check_known_quirks_and_warnings(self):
        """Documents and checks known rating API quirks to track if upstream fixes them."""
        # Quirk 1: ?team filter on /results is ignored by API
        tid = 8998
        sample_team_id = 87214
        r = requests.get(f"{API_URL}/tournaments/{tid}/results", params={"team": sample_team_id}, headers=HEADERS, timeout=TIMEOUT)
        if r.ok:
            data = r.json()
            teams_in_response = {res.get("team", {}).get("id") for res in data if isinstance(res.get("team"), dict)}
            if len(teams_in_response) > 1:
                print(f"\n[QUIRK VERIFIED] /tournaments/{tid}/results ignores ?team={sample_team_id} (returns all {len(teams_in_response)} teams). Client-side filtering is required.")
            else:
                print(f"\n[NOTICE] /tournaments/{tid}/results now respects ?team! Upstream may have implemented it.")

        # Quirk 2: /players ignores ?patronymic filter
        r_pat = requests.get(f"{API_URL}/players", params={"surname": "Иванов", "name": "Иван", "patronymic": "Иванович", "itemsPerPage": 5}, headers=HEADERS, timeout=TIMEOUT)
        if r_pat.ok:
            data = r_pat.json()
            # If server ignored patronymic, results may contain players without 'Иванович'
            non_ivanovich = [p for p in data if (p.get("patronymic") or "") != "Иванович"]
            if non_ivanovich:
                print(f"[QUIRK VERIFIED] /players ignores ?patronymic filter (returned {len(non_ivanovich)} players with differing patronymics).")


def print_banner(text):
    print("=" * 60)
    print(f"  {text}")
    print("=" * 60)


def run_standalone_diagnostic():
    print_banner("DIAGNOSTIC RUN: rating.chgk.info API")
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRatingApiHealth)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if result.wasSuccessful():
        print("\nAll rating API checks passed successfully!")
    else:
        print(f"\nCompleted with issues: {len(result.failures)} failures, {len(result.errors)} errors.")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    import sys
    sys.exit(run_standalone_diagnostic())
