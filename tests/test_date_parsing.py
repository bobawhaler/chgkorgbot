import unittest
from unittest.mock import patch, MagicMock
import datetime
import time
import helpers


class TestDateParsing(unittest.TestCase):
    def test_parse_date_does_not_call_gemini(self):
        with patch("helpers.parse_date_gemini") as mock_gemini:
            dt, with_time = helpers.parse_date("2026-10-15", "Europe/Moscow")
            self.assertEqual(dt, datetime.date(2026, 10, 15))
            self.assertFalse(with_time)
            mock_gemini.assert_not_called()

    def test_parse_date_iso_with_time(self):
        with patch("helpers.parse_date_gemini") as mock_gemini:
            dt, with_time = helpers.parse_date("2026-10-15T12:00:00+03:00", "Europe/Moscow")
            self.assertEqual(dt, datetime.date(2026, 10, 15))
            self.assertFalse(with_time)
            mock_gemini.assert_not_called()

    def test_parse_date_gemini_cooldown_on_402(self):
        helpers.GEMINI_COOLDOWN_UNTIL = 0
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 402
        mock_resp.text = '{"error": {"code": 402, "message": "Prepayment credits depleted"}}'

        with patch("helpers.requests.post", return_value=mock_resp), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "fake_key"}):
            res_dt, with_time = helpers.parse_date_gemini("завтра", "Europe/Moscow")
            self.assertIsNone(res_dt)
            self.assertFalse(with_time)
            self.assertGreater(helpers.GEMINI_COOLDOWN_UNTIL, time.time() + 3500)

            # Second call during cooldown should return immediately without making HTTP call
            with patch("helpers.requests.post") as mock_post_second:
                res_dt2, with_time2 = helpers.parse_date_gemini("завтра", "Europe/Moscow")
                self.assertIsNone(res_dt2)
                self.assertFalse(with_time2)
                mock_post_second.assert_not_called()

        # Reset cooldown after test
        helpers.GEMINI_COOLDOWN_UNTIL = 0

    def test_parse_date_gemini_cooldown_on_429(self):
        helpers.GEMINI_COOLDOWN_UNTIL = 0
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 429
        mock_resp.text = '{"error": {"code": 429, "message": "Resource exhausted"}}'

        with patch("helpers.requests.post", return_value=mock_resp), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "fake_key"}):
            res_dt, with_time = helpers.parse_date_gemini("завтра", "Europe/Moscow")
            self.assertIsNone(res_dt)
            self.assertFalse(with_time)
            self.assertGreater(helpers.GEMINI_COOLDOWN_UNTIL, time.time() + 250)

        # Reset cooldown after test
        helpers.GEMINI_COOLDOWN_UNTIL = 0

    def test_parse_date_natural_language_without_time(self):
        dt, with_time = helpers.parse_date("сегодня", "Europe/Moscow")
        self.assertIsInstance(dt, datetime.date)
        self.assertFalse(with_time)

        dt2, with_time2 = helpers.parse_date("завтра", "Europe/Moscow")
        self.assertIsInstance(dt2, datetime.date)
        self.assertFalse(with_time2)

        dt3, with_time3 = helpers.parse_date("сегодня в 19:00", "Europe/Moscow")
        self.assertTrue(with_time3)
        self.assertEqual(dt3.hour, 19)
        self.assertEqual(dt3.minute, 0)

    def test_get_tourns_without_maii_rating(self):
        import rating_api
        fake_tourns = [
            {
                "id": 99999,
                "name": "Тестовый турнир",
                "type": {"name": "Синхрон"},
                "difficultyForecast": 3.5,
                "rating": True,  # has 'rating', missing 'maiiRating'
                "editors": [],
            }
        ]
        with patch.object(rating_api, "_fetch_paginated", return_value=fake_tourns), \
             patch("helpers.get_chat_min_difficulty", return_value=0), \
             patch("helpers.get_chat_max_difficulty", return_value=10), \
             patch("datastore.cache_tournaments_batch"):
            res = rating_api.get_tourns(datetime.date(2026, 10, 15), {}, 12345, with_time=False, only_rated=False)
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0]["id"], 99999)
            self.assertTrue(res[0]["rating"])


if __name__ == "__main__":
    unittest.main()
