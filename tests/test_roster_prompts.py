import os
os.environ.setdefault("TELEGRAM_API_TOKEN", "dummy_token")

import unittest
from unittest.mock import patch, MagicMock
import helpers
import datastore


class TestRosterPrompts(unittest.TestCase):
    def test_random_roster_prompt_format(self):
        url = "https://t.me/test_bot?start=roster_123_456"
        for _ in range(50):
            prompt = helpers.get_random_roster_prompt(url)
            self.assertIn(url, prompt)
            self.assertIn("<a href=", prompt)
            has_expected_keyword = any(kw in prompt.lower() for kw in ["состав", "игроков", "боте", "бот"])
            self.assertTrue(has_expected_keyword, f"Prompt did not match expected wording: {prompt}")

    @patch("datastore.get_datastore_client")
    def test_get_user_registration_reminders_count_empty(self, mock_client_func):
        mock_client = MagicMock()
        mock_client_func.return_value = mock_client
        mock_client.get.return_value = None

        count = datastore.get_user_registration_reminders_count(12345)
        self.assertEqual(count, 0)

    @patch("datastore.get_datastore_client")
    def test_get_user_registration_reminders_count_existing(self, mock_client_func):
        mock_client = MagicMock()
        mock_client_func.return_value = mock_client
        mock_entity = {"user_id": 12345, "registration_reminders_count": 2}
        mock_client.get.return_value = mock_entity

        count = datastore.get_user_registration_reminders_count(12345)
        self.assertEqual(count, 2)

    @patch("datastore.get_datastore_client")
    def test_increment_user_registration_reminders_count(self, mock_client_func):
        mock_client = MagicMock()
        mock_client_func.return_value = mock_client
        
        # Simulating transaction context manager
        mock_client.transaction.return_value.__enter__ = MagicMock()
        mock_client.transaction.return_value.__exit__ = MagicMock()
        
        mock_entity = {"user_id": 12345, "registration_reminders_count": 1}
        mock_client.get.return_value = mock_entity

        new_count = datastore.increment_user_registration_reminders_count(12345, "testuser")
        self.assertEqual(new_count, 2)
        self.assertEqual(mock_entity["registration_reminders_count"], 2)
        self.assertEqual(mock_entity["username"], "testuser")
        mock_client.put.assert_called_once_with(mock_entity)

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("telegram_api.set_message_reaction")
    @patch("datastore.register_team_in_ds")
    @patch("helpers.get_chat_collect_rosters")
    @patch("datastore.has_user_started_bot")
    @patch("datastore.get_user_registration_reminders_count")
    @patch("datastore.increment_user_registration_reminders_count")
    @patch("datastore.get_team_registration_by_msg")
    def test_registration_roster_prompt_max_3_times(
        self,
        mock_get_reg,
        mock_inc_rem,
        mock_get_rem_count,
        mock_started_bot,
        mock_collect_rosters,
        mock_register_team,
        mock_reaction,
        mock_edit_msg,
        mock_send_msg
    ):
        import handlers

        mock_get_reg.return_value = {
            "sync_req_id": "999",
            "message_id": 100,
            "tourn_name": "Test Tournament",
            "thread_id": 42
        }
        mock_collect_rosters.return_value = True
        mock_register_team.return_value = {"teams": []}
        mock_started_bot.return_value = False

        body = {
            "message": {
                "message_id": 200,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 777, "username": "player", "first_name": "Player"},
                "text": "My Team",
                "reply_to_message": {"message_id": 100}
            }
        }

        # Case 1: Reminders count = 0 (< 3) -> should send prompt and increment
        mock_get_rem_count.return_value = 0
        handlers.command_handler(body)
        mock_send_msg.assert_called_once()
        mock_inc_rem.assert_called_once_with(777, username="player")

        mock_send_msg.reset_mock()
        mock_inc_rem.reset_mock()

        # Case 2: Reminders count = 2 (< 3) -> should send prompt and increment
        mock_get_rem_count.return_value = 2
        handlers.command_handler(body)
        mock_send_msg.assert_called_once()
        mock_inc_rem.assert_called_once_with(777, username="player")

        mock_send_msg.reset_mock()
        mock_inc_rem.reset_mock()

        # Case 3: Reminders count = 3 (>= 3) -> should NOT send prompt and NOT increment
        mock_get_rem_count.return_value = 3
        handlers.command_handler(body)
        mock_send_msg.assert_not_called()
        mock_inc_rem.assert_not_called()


if __name__ == "__main__":
    unittest.main()

