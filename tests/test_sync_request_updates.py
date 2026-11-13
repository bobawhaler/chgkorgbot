import unittest
from unittest.mock import patch, MagicMock
import datetime
import pytz

import handlers
import datastore
import helpers


class TestSyncRequestUpdates(unittest.TestCase):

    def setUp(self):
        self.venue_id = "3014"
        self.chat_id = -100123456789
        self.monitored_venues = {self.venue_id: [self.chat_id]}
        self.initial_dt = datetime.datetime(2026, 11, 20, 16, 0, 0, tzinfo=pytz.utc)
        self.sync_req_id = "998877"

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("datastore.update_team_registration")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_upcoming_sync_requests")
    def test_start_time_changed_updates_message_only(
        self,
        mock_get_upcoming,
        mock_get_chat_config,
        mock_get_active_regs,
        mock_update_reg,
        mock_edit_message,
        mock_send_message,
    ):
        mock_get_chat_config.return_value = {"timezone": "Europe/Berlin", "thread_id": 42}
        
        reg_entity = {
            "chat_id": self.chat_id,
            "thread_id": 42,
            "sync_req_id": self.sync_req_id,
            "message_id": 1001,
            "tourn_name": "Тестовый Синхрон",
            "representative_text": "Представитель: Тестов Тест (10001)",
            "narrator_text": "Ведущий: Ведущий Ведущ (10002)",
            "start_time": "20.11 17:00",
            "start_time_ts": int(self.initial_dt.timestamp()),
            "teams": [{"team_name": "Знатоки", "user_id": 123}],
            "status": "active",
            "register_teams": True,
            "tourn_id": 5001,
        }
        mock_get_active_regs.return_value = [reg_entity]

        # New start time moved by 1 hour (17:00 UTC -> 18:00 in Berlin)
        new_dt = datetime.datetime(2026, 11, 20, 17, 0, 0, tzinfo=pytz.utc)
        mock_get_upcoming.return_value = [
            {
                "id": self.sync_req_id,
                "tourn_id": 5001,
                "status": "A",
                "representative": {"id": 10001, "name": "Тест", "surname": "Тестов"},
                "narrator": {"id": 10002, "name": "Ведущ", "surname": "Ведущий"},
                "dateStart": new_dt,
            }
        ]
        
        edit_resp = MagicMock()
        edit_resp.ok = True
        mock_edit_message.return_value = edit_resp

        handlers.check_and_update_active_sync_requests(self.monitored_venues)

        # Verify edit_message_text called with new start time
        self.assertTrue(mock_edit_message.called)
        edit_args, _ = mock_edit_message.call_args
        self.assertEqual(edit_args[0], self.chat_id)
        self.assertEqual(edit_args[1], 1001)
        self.assertIn("18:00", edit_args[2])
        self.assertIn("Знатоки", edit_args[2])

        # Verify update_team_registration called with updated fields
        mock_update_reg.assert_called_once()
        self.assertEqual(reg_entity["start_time_ts"], int(new_dt.timestamp()))
        self.assertEqual(reg_entity["start_time"], "20.11 18:00")

        # Verify NO extra notification message is sent (only update message)
        self.assertFalse(mock_send_message.called)

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("datastore.update_team_registration")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_upcoming_sync_requests")
    def test_narrator_changed_silently_updates_message(
        self,
        mock_get_upcoming,
        mock_get_chat_config,
        mock_get_active_regs,
        mock_update_reg,
        mock_edit_message,
        mock_send_message,
    ):
        mock_get_chat_config.return_value = {"timezone": "Europe/Berlin"}
        
        reg_entity = {
            "chat_id": self.chat_id,
            "sync_req_id": self.sync_req_id,
            "message_id": 1002,
            "tourn_name": "Тестовый Синхрон",
            "representative_text": "Представитель: Тестов Тест (10001)",
            "narrator_text": "Ведущий: Старый Ведущий (10002)",
            "start_time": "20.11 17:00",
            "start_time_ts": int(self.initial_dt.timestamp()),
            "teams": [],
            "status": "active",
            "register_teams": True,
        }
        mock_get_active_regs.return_value = [reg_entity]

        # Same dateStart, but new narrator
        mock_get_upcoming.return_value = [
            {
                "id": self.sync_req_id,
                "tourn_id": 5001,
                "status": "A",
                "representative": {"id": 10001, "name": "Тест", "surname": "Тестов"},
                "narrator": {"id": 10003, "name": "Новый", "surname": "Ведущий"},
                "dateStart": self.initial_dt,
            }
        ]

        handlers.check_and_update_active_sync_requests(self.monitored_venues)

        self.assertTrue(mock_edit_message.called)
        edit_args, _ = mock_edit_message.call_args
        self.assertIn("Новый", edit_args[2])

        # No notification message should be sent for narrator change alone
        self.assertFalse(mock_send_message.called)
        mock_update_reg.assert_called_once()

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("datastore.update_team_registration")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_upcoming_sync_requests")
    def test_status_canceled_updates_message_and_notifies(
        self,
        mock_get_upcoming,
        mock_get_chat_config,
        mock_get_active_regs,
        mock_update_reg,
        mock_edit_message,
        mock_send_message,
    ):
        mock_get_chat_config.return_value = {"timezone": "Europe/Berlin"}
        
        reg_entity = {
            "chat_id": self.chat_id,
            "sync_req_id": self.sync_req_id,
            "message_id": 1003,
            "tourn_name": "Тестовый Синхрон",
            "representative_text": "Представитель: Тестов Тест (10001)",
            "narrator_text": "Ведущий: Ведущий Ведущ (10002)",
            "start_time": "20.11 17:00",
            "start_time_ts": int(self.initial_dt.timestamp()),
            "teams": [],
            "status": "active",
            "register_teams": True,
        }
        mock_get_active_regs.return_value = [reg_entity]

        mock_get_upcoming.return_value = [
            {
                "id": self.sync_req_id,
                "tourn_id": 5001,
                "status": "C",  # Canceled
                "representative": {"id": 10001, "name": "Тест", "surname": "Тестов"},
                "narrator": {"id": 10002, "name": "Ведущ", "surname": "Ведущий"},
                "dateStart": self.initial_dt,
            }
        ]
        
        edit_resp = MagicMock()
        edit_resp.ok = True
        mock_edit_message.return_value = edit_resp

        handlers.check_and_update_active_sync_requests(self.monitored_venues)

        self.assertTrue(mock_edit_message.called)
        edit_args, _ = mock_edit_message.call_args
        self.assertIn("отменена", edit_args[2])

        self.assertEqual(reg_entity["status"], "canceled")
        mock_update_reg.assert_called_once()

        self.assertTrue(mock_send_message.called)
        send_args, _ = mock_send_message.call_args
        self.assertIn("отменена", send_args[2])

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("datastore.update_team_registration")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_upcoming_sync_requests")
    def test_announcement_only_registration_updated(
        self,
        mock_get_upcoming,
        mock_get_chat_config,
        mock_get_active_regs,
        mock_update_reg,
        mock_edit_message,
        mock_send_message,
    ):
        mock_get_chat_config.return_value = {"timezone": "Europe/Berlin"}
        
        reg_entity = {
            "chat_id": self.chat_id,
            "sync_req_id": self.sync_req_id,
            "message_id": 1004,
            "tourn_name": "Тестовый Синхрон",
            "representative_text": "Представитель: Тестов Тест (10001)",
            "narrator_text": "Ведущий: Ведущий Ведущ (10002)",
            "start_time": "20.11 17:00",
            "start_time_ts": int(self.initial_dt.timestamp()),
            "teams": [],
            "status": "active",
            "register_teams": False,
        }
        mock_get_active_regs.return_value = [reg_entity]

        new_dt = datetime.datetime(2026, 11, 20, 18, 0, 0, tzinfo=pytz.utc)
        mock_get_upcoming.return_value = [
            {
                "id": self.sync_req_id,
                "tourn_id": 5001,
                "status": "A",
                "representative": {"id": 10001, "name": "Тест", "surname": "Тестов"},
                "narrator": {"id": 10002, "name": "Ведущ", "surname": "Ведущий"},
                "dateStart": new_dt,
            }
        ]
        
        edit_resp = MagicMock()
        edit_resp.ok = True
        mock_edit_message.return_value = edit_resp

        handlers.check_and_update_active_sync_requests(self.monitored_venues)

        self.assertTrue(mock_edit_message.called)
        edit_args, _ = mock_edit_message.call_args
        self.assertIn("19:00", edit_args[2])
        self.assertNotIn("Регистрация команд открыта", edit_args[2])

        # Verify no notification message is sent (update message only)
        self.assertFalse(mock_send_message.called)
        mock_update_reg.assert_called_once()

    @patch("telegram_api.send_message")
    @patch("telegram_api.edit_message_text")
    @patch("datastore.update_team_registration")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_upcoming_sync_requests")
    def test_nothing_changed_does_no_api_calls(
        self,
        mock_get_upcoming,
        mock_get_chat_config,
        mock_get_active_regs,
        mock_update_reg,
        mock_edit_message,
        mock_send_message,
    ):
        mock_get_chat_config.return_value = {"timezone": "Europe/Berlin"}
        
        reg_entity = {
            "chat_id": self.chat_id,
            "sync_req_id": self.sync_req_id,
            "message_id": 1005,
            "tourn_name": "Тестовый Синхрон",
            "representative_text": "Представитель: Тест Тестов",
            "narrator_text": "Ведущий: Ведущ Ведущий",
            "start_time": "20.11 17:00",
            "start_time_ts": int(self.initial_dt.timestamp()),
            "teams": [],
            "status": "active",
            "register_teams": True,
        }
        mock_get_active_regs.return_value = [reg_entity]

        mock_get_upcoming.return_value = [
            {
                "id": self.sync_req_id,
                "tourn_id": 5001,
                "status": "A",
                "representative": {"id": 10001, "name": "Тест", "surname": "Тестов"},
                "narrator": {"id": 10002, "name": "Ведущ", "surname": "Ведущий"},
                "dateStart": self.initial_dt,
            }
        ]

        handlers.check_and_update_active_sync_requests(self.monitored_venues)

        self.assertFalse(mock_edit_message.called)
        self.assertFalse(mock_send_message.called)
        self.assertFalse(mock_update_reg.called)


if __name__ == "__main__":
    unittest.main()
