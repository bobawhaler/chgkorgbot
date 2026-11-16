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

    def test_webapp_urls_and_markup(self):
        # 1. get_webapp_url with and without start_param
        url1 = helpers.get_webapp_url()
        self.assertIn("/webapp/roster?v=", url1)
        url2 = helpers.get_webapp_url(start_param="roster_123_456")
        self.assertIn("&startapp=roster_123_456", url2)

        # 2. get_webapp_direct_url without app name (standard bot PM link)
        with patch.dict(os.environ, {"TELEGRAM_APP_NAME": ""}):
            direct_pm = helpers.get_webapp_direct_url("roster_123_456")
            self.assertEqual(direct_pm, "https://t.me/gamepollbot?start=roster_123_456")

        # 3. get_webapp_direct_url with app name configured via BotFather
        with patch.dict(os.environ, {"TELEGRAM_APP_NAME": "roster"}):
            direct_app = helpers.get_webapp_direct_url("roster_123_456")
            self.assertEqual(direct_app, "https://t.me/gamepollbot/roster?startapp=roster_123_456")

        # 4. get_roster_inline_markup (web_app button for PM)
        markup_pm = helpers.get_roster_inline_markup(text="📋 Открыть составы")
        self.assertEqual(len(markup_pm["inline_keyboard"]), 1)
        self.assertEqual(markup_pm["inline_keyboard"][0][0]["text"], "📋 Открыть составы")
        self.assertIn("web_app", markup_pm["inline_keyboard"][0][0])
        self.assertIn("/webapp/roster", markup_pm["inline_keyboard"][0][0]["web_app"]["url"])

        # 5. get_group_roster_inline_markup (url button for groups)
        markup_grp = helpers.get_group_roster_inline_markup(start_param="roster_123_456", text="📋 Сдать состав")
        self.assertEqual(len(markup_grp["inline_keyboard"]), 1)
        self.assertEqual(markup_grp["inline_keyboard"][0][0]["text"], "📋 Сдать состав")
        self.assertIn("url", markup_grp["inline_keyboard"][0][0])
        self.assertIn("roster_123_456", markup_grp["inline_keyboard"][0][0]["url"])

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

    @patch("rating_api.search_players")
    @patch("datastore.search_cached_players")
    @patch("datastore.get_venues_team_rosters")
    @patch("datastore.get_chat_config")
    @patch("rating_api.get_team_players")
    @patch("datastore.get_user_history")
    @patch("datastore.get_user_mapping")
    def test_search_players_tiered_case_insensitive(
        self,
        mock_mapping,
        mock_history,
        mock_team_players,
        mock_chat_cfg,
        mock_venue_rosters,
        mock_cached,
        mock_api_search,
    ):
        mock_mapping.return_value = {
            "rating_player_id": 101,
            "name": "Александр",
            "surname": "Замятин",
            "patronymic": "Александрович",
            "town": "Берлин",
        }
        mock_history.return_value = {
            "players": [
                {
                    "player_id": 202,
                    "name": "Артём",
                    "surname": "Фёдоров",
                    "patronymic": "Игоревич",
                    "town": "Мюнхен",
                }
            ]
        }
        mock_team_players.return_value = [
            {
                "id": 303,
                "name": "Галина",
                "surname": "Тер-Акопян",
                "patronymic": "",
                "town": "Ереван",
            }
        ]
        mock_chat_cfg.return_value = {"venues": [3053]}
        mock_venue_rosters.return_value = []
        mock_cached.return_value = []
        mock_api_search.return_value = []

        # Lowercase surname
        res1 = helpers.search_players_tiered("замятин", user_id=1, rating_team_id=10, chat_id=-100)
        self.assertEqual([r["id"] for r in res1], [101])

        # Uppercase reversed order (Name Surname)
        res2 = helpers.search_players_tiered("АЛЕКСАНДР ЗАМЯТИН", user_id=1, rating_team_id=10, chat_id=-100)
        self.assertEqual([r["id"] for r in res2], [101])

        # Mixed case + е/ё normalization
        res3 = helpers.search_players_tiered("артем федоров", user_id=1, rating_team_id=10, chat_id=-100)
        self.assertEqual([r["id"] for r in res3], [202])

        # Hyphenated surname in lowercase
        res4 = helpers.search_players_tiered("тер-акопян", user_id=1, rating_team_id=10, chat_id=-100)
        self.assertEqual([r["id"] for r in res4], [303])

    def test_player_name_capitalization_normalization(self):
        self.assertEqual(helpers.normalize_person_name_part("иван"), "Иван")
        self.assertEqual(helpers.normalize_person_name_part("ИВАНОВ"), "Иванов")
        self.assertEqual(helpers.normalize_person_name_part("тер-акопян"), "Тер-Акопян")
        self.assertEqual(helpers.normalize_person_name_part("  анна-мария  "), "Анна-Мария")
        self.assertEqual(helpers.normalize_person_name_part("Рафик гызы"), "Рафик гызы")
        self.assertEqual(helpers.normalize_person_name_part("рафик гызы"), "Рафик гызы")
        self.assertEqual(helpers.normalize_person_name_part("Али оглы"), "Али оглы")
        self.assertEqual(helpers.normalize_person_name_part("АЛИ ОГЛЫ"), "Али оглы")
        self.assertEqual(helpers.normalize_person_name_part("джамаг-кызы"), "Джамаг-кызы")

        # CSV export should normalize legacy lowercase/uppercase unrated players and preserve гызы/оглы
        teams = [
            {
                "display_name": "Тест",
                "rating_team_id": 123,
                "roster": [
                    {
                        "id": None,
                        "surname": "иванов",
                        "name": "пётр",
                        "patronymic": "сергеевич",
                        "flag": "Б",
                    },
                    {
                        "id": None,
                        "surname": "тер-акопян",
                        "name": "АННА-МАРИЯ",
                        "patronymic": "",
                        "flag": "Л",
                    },
                    {
                        "player_id": 1907,
                        "surname": "Бабаева",
                        "name": "Гюнель",
                        "patronymic": "Рафик гызы",
                        "flag": "Б",
                    },
                    {
                        "player_id": 73840,
                        "surname": "Алиев",
                        "name": "Мирза",
                        "patronymic": "Али оглы",
                        "flag": "Б",
                    },
                ],
            }
        ]
        csv_text = helpers.generate_roster_csv(teams)
        self.assertIn("Иванов;Пётр;Сергеевич", csv_text)
        self.assertIn("Тер-Акопян;Анна-Мария;", csv_text)
        self.assertIn("1907;Бабаева;Гюнель;Рафик гызы;", csv_text)
        self.assertIn("73840;Алиев;Мирза;Али оглы;", csv_text)

    def test_export_roster_new_players_and_teams_no_id(self):
        """Verifies that new players and new teams do NOT have IDs in CSV export."""
        self.assertEqual(helpers.format_export_id(None), "")
        self.assertEqual(helpers.format_export_id(""), "")
        self.assertEqual(helpers.format_export_id("None"), "")
        self.assertEqual(helpers.format_export_id("null"), "")
        self.assertEqual(helpers.format_export_id(0), "")
        self.assertEqual(helpers.format_export_id("0"), "")
        self.assertEqual(helpers.format_export_id(-1), "")
        self.assertEqual(helpers.format_export_id(346682), "346682")
        self.assertEqual(helpers.format_export_id("109054"), "109054")

        # Test full CSV generation with a new unrated team and new unrated players
        teams = [
            {
                "team_name": "Новая Команда",
                "display_name": "Новая Команда",
                "rating_team_id": None,
                "town": "Берлин",
                "roster": [
                    {
                        "player_id": None,
                        "surname": "Новичков",
                        "name": "Иван",
                        "patronymic": "",
                        "status": "Б",
                    },
                    {
                        "player_id": "None",  # corrupted/string None
                        "surname": "Новичкова",
                        "name": "Анна",
                        "patronymic": "Сергеевна",
                        "status": "К",
                    },
                    {
                        "player_id": 12345,  # rated player
                        "surname": "Опытный",
                        "name": "Пётр",
                        "patronymic": "Ильич",
                        "status": "Л",
                    },
                ],
            }
        ]
        csv_text = helpers.generate_roster_csv(teams)
        lines = csv_text.splitlines()
        self.assertEqual(len(lines), 3)

        # 1st row: new team (idteam is empty), new player (idplayer is empty)
        self.assertEqual(lines[0], ";Новая Команда;Берлин;Б;;Новичков;Иван;;")
        # 2nd row: new team (idteam is empty), new player with "None" string (idplayer is empty)
        self.assertEqual(lines[1], ";Новая Команда;Берлин;К;;Новичкова;Анна;Сергеевна;")
        # 3rd row: new team (idteam is empty), rated player (idplayer is 12345)
        self.assertEqual(lines[2], ";Новая Команда;Берлин;Л;12345;Опытный;Пётр;Ильич;")

    @patch("telegram_api.edit_message_text")
    @patch("telegram_api.set_message_reaction")
    @patch("telegram_api.send_message")
    @patch("datastore.get_team_registration_by_team_msg")
    @patch("datastore.find_user_team_registration")
    @patch("datastore.register_team_in_ds")
    @patch("helpers.get_chat_collect_rosters")
    def test_edited_message_renames_team(
        self,
        mock_collect_rosters,
        mock_register_team,
        mock_find_user_reg,
        mock_get_reg_by_team_msg,
        mock_send_msg,
        mock_reaction,
        mock_edit_msg,
    ):
        import handlers

        mock_collect_rosters.return_value = True
        reg = {
            "sync_req_id": "12345",
            "message_id": 999,
            "tourn_name": "Весенний кубок",
            "thread_id": 10,
        }
        mock_get_reg_by_team_msg.return_value = reg
        mock_register_team.return_value = {
            "teams": [{"team_name": "Новое название", "user_id": 777}]
        }

        body = {
            "edited_message": {
                "message_id": 555,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 777, "username": "captain", "first_name": "Cap"},
                "text": "Новое название",
            }
        }

        handlers.command_handler(body)

        mock_register_team.assert_called_once_with(
            -1001, "12345", "Новое название", 777, "captain", message_id=555
        )
        mock_edit_msg.assert_called_once()
        mock_reaction.assert_called_once_with(-1001, 555, "❤️")
        # On edit, prompt for roster should NOT be sent
        mock_send_msg.assert_not_called()

    @patch("telegram_api.edit_message_text")
    @patch("telegram_api.set_message_reaction")
    @patch("telegram_api.send_message")
    @patch("datastore.find_user_team_registration")
    @patch("datastore.unregister_team_in_ds")
    @patch("helpers.get_chat_collect_rosters")
    def test_unregister_command_success(
        self,
        mock_collect_rosters,
        mock_unregister_team,
        mock_find_user_reg,
        mock_send_msg,
        mock_reaction,
        mock_edit_msg,
    ):
        import handlers

        mock_collect_rosters.return_value = True
        reg = {
            "sync_req_id": "12345",
            "message_id": 999,
            "tourn_name": "Весенний кубок",
            "thread_id": 10,
        }
        team_info = {"team_name": "Победа", "display_name": "Победа", "user_id": 777}
        mock_find_user_reg.return_value = (reg, team_info)
        mock_unregister_team.return_value = {"teams": []}

        body = {
            "message": {
                "message_id": 666,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 777, "username": "captain", "first_name": "Cap"},
                "text": "/unregister",
            }
        }

        handlers.command_handler(body)

        mock_find_user_reg.assert_called_once_with(-1001, 777, sync_req_id=None)
        mock_unregister_team.assert_called_once_with(-1001, "12345", 777)
        mock_edit_msg.assert_called_once()
        mock_reaction.assert_called_once_with(-1001, 666, "❤️")
        mock_send_msg.assert_called_once()
        call_args = mock_send_msg.call_args[0]
        self.assertIn("Регистрация команды", call_args[2])
        self.assertIn("Победа", call_args[2])
        self.assertIn("отменена", call_args[2])

    @patch("telegram_api.send_message")
    @patch("datastore.find_user_team_registration")
    def test_unregister_command_no_registration(
        self,
        mock_find_user_reg,
        mock_send_msg,
    ):
        import handlers

        mock_find_user_reg.return_value = (None, None)

        body = {
            "message": {
                "message_id": 777,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 888, "username": "nobody", "first_name": "Nobody"},
                "text": "/unregister",
            }
        }

        handlers.command_handler(body)

        mock_find_user_reg.assert_called_once_with(-1001, 888, sync_req_id=None)
        mock_send_msg.assert_called_once()
        call_args = mock_send_msg.call_args[0]
        self.assertIn("У вас нет активной зарегистрированной команды", call_args[2])

    @patch("datastore.cleanup_old_chat_commands")
    @patch("datastore.cleanup_old_cached_tournaments")
    @patch("datastore.update_team_registration")
    @patch("telegram_api.send_message")
    @patch("helpers.get_registration_start_ts")
    @patch("helpers.get_chat_collect_rosters")
    @patch("datastore.get_all_active_registrations")
    @patch("datastore.cleanup_old_registrations")
    @patch("datastore.get_monitored_venues")
    @patch("datastore.traverse_finished_tasks")
    def test_system_tic_handler_roster_reminder(
        self,
        mock_traverse_tasks,
        mock_monitored_venues,
        mock_cleanup_regs,
        mock_get_active_regs,
        mock_collect_rosters,
        mock_start_ts,
        mock_send_msg,
        mock_update_reg,
        mock_cleanup_cached,
        mock_cleanup_cmds,
    ):
        import time
        import handlers

        mock_traverse_tasks.return_value = []
        mock_monitored_venues.return_value = {}  # Empty venues so chat_id is not bound earlier
        mock_collect_rosters.return_value = True

        now_ts = int(time.time())
        mock_start_ts.return_value = now_ts - 3600  # Started 1 hour ago

        reg = {
            "chat_id": -1001234567,
            "sync_req_id": "98765",
            "tourn_name": "Тестовый кубок",
            "teams": [
                {
                    "user_id": 555,
                    "team_name": "Знатоки",
                    "roster_submitted": False,
                    "roster": [],
                    "reminders_count": 0,
                }
            ],
        }
        mock_get_active_regs.return_value = [reg]

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_send_msg.return_value = mock_resp

        handlers.system_tic_handler()

        mock_send_msg.assert_called_once()
        args, kwargs = mock_send_msg.call_args
        self.assertEqual(args[0], 555)  # recipient user_id
        self.assertIn("Напоминание о сдаче состава", args[2])
        reply_markup = kwargs.get("reply_markup")
        self.assertIsNotNone(reply_markup)
        web_app_url = reply_markup["inline_keyboard"][0][0]["web_app"]["url"]
        self.assertIn("startapp=roster_98765_-1001234567", web_app_url)
        mock_update_reg.assert_called_once_with(reg)
        self.assertEqual(reg["teams"][0]["reminders_count"], 1)

    def test_is_valid_team_name(self):
        self.assertTrue(helpers.is_valid_team_name("Победа"))
        self.assertTrue(helpers.is_valid_team_name("Знатоки"))
        self.assertTrue(helpers.is_valid_team_name("Что? Где? Когда?"))
        self.assertTrue(helpers.is_valid_team_name("Принцесса Апелляции на снятие в другом замке"))

        # Rejects empty / whitespace / commands
        self.assertFalse(helpers.is_valid_team_name(""))
        self.assertFalse(helpers.is_valid_team_name("   "))
        self.assertFalse(helpers.is_valid_team_name("/unregister"))
        self.assertFalse(helpers.is_valid_team_name("/test"))

        # Rejects multiline discussions
        self.assertFalse(helpers.is_valid_team_name("Line 1\nLine 2"))
        self.assertFalse(helpers.is_valid_team_name("Paragraph 1\r\nParagraph 2"))

        # Rejects URLs
        self.assertFalse(helpers.is_valid_team_name("Team https://rating.chgk.info"))
        self.assertFalse(helpers.is_valid_team_name("Check http://example.com"))
        self.assertFalse(helpers.is_valid_team_name("Join t.me/mychat"))

        # Rejects long messages (>100 chars)
        self.assertFalse(helpers.is_valid_team_name("A" * 101))

    def test_is_registration_open(self):
        import time
        now_ts = int(time.time())

        # Active tournament starting in the future
        reg_future = {"status": "active", "start_time_ts": now_ts + 3600}
        self.assertTrue(helpers.is_registration_open(reg_future))

        # Active tournament that started 1 hour ago
        reg_recent = {"status": "active", "start_time_ts": now_ts - 3600}
        self.assertTrue(helpers.is_registration_open(reg_recent))

        # Active tournament that started 4 hours ago (game finished)
        reg_past = {"status": "active", "start_time_ts": now_ts - (4 * 3600)}
        self.assertFalse(helpers.is_registration_open(reg_past))

        # Canceled / archived / closed tournament
        reg_canceled = {"status": "canceled", "start_time_ts": now_ts + 3600}
        self.assertFalse(helpers.is_registration_open(reg_canceled))
        reg_archived = {"status": "archived", "start_time_ts": now_ts + 3600}
        self.assertFalse(helpers.is_registration_open(reg_archived))
        reg_closed = {"status": "closed", "start_time_ts": now_ts + 3600}
        self.assertFalse(helpers.is_registration_open(reg_closed))

    @patch("datastore.register_team_in_ds")
    @patch("datastore.get_team_registration_by_msg")
    @patch("datastore.get_team_registration_by_team_msg")
    def test_reply_to_another_team_message_does_not_register_team(
        self,
        mock_get_reg_by_team_msg,
        mock_get_reg_by_msg,
        mock_register_team,
    ):
        import handlers

        # Replying to a teammate or other user's message (which has a team message id)
        # mock_get_reg_by_msg returns None (because it's not the bot's announcement)
        mock_get_reg_by_msg.return_value = None
        mock_get_reg_by_team_msg.return_value = {
            "sync_req_id": "12345",
            "message_id": 100,
            "status": "active",
        }

        body = {
            "message": {
                "message_id": 500,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 999, "username": "chatty_user"},
                "text": "Я хз, в регламенте написано что-то другое...",
                "reply_to_message": {"message_id": 200},
            }
        }

        handlers.command_handler(body)

        mock_register_team.assert_not_called()

    @patch("datastore.register_team_in_ds")
    @patch("datastore.find_user_team_registration")
    @patch("datastore.get_team_registration_by_team_msg")
    def test_editing_unrelated_message_does_not_rename_team(
        self,
        mock_get_reg_by_team_msg,
        mock_find_user_reg,
        mock_register_team,
    ):
        import handlers

        # User 777 has a registered team in the chat, but edits an unrelated message (id 999)
        mock_get_reg_by_team_msg.return_value = None
        mock_find_user_reg.return_value = (
            {"sync_req_id": "12345", "status": "active"},
            {"team_name": "Знатоки", "user_id": 777, "message_id": 555},
        )

        body = {
            "edited_message": {
                "message_id": 999,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 777, "username": "captain"},
                "text": "Кто пойдет пить кофе?",
            }
        }

        handlers.command_handler(body)

        mock_register_team.assert_not_called()

    @patch("datastore.register_team_in_ds")
    @patch("datastore.get_team_registration_by_msg")
    def test_multiline_reply_rejected_as_team_name(
        self,
        mock_get_reg_by_msg,
        mock_register_team,
    ):
        import handlers

        mock_get_reg_by_msg.return_value = {
            "sync_req_id": "12345",
            "message_id": 100,
            "status": "active",
        }

        # User replies to tournament announcement with a multiline discussion
        body = {
            "message": {
                "message_id": 500,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 888, "username": "discussing_user"},
                "text": "Я хз, в регламенте написано:\n3.8 На турнире разрешены апелляции\nhttps://rating.chgk.info",
                "reply_to_message": {"message_id": 100},
            }
        }

        handlers.command_handler(body)

        mock_register_team.assert_not_called()

    @patch("datastore.register_team_in_ds")
    @patch("datastore.get_team_registration_by_msg")
    def test_reply_after_tournament_ended_does_not_register_team(
        self,
        mock_get_reg_by_msg,
        mock_register_team,
    ):
        import time
        import handlers

        now_ts = int(time.time())
        # Tournament ended 5 hours ago
        mock_get_reg_by_msg.return_value = {
            "sync_req_id": "12345",
            "message_id": 100,
            "status": "active",
            "start_time_ts": now_ts - 5 * 3600,
        }

        body = {
            "message": {
                "message_id": 501,
                "chat": {"id": -1001, "type": "supergroup"},
                "from": {"id": 777, "username": "late_user"},
                "text": "Опоздавшие",
                "reply_to_message": {"message_id": 100},
            }
        }

        handlers.command_handler(body)

        mock_register_team.assert_not_called()


if __name__ == "__main__":
    unittest.main()
