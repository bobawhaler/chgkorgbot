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


if __name__ == "__main__":
    unittest.main()
