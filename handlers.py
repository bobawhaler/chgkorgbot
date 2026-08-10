import traceback
import json
import time
import pytz

import rating_api
import telegram_api
import helpers
import datastore
import debug


def build_help_text(chat_id, is_private=True):
    chat_config = datastore.get_chat_config(chat_id) or {}
    collect_teams = chat_config.get("collect_teams", True)

    sections = ["📖 <b>Справка по командам бота</b>"]

    if is_private:
        sections.append(
            "<b>Турниры и расписание:</b>\n"
            "• <code>/tourns &lt;дата/время&gt;</code> — расписание ближайших турниров\n"
            "• <code>/rtourns &lt;дата/время&gt;</code> — расписание рейтингуемых турниров\n"
            "• <code>/poll &lt;1,2...&gt;</code> — опрос по выбранным турнирам\n"
            "• <code>/feedback</code> — опрос впечатлений о сыгранном пакете"
        )
        if collect_teams:
            sections.append(
                "<b>Управление составами команд:</b>\n"
                "• <code>/roster</code> (или <code>/myteams</code>) — меню управления составом вашей команды\n"
                "• <code>/rosters</code> (или <code>/exportroster</code>, <code>/csv</code>) — статус сбора составов и скачивание CSV для сайта рейтинга\n"
                "• <code>/setmyid &lt;id_или_ФИО&gt;</code> (или <code>/myid</code>) — привязка вашего профиля на сайте рейтинга\n"
                "• <code>/cancel</code> (или <code>/stop</code>, <code>отмена</code>) — выход из любого режима ввода текста"
            )
        sections.append(
            "<b>Настройки:</b>\n"
            "• <code>/setcollectteams &lt;on|off&gt;</code> — включение/выключение сбора заявок команд\n"
            "• <code>/setvenues &lt;id1,id2...&gt;</code> — настройка мониторинга площадок\n"
            "• <code>/settimezone &lt;tz&gt;</code> — часовой пояс чата (напр. Europe/Moscow)\n"
            "• <code>/setmindifficulty &lt;N&gt;</code> — мин. сложность турниров\n"
            "• <code>/setmaxdifficulty &lt;N&gt;</code> — макс. сложность турниров"
        )
        sections.append(
            "<b>Системные:</b>\n"
            "• <code>/help</code> — эта справка"
        )
    else:
        if collect_teams:
            sections.append(
                "<b>Регистрация команд:</b>\n"
                "• <b>Reply с названием команды</b> на анонс турнира — зарегистрировать команду\n"
                "• <code>/unregister</code> или <code>отмена</code> (как reply) — отменить регистрацию команды\n"
                "• <code>/rosters</code> (или <code>/exportroster</code>, <code>/csv</code>) — статус сбора составов и скачивание CSV для сайта рейтинга"
            )
        sections.append(
            "<b>Голосования и турниры:</b>\n"
            "• <code>/tourns &lt;дата/время&gt;</code> — расписание турниров на дату\n"
            "• <code>/rtourns &lt;дата/время&gt;</code> — расписание только рейтингуемых турниров\n"
            "• <code>/poll &lt;1,2,...&gt; [title] [до время]</code> — опрос по выбранным турнирам\n"
            "• <code>/stop</code> (reply на опрос) — подвести итоги и завершить опрос\n"
            "• <code>/cancel</code> (reply on опрос) — отменить опрос\n"
            "• <code>/feedback</code> — опрос впечатлений о сыгранном пакете"
        )
        sections.append(
            "<b>Настройки чата:</b>\n"
            "• <code>/setcollectteams &lt;on|off&gt;</code> — включение/выключение сбора заявок от команд\n"
            "• <code>/setvenues &lt;id1,id2...&gt;</code> — настройка мониторинга площадок\n"
            "• <code>/settimezone &lt;tz&gt;</code> — часовой пояс чата\n"
            "• <code>/setmindifficulty &lt;N&gt;</code> / <code>/setmaxdifficulty &lt;N&gt;</code> — фильтр сложности"
        )

    return "\n\n".join(sections)


def system_tic_handler():
    t0 = time.perf_counter()
    # telegram_api.set_webhook()
    
    for task, multiple_candidates in datastore.traverse_finished_tasks():
        thread_id = None
        chat_config = datastore.get_chat_config(task["chat_id"])
        if chat_config:
            thread_id = chat_config.get("thread_id", None)
            
        telegram_api.finalize_poll(
            task["chat_id"],
            thread_id,
            task["message_id"],
            task.get("tourn_ids", []),
            with_results=task.get("with_results", False),
            multiple_candidates=multiple_candidates,
        )
        
    monitored_venues = datastore.get_monitored_venues()
    for venue_id, chat_ids in monitored_venues.items():
        sync_reqs = rating_api.get_new_sync_requests(venue_id)
        if not sync_reqs:
            continue
            
        for sync_req in sync_reqs:
            if datastore.is_known_sync_request(sync_req["id"]):
                continue

            tourn = rating_api.get_tourn_by_id(sync_req["tourn_id"])
            if not tourn or not tourn.get("name"):
                continue
            
            datastore.add_known_sync_request(sync_req["id"])
            tourn_name = tourn["name"]
            
            representative_form, representative_is_feminine = (
                helpers.get_person_form(sync_req["representative"])
            )
            if representative_is_feminine:
                representative_text = "Представительница: " + representative_form
            else:
                representative_text = "Представитель: " + representative_form

            narrator_form, narrator_is_feminine = helpers.get_person_form(
                sync_req["narrator"]
            )
            if narrator_is_feminine:
                narrator_text = "Ведущая: " + narrator_form
            else:
                narrator_text = "Ведущий: " + narrator_form

            url = f'https://rating.chgk.info/tournament/{sync_req["tourn_id"]}'
            
            for chat_id in chat_ids:
                chat_config = datastore.get_chat_config(chat_id) or {}
                start_time = (
                    sync_req["dateStart"]
                    .astimezone(
                        pytz.timezone(
                            helpers.resolve_timezone(chat_config.get("timezone"))
                        )
                    )
                    .strftime("%d.%m %H:%M")
                )
                
                start_time_ts = int(sync_req["dateStart"].timestamp())
                
                thread_id = chat_config.get("thread_id", None)
                if chat_config.get("collect_teams"):
                    msg_text = helpers.format_team_registration_text(
                        tourn_name, url, representative_text, narrator_text, start_time, []
                    )
                    resp = telegram_api.send_message(
                        int(chat_id),
                        thread_id,
                        msg_text,
                        formatted=True,
                    )
                    if resp and resp.ok:
                        res_data = resp.json().get("result", {})
                        message_id = res_data.get("message_id")
                        if message_id:
                            datastore.add_team_registration(
                                int(chat_id),
                                thread_id,
                                sync_req["id"],
                                message_id,
                                tourn_name,
                                representative_text,
                                narrator_text,
                                start_time,
                                start_time_ts=start_time_ts,
                            )
                else:
                    telegram_api.send_formatted_message(
                        int(chat_id),
                        thread_id,
                        f'Подана заявка на <a href="{url}">"{tourn_name}"</a>. {representative_text}. {narrator_text}. Начало: {start_time}',
                    )

    # Check automated roster reminders
    now_ts = int(time.time())
    active_regs = datastore.get_all_active_registrations()
    for reg in active_regs:
        start_ts = helpers.get_registration_start_ts(reg)
        if start_ts and now_ts < start_ts:
            # Tournament has not started yet! Do not send any reminders before start_ts.
            continue

        teams = reg.get("teams", [])
        updated_teams = False
        for t in teams:
            if not t.get("roster_submitted") and not t.get("roster"):
                rem_count = t.get("reminders_count", 0)
                if rem_count >= 10:
                    # Maximum 10 automated reminders reached; representative can send manual reminders if needed.
                    continue
                last_rem = t.get("last_reminder_ts", 0)

                should_remind = False
                if rem_count == 0:
                    should_remind = True
                else:
                    # Subsequent reminders follow intervals after 1st reminder
                    interval_map = {1: 3000, 2: 3000, 3: 3000, 4: 1800, 5: 900}
                    req_interval = interval_map.get(rem_count, 900)
                    if now_ts - last_rem >= req_interval:
                        should_remind = True

                if should_remind:
                    user_id = t.get("user_id")
                    team_name = t.get("display_name") or t.get("team_name")
                    tourn_name = reg.get("tourn_name")
                    msg_text = (
                        f"⏰ <b>Напоминание о сдаче состава!</b>\n\n"
                        f"Наступило время турнира <b>\"{tourn_name}\"</b>.\n"
                        f"Пожалуйста, сдайте состав вашей команды <b>\"{team_name}\"</b>.\n\n"
                        f"Отправьте /roster в этот личный чат с ботом для вызова меню выбора и забора состава."
                    )
                    pm_res = telegram_api.send_message(user_id, None, msg_text, formatted=True)
                    t["reminders_count"] = rem_count + 1
                    t["last_reminder_ts"] = now_ts
                    updated_teams = True
        if updated_teams:
            datastore.update_team_registration(reg)

    datastore.cleanup_old_cached_tournaments()
    debug.log("system_tic_handler", t0)


# --- PM Roster UI Renderers ---

def render_team_selection_ui(chat_id, user_id, context_data, message_id=None):
    history = datastore.get_user_history(user_id)
    history_teams = history.get("teams", [])

    lines = [
        "👥 <b>Идентификация команды</b>",
        f"Турнир: <b>{context_data.get('tourn_name', '')}</b>",
        "",
        "Выберите команду из вашей истории заявлявшихся ранее или найдите ее на сайте рейтинга:"
    ]

    keyboard = []
    for t in history_teams[:5]:
        tid = t.get("team_id") or 0
        tname = t.get("name", "Команда")
        keyboard.append([{"text": f"🏆 {tname}", "callback_data": f"roster:team_select:{tid}"}])

    keyboard.append([{"text": "🔍 Найти команду в рейтинге", "callback_data": "roster:team_search"}])
    keyboard.append([{"text": "➕ Новая команда (без ID)", "callback_data": "roster:team_new"}])

    reply_markup = {"inline_keyboard": keyboard}
    text = "\n".join(lines)

    if message_id:
        telegram_api.edit_message_text(chat_id, message_id, text, formatted=True, reply_markup=reply_markup)
    else:
        telegram_api.send_message(chat_id, None, text, formatted=True, reply_markup=reply_markup)


def render_roster_ui(chat_id, user_id, context_data, message_id=None):
    team_name = context_data.get("team_name", "Команда")
    display_name = context_data.get("display_name", team_name)
    rating_team_id = context_data.get("rating_team_id")
    roster = context_data.get("roster", [])

    team_info = f"Команда: <b>{display_name}</b>"
    if rating_team_id:
        team_info += f" (Рейтинг ID: {rating_team_id})"
    elif display_name != team_name:
        team_info += f" (изн: {team_name})"

    town = context_data.get("town", "")
    town_info = f"Город: <b>{town}</b>" if town else "Город: <i>не указан</i>"

    lines = [
        "📋 <b>Управление составом команды</b>",
        team_info,
        town_info,
        "",
        "<b>Текущий состав:</b>"
    ]
    
    status_symbols = {"K": "👑 [К]", "B": "🛡 [Б]", "L": "⚔️ [Л]"}
    
    if not roster:
        lines.append("<i>(состав пока пуст)</i>")
    else:
        for idx, p in enumerate(roster, 1):
            st = status_symbols.get(p.get("status", "B"), "🛡 [Б]")
            pid_str = f" (ID: {p['player_id']})" if p.get("player_id") else " (без ID)"
            p_name = f"{p.get('name', '')} {p.get('surname', '')}".strip() or p.get("display_name", "Игрок")
            lines.append(f"{idx}. {st} {p_name}{pid_str}")

    lines.append("\n<i>Нажмите на статус игрока для смены (К - капитан, Б - базовый, Л - легионер) или добавляйте игроков из подсказок:</i>")

    keyboard = []
    
    # 1. Roster player buttons
    for idx, p in enumerate(roster):
        p_name = f"{p.get('name', '')} {p.get('surname', '')}".strip() or f"Игрок {idx+1}"
        cur_st = p.get("status", "B")
        next_st_map = {"K": "B", "B": "L", "L": "K"}
        next_st = next_st_map.get(cur_st, "B")
        st_symbol = status_symbols.get(cur_st, "[Б]")
        keyboard.append([
            {"text": f"{st_symbol} {p_name}", "callback_data": f"roster:set_status:{idx}:{next_st}"},
            {"text": "❌", "callback_data": f"roster:rem_player:{idx}"}
        ])

    # 2. Hints with ranking based on team activity & user history frequency + recency
    roster_pids = {p["player_id"] for p in roster if p.get("player_id")}
    roster_names = {f"{p.get('surname', '')} {p.get('name', '')}".lower() for p in roster}

    candidates_map = {}

    # Self profile
    user_mapping = datastore.get_user_mapping(user_id)
    if user_mapping and user_mapping.get("rating_player_id"):
        my_pid = user_mapping["rating_player_id"]
        my_pname = f"{user_mapping.get('surname', '')} {user_mapping.get('name', '')}".strip()
        if my_pid not in roster_pids and my_pname.lower() not in roster_names:
            key = f"pid_{my_pid}"
            candidates_map[key] = {
                "pid": my_pid,
                "name": user_mapping.get("name", ""),
                "surname": user_mapping.get("surname", ""),
                "patronymic": user_mapping.get("patronymic", ""),
                "town": user_mapping.get("town", ""),
                "pname": my_pname,
                "is_self": True,
                "score": 10000.0,
            }

    # Team players (recent seasons & tournaments)
    if rating_team_id:
        team_players = rating_api.get_team_players(rating_team_id)
        for tp in team_players:
            pid = tp.get("id") or tp.get("player_id")
            pname = f"{tp.get('surname', '')} {tp.get('name', '')}".strip()
            if (pid and pid in roster_pids) or (pname.lower() in roster_names):
                continue
            key = f"pid_{pid}" if pid else f"name_{pname.lower()}"
            if key not in candidates_map:
                candidates_map[key] = {
                    "pid": pid,
                    "name": tp.get("name", ""),
                    "surname": tp.get("surname", ""),
                    "patronymic": tp.get("patronymic", ""),
                    "town": tp.get("town", ""),
                    "pname": pname,
                    "score": 0.0,
                }
            t_score = 0.0
            t_score += tp.get("tourn_count", 0) * 50.0
            t_score += tp.get("tourn_recency", 0) * 20.0
            t_score += tp.get("season_recency", 0) * 100.0
            candidates_map[key]["score"] += t_score

    # User history players (frequently and recently entered by user)
    history = datastore.get_user_history(user_id)
    history_players = history.get("players", [])
    now_ts = time.time()

    for idx, hp in enumerate(history_players):
        pid = hp.get("player_id") or hp.get("id")
        pname = f"{hp.get('surname', '')} {hp.get('name', '')}".strip()
        if (pid and pid in roster_pids) or (pname.lower() in roster_names):
            continue
        key = f"pid_{pid}" if pid else f"name_{pname.lower()}"
        if key not in candidates_map:
            candidates_map[key] = {
                "pid": pid,
                "name": hp.get("name", ""),
                "surname": hp.get("surname", ""),
                "patronymic": hp.get("patronymic", ""),
                "town": hp.get("town", ""),
                "pname": pname,
                "score": 0.0,
            }
        h_score = 0.0
        use_count = hp.get("count", 1)
        h_score += use_count * 40.0
        h_score += max(0.0, 100.0 - idx * 5.0)

        last_used_str = hp.get("last_used")
        if last_used_str:
            try:
                import datetime
                lu_dt = datetime.datetime.fromisoformat(last_used_str)
                age_days = (now_ts - lu_dt.timestamp()) / 86400.0
                if age_days <= 1.0:
                    h_score += 150.0
                elif age_days <= 7.0:
                    h_score += 100.0
                elif age_days <= 30.0:
                    h_score += 50.0
                elif age_days <= 365.0:
                    h_score += 20.0
            except Exception:
                pass

        candidates_map[key]["score"] += h_score

    candidates = list(candidates_map.values())
    candidates.sort(key=lambda c: c["score"], reverse=True)
    candidates = candidates[:36]

    if candidates:
        page_size = 6
        total_candidates = len(candidates)
        hints_page = context_data.get("hints_page", 0)
        total_pages = (total_candidates + page_size - 1) // page_size
        if hints_page >= total_pages:
            hints_page = 0
            context_data["hints_page"] = 0
            
        start_idx = hints_page * page_size
        page_candidates = candidates[start_idx : start_idx + page_size]
        
        header_text = f"--- Подсказки ({start_idx+1}-{min(start_idx+page_size, total_candidates)} из {total_candidates}) ---"
        keyboard.append([{"text": header_text, "callback_data": "roster:noop"}])
        
        for c in page_candidates:
            c_fio = f"{c['surname']} {c['name']}".strip()
            if c.get('patronymic'):
                c_fio += f" {c['patronymic'][0]}."
            c_town = f" ({c['town']})" if c.get("town") else ""
            if c.get("is_self"):
                c_label = f"➕ Добавить себя ([ID {c['pid']}] {c_fio}{c_town})"
            else:
                c_label = f"➕ [ID {c['pid']}] {c_fio}{c_town}" if c.get("pid") else f"➕ {c_fio}"
            cb = f"roster:add_hint:{c['pid'] or 0}"
            keyboard.append([{"text": c_label, "callback_data": cb}])
            
        if total_pages > 1:
            page_nav = []
            if hints_page > 0:
                page_nav.append({"text": "⬅️ Назад", "callback_data": f"roster:hints_page:{hints_page - 1}"})
            if hints_page < total_pages - 1:
                rem = total_candidates - (start_idx + page_size)
                page_nav.append({"text": f"➡️ Еще подсказки ({rem})", "callback_data": f"roster:hints_page:{hints_page + 1}"})
            if page_nav:
                keyboard.append(page_nav)

    # 3. Action buttons
    keyboard.append([
        {"text": "🔍 Поиск игрока", "callback_data": "roster:search_player"},
        {"text": "➕ Игрок без ID", "callback_data": "roster:add_unrated"}
    ])
    keyboard.append([
        {"text": "✏️ Сменить название", "callback_data": "roster:rename_team"},
        {"text": "🏙 Изменить город", "callback_data": "roster:edit_town"}
    ])
    keyboard.append([
        {"text": "🔄 Сменить базовую команду", "callback_data": "roster:change_base_team"}
    ])
    keyboard.append([
        {"text": "✅ Сохранить состав", "callback_data": "roster:save"}
    ])

    reply_markup = {"inline_keyboard": keyboard}
    text = "\n".join(lines)
    
    if message_id:
        telegram_api.edit_message_text(chat_id, message_id, text, formatted=True, reply_markup=reply_markup)
    else:
        telegram_api.send_message(chat_id, None, text, formatted=True, reply_markup=reply_markup)


def handle_callback_query(cq):
    cq_id = cq["id"]
    user_id = cq["from"]["id"]
    chat_id = user_id
    data = cq.get("data", "")
    message_id = cq.get("message", {}).get("message_id")

    if not data.startswith("roster:"):
        telegram_api.answer_callback_query(cq_id)
        return

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "noop":
        telegram_api.answer_callback_query(cq_id)
        return

    if action in ("team_hist", "team_select", "sel_reg"):
        telegram_api.answer_callback_query(cq_id, text="⏳ Загрузка состава команды...")
    else:
        telegram_api.answer_callback_query(cq_id)

    telegram_api.send_chat_action(chat_id, "typing")

    state_name, context_data = datastore.get_user_state(user_id)

    if action == "sel_reg":
        if len(parts) >= 4:
            sync_req_id = parts[2]
            group_chat_id = int(parts[3])
            context_data = {
                "sync_req_id": sync_req_id,
                "chat_id": group_chat_id,
                "roster": [],
                "team_name": "",
                "display_name": "",
                "rating_team_id": None,
            }
            reg = datastore.get_team_registration(group_chat_id, sync_req_id)
            if reg:
                context_data["tourn_name"] = reg.get("tourn_name", "")
                for t in reg.get("teams", []):
                    if t.get("user_id") == user_id:
                        context_data["team_name"] = t.get("team_name", "")
                        context_data["display_name"] = t.get("display_name", t.get("team_name", ""))
                        context_data["rating_team_id"] = t.get("rating_team_id")
                        context_data["roster"] = list(t.get("roster", []))
                        if t.get("town"):
                            context_data["town"] = t.get("town")
                        break

            if context_data.get("team_name"):
                render_roster_ui(chat_id, user_id, context_data, message_id)
                datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
            else:
                render_team_selection_ui(chat_id, user_id, context_data, message_id)
                datastore.set_user_state(user_id, "SELECTING_TEAM", context_data)

    elif action in ("team_hist", "team_select"):
        team_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() and int(parts[2]) > 0 else None
        town = ""
        if team_id:
            team_info = rating_api.get_team_by_id(team_id)
            team_name = team_info.get("name", "Команда")
            town = team_info.get("town", "")
        else:
            team_name = context_data.get("searched_team_name") or (parts[3] if len(parts) > 3 else "Команда")
        context_data["rating_team_id"] = team_id
        context_data["team_name"] = team_name
        context_data["display_name"] = team_name
        if town:
            context_data["town"] = town

        roster = context_data.get("roster", [])
        if roster:
            new_roster = []
            for p in roster:
                pid = p.get("player_id")
                new_st = helpers.determine_player_default_status(team_id, pid, new_roster)
                p_copy = dict(p)
                p_copy["status"] = new_st
                new_roster.append(p_copy)
            context_data["roster"] = new_roster

        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        render_roster_ui(chat_id, user_id, context_data, message_id)

    elif action == "change_base_team":
        render_team_selection_ui(chat_id, user_id, context_data, message_id)
        datastore.set_user_state(user_id, "SELECTING_TEAM", context_data)

    elif action == "edit_town":
        msg_text = (
            "🏙 <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"Текущий город: <b>{context_data.get('town', 'не указан')}</b>\n\n"
            "Пришлите новый город команды в ответ на это сообщение:\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "❌ Отменить ввод / Назад", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_TOWN", context_data)

    elif action == "rename_team":
        msg_text = (
            "✏️ <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"Текущее название: <b>{context_data.get('display_name', 'Команда')}</b>\n\n"
            "Пришлите новое название команды для этого турнира в ответ на это сообщение:\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "❌ Отменить ввод / Назад", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_DISPLAY_NAME", context_data)

    elif action == "team_search":
        msg_text = (
            "🔍 <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Пришлите название команды или ее ID в рейтинге для поиска:\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "❌ Отменить поиск / Назад", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_SEARCH_TEAM", context_data)

    elif action == "team_new":
        msg_text = (
            "✍️ <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Пришлите название новой команды:\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "❌ Отменить ввод / Назад", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_NEW_TEAM", context_data)

    elif action == "add_hint":
        pid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() and int(parts[2]) > 0 else None
        name = ""
        surname = ""
        patronymic = ""
        if pid:
            p_info = rating_api.get_player_by_id(pid)
            if p_info:
                name = p_info.get("name", "")
                surname = p_info.get("surname", "")
                patronymic = p_info.get("patronymic", "")
        if not surname and len(parts) > 4:
            name = parts[3]
            surname = parts[4]

        roster = context_data.get("roster", [])
        if not any((pid and p.get("player_id") == pid) or (surname and p.get("surname", "").lower() == surname.lower() and p.get("name", "").lower() == name.lower()) for p in roster):
            rating_team_id = context_data.get("rating_team_id")
            status = helpers.determine_player_default_status(rating_team_id, pid, roster)
            roster.append({
                "player_id": pid,
                "name": name,
                "surname": surname,
                "patronymic": patronymic,
                "status": status,
            })
            context_data["roster"] = roster
            datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
            
        render_roster_ui(chat_id, user_id, context_data, message_id)

    elif action == "set_status":
        idx = int(parts[2])
        new_status = parts[3]
        roster = context_data.get("roster", [])
        if 0 <= idx < len(roster):
            roster[idx]["status"] = new_status
            context_data["roster"] = roster
            datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        render_roster_ui(chat_id, user_id, context_data, message_id)

    elif action == "rem_player":
        idx = int(parts[2])
        roster = context_data.get("roster", [])
        if 0 <= idx < len(roster):
            roster.pop(idx)
            context_data["roster"] = roster
            datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        render_roster_ui(chat_id, user_id, context_data, message_id)

    elif action == "search_player":
        msg_text = (
            "🔍 <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Пришлите ФИО или ID игрока для поиска в рейтинге:\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "📋 Вернуться к составу", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_SEARCH_PLAYER", context_data)

    elif action == "add_unrated":
        msg_text = (
            "✍️ <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Пришлите Имя и Фамилию игрока (без ID):\n"
            "<i>(или нажмите кнопку ниже для отмены)</i>"
        )
        keyboard = [[{"text": "📋 Вернуться к составу", "callback_data": "roster:back_to_roster"}]]
        telegram_api.edit_message_text(chat_id, message_id, msg_text, formatted=True, reply_markup={"inline_keyboard": keyboard})
        datastore.set_user_state(user_id, "ENTERING_UNRATED_PLAYER", context_data)

    elif action == "add_unrated_search":
        unrated_name = context_data.get("unrated_name_fallback", "")
        parts = unrated_name.split(None, 1)
        name = parts[0] if parts else unrated_name
        surname = parts[1] if len(parts) > 1 else ""
        roster = context_data.get("roster", [])
        rating_team_id = context_data.get("rating_team_id")
        status = helpers.determine_player_default_status(rating_team_id, None, roster)
        roster.append({
            "player_id": None,
            "name": name,
            "surname": surname,
            "status": status,
        })
        context_data["roster"] = roster
        render_roster_ui(chat_id, user_id, context_data, message_id)
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)

    elif action == "hints_page":
        next_page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        context_data["hints_page"] = next_page
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        render_roster_ui(chat_id, user_id, context_data, message_id)

    elif action == "prompt_setmyid":
        datastore.set_user_state(user_id, "ENTERING_SET_MY_ID", context_data)
        cancel_kb = [[{"text": "❌ Отменить ввод", "callback_data": "roster:back_to_roster"}]]
        prompt_msg = (
            "🔍 <b>ОЖИДАЕТСЯ ВВОД ТЕКСТА</b>\n\n"
            "Введите ваш **ID на сайте рейтинга** (например: <code>13551</code>) или **ФИО** (например: <code>Вадим Карлинский</code>):"
        )
        telegram_api.send_message(chat_id, None, prompt_msg, formatted=True, reply_markup={"inline_keyboard": cancel_kb})

    elif action == "link_myid":
        pid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        pdata = rating_api.get_player_by_id(pid)
        if pdata:
            datastore.set_user_mapping(
                user_id,
                pid,
                pdata.get("name", ""),
                pdata.get("surname", ""),
                pdata.get("patronymic", ""),
                pdata.get("town", ""),
                cq["from"].get("username", "")
            )
            fio = f"{pdata.get('surname')} {pdata.get('name')} {pdata.get('patronymic', '')}".strip()
            town_str = f" ({pdata['town']})" if pdata.get("town") else ""
            telegram_api.answer_callback_query(cq_id, text=f"Профиль [ID {pid}] {fio} привязан!")
            telegram_api.send_message(chat_id, None, f"✅ <b>Успешно привязан профиль сайта рейтинга!</b>\n\n👤 [ID {pid}] <b>{fio}</b>{town_str}", formatted=True)
            datastore.clear_user_state(user_id)
            if context_data.get("team_name") or context_data.get("sync_req_id"):
                render_roster_ui(chat_id, user_id, context_data)
                datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)

    elif action in ("back_to_roster", "cancel_input"):
        if context_data.get("team_name") or context_data.get("sync_req_id"):
            render_roster_ui(chat_id, user_id, context_data, message_id)
            datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        else:
            datastore.clear_user_state(user_id)
            telegram_api.edit_message_text(chat_id, message_id, "❌ <b>Режим ввода отменен.</b>\n\nДля вызова меню используйте /roster.", formatted=True)

    elif action == "export_csv":
        sync_req_id = parts[2]
        group_chat_id = int(parts[3])
        reg = datastore.get_team_registration(group_chat_id, sync_req_id)
        if reg:
            teams = reg.get("teams", [])
            csv_content = helpers.generate_roster_csv(teams)
            tourn_name_safe = helpers.normalize_tourn_name(reg.get("tourn_name", "roster"))
            filename = f"roster_{sync_req_id}_{tourn_name_safe}.csv"
            caption = f"📄 <b>Файл импорта составов для сайта рейтинга</b>\nТурнир: \"{reg.get('tourn_name')}\"\nКоманд: {len(teams)}"
            telegram_api.send_document(user_id, csv_content, filename, caption=caption)
            telegram_api.answer_callback_query(cq_id, text="Файл CSV отправлен в личные сообщения!")

    elif action == "save":
        chat_id_group = context_data.get("chat_id")
        sync_req_id = context_data.get("sync_req_id")
        rating_team_id = context_data.get("rating_team_id")
        team_name = context_data.get("team_name", "Команда")
        display_name = context_data.get("display_name", team_name)
        town = context_data.get("town", "")
        roster = context_data.get("roster", [])

        if chat_id_group and sync_req_id:
            datastore.update_team_roster_in_ds(chat_id_group, sync_req_id, user_id, rating_team_id, team_name, display_name, roster, town=town)
            
            datastore.add_user_history_team(user_id, rating_team_id, team_name)
            for p in roster:
                if p.get("name") or p.get("surname"):
                    datastore.add_user_history_player(user_id, p.get("player_id"), p.get("name", ""), p.get("surname", ""), patronymic=p.get("patronymic", ""))

        datastore.clear_user_state(user_id)

        team_info = f"Команда: <b>{display_name}</b>"
        if rating_team_id:
            team_info += f" (Рейтинг ID: {rating_team_id})"
        elif display_name != team_name:
            team_info += f" (изн: {team_name})"

        town_info = f"Город: <b>{town}</b>" if town else "Город: <i>не указан</i>"

        lines = [
            f"✅ <b>Состав команды «{display_name}» успешно сохранен!</b>\n",
            team_info,
            town_info,
            "",
            "<b>Состав:</b>"
        ]

        status_symbols = {"K": "👑 [К]", "B": "🛡 [Б]", "L": "⚔️ [Л]"}
        if not roster:
            lines.append("<i>(состав пуст)</i>")
        else:
            for idx, p in enumerate(roster, 1):
                st = status_symbols.get(p.get("status", "B"), "🛡 [Б]")
                pid_str = f" (ID: {p['player_id']})" if p.get("player_id") else " (без ID)"
                p_name = f"{p.get('name', '')} {p.get('surname', '')}".strip() or p.get("display_name", "Игрок")
                lines.append(f"{idx}. {st} {p_name}{pid_str}")

        lines.append("\nВы можете в любой момент изменить его, отправив /roster.")
        saved_text = "\n".join(lines)

        keyboard = []
        if sync_req_id and chat_id_group:
            keyboard.append([{"text": "📋 Изменить состав", "callback_data": f"roster:sel_reg:{sync_req_id}:{chat_id_group}"}])

        telegram_api.edit_message_text(chat_id, message_id, saved_text, formatted=True, reply_markup={"inline_keyboard": keyboard} if keyboard else None)

    elif action == "remind_unsubmitted":
        if len(parts) >= 4:
            sync_req_id = parts[2]
            group_chat_id = int(parts[3])
            reg_entity, notified_teams = datastore.reset_unsubmitted_reminders(group_chat_id, sync_req_id)
            tourn_name = reg_entity.get("tourn_name", "турнир") if reg_entity else "турнир"
            
            count_sent = 0
            for item in notified_teams:
                uid = item["user_id"]
                tname = item["team_name"]
                pm_msg = (
                    f"⏰ <b>Напоминание от представителя площадки!</b>\n\n"
                    f"Представитель площадки запрашивает сдачу/проверку состава вашей команды <b>\"{tname}\"</b> на турнир <b>\"{tourn_name}\"</b>.\n\n"
                    f"Пожалуйста, отправьте /roster в этот личный чат с ботом для забора состава."
                )
                res = telegram_api.send_message(uid, None, pm_msg, formatted=True)
                if res and res.ok:
                    count_sent += 1

            group_msg = (
                f"🔔 <b>Напоминание представителя площадки по турниру \"{tourn_name}\"!</b>\n\n"
                f"Представитель возобновил сбор составов. Капитанам команд необходимо проверить и сдать составы.\n"
                f"Для указания состава напишите /roster в ЛС боту."
            )
            telegram_api.send_message(group_chat_id, None, group_msg, formatted=True)
    elif action == "reject_team":
        if len(parts) >= 5:
            sync_req_id = parts[2]
            target_uid = int(parts[3])
            group_chat_id = int(parts[4])
            reg_entity, rejected_tname = datastore.reject_team_roster_in_ds(group_chat_id, sync_req_id, target_uid)
            tourn_name = reg_entity.get("tourn_name", "турнир") if reg_entity else "турнир"
            tname_str = rejected_tname or "вашей команды"
            
            pm_msg = (
                f"⚠️ <b>Запрос исправления состава от представителя площадки!</b>\n\n"
                f"Представитель площадки вернул состав команды <b>\"{tname_str}\"</b> на турнир <b>\"{tourn_name}\"</b> на доработку.\n\n"
                f"Пожалуйста, проверьте и скорректируйте состав. Отправьте /roster в этот личный чат с ботом для редактирования."
            )
            telegram_api.send_message(target_uid, None, pm_msg, formatted=True)
            telegram_api.answer_callback_query(cq_id, text=f"↩️ Состав команды «{tname_str}» возвращен на доработку!", show_alert=True)
            handle_export_roster(chat_id)

    elif action == "remind_team":
        if len(parts) >= 5:
            sync_req_id = parts[2]
            target_uid = int(parts[3])
            group_chat_id = int(parts[4])
            reg_entity = datastore.get_team_registration(group_chat_id, sync_req_id)
            tourn_name = reg_entity.get("tourn_name", "турнир") if reg_entity else "турнир"
            tname_str = "команды"
            if reg_entity:
                for t in reg_entity.get("teams", []):
                    if t.get("user_id") == target_uid:
                        tname_str = t.get("display_name") or t.get("team_name", "команды")
                        break
            
            pm_msg = (
                f"⏰ <b>Напоминание от представителя площадки!</b>\n\n"
                f"Представитель площадки запрашивает состав команды <b>\"{tname_str}\"</b> на турнир <b>\"{tourn_name}\"</b>.\n\n"
                f"Пожалуйста, отправьте /roster в этот личный чат с ботом для указания состава."
            )
            telegram_api.send_message(target_uid, None, pm_msg, formatted=True)
            telegram_api.answer_callback_query(cq_id, text=f"🔔 Напоминание отправлено капитану команды «{tname_str}»!", show_alert=True)


def handle_export_roster(chat_id, thread_id=None):
    now_ts = int(time.time())
    if chat_id > 0:
        raw_regs = datastore.get_user_representative_registrations(chat_id)
    else:
        raw_regs = datastore.get_all_active_registrations(chat_id)

    valid_regs = []
    seen_keys = set()
    for reg in raw_regs:
        sync_req_id = reg.get("sync_req_id")
        reg_chat_id = reg.get("chat_id")
        key = (sync_req_id, reg_chat_id)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        start_ts = helpers.get_registration_start_ts(reg)
        if start_ts and (now_ts - start_ts > 172800):
            reg["status"] = "archived"
            datastore.update_team_registration(reg)
            continue

        valid_regs.append(reg)

    if not valid_regs:
        if chat_id > 0:
            telegram_api.send_message(chat_id, thread_id, "Вам пока не доступны активные заявки команд на ближайшие турниры.\n\nИспользуйте /setmyid <ваш ID на сайте рейтинга> для привязки вашего аккаунта представителя.", formatted=True)
        else:
            telegram_api.send_message(chat_id, thread_id, "В данном чате пока нет активных открытых заявок на ближайшие турниры.", formatted=True)
        return

    for reg in valid_regs:
        sync_req_id = reg.get("sync_req_id")
        tourn_name = reg.get("tourn_name", "Турнир")
        teams = reg.get("teams", [])
        
        submitted_count = sum(1 for t in teams if t.get("roster_submitted") or t.get("roster"))
        lines = [
            f"📊 <b>Статус сбора составов: \"{tourn_name}\"</b>",
            f"Всего зарегистрированных команд: <b>{len(teams)}</b> | Сдано составов: <b>{submitted_count}</b>",
            ""
        ]
        
        if not teams:
            lines.append("<i>(команды пока не зарегистрированы)</i>")
        else:
            for idx, t in enumerate(teams, 1):
                t_disp = t.get("display_name") or t.get("team_name", "Команда")
                u_disp = f"@{t['username']}" if t.get("username") else f"ID {t.get('user_id')}"
                r_list = t.get("roster", [])
                p_count = len(r_list)
                
                if t.get("roster_submitted") or p_count > 0:
                    st_icon = "🟢"
                    st_text = f"состав сдан ({p_count} чел.)"
                else:
                    st_icon = "🔴"
                    st_text = "состав НЕ сдан (0 чел.)"
                
                lines.append(f"{idx}. {st_icon} <b>{t_disp}</b> — {st_text}")
                lines.append(f"   Ответственный: {u_disp}")

        keyboard = []
        if teams:
            for t in teams:
                t_disp = t.get("display_name") or t.get("team_name", "Команда")
                uid = t.get("user_id")
                r_list = t.get("roster", [])
                is_sub = t.get("roster_submitted") or len(r_list) > 0
                if uid:
                    if is_sub:
                        cb = f"roster:reject_team:{sync_req_id}:{uid}:{reg.get('chat_id')}"
                        keyboard.append([{"text": f"↩️ Вернуть на доработку: {t_disp}", "callback_data": cb}])
                    else:
                        cb = f"roster:remind_team:{sync_req_id}:{uid}:{reg.get('chat_id')}"
                        keyboard.append([{"text": f"🔔 Напомнить в ЛС: {t_disp}", "callback_data": cb}])

            cb_csv = f"roster:export_csv:{sync_req_id}:{reg.get('chat_id')}"
            cb_remind = f"roster:remind_unsubmitted:{sync_req_id}:{reg.get('chat_id')}"
            keyboard.append([{"text": "📥 Скачать CSV для сайта рейтинга", "callback_data": cb_csv}])
            keyboard.append([{"text": "🔔 Напомнить всем не сдавшим", "callback_data": cb_remind}])

        telegram_api.send_message(chat_id, thread_id, "\n".join(lines), formatted=True, reply_markup={"inline_keyboard": keyboard})


def handle_private_message(body):
    msg = body["message"]
    chat_id = msg["chat"]["id"]
    user_id = msg["from"]["id"]
    text = msg.get("text", "").strip()

    state_name, context_data = datastore.get_user_state(user_id)

    if helpers.is_debug_allowed(user_id=user_id, chat_id=chat_id) and text.startswith("/testreg"):
        tourn_name = "Тестовый кубок сообщества"
        url = "https://rating.chgk.info/tournament/5000"
        representative_text = "Представитель: Тест Тестов"
        narrator_text = "Ведущий: Тест Ведущий"
        start_time = "сегодня в 19:00"
        sync_req_id = f"test_{int(time.time())}"

        msg_text = helpers.format_team_registration_text(
            tourn_name, url, representative_text, narrator_text, start_time, []
        )
        resp = telegram_api.send_message(chat_id, None, msg_text, formatted=True)
        if resp and resp.ok:
            res_data = resp.json().get("result", {})
            message_id = res_data.get("message_id")
            if message_id:
                datastore.add_team_registration(
                    chat_id,
                    None,
                    sync_req_id,
                    message_id,
                    tourn_name,
                    representative_text,
                    narrator_text,
                    start_time,
                )
        return True

    if "reply_to_message" in msg:
        reply_msg_id = msg["reply_to_message"]["message_id"]
        reg = datastore.get_team_registration_by_msg(chat_id, reply_msg_id)
        if reg:
            username = msg["from"].get("username", "")
            first_name = msg["from"].get("first_name", "")
            last_name = msg["from"].get("last_name", "")
            user_disp = username or f"{first_name} {last_name}".strip()
            raw_text = text.strip()

            if raw_text.lower() in ("/unregister", "отмена") or raw_text.lower().startswith("/unregister") or raw_text.lower().startswith("отмена"):
                updated_entity = datastore.unregister_team_in_ds(chat_id, reg["sync_req_id"], user_id)
                if updated_entity:
                    teams = updated_entity.get("teams", [])
                    url = f'https://rating.chgk.info/tournament/{reg.get("sync_req_id")}'
                    new_text = helpers.format_team_registration_text(
                        reg.get("tourn_name", ""),
                        url,
                        reg.get("representative_text", ""),
                        reg.get("narrator_text", ""),
                        reg.get("start_time", ""),
                        teams,
                    )
                    telegram_api.edit_message_text(chat_id, reg["message_id"], new_text)
            elif not raw_text.startswith("/"):
                team_name = raw_text
                updated_entity = datastore.register_team_in_ds(chat_id, reg["sync_req_id"], team_name, user_id, user_disp)
                if updated_entity:
                    teams = updated_entity.get("teams", [])
                    url = f'https://rating.chgk.info/tournament/{reg.get("sync_req_id")}'
                    new_text = helpers.format_team_registration_text(
                        reg.get("tourn_name", ""),
                        url,
                        reg.get("representative_text", ""),
                        reg.get("narrator_text", ""),
                        reg.get("start_time", ""),
                        teams,
                    )
                    telegram_api.edit_message_text(chat_id, reg["message_id"], new_text)
                    telegram_api.set_message_reaction(chat_id, msg["message_id"], "❤️")

                    context_data = {
                        "sync_req_id": reg["sync_req_id"],
                        "chat_id": chat_id,
                        "tourn_name": reg.get("tourn_name", ""),
                        "team_name": team_name,
                        "display_name": team_name,
                        "rating_team_id": None,
                        "roster": [],
                    }
                    render_team_selection_ui(chat_id, user_id, context_data)
                    datastore.set_user_state(user_id, "SELECTING_TEAM", context_data)
            return True

    if helpers.is_debug_allowed(user_id=user_id, chat_id=chat_id) and text.startswith("/testroster"):

        sync_req_id = f"test_{int(time.time())}"
        group_chat_id = chat_id
        datastore.add_team_registration(group_chat_id, None, sync_req_id, 999999, "Тестовый кубок сообщества", "Представитель: Тест", "Ведущий: Тест", "19:00")
        datastore.register_team_in_ds(group_chat_id, sync_req_id, "Тестовые знатоки", user_id, "user")
        context_data = {
            "sync_req_id": sync_req_id,
            "chat_id": group_chat_id,
            "tourn_name": "Тестовый кубок сообщества",
            "team_name": "Тестовые знатоки",
            "display_name": "Тестовые знатоки",
            "rating_team_id": None,
            "roster": [],
        }
        render_team_selection_ui(chat_id, user_id, context_data)
        datastore.set_user_state(user_id, "SELECTING_TEAM", context_data)
        return True

    if text.startswith("/start"):
        parts = text.split()
        if len(parts) > 1 and parts[1].startswith("roster_"):
            params = parts[1].split("_")
            if len(params) >= 3:
                sync_req_id = params[1]
                group_chat_id = int(params[2])
                context_data = {
                    "sync_req_id": sync_req_id,
                    "chat_id": group_chat_id,
                    "roster": [],
                    "team_name": "",
                    "display_name": "",
                    "rating_team_id": None,
                }
                reg = datastore.get_team_registration(group_chat_id, sync_req_id)
                if reg:
                    context_data["tourn_name"] = reg.get("tourn_name", "")
                    for t in reg.get("teams", []):
                        if t.get("user_id") == user_id:
                            context_data["team_name"] = t.get("team_name", "")
                            context_data["display_name"] = t.get("display_name", t.get("team_name", ""))
                            context_data["rating_team_id"] = t.get("rating_team_id")
                            context_data["roster"] = list(t.get("roster", []))
                            break

                render_team_selection_ui(chat_id, user_id, context_data)
                datastore.set_user_state(user_id, "SELECTING_TEAM", context_data)
                return True

        active_regs = datastore.get_user_active_registrations(user_id)
        if not active_regs:
            telegram_api.send_message(chat_id, None, "Привет! У вас пока нет активных поданных заявок от команд.\n\nЗарегистрируйте команду в групповом чате.", formatted=True)
            return True

        keyboard = []
        for reg_info in active_regs:
            t_name = reg_info["team"].get("display_name") or reg_info["team"].get("team_name")
            tourn_name = reg_info.get("tourn_name")
            cb = f"roster:sel_reg:{reg_info['sync_req_id']}:{reg_info['chat_id']}"
            keyboard.append([{"text": f"🏆 {tourn_name} — {t_name}", "callback_data": cb}])

        telegram_api.send_message(chat_id, None, "Выберите команду для указания/редактирования состава:", formatted=True, reply_markup={"inline_keyboard": keyboard})
        return True

    state_name, context_data = datastore.get_user_state(user_id)

    # Check command escape when user is in input mode
    if text.startswith("/") or text.lower() in ("отмена", "cancel", "stop", "назад"):
        if text in ("/cancel", "/stop") or text.lower() in ("отмена", "cancel", "stop", "назад"):
            datastore.clear_user_state(user_id)
            if context_data.get("team_name") or context_data.get("sync_req_id"):
                telegram_api.send_message(chat_id, None, "❌ <b>Режим ввода отменен. Возврат к составу:</b>", formatted=True)
                render_roster_ui(chat_id, user_id, context_data)
                datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
                return True
            else:
                telegram_api.send_message(chat_id, None, "❌ <b>Режим ввода отменен.</b>\n<i>Для продолжения работы используйте /roster.</i>", formatted=True)
                return True
        elif text.startswith("/") and state_name not in (None, "SELECTING_TEAM", "MANAGING_ROSTER"):
            datastore.clear_user_state(user_id)

    if text.startswith("/setmyid") or text.startswith("/myid"):
        parts = text.split(None, 1)
        if len(parts) > 1 and parts[1].strip():
            arg = parts[1].strip()
            if arg.isdigit():
                pid = int(arg)
                pdata = rating_api.get_player_by_id(pid)
                if pdata:
                    datastore.set_user_mapping(
                        user_id,
                        pid,
                        pdata.get("name", ""),
                        pdata.get("surname", ""),
                        pdata.get("patronymic", ""),
                        pdata.get("town", ""),
                        msg["from"].get("username", "")
                    )
                    fio = f"{pdata.get('surname')} {pdata.get('name')} {pdata.get('patronymic', '')}".strip()
                    town_str = f" ({pdata['town']})" if pdata.get("town") else ""
                    telegram_api.send_message(chat_id, None, f"✅ <b>Успешно привязан профиль сайта рейтинга!</b>\n\n👤 [ID {pid}] <b>{fio}</b>{town_str}", formatted=True)
                    return True
                else:
                    telegram_api.send_message(chat_id, None, f"❌ Игрок с ID {pid} не найден в базе сайта рейтинга.", formatted=True)
                    return True
            else:
                telegram_api.send_chat_action(chat_id, "typing")
                found = rating_api.search_players(arg)
                if not found:
                    telegram_api.send_message(chat_id, None, f"❌ Игроки по запросу \"{arg}\" не найдены.", formatted=True)
                    return True
                keyboard = []
                for p in found[:6]:
                    btn_text = helpers.format_player_button_text(p, prefix="👤")
                    cb = f"roster:link_myid:{p['id']}"
                    keyboard.append([{"text": btn_text, "callback_data": cb}])
                telegram_api.send_message(chat_id, None, f"Выберите ваш профиль из результатов поиска по запросу \"{arg}\":", formatted=True, reply_markup={"inline_keyboard": keyboard})
                return True

        mapping = datastore.get_user_mapping(user_id)
        if mapping and mapping.get("rating_player_id"):
            fio = f"{mapping.get('surname')} {mapping.get('name')} {mapping.get('patronymic', '')}".strip()
            town_str = f" ({mapping['town']})" if mapping.get("town") else ""
            keyboard = [[{"text": "✏️ Изменить привязку ID", "callback_data": "roster:prompt_setmyid"}]]
            telegram_api.send_message(chat_id, None, f"👤 <b>Ваш привязанный профиль сайта рейтинга:</b>\n\n[ID {mapping['rating_player_id']}] <b>{fio}</b>{town_str}\n\n<i>Для смены отправьте <code>/setmyid <новое ID или ФИО></code></i>", formatted=True, reply_markup={"inline_keyboard": keyboard})
        else:
            keyboard = [[{"text": "🔍 Найти свой профиль", "callback_data": "roster:prompt_setmyid"}]]
            telegram_api.send_message(chat_id, None, "У вас пока не привязан профиль сайта рейтинга.\n\nОтправьте <code>/setmyid <ваш ID рейтинга или ФИО></code> для привязки.", formatted=True, reply_markup={"inline_keyboard": keyboard})
        return True

    if text in ("/myteams", "/roster"):
        active_regs = datastore.get_user_active_registrations(user_id)
        if not active_regs:
            telegram_api.send_message(chat_id, None, "У вас пока нет активных зарегистрированных команд.", formatted=True)
            return True
        keyboard = []
        for reg_info in active_regs:
            t_name = reg_info["team"].get("display_name") or reg_info["team"].get("team_name")
            tourn_name = reg_info.get("tourn_name")
            cb = f"roster:sel_reg:{reg_info['sync_req_id']}:{reg_info['chat_id']}"
            keyboard.append([{"text": f"🏆 {tourn_name} — {t_name}", "callback_data": cb}])
        telegram_api.send_message(chat_id, None, "Выберите заявку команды для настройки состава:", formatted=True, reply_markup={"inline_keyboard": keyboard})
        return True

    cmd_pm = text.split()[0].split("@")[0].lower() if text else ""
    if cmd_pm in ("/rosters", "/exportroster", "/csv"):
        handle_export_roster(chat_id)
        return True

    if text == "/help":
        help_text = build_help_text(chat_id, is_private=True)
        telegram_api.send_message(chat_id, None, help_text, formatted=True)
        return True

    if state_name == "ENTERING_SET_MY_ID":
        arg = text.strip()
        if arg.isdigit():
            pid = int(arg)
            pdata = rating_api.get_player_by_id(pid)
            if pdata:
                datastore.set_user_mapping(
                    user_id,
                    pid,
                    pdata.get("name", ""),
                    pdata.get("surname", ""),
                    pdata.get("patronymic", ""),
                    pdata.get("town", ""),
                    msg["from"].get("username", "")
                )
                fio = f"{pdata.get('surname')} {pdata.get('name')} {pdata.get('patronymic', '')}".strip()
                town_str = f" ({pdata['town']})" if pdata.get("town") else ""
                telegram_api.send_message(chat_id, None, f"✅ <b>Успешно привязан профиль сайта рейтинга!</b>\n\n👤 [ID {pid}] <b>{fio}</b>{town_str}", formatted=True)
                datastore.clear_user_state(user_id)
                if context_data.get("team_name") or context_data.get("sync_req_id"):
                    render_roster_ui(chat_id, user_id, context_data)
                    datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
                return True
            else:
                telegram_api.send_message(chat_id, None, f"❌ Игрок с ID {pid} не найден в базе сайта рейтинга.", formatted=True)
                return True
        else:
            telegram_api.send_chat_action(chat_id, "typing")
            found = rating_api.search_players(arg)
            if not found:
                telegram_api.send_message(chat_id, None, f"❌ Игроки по запросу \"{arg}\" не найдены.", formatted=True)
                return True
            keyboard = []
            for p in found[:6]:
                btn_text = helpers.format_player_button_text(p, prefix="👤")
                cb = f"roster:link_myid:{p['id']}"
                keyboard.append([{"text": btn_text, "callback_data": cb}])
            telegram_api.send_message(chat_id, None, f"Выберите ваш профиль из результатов поиска по запросу \"{arg}\":", formatted=True, reply_markup={"inline_keyboard": keyboard})
            return True

    if state_name == "ENTERING_SEARCH_TEAM":
        telegram_api.send_chat_action(chat_id, "typing")
        status_resp = telegram_api.send_message(
            chat_id, None, f"🔍 <i>Ищу команды по запросу «{text}» на сайте рейтинга...</i>", formatted=True
        )
        status_msg_id = None
        if status_resp and hasattr(status_resp, "json"):
            status_msg_id = status_resp.json().get("result", {}).get("message_id")

        context_data["searched_team_name"] = text
        teams_found = rating_api.search_teams(text)

        if status_msg_id:
            telegram_api.delete_message(chat_id, status_msg_id)

        if not teams_found:
            context_data["team_name"] = text
            context_data["display_name"] = text
            context_data["rating_team_id"] = None
            render_roster_ui(chat_id, user_id, context_data)
            datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        else:
            keyboard = []
            for t in teams_found[:5]:
                tname = t["name"]
                if t.get("town"):
                    tname += f" ({t['town']})"
                cb = f"roster:team_select:{t['id']}"
                keyboard.append([{"text": f"🏆 {tname} [ID {t['id']}]", "callback_data": cb}])
            cb_new = "roster:team_select:0"
            keyboard.append([{"text": f"➕ Новая «{text[:12]}» (без ID)", "callback_data": cb_new}])
            telegram_api.send_message(chat_id, None, f"Результаты поиска команд по запросу \"{text}\":", formatted=True, reply_markup={"inline_keyboard": keyboard})
        return

    if state_name == "ENTERING_NEW_TEAM":
        context_data["team_name"] = text
        context_data["display_name"] = text
        context_data["rating_team_id"] = None
        render_roster_ui(chat_id, user_id, context_data)
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        return

    if state_name == "ENTERING_DISPLAY_NAME":
        if text == "-":
            context_data["display_name"] = context_data.get("team_name", "Команда")
        else:
            context_data["display_name"] = text
        render_roster_ui(chat_id, user_id, context_data)
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        return

    if state_name == "ENTERING_TOWN":
        context_data["town"] = text
        render_roster_ui(chat_id, user_id, context_data)
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        return

    if state_name == "ENTERING_SEARCH_PLAYER":
        telegram_api.send_chat_action(chat_id, "typing")
        status_resp = telegram_api.send_message(
            chat_id, None, f"🔍 <i>Ищу игроков по запросу «{text}» на сайте рейтинга...</i>", formatted=True
        )
        status_msg_id = None
        if status_resp and hasattr(status_resp, "json"):
            status_msg_id = status_resp.json().get("result", {}).get("message_id")

        players_found = rating_api.search_players(text)

        if status_msg_id:
            telegram_api.delete_message(chat_id, status_msg_id)

        context_data["unrated_name_fallback"] = text
        datastore.set_user_state(user_id, "ENTERING_SEARCH_PLAYER", context_data)

        if not players_found:
            keyboard = [
                [{"text": f"➕ Добавить «{text[:12]}» (без ID)", "callback_data": "roster:add_unrated_search"}],
                [{"text": "📋 Вернуться к составу", "callback_data": "roster:back_to_roster"}]
            ]
            telegram_api.send_message(
                chat_id,
                None,
                f"Игрок по запросу <b>\"{text}\"</b> на сайте рейтинга не найден.\n\nНажмите на кнопку ниже, чтобы добавить его в состав как игрока без ID:",
                formatted=True,
                reply_markup={"inline_keyboard": keyboard}
            )
            return
        keyboard = []
        for p in players_found[:10]:
            btn_text = helpers.format_player_button_text(p, prefix="👤")
            pid = p["id"]
            cb = f"roster:add_hint:{pid}"
            keyboard.append([{"text": btn_text, "callback_data": cb}])
        keyboard.append([{"text": f"➕ Добавить «{text[:12]}» (без ID)", "callback_data": "roster:add_unrated_search"}])
        telegram_api.send_message(chat_id, None, f"Результаты поиска игроков по запросу \"{text}\":", formatted=True, reply_markup={"inline_keyboard": keyboard})
        return

    if state_name == "ENTERING_UNRATED_PLAYER":
        parts = text.split(None, 1)
        name = parts[0] if parts else text
        surname = parts[1] if len(parts) > 1 else ""
        roster = context_data.get("roster", [])
        rating_team_id = context_data.get("rating_team_id")
        status = helpers.determine_player_default_status(rating_team_id, None, roster)
        roster.append({
            "player_id": None,
            "name": name,
            "surname": surname,
            "status": status,
        })
        context_data["roster"] = roster
        render_roster_ui(chat_id, user_id, context_data)
        datastore.set_user_state(user_id, "MANAGING_ROSTER", context_data)
        return True

    return False


def command_handler(body):
    t0 = time.perf_counter()
    chat_id = None
    try:
        if body and "callback_query" in body:
            handle_callback_query(body["callback_query"])
            return ""

        if body and "poll" in body:
            print(
                f"Update Poll: {body['poll']['id']}, total votes: {body['poll']['total_voter_count']}"
            )
            options = body["poll"]["options"]
            for option in options:
                print(f"Option: {option['text']}, votes: {option['voter_count']}")
        if body and "poll_answer" in body:
            print(body["poll_answer"])

        if body and "message" in body and "text" in body["message"]:
            chat_type = body["message"]["chat"].get("type", "group")
            if chat_type == "private":
                handled = handle_private_message(body)
                if handled:
                    return ""

            inp = [s for s in body["message"]["text"].split() if not s.startswith("@")]
            chat_id = body["message"]["chat"]["id"]
            user_id = body["message"]["from"]["id"] if "from" in body["message"] else None
            thread_id = None
            if (
                "is_forum" in body["message"]["chat"]
                and body["message"]["chat"]["is_forum"]
            ):
                chat_config = datastore.get_chat_config(chat_id)
                if chat_config:
                    thread_id = chat_config.get("thread_id", None)
                else:
                    thread_id = body["message"].get("message_thread_id", None)

            # Check testreg command
            if helpers.is_debug_allowed(user_id=user_id, chat_id=chat_id) and inp[0] == "/testreg":
                tourn_name = "Тестовый кубок сообщества"
                url = "https://rating.chgk.info/tournament/5000"
                representative_text = "Представитель: Тест Тестов"
                narrator_text = "Ведущий: Тест Ведущий"
                start_time = "сегодня в 19:00"
                sync_req_id = f"test_{int(time.time())}"
                
                msg_text = helpers.format_team_registration_text(
                    tourn_name, url, representative_text, narrator_text, start_time, []
                )
                resp = telegram_api.send_message(int(chat_id), thread_id, msg_text, formatted=True)
                if resp and resp.ok:
                    res_data = resp.json().get("result", {})
                    message_id = res_data.get("message_id")
                    if message_id:
                        datastore.add_team_registration(
                            int(chat_id),
                            thread_id,
                            sync_req_id,
                            message_id,
                            tourn_name,
                            representative_text,
                            narrator_text,
                            start_time,
                        )
                return ""

            # Check if this message is a reply to a team registration announcement
            if "reply_to_message" in body["message"]:
                reply_msg_id = body["message"]["reply_to_message"]["message_id"]
                reg = datastore.get_team_registration_by_msg(chat_id, reply_msg_id)
                if reg:
                    user_id = body["message"]["from"]["id"]
                    username = body["message"]["from"].get("username", "")
                    first_name = body["message"]["from"].get("first_name", "")
                    last_name = body["message"]["from"].get("last_name", "")
                    user_disp = username or f"{first_name} {last_name}".strip()
                    
                    raw_text = body["message"]["text"].strip()
                    
                    if raw_text.lower() in ("/unregister", "отмена") or raw_text.lower().startswith("/unregister") or raw_text.lower().startswith("отмена"):
                        updated_entity = datastore.unregister_team_in_ds(chat_id, reg["sync_req_id"], user_id)
                        if updated_entity:
                            teams = updated_entity.get("teams", [])
                            url = f'https://rating.chgk.info/tournament/{reg.get("sync_req_id")}'
                            new_text = helpers.format_team_registration_text(
                                reg.get("tourn_name", ""),
                                url,
                                reg.get("representative_text", ""),
                                reg.get("narrator_text", ""),
                                reg.get("start_time", ""),
                                teams,
                            )
                            telegram_api.edit_message_text(chat_id, reg["message_id"], new_text)
                    elif not raw_text.startswith("/"):
                        team_name = raw_text
                        updated_entity = datastore.register_team_in_ds(chat_id, reg["sync_req_id"], team_name, user_id, user_disp)
                        if updated_entity:
                            teams = updated_entity.get("teams", [])
                            url = f'https://rating.chgk.info/tournament/{reg.get("sync_req_id")}'
                            new_text = helpers.format_team_registration_text(
                                reg.get("tourn_name", ""),
                                url,
                                reg.get("representative_text", ""),
                                reg.get("narrator_text", ""),
                                reg.get("start_time", ""),
                                teams,
                            )
                            telegram_api.edit_message_text(chat_id, reg["message_id"], new_text)
                            telegram_api.set_message_reaction(chat_id, body["message"]["message_id"], "❤️")
                            
                            pm_resp = telegram_api.send_message(
                                user_id,
                                None,
                                f'Привет! Ваша команда "{team_name}" зарегистрирована на турнир "{reg.get("tourn_name")}". Отправьте /roster или /start, чтобы указать состав.',
                                formatted=True,
                            )
                            if not pm_resp or not pm_resp.ok:
                                user_mention = f"@{username}" if username else user_disp
                                telegram_api.send_message(
                                    chat_id,
                                    thread_id,
                                    f'{user_mention}, ваша команда "{team_name}" зарегистрирована! Напишите мне в ЛС, чтобы указать состав.',
                                    reply_to_message_id=body["message"]["message_id"],
                                )
                    return ""

            cmd = inp[0].split("@")[0].lower()

            if cmd in ("/tourns", "/rtourns"):
                date_str = " ".join(inp[1:]) if len(inp) > 1 else "сегодня"
                tourn_date, with_time = helpers.parse_date(
                    date_str, helpers.get_chat_timezone(chat_id)
                )
                if with_time:
                    header = f"Доступно на {tourn_date.strftime('%d.%m.%Y %H:%M')}:"
                else:
                    header = f"Доступно на {tourn_date.strftime('%d.%m.%Y')}:"
                played_tourns = {}
                chat_config = datastore.get_chat_config(chat_id)
                if chat_config and "venues" in chat_config:
                    for venue_id in chat_config["venues"]:
                        played_tourns.update(
                            datastore.get_played_tourns(venue_id, chat_id)
                        )
                only_rated = cmd == "/rtourns"
                tourns_list = rating_api.get_tourns(
                    tourn_date,
                    played_tourns,
                    chat_id,
                    with_time=with_time,
                    only_rated=only_rated,
                )
                tourns_to_show, tourns_to_save = helpers.get_tourns_representations(
                    tourns_list
                )
                datastore.store_data(chat_id, tourns_to_save)
                telegram_api.send_multi_message(
                    chat_id,
                    thread_id,
                    [header] + [f"{i+1}. {e}" for i, e in enumerate(tourns_to_show)],
                )
            elif cmd == "/print" and len(inp) > 1:
                tourns = datastore.fetch_data(chat_id)
                telegram_api.send_message(chat_id, thread_id, tourns[int(inp[1]) - 1])
            elif cmd in ("/stop", "/cancel"):
                message_id = None
                if "reply_to_message" in body["message"]:
                    message_id = body["message"]["reply_to_message"]["message_id"]
                task, multiple_candidates = datastore.pop_task(chat_id, message_id)
                print(task)
                tourn_ids = []
                if task:
                    tourn_ids = task.get("tourn_ids", [])
                    message_id = task.get("message_id", None)
                if message_id:
                    telegram_api.finalize_poll(
                        chat_id,
                        thread_id,
                        message_id,
                        tourn_ids,
                        with_results=(cmd == "/stop"),
                        multiple_candidates=multiple_candidates,
                    )
                else:
                    if multiple_candidates:
                        telegram_api.send_message(
                            chat_id,
                            thread_id,
                            "Ошибка: несколько открытых голосований. Пожалуйста, используйте команду в ответ на сообщение с голосованием.",
                        )
                    else:
                        telegram_api.send_message(
                            chat_id,
                            thread_id,
                            "Ошибка: не найдено открытых голосований. Если такие есть, пожалуйста, используйте команду в ответ на сообщение с голосованием.",
                        )
            elif cmd == "/poll" and len(inp) > 1:
                tourns = datastore.fetch_data(chat_id)
                user_chosen_idxs = [int(i) - 1 for i in inp[1].split(",") if i]
                if not all(0 <= idx < len(tourns) for idx in user_chosen_idxs):
                    telegram_api.send_message(
                        chat_id,
                        thread_id,
                        "Ошибка: неверные номера турниров в списке.",
                    )
                    return ""
                user_chosen_tourns = [tourns[idx] for idx in user_chosen_idxs]
                filtered_tourns = []
                filtered_tourn_ids = []
                for tourn in user_chosen_tourns:
                    if tourn not in filtered_tourns:
                        if isinstance(tourn, dict):
                            filtered_tourns.append(tourn["name"])
                            filtered_tourn_ids.append(tourn["id"])
                        else:
                            filtered_tourns.append(tourn)
                chosen_tourns = filtered_tourns[
                    : (10 - helpers.COMMON_POLL_OPTIONS.__len__())
                ]
                chosen_tourns = chosen_tourns + helpers.COMMON_POLL_OPTIONS
                closing_time = None
                if len(inp) > 2:
                    title = " ".join(inp[2:])
                    split_title = title.lower().split("до")
                    if len(split_title) > 1:
                        closing_time, _ = helpers.parse_date(
                            split_title[1], helpers.get_chat_timezone(chat_id)
                        )
                else:
                    title = "Выбираем"
                resp = telegram_api.create_game_poll(
                    chat_id, thread_id, title, chosen_tourns
                )
                if resp.ok:
                    message = resp.json()
                    if "result" in message and "message_id" in message["result"]:
                        message_id = message["result"]["message_id"]
                        telegram_api.pin_message(chat_id, thread_id, message_id)
                        with_results = True
                        if not closing_time:
                            closing_time = helpers.get_default_poll_closing_time()
                            with_results = False
                        end_time_ts = int(closing_time.timestamp())
                        datastore.add_task(
                            chat_id,
                            message_id,
                            end_time_ts,
                            filtered_tourn_ids[:8],
                            with_results,
                        )
            elif cmd == "/feedback":
                resp = telegram_api.create_feedback_poll(chat_id, thread_id)
            elif cmd == "/settimezone" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, timezone=inp[1])
            elif cmd == "/setvenues" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, venues=inp[1])
            elif cmd == "/setmindifficulty" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, min_difficulty=float(inp[1]))
            elif cmd == "/setmaxdifficulty" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, max_difficulty=float(inp[1]))
            elif cmd == "/setcollectteams" and len(inp) > 1:
                enabled = inp[1].lower() in ("1", "true", "вкл", "on", "enable", "да")
                datastore.update_chat_config(chat_id, thread_id, collect_teams=enabled)
                status_str = "включен" if enabled else "выключен"
                telegram_api.send_message(
                    chat_id,
                    thread_id,
                    f"Сбор заявок команд: {status_str}",
                )
            elif cmd in ("/rosters", "/exportroster", "/csv"):
                handle_export_roster(chat_id, thread_id)
                return ""
            elif cmd == "/help":
                help_text = build_help_text(chat_id, is_private=(chat_id > 0))
                telegram_api.send_message(chat_id, thread_id, help_text, formatted=True)
                return ""
            elif inp[0] == "/debug":
                if not helpers.is_debug_allowed(user_id=user_id, chat_id=chat_id):
                    return ""
                if len(inp) > 1:
                    enabled = inp[1].lower() in ("1", "true", "вкл", "on", "enable")
                    debug.set_debug(enabled)
                    telegram_api.send_message(chat_id, thread_id, f"Debug mode: {'enabled' if enabled else 'disabled'}")
                else:
                    is_on = debug.get_debug()
                    telegram_api.send_message(chat_id, thread_id, f"Debug mode: {'enabled' if is_on else 'disabled'}")
    except Exception as e:
        print(f"Error in command processing {e}")
        print(traceback.format_exc())
    debug.log("command_handler", t0, f"chat_id={chat_id}")
    return ""
