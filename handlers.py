import traceback
import json
import pytz

import rating_api
import telegram_api
import helpers
import datastore

def system_tic_handler():
    telegram_api.set_webhook()
    
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
            tourn = rating_api.get_tourn_by_id(sync_req["tourn_id"])
            if not tourn or not tourn.get("name"):
                continue
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
                
                telegram_api.send_formatted_message(
                    int(chat_id),
                    chat_config.get("thread_id", None),
                    f'Подана заявка на <a href="{url}">"{tourn_name}"</a>. {representative_text}. {narrator_text}. Начало: {start_time}',
                )

def command_handler(request):
    try:
        body = json.loads(request.data)
        # print(body)
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
            inp = [s for s in body["message"]["text"].split() if not s.startswith("@")]
            # print(inp)
            chat_id = body["message"]["chat"]["id"]
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
            if (inp[0] == "/tourns" or inp[0] == "/rtourns") and len(inp) > 1:
                tourn_date, with_time = helpers.parse_date(
                    " ".join(inp[1:]), helpers.get_chat_timezone(chat_id)
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
                only_rated = inp[0] == "/rtourns"
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
                # print(f"Chat ID: {chat_id}")
                # print(f"To save: {len(tourns_to_save)}, to show: {len(tourns_to_show)}")
                datastore.store_data(chat_id, tourns_to_save)
                telegram_api.send_multi_message(
                    chat_id,
                    thread_id,
                    [header] + [f"{i+1}. {e}" for i, e in enumerate(tourns_to_show)],
                )
            elif inp[0] == "/print" and len(inp) > 1:
                tourns = datastore.fetch_data(chat_id)
                telegram_api.send_message(chat_id, thread_id, tourns[int(inp[1]) - 1])
            elif inp[0] == "/stop" or inp[0] == "/cancel":
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
                        with_results=(inp[0] == "/stop"),
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
            elif inp[0] == "/poll" and len(inp) > 1:
                tourns = datastore.fetch_data(chat_id)
                # print(f"Retrieved {tourns}")
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
                    # print(message)
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
            elif inp[0] == "/feedback":
                resp = telegram_api.create_feedback_poll(chat_id, thread_id)
            elif inp[0] == "/settimezone" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, timezone=inp[1])
            elif inp[0] == "/setvenues" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, venues=inp[1])
            elif inp[0] == "/setmindifficulty" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, min_difficulty=float(inp[1]))
            elif inp[0] == "/setmaxdifficulty" and len(inp) > 1:
                datastore.update_chat_config(chat_id, thread_id, max_difficulty=float(inp[1]))
            elif inp[0] == "/help":
                telegram_api.send_message(
                    chat_id,
                    thread_id,
                    "/settimezone <timezone> - настройка часового пояса чата\n/setvenues <venue_id1,venue_id2...> - настройка мониторинга заявок на списке площадок\n/setmindifficulty <min_difficulty> - настройка минимальной сложности турниров\n/setmaxdifficulty <max_difficulty> - настройка максимальной сложности турниров\n/tourns <YYYYMMDD>|<дата и время турнира> - список турниров на дату (и время)\n/rtourns <YYYYMMDD>|<дата и время турнира> - список рейтингуемых турниров на дату (и время)\n/poll <tourn_1,tourn_2,...> [title] [до <время окончания>]- создание голосовалки из 2-8 перечисленных номеров турниров\n/stop - как reply на сообщение с опросом, завершает его и подводит итоги\n/cancel - как reply на сообщение с опросом, завершает его без подведения итогов\n/feedback - опрос впечатлений о сыгранном пакете\n/help - эта подсказка",
                )
    except Exception as e:
        print(f"Error in command processing {e}")
        print(traceback.format_exc())
    return ""
