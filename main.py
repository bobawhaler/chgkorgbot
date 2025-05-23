import json
import traceback
from flask import Flask, request
import rating_api
import telegram_api
import helpers
import pytz


app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    telegram_api.set_webhook()
    return "webhook set!"


# @app.route("/getwebhook", methods=["GET"])
# def get_webhook():
#     response = telegram_api.get_webhook()
#     return response.json()


@app.route("/systemtic", methods=["GET"])
def system_tic():
    set_webhook()
    all_configs = helpers.get_all_configs()
    for task in helpers.traverse_finished_tasks():
        thread_id = None
        if str(task["chat_id"]) in all_configs:
            thread_id = all_configs[str(task["chat_id"])].get("thread_id", None)
        telegram_api.finalize_poll(
            task["chat_id"],
            thread_id,
            task["message_id"],
            task.get("tourn_ids", []),
            with_results=True,
        )
    for chat_id in all_configs:
        if "venues" not in all_configs[chat_id]:
            continue
        for venue_id in all_configs[chat_id]["venues"]:
            sync_reqs = rating_api.get_new_sync_requests(venue_id)
            for sync_req in sync_reqs:
                tourn = rating_api.get_tourn_by_id(sync_req["tourn_id"])
                if not tourn["name"]:
                    continue
                tourn_name = tourn["name"]
                start_time = (
                    sync_req["dateStart"]
                    .astimezone(
                        pytz.timezone(
                            all_configs[chat_id].get("timezone", "Europe/Berlin")
                        )
                    )
                    .strftime("%d.%m %H:%M")
                )
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

                telegram_api.send_formatted_message(
                    int(chat_id),
                    all_configs[chat_id].get("thread_id", None),
                    f'Подана заявка на <a href="{url}">"{tourn_name}"</a>. {representative_text}. {narrator_text}. Начало: {start_time}',
                )

    return ""


@app.route(f"/command{helpers.OBFUSCATION_TOKEN}", methods=["POST"])
def command():
    try:
        all_configs = helpers.get_all_configs()
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
                if str(chat_id) in all_configs:
                    thread_id = all_configs[str(chat_id)].get("thread_id", None)
                else:
                    thread_id = body["message"].get("message_thread_id", None)
            if (inp[0] == "/tourns" or inp[0] == "/rtourns") and len(inp) > 1:
                tourn_date, with_time = helpers.parse_date(
                    " ".join(inp[1:]), helpers.get_chat_timezone(chat_id)
                )
                played_tourns = {}
                if (
                    str(chat_id) in all_configs
                    and "venues" in all_configs[str(chat_id)]
                ):
                    for venue_id in all_configs[str(chat_id)]["venues"]:
                        played_tourns.update(
                            helpers.get_played_tourns(venue_id, chat_id)
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
                helpers.store_data(chat_id, tourns_to_save)
                telegram_api.send_multi_message(
                    chat_id,
                    thread_id,
                    [f"{i+1}. {e}" for i, e in enumerate(tourns_to_show)],
                )
            elif inp[0] == "/print" and len(inp) > 1:
                tourns = helpers.fetch_data(chat_id)
                telegram_api.send_message(chat_id, thread_id, tourns[int(inp[1]) - 1])
            elif inp[0] == "/stop":
                if "reply_to_message" in body["message"]:
                    message_id = body["message"]["reply_to_message"]["message_id"]
                    task = helpers.pop_task(chat_id, message_id)
                    tourn_ids = []
                    if task:
                        tourn_ids = task.get("tourn_ids", [])
                    telegram_api.finalize_poll(
                        chat_id,
                        thread_id,
                        message_id,
                        tourn_ids,
                        with_results=True,
                    )
            elif inp[0] == "/cancel":
                if "reply_to_message" in body["message"]:
                    message_id = body["message"]["reply_to_message"]["message_id"]
                    task = helpers.pop_task(chat_id, message_id)
                    tourn_ids = []
                    if task:
                        tourn_ids = task.get("tourn_ids", [])
                    telegram_api.finalize_poll(
                        chat_id,
                        thread_id,
                        message_id,
                        tourn_ids,
                        with_results=False,
                    )
            elif inp[0] == "/poll" and len(inp) > 1:
                tourns = helpers.fetch_data(chat_id)
                # print(f"Retrieved {tourns}")
                user_chosen_tourns = [tourns[int(i) - 1] for i in inp[1].split(",")]
                filtered_tourns = []
                filtered_tourn_ids = []
                for tourn in user_chosen_tourns:
                    if tourn not in filtered_tourns:
                        if isinstance(tourn, dict):
                            filtered_tourns.append(tourn["name"])
                            filtered_tourn_ids.append(tourn["id"])
                        else:
                            filtered_tourns.append(tourn)
                chosen_tourns = filtered_tourns[:8]
                chosen_tourns.append("буду играть любой")
                chosen_tourns.append("не буду играть")
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
                        if closing_time:
                            end_time_ts = int(closing_time.timestamp())
                            helpers.add_task(
                                chat_id, message_id, end_time_ts, filtered_tourn_ids[:8]
                            )
            elif inp[0] == "/feedback":
                resp = telegram_api.create_feedback_poll(chat_id, thread_id)
            elif inp[0] == "/setupchat":
                timezone = ""
                if len(inp) > 1:
                    timezone = inp[1]
                venues = ""
                if len(inp) > 2:
                    venues = inp[2]
                thread_id = None
                if (
                    "is_forum" in body["message"]["chat"]
                    and body["message"]["chat"]["is_forum"]
                ):
                    thread_id = body["message"].get("message_thread_id", None)
                helpers.make_config(chat_id, timezone, venues, thread_id)
            elif inp[0] == "/help":
                telegram_api.send_message(
                    chat_id,
                    thread_id,
                    "/setupchat <timezone> [venue_id1,venue_id2...] - настройка часового пояса чата и мониторинга заявок на списке площадок\n/tourns <YYYYMMDD>|<дата и время турнира> - список турниров на дату (и время)\n/rtourns <YYYYMMDD>|<дата и время турнира> - список рейтингуемых турниров на дату (и время)\n/poll <tourn_1,tourn_2,...> [title] [до <время окончания>]- создание голосовалки из 2-8 перечисленных номеров турниров\n/stop - как reply на сообщение с опросом, завершает его и подводит итоги\n/cancel - как reply на сообщение с опросом, завершает его без подведения итогов\n/feedback - опрос впечатлений о сыгранном пакете\n/help - эта подсказка",
                )
    except Exception as e:
        print(f"Error in command processing {e}")
        print(traceback.format_exc())
    return ""


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. You
    # can configure startup instructions by adding `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
