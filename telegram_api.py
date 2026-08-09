import requests
import time
from telegram import ParseMode
import random
import helpers
import debug


BASE_URL = "https://api.telegram.org/bot" + helpers.TELEGRAM_API_TOKEN + "/"
HOOK_URL = (
    f"https://{helpers.PROJECT_ID}.appspot.com/command{helpers.OBFUSCATION_TOKEN}"
)
MAX_MESSAGE_SIZE = 4000


def set_webhook():
    t0 = time.perf_counter()
    resp = get_webhook()
    if (
        "result" not in resp
        or "url" not in resp["result"]
        or resp["result"]["url"] != HOOK_URL
    ):
        response = requests.post(
            BASE_URL + "setWebhook",
            json={"url": HOOK_URL, "drop_pending_updates": True},
        )
        debug.log("telegram_api.set_webhook", t0)
        if not response.ok:
            print(f"Error setting webhook {response.status_code}, {response.reason}")
    else:
        debug.log("telegram_api.set_webhook", t0, "already set")


def get_webhook():
    t0 = time.perf_counter()
    resp = requests.get(BASE_URL + "getWebhookInfo", headers={"Accept": "application/json"})
    debug.log("telegram_api.get_webhook", t0)
    return resp.json()


def send_message(
    chat_id, message_thread_id, message, formatted=False, reply_to_message_id=None, reply_markup=None
):
    t0 = time.perf_counter()
    params = {
        "chat_id": str(chat_id),
        "text": message,
        "disable_web_page_preview": True,
    }
    if formatted:
        params["parse_mode"] = ParseMode.HTML
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    if reply_to_message_id:
        params["reply_to_message_id"] = reply_to_message_id
    if reply_markup:
        params["reply_markup"] = reply_markup
    # print(len(params["text"]))
    response = requests.post(
        BASE_URL + "sendMessage",
        json=params,
    )
    debug.log("telegram_api.send_message", t0, f"chat_id={chat_id}")
    if not response.ok:
        print(f"Error sending message {response.status_code}, {response.reason} for chat_id={chat_id}")
    return response



def send_formatted_message(
    chat_id, message_thread_id, message, reply_to_message_id=None
):
    send_message(
        chat_id,
        message_thread_id,
        message,
        formatted=True,
        reply_to_message_id=reply_to_message_id,
    )


def send_multi_message(chat_id, message_thread_id, string_list):
    t0 = time.perf_counter()
    string_pool = []
    pool_size = 0
    for s in string_list:
        if pool_size + 1 + len(s) < MAX_MESSAGE_SIZE:
            string_pool.append(s)
            pool_size += 1 + len(s)
        else:
            send_message(
                chat_id, message_thread_id, "\n".join(string_pool), formatted=True
            )
            string_pool = [s]
            pool_size = len(s)
    if string_pool:
        send_message(chat_id, message_thread_id, "\n".join(string_pool), formatted=True)
    debug.log("telegram_api.send_multi_message", t0, f"chat_id={chat_id}, count={len(string_list)}")


def pin_message(chat_id, message_thread_id, message_id):
    t0 = time.perf_counter()
    params = {"chat_id": str(chat_id), "message_id": str(message_id)}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "pinChatMessage",
        json=params,
    )
    debug.log("telegram_api.pin_message", t0, f"chat_id={chat_id}")
    return response


def unpin_message(chat_id, message_thread_id, message_id):
    t0 = time.perf_counter()
    params = {"chat_id": str(chat_id), "message_id": str(message_id)}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "unpinChatMessage",
        json=params,
    )
    debug.log("telegram_api.unpin_message", t0, f"chat_id={chat_id}")
    return response


def create_game_poll(chat_id, message_thread_id, title, chosen_tourns):
    t0 = time.perf_counter()
    params = {
        "chat_id": str(chat_id),
        "question": title,
        "options": chosen_tourns,
        "is_anonymous": False,
        "allows_multiple_answers": True,
        "protect_content": True,
    }
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "sendPoll",
        json=params,
    )
    debug.log("telegram_api.create_game_poll", t0, f"chat_id={chat_id}")
    return response


def create_feedback_poll(chat_id, message_thread_id):
    t0 = time.perf_counter()
    params = {
        "chat_id": str(chat_id),
        "question": "Сыгранный пакет показался вам...",
        "options": [
            "Простым",
            "Средним по сложности",
            "Сложным",
            "Скучным",
            "Нормальным по интересности",
            "Интересным",
            "Слабым по редактуре",
            "Средним по редактуре",
            "Крутым по редактуре",
            "Нет мнения/посмотреть ответы",
        ],
        "is_anonymous": True,
        "allows_multiple_answers": True,
        "protect_content": True,
    }
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "sendPoll",
        json=params,
    )
    debug.log("telegram_api.create_feedback_poll", t0, f"chat_id={chat_id}")
    return response


def stop_poll(chat_id, message_thread_id, message_id):
    t0 = time.perf_counter()
    params = {"chat_id": str(chat_id), "message_id": message_id}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(BASE_URL + "stopPoll", json=params)
    debug.log("telegram_api.stop_poll", t0, f"chat_id={chat_id}")
    if not response.ok:
        print(f"Error stopping poll {response.status_code}, {response.reason}")
    return response


def get_printable(tourn):
    if tourn[1]:
        url = f"https://rating.chgk.info/tournament/{tourn[1]}"
        return f'<a href="{url}">{tourn[0]}</a>'
    return tourn[0]


def finalize_poll(
    chat_id,
    message_thread_id,
    message_id,
    tourn_ids,
    with_results,
    multiple_candidates=False,
):
    t0 = time.perf_counter()
    unpin_message(chat_id, message_thread_id, message_id)
    resp = stop_poll(chat_id, message_thread_id, message_id)
    if not resp.ok:
        print(f"Error stopping poll {resp.status_code}, {resp.reason}")
    else:
        # print(resp.json())
        result = resp.json()
        if with_results and "result" in result and "options" in result["result"]:
            options = result["result"]["options"]
            max_count = 0
            winners = []
            for i, option in enumerate(options):
                if option["text"] in helpers.COMMON_POLL_OPTIONS:
                    continue
                print(option["voter_count"], option["text"])
                tourn_id = tourn_ids[i] if len(tourn_ids) > i else None
                if option["voter_count"] > max_count:
                    max_count = option["voter_count"]
                    winners = [(option["text"], tourn_id)]
                elif option["voter_count"] == max_count:
                    winners.append((option["text"], tourn_id))
            if len(winners) == 1:
                send_formatted_message(
                    chat_id,
                    message_thread_id,
                    f"Победитель: {get_printable(winners[0])}",
                    message_id if multiple_candidates else None,
                )
            else:
                send_formatted_message(
                    chat_id,
                    message_thread_id,
                    f'Победители: {", ".join([w[0] for w in winners])}.\nСлучайный выбор: {get_printable(random.choice(winners))}',
                    message_id if multiple_candidates else None,
                )
    debug.log("telegram_api.finalize_poll", t0, f"chat_id={chat_id}")


def edit_message_text(
    chat_id, message_id, text, formatted=True, reply_markup=None
):
    t0 = time.perf_counter()
    params = {
        "chat_id": str(chat_id),
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if formatted:
        params["parse_mode"] = ParseMode.HTML
    if reply_markup is not None:
        params["reply_markup"] = reply_markup
    response = requests.post(BASE_URL + "editMessageText", json=params)
    debug.log("telegram_api.edit_message_text", t0, f"chat_id={chat_id}, message_id={message_id}")
    if not response.ok:
        print(f"Error editing message text {response.status_code}, {response.reason}")
    return response


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    t0 = time.perf_counter()
    params = {"callback_query_id": str(callback_query_id)}
    if text:
        params["text"] = text
    if show_alert:
        params["show_alert"] = True
    response = requests.post(BASE_URL + "answerCallbackQuery", json=params)
    debug.log("telegram_api.answer_callback_query", t0, f"id={callback_query_id}")
    return response


def set_message_reaction(chat_id, message_id, emoji="❤️"):
    t0 = time.perf_counter()
    params = {
        "chat_id": str(chat_id),
        "message_id": message_id,
        "reaction": [{"type": "emoji", "emoji": emoji}],
    }
    response = requests.post(
        BASE_URL + "setMessageReaction",
        json=params,
        headers={"Accept": "application/json"},
    )
    debug.log("telegram_api.set_message_reaction", t0, f"chat_id={chat_id}, message_id={message_id}")
    if not response.ok:
        print(f"Error setting reaction {response.status_code}, {response.reason}")
    return response


def send_chat_action(chat_id, action="typing"):
    t0 = time.perf_counter()
    params = {"chat_id": str(chat_id), "action": action}
    response = requests.post(BASE_URL + "sendChatAction", json=params)
    debug.log("telegram_api.send_chat_action", t0, f"chat_id={chat_id}, action={action}")
    return response


def send_document(chat_id, file_content, filename, caption=None):
    t0 = time.perf_counter()
    if isinstance(file_content, str):
        file_content = file_content.encode("utf-8")
    
    files = {"document": (filename, file_content, "text/csv")}
    data = {"chat_id": str(chat_id)}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = ParseMode.HTML
    
    response = requests.post(BASE_URL + "sendDocument", data=data, files=files)
    debug.log("telegram_api.send_document", t0, f"chat_id={chat_id}, filename={filename}")
    return response


def delete_message(chat_id, message_id):
    t0 = time.perf_counter()
    params = {"chat_id": str(chat_id), "message_id": message_id}
    response = requests.post(BASE_URL + "deleteMessage", json=params)
    debug.log("telegram_api.delete_message", t0, f"chat_id={chat_id}, message_id={message_id}")
    return response




