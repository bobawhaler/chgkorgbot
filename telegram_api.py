import requests
from telegram import ParseMode
import random
import helpers


BASE_URL = "https://api.telegram.org/bot" + helpers.TELEGRAM_API_TOKEN + "/"
HOOK_URL = (
    f"https://{helpers.PROJECT_ID}.appspot.com/command{helpers.OBFUSCATION_TOKEN}"
)
MAX_MESSAGE_SIZE = 4000


def set_webhook():
    resp = get_webhook()
    # print(resp)
    if (
        "result" not in resp
        or "url" not in resp["result"]
        or resp["result"]["url"] != HOOK_URL
    ):
        response = requests.post(
            BASE_URL + "setWebhook",
            json={"url": HOOK_URL, "drop_pending_updates": True},
        )
        if not response.ok:
            print(f"Error setting webhook {response.status_code}, {response.reason}")


def get_webhook():
    return requests.get(BASE_URL + "getWebhookInfo")


def send_message(chat_id, message_thread_id, message, formatted=False):
    params = {
        "chat_id": str(chat_id),
        "text": message,
        "disable_web_page_preview": True,
    }
    if formatted:
        params["parse_mode"] = ParseMode.HTML
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    # print(len(params["text"]))
    response = requests.post(
        BASE_URL + "sendMessage",
        json=params,
    )
    if not response.ok:
        print(f"Error sending message {response.status_code}, {response.reason}")


def send_formatted_message(chat_id, message_thread_id, message):
    send_message(chat_id, message_thread_id, message, formatted=True)


def send_multi_message(chat_id, message_thread_id, string_list):
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


def pin_message(chat_id, message_thread_id, message_id):
    params = {"chat_id": str(chat_id), "message_id": str(message_id)}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "pinChatMessage",
        json=params,
    )
    if not response.ok:
        print(f"Error pinning message {response.status_code}, {response.reason}")
    return response


def unpin_message(chat_id, message_thread_id, message_id):
    params = {"chat_id": str(chat_id), "message_id": str(message_id)}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(
        BASE_URL + "unpinChatMessage",
        json=params,
    )
    if not response.ok:
        print(f"Error unpinning message {response.status_code}, {response.reason}")
    return response


def create_game_poll(chat_id, message_thread_id, title, chosen_tourns):
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
    if not response.ok:
        print(f"Error creating poll {response.status_code}, {response.reason}")
    return response


def create_feedback_poll(chat_id, message_thread_id):
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
    if not response.ok:
        print(f"Error creating poll {response.status_code}, {response.reason}")
    return response


def stop_poll(chat_id, message_thread_id, message_id):
    params = {"chat_id": str(chat_id), "message_id": message_id}
    if message_thread_id:
        params["message_thread_id"] = message_thread_id
    response = requests.post(BASE_URL + "stopPoll", json=params)
    if not response.ok:
        print(f"Error stopping poll {response.status_code}, {response.reason}")
    return response


def get_printable(tourn):
    if tourn[1]:
        url = f"https://rating.chgk.info/tournament/{tourn[1]}"
        return f'<a href="{url}">{tourn[0]}</a>'
    return tourn[1]


def finalize_poll(chat_id, message_thread_id, message_id, tourn_ids, with_results):
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
            for i, option in enumerate(options[:-2]):
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
                )
            else:
                send_formatted_message(
                    chat_id,
                    message_thread_id,
                    f'Победители: {", ".join([w[0] for w in winners])}.\nСлучайный выбор: {get_printable(random.choice(winners))}',
                )
