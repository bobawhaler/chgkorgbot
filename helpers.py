from google.cloud import datastore
from dateparser import parse
import datetime
import pytz
import re
import rating_api
from dateutil.relativedelta import relativedelta
import os

PROJECT_ID = os.environ.get("PROJECT_ID")
TELEGRAM_API_TOKEN = os.environ.get("TELEGRAM_API_TOKEN")
OBFUSCATION_TOKEN = os.environ.get("OBFUSCATION_TOKEN")

DEFAULT_TIMEZONE = "Europe/Berlin"
DEFAULT_VENUE_ID = 3053


def get_datastore_client():
    return datastore.Client()


def get_person_form(person):
    person_form = person["name"] + " " + person["surname"]
    is_feminine = (
        person.get("patronymic", "").endswith("на")
        or person.get("patronymic", "").lower().endswith(" гызы")
        or person.get("patronymic", "").lower().endswith(" кызы")
        or person.get("patronymic", "").lower().endswith(" кизи")
        or (
            not person.get("patronymic", "")
            and (
                person["name"].endswith("а")
                and person["name"].lower()
                not in ("никита", "кузьма", "савва", "фома", "лука", "данила")
                or person["name"].endswith("я")
                and person["name"].lower() not in ("илья", "емеля", "добрыня")
                or len(person["surname"]) > 5
                and (
                    person["surname"].endswith("ова")
                    or person["surname"].endswith("ева")
                )
            )
        )
    )
    return person_form, is_feminine


def parse_date(input_date, timezone):
    try:
        result_date = datetime.datetime.strptime(input_date, "%Y%m%d").date()
        print(result_date)
        return result_date, False
    except:
        normal_date = (
            input_date.replace("понедельника", "понедельник")
            .replace("вторника", "вторник")
            .replace("среды", "среда")
            .replace("четверга", "четверг")
            .replace("пятницы", "пятница")
            .replace("субботы", "суббота")
            .replace("воскресенья", "воскресенье")
        )

        result_date = parse(
            normal_date,
            settings={
                "PREFER_DATES_FROM": "future",
                "TIMEZONE": timezone,
                "RETURN_AS_TIMEZONE_AWARE": True,
                "NORMALIZE": True,
            },
        )
        print(result_date)
        if not result_date:
            return datetime.datetime.now().date(), False
        # Crunch for dateparser day of week bug
        week_delta = relativedelta(days=7)
        if (
            "понедельник" in normal_date
            or "вторник" in normal_date
            or "среда" in normal_date
            or "четверг" in normal_date
            or "пятница" in normal_date
            or "суббота" in normal_date
            or "воскресенье" in normal_date
        ) and (result_date - week_delta).replace(
            tzinfo=pytz.UTC
        ) > datetime.datetime.now().replace(
            tzinfo=pytz.UTC
        ):
            result_date -= week_delta

        return result_date, True


# def convert_to_moscow_tz(local_date):
#     old_timezone = pytz.timezone("Europe/Berlin")
#     new_timezone = pytz.timezone("Europe/Moscow")
#     return old_timezone.localize(local_date).astimezone(new_timezone)


# def convert_to_local_tz(moscow_date):
#     new_timezone = pytz.timezone("Europe/Berlin")
#     old_timezone = pytz.timezone("Europe/Moscow")
#     return old_timezone.localize(moscow_date).astimezone(new_timezone)


def get_tourns_representations(tourns):
    tourns_to_save = []
    tourns_to_show = []

    for tourn in sorted(
        tourns, key=lambda r: (r["rating"], r["difficulty"]), reverse=True
    ):
        title = tourn["name"].strip()
        n_questions = (
            f'{tourn["num_questions"]}, ' if tourn["num_questions"] > 0 else ""
        )
        rating = "R, " if tourn["rating"] else ""
        difficulty = f'{tourn["difficulty"]}, ' if tourn["difficulty"] != 0 else ""
        editors = tourn["editors"]
        url = f'https://rating.chgk.info/tournament/{tourn["id"]}'
        tourn_long = (
            f'<a href="{url}">{title}</a> ({n_questions}{rating}{difficulty}{editors})'
        )
        # print(tourn_long)
        tourns_to_show.append(tourn_long)
        tourn_short = f"{title} ({n_questions}{rating}{difficulty}{editors})"
        if len(tourn_short) > 100:
            cut_length = len(tourn_short) - 98
            pos = editors[:-cut_length].rfind(",")
            tourn_short = (
                f"{title} ({n_questions}{rating}{difficulty}{editors[:pos]}...)"
            )
            if len(tourn_short) > 100:
                if len(editors) > 30:
                    editors_cut_length = 30
                    editors_cut_pos = editors[:-editors_cut_length].rfind(",")
                    post_title = f" ({n_questions}{rating}{difficulty}{editors[:editors_cut_pos]}...)"
                else:
                    post_title = f" ({n_questions}{rating}{difficulty}{editors})"
                title_cut_length = 98 - len(post_title)
                title_cut_pos = title[:title_cut_length].rfind(" ")
                tourn_short = f"{title[:title_cut_pos]}...{post_title}"

        tourns_to_save.append({"id": tourn["id"], "name": tourn_short})
    return tourns_to_show, tourns_to_save


def normalize_tourn_name(name):
    norm_text = re.sub(
        " +",
        " ",
        re.sub(r"[^\w\s]", "", name.lower().replace("а/о", "").replace("ё", "е")),
    )
    return (
        norm_text.replace("асинхрон и", "")
        .replace("синхрон и", "")
        .replace("онлайн и", "")
        .replace("офлайн и", "")
        .replace("оффлайн и", "")
        .replace("асинхронный и", "")
        .replace("синхронный и", "")
        .replace("асинхронный", "")
        .replace("синхронный", "")
        .replace("асинхрон", "")
        .replace("синхрон", "")
        .replace("онлайн", "")
        .replace("офлайн", "")
        .replace("оффлайн", "")
        .replace("ua", "")
        .strip()
    )


def store_data(chat_id, tourns_to_save):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, chat_id))
    if not entity:
        entity = datastore.Entity(key=datastore_client.key(PROJECT_ID, chat_id))
    entity.update({"data": tourns_to_save})
    datastore_client.put(entity)


def fetch_data(chat_id):
    datastore_client = get_datastore_client()
    return datastore_client.get(datastore_client.key(PROJECT_ID, chat_id))["data"]


def traverse_finished_tasks():
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "tasks"))
    if entity and "tasks" in entity and entity["tasks"]:
        new_tasks = []
        for task in entity["tasks"]:
            if task["end_time"] <= int(datetime.datetime.now().timestamp()):
                yield task
            else:
                new_tasks.append(task)
        entity.update({"tasks": new_tasks})
        datastore_client.put(entity)


def remove_task(chat_id, message_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "tasks"))
    if entity and "tasks" in entity and entity["tasks"]:
        new_tasks = []
        for task in entity["tasks"]:
            if task["chat_id"] != chat_id or task["message_id"] != message_id:
                new_tasks.append(task)
        entity.update({"tasks": new_tasks})
        datastore_client.put(entity)


def pop_task(chat_id, message_id):
    task_candidates = []
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "tasks"))
    return_task = None
    if entity and "tasks" in entity and entity["tasks"]:
        new_tasks = []
        for task in entity["tasks"]:
            if task["chat_id"] != chat_id:
                new_tasks.append(task)
            else:
                task_candidates.append(task)
        if len(task_candidates) == 1:
            return_task = task_candidates[0]
        elif message_id and len(task_candidates) > 1:
            for task in task_candidates:
                if task["message_id"] == message_id:
                    return_task = task
                else:
                    new_tasks.append(task)
        if return_task:
            entity.update({"tasks": new_tasks})
            datastore_client.put(entity)
    return return_task


def add_task(chat_id, message_id, end_time_ts, tourn_ids):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "tasks"))
    new_tasks = []
    if entity:
        if "tasks" in entity:
            new_tasks = entity["tasks"]
        new_tasks.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "end_time": end_time_ts,
                "tourn_ids": tourn_ids,
            }
        )
        entity.update({"tasks": new_tasks})
        datastore_client.put(entity)
    else:
        entity = datastore.Entity(key=datastore_client.key(PROJECT_ID, "tasks"))
        entity.update(
            {
                "tasks": [
                    {
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "end_time": end_time_ts,
                        "tourn_ids": tourn_ids,
                    }
                ]
            }
        )
        datastore_client.put(entity)


def make_config(chat_id, timezone, venues, thread_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "configs"))
    if not entity:
        entity = datastore.Entity(key=datastore_client.key(PROJECT_ID, "configs"))
    if not timezone:
        timezone = DEFAULT_TIMEZONE
    entity.update(
        {
            str(chat_id): {
                "timezone": timezone,
                "venues": [v.strip() for v in venues.split(",")],
                "thread_id": thread_id,
            }
        }
    )
    datastore_client.put(entity)


def get_all_configs():
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "configs"))
    if entity:
        return entity
    return []


def get_chat_timezone(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "configs"))
    key = str(chat_id)
    if entity and key in entity and "timezone" in entity[key]:
        return entity[key]["timezone"]
    return DEFAULT_TIMEZONE


def get_chat_venues(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key(PROJECT_ID, "configs"))
    key = str(chat_id)
    if entity and key in entity and "venues" in entity[key]:
        return entity[key]["venues"]
    return [DEFAULT_VENUE_ID]

def get_default_poll_closing_time():
    return datetime.datetime.now() + relativedelta(months=1)

def get_played_tourns(venue_id, chat_id):
    datastore_client = get_datastore_client()
    from_date = (datetime.datetime.now() - relativedelta(months=10)).strftime(
        "%Y-%m-%d"
    )

    entity = datastore_client.get(datastore_client.key(PROJECT_ID, chat_id))
    if not entity:
        entity = datastore.Entity(key=datastore_client.key(PROJECT_ID, chat_id))
    stored_played_tourns = [
        t for t in entity.get("played_tourns", []) if t["date"] > from_date
    ]
    if stored_played_tourns:
        months = 1
    else:
        months = 4
    sync_reqs = rating_api.get_sync_requests_ids(venue_id, months)
    stored_sync_reqs = set(
        [played_tourn["sync_req_id"] for played_tourn in stored_played_tourns]
    )
    for sync_req in sync_reqs:
        if sync_req not in stored_sync_reqs:
            tourn_id, tourn_date = rating_api.get_tourn_by_request(sync_req, chat_id)
            if tourn_id:
                tourn = rating_api.get_tourn_by_id(tourn_id)
                if "name" not in tourn or "editors" not in tourn:
                    print(f"Missing data for sync_req {sync_req}, tourn_id {tourn_id}")
                stored_played_tourns.append(
                    {
                        "sync_req_id": sync_req,
                        "tourn_id": tourn_id,
                        "norm_name": (
                            normalize_tourn_name(tourn["name"])
                            if "name" in tourn
                            else ""
                        ),
                        "editors": (
                            ", ".join(
                                sorted(
                                    [
                                        editor["name"][:1] + ". " + editor["surname"]
                                        for editor in tourn["editors"]
                                    ]
                                )
                            )
                            if "editors" in tourn
                            else ""
                        ),
                        "date": tourn_date,
                    }
                )
    entity.update({"played_tourns": stored_played_tourns})
    # print(entity)
    datastore_client.put(entity)

    return {
        played_tourn["tourn_id"]: (
            played_tourn["norm_name"],
            played_tourn["editors"],
            played_tourn["date"],
        )
        for played_tourn in stored_played_tourns
    }
