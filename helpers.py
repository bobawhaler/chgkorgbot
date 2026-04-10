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
DEFAULT_MIN_DIFFICULTY = 3.0
DEFAULT_MAX_DIFFICULTY = 100.0

COMMON_POLL_OPTIONS = ["буду играть любой", "не буду играть"]

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
    key = datastore_client.key("ChatState", str(chat_id))
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            entity = datastore.Entity(key=key, exclude_from_indexes=("data", "played_tourns"))
        entity.update({"data": tourns_to_save})
        datastore_client.put(entity)

def fetch_data(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key("ChatState", str(chat_id)))
    if entity and "data" in entity:
        return entity["data"]
    return []

def traverse_finished_tasks():
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="PollTask")
    
    now = int(datetime.datetime.now().timestamp())
    chat_task_counts = {}
    finished_tasks = []
    
    all_tasks = list(query.fetch())
    for task in all_tasks:
        cid = task.get("chat_id")
        chat_task_counts[cid] = chat_task_counts.get(cid, 0) + 1
        
        if task.get("end_time", 0) <= now:
            finished_tasks.append(task)
            
    for task in finished_tasks:
        cid = task.get("chat_id")
        has_multiple_candidates = chat_task_counts[cid] > 1
        
        valid = False
        with datastore_client.transaction():
            if datastore_client.get(task.key):
                datastore_client.delete(task.key)
                valid = True
        
        if valid:
            yield dict(task), has_multiple_candidates

def remove_task(chat_id, message_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("PollTask", f"{chat_id}_{message_id}")
    datastore_client.delete(key)

def pop_task(chat_id, message_id):
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="PollTask")
    query.add_filter("chat_id", "=", chat_id)
    chat_tasks = list(query.fetch())
    
    return_task = None
    has_multiple_candidates = len(chat_tasks) > 1
    
    if len(chat_tasks) == 1:
        return_task = chat_tasks[0]
    elif message_id and len(chat_tasks) > 1:
        for task in chat_tasks:
            if task.get("message_id") == message_id:
                return_task = task
                break
                
    if return_task:
        with datastore_client.transaction():
            task_entity = datastore_client.get(return_task.key)
            if task_entity:
                datastore_client.delete(return_task.key)
                return_task = dict(task_entity)
            else:
                return_task = None
        
    return return_task, has_multiple_candidates

def add_task(chat_id, message_id, end_time_ts, tourn_ids, with_results):
    datastore_client = get_datastore_client()
    key = datastore_client.key("PollTask", f"{chat_id}_{message_id}")
    entity = datastore.Entity(key=key)
    entity.update({
        "chat_id": chat_id,
        "message_id": message_id,
        "end_time": end_time_ts,
        "tourn_ids": tourn_ids,
        "with_results": with_results,
    })
    datastore_client.put(entity)

def update_chat_config(chat_id, thread_id, **kwargs):
    datastore_client = get_datastore_client()
    key = datastore_client.key("ChatConfig", str(chat_id))
    
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            entity = datastore.Entity(key=key)

        if thread_id is not None:
            entity["thread_id"] = thread_id
            
        for k, v in kwargs.items():
            if k == "venues" and isinstance(v, str):
                entity[k] = [venue.strip() for venue in v.split(",") if venue.strip()]
            else:
                entity[k] = v
                
        datastore_client.put(entity)

def get_all_configs():
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="ChatConfig")
    configs = {}
    for entity in query.fetch():
        configs[entity.key.name] = dict(entity)
    return configs

def get_chat_timezone(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key("ChatConfig", str(chat_id)))
    if entity and "timezone" in entity:
        return entity["timezone"]
    return DEFAULT_TIMEZONE

def get_chat_min_difficulty(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key("ChatConfig", str(chat_id)))
    if entity and "min_difficulty" in entity:
        return entity["min_difficulty"]
    return DEFAULT_MIN_DIFFICULTY

def get_chat_max_difficulty(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key("ChatConfig", str(chat_id)))
    if entity and "max_difficulty" in entity:
        return entity["max_difficulty"]
    return DEFAULT_MAX_DIFFICULTY

def get_chat_venues(chat_id):
    datastore_client = get_datastore_client()
    entity = datastore_client.get(datastore_client.key("ChatConfig", str(chat_id)))
    if entity and "venues" in entity:
        return entity["venues"]
    return []

def get_default_poll_closing_time():
    return datetime.datetime.now() + relativedelta(months=1)

def get_played_tourns(venue_id, chat_id):
    datastore_client = get_datastore_client()
    from_date = (datetime.datetime.now() - relativedelta(months=10)).strftime(
        "%Y-%m-%d"
    )

    key = datastore_client.key("ChatState", str(chat_id))
    
    # Needs transaction to ensure stored_played_tourns won't be overridden if modified simultaneously
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            entity = datastore.Entity(key=key, exclude_from_indexes=("data", "played_tourns"))
            
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
        datastore_client.put(entity)

    return {
        played_tourn["tourn_id"]: (
            played_tourn["norm_name"],
            played_tourn["editors"],
            played_tourn["date"],
        )
        for played_tourn in stored_played_tourns
    }
