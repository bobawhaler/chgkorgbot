from dateparser import parse
import datetime
import pytz
import re
import rating_api
from dateutil.relativedelta import relativedelta
import os
import datastore

PROJECT_ID = os.environ.get("PROJECT_ID")
TELEGRAM_API_TOKEN = os.environ.get("TELEGRAM_API_TOKEN")
OBFUSCATION_TOKEN = os.environ.get("OBFUSCATION_TOKEN")

DEFAULT_TIMEZONE = "Europe/Berlin"
DEFAULT_VENUE_ID = 3053
DEFAULT_MIN_DIFFICULTY = 3.0
DEFAULT_MAX_DIFFICULTY = 100.0

COMMON_POLL_OPTIONS = ["буду играть любой", "не буду играть"]


def resolve_timezone(tz_name):
    """
    Resolves a timezone name to a valid pytz timezone.
    Returns default timezone if resolution fails.
    """
    if tz_name is None:
        return DEFAULT_TIMEZONE
    try:
        pytz.timezone(tz_name)
        return tz_name
    except pytz.UnknownTimeZoneError:
        # It's not a valid IANA timezone, let's try to map it
        tz_map = {
            "EEST": "Europe/Helsinki",
            "CEST": "Europe/Berlin",
            "MSK": "Europe/Moscow",
            "RU": "Europe/Moscow",
            "EET": "Europe/Helsinki",
            "CET": "Europe/Berlin",
        }
        resolved_tz = tz_map.get(tz_name.upper())
        if resolved_tz:
            return resolved_tz

        # If we can't resolve it, return default
        return DEFAULT_TIMEZONE


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

def get_chat_timezone(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "timezone" in chat_config:
        return resolve_timezone(chat_config["timezone"])
    return DEFAULT_TIMEZONE

def get_chat_min_difficulty(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "min_difficulty" in chat_config:
        return chat_config["min_difficulty"]
    return DEFAULT_MIN_DIFFICULTY

def get_chat_max_difficulty(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "max_difficulty" in chat_config:
        return chat_config["max_difficulty"]
    return DEFAULT_MAX_DIFFICULTY

def get_chat_venues(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "venues" in chat_config:
        return chat_config["venues"]
    return []

def get_default_poll_closing_time():
    return datetime.datetime.now() + relativedelta(months=1)
