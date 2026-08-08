from dateparser import parse
import datetime
import pytz
import re
import rating_api
from dateutil.relativedelta import relativedelta
import os
import datastore
import debug

PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
TELEGRAM_API_TOKEN = os.environ.get("TELEGRAM_API_TOKEN")
OBFUSCATION_TOKEN = os.environ.get("OBFUSCATION_TOKEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("DEBUG_CHAT_ID")


def is_debug_allowed(user_id=None, chat_id=None):
    """
    Checks whether debug/test functions are allowed for the given user_id and/or chat_id.
    Allowed if user_id/chat_id matches ADMIN_CHAT_ID or DEBUG_CHAT_ID environment variable.
    """
    admin_id_env = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("DEBUG_CHAT_ID")
    if not admin_id_env:
        return False

    allowed_ids = {s.strip() for s in re.split(r'[,\s]+', str(admin_id_env)) if s.strip()}

    if user_id is not None and str(user_id) in allowed_ids:
        return True
    if chat_id is not None and str(chat_id) in allowed_ids:
        return True

    return False


def determine_player_default_status(rating_team_id, pid, current_roster):
    """
    Determines default roster status ('K', 'B', or 'L') for a player added to the game roster:
    - Base team players get 'K' (if no captain in roster yet) or 'B'.
    - Players not in base roster (or unrated players) get 'L' (Legionnaire).
    """
    has_captain = any(p.get("status") == "K" for p in current_roster)

    if rating_team_id and pid:
        base_pids, captain_id = rating_api.get_team_base_players(rating_team_id)
        if pid in base_pids:
            if not has_captain and (pid == captain_id or not any(p.get("player_id") in base_pids for p in current_roster)):
                return "K"
            return "B" if has_captain else "K"

    if rating_team_id:
        return "L"

    return "K" if not current_roster else "B"


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
    input_str = input_date.strip().lower()

    for fmt in ("%Y%m%d", "%Y-%m-%d", "%d.%m.%Y", "%d.%m"):
        try:
            dt = datetime.datetime.strptime(input_str, fmt)
            if fmt == "%d.%m":
                now_tz = datetime.datetime.now(pytz.timezone(resolve_timezone(timezone)))
                dt = dt.replace(year=now_tz.year)
            if debug.get_debug():
                print("parse_date:", input_date, "->", dt.date())
            return dt.date(), False
        except ValueError:
            pass

    normal_date = (
        input_str.replace("понедельника", "понедельник")
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
    if debug.get_debug():
        print("parse_date:", input_date, "->", result_date)
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

import html

def get_tourns_representations(tourns):
    tourns_to_save = []
    tourns_to_show = []

    for tourn in sorted(
        tourns, key=lambda r: (r["rating"], r["difficulty"]), reverse=True
    ):
        title_raw = tourn["name"].strip()
        title = html.escape(title_raw)
        n_questions = (
            f'{tourn["num_questions"]}, ' if tourn["num_questions"] > 0 else ""
        )
        rating = "R, " if tourn["rating"] else ""
        difficulty = f'{tourn["difficulty"]}, ' if tourn["difficulty"] != 0 else ""
        editors_raw = tourn["editors"]
        editors = html.escape(editors_raw)
        url = f'https://rating.chgk.info/tournament/{tourn["id"]}'
        tourn_long = (
            f'<a href="{url}">{title}</a> ({n_questions}{rating}{difficulty}{editors})'
        )
        tourns_to_show.append(tourn_long)
        tourn_short = f"{title_raw} ({n_questions}{rating}{difficulty}{editors_raw})"
        if len(tourn_short) > 100:
            cut_length = len(tourn_short) - 98
            pos = editors_raw[:-cut_length].rfind(",")
            tourn_short = (
                f"{title_raw} ({n_questions}{rating}{difficulty}{editors_raw[:pos]}...)"
            )
            if len(tourn_short) > 100:
                if len(editors_raw) > 30:
                    editors_cut_length = 30
                    editors_cut_pos = editors_raw[:-editors_cut_length].rfind(",")
                    post_title = f" ({n_questions}{rating}{difficulty}{editors_raw[:editors_cut_pos]}...)"
                else:
                    post_title = f" ({n_questions}{rating}{difficulty}{editors_raw})"
                title_cut_length = 98 - len(post_title)
                title_cut_pos = title_raw[:title_cut_length].rfind(" ")
                tourn_short = f"{title_raw[:title_cut_pos]}...{post_title}"

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


def format_team_registration_text(tourn_name, url, representative_text, narrator_text, start_time, teams):
    lines = [
        f'Подана заявка на <a href="{url}">"{tourn_name}"</a>. {representative_text}. {narrator_text}. Начало: {start_time}',
        "",
        "📝 <b>Регистрация команд открыта!</b>",
        "<i>Ответьте (reply) на это сообщение названием вашей команды для регистрации.</i>",
        "<i>Для отмены ответьте <code>/unregister</code> или <code>отмена</code>.</i>",
        "<i>Для сдачи состава перейдите в ЛС с ботом и нажмите <b>Старт</b>.</i>",
        "",
        f"<b>Зарегистрированные команды ({len(teams)}):</b>",
    ]
    if not teams:
        lines.append("(пока нет поданных заявок)")
    else:
        for i, t in enumerate(teams, 1):
            team_name = t.get("display_name") or t.get("team_name", "Команда")
            username = t.get("username", "")
            user_str = f" (@{username})" if username else ""
            lines.append(f"{i}. {team_name}{user_str}")
    return "\n".join(lines)


def generate_roster_csv(teams_list):
    """
    Generates CSV string formatted for rating.chgk.info import without header row:
    idteam;команда;город;признак капитана (К|Б|Л);idplayer;Ф;И;О;
    """
    rows = []
    status_map = {"K": "К", "B": "Б", "L": "Л", "К": "К", "Б": "Б", "Л": "Л"}
    for t in teams_list:
        idteam = str(t.get("rating_team_id") or "")
        team_name = (t.get("display_name") or t.get("team_name") or "").replace(";", ",")
        town = (t.get("town") or "").replace(";", ",")
        roster = t.get("roster", [])
        for p in roster:
            raw_st = p.get("status", "B")
            status = status_map.get(raw_st, "Б")
            idplayer = str(p.get("player_id") or "")
            surname = (p.get("surname") or "").replace(";", ",")
            name = (p.get("name") or "").replace(";", ",")
            patronymic = (p.get("patronymic") or "").replace(";", ",")
            row = f"{idteam};{team_name};{town};{status};{idplayer};{surname};{name};{patronymic};"
            rows.append(row)
    return "\n".join(rows)


def get_registration_start_ts(reg):
    if reg.get("start_time_ts"):
        return reg["start_time_ts"]
    
    start_str = reg.get("start_time", "")
    if start_str:
        s = start_str.strip()
        tz = pytz.timezone(resolve_timezone(None))
        now_tz = datetime.datetime.now(tz)
        year = now_tz.year
        for fmt in ("%d.%m %H:%M", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d.%m %H:%M:%S"):
            try:
                dt = datetime.datetime.strptime(s, fmt)
                if fmt.startswith("%d.%m "):
                    dt = dt.replace(year=year)
                localized = tz.localize(dt)
                return int(localized.timestamp())
            except ValueError:
                pass
        try:
            parsed_dt, _ = parse_date(start_str, "Europe/Berlin")
            if isinstance(parsed_dt, datetime.datetime):
                return int(parsed_dt.timestamp())
            elif isinstance(parsed_dt, datetime.date):
                return int(datetime.datetime.combine(parsed_dt, datetime.time(19, 0)).timestamp())
        except Exception:
            pass
            
    created_at = reg.get("created_at")
    if isinstance(created_at, datetime.datetime):
        return int(created_at.timestamp())
    return None


