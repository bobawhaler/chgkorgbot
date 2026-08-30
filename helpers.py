from dateparser import parse
import datetime
import time
import pytz
import re
import hmac
import hashlib
import threading
from urllib.parse import parse_qsl, unquote
import rating_api
from dateutil.relativedelta import relativedelta
import os
import json
import requests
import datastore
import debug

PROJECT_ID = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
TELEGRAM_API_TOKEN = os.environ.get("TELEGRAM_API_TOKEN")
OBFUSCATION_TOKEN = os.environ.get("OBFUSCATION_TOKEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("DEBUG_CHAT_ID")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "gamepollbot")


def get_bot_username():
    return os.environ.get("BOT_USERNAME", "gamepollbot")


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


def is_admin_user(user_id):
    """
    Checks strictly whether the user_id belongs to the admin.
    """
    admin_id_env = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("DEBUG_CHAT_ID")
    if not admin_id_env or user_id is None:
        return False
    allowed_ids = {s.strip() for s in re.split(r'[,\s]+', str(admin_id_env)) if s.strip()}
    return str(user_id) in allowed_ids


def determine_player_default_status(rating_team_id, pid, current_roster=None):
    """
    Determines default roster status ('K', 'B', or 'L') for a player added to the game roster:
    - If player is in base roster of rating_team_id on the rating site:
        - Returns 'K' if player is designated as captain (captain_id).
        - Returns 'B' if player is in base roster.
    - Otherwise (not in base roster, unrated, or no rating_team_id): returns 'L' (Legionnaire).
    """
    if rating_team_id and pid:
        try:
            res = rating_api.get_team_base_players(rating_team_id)
            if res and isinstance(res, (tuple, list)) and len(res) == 2:
                base_pids, captain_id = res
                if base_pids and pid in base_pids:
                    if captain_id and pid == captain_id:
                        return "K"
                    return "B"
        except Exception as e:
            print(f"Error determining player default status for team={rating_team_id} pid={pid}: {e}")
    return "L"


def format_player_button_text(p, prefix="👤"):
    """
    Formats player display text for Telegram inline keyboard buttons.
    Shortens patronymic to an initial (e.g. 'Замятин Александр А.') so that full first name
    and surname remain visible without being truncated by Telegram UI.
    """
    pid = p.get("id") or p.get("player_id") or p.get("pid")
    surname = str(p.get("surname", "") or "").strip()
    name = str(p.get("name", "") or "").strip()
    patronymic = str(p.get("patronymic", "") or "").strip()

    pat_str = f" {patronymic[0]}." if patronymic else ""
    town = str(p.get("town", "") or "").strip()
    town_str = f" ({town})" if town else ""
    id_str = f"[ID {pid}] " if pid else ""

    p_fio = f"{surname} {name}{pat_str}".strip()
    if prefix:
        return f"{prefix} {id_str}{p_fio}{town_str}".strip()
    return f"{id_str}{p_fio}{town_str}".strip()


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

def _parse_date_legacy(input_date, timezone):
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


ACTIVE_GEMINI_MODEL = None


def get_active_gemini_model():
    global ACTIVE_GEMINI_MODEL
    if ACTIVE_GEMINI_MODEL:
        return ACTIVE_GEMINI_MODEL

    try:
        persisted = datastore.get_gemini_active_model()
        if persisted:
            ACTIVE_GEMINI_MODEL = persisted
            return ACTIVE_GEMINI_MODEL
    except Exception as e:
        print(f"Error reading persisted Gemini model: {e}")

    env_model = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash").replace("models/", "")
    ACTIVE_GEMINI_MODEL = env_model
    return ACTIVE_GEMINI_MODEL


def set_active_gemini_model(model_name):
    global ACTIVE_GEMINI_MODEL
    clean_model = model_name.replace("models/", "").strip()
    ACTIVE_GEMINI_MODEL = clean_model
    try:
        datastore.set_gemini_active_model(clean_model)
    except Exception as e:
        print(f"Error persisting Gemini model: {e}")


def _discover_latest_gemini_model(api_key):
    """
    Queries Google Gemini API for available models and returns the best flash model name.
    """
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
        resp = requests.get(url, timeout=4.0)
        if resp.ok:
            data = resp.json()
            models = data.get("models", [])
            flash_models = [
                m["name"].replace("models/", "")
                for m in models
                if "generateContent" in m.get("supportedGenerationMethods", [])
                and "flash" in m.get("name", "").lower()
                and "experimental" not in m.get("name", "").lower()
                and "8b" not in m.get("name", "").lower()
            ]
            if flash_models:
                flash_models.sort(reverse=True)
                newest = flash_models[0]
                set_active_gemini_model(newest)
                print(f"[GEMINI_DISCOVERY] Auto-switched to latest model: {newest}")
                return newest
    except Exception as e:
        print(f"[GEMINI_DISCOVERY_ERROR] {e}")
    return get_active_gemini_model()


def parse_date_gemini(input_date, timezone):
    """
    Parses natural language date/time using Google Gemini Flash API.
    Returns: (parsed_date_or_datetime, with_time) or (None, False) on failure/unset API key.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None, False

    try:
        resolved_tz = resolve_timezone(timezone)
        tz_obj = pytz.timezone(resolved_tz)
        now = datetime.datetime.now(tz_obj)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        weekday_ru = [
            "понедельник",
            "вторник",
            "среда",
            "четверг",
            "пятница",
            "суббота",
            "воскресенье",
        ][now.weekday()]

        prompt = (
            f"You are a date and time parser for Russian text in a Telegram bot.\n"
            f"Current reference time: {now_str} (день недели: {weekday_ru})\n"
            f"Timezone: {resolved_tz}\n"
            f'User input: "{input_date}"\n\n'
            f"Parse the user input into a target date and time relative to the current reference time.\n"
            f"Rules:\n"
            f"1. If a relative day is mentioned (e.g. 'завтра', 'в следующую пятницу', 'послезавтра', 'сб'), calculate the exact YYYY-MM-DD.\n"
            f"2. If a specific time is mentioned (e.g. 'в 19:00', '19-30', 'в 7 вечера', 'до 18:00'), extract HH:MM (24h) and set with_time=true.\n"
            f"3. If only a date/day is mentioned without time, set time=null and with_time=false.\n"
            f"4. If only time is mentioned without a date ('в 19:00', '18:00'), assume today if that time is still in the future, or tomorrow if already past, and set with_time=true.\n"
            f"5. If input cannot be parsed as a date/time, set date=null, time=null, with_time=false.\n\n"
            f"Return JSON only:\n"
            f'{{"date": "YYYY-MM-DD" or null, "time": "HH:MM" or null, "with_time": boolean}}'
        )

        model_name = get_active_gemini_model()

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "response_mime_type": "application/json",
                "temperature": 0.0,
            },
        }

        resp = requests.post(url, json=payload, timeout=3.0)
        if not resp.ok:
            print(f"[GEMINI_PARSER_ERROR] HTTP {resp.status_code} with model '{model_name}': {resp.text}")
            
            # Auto-healing: if model is deprecated or not found, try to auto-switch
            if resp.status_code in (404, 400):
                match = re.search(r"use models/([a-zA-Z0-9\.\-]+)", resp.text, re.IGNORECASE)
                if match:
                    new_model = match.group(1).strip()
                    print(f"[GEMINI_AUTO_SWITCH] Switching model from '{model_name}' to suggested '{new_model}'")
                    set_active_gemini_model(new_model)
                else:
                    _discover_latest_gemini_model(api_key)

                current_active = get_active_gemini_model()
                if current_active != model_name:
                    retry_url = f"https://generativelanguage.googleapis.com/v1beta/models/{current_active}:generateContent?key={api_key}"
                    resp = requests.post(retry_url, json=payload, timeout=3.0)
                    if not resp.ok:
                        print(f"[GEMINI_RETRY_ERROR] HTTP {resp.status_code} with model '{current_active}': {resp.text}")
                        return None, False
                else:
                    return None, False
            else:
                return None, False

        res_json = resp.json()
        text_content = res_json["candidates"][0]["content"]["parts"][0]["text"]
        parsed_data = json.loads(text_content)

        date_val = parsed_data.get("date")
        time_val = parsed_data.get("time")
        with_time = bool(parsed_data.get("with_time"))

        if not date_val:
            return None, False

        if with_time and time_val:
            dt = datetime.datetime.strptime(f"{date_val} {time_val}", "%Y-%m-%d %H:%M")
            dt_aware = tz_obj.localize(dt)
            return dt_aware, True
        else:
            dt = datetime.datetime.strptime(date_val, "%Y-%m-%d").date()
            return dt, False

    except Exception as e:
        print(f"[GEMINI_PARSER_EXCEPTION] {e}")
        return None, False


def _format_date_res(res, with_time):
    if res is None:
        return "None"
    if with_time and hasattr(res, "strftime"):
        return f"{res.strftime('%Y-%m-%d %H:%M %Z')} (with_time=True)"
    elif hasattr(res, "strftime"):
        return f"{res.strftime('%Y-%m-%d')} (with_time=False)"
    return f"{res} (with_time={with_time})"


def _is_date_mismatch(legacy_res, legacy_with_time, gemini_res, gemini_with_time):
    if gemini_res is None:
        return False
    if legacy_with_time != gemini_with_time:
        return True
    if not legacy_with_time:
        leg_d = (
            legacy_res.strftime("%Y-%m-%d")
            if hasattr(legacy_res, "strftime")
            else str(legacy_res)
        )
        gem_d = (
            gemini_res.strftime("%Y-%m-%d")
            if hasattr(gemini_res, "strftime")
            else str(gemini_res)
        )
        return leg_d != gem_d
    else:
        try:
            leg_utc = legacy_res.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M")
            gem_utc = gemini_res.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M")
            return leg_utc != gem_utc
        except Exception:
            return str(legacy_res) != str(gemini_res)


def parse_date(input_date, timezone):
    legacy_date, legacy_with_time = _parse_date_legacy(input_date, timezone)

    # Shadow / Debug comparison with Gemini Flash
    try:
        gemini_date, gemini_with_time = parse_date_gemini(input_date, timezone)
        if gemini_date is not None:
            legacy_str = _format_date_res(legacy_date, legacy_with_time)
            gemini_str = _format_date_res(gemini_date, gemini_with_time)
            print(
                f"[DATE_PARSER_DEBUG] Input: '{input_date}' | TZ: {timezone}\n"
                f"  -> Legacy: {legacy_str}\n"
                f"  -> Gemini: {gemini_str}"
            )
            if _is_date_mismatch(
                legacy_date, legacy_with_time, gemini_date, gemini_with_time
            ):
                print(
                    f"[DATE_PARSER_MISMATCH] Date parsing divergence for '{input_date}' (TZ: {timezone}):\n"
                    f"  Legacy -> {legacy_str}\n"
                    f"  Gemini -> {gemini_str}"
                )
    except Exception as e:
        print(f"[DATE_PARSER_COMPARE_ERROR] Failed during comparison: {e}")

    return legacy_date, legacy_with_time

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

def get_chat_register_teams(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "register_teams" in chat_config:
        return bool(chat_config["register_teams"])
    return False

def get_chat_collect_rosters(chat_id):
    chat_config = datastore.get_chat_config(chat_id)
    if chat_config and "collect_rosters" in chat_config:
        return bool(chat_config["collect_rosters"])
    return False

def get_default_poll_closing_time():
    return datetime.datetime.now() + relativedelta(months=1)


def format_team_registration_text(tourn_name, url, representative_text, narrator_text, start_time, teams, include_roster_prompt=True):
    lines = [
        f'Подана заявка на <a href="{url}">"{tourn_name}"</a>. {representative_text}. {narrator_text}. Начало: {start_time}',
        "",
        "📝 <b>Регистрация команд открыта!</b>",
        "<i>Ответьте (reply) на это сообщение названием вашей команды для регистрации.</i>",
        "<i>Для отмены ответьте <code>/unregister</code> или <code>отмена</code>.</i>",
    ]
    if include_roster_prompt:
        lines.append("<i>Для сдачи состава перейдите в ЛС с ботом и нажмите <b>Старт</b>.</i>")
    lines.extend([
        "",
        f"<b>Зарегистрированные команды ({len(teams)}):</b>",
    ])
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


def validate_telegram_init_data(init_data_raw, bot_token=None):
    """
    Validates Telegram WebApp initData query string using HMAC-SHA256 according to Telegram specifications.
    Returns parsed user dict if valid, or None if invalid.
    """
    import hmac
    import hashlib
    import json
    from urllib.parse import parse_qsl

    if not init_data_raw:
        return None
    token = bot_token or TELEGRAM_API_TOKEN or os.environ.get("TELEGRAM_API_TOKEN")
    if not token:
        return None
    try:
        parsed_data = dict(parse_qsl(init_data_raw, keep_blank_values=True))
        if "hash" not in parsed_data:
            return None
        received_hash = parsed_data.pop("hash")
        parsed_data.pop("signature", None)
        
        # Sort key=value pairs alphabetically
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed_data.items()))
        
        # Secret key = HMAC_SHA256(b"WebAppData", bot_token)
        secret_key = hmac.new(b"WebAppData", token.encode("utf-8"), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        
        if hmac.compare_digest(calculated_hash, received_hash):
            user_json = parsed_data.get("user")
            if user_json:
                user = json.loads(user_json)
                return user
            return parsed_data
    except Exception as e:
        print(f"Error validating initData: {e}")
    return None


def get_webapp_url():
    """
    Returns the absolute HTTPS URL of the Mini App.
    """
    import time
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "chgkorgbot"
    custom_domain = os.environ.get("WEBAPP_URL")
    ts = int(time.time())
    if custom_domain:
        return f"{custom_domain.rstrip('/')}/webapp/roster?v={ts}"
    return f"https://{project_id}.appspot.com/webapp/roster?v={ts}"


def trigger_venue_sync(venue_id):
    """
    Triggers background synchronization of a venue.
    Uses Cloud Tasks if configured, or background thread fallback.
    """
    if not venue_id:
        return
    try:
        from google.cloud import tasks_v2
        project = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        queue = os.environ.get("CLOUD_TASKS_QUEUE", "bot-tasks")
        location = os.environ.get("APP_REGION", "us-central1")
        if project and queue:
            client = tasks_v2.CloudTasksClient()
            parent = client.queue_path(project, location, queue)
            target_url = f"https://{project}.appspot.com/sync_venue_task{OBFUSCATION_TOKEN}"
            payload = {"venue_id": int(venue_id)}
            task = {
                "http_request": {
                    "url": target_url,
                    "http_method": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(payload).encode(),
                }
            }
            client.create_task(parent=parent, task=task)
            print(f"[VENUE_SYNC_TRIGGER] Scheduled Cloud Task for venue {venue_id}")
            return
    except Exception as e:
        print(f"[VENUE_SYNC_TRIGGER] Cloud task scheduling failed: {e}")

    try:
        import threading
        import rating_api
        threading.Thread(target=rating_api.sync_venue_history, args=(venue_id,), daemon=True).start()
        print(f"[VENUE_SYNC_TRIGGER] Started background thread for venue {venue_id}")
    except Exception as e:
        print(f"[VENUE_SYNC_TRIGGER] Thread fallback failed: {e}")


def search_teams_tiered(query, user_id=None, chat_id=None):
    """
    Tiered team search:
    1. User recently entered teams (User History)
    2. Teams loaded from chat's venue(s) (Venue Teams)
    3. Direct search on rating site API (Rating API)
    """
    results = []
    seen_ids = set()
    seen_names = set()
    query_clean = (query or "").strip().lower()

    # 1. User History teams
    if user_id:
        history = datastore.get_user_history(user_id)
        for t in history.get("teams", []):
            tid = t.get("team_id") or t.get("id")
            tname = t.get("name") or t.get("team_name", "Команда")
            raw_town = t.get("town")
            town = raw_town.get("name", "") if isinstance(raw_town, dict) else (str(raw_town) if raw_town else "")
            dnames = t.get("display_names", [])

            matched_dname = None
            if query_clean:
                for dn in dnames:
                    if query_clean in str(dn).lower():
                        matched_dname = str(dn)
                        break

            matched = not query_clean or (query_clean in tname.lower() or (town and query_clean in town.lower()) or matched_dname)
            if matched:
                badge_text = f"⭐ Ваша история ({matched_dname})" if matched_dname and matched_dname.lower() != tname.lower() else "⭐ Ваша история"
                entry = {
                    "id": tid,
                    "name": tname,
                    "display_name": matched_dname,
                    "town": town,
                    "source": "history",
                    "badge": badge_text,
                }
                if tid:
                    seen_ids.add(int(tid))
                seen_names.add(tname.lower())
                results.append(entry)

    # 2. Venue teams
    venue_ids = []
    if chat_id:
        cfg = datastore.get_chat_config(chat_id)
        if cfg and "venues" in cfg:
            venue_ids = list(cfg["venues"])
    elif user_id:
        user_regs = datastore.get_user_active_registrations(user_id)
        for ur in user_regs:
            c_cfg = datastore.get_chat_config(ur.get("chat_id"))
            if c_cfg and "venues" in c_cfg:
                for vid in c_cfg["venues"]:
                    if vid not in venue_ids:
                        venue_ids.append(vid)

    if venue_ids:
        v_data = datastore.get_venues_data(venue_ids)
        for t in v_data.get("teams", []):
            tid = t.get("team_id") or t.get("id")
            tname = t.get("name") or t.get("team_name", "Команда")
            raw_town = t.get("town")
            town = raw_town.get("name", "") if isinstance(raw_town, dict) else (str(raw_town) if raw_town else "")
            if tid and int(tid) in seen_ids:
                continue
            if tname.lower() in seen_names:
                continue

            dnames = t.get("display_names", [])
            matched_dname = None
            if query_clean:
                for dn in dnames:
                    if query_clean in str(dn).lower():
                        matched_dname = str(dn)
                        break

            matched = not query_clean or (query_clean in tname.lower() or (town and query_clean in town.lower()) or matched_dname)
            if matched:
                badge_text = f"📍 Площадка ({matched_dname})" if matched_dname and matched_dname.lower() != tname.lower() else "📍 Площадка"
                entry = {
                    "id": tid,
                    "name": tname,
                    "display_name": matched_dname,
                    "town": town,
                    "source": "venue",
                    "badge": badge_text,
                }
                if tid:
                    seen_ids.add(int(tid))
                seen_names.add(tname.lower())
                results.append(entry)

    # 3. Rating site API direct search (only if query provided)
    if query_clean and len(query_clean) >= 2:
        try:
            api_teams = rating_api.search_teams(query)
            for at in api_teams:
                tid = at.get("id")
                tname = at.get("name", "Команда")
                raw_town = at.get("town")
                town = raw_town.get("name", "") if isinstance(raw_town, dict) else (str(raw_town) if raw_town else "")
                if tid and int(tid) in seen_ids:
                    continue
                if tname.lower() in seen_names:
                    continue
                entry = {
                    "id": tid,
                    "name": tname,
                    "town": town,
                    "source": "rating",
                    "badge": "",
                }
                if tid:
                    seen_ids.add(int(tid))
                seen_names.add(tname.lower())
                results.append(entry)
        except Exception as e:
            print(f"Error in rating_api.search_teams during tiered search: {e}")

    def compute_team_relevance(entry):
        if not query_clean:
            source_order = {"history": 3, "venue": 2, "rating": 1}
            return source_order.get(entry.get("source"), 0)

        tname = (entry.get("name") or "").strip().lower()
        town = (entry.get("town") or "").strip().lower()
        dname = (entry.get("display_name") or "").strip().lower()
        source = entry.get("source", "rating")

        score = 0.0

        # 1. Primary priority: Official base team name matches
        if tname == query_clean:
            score += 1000.0
        elif tname.startswith(query_clean):
            score += 800.0
        elif query_clean in tname:
            score += 600.0
        elif all(word in tname for word in query_clean.split()):
            score += 500.0

        # 2. Secondary priority: One-off display name matches
        if dname:
            if dname == query_clean:
                score += 400.0
            elif dname.startswith(query_clean):
                score += 300.0
            elif query_clean in dname:
                score += 200.0
            elif all(word in dname for word in query_clean.split()):
                score += 150.0

        # 3. Lowest priority: Town matches
        if town:
            if town == query_clean:
                score += 100.0
            elif query_clean in town:
                score += 50.0

        # Context source bonus
        if source == "history":
            score += 15.0
        elif source == "venue":
            score += 10.0

        return score

    results.sort(key=compute_team_relevance, reverse=True)
    return results


def search_players_tiered(query, user_id=None, rating_team_id=None, chat_id=None):
    """
    Tiered player search:
    1. User's profile & User history players
    2. Base team players & Venue players for this team/chat
    3. Direct search on rating site API (Rating API)
    """
    results = []
    seen_ids = set()
    seen_names = set()
    query_clean = (query or "").strip().lower()

    # 1. User profile & History players
    if user_id:
        mapping = datastore.get_user_mapping(user_id)
        if mapping and mapping.get("rating_player_id"):
            pid = mapping["rating_player_id"]
            pname = f"{mapping.get('surname', '')} {mapping.get('name', '')}".strip()
            full_name = f"{mapping.get('surname', '')} {mapping.get('name', '')} {mapping.get('patronymic', '')}".strip()
            if not query_clean or query_clean in full_name.lower():
                entry = {
                    "id": pid,
                    "name": mapping.get("name", ""),
                    "surname": mapping.get("surname", ""),
                    "patronymic": mapping.get("patronymic", ""),
                    "town": mapping.get("town", ""),
                    "source": "self",
                    "badge": "👤 Ваш профиль",
                }
                seen_ids.add(int(pid))
                seen_names.add(pname.lower())
                results.append(entry)

        history = datastore.get_user_history(user_id)
        for hp in history.get("players", []):
            pid = hp.get("player_id") or hp.get("id")
            if pid and int(pid) in seen_ids:
                continue
            pname = f"{hp.get('surname', '')} {hp.get('name', '')}".strip()
            full_name = f"{hp.get('surname', '')} {hp.get('name', '')} {hp.get('patronymic', '')}".strip()
            if pname.lower() in seen_names:
                continue
            if not query_clean or query_clean in full_name.lower():
                entry = {
                    "id": pid,
                    "name": hp.get("name", ""),
                    "surname": hp.get("surname", ""),
                    "patronymic": hp.get("patronymic", ""),
                    "town": hp.get("town", ""),
                    "source": "history",
                    "badge": "⭐ Ваша история",
                }
                if pid:
                    seen_ids.add(int(pid))
                seen_names.add(pname.lower())
                results.append(entry)

    # 2. Base team players & Venue players
    if rating_team_id:
        try:
            team_players = rating_api.get_team_players(rating_team_id)
            for tp in team_players:
                pid = tp.get("id") or tp.get("player_id")
                if pid and int(pid) in seen_ids:
                    continue
                pname = f"{tp.get('surname', '')} {tp.get('name', '')}".strip()
                full_name = f"{tp.get('surname', '')} {tp.get('name', '')} {tp.get('patronymic', '')}".strip()
                if pname.lower() in seen_names:
                    continue
                if not query_clean or query_clean in full_name.lower():
                    entry = {
                        "id": pid,
                        "name": tp.get("name", ""),
                        "surname": tp.get("surname", ""),
                        "patronymic": tp.get("patronymic", ""),
                        "town": tp.get("town", ""),
                        "source": "team",
                        "badge": "🛡 Состав команды",
                    }
                    if pid:
                        seen_ids.add(int(pid))
                    seen_names.add(pname.lower())
                    results.append(entry)
        except Exception as e:
            print(f"Error fetching team players in tiered search: {e}")

    # Check venue players for team or venue
    venue_ids = []
    if chat_id:
        cfg = datastore.get_chat_config(chat_id)
        if cfg and "venues" in cfg:
            venue_ids = list(cfg["venues"])

    if venue_ids and rating_team_id:
        v_roster = datastore.get_venues_team_rosters(venue_ids, rating_team_id)
        for vp in v_roster:
            pid = vp.get("player_id") or vp.get("id")
            if pid and int(pid) in seen_ids:
                continue
            pname = f"{vp.get('surname', '')} {vp.get('name', '')}".strip()
            full_name = f"{vp.get('surname', '')} {vp.get('name', '')} {vp.get('patronymic', '')}".strip()
            if pname.lower() in seen_names:
                continue
            if not query_clean or query_clean in full_name.lower():
                entry = {
                    "id": pid,
                    "name": vp.get("name", ""),
                    "surname": vp.get("surname", ""),
                    "patronymic": vp.get("patronymic", ""),
                    "town": vp.get("town", ""),
                    "source": "venue",
                    "badge": "📍 Игрок площадки",
                }
                if pid:
                    seen_ids.add(int(pid))
                seen_names.add(pname.lower())
                results.append(entry)

    # 3. Rating site API direct search
    if query_clean and len(query_clean) >= 3:
        try:
            api_players = rating_api.search_players(query)
            for ap in api_players:
                pid = ap.get("id")
                if pid and int(pid) in seen_ids:
                    continue
                pname = f"{ap.get('surname', '')} {ap.get('name', '')}".strip()
                full_name = f"{ap.get('surname', '')} {ap.get('name', '')} {ap.get('patronymic', '')}".strip()
                if pname.lower() in seen_names:
                    continue
                entry = {
                    "id": pid,
                    "name": ap.get("name", ""),
                    "surname": ap.get("surname", ""),
                    "patronymic": ap.get("patronymic", ""),
                    "town": ap.get("town", ""),
                    "source": "rating",
                    "badge": "",
                }
                if pid:
                    seen_ids.add(int(pid))
                seen_names.add(pname.lower())
                results.append(entry)
        except Exception as e:
            print(f"Error in rating_api.search_players during tiered search: {e}")

    return results


def get_roster_candidates(user_id, rating_team_id=None, current_roster=None, chat_id=None):
    """
    Computes ranked candidate suggestions for building a team roster with strict priority:
    1. User's profile & User history players (highest priority).
    2. Base team players & Venue players for this team.
    """
    import time
    current_roster = current_roster or []
    roster_pids = {p["player_id"] for p in current_roster if p.get("player_id")}
    roster_names = {f"{p.get('surname', '')} {p.get('name', '')}".strip().lower() for p in current_roster}

    base_pids = set()
    captain_id = None
    if rating_team_id:
        try:
            b_res = rating_api.get_team_base_players(rating_team_id)
            if b_res and isinstance(b_res, (tuple, list)) and len(b_res) == 2:
                base_pids = set(b_res[0]) if b_res[0] else set()
                captain_id = b_res[1]
        except Exception as e:
            print(f"Error getting team base players for candidates: {e}")

    def resolve_status(pid):
        if not pid or not rating_team_id:
            return "L"
        if captain_id and pid == captain_id:
            return "K"
        if pid in base_pids:
            return "B"
        return "L"

    candidates_map = {}
    now_ts = time.time()

    # 1. Self profile
    user_mapping = datastore.get_user_mapping(user_id) if user_id else None
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
                "category": "self",
                "badge": "👤 Ваш профиль",
                "default_status": resolve_status(my_pid),
                "score": 100000.0,
            }

    # 2. User history players (frequently and recently entered by user)
    if user_id:
        history = datastore.get_user_history(user_id)
        history_players = history.get("players", [])

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
                    "category": "history",
                    "badge": "⭐ Ваша история",
                    "default_status": resolve_status(pid),
                    "score": 10000.0,
                }
            h_score = 0.0
            use_count = hp.get("count", 1)
            h_score += use_count * 50.0
            h_score += max(0.0, 100.0 - idx * 5.0)

            last_used_str = hp.get("last_used")
            if last_used_str:
                try:
                    lu_dt = datetime.datetime.fromisoformat(last_used_str)
                    age_days = (now_ts - lu_dt.timestamp()) / 86400.0
                    if age_days <= 1.0:
                        h_score += 200.0
                    elif age_days <= 7.0:
                        h_score += 150.0
                    elif age_days <= 30.0:
                        h_score += 100.0
                    elif age_days <= 365.0:
                        h_score += 50.0
                except Exception:
                    pass

            candidates_map[key]["score"] += h_score

    # 3. Base team players from rating site API
    if rating_team_id:
        team_players = rating_api.get_team_players(rating_team_id)
        for tp in team_players:
            pid = tp.get("id") or tp.get("player_id")
            pname = f"{tp.get('surname', '')} {tp.get('name', '')}".strip()
            if (pid and pid in roster_pids) or (pname.lower() in roster_names):
                continue
            key = f"pid_{pid}" if pid else f"name_{pname.lower()}"
            is_base = pid in base_pids if pid else False
            is_cap = (pid == captain_id) if pid and captain_id else False
            if key not in candidates_map:
                candidates_map[key] = {
                    "pid": pid,
                    "name": tp.get("name", ""),
                    "surname": tp.get("surname", ""),
                    "patronymic": tp.get("patronymic", ""),
                    "town": tp.get("town", ""),
                    "pname": pname,
                    "is_base": is_base,
                    "is_captain": is_cap,
                    "category": "team",
                    "badge": "👑 Капитан" if is_cap else ("🛡 Базовый" if is_base else "⚔️ Состав"),
                    "default_status": resolve_status(pid),
                    "score": 0.0,
                }
            t_score = 0.0
            if is_cap:
                t_score += 5000.0
            if is_base:
                t_score += 3000.0
            t_score += tp.get("tourn_count", 0) * 50.0
            t_score += tp.get("tourn_recency", 0) * 20.0
            t_score += tp.get("season_recency", 0) * 100.0
            candidates_map[key]["score"] += t_score

        # 4. Venue team players (played for this team at chat's venue)
        venue_ids = []
        if chat_id:
            cfg = datastore.get_chat_config(chat_id)
            if cfg and "venues" in cfg:
                venue_ids = list(cfg["venues"])
        if venue_ids:
            v_roster = datastore.get_venues_team_rosters(venue_ids, rating_team_id)
            for vp in v_roster:
                pid = vp.get("player_id") or vp.get("id")
                pname = f"{vp.get('surname', '')} {vp.get('name', '')}".strip()
                if (pid and pid in roster_pids) or (pname.lower() in roster_names):
                    continue
                key = f"pid_{pid}" if pid else f"name_{pname.lower()}"
                if key not in candidates_map:
                    candidates_map[key] = {
                        "pid": pid,
                        "name": vp.get("name", ""),
                        "surname": vp.get("surname", ""),
                        "patronymic": vp.get("patronymic", ""),
                        "town": vp.get("town", ""),
                        "pname": pname,
                        "category": "venue",
                        "badge": "📍 Игрок площадки",
                        "default_status": resolve_status(pid),
                        "score": 2500.0,
                    }
                candidates_map[key]["score"] += vp.get("tourn_count", 1) * 100.0

    candidates = list(candidates_map.values())
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates



