# -*- coding: utf-8 -*-

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
import os
import datetime
import json
import pytz
from dateutil.relativedelta import relativedelta


def get_datastore_client():
    import os
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    return datastore.Client(project=project_id) if project_id else datastore.Client()

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
    query.add_filter(filter=PropertyFilter("chat_id", "=", chat_id))
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

def get_monitored_venues():
    datastore_client = get_datastore_client()
    key = datastore_client.key("MonitoredVenues", "main")
    entity = datastore_client.get(key)
    if entity and "venues" in entity:
        return entity["venues"]
    return {}


def update_chat_config(chat_id, thread_id, **kwargs):
    datastore_client = get_datastore_client()
    chat_key = datastore_client.key("ChatConfig", str(chat_id))
    monitored_key = datastore_client.key("MonitoredVenues", "main")
    
    with datastore_client.transaction():
        entity = datastore_client.get(chat_key)
        if not entity:
            entity = datastore.Entity(key=chat_key)

        if thread_id is not None:
            entity["thread_id"] = thread_id
            
        for k, v in kwargs.items():
            if k == "venues" and isinstance(v, str):
                entity[k] = [venue.strip() for venue in v.split(",") if venue.strip()]
            else:
                entity[k] = v
                
        datastore_client.put(entity)

        if "venues" in kwargs:
            new_venues = entity["venues"]
            
            monitored_entity = datastore_client.get(monitored_key)
            if not monitored_entity:
                monitored_entity = datastore.Entity(key=monitored_key)
                monitored_entity["venues"] = {}
                
            venues = monitored_entity.get("venues", {})
            
            for v_id in list(venues.keys()):
                if str(chat_id) in venues[v_id]:
                    venues[v_id].remove(str(chat_id))
                if not venues[v_id]:
                    del venues[v_id]
                    
            for v_id in new_venues:
                v_id_str = str(v_id).strip()
                if not v_id_str:
                    continue
                if v_id_str not in venues:
                    venues[v_id_str] = []
                if str(chat_id) not in venues[v_id_str]:
                    venues[v_id_str].append(str(chat_id))
                    
            monitored_entity["venues"] = venues
            datastore_client.put(monitored_entity)

def get_all_configs():
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="ChatConfig")
    configs = {}
    for entity in query.fetch():
        configs[entity.key.name] = dict(entity)
    return configs

def get_chat_config(chat_id):
    datastore_client = get_datastore_client()
    return datastore_client.get(datastore_client.key("ChatConfig", str(chat_id)))

def get_gemini_active_model():
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("BotConfig", "gemini_model")
        entity = datastore_client.get(key)
        if entity and "model" in entity:
            return entity["model"]
    except Exception as e:
        print(f"Error reading gemini model from datastore: {e}")
    return None

def set_gemini_active_model(model_name):
    if not model_name:
        return
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("BotConfig", "gemini_model")
        entity = datastore.Entity(key=key)
        entity.update({
            "model": model_name,
            "updated_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
    except Exception as e:
        print(f"Error saving gemini model to datastore: {e}")

def get_played_tourns(venue_id, chat_id):
    import rating_api
    from helpers import normalize_tourn_name

    datastore_client = get_datastore_client()
    from_date = (datetime.datetime.now(pytz.utc) - relativedelta(months=10)).strftime(
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
                    if not tourn or "name" not in tourn or "editors" not in tourn:
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

def is_known_sync_request(sync_req_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("KnownSyncRequest", str(sync_req_id))
    return datastore_client.get(key) is not None

def add_known_sync_request(sync_req_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("KnownSyncRequest", str(sync_req_id))
    entity = datastore.Entity(key=key)
    entity.update({"added_at": datetime.datetime.now(pytz.utc)})
    datastore_client.put(entity)

def cleanup_old_sync_requests():
    datastore_client = get_datastore_client()
    threshold = datetime.datetime.now(pytz.utc) - relativedelta(days=7)
    query = datastore_client.query(kind="KnownSyncRequest")
    query.add_filter(filter=PropertyFilter("added_at", "<", threshold))
    
    for entity in query.fetch():
        datastore_client.delete(entity.key)


def get_cached_tournament(tourn_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("CachedTournament", str(tourn_id))
    entity = datastore_client.get(key)
    print(f"[CACHE] get_cached_tournament key={key}, entity={dict(entity) if entity else None}")
    if entity and entity.get("name"):
        raw_editors = entity.get("editors", [])
        editors = []
        for e in raw_editors:
            if isinstance(e, dict):
                editors.append({
                    "name": e.get("name", ""),
                    "surname": e.get("surname", "")
                })
            elif isinstance(e, str):
                parts = e.split(" ", 1)
                if len(parts) == 2:
                    editors.append({"name": parts[0], "surname": parts[1]})
                else:
                    editors.append({"name": e, "surname": ""})
        return {
            "name": entity["name"],
            "editors": editors,
        }
    return None


def cache_tournament(tourn_id, data):
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedTournament", str(tourn_id))
        editors = []
        for e in data.get("editors", []):
            if isinstance(e, dict):
                editors.append({
                    "name": e.get("name", ""),
                    "surname": e.get("surname", "")
                })
            elif isinstance(e, str):
                parts = e.split(" ", 1)
                if len(parts) == 2:
                    editors.append({"name": parts[0], "surname": parts[1]})
                else:
                    editors.append({"name": e, "surname": ""})
        entity = datastore.Entity(key=key)
        entity.update({
            "name": data.get("name", ""),
            "editors": editors,
            "cached_at": datetime.datetime.now(pytz.utc),
        })
        print(f"[CACHE] cache_tournament key={key}, name={data.get('name', '')}, editors={editors}")
        datastore_client.put(entity)
        print(f"[CACHE] cache_tournament put success, entity={dict(entity)}")
    except Exception as e:
        print(f"[CACHE ERROR] cache_tournament failed for {tourn_id}: {e}")


def cleanup_old_cached_tournaments():
    datastore_client = get_datastore_client()
    threshold = datetime.datetime.now(pytz.utc) - relativedelta(days=30)
    query = datastore_client.query(kind="CachedTournament")
    query.add_filter(filter=PropertyFilter("cached_at", "<", threshold))
    
    for entity in query.fetch():
        datastore_client.delete(entity.key)


def cache_tournaments_batch(tournaments_data):
    try:
        datastore_client = get_datastore_client()
        entities = []
        for tourn in tournaments_data:
            tourn_id = tourn.get("id")
            if not tourn_id:
                continue
            key = datastore_client.key("CachedTournament", str(tourn_id))
            
            editors = []
            raw_editors = tourn.get("editors", [])
            if isinstance(raw_editors, list):
                for e in raw_editors:
                    if isinstance(e, dict):
                        editors.append({
                            "name": e.get("name", ""),
                            "surname": e.get("surname", "")
                        })
                    elif isinstance(e, str):
                        parts = e.split(" ", 1)
                        if len(parts) == 2:
                            editors.append({"name": parts[0], "surname": parts[1]})
                        else:
                            editors.append({"name": e, "surname": ""})
            elif isinstance(raw_editors, str):
                for name in raw_editors.split(","):
                    name = name.strip()
                    if name:
                        parts = name.split(" ", 1)
                        if len(parts) == 2:
                            editors.append({"name": parts[0], "surname": parts[1]})
                        else:
                            editors.append({"name": name, "surname": ""})
                            
            entity = datastore.Entity(key=key)
            entity.update({
                "name": tourn.get("name", ""),
                "editors": editors,
                "cached_at": datetime.datetime.now(pytz.utc),
            })
            entities.append(entity)
            
        if entities:
            chunk_size = 400
            for i in range(0, len(entities), chunk_size):
                chunk = entities[i:i + chunk_size]
                datastore_client.put_multi(chunk)
            print(f"[CACHE BATCH] Successfully cached {len(entities)} tournaments")
    except Exception as e:
        print(f"Error in cache_tournaments_batch: {e}")


def get_cached_player(pid):
    if not pid:
        return None
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedPlayer", str(pid))
        entity = datastore_client.get(key)
        if entity and (entity.get("surname") or entity.get("name")):
            return {
                "id": int(entity.get("player_id", pid)),
                "name": entity.get("name", ""),
                "surname": entity.get("surname", ""),
                "patronymic": entity.get("patronymic", ""),
                "town": entity.get("town", ""),
                "rating": entity.get("rating", 0),
                "tourn_count": entity.get("tourn_count", 0),
                "cached_at": entity.get("cached_at"),
            }
    except Exception as e:
        print(f"Error reading CachedPlayer pid={pid}: {e}")
    return None


def cache_player(pid, pdata):
    if not pid or not pdata:
        return
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedPlayer", str(pid))
        entity = datastore.Entity(key=key)
        entity.update({
            "player_id": int(pid),
            "name": pdata.get("name", ""),
            "surname": pdata.get("surname", ""),
            "patronymic": pdata.get("patronymic", ""),
            "town": pdata.get("town", ""),
            "rating": pdata.get("rating", 0),
            "tourn_count": pdata.get("tourn_count", 0),
            "cached_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
    except Exception as e:
        print(f"Error writing CachedPlayer pid={pid}: {e}")


def search_cached_players(query, limit=10):
    if not query:
        return []
    try:
        q_clean = query.strip().lower()
        datastore_client = get_datastore_client()
        if q_clean.isdigit():
            p = get_cached_player(int(q_clean))
            return [p] if p else []

        q = datastore_client.query(kind="CachedPlayer")
        matches = []
        for entity in q.fetch(limit=100):
            p_fio = f"{entity.get('surname', '')} {entity.get('name', '')} {entity.get('patronymic', '')}".lower()
            if q_clean in p_fio:
                matches.append({
                    "id": int(entity.get("player_id", entity.key.name or 0)),
                    "name": entity.get("name", ""),
                    "surname": entity.get("surname", ""),
                    "patronymic": entity.get("patronymic", ""),
                    "town": entity.get("town", ""),
                    "source": "cache",
                    "badge": "⚡ Из кэша",
                })
                if len(matches) >= limit:
                    break
        return matches
    except Exception as e:
        print(f"Error searching cached players: {e}")
    return []


def get_cached_team_base_roster(team_id):
    if not team_id:
        return None
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedTeamBase", str(team_id))
        entity = datastore_client.get(key)
        if entity:
            base_pids = set(entity.get("base_pids", []))
            captain_id = entity.get("captain_id")
            return base_pids, captain_id
    except Exception as e:
        print(f"Error reading CachedTeamBase team_id={team_id}: {e}")
    return None


def cache_team_base_roster(team_id, base_pids, captain_id):
    if not team_id:
        return
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedTeamBase", str(team_id))
        entity = datastore.Entity(key=key, exclude_from_indexes=("base_pids",))
        entity.update({
            "team_id": int(team_id),
            "base_pids": list(base_pids) if base_pids else [],
            "captain_id": int(captain_id) if captain_id else None,
            "cached_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
    except Exception as e:
        print(f"Error writing CachedTeamBase team_id={team_id}: {e}")


def get_cached_team_players(team_id):
    if not team_id:
        return None
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedTeamPlayers", str(team_id))
        entity = datastore_client.get(key)
        if entity and "players" in entity:
            return json.loads(entity["players"])
    except Exception as e:
        print(f"Error reading CachedTeamPlayers team_id={team_id}: {e}")
    return None


def cache_team_players(team_id, players):
    if not team_id or players is None:
        return
    try:
        clean_players = []
        for p in players:
            if not isinstance(p, dict):
                continue
            clean_players.append({
                "id": int(p.get("id") or p.get("player_id") or 0),
                "name": str(p.get("name") or ""),
                "surname": str(p.get("surname") or ""),
                "patronymic": str(p.get("patronymic") or ""),
                "town": str(p.get("town") or ""),
                "season_recency": int(p.get("season_recency") or 0),
                "tourn_count": int(p.get("tourn_count") or 0),
                "tourn_recency": int(p.get("tourn_recency") or 0),
            })
        datastore_client = get_datastore_client()
        key = datastore_client.key("CachedTeamPlayers", str(team_id))
        entity = datastore.Entity(key=key, exclude_from_indexes=("players",))
        entity.update({
            "team_id": int(team_id),
            "players": json.dumps(clean_players, default=str, ensure_ascii=False),
            "cached_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
    except Exception as e:
        print(f"Error writing CachedTeamPlayers team_id={team_id}: {e}")


# --- Team Registration & Roster Functions ---

def add_team_registration(chat_id, thread_id, sync_req_id, message_id, tourn_name, representative_text="", narrator_text="", start_time="", start_time_ts=None, created_by_user_id=None):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    entity = datastore.Entity(key=key, exclude_from_indexes=("teams",))
    entity.update({
        "chat_id": chat_id,
        "thread_id": thread_id,
        "sync_req_id": str(sync_req_id),
        "message_id": message_id,
        "tourn_name": tourn_name,
        "representative_text": representative_text,
        "narrator_text": narrator_text,
        "start_time": start_time,
        "start_time_ts": start_time_ts,
        "created_by_user_id": created_by_user_id,
        "teams": [],
        "status": "active",
        "created_at": datetime.datetime.now(pytz.utc),
    })
    datastore_client.put(entity)
    return entity


def get_team_registration(chat_id, sync_req_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    return datastore_client.get(key)


def get_team_registration_by_msg(chat_id, message_id):
    if not chat_id or not message_id:
        return None
    try:
        datastore_client = get_datastore_client()
        query = datastore_client.query(kind="TeamRegistration")
        query.add_filter(filter=PropertyFilter("status", "=", "active"))

        target_cid = str(chat_id)
        target_mid = str(message_id)

        for entity in query.fetch(limit=100):
            e_cid = str(entity.get("chat_id", ""))
            if e_cid != target_cid:
                continue

            e_mid = str(entity.get("message_id", ""))
            if e_mid == target_mid:
                return entity

    except Exception as e:
        print(f"Error in get_team_registration_by_msg: {e}")
    return None


def register_team_in_ds(chat_id, sync_req_id, team_name, user_id, username):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None
        teams = list(entity.get("teams", []))
        
        # Check if user already registered a team
        updated = False
        for team in teams:
            if team.get("user_id") == user_id:
                team["team_name"] = team_name
                team["display_name"] = team_name
                team["username"] = username
                team["registered_at"] = datetime.datetime.now(pytz.utc).isoformat()
                updated = True
                break
                
        if not updated:
            teams.append({
                "team_name": team_name,
                "user_id": user_id,
                "username": username,
                "registered_at": datetime.datetime.now(pytz.utc).isoformat(),
                "rating_team_id": None,
                "display_name": team_name,
                "roster": [],
            })
            
        entity["teams"] = teams
        datastore_client.put(entity)
        return entity


def unregister_team_in_ds(chat_id, sync_req_id, user_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None
        teams = list(entity.get("teams", []))
        new_teams = [t for t in teams if t.get("user_id") != user_id]
        entity["teams"] = new_teams
        datastore_client.put(entity)
        return entity


def get_user_active_registrations(user_id):
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="TeamRegistration")
    query.add_filter(filter=PropertyFilter("status", "=", "active"))
    active_regs = []
    for entity in query.fetch():
        teams = entity.get("teams", [])
        for team in teams:
            if team.get("user_id") == user_id:
                active_regs.append({
                    "registration_key": entity.key.name,
                    "chat_id": entity.get("chat_id"),
                    "sync_req_id": entity.get("sync_req_id"),
                    "tourn_name": entity.get("tourn_name"),
                    "start_time_ts": entity.get("start_time_ts"),
                    "created_at": entity.get("created_at"),
                    "team": team,
                })
                break
    return _sort_registrations_newest_first(active_regs)


def update_team_roster_in_ds(chat_id, sync_req_id, user_id, rating_team_id, team_name, display_name, roster, town=None):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None
        teams = list(entity.get("teams", []))
        for team in teams:
            if team.get("user_id") == user_id:
                if rating_team_id is not None:
                    team["rating_team_id"] = rating_team_id
                if team_name:
                    team["team_name"] = team_name
                if display_name:
                    team["display_name"] = display_name
                if town:
                    team["town"] = town
                team["roster"] = roster
                team["roster_submitted"] = True if roster else False
                team["submitted_at"] = datetime.datetime.now(pytz.utc).isoformat()
                break
        entity["teams"] = teams
        datastore_client.put(entity)
        return entity


def reset_unsubmitted_reminders(chat_id, sync_req_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None, []
        teams = list(entity.get("teams", []))
        notified_users = []
        for team in teams:
            team["roster_submitted"] = False
            team["reminders_count"] = 0
            team["last_reminder_ts"] = 0
            if team.get("user_id"):
                notified_users.append({
                    "user_id": team["user_id"],
                    "team_name": team.get("display_name") or team.get("team_name", "Команда")
                })
        entity["teams"] = teams
        datastore_client.put(entity)
        return entity, notified_users


def reject_team_roster_in_ds(chat_id, sync_req_id, target_user_id=None, team_index=None):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None, None
        teams = list(entity.get("teams", []))
        rejected_team_name = None
        for idx, team in enumerate(teams):
            match = False
            if team_index is not None and idx == team_index:
                match = True
            elif target_user_id and team.get("user_id") == target_user_id:
                match = True
            if match:
                team["roster_submitted"] = False
                team["submitted_externally"] = False
                team["reminders_count"] = 0
                team["last_reminder_ts"] = 0
                rejected_team_name = team.get("display_name") or team.get("team_name", "Команда")
                break
        entity["teams"] = teams
        datastore_client.put(entity)
        return entity, rejected_team_name


def mark_team_roster_submitted_in_ds(chat_id, sync_req_id, target_user_id=None, team_index=None, submitted=True, external=True):
    datastore_client = get_datastore_client()
    key = datastore_client.key("TeamRegistration", f"{chat_id}_{sync_req_id}")
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            return None, None
        teams = list(entity.get("teams", []))
        updated_team_name = None
        for idx, team in enumerate(teams):
            match = False
            if team_index is not None and idx == team_index:
                match = True
            elif target_user_id and team.get("user_id") == target_user_id:
                match = True
            if match:
                team["roster_submitted"] = bool(submitted)
                team["submitted_externally"] = bool(submitted and external)
                updated_team_name = team.get("display_name") or team.get("team_name", "Команда")
                break
        entity["teams"] = teams
        datastore_client.put(entity)
        return entity, updated_team_name


def _sort_registrations_newest_first(entities):
    def sort_key(entity):
        if not entity:
            return 0.0
        st_ts = entity.get("start_time_ts")
        if st_ts:
            try:
                return float(st_ts)
            except (ValueError, TypeError):
                pass
        cat = entity.get("created_at")
        if cat:
            if isinstance(cat, datetime.datetime):
                return cat.timestamp()
            try:
                return float(cat)
            except (ValueError, TypeError):
                pass
        s_id = entity.get("sync_req_id")
        if s_id and str(s_id).isdigit():
            return float(s_id)
        return 0.0

    return sorted(entities, key=sort_key, reverse=True)


def get_all_active_registrations(chat_id=None, limit=50):
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="TeamRegistration")
    query.add_filter(filter=PropertyFilter("status", "=", "active"))
    if chat_id:
        query.add_filter(filter=PropertyFilter("chat_id", "=", int(chat_id)))
    results = list(query.fetch(limit=limit))
    return _sort_registrations_newest_first(results)


def update_team_registration(entity):
    datastore_client = get_datastore_client()
    datastore_client.put(entity)


def cleanup_old_registrations(days=14):
    """
    Deletes registrations where tournament started more than `days` ago or is archived.
    """
    try:
        datastore_client = get_datastore_client()
        query = datastore_client.query(kind="TeamRegistration")
        now_ts = int(datetime.datetime.now(pytz.utc).timestamp())
        max_age_sec = int(days * 86400)
        keys_to_delete = []

        for reg in query.fetch():
            start_ts = reg.get("start_time_ts")
            created_at = reg.get("created_at")
            created_ts = int(created_at.timestamp()) if created_at else 0

            should_delete = False
            if start_ts and (now_ts - start_ts > max_age_sec):
                should_delete = True
            elif not start_ts and created_ts and (now_ts - created_ts > max_age_sec):
                should_delete = True

            if should_delete:
                keys_to_delete.append(reg.key)

        if keys_to_delete:
            for i in range(0, len(keys_to_delete), 400):
                datastore_client.delete_multi(keys_to_delete[i:i+400])
            print(f"[DATASTORE] Cleaned up/deleted {len(keys_to_delete)} old TeamRegistration entities")

        return len(keys_to_delete)
    except Exception as e:
        print(f"Error cleaning up old registrations: {e}")
        return 0


def archive_old_registrations(days=14):
    return cleanup_old_registrations(days=days)




# --- User History Functions ---

def get_user_history(user_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserHistory", str(user_id))
    entity = datastore_client.get(key)
    if not entity:
        return {"teams": [], "players": []}
    return {
        "teams": list(entity.get("teams", [])),
        "players": list(entity.get("players", [])),
    }


def add_user_history_team(user_id, team_id, team_name, town="", display_name=""):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserHistory", str(user_id))
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            entity = datastore.Entity(key=key, exclude_from_indexes=("teams", "players"))
            teams = []
            players = []
        else:
            teams = list(entity.get("teams", []))
            players = list(entity.get("players", []))

        # Deduplicate team
        existing = False
        for t in teams:
            if (team_id and t.get("team_id") == team_id) or (t.get("name", "").lower() == team_name.lower()):
                t["team_id"] = team_id or t.get("team_id")
                t["name"] = team_name
                if town:
                    t["town"] = town
                if display_name:
                    dnames = list(t.get("display_names", []))
                    if display_name not in dnames:
                        dnames.insert(0, display_name)
                    t["display_names"] = dnames[:10]
                existing = True
                break
        if not existing:
            item = {"team_id": team_id, "name": team_name}
            if town:
                item["town"] = town
            if display_name:
                item["display_names"] = [display_name]
            teams.append(item)

        entity["teams"] = teams
        entity["players"] = players
        datastore_client.put(entity)


def add_user_history_player(user_id, player_id, name, surname, patronymic=""):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserHistory", str(user_id))
    with datastore_client.transaction():
        entity = datastore_client.get(key)
        if not entity:
            entity = datastore.Entity(key=key, exclude_from_indexes=("teams", "players"))
            teams = []
            players = []
        else:
            teams = list(entity.get("teams", []))
            players = list(entity.get("players", []))

        now_str = datetime.datetime.now(pytz.utc).isoformat()
        existing_p = None
        for i, p in enumerate(players):
            if (player_id and p.get("player_id") == player_id) or (p.get("surname", "").lower() == surname.lower() and p.get("name", "").lower() == name.lower()):
                existing_p = players.pop(i)
                break

        if existing_p:
            existing_p["player_id"] = player_id or existing_p.get("player_id")
            existing_p["name"] = name or existing_p.get("name", "")
            existing_p["surname"] = surname or existing_p.get("surname", "")
            existing_p["patronymic"] = patronymic or existing_p.get("patronymic", "")
            existing_p["count"] = existing_p.get("count", 1) + 1
            existing_p["last_used"] = now_str
            players.insert(0, existing_p)
        else:
            players.insert(0, {
                "player_id": player_id,
                "name": name,
                "surname": surname,
                "patronymic": patronymic,
                "count": 1,
                "last_used": now_str,
            })

        entity["teams"] = teams
        entity["players"] = players
        datastore_client.put(entity)


# --- User State Functions for PM Dialogs ---

def get_user_state(user_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserState", str(user_id))
    entity = datastore_client.get(key)
    if entity:
        return entity.get("state"), entity.get("context_data", {})
    return None, {}


def set_user_state(user_id, state_name, context_data=None):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserState", str(user_id))
    entity = datastore.Entity(key=key, exclude_from_indexes=("context_data",))
    entity.update({
        "state": state_name,
        "context_data": context_data or {},
        "updated_at": datetime.datetime.now(pytz.utc),
    })
    datastore_client.put(entity)


def clear_user_state(user_id):
    datastore_client = get_datastore_client()
    key = datastore_client.key("UserState", str(user_id))
    datastore_client.delete(key)


# --- UserMapping Functions (Telegram User ID <-> Rating Player ID) ---

def get_user_mapping(user_id):
    if not user_id:
        return None
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("UserMapping", str(user_id))
        entity = datastore_client.get(key)
        if entity:
            return {
                "telegram_user_id": int(entity.get("telegram_user_id", user_id)),
                "rating_player_id": entity.get("rating_player_id"),
                "name": entity.get("name", ""),
                "surname": entity.get("surname", ""),
                "patronymic": entity.get("patronymic", ""),
                "town": entity.get("town", ""),
                "username": entity.get("username", ""),
            }
    except Exception as e:
        print(f"Error reading UserMapping for user_id={user_id}: {e}")
    return None


def set_user_mapping(user_id, rating_player_id, name, surname, patronymic="", town="", username=""):
    if not user_id:
        return None
    try:
        datastore_client = get_datastore_client()
        key = datastore_client.key("UserMapping", str(user_id))
        entity = datastore.Entity(key=key)
        entity.update({
            "telegram_user_id": int(user_id),
            "rating_player_id": int(rating_player_id) if rating_player_id else None,
            "name": name,
            "surname": surname,
            "patronymic": patronymic,
            "town": town,
            "username": username,
            "updated_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
        return entity
    except Exception as e:
        print(f"Error saving UserMapping for user_id={user_id}: {e}")
    return None


def find_user_by_rating_id(rating_player_id):
    if not rating_player_id:
        return None
    try:
        datastore_client = get_datastore_client()
        query = datastore_client.query(kind="UserMapping")
        query.add_filter(filter=PropertyFilter("rating_player_id", "=", int(rating_player_id)))
        results = list(query.fetch(limit=1))
        if results:
            e = results[0]
            return {
                "telegram_user_id": int(e.get("telegram_user_id")),
                "rating_player_id": e.get("rating_player_id"),
                "name": e.get("name", ""),
                "surname": e.get("surname", ""),
                "patronymic": e.get("patronymic", ""),
                "town": e.get("town", ""),
            }
    except Exception as e:
        print(f"Error finding user by rating_id={rating_player_id}: {e}")
    return None


def is_user_representative(reg_entity, user_id):
    if not user_id or not reg_entity:
        return False
    if reg_entity.get("created_by_user_id") == user_id:
        return True

    mapping = get_user_mapping(user_id)
    rep_text = reg_entity.get("representative_text", "").lower()
    if mapping:
        r_id = mapping.get("rating_player_id")
        if r_id and str(r_id) in rep_text:
            return True
        full_name = f"{mapping.get('name', '')} {mapping.get('surname', '')}".strip().lower()
        rev_name = f"{mapping.get('surname', '')} {mapping.get('name', '')}".strip().lower()
        if (full_name and full_name in rep_text) or (rev_name and rev_name in rep_text):
            return True

    return False


def get_user_representative_registrations(user_id):
    datastore_client = get_datastore_client()
    query = datastore_client.query(kind="TeamRegistration")
    query.add_filter(filter=PropertyFilter("status", "=", "active"))
    rep_regs = []
    seen_keys = set()
    for entity in query.fetch():
        key_str = entity.key.name or str(entity.key.id)
        if key_str in seen_keys:
            continue
        if is_user_representative(entity, user_id) or any(t.get("user_id") == user_id for t in entity.get("teams", [])):
            rep_regs.append(entity)
            seen_keys.add(key_str)
    return _sort_registrations_newest_first(rep_regs)


# --- Test Role Simulation Persistence ---

def get_user_test_role(user_id):
    try:
        if not user_id:
            return None
        datastore_client = get_datastore_client()
        key = datastore_client.key("UserTestRole", str(user_id))
        entity = datastore_client.get(key)
        if entity and "role" in entity:
            return entity["role"]
    except Exception as e:
        print(f"Error fetching test role for user {user_id}: {e}")
    return None


def set_user_test_role(user_id, role):
    try:
        if not user_id:
            return
        datastore_client = get_datastore_client()
        key = datastore_client.key("UserTestRole", str(user_id))
        if not role or role in ("auto", "default", "none", "reset"):
            datastore_client.delete(key)
            print(f"[TEST_ROLE] Reset test role for user {user_id}")
            return
        entity = datastore.Entity(key=key)
        entity.update({
            "user_id": int(user_id),
            "role": str(role).lower().strip(),
            "updated_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
        print(f"[TEST_ROLE] Set test role '{role}' for user {user_id}")
    except Exception as e:
        print(f"Error setting test role for user {user_id}: {e}")


# --- Venue Data & Sync State Persistence ---

def get_venue_sync_state(venue_id):
    try:
        if not venue_id:
            return None
        datastore_client = get_datastore_client()
        key = datastore_client.key("VenueSyncState", str(venue_id))
        entity = datastore_client.get(key)
        if entity:
            return {
                "venue_id": int(venue_id),
                "last_synced_at": entity.get("last_synced_at"),
                "synced_req_ids": list(entity.get("synced_req_ids", [])),
                "updated_at": entity.get("updated_at"),
            }
    except Exception as e:
        print(f"Error getting venue_sync_state for venue {venue_id}: {e}")
    return None


def save_venue_sync_state(venue_id, last_synced_at, synced_req_ids):
    try:
        if not venue_id:
            return
        datastore_client = get_datastore_client()
        key = datastore_client.key("VenueSyncState", str(venue_id))
        entity = datastore.Entity(key=key, exclude_from_indexes=("synced_req_ids",))
        # Keep up to 2000 recent sync_req_ids to avoid unbounded entity size
        clean_req_ids = [str(x) for x in list(synced_req_ids)][-2000:]
        entity.update({
            "venue_id": int(venue_id),
            "last_synced_at": last_synced_at,
            "synced_req_ids": clean_req_ids,
            "updated_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
    except Exception as e:
        print(f"Error saving venue_sync_state for venue {venue_id}: {e}")


def get_venue_data(venue_id):
    try:
        if not venue_id:
            return {"teams": []}
        datastore_client = get_datastore_client()
        key = datastore_client.key("VenueData", str(venue_id))
        entity = datastore_client.get(key)
        if entity:
            return {
                "venue_id": int(venue_id),
                "teams": list(entity.get("teams", [])),
                "updated_at": entity.get("updated_at"),
            }
    except Exception as e:
        print(f"Error getting venue_data for venue {venue_id}: {e}")
    return {"teams": []}


def get_venue_team_roster(venue_id, team_id):
    """
    Fetches the roster of players who played for team_id at venue_id.
    """
    try:
        if not venue_id or not team_id:
            return []
        datastore_client = get_datastore_client()
        key = datastore_client.key("VenueTeamRoster", f"{venue_id}_{team_id}")
        entity = datastore_client.get(key)
        if entity:
            return list(entity.get("players", []))
    except Exception as e:
        print(f"Error getting venue_team_roster for {venue_id}_{team_id}: {e}")
    return []


def get_venues_team_rosters(venue_ids, team_id):
    """
    Fetches and merges rosters of players who played for team_id across multiple venues.
    """
    if not venue_ids or not team_id:
        return []
    try:
        datastore_client = get_datastore_client()
        keys = [datastore_client.key("VenueTeamRoster", f"{vid}_{team_id}") for vid in venue_ids]
        entities = datastore_client.get_multi(keys)
        merged = {}
        for entity in entities:
            if not entity:
                continue
            for p in entity.get("players", []):
                pid = p.get("player_id") or p.get("id")
                if not pid:
                    continue
                pkey = str(pid)
                if pkey not in merged:
                    merged[pkey] = dict(p)
                else:
                    merged[pkey]["tourn_count"] = merged[pkey].get("tourn_count", 0) + p.get("tourn_count", 0)
                    merged[pkey]["last_played_ts"] = max(merged[pkey].get("last_played_ts", 0), p.get("last_played_ts", 0))

        return sorted(list(merged.values()), key=lambda x: (x.get("tourn_count", 0), x.get("last_played_ts", 0)), reverse=True)
    except Exception as e:
        print(f"Error getting venues_team_rosters for team {team_id}: {e}")
    return []


def get_venues_data(venue_ids):
    """
    Fetches and aggregates venue teams across multiple venue IDs.
    Returns: {"teams": [...]}
    """
    if not venue_ids:
        return {"teams": []}

    aggregated_teams = {}

    for vid in venue_ids:
        v_data = get_venue_data(vid)
        for t in v_data.get("teams", []):
            tid = t.get("team_id") or t.get("id")
            if not tid:
                continue
            key = str(tid)
            if key not in aggregated_teams:
                aggregated_teams[key] = {
                    "team_id": tid,
                    "name": t.get("name") or t.get("team_name", "Команда"),
                    "town": t.get("town", ""),
                    "tourn_count": t.get("tourn_count", 0),
                    "last_played_ts": t.get("last_played_ts", 0),
                    "display_names": list(t.get("display_names", [])),
                }
            else:
                aggregated_teams[key]["tourn_count"] += t.get("tourn_count", 0)
                aggregated_teams[key]["last_played_ts"] = max(
                    aggregated_teams[key]["last_played_ts"], t.get("last_played_ts", 0)
                )
                curr_dns = aggregated_teams[key].setdefault("display_names", [])
                for dn in t.get("display_names", []):
                    if dn and dn not in curr_dns:
                        curr_dns.append(dn)

    final_teams = sorted(
        list(aggregated_teams.values()),
        key=lambda x: (x.get("last_played_ts", 0), x.get("tourn_count", 0)),
        reverse=True,
    )
    return {"teams": final_teams}


def update_venue_data_incremental(venue_id, new_teams, new_team_rosters, new_team_display_names=None):
    """
    Merges newly fetched teams, rosters, and display names into VenueData and VenueTeamRoster entities.
    """
    try:
        if not venue_id:
            return
        datastore_client = get_datastore_client()
        key = datastore_client.key("VenueData", str(venue_id))

        with datastore_client.transaction():
            entity = datastore_client.get(key)
            if not entity:
                entity = datastore.Entity(key=key, exclude_from_indexes=("teams",))

            existing_teams = {
                str(t.get("team_id") or t.get("id")): dict(t)
                for t in entity.get("teams", [])
                if (t.get("team_id") or t.get("id"))
            }

            for nt in new_teams:
                tid = nt.get("team_id") or nt.get("id")
                if not tid:
                    continue
                key_t = str(tid)
                if key_t in existing_teams:
                    existing_teams[key_t]["tourn_count"] = (
                        existing_teams[key_t].get("tourn_count", 0) + nt.get("tourn_count", 1)
                    )
                    existing_teams[key_t]["last_played_ts"] = max(
                        existing_teams[key_t].get("last_played_ts", 0), nt.get("last_played_ts", 0)
                    )
                    if nt.get("town") and not existing_teams[key_t].get("town"):
                        existing_teams[key_t]["town"] = nt.get("town")
                else:
                    existing_teams[key_t] = {
                        "team_id": tid,
                        "name": nt.get("name") or nt.get("team_name", "Команда"),
                        "town": nt.get("town", ""),
                        "tourn_count": nt.get("tourn_count", 1),
                        "last_played_ts": nt.get("last_played_ts", 0),
                        "display_names": nt.get("display_names", []),
                    }

            if new_team_display_names:
                for tid_str, d_list in new_team_display_names.items():
                    if tid_str in existing_teams:
                        curr_dnames = list(existing_teams[tid_str].get("display_names", []))
                        names_set = set(curr_dnames)
                        for d_item in d_list:
                            dname = d_item.get("name") if isinstance(d_item, dict) else str(d_item)
                            if dname and dname not in names_set:
                                curr_dnames.append(dname)
                                names_set.add(dname)
                        existing_teams[tid_str]["display_names"] = curr_dnames[:5]

            sorted_teams = sorted(
                list(existing_teams.values()),
                key=lambda x: (x.get("last_played_ts", 0), x.get("tourn_count", 0)),
                reverse=True,
            )

            entity.exclude_from_indexes.add("teams")
            entity.update({
                "venue_id": int(venue_id),
                "teams": sorted_teams[:500],
                "updated_at": datetime.datetime.now(pytz.utc),
            })
            datastore_client.put(entity)

        # Batch save VenueTeamRoster entities in chunks
        all_tids = set(list(new_team_rosters.keys()) + list((new_team_display_names or {}).keys()))
        if not all_tids:
            return

        existing_roster_keys = [datastore_client.key("VenueTeamRoster", f"{venue_id}_{tid_str}") for tid_str in all_tids if tid_str]
        existing_entities_map = {}
        for i in range(0, len(existing_roster_keys), 400):
            batch_e = datastore_client.get_multi(existing_roster_keys[i:i+400])
            for e in batch_e:
                if e:
                    existing_entities_map[e.key.name] = e

        roster_entities = []
        now_utc = datetime.datetime.now(pytz.utc)

        for tid_str in all_tids:
            if not tid_str:
                continue
            r_key_name = f"{venue_id}_{tid_str}"
            r_key = datastore_client.key("VenueTeamRoster", r_key_name)
            prev_entity = existing_entities_map.get(r_key_name)
            r_entity = datastore.Entity(key=r_key, exclude_from_indexes=("players", "display_names"))

            # Aggregate players
            players = new_team_rosters.get(tid_str, [])
            p_dict = {}
            if prev_entity:
                for p in prev_entity.get("players", []):
                    pid = p.get("player_id") or p.get("id")
                    if pid:
                        p_dict[str(pid)] = dict(p)

            for p in players:
                pid = p.get("player_id") or p.get("id")
                if not pid:
                    continue
                pkey = str(pid)
                if pkey not in p_dict:
                    p_dict[pkey] = {
                        "player_id": int(pid),
                        "name": p.get("name", ""),
                        "surname": p.get("surname", ""),
                        "patronymic": p.get("patronymic", ""),
                        "tourn_count": p.get("tourn_count", 1),
                        "last_played_ts": p.get("last_played_ts", 0),
                    }
                else:
                    p_dict[pkey]["tourn_count"] += p.get("tourn_count", 1)
                    p_dict[pkey]["last_played_ts"] = max(p_dict[pkey]["last_played_ts"], p.get("last_played_ts", 0))

            sorted_players = sorted(list(p_dict.values()), key=lambda x: (x.get("tourn_count", 0), x.get("last_played_ts", 0)), reverse=True)

            # Aggregate display names
            d_dict = {}
            if prev_entity:
                for d in prev_entity.get("display_names", []):
                    dname = d.get("name") if isinstance(d, dict) else str(d)
                    if dname:
                        d_dict[dname.strip().lower()] = {
                            "name": dname.strip(),
                            "tourn_count": d.get("tourn_count", 1) if isinstance(d, dict) else 1,
                            "last_played_ts": d.get("last_played_ts", 0) if isinstance(d, dict) else 0,
                        }

            raw_dnames = (new_team_display_names or {}).get(tid_str, [])
            for d in raw_dnames:
                dname = d.get("name") if isinstance(d, dict) else str(d)
                if not dname:
                    continue
                dkey = dname.strip().lower()
                d_ts = d.get("last_played_ts", 0) if isinstance(d, dict) else 0
                if dkey not in d_dict:
                    d_dict[dkey] = {
                        "name": dname.strip(),
                        "tourn_count": 1,
                        "last_played_ts": d_ts,
                    }
                else:
                    d_dict[dkey]["tourn_count"] += 1
                    d_dict[dkey]["last_played_ts"] = max(d_dict[dkey]["last_played_ts"], d_ts)

            sorted_dnames = sorted(list(d_dict.values()), key=lambda x: (x.get("tourn_count", 0), x.get("last_played_ts", 0)), reverse=True)

            r_entity.update({
                "venue_id": int(venue_id),
                "team_id": int(tid_str),
                "players": sorted_players[:50],
                "display_names": sorted_dnames[:20],
                "updated_at": now_utc,
            })
            roster_entities.append(r_entity)

        for i in range(0, len(roster_entities), 400):
            datastore_client.put_multi(roster_entities[i:i+400])

        print(f"[VENUE_DATA] Saved venue {venue_id}: {len(sorted_teams)} teams, {len(roster_entities)} team rosters & display names")
    except Exception as e:
        print(f"Error updating venue_data for venue {venue_id}: {e}")


def get_team_suggested_display_names(team_id=None, base_team_name="", registered_name="", venue_ids=None, user_id=None):
    """
    Returns prioritized and deduplicated list of suggested display names:
    1. registered_name (from current registration)
    2. base_team_name (official rating name)
    3. User's previous display names for this team
    4. Venue's previous display names for this team
    """
    suggestions = []
    seen = set()

    def add_sugg(name, badge, count=0):
        if not name or not str(name).strip():
            return
        n_clean = str(name).strip()
        n_lower = n_clean.lower()
        if n_lower in seen:
            return
        seen.add(n_lower)
        suggestions.append({
            "name": n_clean,
            "badge": badge,
            "count": count
        })

    # 1. Registered name
    if registered_name and registered_name != "Команда":
        add_sugg(registered_name, "⭐ Название из заявки")

    # 2. Base team official name
    if base_team_name and base_team_name != "Команда":
        add_sugg(base_team_name, "🛡 Официальное базовое")

    # 3. User history
    if user_id:
        try:
            u_hist = get_user_history(user_id)
            for t in u_hist.get("teams", []):
                if (team_id and t.get("team_id") == team_id) or (base_team_name and t.get("name", "").lower() == base_team_name.lower()):
                    for dname in t.get("display_names", []):
                        add_sugg(dname, "⭐ Ваша история")
        except Exception as e:
            print(f"Error checking user history display names: {e}")

    # 4. Venue history
    if venue_ids and team_id:
        try:
            datastore_client = get_datastore_client()
            keys = [datastore_client.key("VenueTeamRoster", f"{vid}_{team_id}") for vid in venue_ids]
            entities = datastore_client.get_multi(keys)
            venue_dnames = {}
            for entity in entities:
                if not entity:
                    continue
                for d in entity.get("display_names", []):
                    dname = d.get("name") if isinstance(d, dict) else str(d)
                    if not dname:
                        continue
                    k = dname.strip().lower()
                    cnt = d.get("tourn_count", 1) if isinstance(d, dict) else 1
                    if k not in venue_dnames:
                        venue_dnames[k] = {"name": dname.strip(), "count": cnt}
                    else:
                        venue_dnames[k]["count"] += cnt

            sorted_vd = sorted(list(venue_dnames.values()), key=lambda x: x.get("count", 0), reverse=True)
            for vd in sorted_vd:
                add_sugg(vd["name"], f"📍 Площадка ({vd['count']} игр)", vd["count"])
        except Exception as e:
            print(f"Error checking venue display names: {e}")

    return suggestions


# --- Chat Command History / Logging ---

def save_chat_command(
    chat_id,
    user_id,
    text,
    chat_type="group",
    chat_title="",
    username="",
    first_name="",
    last_name="",
    message_id=None,
    thread_id=None,
    llm_date=None,
):
    try:
        if not text or not text.strip().startswith("/"):
            return None

        datastore_client = get_datastore_client()
        key = datastore_client.key("ChatCommand")
        entity = datastore.Entity(key=key, exclude_from_indexes=("text",))

        # Extract primary command e.g. "/poll" from "/poll@bot 1,2,3"
        raw_cmd = text.strip().split()[0]
        cmd_name = raw_cmd.split("@")[0].lower()

        user_disp = (
            f"{first_name or ''} {last_name or ''}".strip() or username or str(user_id)
        )

        entity.update({
            "chat_id": int(chat_id),
            "chat_type": str(chat_type or "group"),
            "chat_title": str(chat_title or ""),
            "user_id": int(user_id) if user_id else None,
            "username": str(username or ""),
            "user_display_name": user_disp,
            "command": cmd_name,
            "text": text,
            "message_id": int(message_id) if message_id else None,
            "thread_id": int(thread_id) if thread_id else None,
            "llm_date": str(llm_date) if llm_date else None,
            "created_at": datetime.datetime.now(pytz.utc),
        })
        datastore_client.put(entity)
        return entity
    except Exception as e:
        print(f"Error saving ChatCommand: {e}")
        return None


def get_chat_commands(chat_id=None, user_id=None, limit=100, from_date=None):
    try:
        datastore_client = get_datastore_client()
        query = datastore_client.query(kind="ChatCommand", order=["-created_at"])
        if chat_id is not None:
            query.add_filter(filter=PropertyFilter("chat_id", "=", int(chat_id)))
        if user_id is not None:
            query.add_filter(filter=PropertyFilter("user_id", "=", int(user_id)))
        if from_date:
            query.add_filter(filter=PropertyFilter("created_at", ">=", from_date))
        return list(query.fetch(limit=limit))
    except Exception as e:
        print(f"Error fetching ChatCommand list: {e}")
        return []


def cleanup_old_chat_commands(days=30):
    try:
        datastore_client = get_datastore_client()
        threshold = datetime.datetime.now(pytz.utc) - relativedelta(days=days)
        query = datastore_client.query(kind="ChatCommand")
        query.add_filter(filter=PropertyFilter("created_at", "<", threshold))
        
        entities = list(query.fetch(limit=500))
        if entities:
            keys = [e.key for e in entities]
            datastore_client.delete_multi(keys)
            print(f"[CLEANUP] Deleted {len(keys)} old ChatCommand records")
    except Exception as e:
        print(f"Error in cleanup_old_chat_commands: {e}")



