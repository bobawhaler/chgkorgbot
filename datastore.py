# -*- coding: utf-8 -*-

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
import datetime
import pytz
from dateutil.relativedelta import relativedelta
import rating_api


def get_datastore_client():
    import os
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("PROJECT_ID")
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

def get_played_tourns(venue_id, chat_id):
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
        print(f"[CACHE ERROR] cache_tournaments_batch failed: {e}")

