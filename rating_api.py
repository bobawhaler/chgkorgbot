import argparse
import datetime
import time
import concurrent.futures
from dateutil.relativedelta import relativedelta
from urllib.parse import quote
import requests
import helpers
import datastore
import pytz
import debug

API_URL = "https://api.rating.chgk.info"

# Max itemsPerPage per endpoint (from API docs / OpenAPI spec)
_TOURNAMENTS_ITEMS_PER_PAGE = 512
_VENUES_REQUESTS_ITEMS_PER_PAGE = 30


def _fetch_paginated(url_base, params, items_per_page):
    """Fetch all pages from a paginated API endpoint.

    Uses itemsPerPage to minimize requests. Stops when a page returns
    fewer items than requested (last page) or on HTTP error.
    """
    all_items = []
    page = 1

    while True:
        page_params = {**params, "page": page, "itemsPerPage": items_per_page}
        response = requests.get(
            url_base,
            params=page_params,
            headers={"Accept": "application/json"},
            allow_redirects=False,
        )
        if response.status_code not in (200, 201, 202, 204):
            return None

        items = response.json()
        if not items:
            break
        all_items.extend(items)
        # If we got fewer items than requested, this was the last page
        if len(items) < items_per_page:
            break
        page += 1

    return all_items


def get_tourn_by_id(tourn_id):
    try:
        import datastore
        tourn = datastore.get_cached_tournament(tourn_id)
        if tourn:
            print(f"[CACHE HIT] get_tourn_by_id tourn_id={tourn_id}")
            return tourn
    except Exception as e:
        print(f"[CACHE ERROR] failed to read cache in get_tourn_by_id for {tourn_id}: {e}")

    t0 = time.perf_counter()
    url = f"{API_URL}/tournaments/{tourn_id}"
    response = requests.get(url, headers={"Accept": "application/json"})
    if not response.ok:
        print(
            f"Error getting tournament by id {tourn_id}, {response.status_code}, {response.reason}"
        )
        return {}
    debug.log("rating_api.get_tourn_by_id", t0, f"tourn_id={tourn_id}")
    tourn = response.json()

    if tourn and tourn.get("name"):
        try:
            import datastore
            datastore.cache_tournament(tourn_id, tourn)
            print(f"[CACHE WRITE] cached tourn {tourn_id}: {tourn.get('name')}")
        except Exception as e:
            print(f"[CACHE ERROR] failed to write cache in get_tourn_by_id for {tourn_id}: {e}")

    return tourn


def get_tourn_by_request(request_id, chat_id):
    t0 = time.perf_counter()
    url = f"{API_URL}/tournament_synch_requests/{request_id}"
    response = requests.get(url, headers={"Accept": "application/json"})
    if not response.ok:
        print(
            f"Error getting sync request by id {request_id}, {response.status_code}, {response.reason}"
        )
        return None, None
    result = response.json()
    debug.log("rating_api.get_tourn_by_request", t0, f"request_id={request_id}")
    return result.get("tournamentId", None), helpers.parse_date(
        result.get("issuedAt", ""), helpers.get_chat_timezone(chat_id)
    )[0].strftime("%Y-%m-%d")


def get_sync_requests_ids(venue_id, months):
    t0 = time.perf_counter()
    from_date = (datetime.datetime.now() - relativedelta(months=months)).strftime(
        "%Y-%m-%d"
    )
    result = []
    if not venue_id:
        return result

    params = {"dateStart[after]": from_date}
    sync_requests = _fetch_paginated(
        f"{API_URL}/venues/{venue_id}/requests",
        params,
        _VENUES_REQUESTS_ITEMS_PER_PAGE,
    )

    if sync_requests is None:
        return result

    for sync_req in sync_requests:
        if sync_req["status"] == "A":
            result.append(str(sync_req["id"]))

    debug.log("rating_api.get_sync_requests_ids", t0, f"venue={venue_id}, months={months} -> {len(result)}")
    return result


def get_new_sync_requests(venue_id):
    t0 = time.perf_counter()
    now = datetime.datetime.now(pytz.utc)
    from_date = (now - relativedelta(days=1)).strftime("%Y-%m-%d %H:%M")
    to_date = (now + relativedelta(days=1)).strftime("%Y-%m-%d %H:%M")
    result = []
    if not venue_id:
        return result

    params = {"issuedAt[after]": from_date, "issuedAt[before]": to_date}
    sync_requests = _fetch_paginated(
        f"{API_URL}/venues/{venue_id}/requests",
        params,
        _VENUES_REQUESTS_ITEMS_PER_PAGE,
    )

    if sync_requests is None:
        return result

    for sync_req in sync_requests:

        narrator = ""
        if "narrator" in sync_req:
            narrator = sync_req["narrator"]
        elif "narrators" in sync_req:
            narrator = sync_req["narrators"][0]
        else:
            print("Error: no narrators in sync request")

        result.append(
            {
                "id": str(sync_req["id"]),
                "tourn_id": sync_req["tournamentId"],
                "status": sync_req["status"],
                "representative": sync_req["representative"],
                "narrator": narrator,
                "dateStart": datetime.datetime.strptime(
                    sync_req["dateStart"], "%Y-%m-%dT%H:%M:%S%z"
                ),
            }
        )

    debug.log("rating_api.get_new_sync_requests", t0, f"venue={venue_id} -> {len(result)}")
    return result


def get_tourns(tourn_date, played_tourns, chat_id, with_time=None, only_rated=False):
    t0 = time.perf_counter()
    from_date = (tourn_date - relativedelta(months=1)).strftime("%Y-%m-%d")
    try:
        _debug = debug.get_debug()
    except Exception:
        _debug = False
    if _debug:
        print(tourn_date, from_date)
    result = []
    played_tourns_ids = played_tourns.keys()
    played_syncs = {}
    for tourn_id in played_tourns_ids:
        played_syncs[played_tourns[tourn_id][0]] = {
            "editors": played_tourns[tourn_id][1],
            "date": played_tourns[tourn_id][2],
        }
    if _debug:
        print("played_syncs:", played_syncs)

    if with_time:
        to_date = tourn_date.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M")
        params = {
            "dateStart[before]": to_date,
            "dateStart[after]": from_date,
            "dateEnd[after]": to_date,
            "type[]": [3, 8],
        }
    else:
        to_date = tourn_date.strftime("%Y-%m-%d")
        params = {
            "dateStart[before]": f"{to_date} 23:59",
            "dateStart[after]": from_date,
            "dateEnd[after]": f"{to_date} 23:59",
            "type[]": [3, 8],
        }

    req = requests.Request("GET", f"{API_URL}/tournaments", params=params)
    prepared = req.prepare()
    if _debug:
        print(prepared.url)

    tournaments = _fetch_paginated(
        f"{API_URL}/tournaments",
        params,
        _TOURNAMENTS_ITEMS_PER_PAGE,
    )

    if tournaments is None:
        return result

    if _debug:
        print("tournaments count:", len(tournaments))

    for tourn in tournaments:
        if _debug:
            print("tourn:", tourn.get("id"), tourn.get("name"))
        if (
            "difficultyForecast" in tourn
            and tourn["difficultyForecast"]
            and (
                tourn["difficultyForecast"] < helpers.get_chat_min_difficulty(chat_id)
                or tourn["difficultyForecast"] > helpers.get_chat_max_difficulty(chat_id)
            )
            or only_rated
            and ("maiiRating" not in tourn or not tourn["maiiRating"])
        ):
            continue
        if (
            "type" not in tourn
            or "name" not in tourn["type"]
            or tourn["type"]["name"] == "Обычный"
        ):
            continue
        if "id" not in tourn or tourn["id"] in played_tourns_ids:
            continue
        tourn_editors = (
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
        )
        if tourn["type"]["name"] in ("Асинхрон", "Онлайн"):
            norm_name = helpers.normalize_tourn_name(tourn["name"])
            async_start_date, _ = helpers.parse_date(
                tourn["dateStart"], helpers.get_chat_timezone(chat_id)
            )
            sync_from_date = (async_start_date - relativedelta(months=1)).strftime(
                "%Y-%m-%d"
            )
            if _debug:
                print("async dedup:", norm_name, tourn_editors, sync_from_date)
            if (
                norm_name in played_syncs
                and tourn_editors == played_syncs[norm_name]["editors"]
                and sync_from_date < played_syncs[norm_name]["date"]
            ):
                continue
        tourn_questions = 0
        if "questionQty" in tourn:
            for n_tour in tourn["questionQty"]:
                tourn_questions += tourn["questionQty"][n_tour]
        difficulty = (
            tourn["difficultyForecast"]
            if "difficultyForecast" in tourn and tourn["difficultyForecast"]
            else 0
        )
        result.append(
            {
                "id": tourn["id"],
                "name": tourn["name"],
                "num_questions": tourn_questions,
                "rating": tourn["maiiRating"],
                "difficulty": difficulty,
                "editors": tourn_editors,
            }
        )
    debug.log("rating_api.get_tourns", t0, f"date={tourn_date}, only_rated={only_rated} -> {len(result)}")
    if tournaments:
        try:
            import datastore
            datastore.cache_tournaments_batch(tournaments)
        except Exception as e:
            print(f"[CACHE ERROR] failed to write cache batch in get_tourns: {e}")
    return result


def get_team_by_id(team_id):
    t0 = time.perf_counter()
    if not team_id:
        return {}
    try:
        resp = requests.get(f"{API_URL}/teams/{team_id}", headers={"Accept": "application/json"})
        if resp.ok:
            data = resp.json()
            if isinstance(data, dict) and "id" in data:
                town = data.get("town", {})
                town_name = town.get("name", "") if isinstance(town, dict) else ""
                return {"id": data["id"], "name": data.get("name", ""), "town": town_name}
    except Exception as e:
        print(f"Error fetching team by id={team_id}: {e}")
    return {}


def search_teams(query):
    t0 = time.perf_counter()
    query = str(query).strip()
    if not query:
        return []
    result = []
    try:
        if query.isdigit():
            resp = requests.get(f"{API_URL}/teams/{query}", headers={"Accept": "application/json"})
            if resp.ok:
                data = resp.json()
                if isinstance(data, dict) and "id" in data:
                    town = data.get("town", {})
                    town_name = town.get("name", "") if isinstance(town, dict) else ""
                    result.append({"id": data["id"], "name": data.get("name", ""), "town": town_name})
                    return result

        resp = requests.get(f"{API_URL}/teams", params={"name": query, "itemsPerPage": 15}, headers={"Accept": "application/json"})
        if resp.ok:
            data = resp.json()
            if isinstance(data, list):
                teams_raw = []
                for item in data:
                    town = item.get("town", {})
                    town_name = town.get("name", "") if isinstance(town, dict) else ""
                    teams_raw.append({"id": item.get("id"), "name": item.get("name", ""), "town": town_name})

                def fetch_team_metrics(team_item):
                    team_id = team_item["id"]
                    latest_season = 0
                    tourn_count = 0
                    try:
                        r_s = requests.get(f"{API_URL}/teams/{team_id}/seasons", params={"itemsPerPage": 100}, headers={"Accept": "application/json"}, timeout=3)
                        if r_s.ok:
                            seasons = r_s.json()
                            if isinstance(seasons, list) and seasons:
                                latest_season = max((s.get("idseason", 0) for s in seasons if isinstance(s, dict)), default=0)
                        r_t = requests.get(f"{API_URL}/teams/{team_id}/tournaments", params={"itemsPerPage": 1000}, headers={"Accept": "application/json"}, timeout=3)
                        if r_t.ok:
                            tourns = r_t.json()
                            if isinstance(tourns, list):
                                tourn_count = len(tourns)
                    except Exception:
                        pass
                    return team_id, latest_season, tourn_count

                if teams_raw:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        metrics_list = list(executor.map(fetch_team_metrics, teams_raw))
                    metrics_map = {m[0]: (m[1], m[2]) for m in metrics_list}
                    for t in teams_raw:
                        s_val, tc_val = metrics_map.get(t["id"], (0, 0))
                        t["latest_season"] = s_val
                        t["tourn_count"] = tc_val

                    teams_raw.sort(key=lambda x: (x.get("latest_season", 0), x.get("tourn_count", 0)), reverse=True)
                    result = teams_raw
    except Exception as e:
        print(f"Error searching teams query={query}: {e}")
    debug.log("rating_api.search_teams", t0, f"query={query} -> {len(result)}")
    return result


def get_player_town(pid):
    if not pid:
        return ""
    try:
        r = requests.get(f"{API_URL}/players/{pid}/seasons", params={"itemsPerPage": 10}, headers={"Accept": "application/json"})
        if r.ok and isinstance(r.json(), list) and r.json():
            seasons = r.json()
            latest_team_id = seasons[-1].get("idteam")
            if latest_team_id:
                t = get_team_by_id(latest_team_id)
                if t and t.get("town"):
                    return t["town"]
    except Exception:
        pass
    return ""


def get_player_by_id(pid):
    if not pid:
        return None

    try:
        import datastore
        ds_player = datastore.get_cached_player(pid)
        if ds_player:
            return ds_player
    except Exception:
        pass

    try:
        resp = requests.get(f"{API_URL}/players/{pid}", headers={"Accept": "application/json"})
        if resp.ok:
            p = resp.json()
            if isinstance(p, dict) and "id" in p:
                pdata = {
                    "id": p["id"],
                    "name": p.get("name", ""),
                    "surname": p.get("surname", ""),
                    "patronymic": p.get("patronymic", ""),
                    "town": get_player_town(p["id"]),
                }
                try:
                    import datastore
                    datastore.cache_player(pid, pdata)
                except Exception:
                    pass
                return pdata
    except Exception as e:
        print(f"Error fetching player id={pid}: {e}")
    return None


def get_team_players(team_id):
    t0 = time.perf_counter()
    if not team_id:
        return []

    cached = datastore.get_cached_team_players(team_id)
    if cached is not None:
        debug.log("rating_api.get_team_players [CACHED]", t0, f"team_id={team_id} -> {len(cached)}")
        return cached

    players_dict = {}
    try:
        # 1. Fetch recent season members (latest 2 seasons)
        resp = requests.get(f"{API_URL}/teams/{team_id}/seasons", params={"itemsPerPage": 100}, headers={"Accept": "application/json"})
        if resp.ok:
            data = resp.json()
            if isinstance(data, list) and data:
                max_season = max((item.get("idseason", 0) for item in data if isinstance(item, dict)), default=0)
                recent_items = [item for item in data if isinstance(item, dict) and item.get("idseason", 0) >= max_season - 1]
                recent_items.sort(key=lambda x: x.get("idseason", 0), reverse=True)

                player_ids = []
                season_map = {}
                for item in recent_items:
                    pid = item.get("idplayer")
                    if pid:
                        if pid not in player_ids:
                            player_ids.append(pid)
                        season_map[pid] = max(season_map.get(pid, 0), item.get("idseason", 0))

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    fetched_players = list(executor.map(get_player_by_id, player_ids[:30]))
                    for p in fetched_players:
                        if p and isinstance(p, dict) and "id" in p:
                            pid = p["id"]
                            players_dict[pid] = {
                                "id": pid,
                                "name": p.get("name", ""),
                                "surname": p.get("surname", ""),
                                "patronymic": p.get("patronymic", ""),
                                "town": p.get("town", ""),
                                "season_recency": 2 if season_map.get(pid) == max_season else 1,
                                "tourn_count": 0,
                                "tourn_recency": 0,
                            }

        # 2. Fetch players from recent tournaments played by team (last 30 tournaments / past year)
        resp_t = requests.get(f"{API_URL}/teams/{team_id}/tournaments", params={"itemsPerPage": 100}, headers={"Accept": "application/json"})
        if resp_t.ok:
            tourns = resp_t.json()
            if isinstance(tourns, list) and tourns:
                latest_tourns = tourns[-30:]

                def fetch_tourn_roster(t_item):
                    tid = t_item.get("idtournament")
                    if not tid:
                        return []
                    try:
                        r_res = requests.get(f"{API_URL}/tournaments/{tid}/results", params={"team": team_id, "includeTeamMembers": 1}, headers={"Accept": "application/json"}, timeout=5)
                        if r_res.ok:
                            res_list = r_res.json()
                            members = []
                            if isinstance(res_list, list):
                                for res in res_list:
                                    res_team_id = res.get("team", {}).get("id") if isinstance(res.get("team"), dict) else res.get("idteam")
                                    if str(res_team_id) == str(team_id):
                                        for member in res.get("teamMembers", []):
                                            if member.get("player") and member["player"].get("id"):
                                                members.append(member["player"])
                            return members
                    except Exception:
                        pass
                    return []

                with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
                    all_rosters = list(executor.map(fetch_tourn_roster, latest_tourns))

                for recency_idx, members in enumerate(all_rosters, 1):
                    for p in members:
                        pid = p["id"]
                        if pid not in players_dict:
                            players_dict[pid] = {
                                "id": pid,
                                "name": p.get("name", ""),
                                "surname": p.get("surname", ""),
                                "patronymic": p.get("patronymic", ""),
                                "season_recency": 0,
                                "tourn_count": 0,
                                "tourn_recency": 0,
                            }
                        players_dict[pid]["tourn_count"] = players_dict[pid].get("tourn_count", 0) + 1
                        players_dict[pid]["tourn_recency"] = max(players_dict[pid].get("tourn_recency", 0), recency_idx)
    except Exception as e:
        print(f"Error fetching team players for team_id={team_id}: {e}")
    result = list(players_dict.values())
    if result:
        datastore.cache_team_players(team_id, result)
    debug.log("rating_api.get_team_players", t0, f"team_id={team_id} -> {len(result)}")
    return result


def get_team_base_players(team_id):
    if not team_id:
        return set(), None

    cached = datastore.get_cached_team_base_roster(team_id)
    if cached is not None:
        return cached

    try:
        resp = requests.get(f"{API_URL}/teams/{team_id}/seasons", params={"itemsPerPage": 100}, headers={"Accept": "application/json"}, timeout=4)
        if resp.ok:
            data = resp.json()
            if isinstance(data, list) and data:
                max_season = max((item.get("idseason", 0) for item in data if isinstance(item, dict)), default=0)
                base_items = [item for item in data if isinstance(item, dict) and item.get("idseason") == max_season and not item.get("dateRemoved")]
                base_pids = {item["idplayer"] for item in base_items if item.get("idplayer")}
                captain_id = None
                for item in base_items:
                    if item.get("playerNumber") == 1:
                        captain_id = item.get("idplayer")
                        break
                datastore.cache_team_base_roster(team_id, base_pids, captain_id)
                return base_pids, captain_id
    except Exception as e:
        print(f"Error fetching base players for team_id={team_id}: {e}")
    return set(), None


def search_players(query):
    t0 = time.perf_counter()
    query = str(query).strip()
    if not query:
        return []
    
    words = query.split()
    results = {}

    # If numeric ID, direct lookup first
    if len(words) == 1 and words[0].isdigit():
        p_by_id = get_player_by_id(int(words[0]))
        if p_by_id:
            results[p_by_id["id"]] = p_by_id
            return [p_by_id]

    def do_search(params):
        try:
            resp = requests.get(f"{API_URL}/players", params={**params, "itemsPerPage": 30, "page": 1}, headers={"Accept": "application/json"}, timeout=4)
            if resp.ok and isinstance(resp.json(), list):
                for p in resp.json():
                    if isinstance(p, dict) and "id" in p and p["id"] not in results:
                        pdata = {
                            "id": p["id"],
                            "name": p.get("name", ""),
                            "surname": p.get("surname", ""),
                            "patronymic": p.get("patronymic", ""),
                            "town": "",
                            "rating": 0,
                            "tourn_count": 0,
                        }
                        results[p["id"]] = pdata
        except Exception as e:
            print(f"Error in search request {params}: {e}")

    try:
        if len(words) == 1:
            do_search({"surname": words[0].capitalize()})
            if words[0] != words[0].capitalize():
                do_search({"surname": words[0]})
            if len(results) < 5:
                do_search({"name": words[0].capitalize()})
        elif len(words) == 2:
            w0, w1 = words[0], words[1]
            do_search({"surname": w1.capitalize(), "name": w0.capitalize()})
            do_search({"surname": w0.capitalize(), "name": w1.capitalize()})
        elif len(words) >= 3:
            w0, w1, w2 = words[0], words[1], words[2]
            do_search({"surname": w0.capitalize(), "name": w1.capitalize(), "patronymic": w2.capitalize()})
            do_search({"surname": w2.capitalize(), "name": w0.capitalize(), "patronymic": w1.capitalize()})
            if not results:
                do_search({"surname": w1.capitalize(), "name": w0.capitalize()})
                do_search({"surname": w0.capitalize(), "name": w1.capitalize()})
    except Exception as e:
        print(f"Error searching players query={query}: {e}")

    result = list(results.values())

    # Fast town enrichment for top results
    top_pids = [p["id"] for p in result[:10] if not p.get("town")]
    if top_pids:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                towns = list(executor.map(get_player_town, top_pids))
                for pid, town in zip(top_pids, towns):
                    if town and pid in results:
                        results[pid]["town"] = town
        except Exception:
            pass

    for p in result:
        try:
            import datastore
            datastore.cache_player(p["id"], p)
        except Exception:
            pass

    debug.log("rating_api.search_players", t0, f"query={query} -> {len(result)}")
    return list(results.values())

def sync_venue_history(venue_id, months=12):
    """
    Incrementally synchronizes tournaments, teams, and rosters for a venue from the rating API.
    Fetches up to `months` back on first sync, or incrementally since last sync.
    Stores aggregated teams and rosters into Datastore (VenueData and VenueSyncState).
    """
    t0 = time.perf_counter()
    import datastore

    if not venue_id:
        return {"teams_count": 0, "tournaments_count": 0}

    state = datastore.get_venue_sync_state(venue_id) or {}
    last_synced_at = state.get("last_synced_at")
    synced_req_ids = set(state.get("synced_req_ids", []))

    now = datetime.datetime.now(pytz.utc)

    if last_synced_at and isinstance(last_synced_at, datetime.datetime):
        from_dt = last_synced_at - datetime.timedelta(days=14)
    else:
        from_dt = now - relativedelta(months=months)

    from_date_str = from_dt.strftime("%Y-%m-%d")

    params = {"dateStart[after]": from_date_str}
    sync_requests = _fetch_paginated(
        f"{API_URL}/venues/{venue_id}/requests",
        params,
        _VENUES_REQUESTS_ITEMS_PER_PAGE,
    )

    if not sync_requests:
        datastore.save_venue_sync_state(venue_id, now, synced_req_ids)
        return {"teams_count": 0, "tournaments_count": 0}

    valid_reqs = [r for r in sync_requests if r.get("status") == "A" and r.get("id")]
    
    def process_sync_req(sync_req):
        s_id = str(sync_req["id"])
        if s_id in synced_req_ids:
            return None
        
        t_id = sync_req.get("tournamentId")
        issued_at = sync_req.get("issuedAt") or sync_req.get("dateStart")
        issued_ts = 0
        if issued_at:
            try:
                dt_obj = datetime.datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
                issued_ts = int(dt_obj.timestamp())
            except Exception:
                issued_ts = int(now.timestamp())

        if not t_id:
            try:
                r_det = requests.get(f"{API_URL}/tournament_synch_requests/{s_id}", headers={"Accept": "application/json"}, timeout=4)
                if r_det.ok:
                    t_id = r_det.json().get("tournamentId")
            except Exception:
                pass

        if not t_id:
            return s_id, [], {}, {}

        teams_data = []
        rosters_data = {}
        display_names_data = {}
        try:
            r_res = requests.get(
                f"{API_URL}/tournaments/{t_id}/results",
                params={"venue": venue_id, "includeTeamMembers": 1, "itemsPerPage": 100},
                headers={"Accept": "application/json"},
                timeout=6,
            )
            if r_res.ok and isinstance(r_res.json(), list):
                results_list = r_res.json()
                for res_item in results_list:
                    # Verify matching venue or synchRequest if present in result item
                    sr = res_item.get("synchRequest")
                    if sr and isinstance(sr, dict):
                        sr_vid = sr.get("venue", {}).get("id") if isinstance(sr.get("venue"), dict) else None
                        sr_id = sr.get("id")
                        if sr_vid and int(sr_vid) != int(venue_id) and sr_id and str(sr_id) != str(s_id):
                            continue

                    team_obj = res_item.get("team") if isinstance(res_item.get("team"), dict) else {}
                    t_id_val = team_obj.get("id") or res_item.get("idteam")
                    if not t_id_val:
                        continue
                    
                    t_name = team_obj.get("name") or res_item.get("current", {}).get("name") or res_item.get("current_name") or "Команда"
                    t_town = ""
                    if isinstance(team_obj.get("town"), dict):
                        t_town = team_obj.get("town", {}).get("name", "")
                    elif isinstance(team_obj.get("town"), str):
                        t_town = team_obj.get("town", "")
                    elif isinstance(res_item.get("current", {}).get("town"), dict):
                        t_town = res_item.get("current", {}).get("town", {}).get("name", "")
                    
                    teams_data.append({
                        "team_id": int(t_id_val),
                        "name": t_name,
                        "town": t_town,
                        "tourn_count": 1,
                        "last_played_ts": issued_ts,
                    })

                    curr_name = (res_item.get("current", {}).get("name") or res_item.get("current_name") or "").strip()
                    if curr_name:
                        tid_k = str(t_id_val)
                        if tid_k not in display_names_data:
                            display_names_data[tid_k] = []
                        display_names_data[tid_k].append({
                            "name": curr_name,
                            "last_played_ts": issued_ts,
                        })

                    members = []
                    for tm in res_item.get("teamMembers", []):
                        player_obj = tm.get("player") if isinstance(tm.get("player"), dict) else {}
                        p_id = player_obj.get("id") or tm.get("idplayer")
                        if p_id:
                            members.append({
                                "player_id": int(p_id),
                                "name": player_obj.get("name", ""),
                                "surname": player_obj.get("surname", ""),
                                "patronymic": player_obj.get("patronymic", ""),
                                "tourn_count": 1,
                                "last_played_ts": issued_ts,
                            })
                    if members:
                        rosters_data[str(t_id_val)] = members
        except Exception as e:
            print(f"Error fetching results for tournament {t_id} (sync_req {s_id}): {e}")

        return s_id, teams_data, rosters_data, display_names_data

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(process_sync_req, valid_reqs))

    all_teams_flat = []
    all_rosters_merged = {}
    all_display_names = {}
    synced_ids_set = set(synced_req_ids)

    for item in results:
        if not item:
            continue
        s_id, t_data, r_data, d_data = item
        synced_ids_set.add(int(s_id))
        all_teams_flat.extend(t_data)
        for tid_str, p_list in r_data.items():
            if tid_str not in all_rosters_merged:
                all_rosters_merged[tid_str] = []
            all_rosters_merged[tid_str].extend(p_list)
        for tid_str, d_list in d_data.items():
            if tid_str not in all_display_names:
                all_display_names[tid_str] = []
            all_display_names[tid_str].extend(d_list)

    if all_teams_flat or all_rosters_merged or all_display_names:
        datastore.update_venue_data_incremental(venue_id, all_teams_flat, all_rosters_merged, all_display_names)

    datastore.save_venue_sync_state(venue_id, now, synced_ids_set)
    debug.log("rating_api.sync_venue_history", t0, f"venue_id={venue_id}, new_tourns={len([r for r in results if r])}, teams={len(all_teams_flat)}")
    return {"teams_count": len(all_teams_flat), "tournaments_count": len([r for r in results if r])}


def main():

    parser = argparse.ArgumentParser(description="This is a help message")
    parser.add_argument(
        "-d", "--date", type=str, required=True, help="Start date in YYYYMMDD format"
    )
    args = parser.parse_args()
    # print(get_tourns(parse_date(args.date)))


if __name__ == "__main__":
    main()
