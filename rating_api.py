import argparse
import datetime
import time
import concurrent.futures
from dateutil.relativedelta import relativedelta
from urllib.parse import quote
import requests
import helpers
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

        resp = requests.get(f"{API_URL}/teams", params={"name": query, "itemsPerPage": 10}, headers={"Accept": "application/json"})
        if resp.ok:
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    town = item.get("town", {})
                    town_name = town.get("name", "") if isinstance(town, dict) else ""
                    result.append({"id": item.get("id"), "name": item.get("name", ""), "town": town_name})
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
    players_dict = {}
    try:
        resp = requests.get(f"{API_URL}/teams/{team_id}/seasons", params={"itemsPerPage": 100}, headers={"Accept": "application/json"})
        if resp.ok:
            data = resp.json()
            if isinstance(data, list):
                player_ids = []
                for item in data:
                    if isinstance(item, dict) and "idplayer" in item and item["idplayer"]:
                        if item["idplayer"] not in player_ids:
                            player_ids.append(item["idplayer"])

                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    fetched_players = list(executor.map(get_player_by_id, player_ids[:20]))
                    for p in fetched_players:
                        if p and isinstance(p, dict) and "id" in p:
                            players_dict[p["id"]] = p
    except Exception as e:
        print(f"Error fetching team players for team_id={team_id}: {e}")
    result = list(players_dict.values())
    debug.log("rating_api.get_team_players", t0, f"team_id={team_id} -> {len(result)}")
    return result


def search_players(query):
    t0 = time.perf_counter()
    query = str(query).strip()
    if not query:
        return []
    
    words = query.split()
    results = {}

    def do_search(params):
        try:
            resp = requests.get(f"{API_URL}/players", params={**params, "itemsPerPage": 10}, headers={"Accept": "application/json"})
            if resp.ok and isinstance(resp.json(), list):
                for p in resp.json():
                    if isinstance(p, dict) and "id" in p and p["id"] not in results:
                        pdata = {
                            "id": p["id"],
                            "name": p.get("name", ""),
                            "surname": p.get("surname", ""),
                            "patronymic": p.get("patronymic", ""),
                            "town": "",
                        }
                        results[p["id"]] = pdata
        except Exception as e:
            print(f"Error in search request {params}: {e}")

    try:
        if len(words) == 1:
            if words[0].isdigit():
                p_by_id = get_player_by_id(int(words[0]))
                if p_by_id:
                    results[p_by_id["id"]] = p_by_id
            do_search({"surname": words[0]})
            if len(results) < 5:
                do_search({"name": words[0]})
        elif len(words) == 2:
            w0, w1 = words[0], words[1]
            do_search({"surname": w1, "name": w0})
            do_search({"surname": w0, "name": w1})
        elif len(words) >= 3:
            w0, w1, w2 = words[0], words[1], words[2]
            do_search({"surname": w0, "name": w1, "patronymic": w2})
            do_search({"surname": w2, "name": w0, "patronymic": w1})
            if not results:
                do_search({"surname": w1, "name": w0})
                do_search({"surname": w0, "name": w1})
    except Exception as e:
        print(f"Error searching players query={query}: {e}")

    if results:
        pids = list(results.keys())[:10]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            towns = list(executor.map(get_player_town, pids))
            for pid, town in zip(pids, towns):
                if town and pid in results:
                    results[pid]["town"] = town
                    try:
                        import datastore
                        datastore.cache_player(pid, results[pid])
                    except Exception:
                        pass

    result = list(results.values())
    debug.log("rating_api.search_players", t0, f"query={query} -> {len(result)}")
    return result


def main():

    parser = argparse.ArgumentParser(description="This is a help message")
    parser.add_argument(
        "-d", "--date", type=str, required=True, help="Start date in YYYYMMDD format"
    )
    args = parser.parse_args()
    # print(get_tourns(parse_date(args.date)))


if __name__ == "__main__":
    main()
