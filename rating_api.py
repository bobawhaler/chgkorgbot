import argparse
import datetime
from dateutil.relativedelta import relativedelta
import requests
import helpers
import pytz


def get_tourn_by_id(tourn_id):
    url = f"https://api.rating.chgk.net/tournaments/{tourn_id}"
    response = requests.get(url, headers={"Accept": "application/json"})
    if not response.ok:
        print(
            f"Error getting tournament by id {tourn_id}, {response.status_code}, {response.reason}"
        )
        return {}
    return response.json()


def get_tourn_by_request(request_id, chat_id):
    url = f"https://api.rating.chgk.net/tournament_synch_requests/{request_id}"
    response = requests.get(url, headers={"Accept": "application/json"})
    if not response.ok:
        print(
            f"Error getting sync request by id {request_id}, {response.status_code}, {response.reason}"
        )
        return None, None
    result = response.json()
    return result.get("tournamentId", None), helpers.parse_date(
        result.get("issuedAt", ""), helpers.get_chat_timezone(chat_id)
    )[0].strftime("%Y-%m-%d")


def get_sync_requests_ids(venue_id, months):
    from_date = (datetime.datetime.now() - relativedelta(months=months)).strftime(
        "%Y-%m-%d"
    )
    # print(d, fd)
    result = []
    if not venue_id:
        return result
    for i in range(1, 30):
        url = f"https://api.rating.chgk.net/venues/{venue_id}/requests?page={i}&itemsPerPage=30&dateStart%5Bafter%5D={from_date}"
        response = requests.get(url, headers={"Accept": "application/json"})
        if not response.ok:
            print(
                f"Error getting sync requests for venue {venue_id}, {response.status_code}, {response.reason}"
            )
            break
        sync_requests = response.json()
        if not sync_requests:
            break
        for sync_req in sync_requests:
            if sync_req["status"] == "A":
                result.append(str(sync_req["id"]))

    return result


def get_new_sync_requests(venue_id):
    tz = pytz.timezone("Europe/Moscow")
    from_date = (datetime.datetime.now(tz) - relativedelta(minutes=15)).strftime(
        "%Y-%m-%d %H:%M"
    )
    # from_date = (datetime.datetime.now(tz) - relativedelta(days=7)).strftime(
    #     "%Y-%m-%d %H:%M"
    # )
    # print(d, fd)
    result = []
    if not venue_id:
        return result
    for i in range(1, 30):
        url = f"https://api.rating.chgk.net/venues/{venue_id}/requests?page={i}&itemsPerPage=30&issuedAt%5Bafter%5D={from_date}"
        response = requests.get(url, headers={"Accept": "application/json"})
        if not response.ok:
            print(
                f"Error getting new sync requests for venue {venue_id}, {response.status_code}, {response.reason}"
            )
        else:
            sync_requests = response.json()
            if not sync_requests:
                break
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

    return result


def get_tourns(tourn_date, played_tourns, chat_id, with_time=None, only_rated=False):
    from_date = (tourn_date - relativedelta(months=1)).strftime("%Y-%m-%d")
    print(tourn_date, from_date)
    result = []
    played_tourns_ids = played_tourns.keys()
    played_syncs = {}
    for tourn_id in played_tourns_ids:
        played_syncs[played_tourns[tourn_id][0]] = {
            "editors": played_tourns[tourn_id][1],
            "date": played_tourns[tourn_id][2],
        }
    # print(played_syncs)
    for i in range(1, 30):
        if with_time:
            to_date = requests.utils.quote(
                tourn_date.astimezone(pytz.timezone("Europe/Moscow")).strftime(
                    "%Y-%m-%d %H:%M"
                )
            )
            url = f"https://api.rating.chgk.net/tournaments?page={i}&itemsPerPage=50&dateStart%5Bbefore%5D={to_date}&dateStart%5Bafter%5D={from_date}&dateEnd%5Bafter%5D={to_date}&type=%D0%A1%D0%B8%D0%BD%D1%85%D1%80%D0%BE%D0%BD%2C%D0%90%D1%81%D0%B8%D0%BD%D1%85%D1%80%D0%BE%D0%BD"
        else:
            to_date = tourn_date.strftime("%Y-%m-%d")
            url = f"https://api.rating.chgk.net/tournaments?page={i}&itemsPerPage=50&dateStart%5Bbefore%5D={to_date}%2008%3A00&dateStart%5Bafter%5D={from_date}&dateEnd%5Bafter%5D={to_date}%2022%3A00&type=%D0%A1%D0%B8%D0%BD%D1%85%D1%80%D0%BE%D0%BD%2C%D0%90%D1%81%D0%B8%D0%BD%D1%85%D1%80%D0%BE%D0%BD"
        # print(url)
        response = requests.get(url, headers={"Accept": "application/json"})
        tournaments = response.json()
        # print(tournaments)
        if not response.ok:
            print(f"Error in get_tourns {response.status_code}, {response.reason}")
            break
        if not tournaments:
            break
        for tourn in tournaments:
            # print(tourn)
            if (
                "difficultyForecast" in tourn
                and tourn["difficultyForecast"]
                and tourn["difficultyForecast"] < 3
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
                # print(
                #     norm_name,
                #     tourn_editors,
                #     sync_from_date,
                # )
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
