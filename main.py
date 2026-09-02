import json
import os
import traceback
from flask import Flask, request, render_template, jsonify, make_response
import telegram_api
import helpers
import handlers
import datastore
import rating_api
from google.cloud import tasks_v2

app = Flask(__name__)
_tasks_client = None

def get_tasks_client():
    global _tasks_client
    if _tasks_client is None:
        try:
            _tasks_client = tasks_v2.CloudTasksClient()
        except Exception as e:
            print(f"Error initializing CloudTasksClient: {e}")
            _tasks_client = None
    return _tasks_client

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/setwebhook", methods=["GET"])
def set_webhook():
    telegram_api.set_webhook()
    return "webhook set!"



# @app.route("/getwebhook", methods=["GET"])
# def get_webhook():
#     response = telegram_api.get_webhook()
#     return response.json()


@app.route("/systemtic", methods=["GET"])
def system_tic():
    handlers.system_tic_handler()
    return ""

@app.route(f"/command{helpers.OBFUSCATION_TOKEN}", methods=["POST"])
def command():
    if not request.data:
        return ""
    
    try:
        body = json.loads(request.data)
        project = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        queue = os.environ.get("CLOUD_TASKS_QUEUE", "bot-tasks")
        location = os.environ.get("APP_REGION", "us-central1")
        
        client = get_tasks_client()
        if client:
            parent = client.queue_path(project, location, queue)
            target_url = f"{request.host_url}process_task{helpers.OBFUSCATION_TOKEN}"
            
            task = {
                "http_request": {
                    "url": target_url,
                    "http_method": "POST",
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(body).encode(),
                }
            }
            client.create_task(parent=parent, task=task)
        else:
            # Fallback to direct synchronous execution if tasks client unavailable
            handlers.command_handler(body)
    except Exception as e:
        print(f"Error scheduling Cloud Task: {e}")
        
    return ""

@app.route(f"/process_task{helpers.OBFUSCATION_TOKEN}", methods=["POST"])
def process_task():
    try:
        body = request.get_json()
        if body:
            handlers.command_handler(body)
    except Exception as e:
        print(f"Error in task processing: {e}")
    return ""


@app.route(f"/sync_venue_task{helpers.OBFUSCATION_TOKEN}", methods=["POST"])
def sync_venue_task():
    try:
        body = request.get_json(silent=True) or {}
        venue_id = body.get("venue_id")
        if venue_id:
            rating_api.sync_venue_history(venue_id)
    except Exception as e:
        print(f"Error in sync_venue_task: {e}")
    return ""


# --- Mini App (WebApp) Routes ---

def get_authenticated_user(data):
    init_data = data.get("initData", "")
    user = helpers.validate_telegram_init_data(init_data)
    user_id = user.get("id") if isinstance(user, dict) else None

    # Fallback to user_id in payload if validation failed (e.g. dev/debug/local)
    if user_id is None:
        payload_uid = data.get("user_id")
        if payload_uid:
            try:
                user_id = int(payload_uid)
                user = {"id": user_id}
            except (ValueError, TypeError):
                user_id = None

    if user_id:
        is_admin = helpers.is_debug_allowed(user_id=user_id)
        return user, user_id, is_admin
    return None, None, False


@app.route("/webapp/roster", methods=["GET"])
def webapp_roster():
    resp = make_response(render_template("miniapp.html"))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/api/miniapp/init", methods=["POST"])
def api_miniapp_init():
    try:
        data = request.get_json(silent=True) or {}
        user, user_id, is_admin = get_authenticated_user(data)
        
        if not user or not user_id:
            return jsonify({"error": "Доступ запрещен. Не удалось авторизовать пользователя Telegram."}), 403

        try:
            user_mapping = datastore.get_user_mapping(user_id) if user_id else None
        except Exception as e:
            print(f"Error fetching user_mapping: {e}")
            user_mapping = None

        try:
            history = datastore.get_user_history(user_id) if user_id else {"teams": [], "players": []}
        except Exception as e:
            print(f"Error fetching user_history: {e}")
            history = {"teams": [], "players": []}

        can_switch_roles = bool(is_admin)
        active_test_role = None

        if is_admin:
            requested_role = data.get("test_role")
            stored_role = datastore.get_user_test_role(user_id)
            active_test_role = requested_role if requested_role else stored_role
            if requested_role and requested_role != stored_role:
                datastore.set_user_test_role(user_id, requested_role)

        if active_test_role == "guest":
            raw_regs = []
        elif active_test_role == "player":
            try:
                all_active = datastore.get_all_active_registrations()
                raw_regs = []
                for reg in all_active:
                    r_cid = reg.get("chat_id")
                    teams = reg.get("teams", [])
                    if teams:
                        raw_regs.append({
                            "sync_req_id": reg.get("sync_req_id"),
                            "chat_id": r_cid,
                            "tourn_name": reg.get("tourn_name"),
                            "is_representative": False,
                            "teams": teams
                        })
                    else:
                        raw_regs.append({
                            "sync_req_id": reg.get("sync_req_id"),
                            "chat_id": r_cid,
                            "tourn_name": reg.get("tourn_name"),
                            "is_representative": False,
                            "teams": [{
                                "team_name": "Тестовая Команда",
                                "display_name": "Тестовая Команда",
                                "user_id": user_id,
                                "rating_team_id": 90914,
                                "town": "Берлин",
                                "roster": [],
                                "roster_submitted": False
                            }]
                        })
            except Exception as e:
                print(f"Error in player role simulation: {e}")
                raw_regs = []
        elif active_test_role in ("rep", "representative"):
            try:
                all_active = datastore.get_all_active_registrations()
                raw_regs = []
                for reg in all_active:
                    r_cid = reg.get("chat_id")
                    raw_regs.append({
                        "sync_req_id": reg.get("sync_req_id"),
                        "chat_id": r_cid,
                        "tourn_name": reg.get("tourn_name"),
                        "is_representative": True,
                        "teams": reg.get("teams", [])
                    })
            except Exception as e:
                print(f"Error in rep role simulation: {e}")
                raw_regs = []
        elif is_admin:
            try:
                raw_regs = datastore.get_all_active_registrations()
                for r in raw_regs:
                    r["is_representative"] = True
            except Exception as e:
                print(f"Error fetching active registrations: {e}")
                raw_regs = []
        else:
            try:
                all_active = datastore.get_all_active_registrations()
                raw_regs = []
                for reg in all_active:
                    r_cid = reg.get("chat_id")
                    if not helpers.get_chat_collect_rosters(r_cid):
                        continue
                    
                    is_rep = datastore.is_user_representative(reg, user_id)
                    teams = reg.get("teams", [])
                    user_teams = [t for t in teams if t.get("user_id") == user_id]

                    if is_rep:
                        raw_regs.append({
                            "sync_req_id": reg.get("sync_req_id"),
                            "chat_id": r_cid,
                            "tourn_name": reg.get("tourn_name"),
                            "is_representative": True,
                            "teams": teams
                        })
                    elif user_teams:
                        for ut in user_teams:
                            raw_regs.append({
                                "sync_req_id": reg.get("sync_req_id"),
                                "chat_id": r_cid,
                                "tourn_name": reg.get("tourn_name"),
                                "is_representative": False,
                                "teams": [ut]
                            })
            except Exception as e:
                print(f"Error fetching user registrations: {e}")
                raw_regs = []

        tournaments = []
        registrations = []
        seen_keys = set()
        venue_ids = []

        for reg in raw_regs:
            sync_req_id = reg.get("sync_req_id")
            reg_chat_id = reg.get("chat_id")
            is_rep = bool(reg.get("is_representative") or (is_admin and active_test_role in ("admin", "rep", "representative", None)))
            if active_test_role == "player":
                is_rep = False

            if reg_chat_id:
                try:
                    c_cfg = datastore.get_chat_config(reg_chat_id)
                    if c_cfg and "venues" in c_cfg:
                        for vid in c_cfg["venues"]:
                            if vid not in venue_ids:
                                venue_ids.append(vid)
                except Exception as e:
                    print(f"Error getting chat config: {e}")

            raw_teams = reg.get("teams", [])
            tourn_name = reg.get("tourn_name", "Турнир")
            
            teams_list = []
            if raw_teams:
                for t in raw_teams:
                    is_sub = bool(t.get("roster_submitted") or (t.get("roster") and len(t.get("roster")) > 0) or t.get("submitted_externally"))
                    if t.get("roster_submitted") is False and not t.get("submitted_externally"):
                        is_sub = False
                    team_dict = {
                        "team_name": t.get("team_name", "Команда"),
                        "display_name": t.get("display_name", t.get("team_name", "Команда")),
                        "rating_team_id": t.get("rating_team_id"),
                        "town": t.get("town", ""),
                        "roster": list(t.get("roster", [])),
                        "roster_submitted": is_sub,
                        "submitted_externally": bool(t.get("submitted_externally")),
                        "user_id": t.get("user_id"),
                        "username": t.get("username", "")
                    }
                    teams_list.append(team_dict)
                    registrations.append({
                        "sync_req_id": sync_req_id,
                        "chat_id": reg_chat_id,
                        "tourn_name": tourn_name,
                        "is_representative": is_rep,
                        "team": team_dict
                    })

            submitted_count = sum(1 for t in teams_list if t.get("roster_submitted"))
            tournaments.append({
                "sync_req_id": sync_req_id,
                "chat_id": reg_chat_id,
                "tourn_name": tourn_name,
                "is_representative": is_rep,
                "submitted_count": submitted_count,
                "total_count": len(teams_list),
                "teams": teams_list
            })

        venue_teams = []
        if venue_ids:
            try:
                v_data = datastore.get_venues_data(venue_ids)
                venue_teams = v_data.get("teams", [])
            except Exception as e:
                print(f"Error getting venue teams: {e}")

        hist_teams = []
        for ht in history.get("teams", []):
            ht_dict = dict(ht)
            raw_t = ht_dict.get("town")
            ht_dict["town"] = raw_t.get("name", "") if isinstance(raw_t, dict) else (str(raw_t) if raw_t else "")
            hist_teams.append(ht_dict)

        return jsonify({
            "user": user,
            "user_mapping": user_mapping,
            "tournaments": tournaments,
            "registrations": registrations,
            "history_teams": hist_teams,
            "venue_teams": venue_teams,
            "can_switch_roles": can_switch_roles,
            "active_test_role": active_test_role or ("admin" if is_admin else "player")
        })
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Внутренняя ошибка сервера: {exc}"}), 500


@app.route("/api/miniapp/export_csv", methods=["GET"])
def api_miniapp_export_csv():
    try:
        init_data = request.args.get("initData", "")
        user_id_raw = request.args.get("user_id")
        sync_req_id = request.args.get("sync_req_id")
        chat_id_raw = request.args.get("chat_id")
        send_tg = request.args.get("send_tg") == "1"

        user, user_id, is_admin = get_authenticated_user({"initData": init_data, "user_id": user_id_raw})
        if not user or not user_id:
            return jsonify({"error": "Доступ запрещен"}), 403

        if not sync_req_id or not chat_id_raw:
            return jsonify({"error": "Не указан ID турнира или чата"}), 400

        try:
            chat_id = int(chat_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "Некорректный ID чата"}), 400

        reg = datastore.get_team_registration(chat_id, sync_req_id)
        if not reg:
            return jsonify({"error": "Турнир не найден"}), 404

        stored_role = datastore.get_user_test_role(user_id) if is_admin else None
        is_rep = bool(is_admin or datastore.is_user_representative(reg, user_id) or stored_role in ("admin", "rep", "representative"))
        if not is_rep:
            return jsonify({"error": "Экспорт CSV доступен только представителям турнира"}), 403

        teams = reg.get("teams", [])
        csv_content = helpers.generate_roster_csv(teams)
        tourn_name = reg.get("tourn_name", "roster")
        tourn_name_safe = helpers.normalize_tourn_name(tourn_name)
        filename = f"roster_{sync_req_id}_{tourn_name_safe}.csv"

        if send_tg:
            caption = f"📄 <b>Файл импорта составов для сайта рейтинга</b>\nТурнир: «{tourn_name}»\nКоманд: {len(teams)}"
            telegram_api.send_document(user_id, csv_content, filename, caption=caption)
            return jsonify({"ok": True, "sent_to_tg": True, "filename": filename})

        resp = make_response(csv_content)
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp
    except Exception as exc:
        print(f"Error exporting CSV: {exc}")
        return jsonify({"error": f"Ошибка выгрузки CSV: {exc}"}), 500


@app.route("/api/miniapp/reject_roster", methods=["POST"])
def api_miniapp_reject_roster():
    try:
        data = request.get_json() or {}
        user, user_id, is_admin = get_authenticated_user(data)
        if not user or not user_id:
            return jsonify({"error": "Доступ запрещен"}), 403

        sync_req_id = data.get("sync_req_id")
        chat_id_raw = data.get("chat_id")
        target_uid_raw = data.get("target_user_id")
        team_idx = data.get("team_index")

        if not sync_req_id or not chat_id_raw:
            return jsonify({"error": "Не указан ID турнира или чата"}), 400

        try:
            chat_id = int(chat_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "Некорректный ID чата"}), 400

        reg = datastore.get_team_registration(chat_id, sync_req_id)
        if not reg:
            return jsonify({"error": "Турнир не найден"}), 404

        stored_role = datastore.get_user_test_role(user_id) if is_admin else None
        is_rep = bool(is_admin or datastore.is_user_representative(reg, user_id) or stored_role in ("admin", "rep", "representative"))
        if not is_rep:
            return jsonify({"error": "Действие доступно только представителям турнира"}), 403

        target_uid = int(target_uid_raw) if target_uid_raw and str(target_uid_raw).isdigit() else None
        t_index = int(team_idx) if team_idx is not None else None

        reg_entity, rejected_tname = datastore.reject_team_roster_in_ds(chat_id, sync_req_id, target_user_id=target_uid, team_index=t_index)
        tourn_name = reg_entity.get("tourn_name", "турнир") if reg_entity else "турнир"
        tname_str = rejected_tname or "вашей команды"

        if target_uid:
            pm_msg = (
                f"⚠️ <b>Запрос исправления состава от представителя площадки!</b>\n\n"
                f"Представитель площадки вернул состав команды <b>\"{tname_str}\"</b> на турнир <b>\"{tourn_name}\"</b> на доработку.\n\n"
                f"Пожалуйста, проверьте и скорректируйте состав по кнопке <b>«Составы»</b> или отправьте /roster в этот личный чат с ботом."
            )
            telegram_api.send_message(target_uid, None, pm_msg, formatted=True)

        return jsonify({"ok": True, "team_name": tname_str, "tourn_name": tourn_name})
    except Exception as exc:
        print(f"Error in api_miniapp_reject_roster: {exc}")
        return jsonify({"error": f"Ошибка возврата состава: {exc}"}), 500


@app.route("/api/miniapp/mark_submitted", methods=["POST"])
def api_miniapp_mark_submitted():
    try:
        data = request.get_json() or {}
        user, user_id, is_admin = get_authenticated_user(data)
        if not user or not user_id:
            return jsonify({"error": "Доступ запрещен"}), 403

        sync_req_id = data.get("sync_req_id")
        chat_id_raw = data.get("chat_id")
        target_uid_raw = data.get("target_user_id")
        team_idx = data.get("team_index")
        submitted = data.get("submitted", True)
        external = data.get("external", True)

        if not sync_req_id or not chat_id_raw:
            return jsonify({"error": "Не указан ID турнира или чата"}), 400

        try:
            chat_id = int(chat_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "Некорректный ID чата"}), 400

        reg = datastore.get_team_registration(chat_id, sync_req_id)
        if not reg:
            return jsonify({"error": "Турнир не найден"}), 404

        stored_role = datastore.get_user_test_role(user_id) if is_admin else None
        is_rep = bool(is_admin or datastore.is_user_representative(reg, user_id) or stored_role in ("admin", "rep", "representative"))
        if not is_rep:
            return jsonify({"error": "Действие доступно только представителям турнира"}), 403

        target_uid = int(target_uid_raw) if target_uid_raw and str(target_uid_raw).isdigit() else None
        t_index = int(team_idx) if team_idx is not None else None

        reg_entity, updated_tname = datastore.mark_team_roster_submitted_in_ds(
            chat_id, sync_req_id, target_user_id=target_uid, team_index=t_index, submitted=submitted, external=external
        )
        return jsonify({"ok": True, "team_name": updated_tname, "submitted": submitted, "submitted_externally": external})
    except Exception as exc:
        print(f"Error in api_miniapp_mark_submitted: {exc}")
        return jsonify({"error": f"Ошибка обновления статуса: {exc}"}), 500


@app.route("/api/miniapp/remind_roster", methods=["POST"])
def api_miniapp_remind_roster():
    try:
        data = request.get_json() or {}
        user, user_id, is_admin = get_authenticated_user(data)
        if not user or not user_id:
            return jsonify({"error": "Доступ запрещен"}), 403

        sync_req_id = data.get("sync_req_id")
        chat_id_raw = data.get("chat_id")
        target_uid_raw = data.get("target_user_id")
        team_name = data.get("team_name", "Команда")

        if not sync_req_id or not chat_id_raw or not target_uid_raw:
            return jsonify({"error": "Недостаточно параметров"}), 400

        try:
            chat_id = int(chat_id_raw)
            target_uid = int(target_uid_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "Некорректный ID чата или пользователя"}), 400

        reg = datastore.get_team_registration(chat_id, sync_req_id)
        if not reg:
            return jsonify({"error": "Турнир не найден"}), 404

        stored_role = datastore.get_user_test_role(user_id) if is_admin else None
        is_rep = bool(is_admin or datastore.is_user_representative(reg, user_id) or stored_role in ("admin", "rep", "representative"))
        if not is_rep:
            return jsonify({"error": "Действие доступно только представителям турнира"}), 403

        tourn_name = reg.get("tourn_name", "турнир")
        pm_msg = (
            f"⏰ <b>Напоминание от представителя площадки!</b>\n\n"
            f"Представитель площадки запрашивает состав команды <b>\"{team_name}\"</b> на турнир <b>\"{tourn_name}\"</b>.\n\n"
            f"Пожалуйста, откройте кнопку <b>«Составы»</b> или отправьте /roster в этот личный чат с ботом для сдачи состава."
        )
        res = telegram_api.send_message(target_uid, None, pm_msg, formatted=True)
        if res and res.ok:
            return jsonify({"ok": True, "sent": True})
        else:
            return jsonify({"ok": False, "error": "Не удалось отправить сообщение пользователю"}), 400
    except Exception as exc:
        print(f"Error in api_miniapp_remind_roster: {exc}")
        return jsonify({"error": f"Ошибка отправки напоминания: {exc}"}), 500


@app.route("/api/miniapp/candidates", methods=["GET"])
def api_miniapp_candidates():
    team_id_raw = request.args.get("team_id")
    base_name = request.args.get("base_name", "")
    reg_name = request.args.get("registered_name", "")
    user_id_raw = request.args.get("user_id")
    chat_id_raw = request.args.get("chat_id")
    target_uid_raw = request.args.get("target_user_id")
    init_data = request.args.get("initData", "")
    
    user, user_id, is_admin = get_authenticated_user({"initData": init_data, "user_id": user_id_raw})
    
    if not user or not user_id:
        return jsonify({"error": "Доступ запрещен"}), 403

    effective_uid = int(target_uid_raw) if target_uid_raw and str(target_uid_raw).isdigit() else user_id
    team_id = int(team_id_raw) if team_id_raw and team_id_raw.isdigit() and int(team_id_raw) > 0 else None
    chat_id = int(chat_id_raw) if chat_id_raw and (chat_id_raw.isdigit() or (chat_id_raw.startswith("-") and chat_id_raw[1:].isdigit())) else None

    venue_ids = []
    if chat_id:
        cfg = datastore.get_chat_config(chat_id)
        if cfg and "venues" in cfg:
            venue_ids = list(cfg["venues"])

    candidates = helpers.get_roster_candidates(effective_uid, team_id, chat_id=chat_id)
    display_names = datastore.get_team_suggested_display_names(
        team_id=team_id,
        base_team_name=base_name,
        registered_name=reg_name,
        venue_ids=venue_ids,
        user_id=effective_uid
    )
    return jsonify({"candidates": candidates, "display_names": display_names})


@app.route("/api/miniapp/search_players", methods=["GET"])
def api_miniapp_search_players():
    query = request.args.get("query", "").strip()
    team_id_raw = request.args.get("team_id")
    user_id_raw = request.args.get("user_id")
    chat_id_raw = request.args.get("chat_id")
    init_data = request.args.get("initData", "")

    user, user_id, is_admin = get_authenticated_user({"initData": init_data, "user_id": user_id_raw})
    team_id = int(team_id_raw) if team_id_raw and team_id_raw.isdigit() and int(team_id_raw) > 0 else None
    chat_id = int(chat_id_raw) if chat_id_raw and (chat_id_raw.isdigit() or (chat_id_raw.startswith("-") and chat_id_raw[1:].isdigit())) else None

    players = helpers.search_players_tiered(query, user_id=user_id, rating_team_id=team_id, chat_id=chat_id)
    return jsonify({"players": players})


@app.route("/api/miniapp/search_teams", methods=["GET"])
def api_miniapp_search_teams():
    query = request.args.get("query", "").strip()
    user_id_raw = request.args.get("user_id")
    chat_id_raw = request.args.get("chat_id")
    init_data = request.args.get("initData", "")

    user, user_id, is_admin = get_authenticated_user({"initData": init_data, "user_id": user_id_raw})
    chat_id = int(chat_id_raw) if chat_id_raw and (chat_id_raw.isdigit() or (chat_id_raw.startswith("-") and chat_id_raw[1:].isdigit())) else None

    teams = helpers.search_teams_tiered(query, user_id=user_id, chat_id=chat_id)
    return jsonify({"teams": teams})


@app.route("/api/miniapp/team_details", methods=["GET"])
def api_miniapp_team_details():
    team_id_raw = request.args.get("team_id")
    if not team_id_raw or not team_id_raw.isdigit():
        return jsonify({"team": {}})
    team = rating_api.get_team_by_id(int(team_id_raw))
    return jsonify({"team": team})


@app.route("/api/miniapp/display_names", methods=["GET"])
def api_miniapp_display_names():
    team_id_raw = request.args.get("team_id")
    base_name = request.args.get("base_name", "")
    reg_name = request.args.get("registered_name", "")
    chat_id_raw = request.args.get("chat_id")
    user_id_raw = request.args.get("user_id")
    init_data = request.args.get("initData", "")

    user, user_id, is_admin = get_authenticated_user({"initData": init_data, "user_id": user_id_raw})
    team_id = int(team_id_raw) if team_id_raw and team_id_raw.isdigit() and int(team_id_raw) > 0 else None
    chat_id = int(chat_id_raw) if chat_id_raw and (chat_id_raw.isdigit() or (chat_id_raw.startswith("-") and chat_id_raw[1:].isdigit())) else None

    venue_ids = []
    if chat_id:
        cfg = datastore.get_chat_config(chat_id)
        if cfg and "venues" in cfg:
            venue_ids = list(cfg["venues"])

    display_names = datastore.get_team_suggested_display_names(
        team_id=team_id,
        base_team_name=base_name,
        registered_name=reg_name,
        venue_ids=venue_ids,
        user_id=user_id
    )
    return jsonify({"display_names": display_names})


@app.route("/api/miniapp/save_roster", methods=["POST"])
def api_miniapp_save_roster():
    data = request.get_json() or {}
    user, user_id, is_admin = get_authenticated_user(data)

    if not user or not user_id:
        return jsonify({"error": "Доступ запрещен"}), 403

    chat_id = data.get("chat_id")
    sync_req_id = data.get("sync_req_id")
    team_name = data.get("team_name", "Команда")
    display_name = data.get("display_name", team_name)
    town = data.get("town", "")
    rating_team_id = data.get("rating_team_id")
    roster = data.get("roster", [])
    team_index = data.get("team_index")
    target_user_id = data.get("target_user_id")
    registered_name = data.get("registered_name")

    if not chat_id or not sync_req_id:
        return jsonify({"error": "Не указан ID чата или турнира"}), 400

    updated_entity = datastore.update_team_roster_in_ds(
        chat_id,
        sync_req_id,
        user_id,
        rating_team_id,
        team_name,
        display_name,
        roster,
        town=town,
        team_index=team_index,
        target_user_id=target_user_id,
        registered_name=registered_name
    )

    if not updated_entity:
        return jsonify({"error": "Не удалось сохранить состав в базе данных. Проверьте регистрацию на турнир."}), 500

    datastore.add_user_history_team(user_id, rating_team_id, team_name, town=town, display_name=display_name)
    for p in roster:
        if p.get("name") or p.get("surname"):
            datastore.add_user_history_player(
                user_id, p.get("player_id"), p.get("name", ""), p.get("surname", ""), patronymic=p.get("patronymic", "")
            )

    team_info = f"Команда: <b>{display_name}</b>"
    if rating_team_id:
        team_info += f" (Рейтинг ID: {rating_team_id})"
    town_info = f"Город: <b>{town}</b>" if town else "Город: <i>не указан</i>"

    lines = [
        f"✅ <b>Состав команды «{display_name}» сохранен через Mini App!</b>\n",
        team_info,
        town_info,
        "",
        "<b>Состав:</b>"
    ]
    status_symbols = {"K": "👑 [К]", "B": "🛡 [Б]", "L": "⚔️ [Л]"}
    if not roster:
        lines.append("<i>(состав пуст)</i>")
    else:
        for idx, p in enumerate(roster, 1):
            st = status_symbols.get(p.get("status", "B"), "🛡 [Б]")
            pid_str = f" (ID: {p['player_id']})" if p.get("player_id") else " (без ID)"
            p_name = f"{p.get('name', '')} {p.get('surname', '')}".strip() or p.get("display_name", "Игрок")
            lines.append(f"{idx}. {st} {p_name}{pid_str}")

    saved_text = "\n".join(lines)
    webapp_btn = {"text": "📱 Открыть Mini App составов", "web_app": {"url": helpers.get_webapp_url()}}
    telegram_api.send_message(
        user_id,
        None,
        saved_text,
        formatted=True,
        reply_markup={"inline_keyboard": [[webapp_btn]]}
    )

    return jsonify({"ok": True})


@app.route("/api/miniapp/link_profile", methods=["POST"])
def api_miniapp_link_profile():
    data = request.get_json() or {}
    user, user_id, is_admin = get_authenticated_user(data)

    if not user or not user_id:
        return jsonify({"error": "Доступ запрещен"}), 403

    pid = data.get("player_id")
    name = data.get("name", "")
    surname = data.get("surname", "")
    patronymic = data.get("patronymic", "")
    town = data.get("town", "")
    username = user.get("username", "") if isinstance(user, dict) else ""

    if pid and user_id:
        datastore.set_user_mapping(
            user_id,
            int(pid),
            name,
            surname,
            patronymic,
            town,
            username
        )
        return jsonify({"ok": True})
    return jsonify({"error": "Не указан ID игрока"}), 400


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)

