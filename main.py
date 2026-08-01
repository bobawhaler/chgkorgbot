import json
import os
from flask import Flask, request
import telegram_api
import helpers
import handlers
from google.cloud import tasks_v2

app = Flask(__name__)
client = tasks_v2.CloudTasksClient()

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

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
