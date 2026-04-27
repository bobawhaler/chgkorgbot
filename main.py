import json
from flask import Flask, request
import telegram_api
import helpers
import handlers


app = Flask(__name__)


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
    handlers.command_handler(request)
    return ""


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. You
    # can configure startup instructions by adding `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
