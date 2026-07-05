import time
import helpers
import datastore

def get_debug():
    config = datastore.get_chat_config("bot_debug")
    return config and config.get("debug", False)

def set_debug(enabled):
    datastore.update_chat_config("bot_debug", None, debug=enabled)

def log(name, t0, *args):
    if get_debug():
        ms = (time.perf_counter() - t0) * 1000
        print(f"[{ms:.0f}ms] {name} {' '.join(str(a) for a in args)}")
