# -*- coding: utf-8 -*-

_ENABLED = False


def get_debug():
    return _ENABLED


def set_debug(enabled):
    global _ENABLED
    _ENABLED = enabled


def log(prefix, duration, msg=None):
    if not _ENABLED:
        return
    line = f"[{duration:.2f}s] {prefix}"
    if msg:
        line += f" -> {msg}"
    print(line)
