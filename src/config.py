from datetime import datetime, timedelta


# TODO: DEBUG mode. Turn to 1800
notification_interval = 30

try:
    with open("res/telegram.key", 'r') as f:
        telegram_key = f.read()
except FileNotFoundError:
    raise FileNotFoundError("Can't find telegram token. It must be stored at res/telegram.key")

try:
    with open("res/polygon.key", 'r') as f:
        polygon_key = f.read()
except FileNotFoundError:
    polygon_key = ""

try:
    with open("res/moex.key", 'r') as f:
        moex_login_password = f.read().split()
except FileNotFoundError:
    moex_login_password = ("", "")

ERROR_LOG_FILENAME = "error.log"
LOGGER_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s:%(name)s:%(process)d:%(lineno)d %(levelname)s %(module)s.%(funcName)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "command": {
            "format": "%(asctime)s %(levelname)s %(funcName)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "[%(levelname)s] in %(module)s.%(funcName)s: %(message)s",
        },
    },
    "handlers": {
        "error_logfile": {
            "formatter": "default",
            "level": "ERROR",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": ERROR_LOG_FILENAME,
            "backupCount": 2,
        },
        "verbose_output": {
            "formatter": "simple",
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "bot": {
            "level": "DEBUG",
            "handlers": [
                "verbose_output",
            ],
        },
        "submodule": {
            "level": "DEBUG",
            "handlers": [
                "verbose_output",
            ],
        },
    },
    "root": {
        "level": "INFO",
        "handlers": [
            "error_logfile"
        ]
    },
}
