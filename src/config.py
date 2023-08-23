from datetime import datetime, timedelta


start_from_date = datetime.now() - timedelta(31)
user_interval = "day"

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
