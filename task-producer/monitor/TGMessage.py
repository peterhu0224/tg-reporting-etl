import requests


class TGMessage:
    BOT_NAME = "ETL_alert_bot"
    TOKEN = "5644939261:AAF3OZzfC3swB128e-jK6zN6qQDl9TWxWYg"
    CHAT_ID = "-1001877342829"

    def __init__(self):
        pass

    @classmethod
    def get_bot_update_info(cls):
        update_url = f"https://api.telegram.org/bot{cls.TOKEN}/getUpdates"
        return requests.get(update_url).json()

    @classmethod
    def send_msg_to_tg(cls, message):
        msg_url = f"https://api.telegram.org/bot{cls.TOKEN}/sendMessage?chat_id={cls.CHAT_ID}&text={message}"
        requests.get(msg_url).json()
