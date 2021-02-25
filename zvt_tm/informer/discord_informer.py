import time

import requests
from zvt import zvt_config


class DiscordInformer(object):

    def __init__(self):
        self.session = requests.Session()
        self.webhook_url = zvt_config['discord_webhook_url']

    def send_message(self, content, **kwargs):
        if self.webhook_url:
            payload = {
                "content": content
            }
            headers = {
                'Content-Type': 'application/json',
            }
            rate_limited = True
            resp = None

            while rate_limited:
                resp = self.session.post(self.webhook_url, json=payload,
                                         headers=headers)

                if resp.status_code == 429:  # Too many request
                    time.sleep(resp.json()['retry_after'] / 1000.0)
                    continue
                else:
                    rate_limited = False

            if resp.status_code == 204:  # method DELETE
                return

            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err:
                print(err)
            else:
                print("Payload delivered successfully, code {}.".format(resp.status_code))


def __exit__(self, *args):
    return self.session.close()
