import requests
from datetime import datetime
import time
import csv
import pandas as pd
import codecs

SLACK_URL = "https://slack.com/api/conversations.history"
TOKEN = "xoxb-2923753693205-3914837757633-ZW0PXXMiWZkEBw4wQmFcFDGE"

def main(CHANNEL_NAME,SLACK_CHANNEL_ID):
    # Slackからエクスポートしたデータでメンバー名のDict作成
    members = pd.read_csv("members.csv", encoding="utf-8")
    members = members[["userid", "fullname"]]
    member_dic = {}
    for row in members.iterrows():
        member_dic[row[1]["userid"]] = row[1]["fullname"]
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "limit": 10000
        # "oldest": "1622761200"
    }
    headersAuth = {
        'Authorization': 'Bearer ' + str(TOKEN),
    }
    response = requests.get(SLACK_URL, headers=headersAuth, params=payload)
    json_data = response.json()
    msgs = json_data['messages']
    msg = msgs[0]
    text = msg["text"]
    encoded = text.encode("cp932", "ignore")
    text = encoded.decode("cp932")
    dt = datetime.fromtimestamp(float(msg["ts"]))
    userid = msg["user"]
    username = member_dic[userid]
    print([text, username,dt])
    print('reply_count' in msg)
    # print(msgs)

if __name__ == '__main__':
    CHANNEL_NAME = "b_宣言"  # ファイル名作成用
    SLACK_CHANNEL_ID = 'C02US9KRJNB'
    main(CHANNEL_NAME,SLACK_CHANNEL_ID)