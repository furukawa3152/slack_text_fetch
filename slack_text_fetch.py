import requests
from datetime import datetime
import time
import csv
import pandas as pd
import codecs

SLACK_URL = "https://slack.com/api/conversations.history"
TOKEN = "xoxb-2923753693205-3914837757633-ZW0PXXMiWZkEBw4wQmFcFDGE"


def get_repry(channel_id,ts):
    rep_url = "https://slack.com/api/conversations.replies"

    header = {
        "Authorization": "Bearer {}".format(TOKEN)
    }

    payload = {
        "channel": channel_id,
        "ts": ts
    }

    res = requests.get(rep_url, headers=header, params=payload)
    repries = res.json()["messages"]
    return repries


def main(CHANNEL_NAME,SLACK_CHANNEL_ID):
    # Slackからエクスポートしたデータでメンバー名のDict作成
    counter = 0
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
    body = []
    header = ["text", "user", "ts"]
    for msg in msgs:
        try:
            # print(msg["text"])
            # print(msg['reply_count'])
            if 'reply_count' in msg: #reply_countキーはリプライがあるときのみ存在
                # print(get_repry(msg["ts"]))
                count = len(get_repry(SLACK_CHANNEL_ID,msg["ts"]))
                for i in range(count):
                    repry = get_repry(SLACK_CHANNEL_ID,msg["ts"])[i]
                    dt = datetime.fromtimestamp(float(repry["ts"]))
                    # print(dt)
                    text = repry["text"]
                    encoded = text.encode("cp932", "ignore") #文字コードエラーになる文字を除外
                    text = encoded.decode("cp932")
                    if i != 0:
                        text = "Re:" + text
                    # text = repry["text"].replace(u'\xa0', ' ')
                    # text = text.replace(u'\u7626', ' ')
                    # text = text.replace(u'\u2022', ' ')
                    userid = repry["user"]
                    username = member_dic[userid]
                    body.append([text, username, dt])
                    counter += 1
                    if counter % 10 == 0:
                        print(str(counter) + "get")
                    # print([repry["text"],repry["user"],repry["ts"]])
                    # print(repry)

                time.sleep(0.5)
            else:
                text = msg["text"]
                encoded = text.encode("cp932", "ignore")
                text = encoded.decode("cp932")
                dt = datetime.fromtimestamp(float(msg["ts"]))
                userid = msg["user"]
                username = member_dic[userid]
                body.append([text, username, dt])
                counter += 1
                if counter % 10 == 0:
                    print(str(counter) + "get")


        except:
            pass
    with open(f"{CHANNEL_NAME}.csv", "w+", newline="", encoding="cp932") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(body)
    f.close()
    # print(body)


if __name__ == "__main__":
    # CHANNEL_NAME = "b_宣言"  # ファイル名作成用
    # SLACK_CHANNEL_ID = 'C02US9KRJNB'
    # main(CHANNEL_NAME,SLACK_CHANNEL_ID)
    channels = pd.read_csv("channel_list.csv",encoding="cp932")
    channels = channels[["channel_name","channel_id"]]
    channel_list = []
    for channel in channels.iterrows():
        channel_list.append([channel[1]["channel_name"],channel[1]["channel_id"]])
        channel_name = channel[1]["channel_name"]
        channel_id = channel[1]["channel_id"]
        main(channel_name,channel_id)
        print(f"{channel_name}:finished")
        time.sleep(10)

