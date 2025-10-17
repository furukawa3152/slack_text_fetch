import os
import signal
import requests
from datetime import datetime
import time
import csv
import pandas as pd
import codecs
from typing import Any, Dict, List, Optional

CONVERSATIONS_HISTORY_URL: str = "https://slack.com/api/conversations.history"
CONVERSATIONS_REPLIES_URL: str = "https://slack.com/api/conversations.replies"
CONVERSATIONS_LIST_URL: str = "https://slack.com/api/conversations.list"
USERS_LIST_URL: str = "https://slack.com/api/users.list"
CONVERSATIONS_JOIN_URL: str = "https://slack.com/api/conversations.join"

_should_stop: bool = False


def _handle_sigint(signum: int, frame: Any) -> None:
    """Signal handler that sets a stop flag for graceful shutdown."""
    global _should_stop
    _should_stop = True


def should_stop() -> bool:
    """Return True if a stop has been requested via SIGINT, STOP file, or env flag.

    - SIGINT: Ctrl+C once to request graceful stop（2回目で通常のKeyboardInterruptになる動作を推奨）
    - STOPファイル: プロジェクト直下に `STOP` という空ファイルを作成
    - 環境変数: `STOP_NOW=1`
    """
    if _should_stop:
        return True
    if os.getenv("STOP_NOW") == "1":
        return True
    if os.path.exists("STOP"):
        return True
    return False


def _parse_datetime_to_epoch(dt_str: str) -> Optional[float]:
    """Parse ISO-like datetime string to epoch seconds.

    Returns None if parse fails.
    """
    try:
        # Python's fromisoformat handles 'YYYY-mm-dd HH:MM:SS[.ffffff]'
        return datetime.fromisoformat(dt_str).timestamp()
    except Exception:
        return None


def get_existing_channel_latest_epoch(csv_path: str) -> Optional[float]:
    """Read existing channel CSV and return the max timestamp (epoch seconds).

    Assumes the 3rd column 'ts' is a datetime string written by this script.
    Returns None if file missing or no valid rows.
    """
    if not os.path.exists(csv_path):
        return None
    latest: Optional[float] = None
    try:
        with open(csv_path, "r", encoding="cp932", newline="") as f:
            reader = csv.reader(f)
            # Skip header
            try:
                next(reader)
            except StopIteration:
                return None
            for row in reader:
                if len(row) < 3:
                    continue
                epoch = _parse_datetime_to_epoch(row[2])
                if epoch is None:
                    continue
                if latest is None or epoch > latest:
                    latest = epoch
    except Exception:
        return None
    return latest

def get_token() -> str:
    """Return Slack token from credential.csv.

    Supported formats (headered CSV):
    1) Column 'SLACK_BOT_TOKEN' (first non-empty row used)
    2) Column 'token' (first non-empty row used)
    3) Columns 'key','value' (use row where key in {'SLACK_BOT_TOKEN','token'})

    You can override path via env SLACK_CREDENTIAL_CSV; default 'credential.csv'.
    """
    path: str = os.getenv("SLACK_CREDENTIAL_CSV", "credential.csv")
    if not os.path.exists(path):
        raise RuntimeError(
            f"認証ファイルが見つかりません: {path}。'credential.csv' を作成してください。"
        )
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames: Optional[List[str]] = reader.fieldnames
        if not fieldnames:
            raise RuntimeError("credential.csv にヘッダ行がありません。")

        # 1) SLACK_BOT_TOKEN 列
        if "SLACK_BOT_TOKEN" in fieldnames:
            for row in reader:
                candidate: str = (row.get("SLACK_BOT_TOKEN") or "").strip()
                if candidate:
                    return candidate
            raise RuntimeError("credential.csv の SLACK_BOT_TOKEN 列が空です。")

        # 2) token 列
        f.seek(0)
        reader = csv.DictReader(f)
        if "token" in fieldnames:
            for row in reader:
                candidate = (row.get("token") or "").strip()
                if candidate:
                    return candidate
            raise RuntimeError("credential.csv の token 列が空です。")

        # 3) key,value 形式
        f.seek(0)
        reader = csv.DictReader(f)
        if set(["key", "value"]).issubset(set(fieldnames)):
            for row in reader:
                key: str = (row.get("key") or "").strip()
                value: str = (row.get("value") or "").strip()
                if key in {"SLACK_BOT_TOKEN", "token"} and value:
                    return value
            raise RuntimeError("credential.csv に対象キー(SLACK_BOT_TOKEN/token)が見つかりません。")

    raise RuntimeError(
        "credential.csv の形式が不正です。'SLACK_BOT_TOKEN' 列、'token' 列、または 'key,value' 形式に対応しています。"
    )


def http_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any], retries: int = 3, backoff_seconds: float = 1.0) -> Dict[str, Any]:
    """GET JSON with simple retry/backoff.

    Args:
        url: Request URL.
        headers: HTTP headers.
        params: Query parameters.
        retries: Number of retry attempts.
        backoff_seconds: Sleep seconds between retries (linear backoff).

    Returns:
        Parsed JSON dict.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        if should_stop():
            raise KeyboardInterrupt("停止フラグ検知により中断しました")
        try:
            res = requests.get(url, headers=headers, params=params, timeout=30)
            res.raise_for_status()
            data = res.json()
            if not data.get("ok", True):
                # Slack API error surfaced in body
                raise RuntimeError(f"Slack API error: {data}")
            return data
        except Exception as exc:  # retry on any exception
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)
            else:
                raise
    # Should not reach here
    assert last_exc is not None
    raise last_exc


def get_repry(channel_id: str, ts: str, token: str) -> List[Dict[str, Any]]:
    """Fetch replies (thread messages) for a given parent message timestamp.

    Args:
        channel_id: Slack channel ID.
        ts: Parent message ts.
        token: Slack token for Authorization header.

    Returns:
        List of message objects.
    """
    headers_auth: Dict[str, str] = {"Authorization": f"Bearer {token}"}
    payload: Dict[str, Any] = {"channel": channel_id, "ts": ts}
    data = http_get_json(CONVERSATIONS_REPLIES_URL, headers=headers_auth, params=payload)
    replies: List[Dict[str, Any]] = data.get("messages", [])
    return replies


def fetch_channel_history(token: str, channel_id: str, oldest: Optional[float] = None) -> List[Dict[str, Any]]:
    """Fetch channel messages with pagination. Optionally only newer than 'oldest'."""
    headers_auth: Dict[str, str] = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"channel": channel_id, "limit": 1000}
    if oldest is not None:
        params["oldest"] = f"{oldest:.6f}"
        params["inclusive"] = False
    messages: List[Dict[str, Any]] = []
    while True:
        if should_stop():
            break
        data = http_get_json(CONVERSATIONS_HISTORY_URL, headers=headers_auth, params=params)
        messages.extend(data.get("messages", []))
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor:
            params["cursor"] = cursor
        else:
            break
    return messages


def main(CHANNEL_NAME: str, SLACK_CHANNEL_ID: str, token: str) -> None:
    # Slackからエクスポートしたデータでメンバー名のDict作成
    counter: int = 0
    members = pd.read_csv("members.csv", encoding="utf-8")
    members = members[["userid", "fullname"]]
    member_dic: Dict[str, str] = {}
    for row in members.iterrows():
        member_dic[row[1]["userid"]] = row[1]["fullname"]
    channel_csv_path: str = f"{CHANNEL_NAME}.csv"
    oldest_epoch: Optional[float] = get_existing_channel_latest_epoch(channel_csv_path)

    # Fetch messages (paginate), optionally only newer than oldest_epoch
    try:
        msgs: List[Dict[str, Any]] = fetch_channel_history(token, SLACK_CHANNEL_ID, oldest=oldest_epoch)
    except RuntimeError as e:
        msg_txt = str(e)
        if 'not_in_channel' in msg_txt and os.getenv('AUTO_JOIN', '0') == '1' and not should_stop():
            joined = try_join_channel(SLACK_CHANNEL_ID, token)
            if joined and not should_stop():
                msgs = fetch_channel_history(token, SLACK_CHANNEL_ID, oldest=oldest_epoch)
            else:
                return
        else:
            return

    body: List[List[Any]] = []
    header: List[str] = ["text", "user", "ts"]
    for msg in msgs:
        if should_stop():
            break
        try:
            # print(msg["text"])
            # print(msg['reply_count'])
            if 'reply_count' in msg: #reply_countキーはリプライがあるときのみ存在
                # print(get_repry(msg["ts"]))
                replies = get_repry(SLACK_CHANNEL_ID, msg["ts"], token)
                # フィルタ: 既存の最新時刻より新しい返信のみ
                filtered_replies: List[Dict[str, Any]] = []
                for r in replies:
                    r_ts = float(r.get("ts", "0"))
                    if oldest_epoch is None or r_ts > oldest_epoch:
                        filtered_replies.append(r)
                for i, repry in enumerate(filtered_replies):
                    if should_stop():
                        break
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
                    userid = repry.get("user", "")
                    username = member_dic.get(userid, userid)
                    body.append([text, username, dt])
                    counter += 1
                    if counter % 10 == 0:
                        print(str(counter) + "get")
                    # print([repry["text"],repry["user"],repry["ts"]])
                    # print(repry)

                if should_stop():
                    break
                time.sleep(0.5)
            else:
                # フィルタ: 既存の最新時刻より新しいメッセージのみ
                msg_ts = float(msg.get("ts", "0"))
                if oldest_epoch is None or msg_ts > oldest_epoch:
                    text = msg["text"]
                    encoded = text.encode("cp932", "ignore")
                    text = encoded.decode("cp932")
                    dt = datetime.fromtimestamp(float(msg["ts"]))
                    userid = msg.get("user", "")
                    username = member_dic.get(userid, userid)
                    body.append([text, username, dt])
                    counter += 1
                    if counter % 10 == 0:
                        print(str(counter) + "get")


        except:
            pass
    # 追記 or 新規作成
    file_exists: bool = os.path.exists(channel_csv_path)
    write_header: bool = not file_exists
    mode: str = "a" if file_exists else "w+"
    with open(channel_csv_path, mode, newline="", encoding="cp932") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerows(body)
    f.close()
    # print(body)


def fetch_all_channels(token: str, types: str = "public_channel,private_channel") -> List[Dict[str, str]]:
    """Fetch all channels using conversations.list with pagination.

    Args:
        token: Slack token.
        types: Comma-separated types parameter.

    Returns:
        List of dicts containing channel_name and channel_id.
    """
    headers_auth: Dict[str, str] = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"types": types, "limit": 1000}
    only_joined: bool = os.getenv("ONLY_JOINED", "1") == "1"
    channels_acc: List[Dict[str, str]] = []
    while True:
        if should_stop():
            break
        data = http_get_json(CONVERSATIONS_LIST_URL, headers=headers_auth, params=params)
        for ch in data.get("channels", []):
            if only_joined and not ch.get("is_member", False):
                continue
            channels_acc.append({
                "channel_name": ch.get("name", ""),
                "channel_id": ch.get("id", "")
            })
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor:
            params["cursor"] = cursor
        else:
            break
    return channels_acc


def try_join_channel(channel_id: str, token: str) -> bool:
    """Attempt to join a public channel. Returns True if joined (or already in)."""
    headers_auth: Dict[str, str] = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"channel": channel_id}
    try:
        data = http_get_json(CONVERSATIONS_JOIN_URL, headers=headers_auth, params=params)
        if data.get("ok"):
            return True
    except Exception:
        pass
    return False


def fetch_all_users(token: str) -> List[Dict[str, str]]:
    """Fetch all workspace users via users.list with pagination.

    Returns list of dicts with keys: userid, fullname.
    Excludes bots and deleted users by default.
    """
    headers_auth: Dict[str, str] = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"limit": 200}
    users_acc: List[Dict[str, str]] = []
    while True:
        if should_stop():
            break
        data = http_get_json(USERS_LIST_URL, headers=headers_auth, params=params)
        for u in data.get("members", []):
            if u.get("deleted") or u.get("is_bot"):
                continue
            uid: str = u.get("id", "")
            profile: Dict[str, Any] = u.get("profile", {}) or {}
            name: str = (
                profile.get("real_name_normalized")
                or profile.get("real_name")
                or profile.get("display_name_normalized")
                or profile.get("display_name")
                or u.get("name", "")
            )
            users_acc.append({"userid": uid, "fullname": name})
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if cursor:
            params["cursor"] = cursor
        else:
            break
    return users_acc


def write_members_csv(users: List[Dict[str, str]], path: str = "members.csv") -> None:
    """Write users mapping to CSV with utf-8 encoding.

    Columns: userid, fullname
    """
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["userid", "fullname"])
        writer.writeheader()
        writer.writerows(users)


def ensure_members_csv(token: str, path: str = "members.csv") -> None:
    """Ensure members.csv exists or refresh when REFRESH_MEMBERS=1."""
    refresh: bool = os.getenv("REFRESH_MEMBERS", "0") == "1"
    if (not os.path.exists(path)) or refresh:
        users = fetch_all_users(token)
        write_members_csv(users, path)


def write_channel_list_csv(channels: List[Dict[str, str]], path: str = "channel_list.csv") -> None:
    """Write channels to CSV compatible with downstream reader."""
    with open(path, "w", newline="", encoding="cp932") as f:
        writer = csv.DictWriter(f, fieldnames=["channel_name", "channel_id"])
        writer.writeheader()
        writer.writerows(channels)


def ensure_channel_list(token: str, path: str = "channel_list.csv") -> None:
    """Ensure channel list CSV exists; if not, fetch and create it."""
    if not os.path.exists(path):
        channels = fetch_all_channels(token)
        write_channel_list_csv(channels, path)


if __name__ == "__main__":
    # Install SIGINT handler for graceful stop
    try:
        signal.signal(signal.SIGINT, _handle_sigint)
    except Exception:
        pass
    token = get_token()
    ensure_members_csv(token, path="members.csv")
    ensure_channel_list(token, path="channel_list.csv")
    channels_df = pd.read_csv("channel_list.csv", encoding="cp932")
    channels_df = channels_df[["channel_name", "channel_id"]]
    channel_list: List[List[str]] = []
    for channel in channels_df.iterrows():
        if should_stop():
            break
        channel_list.append([channel[1]["channel_name"], channel[1]["channel_id"]])
        channel_name: str = channel[1]["channel_name"]
        channel_id: str = channel[1]["channel_id"]
        main(channel_name, channel_id, token)
        print(f"{channel_name}:finished")
        if should_stop():
            break
        time.sleep(10)