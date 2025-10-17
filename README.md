# slack_text_fetch

Slack のチャンネルメッセージを CSV にエクスポートするツールです。チャンネル一覧とメンバー一覧を API から取得し、既存 CSV がある場合は差分のみを追記します。

## 機能概要
- 差分追記: 既存 CSV の最終時刻以降のメッセージ・スレッド返信のみ追加
- スレッド対応: 親メッセージに紐づく返信も新規分のみ取得
- 自動取得:
  - チャンネル一覧 `channel_list.csv`（`conversations.list`）
  - メンバー一覧 `members.csv`（`users.list`）
- 停止ギミック: 以下のいずれかで安全に停止
  - Ctrl+C（SIGINT 1回目で穏やか停止、2回目で通常割り込み）
  - プロジェクト直下に `STOP` ファイル作成
  - 環境変数 `STOP_NOW=1`

## 必要な権限（Bot Token Scopes）
必須（用途ごと）
- メッセージ取得（公開/非公開）: `channels:history`, `groups:history`
- チャンネル一覧取得: `channels:read`（公開）, `groups:read`（非公開）
- メンバー一覧: `users:read`（メールが要る場合は `users:read.email`）

任意（必要時のみ）
- 公開チャンネルへ自動参加（`AUTO_JOIN=1` を使う場合）: `channels:join`
- DM/マルチDMを扱う場合: `im:read`, `mpim:read` と履歴系 `im:history`, `mpim:history`

> 注: 付与後はワークスペースへ再インストール（Reinstall）して新トークンを反映してください。

## トークンの取得・保存（credential.csv）
トークンはリポジトリ管理外の `credential.csv` から読み込みます（`.gitignore` 済み）。

重要事項（ワークスペース境界）
- Slack のトークンは「発行元ワークスペースにインストールされたアプリ」に紐づきます。
- あるワークスペースで発行したトークンは、別のワークスペースでは使用できません。
- 複数ワークスペースで使う場合は、各ワークスペースにアプリをインストールし、必要スコープ付与・再インストール後、発行されたトークンをワークスペースごとに用意してください（`SLACK_CREDENTIAL_CSV`で切り替え可能）。

推奨フォーマット（パターン3: key,value 形式）
```csv
key,value
SLACK_BOT_TOKEN,xoxb-********************************
```

対応フォーマット（いずれか1つでOK）
1) 列名 `SLACK_BOT_TOKEN`
2) 列名 `token`
3) 列名 `key,value`（`key` が `SLACK_BOT_TOKEN` または `token`）

パスの上書き: 環境変数 `SLACK_CREDENTIAL_CSV` で設定可能（既定は `credential.csv`）。

## 使い方
1. トークンを `credential.csv` に保存（上記推奨フォーマット）
2. 必要スコープを付与し、アプリをワークスペースに再インストール
3. 実行
   ```bash
   python3 slack_text_fetch.py
   ```

### 任意の環境変数
- `ONLY_JOINED=1`（既定）: 参加済みチャンネルのみ対象化。全体対象にする場合は `0`。
- `AUTO_JOIN=1`: 未参加で `not_in_channel` の際に公開チャンネルへ自動参加を試行（要 `channels:join`）。
- `REFRESH_MEMBERS=1`: 実行時に毎回 `members.csv` を再生成。
- `SLACK_CREDENTIAL_CSV=/path/to/credential.csv`: 認証 CSV のパスを上書き。
- 停止: `STOP_NOW=1` または `touch STOP`、もしくは Ctrl+C。

## 出力
- `channel_list.csv`（CP932, 列: `channel_name`, `channel_id`）
- `members.csv`（UTF-8, 列: `userid`, `fullname`）
- 各チャンネル CSV（例: `a_general.csv`、CP932, 列: `text`, `user`, `ts`）

## 差分追記の仕組み
- 既存 CSV の `ts`（日時文字列）から最大値を取得
- `conversations.history` の `oldest` に反映し、その時刻より新しいメッセージのみ取得
- スレッド返信も同様に、`ts` が最大値より新しいもののみ追記

> フル再取得したい場合は、対象チャンネルの CSV を削除してから実行してください。

## トラブルシューティング
- `missing_scope`: トークンのスコープ不足。上記スコープを追加し、再インストールしてトークンを更新。
- `not_in_channel`: 参加していないチャンネル。`ONLY_JOINED=1` のままにするか、`AUTO_JOIN=1` と `channels:join` を付与して自動参加を許可。非公開は招待が必要。

## 注意
- CSV の文字コードは Excel で開きやすいよう `CP932`（チャンネルCSVとチャンネル一覧）/`UTF-8`（メンバーCSV）を採用しています。