# temp_dbt

dbtプロジェクトのテンプレートリポジトリです。

## メモ
- viewよりtable+cronの方が、毎回テストがちゃんと通ってよい？

## docsの見方
- ciの`Upload Documantation artifacts`を開き、URLから.zipをダウンロード
- .zipを解凍
- ローカルのターミナルで解凍したディレクトリにいく
- ターミナル上で以下コマンドを実行
  - `python -m http.server 8080`
- chromeなどで以下URLを開く
  - `http://localhost:8080`

# raw,stg,dwh,dmの使い分け
- raw: そのままimport
- stg
  - 1rawごとに1stg
  - 型変換(日時はdatetime,JSTにするなど)、レコード重複削除や空レコード削除など
  - viewで作成(公式もそう推奨しているっぽい)
- dwh
  - 複数の分析やdmで使いまわしそうなもの
  - 複雑で処理にコストがかかるならtableに、そうでもない（毎回計算コストかかっても問題じゃない）ならviewっぽい
    - 今回は、price_historyだけtableにしてみる(レコード多めなので)
- (ここにセマンティックレイヤーを入れることもある？)
- dm
  - ダッシュボードなど１用途に１対応
  - 基本table（速さ、安定性重視らしい）

# ローカルでdbt実行方法
- dbt-bigqueryをインストールしている仮想環境をアクティブenv2
- dbtのレポジトリへ移動
- credentialをセット
  - `set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\hiroa\Documents\credentials\terraform-gha-dev.json`
  - `set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\hiroa\Documents\credentials\terraform-gha-prod.json`
- `dbt deps`で依存パッケージをインストール
- `set PYTHONUTF8=1`でutf8で読み込むように変更
- コマンド実行
  - analysesのコンパイル
    - `dbt compile --select path:analyses/`
  - 過去パラメータでrun
    - `dbt run --target prod --select dm_daily_aum dm_daily_all_term_pl dm_daily_position_value dm_daily_summary --vars "date_1day_ago: 2025-09-06"`
    - `dbt run --target prod --select dm_daily_summary --vars "date_1day_ago: 2025-09-05"`

    - `dbt run --select dwh_price_history --vars "{""date_28day_ago"": ""2025-07-01"", ""date_3day_ago"": ""2025-08-01"", ""date_1day_ago"": ""2025-08-31""}"`
    - `dbt run --target prod --select dwh_price_history --vars "{"date_28day_ago": "2019-12-01", "date_3day_ago": "2020-01-01", "date_1day_ago": "2024-12-31"}" && dbt test --target prod --select dwh_price_history --vars "{"date_28day_ago": "2019-12-01", "date_3day_ago": "2020-01-01", "date_1day_ago": "2024-12-31"}"`

## 🚀 セットアップ

### 1. 依存関係のインストール
```bash
pip install -r requirements.txt
```

### 2. dbtパッケージのインストール
```bash
dbt deps
```

### 3. 認証設定
BigQueryの認証情報を設定してください。

### 4. 接続テスト
```bash
dbt debug
```

## 📁 プロジェクト構成

```
temp_dbt/
├── models/
│   └── dwh/                    # データウェアハウスモデル
│       ├── dwh_asset_status_history.sql
│       └── schema.yml
├── macros/                     # 再利用可能なSQLマクロ
├── tests/                      # データテスト
├── seeds/                      # 静的データファイル
├── snapshots/                  # スナップショット
└── analyses/                   # アドホック分析
```

## 🔄 CI/CD

### GitHub Actions ワークフロー

- **Pull Request**: `dbt-ci.yml`
  - モデルの構文チェック
  - テスト実行
  - ドライラン実行

- **Main/Master ブランチ**: `dbt-deploy.yml`
  - 本番環境へのデプロイ
  - 全テスト実行
  - ドキュメント生成

### 必要なシークレット設定

GitHub リポジトリの Settings > Secrets and variables > Actions で以下を設定：

- `GOOGLE_CREDENTIALS`: BigQuery サービスアカウントのJSONキー

## 🛠️ 開発フロー

1. フィーチャーブランチを作成
2. モデルやテストを追加/修正
3. Pull Requestを作成
4. CI が自動実行され、結果がPRにコメント
5. レビュー後にマージ
6. 本番環境に自動デプロイ

## 📊 dbt コマンド

```bash
# モデル実行
dbt run

# テスト実行
dbt test

# ドキュメント生成
dbt docs generate
dbt docs serve

# 特定のモデルのみ実行
dbt run --select dwh_asset_status_history
```