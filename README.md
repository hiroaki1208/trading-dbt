# temp_dbt

dbtプロジェクトのテンプレートリポジトリです。

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