# note-data-collector

noteクリエイターのデータを収集し、各アプリリポジトリに配信するハブ。

## リポジトリ構成

```
note-data-collector/
├── scripts/collect.py       # 共通収集スクリプト
├── creators.txt             # メンバーシップユーザー（全ツール共通）
├── fan-board/testers.txt    # note-fan-board固有のテスター
├── data/                    # 収集データ本体
└── .github/workflows/       # Actions定義
```

## 配信先リポジトリ

- **note-fan-board** (public) — ダッシュボードUI + GitHub Pages

## バージョニング（note-fan-board）

- **メジャー (x.0.0)** — 大きな仕様変更、互換性が壊れる変更
- **マイナー (0.x.0)** — 新機能追加、タブ追加など
- **パッチ (0.0.x)** — バグ修正、レイアウト微調整、テキスト変更

index.htmlの `?v=` パラメータで管理。変更時は必ず更新する。
