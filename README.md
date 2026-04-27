# 下載者

Windows 桌面下載工具，使用 `tkinter` 製作介面，主要下載核心混合使用 `yt-dlp`、`curl_cffi`、直接 HTTP、以及 `ffmpeg`/`ffprobe`。

## Open Source

本專案以 **MIT License** 釋出。

## Source Provenance

- 此版本原始碼為可讀 Python 原始碼。
- 原始碼曾經過重建、修復與重構。
- 目前公開版本的原始碼由 **OpenAI Codex** 協助產生、整理、修正與維護。

## 目前功能

- Windows GUI 下載器
- 多語系介面自動依系統語系切換
  - 繁體中文
  - 簡體中文
  - English
  - 日本語
- 支援影片與 MP3 下載
- 支援拖放網址與部份拖放檔案流程
- 支援未完成任務保存、暫停、續傳、刪除
- 支援磁碟空間不足時自動暫停
- 支援同網域與同來源頁的下載佇列限制

## 真實支援狀態

以下是依目前程式碼實作與近期測試整理出的真實狀態。

### 已有專用流程

- 一般直接檔案連結下載
- 一般 `m3u8` / `ffmpeg` 路徑
- YouTube / `yt-dlp`
- Anime1
- MovieFFM
- 小鴨看看 (`xiaoyakankan`)
- Gimy 系列站點
  - `gimytv.biz`
  - `gimytv.io`
  - `gimy01.tv`
- Jable
- NJAVTV
- MissAV
- Threads
- Twitter / X
- Instagram
- Facebook

### 穩定度說明

- `YouTube`、一般直連、一般 `m3u8` 路徑相對穩定。
- `Anime1`、`MovieFFM`、`小鴨看看`、`Gimy` 有專用解析流程，但會受站點頁面結構、Cloudflare、防盜鏈、失效片源影響。
- `Jable`、`NJAVTV`、`MissAV` 依賴站點當前播放器結構與 `ffmpeg` 相容性，屬於可用但容易受上游變動影響的支援。
- `Instagram`、`Facebook`、`Threads`、`Twitter/X` 因平台反爬、防盜鏈、cookies、第三方 API 變動影響，屬於 **實驗性 / 易失效** 支援。

## 不保證的事項

- 任何站點的支援都可能因網站改版而失效。
- 某些來源雖然能解析網址，但最終下載仍可能被來源站封鎖。
- 某些整季頁雖然能展開集數，但部份集數可能本身已經播放失效。

## 核心依賴

- `yt-dlp`
- `curl_cffi`
- `Pillow`
- `ffmpeg`
- `ffprobe`

## 開發檔案

- 主程式：[C:\antigravity\ai_tets\downloader.py](C:\antigravity\ai_tets\downloader.py)
- 安全入口：[C:\antigravity\ai_tets\downloader_safe.py](C:\antigravity\ai_tets\downloader_safe.py)
- 打包設定：[C:\antigravity\ai_tets\downloader.spec](C:\antigravity\ai_tets\downloader.spec)

## 發版同步規則

- 本地版本號會持續遞增。
- 依目前要求，**每隔十版同步更新一次 GitHub**。

