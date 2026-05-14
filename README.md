# Downloader

Windows GUI 下載器，使用 `tkinter` 製作介面，整合 `yt-dlp`、`curl_cffi`、HTTP 直連下載與 `ffmpeg` / `ffprobe`。

## Open Source

本專案採用 **MIT License**。

## Source Provenance

- 現行主程式為 Python 專案。
- 專案已經歷多輪持續維護、重構、除錯與站點相容性修正。
- 目前維護流程包含人工調整與 **OpenAI Codex** 協作修正，但最終以 repo 內實際原始碼與 build 產物為準。

## 目前功能

- Windows GUI 下載器
- 多語系介面
  - 繁體中文
  - 簡體中文
  - English
  - 日本語
- 支援影片與 MP3 下載
- 支援拖放網址
- 支援未完成任務保存、暫停、續傳、刪除
- 支援磁碟空間不足時自動暫停
- 支援同網域與同來源頁的下載佇列限制
- 支援依站點選擇不同下載路徑
  - native HLS
  - generic `yt-dlp`
  - `ffmpeg`
  - HTTP 直連 / multipart

## 續傳機制

- 所有已支援網站只要最後落到 manifest 下載，現在都先走共用的續傳決策流程。
- `downloads.json` 會保存任務狀態、輸出檔名、暫存檔名與部份續傳資訊。
- 程式重新開啟後，會優先沿用既有 resume artifact，而不是重新猜新的暫存檔路徑。
- 若續傳任務在開始後長時間低速，會依目前策略自動回原頁重分析下載點。
- 個別站點仍可能因上游播放器差異，採用較保守的續傳路徑。

## 真實支援狀態

以下是目前 repo 內已有專用處理或已實際接過下載流程的站點：

- 一般 HTTP / 直連媒體 / 一般 `m3u8`
- YouTube / `yt-dlp`
- Anime1
- MovieFFM
- Gimy 系列
  - `gimytv.biz`
  - `gimytv.io`
  - `gimy01.tv`
  - `gimyai.tw`
- XiaoyaKankan
- 99iTV
- 777TV
- 3KOR
- NNYY
- Jable
- NJAVTV
- MissAV
- 18JAV
- 18AV / `18av.mm-cg.com`
  - `animation_content`
  - `CensoredAnimation_content`
  - `UncensoredAnimation_content`
  - `censored_content`
  - `uncensored_content`
  - `chinese_content`
  - `reducing-mosaic_content`
  - `amateurjav_content`
- PPP.Porn
- HoHoJ.TV
- PikPak 分享頁
  - 目前可辨識頁面、片名與候選媒體
  - 但受保護分享仍可能需要站方驗證，未必可匿名直接下載
- Hanime1 / HanimeOne
- Facebook
- Instagram
- Threads
- Twitter / X

## 支援說明

- `MovieFFM`、`Gimy`、`XiaoyaKankan`、`18AV`、`18JAV`、`HoHoJ.TV`、`PPP.Porn` 這類站點，多半依賴站方當前播放器結構、外部 CDN、保護播放器或 iframe 內頁。
- `Jable`、`NJAVTV`、`MissAV`、`NNYY` 等站點依賴當前 `m3u8` 結構與 `yt-dlp` / `ffmpeg` 相容性，屬於可用但容易受上游變動影響的支援。
- `Instagram`、`Facebook`、`Threads`、`Twitter/X` 因平台反爬、防盜鏈、cookies、第三方 API 變動影響，屬於 **實驗性 / 易失效** 支援。
- `PikPak`、某些受保護播放器頁、以及部份外站 iframe 來源，可能只能辨識頁面與播放器，但不一定能匿名直接下載實際檔案。

## 限制與風險

- 任何站點支援都可能因網站改版而失效。
- 某些站點雖然能播放，但實際下載流可能是外部 CDN、保護播放器、受限 API 或暫時失效片源。
- 某些站點會依來源主機不同，自動改走較保守或較快的下載器。
- 續傳流程已盡量統一，但站點上游若主動更換播放清單或 token，仍可能使舊續傳狀態失效。

## 主要依賴

- `yt-dlp`
- `curl_cffi`
- `requests`
- `Pillow`
- `ffmpeg`
- `ffprobe`

## 主要檔案

- 主程式：[C:\antigravity\ai_test\downloader.py](C:\antigravity\ai_test\downloader.py)
- 安全入口：[C:\antigravity\ai_test\downloader_safe.py](C:\antigravity\ai_test\downloader_safe.py)
- 打包設定：[C:\antigravity\ai_test\downloader.spec](C:\antigravity\ai_test\downloader.spec)
- GitHub 發布腳本：[C:\antigravity\ai_test\publish_github.ps1](C:\antigravity\ai_test\publish_github.ps1)
- build 腳本：[C:\antigravity\ai_test\build_downloader.ps1](C:\antigravity\ai_test\build_downloader.ps1)

## 發布與同步規則

- GitHub 說明與原始碼應以 repo 內最新狀態為準。
- 目前自動同步規則不是固定每小時檢查。
- 改為每次編譯完成後，由 `build_downloader.ps1` 讀取 `APP_BUILD`：
  - 若版本尾碼是 `10` 的倍數，才自動同步 GitHub
  - 其餘版本只編譯，不自動發布
- 如需非整十版本同步，可手動執行 [C:\antigravity\ai_test\publish_github.ps1](C:\antigravity\ai_test\publish_github.ps1)。
