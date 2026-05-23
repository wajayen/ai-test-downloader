# Downloader

Windows 圖形化影音下載工具，使用 `tkinter` 提供操作介面，整合 `yt-dlp`、`curl_cffi`、`requests`、`ffmpeg` 與 `ffprobe`，支援一般影片網址、串流清單、指定網站解析、檔名/番號搜尋、續傳與 Windows 相容 MP4 輸出。

## 開源授權

本專案以 **MIT License** 釋出。

## 主要功能

- Windows GUI 下載器，支援輸入網址、拖放網址、批次任務與下載佇列。
- 支援多語言介面：繁體中文、簡體中文、English、日本語。
- 支援影片下載、音訊/MP3 下載、封面與基本資訊擷取。
- 支援檔名與番號搜尋，可先顯示搜尋結果確認視窗，再選擇是否下載。
- 搜尋排序會綜合檔名/番號吻合度、畫質、中文字幕與預估下載速度。
- 支援直接影片網址、HTTP 檔案、HLS `m3u8`、多段串流、iframe 與常見 CDN 解析。
- 下載完成後會檢查 MP4 相容性，必要時 remux 或轉碼為 Windows 預設播放器較容易播放的格式。
- `activity.log` 與 `error.log` 只保留最近紀錄，避免記錄檔無限制變大。

## 續傳與下載流程

- 下載狀態會寫入 `downloads.json`，包含來源網址、輸出檔、暫存檔、進度與站台資訊。
- 支援 HTTP Range、HLS 分段、`yt-dlp` 與 `ffmpeg` 流程的續傳或重試。
- 重新啟動軟體後會優先讀取既有任務狀態，避免已下載檔案歸零重來。
- 下載流程會依序嘗試站台專用解析、直接媒體連結、HLS 平行分段、`yt-dlp`、`ffmpeg` 與通用 HTTP fallback。
- 對可平行下載的 HLS 來源，會使用多 worker 分段下載以提高速度。

## 支援來源

以下為目前程式內建或已整合流程的主要來源類型：

- 一般 HTTP / HTTPS 影片、音訊與 `m3u8`
- YouTube 與 `yt-dlp` 支援網站
- Anime1
- MovieFFM
- Gimy 系列
- XiaoyaKankan / 小鴨影音
- 99iTV
- 777TV
- 3KOR
- NNYY
- Jable
- NJAVTV
- MissAV
- 18JAV
- 18AV / `18av.mm-cg.com`
- PPP.Porn
- HoHoJ.TV
- AVJoy
- AVBebe
- GoodAV17
- JavFilms
- TKTUBE
- PikPak 分享頁
- MEGA 免空連結
- Hanime1 / HanimeOne
- Facebook
- Instagram
- Threads
- Twitter / X

## 搜尋功能

- 直接輸入影片網址時，會依網址判斷站台並進入下載流程。
- 直接輸入檔名、片名或番號時，會走搜尋流程，不再把純文字誤判為網址。
- 番號搜尋會優先比對精準番號，例如 `MIDE-570`、`ROE-209`、`MKON-047`。
- 中文片名搜尋會過濾不包含關鍵字的明顯無關結果，降低錯誤站台結果混入。
- 已支援 MovieFFM、小鴨影音、AVJoy、HoHoJ.TV 等站台搜尋結果接入下載。

## 重要檔案

- 主程式：`downloader.py`
- 安全備份版：`downloader_safe.py`
- PyInstaller 設定：`downloader.spec`
- 編譯腳本：`build_downloader.ps1`
- 執行紀錄檢查：`check_runtime_logs.ps1`
- GitHub 發布腳本：`publish_github.ps1`
- 下載任務狀態：`dist/downloads.json`
- 錯誤紀錄：`dist/error.log`
- 活動紀錄：`dist/activity.log`

## 編譯與同步規則

- 每次修改程式碼前，應先檢查 `dist/error.log` 與 `dist/activity.log`，確認上一版是否有新的執行錯誤；若有，需一併修正。
- 編譯由 `build_downloader.ps1` 執行，腳本會讀取 `downloader.py` 內的 `APP_BUILD`。
- 目前不是每小時固定同步 GitHub，而是每次編譯完成後比對版本編號。
- 當 build 尾碼為 `10` 的倍數時，才自動執行 GitHub 同步與發布。
- 下次同步 GitHub 前，需依照目前程式實際功能檢查並更新本 README，確認說明內容與新版功能一致。
- 如需非整十版本手動同步，可執行 `publish_github.ps1`。

## 開發注意事項

- 修改 `downloader.py` 後，需同步更新 `downloader_safe.py`。
- 編譯前應執行 Python 語法檢查，避免 PyInstaller 打包後才發現錯誤。
- 對下載流程的修正應盡量集中在通用 fallback、站台解析、檔名清理、續傳狀態與 Windows 相容輸出流程，避免每個站台各自複製大量重複邏輯。
- 新增站台支援時，應優先確認是否能取得直接影片、HLS manifest 或 iframe 內可播放來源，再接入共用下載流程。
