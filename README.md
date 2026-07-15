# AI Test Downloader

Windows 圖形介面影片下載工具，支援拖放網址、輸入網址、檔名 / 番號搜尋、多站備援、續傳、佇列管理與自動封裝為 Windows 較容易播放的 MP4。程式主要整合 `yt-dlp`、`curl_cffi`、`requests`、`ffmpeg`、`ffprobe` 與站台專用解析規則。

目前版本：`20260715-3754`

## 目前狀態

- 主要功能可用：網址下載、搜尋下載、劇集 / 播放清單確認視窗、暫停、續傳、刪除、清除完成、關閉前下載中提醒。
- HLS 解密金鑰安全下載與 Session 複用：支援利用 `curl_cffi` (Chrome 120 指紋) 下載 AES-128 金鑰，並支援會話的 TCP Keep-Alive 複用與資源關閉生命週期優化。
- 工作執行緒 Session 管理與強效回收登出：併發下載分段時集中追蹤所有工作執行緒建立的連線會話，並在任務完成或終止時主動調用 `self._close_network_session` 將其從活動連線登冊（`_active_network_sessions`）中登出並回收，從根源上徹底解決長期運行時產生的記憶體與連接埠洩漏。
- 磁碟空間預分配（Pre-allocation）效能優化：下載分段前調用 `truncate` 預留磁碟空間，極大降低 NTFS 隨機碎片，加速 IO 寫入。
- 日誌會保留最近 10 筆 `error.log` 與 `activity.log` 內容，避免檔案無限制增長。
- 編譯流程會先檢查最新執行紀錄，再依 `APP_BUILD` 建置執行檔；符合同步條件時會自動建立 GitHub commit、tag 與 release asset。
- 已支援多站最高畫質優先選擇；若站台只提供較低畫質或需要登入 / cookies，會依可取得的來源下載。
- AV01 目前可解析授權 manifest 與 1080p 串流；若 native HLS 產物驗證失敗，會自動回退 ffmpeg 下載。
- MissAV 舊續傳任務會在啟動載入時重新正規化番號與中文字幕檔名，避免頁面標題亂碼污染任務列表與續傳比對。
- 番號搜尋會避開 BestJavPorn / JavDock 只回傳短預覽片段的假可下載來源，並保留其他搜尋頁作為自動換源候選。
- BestJavPorn / JavDock 會解析本頁加密 API 與 player config 取得真正 HLS，避免誤抓推薦卡片的預覽影片。已支援繞過 JAVDock 經由 Google 翻譯代理傳輸的分段防盜鏈機制（包含 `image/png` 偽裝媒體處理），並引入全局常數統一優化 fMP4 格式中 `ftyp`、`moof`、`mdat`、`styp`、`sidx` 與 `free` 等箱體特徵的檢測與解封裝。
- 85xvideo 會解析 WordPress / VideoJS 頁面內的直接 HLS source，並交由既有最高畫質與分段下載流程處理。
- 部分成人或串流站台會因 CDN、地區、來源失效、Cloudflare 或站方變更而需要重新解析或改走備援搜尋。

## 主要功能

- 拖放或貼上網址後自動判斷站台與下載類型。
- 輸入檔名、番號、片名或女優名稱時，會搜尋支援站台並顯示結果確認視窗。
- 搜尋結果排序會優先考慮檔名 / 番號吻合度、畫質、中文字幕、熱門程度與預估下載可行性。
- 搜尋結果低於 720p 的來源會盡量排除，除非沒有其他可用來源。
- 支援劇集頁與播放清單確認，下載前會列出集數或歌曲數量。
- 支援 YouTube 影片、播放清單與 MP3 模式，影片下載會優先選擇可取得的最高畫質。
- 支援 HLS / m3u8、直接 MP4、HTTP Range、多分段下載、yt-dlp native fragment 與 ffmpeg fallback。
- 下載完成後會驗證檔案大小與媒體資訊，避免 0 bytes、短片段或無法播放的檔案被誤判完成。
- 支援續傳狀態保存；重新啟動後會從 `downloads.json` 與暫存檔恢復未完成任務。
- 同一來源與同一網域有全域併發限制，避免單站過載；預設同來源最多 3 個檔案。
- 下載中關閉程式會提示仍有下載任務，並嘗試保存續傳狀態與清理背景執行緒。
- 支援多語言介面：繁體中文、簡體中文、英文、日文。

## 支援下載來源

下列為目前程式碼中已加入直接下載或解析流程的主要來源。實際可用性仍依站台當下播放來源、地區限制與登入狀態而定。

- 一般影片與影集：MovieFFM、Gimy 系列、XiaoyaKankan、小鴨看看、YFSP、NNYY、iQIYI、Dailymotion、YouTube、Bilibili、Anime1、Ani Gamer、Ikanbot、3KOR、DramaSQ、Olevod / OleHDTV、Thanju、99iTV、777TV。
- 成人影片站台：MissAV、NJAV / NJAVTV、Jable、TKTUBE、18JAV、18AV、AVJoy、AVBebe、GoodAV17、JavFilms、HayAV、85xvideo、BestJavPorn、JavDock、TinyAVideo、SupJav、AV01、Hanime1 / HanimeOne。
- 社群與平台：Facebook、Instagram、Threads、Twitter / X、TikTok。
- 免空 / 雲端：MEGA、PikPak。

## 搜尋支援

檔名 / 番號搜尋會嘗試比對多個支援站台，並排除無法取得實際影片檔案的網頁。搜尋來源包含：

- Anime1 / Ani Gamer
- MovieFFM / Gimy / XiaoyaKankan / YFSP / NNYY / iQIYI / YouTube / Dailymotion
- 3KOR / DramaSQ / Olevod / Thanju / Ikanbot / 777TV / 99iTV
- AVBebe / AVJoy / HayAV / 85xvideo / BestJavPorn / JavDock / TinyAVideo / SupJav / AV01
- MissAV / NJAV / NJAVTV / Jable / JavFilms / 18JAV / 18AV / GoodAV17 / HoHoJ / TKTUBE

搜尋結果若是劇集頁，程式會盡量自動判斷為整季 / 多集下載，而不是只下載單集。

## 下載與續傳流程

1. 建立任務後，程式會先解析來源頁與可用串流。
2. 若有多個畫質或多個 server，優先挑選最高畫質與較快來源。
3. 若可使用 HTTP Range，會採用多分段下載並保存分段進度。
4. 若是 HLS，會依站台規則選擇 parallel HLS、yt-dlp native HLS 或 ffmpeg。
5. 若已存在同名檔案，會先比對檔案大小與媒體資訊；相同才視為已完成。
6. 若續傳保留檔與目前來源大小或內容不合理，會重新解析來源，避免錯誤續傳造成破檔。
7. 若已記錄的下載連結超過 5 秒未開始，會重新分析原始來源頁。
8. 下載完成後會檢查檔案大小、duration 與 ffprobe 結果，必要時會 remux 成較相容的 MP4。

## 效能與限流

- 全域同來源下載上限預設為 3 個檔案。
- HLS parallel worker 依站台調整，例如 MovieFFM / Gimy / HayAV / 85xvideo / JavDock / TinyAVideo / SupJav 等站台有較高分段併發。
- yt-dlp fragment 下載預設有重試、fragment retry、檔案存取 retry 與較大的 HTTP chunk。
- 低速續傳會觸發重新解析，避免卡在失效 CDN 或過慢來源。
- 對部分站台會優先測試多個 fallback URL，並跳過已知錯誤或失效來源。
- TKTUBE 直接 MP4 會降低單任務 range 連線數，搭配更小 range segments 與站台專屬 timeout，避免同站過高併發造成連線逾時。
- TKTUBE 標題若含亂碼會回退使用番號，避免下載檔名與續傳狀態被亂碼污染。

## 日誌與狀態檔

- `dist/downloads.json`：下載任務與續傳狀態。
- `dist/error.log`：最近錯誤紀錄，最多保留 10 筆。
- `dist/activity.log`：重要流程紀錄，最多保留 10 筆。
- `dist/ffmpeg.exe` / `dist/ffprobe.exe`：下載、封裝與媒體驗證工具。
- 高頻續傳 checkpoint 使用原子寫入但不再產生 `.bak`，避免下載或執行目錄累積大量暫存備份檔。

每次修改程式碼前，應先執行：

```powershell
C:\antigravity\ai_test\check_runtime_logs.ps1
```

如果最新版本有新的執行錯誤，應先修正該錯誤，再進行其他調整。

## 編譯與發布

主要建置腳本：

```powershell
C:\antigravity\ai_test\build_downloader.ps1
```

建置流程會：

- 讀取 `downloader.py` 內的 `APP_BUILD`。
- 執行 runtime log pre-fix check。
- 使用 PyInstaller 產生 `dist/downloader.exe`。
- 複製必要的 `ffmpeg.exe` 與 `ffprobe.exe`，只有來源較新或 hash 不同時才覆蓋。
- 符合版本同步條件時，自動 commit、push、建立 tag 與 GitHub release asset。

GitHub CLI 在 Codex Windows 終端若出現偶發 401，需確認 `gh api user`、credential helper 與 `hosts.yml` 內是否有 `oauth_token`；必要時改用 `gh auth login --insecure-storage` 重新登入。

## 專案檔案

- `downloader.py`：主要程式碼。
- `downloader_safe.py`：同步維護的安全備份來源。
- `downloader.spec`：PyInstaller 設定。
- `build_downloader.ps1`：編譯與發布腳本。
- `check_runtime_logs.ps1`：修改前執行紀錄檢查。
- `publish_github.ps1`：GitHub 發布輔助腳本。
- `README.md`：目前功能與狀態說明。

## 維護原則

- 優先修正會造成下載錯誤、破檔、0 bytes、續傳歸零或狀態顯示錯誤的問題。
- 修改下載規則時，應同時考慮解析、下載、暫停、續傳、完成驗證、刪除與關閉程式流程。
- 支援新站台前，需確認能取得實際影片檔案；若只能取得廣告、iframe、列表頁或免空頁，不應標示為可下載。
- 對站台支援採保守策略：能穩定解析才加入，失效或只能取得錯誤影片時應移除或降為搜尋備援。
