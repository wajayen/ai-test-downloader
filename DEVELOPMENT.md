# 抓抓抓 Downloader - Development Guidelines

## Internationalization (I18N) Requirement
**CRITICAL RULE:** This application supports a dynamic multi-language engine (currently Traditional Chinese, English, and Japanese). 
Any future UI changes, notifications, error messages, context menus, and statuses MUST use the dictionary variables (the `t(key)` function and `I18N_DICT` configuration) and must NOT be hardcoded as raw Chinese or English strings.

### Process for adding new text:
1. Locate `I18N_DICT` in the code.
2. Add a new key and provide the translation for `zh_TW`, `en_US`, and `ja_JP`.
3. Use `t("your_new_key")` in the UI rendering or prompt logic.

## Technical Details
- Core: `yt-dlp` (Video / Audio fetching)
- Fetcher: `curl_cffi` (For bypassing Cloudflare / Anti-bot protections on Anime1, MissAV, etc.)
- GUI GUI: `tkinter` + `tkinterdnd2`

## Compilation / Deployment
**CRITICAL RULE:** When compiling or packaging this software into an executable (e.g. using PyInstaller), the configured output filename MUST use the original English filename: `downloader` (e.g. `downloader.exe`). Do NOT use localized titles or nicknames like "ZhuaZhuaZhua" for the binary output.

*This document was created automatically at the user's request to ensure all future maintainers or AI agents respect the structural framework.*
