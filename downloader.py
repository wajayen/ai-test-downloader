"""Readable restored source for downloader 2122.

This file was rebuilt from the recovered 2122 executable payload and then
hand-repaired into a maintainable Python source file.
"""

from __future__ import annotations

import concurrent.futures
import copy
import ctypes
import glob
import hashlib
import html
import json
import locale
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.request
import urllib.parse
import zipfile
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from tkinterdnd2 import DND_ALL, DND_FILES, DND_TEXT  # type: ignore
except Exception:  # pragma: no cover - optional integration
    DND_ALL = None
    DND_FILES = None
    DND_TEXT = None
    TkinterDnD = None
else:  # pragma: no cover - optional integration
    try:
        from tkinterdnd2 import TkinterDnD  # type: ignore
    except Exception:
        TkinterDnD = None

try:
    import yt_dlp  # noqa: F401
except Exception:  # pragma: no cover - readable reconstruction only
    yt_dlp = None


APP_BUILD = "20260427-2236"
CURRENT_LANG = "en_US"
if getattr(sys, "frozen", False):
    _APP_DIR = os.path.abspath(os.path.dirname(sys.executable))
else:
    _APP_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(_APP_DIR, "config.json")
STATE_FILE = os.path.join(_APP_DIR, "downloads.json")
ERROR_LOG_FILE = os.path.join(_APP_DIR, "error.log")
MAX_DOWNLOADS_PER_DOMAIN = 3
MAX_DOWNLOADS_PER_SOURCE_PAGE = 1
MAX_QUEUE_TASKS = 300
DISK_SPACE_RESERVE_BYTES = 256 * 1024 * 1024
STATE_PERSIST_INTERVAL_SECONDS = 2.5
RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS = 2.0
RESUME_PROGRESS_MIN_BYTES_DELTA = 2 * 1024 * 1024
ERROR_LOG_DEDUPE_WINDOW_SECONDS = 2.0
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
FFMPEG_WINDOWS_FFMPEG_URL = "https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffmpeg-6.1-win-64.zip"
FFMPEG_WINDOWS_FFPROBE_URL = "https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffprobe-6.1-win-64.zip"
TERMINAL_TASK_STATES = frozenset(("FINISHED", "DELETED", "DELETE_REQUESTED"))
PAUSED_TASK_STATES = frozenset(("PAUSED", "PAUSE_REQUESTED"))
IMPERSONATION_SITE_MARKERS = ("missav", "gimy", "movieffm", "xiaoyakankan", "jable", "njavtv", "anime1")
DELETE_CLEANUP_TASK_STATES = frozenset(("PAUSED", "QUEUED"))
DELETE_REQUEST_TASK_STATES = frozenset(("DOWNLOADING", "PAUSED", "PAUSE_REQUESTED", "QUEUED"))
STOP_REQUEST_TASK_STATES = frozenset(("PAUSE_REQUESTED", "DELETE_REQUESTED"))
RESUMABLE_TASK_STATES = frozenset(("PAUSED", "ERROR"))
STOP_REASON_PAUSE = "pause"
STOP_REASON_DELETE = "delete"
STOP_REASONS = frozenset((STOP_REASON_PAUSE, STOP_REASON_DELETE))
_ERROR_LOG_RECENT = {}

FORCED_M3U8_SITE_RULES = {
    "jable": {
        "hosts": ("mushroomtrack.com",),
        "origin": "https://jable.tv",
        "referer": "https://jable.tv/",
    },
    "movieffm": {
        "hosts": (
            "qsstvw.com",
            "qqqrst.com",
            "play-cdn16.com",
            "ffzy-online2.com",
            "ijycnd.com",
            "lz-cdn6.com",
            "taopianplay1.com",
            "gsuus.com",
            "hhuus.com",
            "bfllvip.com",
            "dytt-video.com",
            "mzm3u8.vip",
            "xluuss.com",
            "kunyu.com.cn",
            "ryplay8.com",
            "zuidazym3u8.com",
            "huyall.com",
        ),
        "origin": "https://www.movieffm.net",
        "referer": "https://www.movieffm.net/",
    },
    "njavtv": {
        "hosts": ("surrit.com",),
        "origin": "https://njavtv.com",
        "referer": "https://njavtv.com/",
    },
    "xiaoyakankan": {
        "hosts": (
            "bfvvs.com",
            "ijycnd.com",
            "gsuus.com",
            "hhuus.com",
            "huyall.com",
            "jisuzyv.com",
            "play-cdn15.com",
            "subokk.com",
            "qsstvw.com",
            "kuaichezym3u8.com",
            "myqqdd.com",
            "rsfcxq.com",
            "zuidazym3u8.com",
            "gghijk.com",
            "yuglf.com",
            "hhiklm.com",
        ),
        "origin": "https://tw.xiaoyakankan.com",
        "referer": "https://tw.xiaoyakankan.com/",
    },
}

RE_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
state_lock = threading.RLock()
single_instance_mutex = None
anime1_dl_lock = threading.Lock()
ytdl_init_lock = threading.Lock()

_I18N_SNAPSHOT = Path(__file__).with_name("downloader_2122_i18n_snapshot.json")
if _I18N_SNAPSHOT.exists():
    I18N_DICT = json.loads(_I18N_SNAPSHOT.read_text(encoding="utf-8"))
else:  # pragma: no cover - fallback only
    I18N_DICT = {
        "zh_TW": {"app_title": "下載者", "msg_warning": "警告"},
        "en_US": {"app_title": "Downloader", "msg_warning": "Warning"},
        "ja_JP": {"app_title": "ダウンローダー", "msg_warning": "警告"},
    }

I18N_PATCH = {
    "zh_TW": {
        "app_title": "下載者",
        "subtitle": "支援拖放、排隊、續傳與多站點下載",
        "basic_settings": "基本設定",
        "save_dir": "預設儲存位置：",
        "label_lang": "介面語言：",
        "browse": "瀏覽...",
        "new_url": "新的影片網址：",
        "format_video": "影片",
        "format_audio": "MP3",
        "add_task": "加入下載清單",
        "impersonate_chk": "啟用 MissAV/Gimy 頁站點繞過模式",
        "chk_topmost": "視窗置頂",
        "list_frame": "下載任務列表",
        "col_name": "檔名",
        "col_progress": "進度",
        "col_size": "大小",
        "col_speed_eta": "速度 / 剩餘時間",
        "col_status": "狀態",
        "menu_resume": "續傳 / 重試",
        "menu_pause": "暫停",
        "menu_delete": "刪除",
        "menu_clear": "清除已完成",
        "ctx_cut": "剪下",
        "ctx_copy": "複製",
        "ctx_paste": "貼上",
        "ctx_select_all": "全選",
        "status_done": "完成",
        "status_paused": "已暫停",
        "status_downloading": "下載中",
        "status_error": "錯誤",
        "status_deleting": "刪除中",
        "status_queued": "排隊中",
        "status_starting": "準備下載",
        "status_finalizing": "整理中",
        "status_processing": "整理中",
        "msg_error": "錯誤",
        "msg_warning": "警告",
        "msg_already_running": "已有執行中的下載者，請先關閉舊視窗。",
        "msg_close_warn": "目前仍有下載任務進行中，確定要關閉程式嗎？",
        "msg_ffmpeg_required_title": "需要 FFmpeg",
        "msg_ffmpeg_required_body": "此功能需要 FFmpeg。是否現在安裝？",
        "msg_playlist_add_all": "共找到 {count} 個項目，是否全部加入下載清單？",
        "msg_fetch_anime1_empty": "Anime1 沒有找到可下載的項目。",
        "msg_fetch_gimy_empty": "Gimy 沒有找到可下載的項目。",
        "msg_fetch_movieffm_empty": "MovieFFM 沒有找到可下載的項目。",
        "msg_fetch_anime1_failed": "Anime1 解析失敗：{error}",
        "msg_fetch_gimy_failed": "Gimy 解析失敗：{error}",
        "msg_fetch_movieffm_failed": "MovieFFM 解析失敗：{error}",
        "msg_fetch_hanime_failed": "Hanime1 解析失敗：{error}",
        "msg_text_read_failed": "讀取文字檔失敗：{error}",
        "msg_local_file_copied": "已複製：{name}",
        "msg_local_file_copy_speed": "本機複製",
        "msg_local_file_copy_failed": "複製本機檔案失敗：{error}",
        "msg_resume_name": "未完成項目",
        "msg_ffmpeg_window_title": "下載者 / 安裝 FFmpeg",
        "msg_ffmpeg_mac_hint": "macOS 可使用 Homebrew 安裝 FFmpeg。",
        "msg_ffmpeg_mac_status": "正在安裝 FFmpeg...",
        "msg_ffmpeg_mac_button": "開啟終端機安裝",
        "msg_ffmpeg_linux_hint": "Linux 請使用套件管理器安裝 FFmpeg。",
        "msg_ffmpeg_linux_command": "例如：sudo apt install ffmpeg",
        "msg_ffmpeg_close": "關閉",
        "msg_ffmpeg_win_hint": "正在下載 FFmpeg，請稍候...",
        "msg_ffmpeg_progress_download": "下載進度：{progress}",
        "msg_download_error": "下載錯誤：{error}",
        "msg_file_exists": "檔案已存在",
        "err_no_extractor": "找不到可用的解析器",
        "err_missing_json_url": "找不到 JSON 來源",
        "err_missing_mp4_source": "找不到 MP4 來源",
        "err_invalid_instagram_url": "Instagram 網址格式不正確",
        "err_api_status": "API 狀態錯誤：{status}",
        "err_http_status": "HTTP 錯誤：{status}",
        "err_missav_bypass": "正在解析 MissAV...",
        "err_direct_fail": "直接下載失敗，改用解析器重試...",
        "err_site_parse": "解析失敗",
        "err_net": "連線不穩，請稍後重試",
        "eta_processing": "整理中",
        "eta_file_exists": "檔案已存在",
        "eta_direct_media": "正在下載直接媒體",
        "eta_site_jable": "正在解析 Jable...",
        "eta_site_njavtv": "正在解析 NJAVTV...",
        "eta_site_movieffm": "正在解析 MovieFFM...",
        "eta_site_gimy": "正在解析 Gimy...",
        "eta_site_hanime": "正在解析 Hanime1...",
        "eta_site_missav": "正在解析 MissAV...",
        "eta_site_threads": "正在解析 Threads...",
        "eta_site_instagram": "正在解析 Instagram...",
        "eta_site_twitter": "正在解析 Twitter/X...",
        "eta_found_stream": "已取得串流網址，準備下載",
        "eta_found_media": "已取得媒體網址，準備下載",
        "overview_idle": "待命中",
        "lang_name_zh_TW": "繁體中文",
        "lang_name_zh_CN": "简体中文",
        "lang_name_en_US": "English",
        "lang_name_ja_JP": "日本語",
    },
    "zh_CN": {
        "app_title": "下载者",
        "subtitle": "支持拖放、排队、续传与多站点下载",
        "basic_settings": "基本设置",
        "save_dir": "默认保存位置：",
        "label_lang": "界面语言：",
        "browse": "浏览...",
        "new_url": "新的影片网址：",
        "format_video": "视频",
        "format_audio": "MP3",
        "add_task": "加入下载清单",
        "impersonate_chk": "启用 MissAV/Gimy 站点绕过模式",
        "chk_topmost": "窗口置顶",
        "list_frame": "下载任务列表",
        "col_name": "文件名",
        "col_progress": "进度",
        "col_size": "大小",
        "col_speed_eta": "速度 / 剩余时间",
        "col_status": "状态",
        "menu_resume": "续传 / 重试",
        "menu_pause": "暂停",
        "menu_delete": "删除",
        "menu_clear": "清除已完成",
        "ctx_cut": "剪切",
        "ctx_copy": "复制",
        "ctx_paste": "粘贴",
        "ctx_select_all": "全选",
        "status_done": "完成",
        "status_paused": "已暂停",
        "status_downloading": "下载中",
        "status_error": "错误",
        "status_deleting": "删除中",
        "status_queued": "排队中",
        "status_starting": "准备下载",
        "status_finalizing": "整理中",
        "status_processing": "整理中",
        "msg_error": "错误",
        "msg_warning": "警告",
        "msg_already_running": "已有执行中的下载者，请先关闭旧窗口。",
        "msg_close_warn": "目前仍有下载任务进行中，确定要关闭程序吗？",
        "msg_ffmpeg_required_title": "需要 FFmpeg",
        "msg_ffmpeg_required_body": "此功能需要 FFmpeg。是否现在安装？",
        "msg_playlist_add_all": "共找到 {count} 个项目，是否全部加入下载清单？",
        "msg_fetch_anime1_empty": "Anime1 没有找到可下载的项目。",
        "msg_fetch_gimy_empty": "Gimy 没有找到可下载的项目。",
        "msg_fetch_movieffm_empty": "MovieFFM 没有找到可下载的项目。",
        "msg_fetch_anime1_failed": "Anime1 解析失败：{error}",
        "msg_fetch_gimy_failed": "Gimy 解析失败：{error}",
        "msg_fetch_movieffm_failed": "MovieFFM 解析失败：{error}",
        "msg_fetch_hanime_failed": "Hanime1 解析失败：{error}",
        "msg_text_read_failed": "读取文本文件失败：{error}",
        "msg_local_file_copied": "已复制：{name}",
        "msg_local_file_copy_speed": "本地复制",
        "msg_local_file_copy_failed": "复制本地文件失败：{error}",
        "msg_resume_name": "未完成项目",
        "msg_ffmpeg_window_title": "下载者 / 安装 FFmpeg",
        "msg_ffmpeg_mac_hint": "macOS 可使用 Homebrew 安装 FFmpeg。",
        "msg_ffmpeg_mac_status": "正在安装 FFmpeg...",
        "msg_ffmpeg_mac_button": "打开终端安装",
        "msg_ffmpeg_linux_hint": "Linux 请使用软件包管理器安装 FFmpeg。",
        "msg_ffmpeg_linux_command": "例如：sudo apt install ffmpeg",
        "msg_ffmpeg_close": "关闭",
        "msg_ffmpeg_win_hint": "正在下载 FFmpeg，请稍候...",
        "msg_ffmpeg_progress_download": "下载进度：{progress}",
        "msg_download_error": "下载错误：{error}",
        "msg_file_exists": "文件已存在",
        "err_no_extractor": "找不到可用解析器",
        "err_missing_json_url": "找不到 JSON 来源",
        "err_missing_mp4_source": "找不到 MP4 来源",
        "err_invalid_instagram_url": "Instagram 链接格式不正确",
        "err_api_status": "API 状态错误：{status}",
        "err_http_status": "HTTP 错误：{status}",
        "err_missav_bypass": "正在解析 MissAV...",
        "err_direct_fail": "直接下载失败，改用解析器重试...",
        "err_site_parse": "解析失败",
        "err_net": "连接不稳定，请稍后重试",
        "eta_processing": "整理中",
        "eta_file_exists": "文件已存在",
        "eta_direct_media": "正在下载直接媒体",
        "eta_site_jable": "正在解析 Jable...",
        "eta_site_njavtv": "正在解析 NJAVTV...",
        "eta_site_movieffm": "正在解析 MovieFFM...",
        "eta_site_gimy": "正在解析 Gimy...",
        "eta_site_hanime": "正在解析 Hanime1...",
        "eta_site_missav": "正在解析 MissAV...",
        "eta_site_threads": "正在解析 Threads...",
        "eta_site_instagram": "正在解析 Instagram...",
        "eta_site_twitter": "正在解析 Twitter/X...",
        "eta_found_stream": "已取得串流网址，准备下载",
        "eta_found_media": "已取得媒体网址，准备下载",
        "overview_idle": "待命中",
        "lang_name_zh_TW": "繁體中文",
        "lang_name_zh_CN": "简体中文",
        "lang_name_en_US": "English",
        "lang_name_ja_JP": "日本語",
    },
    "en_US": {
        "app_title": "Downloader",
        "subtitle": "Supports drag and drop, queueing, resume, and multi-site downloads",
        "basic_settings": "Basic Settings",
        "save_dir": "Default Save Directory:",
        "label_lang": "Language:",
        "browse": "Browse...",
        "new_url": "New Video URL:",
        "format_video": "Video",
        "format_audio": "MP3",
        "add_task": "Add to Download List",
        "impersonate_chk": "Enable browser impersonation for MissAV / Gimy",
        "chk_topmost": "Always on top",
        "list_frame": "Download Task List",
        "col_name": "Filename",
        "col_progress": "Progress",
        "col_size": "Size",
        "col_speed_eta": "Speed / ETA",
        "col_status": "Status",
        "menu_resume": "Resume / Retry",
        "menu_pause": "Pause",
        "menu_delete": "Delete",
        "menu_clear": "Clear Finished",
        "ctx_cut": "Cut",
        "ctx_copy": "Copy",
        "ctx_paste": "Paste",
        "ctx_select_all": "Select All",
        "status_done": "Done",
        "status_paused": "Paused",
        "status_downloading": "Downloading",
        "status_error": "Error",
        "status_deleting": "Deleting",
        "status_queued": "Queued",
        "status_starting": "Starting",
        "status_finalizing": "Finalizing",
        "status_processing": "Processing",
        "msg_error": "Error",
        "msg_warning": "Warning",
        "msg_already_running": "Downloader is already running.",
        "msg_close_warn": "Downloads are still in progress. Are you sure you want to close the app?",
        "msg_file_exists": "File already exists",
        "err_site_parse": "Parse failed",
        "eta_found_stream": "Stream URL acquired",
        "eta_found_media": "Media URL acquired",
        "overview_idle": "Idle",
        "lang_name_zh_TW": "Traditional Chinese",
        "lang_name_zh_CN": "Simplified Chinese",
        "lang_name_en_US": "English",
        "lang_name_ja_JP": "Japanese",
    },
    "ja_JP": {
        "app_title": "ダウンローダー",
        "subtitle": "ドラッグ＆ドロップ、キュー、再開、複数サイト対応",
        "basic_settings": "基本設定",
        "save_dir": "保存先：",
        "label_lang": "言語：",
        "browse": "参照...",
        "new_url": "新しい動画 URL：",
        "format_video": "動画",
        "format_audio": "MP3",
        "add_task": "ダウンロード一覧に追加",
        "impersonate_chk": "MissAV / Gimy にブラウザ偽装を使う",
        "chk_topmost": "最前面に表示",
        "list_frame": "ダウンロード一覧",
        "col_name": "ファイル名",
        "col_progress": "進捗",
        "col_size": "サイズ",
        "col_speed_eta": "速度 / 残り時間",
        "col_status": "状態",
        "menu_resume": "再開 / 再試行",
        "menu_pause": "一時停止",
        "menu_delete": "削除",
        "menu_clear": "完了を消去",
        "ctx_cut": "切り取り",
        "ctx_copy": "コピー",
        "ctx_paste": "貼り付け",
        "ctx_select_all": "すべて選択",
        "status_done": "完了",
        "status_paused": "一時停止",
        "status_downloading": "ダウンロード中",
        "status_error": "エラー",
        "status_deleting": "削除中",
        "status_queued": "待機中",
        "status_starting": "開始中",
        "status_finalizing": "整理中",
        "status_processing": "整理中",
        "msg_error": "エラー",
        "msg_warning": "警告",
        "msg_already_running": "既にダウンローダーが起動しています。",
        "msg_close_warn": "ダウンロード中のタスクがあります。終了してもよろしいですか？",
        "msg_file_exists": "ファイルは既に存在します",
        "err_site_parse": "解析失敗",
        "eta_found_stream": "ストリーム URL を取得しました",
        "eta_found_media": "メディア URL を取得しました",
        "overview_idle": "待機中",
        "lang_name_zh_TW": "繁体字中国語",
        "lang_name_zh_CN": "簡体字中国語",
        "lang_name_en_US": "英語",
        "lang_name_ja_JP": "日本語",
    },
}

for _lang, _entries in I18N_PATCH.items():
    I18N_DICT.setdefault(_lang, {})
    I18N_DICT[_lang].update(_entries)


SUPPORTED_LANGS = ("zh_TW", "zh_CN", "en_US", "ja_JP")
LANGUAGE_OPTIONS = {
    "zh_TW": "lang_name_zh_TW",
    "zh_CN": "lang_name_zh_CN",
    "en_US": "lang_name_en_US",
    "ja_JP": "lang_name_ja_JP",
}


def normalize_language_code(lang_code):
    if not lang_code:
        return None
    code = str(lang_code).strip().replace("-", "_")
    lower = code.lower()
    if lower in {"zh_tw", "zh_hk", "zh_mo", "zh_hant", "zh_hant_tw", "zh_hant_hk"}:
        return "zh_TW"
    if lower in {"zh_cn", "zh_sg", "zh_hans", "zh_hans_cn", "zh_hans_sg"}:
        return "zh_CN"
    if lower.startswith("ja"):
        return "ja_JP"
    if lower.startswith("en"):
        return "en_US"
    return None


def detect_default_language():
    candidates = []
    try:
        candidates.extend([locale.getlocale()[0], locale.getdefaultlocale()[0]])
    except Exception:
        pass
    candidates.extend(
        [
            os.environ.get("LC_ALL"),
            os.environ.get("LC_MESSAGES"),
            os.environ.get("LANG"),
            os.environ.get("LANGUAGE"),
        ]
    )
    for candidate in candidates:
        normalized = normalize_language_code(candidate)
        if normalized:
            return normalized
    return "en_US"


CURRENT_LANG = detect_default_language()


def language_display_name(lang_code):
    lang_code = normalize_language_code(lang_code) or "en_US"
    return t(LANGUAGE_OPTIONS.get(lang_code, "lang_name_en_US"))


class StopDownloadException(BaseException):
    """Control-flow exception used by the original downloader runtime."""


def t(key, **kwargs):
    text = I18N_DICT.get(CURRENT_LANG, {}).get(key, I18N_DICT.get("zh_TW", {}).get(key, key))
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text


def _contains_suspicious_text(text):
    if text is None:
        return False
    text = str(text)
    stripped = text.strip()
    if not stripped:
        return True
    question_ratio = stripped.count("?") / max(len(stripped), 1)
    return question_ratio > 0.4 or stripped in {"???", "??????"} or "�" in stripped


def _derive_task_name_from_url(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return ""
    parsed = urllib.parse.urlparse(normalized)
    if _is_anime1_category_url(normalized):
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        cat = query.get("cat", [""])[0]
        return f"{parsed.netloc} cat={cat}" if cat else parsed.netloc
    slug = parsed.path.rstrip("/").split("/")[-1]
    return slug or parsed.netloc


def _is_anime1_category_url(url):
    if not isinstance(url, str) or not url.strip():
        return False
    parsed = urllib.parse.urlparse(url.strip())
    host = parsed.netloc.lower()
    if "anime1.me" not in host and "anime1.pw" not in host:
        return False
    if "/category/" in parsed.path.lower():
        return True
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    return "cat" in query


def _is_anime1_episode_url(url):
    if not isinstance(url, str) or not url.strip():
        return False
    normalized = _normalize_download_url(url)
    if not normalized:
        return False
    return bool(re.match(r"^https://anime1\.(?:me|pw)/\d+/?$", normalized))


def _is_anime1_media_url(url_or_header):
    if not isinstance(url_or_header, str) or not url_or_header.strip():
        return False
    lowered = url_or_header.lower()
    return "anime1.me" in lowered or "anime1.pw" in lowered or ".v.anime1.me" in lowered


def _normalize_state_entry(entry):
    normalized = dict(entry) if isinstance(entry, dict) else {}
    url = _normalize_download_url(normalized.get("url", ""))
    name = str(normalized.get("name", "")).strip()
    if _is_anime1_category_url(url):
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        cat = query.get("cat", [""])[0]
        if cat:
            url = f"https://anime1.pw/?cat={cat}"
    if not name or _contains_suspicious_text(name):
        name = _derive_task_name_from_url(url)
    normalized["url"] = url
    normalized["name"] = name
    normalized["is_mp3"] = bool(normalized.get("is_mp3", False))
    source_site = str(normalized.get("source_site", "") or "").strip().lower() or None
    normalized["source_site"] = source_site
    fallback_urls = normalized.get("fallback_urls", [])
    normalized["fallback_urls"] = [u for u in fallback_urls if isinstance(u, str) and u.strip()] if isinstance(fallback_urls, list) else []
    normalized["source_page"] = _normalize_download_url(normalized.get("source_page", ""))
    return normalized


def _normalize_download_url(url):
    if not isinstance(url, str):
        return ""
    raw = html.unescape(url).strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if netloc in ("www.youtube.com", "youtube.com", "m.youtube.com") and parsed.path == "/watch":
        query_map = {}
        for key, value in query:
            query_map.setdefault(key, []).append(value)
        if query_map.get("v"):
            query = [(key, value) for key, value in query if key not in ("list", "index", "start_radio", "pp", "feature")]
    elif netloc == "youtu.be":
        query = [(key, value) for key, value in query if key not in ("list", "index", "start_radio", "pp", "feature")]
    normalized_query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunsplit((scheme, netloc, parsed.path, normalized_query, ""))


def _detect_browser_cookie_source():
    sources = _detect_browser_cookie_sources()
    return sources[0] if sources else None


def _detect_browser_cookie_sources():
    user_profiles = []
    env_user = os.environ.get("USERPROFILE", "")
    if env_user:
        user_profiles.append(env_user)
    try:
        users_root = Path("C:/Users")
        if users_root.is_dir():
            for child in users_root.iterdir():
                if child.is_dir():
                    user_profiles.append(str(child))
    except Exception:
        pass
    seen_profiles = []
    for profile in user_profiles:
        if profile and profile not in seen_profiles:
            seen_profiles.append(profile)
    candidates = []
    for profile in seen_profiles:
        candidates.extend(
            [
                ("firefox", os.path.join(profile, "AppData", "Roaming", "Mozilla", "Firefox", "Profiles")),
                ("edge", os.path.join(profile, "AppData", "Local", "Microsoft", "Edge", "User Data")),
                ("chrome", os.path.join(profile, "AppData", "Local", "Google", "Chrome", "User Data")),
            ]
        )
    results = []
    seen = set()
    for browser, path in candidates:
        try:
            key = (browser,)
            if path and os.path.isdir(path) and key not in seen:
                seen.add(key)
                results.append(key)
        except Exception:
            continue
    return results


def _extract_candidate_media_urls(text, allowed_exts=(".mp4", ".m3u8", ".mpd")):
    if not isinstance(text, str) or not text:
        return []
    normalized = html.unescape(text)
    normalized = normalized.replace("\\/", "/").replace("\\u002F", "/")
    normalized = normalized.replace("\\u0026", "&").replace("\\x3C", "<").replace("\\x3E", ">")
    candidates = []
    patterns = [
        r"https?://[^\s\"'<>\\]+",
        r"https:\\/\\/[^\s\"'<>\\]+",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, normalized):
            candidate = _normalize_download_url(match.replace("\\/", "/"))
            if any(ext in candidate.lower() for ext in allowed_exts):
                candidates.append(candidate)
    seen = set()
    deduped = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    return deduped


def _extract_json_script_blocks(text):
    if not isinstance(text, str) or not text:
        return []
    return re.findall(r"<script[^>]+type=[\"']application/json[\"'][^>]*>(.*?)</script>", text, re.IGNORECASE | re.DOTALL)


def _walk_media_urls(value, results):
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str):
                candidate = _normalize_download_url(item)
                if candidate and any(ext in candidate.lower() for ext in (".mp4", ".m3u8", ".mpd")):
                    results.append(candidate)
            _walk_media_urls(item, results)
    elif isinstance(value, list):
        for item in value:
            _walk_media_urls(item, results)


def _extract_instagram_media_candidates(page_html):
    candidates = _extract_candidate_media_urls(page_html)
    for blob in _extract_json_script_blocks(page_html):
        candidates.extend(_extract_candidate_media_urls(blob))
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        nested = []
        _walk_media_urls(parsed, nested)
        candidates.extend(nested)
    seen = set()
    final = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            final.append(candidate)
    return final


def _extract_instagram_media_via_savereels(url):
    c_req = get_curl_cffi_requests()
    session = c_req.Session(impersonate="chrome110")
    base_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://savereels.app",
        "Referer": "https://savereels.app/",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    resolve_resp = session.post(
        "https://savereels.app/api/resolve",
        headers=base_headers,
        data=json.dumps({"url": url, "media_type": "video"}),
        timeout=30,
    )
    if resolve_resp.status_code >= 400:
        raise Exception(f"SaveReels resolve HTTP {resolve_resp.status_code}")
    resolve_data = resolve_resp.json()
    preview = resolve_data.get("data") or {}

    token = str(resolve_data.get("download_token") or "").strip()
    if not token:
        raise Exception("SaveReels download token missing")
    download_resp = session.post(
        "https://savereels.app/api/download",
        headers=base_headers,
        data=json.dumps({"download_token": token}),
        timeout=30,
    )
    if download_resp.status_code >= 400:
        raise Exception(f"SaveReels download HTTP {download_resp.status_code}")
    task_id = str((download_resp.json() or {}).get("task_id") or "").strip()
    if not task_id:
        raise Exception("SaveReels task id missing")

    status_headers = {
        "Accept": "application/json",
        "Referer": "https://savereels.app/",
        "User-Agent": base_headers["User-Agent"],
    }
    last_message = ""
    for _ in range(15):
        status_resp = session.get(
            f"https://savereels.app/api/status/{task_id}",
            headers=status_headers,
            timeout=30,
        )
        if status_resp.status_code >= 400:
            raise Exception(f"SaveReels status HTTP {status_resp.status_code}")
        status_data = status_resp.json()
        status = str(status_data.get("status") or "").strip().lower()
        last_message = str(status_data.get("message") or "").strip()
        if status == "completed":
            video = status_data.get("video") or {}
            proxy_url = _normalize_download_url(status_data.get("download_url", ""))
            candidates = []
            for item in video.get("download_candidates") or []:
                if not isinstance(item, dict):
                    continue
                candidate = _normalize_download_url(item.get("url", ""))
                if candidate:
                    candidates.append(candidate)
            media_url = ""
            for candidate in candidates:
                if candidate.lower().endswith(".mp4"):
                    media_url = candidate
                    break
            if not media_url and candidates:
                media_url = candidates[0]
            if proxy_url:
                media_url = proxy_url
            if not media_url:
                raise Exception("SaveReels completed without media URL")
            page_title = str(video.get("author") or preview.get("author") or "").strip()
            return {
                "media_url": media_url,
                "fallback_urls": [u for u in candidates if u != media_url],
                "page_title": page_title,
            }
        if status == "failed":
            raise Exception(last_message or "SaveReels failed")
        time.sleep(1.5)
    raise Exception(last_message or "SaveReels timed out")


def _safe_header_url(url):
    try:
        parsed = urllib.parse.urlsplit(str(url or ""))
    except Exception:
        return str(url or "")
    if not parsed.scheme or not parsed.netloc:
        return str(url or "")
    safe_path = urllib.parse.quote(parsed.path or "/", safe="/%")
    safe_query = parsed.query
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, safe_path, safe_query, ""))


def _extract_facebook_graphql_payload(page_html):
    lsd_match = re.search(r'"lsd"\s*:\s*\{"name":"lsd","value":"([^"]+)"', page_html)
    query_match = re.search(
        r'FBReelsRootWithEntrypointQueryRelayPreloader_[^"]+","queryID":"(\d+)","variables":(\{.*?\}),"queryName":"FBReelsRootWithEntrypointQuery"',
        page_html,
        re.DOTALL,
    )
    if not lsd_match or not query_match:
        return None
    return {
        "lsd": lsd_match.group(1),
        "doc_id": query_match.group(1),
        "variables": query_match.group(2),
    }


def _extract_facebook_media_candidates(page_html):
    candidates = _extract_candidate_media_urls(page_html)
    for blob in _extract_json_script_blocks(page_html):
        candidates.extend(_extract_candidate_media_urls(blob))
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        nested = []
        _walk_media_urls(parsed, nested)
        candidates.extend(nested)
    seen = set()
    final = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            final.append(candidate)
    return final


def _extract_missav_m3u8_candidates(page_html):
    candidates = []
    direct_m = re.search(r'source="([^"]+\.m3u8[^"]*)"', page_html, re.IGNORECASE)
    if direct_m:
        candidates.append(html.unescape(direct_m.group(1)).strip())
    any_m = re.search(r'(https?://[^"\'\s]+\.m3u8[^"\'\s]*)', page_html)
    if any_m:
        candidates.append(html.unescape(any_m.group(1)).strip())
    packed_tokens_m = re.search(r"'(m3u8\|[^']+)'", page_html)
    packed_template_m = re.search(r"8://7\.6/5-4-3-2-1/d\.0", page_html)
    if packed_tokens_m and packed_template_m:
        tokens = packed_tokens_m.group(1).split("|")

        def decode_token(token):
            if token.isdigit():
                idx = int(token)
                if 0 <= idx < len(tokens):
                    return tokens[idx]
            return token

        if len(tokens) > 13:
            candidates.append(
                f"{tokens[8]}://{tokens[7]}.{tokens[6]}/{tokens[5]}-{tokens[4]}-{tokens[3]}-{tokens[2]}-{tokens[1]}/{tokens[13]}.{tokens[0]}"
            )
        for source_m in re.finditer(r"8://7\.6/5-4-3-2-1/a/([0-9]+)\.0", page_html):
            decoded = decode_token(source_m.group(1))
            if decoded:
                candidates.append(
                    f"{tokens[8]}://{tokens[7]}.{tokens[6]}/{tokens[5]}-{tokens[4]}-{tokens[3]}-{tokens[2]}-{tokens[1]}/{tokens[14]}/{decoded}.{tokens[0]}"
                )
    deduped = []
    seen = set()
    for candidate in candidates:
        normalized = _normalize_download_url(candidate)
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def save_state_entries(entries):
    _atomic_json_dump(STATE_FILE, entries)


def _atomic_json_dump(path, payload):
    path = os.path.abspath(path)
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, path)
    try:
        shutil.copyfile(path, f"{path}.bak")
    except Exception:
        pass


def _load_json_with_backup(path, default):
    for candidate in (path, f"{path}.bak"):
        if not os.path.exists(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            continue
    return default


def get_curl_cffi_requests():
    try:
        from curl_cffi import requests as c_req

        return c_req
    except Exception as exc:
        raise RuntimeError("curl_cffi is unavailable in this environment") from exc


class UIThrottler:
    def __init__(self, root, tree, update_interval=1.0):
        self.root = root
        self.tree = tree
        self.update_interval = update_interval
        self._last_updates = {}
        self._pending_updates = {}
        self._lock = threading.Lock()
        self._flush_scheduled = False

    def update(self, item_id, col, value, force=False):
        if col == "status":
            force = True
        now = time.time()
        with self._lock:
            last_map = self._last_updates.setdefault(item_id, {})
            last_val, last_time = last_map.get(col, (None, 0))
            if not force and value == last_val:
                return
            last_map[col] = (value, now)
            pending_map = self._pending_updates.setdefault(item_id, {})
            if not force and pending_map.get(col) == value:
                return
            pending_map[col] = value
            if self._flush_scheduled:
                return
            self._flush_scheduled = True
        try:
            self.root.after(120, self._flush_updates)
        except Exception:
            with self._lock:
                self._flush_scheduled = False
            return

    def _flush_updates(self):
        try:
            with self._lock:
                pending = self._pending_updates
                self._pending_updates = {}
                self._flush_scheduled = False
            for item_id, updates in pending.items():
                for col, value in updates.items():
                    try:
                        current = self.tree.set(item_id, column=col)
                        if current == value:
                            continue
                        self.tree.set(item_id, column=col, value=value)
                    except tk.TclError:
                        continue
        except Exception:
            return


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        data = _load_json_with_backup(CONFIG_FILE, {})
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_config(config):
    current = load_config()
    if isinstance(config, dict):
        current.update(config)
    _atomic_json_dump(CONFIG_FILE, current)


def load_state():
    if not os.path.exists(STATE_FILE):
        return []
    try:
        raw = _load_json_with_backup(STATE_FILE, [])
        if not isinstance(raw, list):
            return []
        return [_normalize_state_entry(entry) for entry in raw]
    except Exception:
        return []


def _match_forced_m3u8_site(url, task=None):
    task = task or {}
    normalized_url = _normalize_download_url(url)
    if not normalized_url.lower().endswith(".m3u8"):
        return None
    source_site = str(task.get("source_site", "") or "").strip().lower()
    if source_site in FORCED_M3U8_SITE_RULES:
        return source_site
    if task.get("fallback_urls"):
        return "movieffm"
    hostname = urllib.parse.urlparse(normalized_url).netloc.lower().split(":", 1)[0]
    for site_name, config in FORCED_M3U8_SITE_RULES.items():
        if any(hostname.endswith(host_suffix) for host_suffix in config["hosts"]):
            return site_name
    return None


def add_to_state(url, name, is_mp3=False, source_site=None, extra_task_data=None):
    normalized_url = _normalize_download_url(url)
    if not normalized_url:
        return
    extra_task_data = extra_task_data or {}
    with state_lock:
        state = load_state()
        if any(_normalize_download_url(entry.get("url", "")) == normalized_url for entry in state):
            return
        state.append(
            _normalize_state_entry(
                {
                    "url": normalized_url,
                    "name": name,
                    "is_mp3": is_mp3,
                    "source_site": source_site,
                    "fallback_urls": list(extra_task_data.get("fallback_urls", [])),
                    "source_page": extra_task_data.get("source_page", ""),
                }
            )
        )
        save_state_entries(state)


def remove_from_state(url):
    normalized_url = _normalize_download_url(url)
    with state_lock:
        state = [entry for entry in load_state() if _normalize_download_url(entry.get("url", "")) != normalized_url]
        save_state_entries(state)


def update_state_entry(url, **fields):
    normalized_url = _normalize_download_url(url)
    if not normalized_url:
        return
    with state_lock:
        state = load_state()
        changed = False
        for idx, entry in enumerate(state):
            if _normalize_download_url(entry.get("url", "")) != normalized_url:
                continue
            merged = dict(entry)
            merged.update(fields)
            normalized_entry = _normalize_state_entry(merged)
            if normalized_entry != entry:
                state[idx] = normalized_entry
                changed = True
        if changed:
            save_state_entries(state)


def replace_state_entries(entries):
    normalized_entries = []
    seen = set()
    for entry in entries or []:
        normalized = _normalize_state_entry(entry)
        normalized_url = _normalize_download_url(normalized.get("url", ""))
        if not normalized_url or normalized_url in seen:
            continue
        seen.add(normalized_url)
        normalized_entries.append(normalized)
    with state_lock:
        save_state_entries(normalized_entries)


def clean_ansi(text):
    if not isinstance(text, str):
        return ""
    return RE_ANSI_ESCAPE.sub("", text)


def summarize_error_message(exc, fallback_key, limit=120):
    message = clean_ansi(str(exc)).strip()
    if not message:
        return t(fallback_key)
    message = " ".join(message.split())
    return message[:limit]


def format_transfer_rate(bytes_per_second):
    try:
        value = max(float(bytes_per_second or 0), 0.0)
    except (TypeError, ValueError):
        return "0 B/s"
    if value >= 1024 * 1024:
        return f"{value / 1024 / 1024:.2f} MB/s"
    if value >= 1024:
        return f"{value / 1024:.2f} KB/s"
    return f"{value:.0f} B/s"


def format_eta(seconds):
    try:
        value = max(int(float(seconds or 0)), 0)
    except (TypeError, ValueError):
        return "--:--"
    hours, rem = divmod(value, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def format_progress_percent(downloaded_bytes, total_bytes, cap_at_99=True):
    try:
        downloaded = max(float(downloaded_bytes or 0), 0.0)
        total = max(float(total_bytes or 0), 0.0)
    except (TypeError, ValueError):
        return None
    if total <= 0:
        return None
    upper = 99.0 if cap_at_99 else 100.0
    return max(0.0, min((downloaded / total) * 100.0, upper))


def app_version_text():
    return APP_BUILD.split("-")[-1]


def app_title_text():
    return f"{t('app_title')} v{app_version_text()}"


def _warning_title_text_fallback():
    translated = t("msg_warning")
    return translated if translated != "msg_warning" else "警告"


def unpack_packed_javascript(text):
    if not text:
        return None
    packed_re = re.compile(
        r"eval\(function\(p,a,c,k,e,d\).*?return p}\('(?P<p>.*?)',(?P<a>\d+),(?P<c>\d+),'(?P<k>.*?)'\.split\('\|'\),0,\{}\)\)",
        re.S,
    )
    match = packed_re.search(text)
    if not match:
        return None

    payload = match.group("p")
    base = int(match.group("a"))
    tokens = match.group("k").split("|")

    def _to_base(value, radix):
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        if value == 0:
            return "0"
        out = ""
        while value:
            value, rem = divmod(value, radix)
            out = chars[rem] + out
        return out

    decoded = payload
    for idx in range(len(tokens) - 1, -1, -1):
        token = tokens[idx]
        if not token:
            continue
        key = _to_base(idx, base)
        decoded = re.sub(r"\b" + re.escape(key) + r"\b", token, decoded)
    return decoded


def default_short_name_for_url(url, is_mp3=False):
    short_name = url.split("/")[-1] if "/" in url else url
    if len(short_name) > 35:
        short_name = short_name[:35] + "..."
    if is_mp3:
        short_name = "[MP3] " + short_name
    return short_name


def is_auto_generated_short_name(url, short_name, is_mp3=False):
    if not short_name:
        return True
    return short_name == default_short_name_for_url(url, is_mp3=is_mp3)


def create_taiwan_map_icon():
    for name in ("taiwan_symbol_icon.png", "taiwan_satellite.png", "taiwan_outline.gif"):
        icon_path = Path(__file__).with_name(name)
        if not icon_path.exists():
            continue
        try:
            return tk.PhotoImage(file=str(icon_path))
        except Exception:
            continue
    return None


def write_error_log(context, exc, **extra):
    try:
        now = time.time()
        signature = (
            context,
            type(exc).__name__,
            str(exc),
            tuple((key, str(value)) for key, value in sorted(extra.items())),
        )
        last_seen = _ERROR_LOG_RECENT.get(signature)
        if last_seen and now - last_seen < ERROR_LOG_DEDUPE_WINDOW_SECONDS:
            return
        _ERROR_LOG_RECENT[signature] = now
        if len(_ERROR_LOG_RECENT) > 256:
            cutoff = now - ERROR_LOG_DEDUPE_WINDOW_SECONDS
            for stale_key in [key for key, ts in _ERROR_LOG_RECENT.items() if ts < cutoff]:
                _ERROR_LOG_RECENT.pop(stale_key, None)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            f"[{timestamp}] {context}",
            f"build: {APP_BUILD}",
            f"exception: {type(exc).__name__}: {exc}",
        ]
        for key, value in extra.items():
            lines.append(f"{key}: {value}")
        lines.append("traceback:")
        lines.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip())
        lines.append("--------------------------------------------------------------------------------")
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception:
        return


def has_local_ffmpeg_binaries():
    if platform.system() != "Windows":
        return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
    return os.path.exists(os.path.join(_APP_DIR, "ffmpeg.exe")) and os.path.exists(os.path.join(_APP_DIR, "ffprobe.exe"))


def install_ffmpeg_to_app_dir(progress_callback=None):
    if platform.system() != "Windows":
        raise RuntimeError("Automatic FFmpeg installation is only implemented for Windows.")
    targets = []
    ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe")
    ffprobe_path = os.path.join(_APP_DIR, "ffprobe.exe")
    if not os.path.exists(ffmpeg_path):
        targets.append((FFMPEG_WINDOWS_FFMPEG_URL, "ffmpeg.exe", os.path.join(_APP_DIR, "ffmpeg-download.zip")))
    if not os.path.exists(ffprobe_path):
        targets.append((FFMPEG_WINDOWS_FFPROBE_URL, "ffprobe.exe", os.path.join(_APP_DIR, "ffprobe-download.zip")))
    if not targets:
        if callable(progress_callback):
            progress_callback(100, "done")
        return True

    total_expected = 0
    for url, _, _ in targets:
        try:
            req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": DEFAULT_USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as res:
                total_expected += int(res.headers.get("Content-Length", "0") or 0)
        except Exception:
            pass

    downloaded_total = 0
    try:
        if callable(progress_callback):
            progress_callback(5, "prepare")
        for url, dest_name, zip_path in targets:
            req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
            with urllib.request.urlopen(req, timeout=120) as res, open(zip_path, "wb") as out_f:
                while True:
                    chunk = res.read(1024 * 1024)
                    if not chunk:
                        break
                    out_f.write(chunk)
                    downloaded_total += len(chunk)
                    if callable(progress_callback):
                        if total_expected > 0:
                            percent = min(80, max(5, int(downloaded_total * 80 / total_expected)))
                            progress_callback(percent, "download")
                        else:
                            mb_done = downloaded_total / (1024 * 1024)
                            progress_callback(5, f"download:{mb_done:.1f}MB")
            if not os.path.exists(zip_path) or os.path.getsize(zip_path) <= 0:
                raise FileNotFoundError(f"Downloaded archive missing or empty: {os.path.basename(zip_path)}")
            if callable(progress_callback):
                progress_callback(85, "extract")
            with zipfile.ZipFile(zip_path, "r") as zf:
                member_name = None
                for name in zf.namelist():
                    normalized = name.replace("\\", "/").lower()
                    if normalized.endswith("/" + dest_name.lower()) or normalized.endswith(dest_name.lower()):
                        member_name = name
                        break
                if not member_name:
                    raise FileNotFoundError(f"{dest_name} not found in {os.path.basename(zip_path)}")
                temp_dest = os.path.join(_APP_DIR, dest_name + ".download")
                final_dest = os.path.join(_APP_DIR, dest_name)
                with zf.open(member_name, "r") as src, open(temp_dest, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                os.replace(temp_dest, final_dest)
            try:
                os.remove(zip_path)
            except Exception:
                pass
        if not has_local_ffmpeg_binaries():
            raise FileNotFoundError("FFmpeg installation finished but binaries were not found in application directory")
        if callable(progress_callback):
            progress_callback(100, "done")
        return True
    finally:
        for _, _, zip_path in targets:
            try:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
            except Exception:
                pass


def acquire_single_instance_lock():
    global single_instance_mutex
    if platform.system() != "Windows":
        return True
    try:
        kernel32 = ctypes.windll.kernel32
        mutex_name = "Global\\AiTetsDownloaderSingleInstance"
        handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not handle:
            return True
        single_instance_mutex = handle
        already_exists = kernel32.GetLastError() == 183
        return not already_exists
    except Exception:
        return True


def make_context_menu(widget):
    menu = tk.Menu(widget, tearoff=0)
    menu.add_command(label=t("ctx_cut"), command=lambda: widget.event_generate("<<Cut>>"))
    menu.add_command(label=t("ctx_copy"), command=lambda: widget.event_generate("<<Copy>>"))
    menu.add_command(label=t("ctx_paste"), command=lambda: widget.event_generate("<<Paste>>"))
    menu.add_command(label=t("ctx_select_all"), command=lambda: widget.event_generate("<<SelectAll>>"))

    def show_menu(event):
        menu.tk_popup(event.x_root, event.y_root)

    widget.bind("<Button-3>", show_menu)
    return menu


class DownloadManagerApp:
    """Main desktop downloader application."""

    def __init__(self, root):
        self.root = root
        self.root.title(app_title_text())
        self.root.geometry("850x550")
        self.root.resizable(True, True)
        self.root.minsize(850, 550)
        try:
            self.root.attributes("-topmost", True)
        except Exception:
            pass
        self._app_icon = create_taiwan_map_icon()
        if self._app_icon is not None:
            try:
                self.root.iconphoto(True, self._app_icon)
            except Exception:
                pass
        self.root.configure(bg="#f4f7fb")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)
        self.tasks = {}
        self._m3u8_total_bytes_cache = {}
        self.config = load_config()
        global CURRENT_LANG
        CURRENT_LANG = detect_default_language()
        self.save_dir_var = tk.StringVar(value=self.config.get("save_dir", os.path.expanduser("~/Downloads")))
        self.format_var = tk.StringVar(value=t("format_video") if "format_video" in I18N_DICT.get(CURRENT_LANG, {}) else "Video")
        self.tree = None
        self.ui_throttler = None
        self.throttler = None
        self.url_entry = None
        self.tree_menu = None
        self.header_title_label = None
        self.settings_frame = None
        self.save_dir_label = None
        self.save_dir_entry = None
        self.browse_button = None
        self.new_url_label = None
        self.add_button = None
        self.topmost_checkbox = None
        self.list_frame = None
        self.format_dropdown = None
        self.action_buttons = {}
        self._ffmpeg_install_started = False
        self._last_state_persist_at = 0.0
        self._last_state_persist_signature = None
        self._resume_progress_cache = {}
        self._resume_progress_lock = threading.Lock()
        self._pending_status_styles = {}
        self._status_style_flush_scheduled = False
        self._summary_refresh_scheduled = False
        self._queue_process_scheduled = False
        self._last_overview_text = None
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.setup_ui()
        if self.tree is not None:
            self.ui_throttler = UIThrottler(self.root, self.tree, update_interval=1.0)
            self.throttler = self.ui_throttler
        self.resume_unfinished_tasks()
        self.root.after(400, self._auto_install_ffmpeg_if_missing)

    def _auto_install_ffmpeg_if_missing(self):
        if platform.system() != "Windows":
            return
        if self._ffmpeg_install_started:
            return
        if has_local_ffmpeg_binaries():
            return
        self._ffmpeg_install_started = True
        self.download_ffmpeg_interactive(None)

    def setup_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("vista")
        except Exception:
            pass
        style.configure("App.Treeview", rowheight=30, font=("Microsoft JhengHei UI", 10))
        style.configure("App.Treeview.Heading", font=("Microsoft JhengHei UI", 10, "bold"), padding=(8, 6))
        style.configure("App.TCombobox", padding=4)
        style.configure("App.Vertical.TScrollbar", arrowsize=14)
        style.configure("App.Horizontal.TScrollbar", arrowsize=14)

        header_frame = tk.Frame(self.root, bg="#f4f7fb")
        header_frame.grid(row=0, column=0, padx=20, pady=(16, 8), sticky="ew")
        header_frame.columnconfigure(0, weight=1)
        center_frame = tk.Frame(header_frame, bg="#f4f7fb")
        center_frame.grid(row=0, column=0, sticky="n")
        self.header_title_label = tk.Label(
            center_frame,
            text=app_title_text(),
            font=("Microsoft JhengHei UI", 18, "bold"),
            bg="#f4f7fb",
            fg="#123b5d",
            anchor="center",
            justify="center",
        )
        self.header_title_label.grid(row=0, column=0, sticky="ew")
        self.overview_var = tk.StringVar(value=t("overview_idle"))
        tk.Label(
            header_frame,
            textvariable=self.overview_var,
            font=("Microsoft JhengHei UI", 9, "bold"),
            bg="#ddebf7",
            fg="#224563",
            padx=10,
            pady=5,
            bd=1,
            relief="solid",
        ).grid(row=0, column=1, padx=(16, 0), sticky="ne")

        action_bar = tk.Frame(self.root, bg="#f4f7fb")
        action_bar.grid(row=1, column=0, padx=20, pady=(8, 0))

        def make_action_btn(text, command, bg):
            return tk.Button(
                action_bar,
                text=text,
                command=command,
                font=("Microsoft JhengHei UI", 9),
                bg=bg,
                fg="white",
                relief="flat",
                padx=12,
                pady=4,
                cursor="hand2",
            )

        self.action_buttons["resume"] = make_action_btn(t("menu_resume"), self.resume_selected, "#2e7dbe")
        self.action_buttons["resume"].pack(side="left", padx=(0, 8))
        self.action_buttons["pause"] = make_action_btn(t("menu_pause"), self.pause_selected, "#d7871f")
        self.action_buttons["pause"].pack(side="left", padx=(0, 8))
        self.action_buttons["delete"] = make_action_btn(t("menu_delete"), self.delete_selected, "#c94a4a")
        self.action_buttons["delete"].pack(side="left", padx=(0, 8))
        self.action_buttons["clear"] = make_action_btn(t("menu_clear"), self.clear_all_finished, "#607d8b")
        self.action_buttons["clear"].pack(side="left")

        settings_frame = tk.LabelFrame(
            self.root,
            text=t("basic_settings"),
            font=("Microsoft JhengHei UI", 9, "bold"),
            padx=12,
            pady=10,
            bg="#f8fbff",
            fg="#204a69",
            bd=1,
            relief="groove",
        )
        self.settings_frame = settings_frame
        settings_frame.grid(row=2, column=0, padx=20, pady=(8, 0), sticky="ew")
        settings_frame.columnconfigure(1, weight=1)

        self.save_dir_label = tk.Label(settings_frame, text=t("save_dir"), font=("Microsoft JhengHei UI", 9), bg="#f8fbff", fg="#24435b")
        self.save_dir_label.grid(row=0, column=0, sticky="w", pady=(4, 0))
        default_dir = self.config.get("save_dir", "")
        if not default_dir:
            default_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            os.makedirs(default_dir, exist_ok=True)
        self.save_dir_var.set(default_dir)
        self.save_dir_entry = tk.Entry(settings_frame, textvariable=self.save_dir_var, font=("Segoe UI", 9), relief="groove", bd=1)
        self.save_dir_entry.grid(row=0, column=1, sticky="ew", ipady=1)
        self.save_dir_entry.bind("<FocusOut>", lambda _event: self.persist_save_dir())
        self.save_dir_entry.bind("<Return>", lambda _event: self.persist_save_dir())
        self.browse_button = tk.Button(
            settings_frame,
            text=t("browse"),
            command=self.browse_folder,
            font=("Microsoft JhengHei UI", 9),
            bg="#e5eef7",
            fg="#163a59",
            relief="flat",
            padx=10,
            pady=4,
            cursor="hand2",
        )
        self.browse_button.grid(row=0, column=2, padx=(10, 0))

        input_frame = tk.Frame(settings_frame, bg="#f8fbff")
        input_frame.grid(row=1, column=0, columnspan=3, pady=(14, 0), sticky="ew")
        input_frame.columnconfigure(0, weight=1)
        self.new_url_label = tk.Label(input_frame, text=t("new_url"), font=("Microsoft JhengHei UI", 9), bg="#f8fbff", fg="#d96c00")
        self.new_url_label.grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.url_entry = tk.Entry(input_frame, font=("Segoe UI", 11), relief="groove", bd=1)
        self.url_entry.grid(row=1, column=0, sticky="ew", ipady=4)
        self.url_entry.bind("<Return>", lambda _event: self.add_new_download())
        make_context_menu(self.url_entry)
        self.format_dropdown = ttk.Combobox(
            input_frame,
            textvariable=self.format_var,
            values=[t("format_video"), t("format_audio")],
            state="readonly",
            width=10,
            font=("Segoe UI", 9),
            style="App.TCombobox",
        )
        self.format_dropdown.grid(row=1, column=1, padx=(12, 0))
        self.format_dropdown.current(0)
        self.add_button = tk.Button(
            input_frame,
            text=t("add_task"),
            font=("Microsoft JhengHei UI", 9, "bold"),
            bg="#1f8f5f",
            fg="white",
            activebackground="#19744d",
            activeforeground="white",
            relief="flat",
            command=self.add_new_download,
            padx=12,
            pady=5,
            cursor="hand2",
        )
        self.add_button.grid(row=1, column=2, padx=(12, 0), ipadx=6)
        self.topmost_var = tk.BooleanVar(value=True)

        def toggle_topmost():
            try:
                self.root.attributes("-topmost", bool(self.topmost_var.get()))
            except Exception:
                return

        self.topmost_checkbox = tk.Checkbutton(
            header_frame,
            text=t("chk_topmost"),
            variable=self.topmost_var,
            command=toggle_topmost,
            font=("Microsoft JhengHei UI", 9),
            fg="#4e5f6d",
            bg="#f4f7fb",
            activebackground="#f4f7fb",
        )
        self.topmost_checkbox.grid(row=1, column=1, padx=(16, 0), pady=(6, 0), sticky="ne")

        list_frame = tk.LabelFrame(
            self.root,
            text=t("list_frame"),
            font=("Microsoft JhengHei UI", 9, "bold"),
            bg="#f4f7fb",
            fg="#204a69",
            bd=1,
            relief="groove",
            padx=10,
            pady=10,
            name="list_frame",
        )
        self.list_frame = list_frame
        list_frame.grid(row=3, column=0, padx=20, pady=(12, 12), sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        columns = ("name", "progress", "size", "speed_eta", "status")
        self.tree = ttk.Treeview(list_frame, columns=columns, show="headings", style="App.Treeview")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.heading("name", text=t("col_name"))
        self.tree.heading("progress", text=t("col_progress"))
        self.tree.heading("size", text=t("col_size"))
        self.tree.heading("speed_eta", text=t("col_speed_eta"))
        self.tree.heading("status", text=t("col_status"))
        self.tree.column("name", width=440, anchor="w")
        self.tree.column("progress", width=90, anchor="center")
        self.tree.column("size", width=100, anchor="center")
        self.tree.column("speed_eta", width=180, anchor="center")
        self.tree.column("status", width=120, anchor="center")
        self.tree.tag_configure("row_downloading", background="#ecfdf5")
        self.tree.tag_configure("row_queued", background="#eff6ff")
        self.tree.tag_configure("row_paused", background="#fff7ed")
        self.tree.tag_configure("row_done", background="#f3f4f6")
        self.tree.tag_configure("row_finalizing", background="#f5f3ff")
        self.tree.tag_configure("row_error", background="#fef2f2")
        self._register_drop_targets(self.root)
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview, style="App.Vertical.TScrollbar")
        scrollbar.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL, command=self.tree.xview, style="App.Horizontal.TScrollbar")
        xscroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=scrollbar.set, xscrollcommand=xscroll.set)

        self.tree_menu = tk.Menu(self.root, tearoff=0)
        self.tree_menu.add_command(label=t("menu_resume"), command=self.resume_selected)
        self.tree_menu.add_command(label=t("menu_pause"), command=self.pause_selected)
        self.tree_menu.add_command(label=t("menu_delete"), command=self.delete_selected)
        self.tree_menu.add_separator()
        self.tree_menu.add_command(label=t("menu_clear"), command=self.clear_all_finished)
        self.tree.bind("<Button-3>", self.show_tree_menu)
        self.tree.bind("<Delete>", self._handle_delete_key)
        self.tree.bind("<BackSpace>", self._handle_delete_key)
        self.tree.bind("<Control-a>", self.select_all_tasks)
        self.tree.bind("<B1-Motion>", self.drag_select)
        self.tree.bind("<<TreeviewSelect>>", self._handle_tree_select)
        self._refresh_ui_summary()

    def _register_drop_targets(self, widget):
        if widget is None:
            return
        try:
            supported_types = [token for token in (DND_ALL, DND_TEXT, DND_FILES) if token]
            if supported_types and hasattr(widget, "drop_target_register"):
                widget.drop_target_register(*supported_types)
                widget.dnd_bind("<<Drop>>", self.handle_drop)
        except Exception:
            pass
        try:
            children = widget.winfo_children()
        except Exception:
            children = []
        for child in children:
            self._register_drop_targets(child)

    def drag_select(self, event):
        item_id = self.tree.identify_row(event.y)
        if item_id:
            self.tree.selection_add(item_id)

    def show_tree_menu(self, event):
        item_id = self.tree.identify_row(event.y)
        if item_id:
            if item_id not in self.tree.selection():
                self.tree.selection_set(item_id)
            self.tree_menu.tk_popup(event.x_root, event.y_root)

    def show_menu(self, event):
        self.show_tree_menu(event)

    def _selected_task_ids(self):
        selected = list(self.tree.selection()) if self.tree is not None else []
        if selected:
            return [item_id for item_id in selected if item_id in self.tasks]
        focused = self.tree.focus() if self.tree is not None else ""
        if focused and focused in self.tasks:
            return [focused]
        return []

    def select_all_tasks(self, event=None):
        if self.tree is None:
            return "break"
        item_ids = list(self.tree.get_children())
        if item_ids:
            self.tree.selection_set(item_ids)
            self._focus_tree_item(item_ids[0])
        self._refresh_ui_summary()
        return "break"

    def browse_folder(self):
        selected = filedialog.askdirectory(initialdir=self.save_dir_var.get() or _APP_DIR)
        if selected:
            self.save_dir_var.set(selected)
            self.persist_save_dir()

    def persist_save_dir(self):
        raw_value = self.save_dir_var.get().strip()
        if not raw_value:
            default_dir = os.path.join(os.path.expanduser("~"), "Downloads")
            self.save_dir_var.set(default_dir)
            raw_value = default_dir
        normalized = os.path.abspath(os.path.expanduser(raw_value))
        try:
            os.makedirs(normalized, exist_ok=True)
        except Exception:
            return
        self.save_dir_var.set(normalized)
        save_config({"save_dir": normalized})

    def refresh_language_ui(self):
        try:
            self.root.title(app_title_text())
        except Exception:
            pass
        if self.header_title_label is not None:
            self.header_title_label.configure(text=app_title_text())
        if self.settings_frame is not None:
            self.settings_frame.configure(text=t("basic_settings"))
        if self.save_dir_label is not None:
            self.save_dir_label.configure(text=t("save_dir"))
        if self.browse_button is not None:
            self.browse_button.configure(text=t("browse"))
        if self.new_url_label is not None:
            self.new_url_label.configure(text=t("new_url"))
        if self.add_button is not None:
            self.add_button.configure(text=t("add_task"))
        if self.topmost_checkbox is not None:
            self.topmost_checkbox.configure(text=t("chk_topmost"))
        if self.list_frame is not None:
            self.list_frame.configure(text=t("list_frame"))
        if self.format_dropdown is not None:
            values = [t("format_video"), t("format_audio")]
            self.format_dropdown.configure(values=values)
            if self.format_var.get() not in values:
                self.format_var.set(values[0])
        if self.tree is not None:
            self.tree.heading("name", text=t("col_name"))
            self.tree.heading("progress", text=t("col_progress"))
            self.tree.heading("size", text=t("col_size"))
            self.tree.heading("speed_eta", text=t("col_speed_eta"))
            self.tree.heading("status", text=t("col_status"))
        if self.tree_menu is not None:
            try:
                self.tree_menu.entryconfigure(0, label=t("menu_resume"))
                self.tree_menu.entryconfigure(1, label=t("menu_pause"))
                self.tree_menu.entryconfigure(2, label=t("menu_delete"))
                self.tree_menu.entryconfigure(4, label=t("menu_clear"))
            except Exception:
                pass
        if self.action_buttons:
            if self.action_buttons.get("resume") is not None:
                self.action_buttons["resume"].configure(text=t("menu_resume"))
            if self.action_buttons.get("pause") is not None:
                self.action_buttons["pause"].configure(text=t("menu_pause"))
            if self.action_buttons.get("delete") is not None:
                self.action_buttons["delete"].configure(text=t("menu_delete"))
            if self.action_buttons.get("clear") is not None:
                self.action_buttons["clear"].configure(text=t("menu_clear"))
        self._refresh_ui_summary()

    def resume_unfinished_tasks(self):
        saved_tasks = load_state()
        for task in saved_tasks:
            url = task.get("url")
            name = task.get("name", t("msg_resume_name") if "msg_resume_name" in I18N_DICT.get(CURRENT_LANG, {}) else "未完成項目")
            is_mp3 = task.get("is_mp3", False)
            source_site = task.get("source_site")
            extra_task_data = {
                "fallback_urls": list(task.get("fallback_urls", [])),
                "source_page": task.get("source_page"),
            }
            if not url:
                continue
            self._start_download_thread(
                url,
                name,
                is_mp3=is_mp3,
                source_site=source_site,
                extra_task_data=extra_task_data,
                resume_requested=True,
            )

    def add_new_download(self):
        if not self.url_entry:
            return
        self.persist_save_dir()
        new_url = self.url_entry.get().strip()
        if not new_url:
            self._show_warning(t("new_url"))
            return

        format_choice = self.format_var.get() if hasattr(self, "format_var") else ""
        is_mp3 = format_choice == t("format_audio")
        ffmpeg_ok = has_local_ffmpeg_binaries() if platform.system() == "Windows" else bool(shutil.which("ffmpeg"))
        if is_mp3 and not ffmpeg_ok:
            self.download_ffmpeg_interactive(lambda: self._final_add_download(new_url, is_mp3))
            return

        def fetch_anime1():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15)
                links_info = []
                for link, title in re.findall(r'<h2 class="entry-title"><a href="([^"]+)"[^>]*>(.*?)</a>', resp.text):
                    normalized_link = _normalize_download_url(urllib.parse.urljoin(new_url, html.unescape(link)))
                    if not _is_anime1_episode_url(normalized_link):
                        continue
                    clean_title = html.unescape(re.sub(r"<.*?>", "", title)).replace("&#8211;", "-").strip()
                    if not clean_title:
                        continue
                    links_info.append((normalized_link.rstrip("/"), clean_title))
                deduped_links = []
                seen_links = set()
                for link, title in links_info:
                    if link in seen_links:
                        continue
                    seen_links.add(link)
                    deduped_links.append((link, title))
                links_info = deduped_links
                if not links_info:
                    self._schedule_warning(t("msg_fetch_anime1_empty"))
                    return
                for link, title in reversed(links_info):
                    clean_title = html.unescape(title).replace("&#8211;", "-").strip()
                    self.root.after(
                        0,
                        lambda _link=link, _title=clean_title: self._final_add_download(
                            _link,
                            is_mp3,
                            _title,
                            source_site="anime1",
                            extra_task_data=self._build_extra_task_data(source_page=new_url),
                        ),
                    )
            except Exception as e:
                self._schedule_error(t("msg_fetch_anime1_failed", error=e))

        def fetch_gimy_detail():
            try:
                c_req = get_curl_cffi_requests()
                detail_headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": new_url}

                def gimy_fetch_detail_text(target_url):
                    last_exc = None
                    for impersonate_name in ("chrome110", "chrome120", "edge101"):
                        try:
                            resp_obj = c_req.get(target_url, impersonate=impersonate_name, timeout=15, headers=detail_headers)
                            return resp_obj.text
                        except Exception as inner_exc:
                            last_exc = inner_exc
                    try:
                        req = urllib.request.Request(target_url, headers=detail_headers)
                        with urllib.request.urlopen(req, timeout=20) as resp_obj:
                            return resp_obj.read().decode("utf-8", "ignore")
                    except Exception as fallback_exc:
                        raise fallback_exc from last_exc

                resp_text = gimy_fetch_detail_text(new_url)
                parsed = urllib.parse.urlparse(new_url)
                base = f"{parsed.scheme}://{parsed.netloc}"

                matches = list(
                    re.finditer(
                        r'href=[\"\'](/(?:(?:vod)?play/[0-9]+\-[0-9]+\-[0-9]+\.html|video/[0-9]+\-[0-9]+\.html(?:#sid=\d+)?|eps/[0-9]+\-[0-9]+(?:\-[0-9]+)?\.html))[\"\'][^>]*>(.*?)</a>',
                        resp_text,
                    )
                )
                if not matches:
                    self._schedule_warning(t("err_site_parse"))
                    return
                seen_title = set()
                seen_urls = set()
                drama_name = "Gimy"
                m_title = re.search(r"<title>(.*?)</title>", resp_text)
                if m_title:
                    drama_name = html.unescape(m_title.group(1)).split("-")[0].strip() or drama_name
                    drama_name = "".join(c for c in drama_name if c not in '\\/:*?"<>|')
                episodes = []
                for match in matches:
                    link = match.group(1)
                    title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
                    if not title:
                        continue
                    link_lower = link.lower()
                    title_lower = title.lower()
                    if "yu-gao" in link_lower or "預告" in title or "预告" in title or "trailer" in title_lower or "preview" in title_lower:
                        continue
                    normalized_title = re.sub(r"\s+", "", title)
                    if normalized_title in seen_title:
                        continue
                    ep_url = urllib.parse.urljoin(base, link)
                    if ep_url in seen_urls:
                        continue
                    seen_title.add(normalized_title)
                    seen_urls.add(ep_url)
                    full_name = " ".join(part for part in (drama_name, title) if part).strip()
                    episodes.append((ep_url, full_name))
                if not episodes:
                    self._schedule_warning("Gimy 此頁所有劇集目前播放失效")
                    return

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, episodes[0])
                    for ep_url, full_name in targets:
                        self._final_add_download(
                            ep_url,
                            is_mp3,
                            full_name,
                            "gimy",
                            self._build_extra_task_data(source_page=new_url),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as e:
                self._schedule_site_parse_error(e)

        def fetch_hanime_playlist():
            try:
                parsed = urllib.parse.urlparse(new_url)
                qs = urllib.parse.parse_qs(parsed.query)
                list_id = (qs.get("list") or [""])[0]
                if not list_id:
                    return
                pl_url = "https://hanime1.me/playlist?list=" + list_id
                c_req = get_curl_cffi_requests()
                resp = c_req.get(pl_url, impersonate="chrome110", timeout=15)
                seen = set()
                links = []
                for match in re.finditer(r'href=[\"\'](https?://hanime1\.me/watch\?v=[^\"\']+)[\"\']', resp.text):
                    link = match.group(1).replace("&amp;", "&").split("&list=", 1)[0]
                    if link in seen:
                        continue
                    seen.add(link)
                    links.append(link)
                if not links:
                    return

                def ask():
                    for link in self._choose_playlist_targets(links, links[0]):
                        self._final_add_download(link, is_mp3)

                self._schedule_ui_call(ask)
            except Exception as e:
                self._schedule_site_parse_error(e)

        def fetch_movieffm_drama():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                text = resp.text
                title_match = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
                drama_title = "MovieFFM"
                if title_match:
                    drama_title = html.unescape(title_match.group(1)).split("-")[0].strip() or drama_title
                    drama_title = "".join(c for c in drama_title if c not in '\\/:*?"<>|')
                episodes = []
                seen_urls = set()
                seen_names = set()
                episode_fallbacks = {}

                def normalize_episode_key(name_text):
                    cleaned = re.sub(r"\s+", " ", str(name_text or "").strip())
                    match = re.search(r"(\d+)$", cleaned)
                    if not match:
                        return cleaned.lower()
                    num = int(match.group(1))
                    return f"{num:02d}" if num < 100 else str(num)

                def add_episode(ep_url, ep_name, allow_direct_m3u8=False):
                    if not ep_url:
                        return
                    cleaned_url = html.unescape(str(ep_url)).replace("\\/", "/").strip()
                    cleaned_url = urllib.parse.urljoin(new_url, cleaned_url)
                    if not cleaned_url.startswith("http"):
                        return
                    if cleaned_url.lower().endswith(".m3u8") and not allow_direct_m3u8:
                        return
                    if cleaned_url in seen_urls:
                        return
                    cleaned_name = re.sub(r"<[^>]+>", "", html.unescape(str(ep_name or "")).strip())
                    cleaned_name = re.sub(r"\s+", " ", cleaned_name).strip()
                    if not cleaned_name:
                        tail = urllib.parse.urlparse(cleaned_url).path.rstrip("/").split("/")[-1]
                        cleaned_name = tail or t("msg_resume_name")
                    episode_key = normalize_episode_key(cleaned_name)
                    if re.fullmatch(r"\d+", cleaned_name):
                        cleaned_name = episode_key
                    name_key = episode_key
                    bucket = episode_fallbacks.setdefault(episode_key, [])
                    if cleaned_url not in bucket:
                        bucket.append(cleaned_url)
                    if name_key in seen_names:
                        return
                    full_name = f"{drama_title} {cleaned_name}".strip()
                    seen_urls.add(cleaned_url)
                    seen_names.add(name_key)
                    episodes.append((cleaned_url, full_name))

                for name, ep_url in re.findall(r'"name"\s*:\s*"([^"]+)"\s*,\s*"url"\s*:\s*"([^"]+)"', text):
                    add_episode(ep_url, name, allow_direct_m3u8=ep_url.lower().endswith(".m3u8"))
                for ep_url in re.findall(r'https://www\.movieffm\.net/[^"\']+', text):
                    if "/play/" in ep_url or "/vodplay/" in ep_url or "/episode/" in ep_url:
                        add_episode(ep_url, _derive_task_name_from_url(ep_url))
                if not episodes:
                    self._schedule_warning(t("msg_fetch_movieffm_empty"))
                    return

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, episodes[0])
                    for ep_url, ep_name in targets:
                        episode_key = normalize_episode_key(ep_name.rsplit(" ", 1)[-1])
                        fallback_urls = [u for u in episode_fallbacks.get(episode_key, []) if u != ep_url]
                        self._final_add_download(
                            ep_url,
                            is_mp3=is_mp3,
                            custom_name=ep_name,
                            source_site="movieffm",
                            extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=fallback_urls),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("movieffm parse failure", exc, url=new_url)
                self._schedule_error(t("msg_fetch_movieffm_failed", error=exc))

        def fetch_xiaoyakankan_post():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                text = resp.text
                title_match = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
                drama_title = "XiaoyaKankan"
                if title_match:
                    drama_title = html.unescape(title_match.group(1)).split("-")[0].strip() or drama_title
                    drama_title = "".join(c for c in drama_title if c not in '\\/:*?"<>|')
                match = re.search(r"var\s+pp\s*=\s*(\{.*?\})\s*;", text, re.DOTALL)
                if not match:
                    raise Exception("Failed to locate xiaoyakankan pp data")
                data = json.loads(match.group(1))
                lines = data.get("lines") or []
                if not isinstance(lines, list) or not lines:
                    raise Exception("Failed to locate xiaoyakankan lines")
                primary_entry = next((row for row in lines if isinstance(row, list) and len(row) >= 4 and isinstance(row[3], list) and row[3]), None)
                if not primary_entry:
                    raise Exception("Failed to locate xiaoyakankan episode list")

                parsed_url = urllib.parse.urlparse(new_url)
                parsed_qs = urllib.parse.parse_qs(parsed_url.query)
                current_vod_raw = (parsed_qs.get("vod") or [""])[0]
                current_episode_index = 0
                if "-" in current_vod_raw:
                    try:
                        current_episode_index = max(int(current_vod_raw.rsplit("-", 1)[-1]), 0)
                    except ValueError:
                        current_episode_index = 0
                base_page_url = urllib.parse.urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
                primary_key = str(primary_entry[0])
                primary_candidates = primary_entry[3]
                episode_count = len(primary_candidates)
                episodes = []
                for episode_index in range(episode_count):
                    chosen_urls = []
                    for row in lines:
                        if not isinstance(row, list) or len(row) < 4:
                            continue
                        row_key = str(row[0])
                        row_candidates = row[3]
                        if not isinstance(row_candidates, list) or episode_index >= len(row_candidates):
                            continue
                        candidate_url = _normalize_download_url(row_candidates[episode_index])
                        if candidate_url and candidate_url not in chosen_urls:
                            chosen_urls.append(candidate_url)
                    if not chosen_urls:
                        continue
                    episode_url = f"{base_page_url}?vod={primary_key}-{episode_index}"
                    episode_name = f"{drama_title} {episode_index + 1:02d}"
                    episodes.append((episode_url, episode_name, chosen_urls))
                if not episodes:
                    self._schedule_warning(t("msg_fetch_movieffm_empty"))
                    return

                def enqueue():
                    selected_episode = episodes[current_episode_index] if 0 <= current_episode_index < len(episodes) else episodes[0]
                    targets = self._choose_playlist_targets(episodes, selected_episode)
                    for episode_url, episode_name, chosen_urls in targets:
                        self._final_add_download(
                            episode_url,
                            is_mp3=is_mp3,
                            custom_name=episode_name,
                            source_site="xiaoyakankan",
                            extra_task_data=self._build_extra_task_data(source_page=base_page_url, fallback_urls=chosen_urls[1:]),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("xiaoyakankan series parse failure", exc, url=new_url)
                self._schedule_site_parse_error(exc, limit=120)

        lowered = new_url.lower()
        if _is_anime1_category_url(new_url):
            self._start_background_parse(fetch_anime1)
            return
        if "gimy" in lowered and ("/detail/" in lowered or "/voddetail/" in lowered or "/voddetail2/" in lowered or "/vod/" in lowered):
            self._start_background_parse(fetch_gimy_detail)
            return
        if "hanime1.me" in lowered and "list=" in lowered:
            self._start_background_parse(fetch_hanime_playlist)
            return
        if "movieffm.net" in lowered and "/drama/" in lowered:
            self._start_background_parse(fetch_movieffm_drama)
            return
        if "xiaoyakankan.com" in lowered and "/post/" in lowered:
            self._start_background_parse(fetch_xiaoyakankan_post)
            return
        self._final_add_download(new_url, is_mp3=is_mp3)
        return

    def _final_add_download(self, url, is_mp3, custom_name=None, source_site=None, extra_task_data=None):
        url = _normalize_download_url(url)
        if self._count_live_tasks() >= MAX_QUEUE_TASKS:
            self._show_warning(self._queue_limit_reached_text())
            self._clear_url_entry()
            return
        existing_item_id = self._find_existing_task_id(url, is_mp3)
        if existing_item_id:
            self._focus_tree_item(existing_item_id)
            self._clear_url_entry()
            return
        short_name = custom_name if custom_name else default_short_name_for_url(url, is_mp3=is_mp3)
        add_to_state(url, short_name, is_mp3, source_site=source_site, extra_task_data=extra_task_data)
        self._start_download_thread(url, short_name, is_mp3=is_mp3, source_site=source_site, extra_task_data=extra_task_data)
        self._clear_url_entry()

    def _find_existing_task_id(self, url, is_mp3):
        normalized_url = _normalize_download_url(url)
        if not normalized_url:
            return None
        for item_id, task in self.tasks.items():
            task_url = _normalize_download_url(task.get("url", ""))
            if task_url != normalized_url:
                continue
            if bool(task.get("is_mp3", False)) != bool(is_mp3):
                continue
            if task.get("state") == "DELETED":
                continue
            return item_id
        return None

    def _choose_playlist_targets(self, episodes, selected_episode=None):
        if not episodes:
            return []
        default_target = selected_episode if selected_episode in episodes else episodes[0]
        if len(episodes) > 1 and self._ask_warning_yesno(self._playlist_add_all_text(len(episodes))):
            return episodes
        return [default_target]

    def download_ffmpeg_interactive(self, on_complete_callback):
        dialog = tk.Toplevel(self.root)
        dialog.title(t("msg_ffmpeg_window_title"))
        dialog.geometry("400x160")
        dialog.transient(self.root)
        dialog.grab_set()
        sys_os = platform.system()

        if sys_os == "Darwin":
            tk.Label(dialog, text=t("msg_ffmpeg_mac_hint"), font=("Microsoft JhengHei", 10)).pack(pady=10)
            status_label = tk.Label(dialog, text=t("msg_ffmpeg_mac_status"), font=("Arial", 9))
            status_label.pack(pady=5)

            def run_brew():
                try:
                    subprocess.run(["brew", "install", "ffmpeg"], check=False)
                finally:
                    dialog.after(0, dialog.destroy)
                    if callable(on_complete_callback):
                        dialog.after(0, on_complete_callback)

            tk.Button(dialog, text=t("msg_ffmpeg_mac_button"), command=lambda: threading.Thread(target=run_brew, daemon=True).start()).pack(pady=10)
            return

        if sys_os == "Linux":
            tk.Label(dialog, text=t("msg_ffmpeg_linux_hint"), font=("Microsoft JhengHei", 10)).pack(pady=10)
            tk.Label(dialog, text=t("msg_ffmpeg_linux_command"), font=("Arial", 10)).pack(pady=20)
            tk.Button(dialog, text=t("msg_ffmpeg_close"), command=dialog.destroy).pack(pady=15)
            return

        tk.Label(dialog, text=t("msg_ffmpeg_win_hint"), font=("Microsoft JhengHei", 10)).pack(pady=10)
        progress = ttk.Progressbar(dialog, orient="horizontal", length=300, mode="determinate")
        progress.pack(pady=15)
        progress_label = tk.Label(dialog, text=t("msg_ffmpeg_progress_download", progress="0%"), font=("Arial", 9))
        progress_label.pack(pady=5)

        def _dl_thread():
            try:
                def _progress_callback(percent, phase):
                    if phase == "prepare":
                        text = "Preparing FFmpeg download..."
                    elif phase == "extract":
                        text = "Extracting FFmpeg..."
                    elif phase == "done":
                        text = t("msg_ffmpeg_progress_download", progress="100%")
                    elif isinstance(phase, str) and phase.startswith("download:"):
                        text = f"Downloading FFmpeg... {phase.split(':', 1)[1]}"
                    else:
                        text = t("msg_ffmpeg_progress_download", progress=f"{percent}%")
                    dialog.after(0, progress.configure, {"value": percent})
                    dialog.after(0, progress_label.configure, {"text": text})

                install_ffmpeg_to_app_dir(progress_callback=_progress_callback)
                dialog.after(0, progress.configure, {"value": 100})
                dialog.after(0, progress_label.configure, {"text": t("msg_ffmpeg_progress_download", progress="100%")})
                dialog.after(0, dialog.destroy)
                if callable(on_complete_callback):
                    dialog.after(0, on_complete_callback)
            except Exception as e:
                write_error_log("ffmpeg auto install failure", e, ffmpeg_url=FFMPEG_WINDOWS_FFMPEG_URL, ffprobe_url=FFMPEG_WINDOWS_FFPROBE_URL, app_dir=_APP_DIR)
                dialog.after(0, lambda: progress_label.configure(text=str(e)))

        self._start_daemon_thread(_dl_thread)

    def handle_drop(self, event):
        data = str(getattr(event, "data", ""))
        parsed_items = []
        for raw in re.split(r"[\r\n]+", data):
            candidate = raw.strip().strip("{}")
            if not candidate:
                continue
            if candidate.startswith("file:///"):
                try:
                    parsed = urllib.parse.urlsplit(candidate)
                    local_path = urllib.request.url2pathname(parsed.path)
                    if os.name == "nt" and re.match(r"^/[A-Za-z]:", local_path):
                        local_path = local_path[1:]
                    parsed_items.append(local_path)
                    continue
                except Exception:
                    pass
            parsed_items.append(candidate)
        urls = []
        for candidate in parsed_items:
            urls.extend(re.findall(r"https?://[^\s{}]+", candidate))
        temp_added = False
        for url in set(urls):
            self._set_url_entry(url)
            self.add_new_download()
            temp_added = True
        if temp_added:
            return

        try:
            paths = self.root.tk.splitlist(data)
        except Exception:
            paths = [data] if data else []

        for path in paths:
            path_str = str(path)
            if not os.path.isfile(path_str):
                continue

            if path_str.endswith(".txt"):
                raw_data = b""
                try:
                    with open(path_str, "rb") as f:
                        raw_data = f.read()
                except Exception as e:
                    self._show_error(t("msg_text_read_failed", error=str(e)))
                    continue
                lines = []
                for enc in ("utf-8", "utf-16", "utf-16le", "big5", "mbcs"):
                    try:
                        lines = raw_data.decode(enc).splitlines()
                        break
                    except Exception:
                        continue
                for line in lines:
                    l = line.strip()
                    if not l.startswith("http"):
                        continue
                    self._set_url_entry(l)
                    self.add_new_download()
                continue

            if path_str.endswith(".url") or path_str.endswith(".webloc"):
                raw_data = b""
                try:
                    with open(path_str, "rb") as f:
                        raw_data = f.read()
                except Exception:
                    raw_data = b""
                detected_url = None
                for enc in ("utf-8", "utf-16", "utf-16le", "big5", "mbcs"):
                    try:
                        text = raw_data.decode(enc)
                    except Exception:
                        continue
                    for line in text.splitlines():
                        l = line.strip()
                        if l.upper().startswith("URL="):
                            detected_url = l[4:]
                            break
                        if "<string>http" in l:
                            m = re.search(r"<string>(https?://.*?)</string>", l)
                            if m:
                                detected_url = m.group(1)
                                break
                    if detected_url:
                        break
                if detected_url:
                    self._set_url_entry(detected_url)
                    self.add_new_download()
                continue

            ext = os.path.splitext(path_str)[1].lower()
            if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".webm", ".mov", ".mp3", ".m4a"):
                continue
            try:
                dest = os.path.join(self.save_dir_var.get(), os.path.basename(path_str))
                if os.path.abspath(path_str) != os.path.abspath(dest):
                    shutil.copy2(path_str, dest)
                    copied_id = self.tree.insert(
                        "",
                        tk.END,
                        values=(
                            t("msg_local_file_copied", name=os.path.basename(path_str)),
                            "100%",
                            "-",
                            t("msg_local_file_copy_speed"),
                            t("status_done"),
                        ),
                    )
                    self._apply_row_status_style(copied_id, t("status_done"))
            except Exception as e:
                self._show_error(t("msg_local_file_copy_failed", error=str(e)))

    def _start_download_thread(self, url, short_name, existing_item_id=None, is_mp3=False, source_site=None, extra_task_data=None, resume_requested=False):
        extra_task_data = extra_task_data or {}
        item_id = existing_item_id
        if item_id is None:
            item_id = self.tree.insert("", tk.END, values=(short_name, "0.0%", "-", "-", t("status_queued")))
            self._apply_row_status_style(item_id, t("status_queued"))

            def fetch_title():
                if " " in short_name:
                    return
                if ".html" not in short_name and ".php" not in short_name and len(short_name) <= 15:
                    return
                try:
                    c_req = get_curl_cffi_requests()
                    resp = c_req.head(url, impersonate="chrome110", timeout=5)
                    if "text/html" not in (resp.headers.get("Content-Type", "") or "").lower():
                        return
                    resp = c_req.get(url, impersonate="chrome110", timeout=5)
                    txt = resp.text
                except Exception:
                    try:
                        req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT}, method="HEAD")
                        resp = urllib.request.urlopen(req, timeout=5)
                        if "text/html" not in (resp.headers.get("Content-Type", "") or "").lower():
                            return
                        req.method = "GET"
                        txt = urllib.request.urlopen(req, timeout=5).read(102400).decode("utf-8", "ignore")
                    except Exception:
                        return
                m = re.search(r"<title>(.*?)</title>", txt, re.IGNORECASE)
                if not m:
                    return
                title = html.unescape(m.group(1)).split("-")[0].strip()
                title = "".join(c for c in title if c not in '\\/:*?"<>|')
                if title and self.tasks.get(item_id, {}).get("state") == "QUEUED":
                    self.tasks[item_id]["short_name"] = title
                    self._schedule_tree_update(item_id, "name", title)

            self._start_daemon_thread(fetch_title)
        else:
            self._set_task_queued_ui(item_id)
            if source_site is None:
                source_site = self.tasks.get(item_id, {}).get("source_site")
        task_data = {
            "url": url,
            "state": "QUEUED",
            "filename": None,
            "temp_filename": None,
            "short_name": short_name,
            "is_mp3": bool(is_mp3),
            "source_site": source_site,
        }
        if extra_task_data:
            task_data.update(extra_task_data)
        task_data["fallback_urls"] = list(task_data.get("fallback_urls", []))
        task_data["source_page"] = _normalize_download_url(task_data.get("source_page", ""))
        task_data["resume_requested"] = bool(existing_item_id is not None or resume_requested)
        task_data["_stop_reason"] = None
        self.tasks[item_id] = task_data
        try:
            self.persist_unfinished_state()
        except Exception:
            pass
        self._schedule_summary_refresh()
        self._schedule_process_queue()

    def update_tree(self, item_id, col, value, force=False):
        throttler = self.ui_throttler or self.throttler
        if throttler and self.tree is not None:
            throttler.update(item_id, col, value, force=force)
            if col == "status":
                self._schedule_status_style(item_id, value)
                self._schedule_summary_refresh()

    def _schedule_status_style(self, item_id, status_text):
        self._pending_status_styles[item_id] = status_text
        if self._status_style_flush_scheduled:
            return
        self._status_style_flush_scheduled = True
        try:
            self.root.after(120, self._flush_status_styles)
        except Exception:
            self._status_style_flush_scheduled = False

    def _flush_status_styles(self):
        pending = self._pending_status_styles
        self._pending_status_styles = {}
        self._status_style_flush_scheduled = False
        for item_id, status_text in pending.items():
            self._apply_row_status_style(item_id, status_text)

    def _schedule_summary_refresh(self):
        if self._summary_refresh_scheduled:
            return
        self._summary_refresh_scheduled = True
        try:
            self.root.after(180, self._flush_summary_refresh)
        except Exception:
            self._summary_refresh_scheduled = False

    def _flush_summary_refresh(self):
        self._summary_refresh_scheduled = False
        self._refresh_ui_summary()

    def _schedule_process_queue(self, delay=0):
        if self._queue_process_scheduled:
            return
        self._queue_process_scheduled = True
        try:
            self.root.after(delay, self._flush_process_queue)
        except Exception:
            self._queue_process_scheduled = False

    def _flush_process_queue(self):
        self._queue_process_scheduled = False
        self._process_queue()

    def _apply_row_status_style(self, item_id, status_text):
        try:
            status_text = str(status_text or "")
            if status_text == t("status_downloading"):
                tags = ("row_downloading",)
            elif status_text == t("status_queued") or status_text == t("status_starting"):
                tags = ("row_queued",)
            elif status_text == t("status_paused"):
                tags = ("row_paused",)
            elif status_text == t("status_done"):
                tags = ("row_done",)
            elif status_text == t("status_finalizing"):
                tags = ("row_finalizing",)
            elif status_text == t("status_error") or "錯" in status_text or "Error" in status_text:
                tags = ("row_error",)
            else:
                tags = ()
            if tuple(self.tree.item(item_id, "tags")) == tuple(tags):
                return
            self.tree.item(item_id, tags=tags)
        except tk.TclError:
            return

    def _refresh_ui_summary(self):
        counts = self._collect_state_counts()
        downloading = counts["DOWNLOADING"]
        queued = counts["QUEUED"]
        paused = counts["PAUSED"]
        errors = counts["ERROR"]
        if hasattr(self, "overview_var"):
            parts = []
            if downloading:
                parts.append(f"{t('status_downloading')} {downloading}")
            if queued:
                parts.append(f"{t('status_queued')} {queued}")
            if paused:
                parts.append(f"{t('status_paused')} {paused}")
            if errors:
                parts.append(f"{t('status_error')} {errors}")
            overview_text = " | ".join(parts) if parts else t("overview_idle")
            if overview_text == self._last_overview_text:
                return
            self._last_overview_text = overview_text
            self.overview_var.set(overview_text)

    def _remove_partial_output(self, item_id):
        task = self.tasks.get(item_id, {})
        for key in ("filename", "temp_filename"):
            filename = task.get(key)
            if not filename:
                continue
            try:
                if os.path.isfile(filename):
                    os.remove(filename)
            except OSError:
                pass
            if key != "temp_filename":
                continue
            sidecars = [filename + ".resume", filename + ".merged", filename + ".progress.json"]
            root, ext = os.path.splitext(filename)
            if ext:
                sidecars.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
            for sidecar in sidecars:
                try:
                    if os.path.isfile(sidecar):
                        os.remove(sidecar)
                except OSError:
                    continue

    def _mark_task_finished(self, item_id):
        task = self.tasks.get(item_id)
        if not task:
            return
        filename = task.get("filename")
        self._update_task_size_from_file(item_id, filename)
        self._set_task_finished_ui(item_id)
        self._finalize_completed_task(task)

    def _discard_task(self, item_id):
        self._cleanup_temp_files(item_id)
        self._remove_partial_output(item_id)
        self.tasks.pop(item_id, None)

    def _delete_tree_item(self, item_id):
        try:
            self.tree.delete(item_id)
            self._schedule_summary_refresh()
        except tk.TclError:
            return
        except Exception:
            return

    def _handle_stopped_download(self, item_id):
        task = self.tasks.get(item_id, {})
        state = task.get("state")
        if state in ("DELETED", "DELETE_REQUESTED"):
            self._discard_task(item_id)
            return
        if state == "PAUSE_REQUESTED":
            self._set_task_paused_ui(item_id)
            task["state"] = "PAUSED"
            task["_stop_reason"] = None
            return

    def _is_live_task(self, task):
        return task.get("state") not in TERMINAL_TASK_STATES

    def _iter_live_tasks(self):
        for task in self.tasks.values():
            if self._is_live_task(task):
                yield task

    def _collect_state_counts(self):
        counts = {"DOWNLOADING": 0, "QUEUED": 0, "PAUSED": 0, "ERROR": 0}
        for task in self.tasks.values():
            state = task.get("state")
            if state == "DOWNLOADING":
                counts["DOWNLOADING"] += 1
            elif state == "QUEUED":
                counts["QUEUED"] += 1
            elif state in PAUSED_TASK_STATES:
                counts["PAUSED"] += 1
            elif state == "ERROR":
                counts["ERROR"] += 1
        return counts

    def _downloading_status_text(self):
        return t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中"

    def _warning_title_text(self):
        return _warning_title_text_fallback()

    def _show_warning(self, message, parent=None):
        messagebox.showwarning(self._warning_title_text(), message, parent=parent)

    def _schedule_warning(self, message, parent=None):
        self._schedule_ui_call(lambda msg=message, target=parent: self._show_warning(msg, parent=target))

    def _ask_warning_yesno(self, message, parent=None):
        return messagebox.askyesno(self._warning_title_text(), message, parent=parent)

    def _error_title_text(self):
        translated = t("msg_error")
        return translated if translated != "msg_error" else "錯誤"

    def _show_error(self, message, parent=None):
        messagebox.showerror(self._error_title_text(), message, parent=parent)

    def _schedule_error(self, message, parent=None):
        self._schedule_ui_call(lambda msg=message, target=parent: self._show_error(msg, parent=target))

    def _queue_limit_reached_text(self):
        return f"Queue limit reached ({MAX_QUEUE_TASKS})."

    def _playlist_add_all_text(self, count):
        return t("msg_playlist_add_all", count=count)

    def _clear_url_entry(self):
        self.url_entry.delete(0, tk.END)

    def _set_url_entry(self, value):
        self._clear_url_entry()
        self.url_entry.insert(0, value)

    def _start_daemon_thread(self, target, *args, **kwargs):
        threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True).start()

    def _start_background_parse(self, target):
        self._start_daemon_thread(target)
        self._clear_url_entry()

    def _schedule_ui_call(self, callback):
        self.root.after(0, callback)

    def _schedule_tree_update(self, item_id, col, value, force=False):
        self._schedule_ui_call(
            lambda _item_id=item_id, _col=col, _value=value, _force=force: self.update_tree(_item_id, _col, _value, force=_force)
        )

    def _handle_delete_key(self, _event=None):
        self.delete_selected()

    def _handle_tree_select(self, _event=None):
        self._refresh_ui_summary()

    def _focus_tree_item(self, item_id):
        if not self.tree or not item_id:
            return
        self.tree.selection_set(item_id)
        self.tree.focus(item_id)
        self.tree.see(item_id)

    def _build_extra_task_data(self, source_page=None, fallback_urls=None):
        data = {}
        if source_page:
            data["source_page"] = source_page
        if fallback_urls:
            data["fallback_urls"] = list(fallback_urls)
        return data

    def _is_stop_requested_state(self, state):
        return state in STOP_REQUEST_TASK_STATES

    def _is_delete_requested_state(self, state):
        return state == "DELETE_REQUESTED"

    def _is_pause_requested_state(self, state):
        return state == "PAUSE_REQUESTED"

    def _count_tasks_in_states(self, *states):
        counts = self._collect_state_counts()
        total = 0
        for state in states:
            if state == "PAUSE_REQUESTED":
                total += counts["PAUSED"]
            elif state in counts:
                total += counts[state]
        return total

    def _build_persistable_task_snapshot(self):
        entries = []
        signature_parts = []
        entries_append = entries.append
        signature_append = signature_parts.append
        normalize_url = _normalize_download_url
        default_name = default_short_name_for_url
        for task in self._iter_live_tasks():
            url = normalize_url(task.get("url", ""))
            if not url:
                continue
            is_mp3 = bool(task.get("is_mp3", False))
            name = task.get("short_name") or task.get("name") or default_name(url, is_mp3=is_mp3)
            source_site = task.get("source_site") or ""
            fallback_urls = tuple(task.get("fallback_urls", []) or [])
            source_page = task.get("source_page", "")
            entries_append(
                {
                    "url": url,
                    "name": name,
                    "is_mp3": is_mp3,
                    "source_site": source_site or None,
                    "fallback_urls": list(fallback_urls),
                    "source_page": source_page,
                }
            )
            signature_append((url, name, is_mp3, source_site, fallback_urls, source_page))
        return entries, tuple(signature_parts)

    def _mark_existing_file_complete(self, item_id, message):
        task = self.tasks.get(item_id)
        filename = task.get("filename") if task else ""
        self._update_task_size_from_file(item_id, filename)
        self._set_task_finished_ui(item_id, message)
        if task:
            self._finalize_completed_task(task, clear_resume_requested=True)

    def _count_live_tasks(self):
        count = 0
        for _ in self._iter_live_tasks():
            count += 1
        return count

    def _complete_if_output_exists(self, item_id):
        task = self.tasks.get(item_id, {})
        short_name = task.get("short_name", "")
        if not short_name or short_name == "Queued":
            return False
        if task.get("resume_requested"):
            return False
        save_dir = self.save_dir_var.get()
        possible_exts = [".mp4", ".mkv", ".webm", ".mp3", ".m4a"]
        message = t("msg_file_exists") if "msg_file_exists" in I18N_DICT.get(CURRENT_LANG, {}) else "檔案已存在"
        safe_name = re.sub(r'[\\\\/:*?"<>|]+', "_", short_name).strip()
        for ext in possible_exts:
            if os.path.exists(os.path.join(save_dir, f"{safe_name}{ext}")):
                self._mark_existing_file_complete(item_id, message)
                return True
        return False

    def _extract_domain(self, url):
        domain = urllib.parse.urlparse(url).netloc
        if not domain:
            domain = url.split("/")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    def _get_task_queue_keys(self, task):
        return (
            self._extract_domain(task.get("url", "")),
            _normalize_download_url(task.get("source_page", "")),
        )

    def _should_use_impersonation(self, url, source_site=None):
        site = str(source_site or "").strip().lower()
        if site in IMPERSONATION_SITE_MARKERS:
            return True
        domain = self._extract_domain(url)
        return any(marker in domain for marker in IMPERSONATION_SITE_MARKERS)

    def _request_task_deletion(self, item_id, state):
        self.tasks[item_id]["_stop_reason"] = STOP_REASON_DELETE
        self.tasks[item_id]["state"] = "DELETE_REQUESTED"
        try:
            self._cleanup_temp_files(item_id)
            if state in DELETE_CLEANUP_TASK_STATES:
                self._remove_partial_output(item_id)
            self._delete_tree_item(item_id)
            if state in DELETE_CLEANUP_TASK_STATES:
                self.tasks.pop(item_id, None)
            self._schedule_process_queue()
        except Exception:
            return

    def _delete_finished_task(self, item_id):
        try:
            self._cleanup_temp_files(item_id)
            self._delete_tree_item(item_id)
            self.tasks.pop(item_id, None)
        except Exception:
            return

    def _get_m3u8_duration(self, url, headers=None):
        c_req = get_curl_cffi_requests()
        headers = {
            "Referer": (headers or {}).get("Referer"),
            "Origin": (headers or {}).get("Origin"),
        }
        resp = c_req.get(url, impersonate="chrome110", timeout=15, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"HTTP {resp.status_code}")
        text = resp.text
        if "#EXT-X-STREAM-INF" in text:
            variant = None
            lines = [line.strip() for line in text.splitlines()]
            for idx, line in enumerate(lines):
                if not line.startswith("#EXT-X-STREAM-INF"):
                    continue
                if idx + 1 >= len(lines):
                    continue
                candidate = lines[idx + 1].strip()
                if not candidate or candidate.startswith("#"):
                    continue
                variant = urllib.parse.urljoin(url, candidate)
                break
            if variant:
                resp = c_req.get(variant, impersonate="chrome110", timeout=15, headers=headers)
                if resp.status_code != 200:
                    raise Exception(f"HTTP {resp.status_code}")
                text = resp.text
        total_seconds = 0.0
        for value in re.findall(r"#EXTINF:([0-9.]+)", text):
            try:
                total_seconds += float(value)
            except ValueError:
                continue
        return total_seconds

    def _estimate_m3u8_media_bps(self, url, headers=None):
        c_req = get_curl_cffi_requests()
        headers = {
            "Referer": (headers or {}).get("Referer"),
            "Origin": (headers or {}).get("Origin"),
        }
        try:
            resp = c_req.get(url, impersonate="chrome110", timeout=15, headers=headers)
            if resp.status_code != 200:
                return None
            text = resp.text
            playlist_url = url
            if "#EXT-X-STREAM-INF" in text:
                variant = None
                lines = [line.strip() for line in text.splitlines()]
                for idx, line in enumerate(lines):
                    if not line.startswith("#EXT-X-STREAM-INF"):
                        continue
                    if idx + 1 >= len(lines):
                        continue
                    candidate = lines[idx + 1].strip()
                    if not candidate or candidate.startswith("#"):
                        continue
                    variant = urllib.parse.urljoin(url, candidate)
                    break
                if variant:
                    playlist_url = variant
                    resp = c_req.get(variant, impersonate="chrome110", timeout=15, headers=headers)
                    if resp.status_code != 200:
                        return None
                    text = resp.text
            lines = [line.strip() for line in text.splitlines()]
            for idx, line in enumerate(lines):
                if not line.startswith("#EXTINF:"):
                    continue
                try:
                    segment_seconds = float(line.split(":", 1)[1].split(",", 1)[0].strip())
                except ValueError:
                    continue
                if segment_seconds <= 0:
                    continue
                for seg_idx in range(idx + 1, len(lines)):
                    segment_ref = lines[seg_idx].strip()
                    if not segment_ref or segment_ref.startswith("#"):
                        continue
                    segment_url = urllib.parse.urljoin(playlist_url, segment_ref)
                    seg_resp = c_req.get(segment_url, impersonate="chrome110", timeout=20, headers=headers)
                    if seg_resp.status_code != 200:
                        return None
                    segment_bytes = len(seg_resp.content or b"")
                    if segment_bytes > 0:
                        return segment_bytes / segment_seconds
                    break
        except Exception:
            return None
        return None

    def _get_m3u8_total_bytes(self, url, headers=None):
        headers = {
            "Referer": (headers or {}).get("Referer"),
            "Origin": (headers or {}).get("Origin"),
            "User-Agent": DEFAULT_USER_AGENT,
        }
        cache_key = _normalize_download_url(url)
        cached_total = self._m3u8_total_bytes_cache.get(cache_key)
        if cached_total:
            return cached_total
        c_req = get_curl_cffi_requests()

        def _fetch_playlist(target_url):
            resp = c_req.get(target_url, impersonate="chrome110", timeout=15, headers=headers)
            if resp.status_code != 200:
                raise Exception(f"HTTP {resp.status_code}")
            return resp.text

        def _parse_attr_list(line):
            attrs = {}
            payload = line.split(":", 1)[1] if ":" in line else ""
            for key, value in re.findall(r'([A-Z0-9-]+)=(".*?"|[^,]+)', payload):
                attrs[key] = value.strip().strip('"')
            return attrs

        def _probe_segment_bytes(segment_url):
            try:
                head_resp = c_req.head(segment_url, impersonate="chrome110", timeout=15, headers=headers)
                if head_resp.status_code < 400:
                    content_length = head_resp.headers.get("Content-Length") or head_resp.headers.get("content-length")
                    if content_length:
                        return max(int(content_length), 0)
            except Exception:
                pass
            try:
                get_resp = c_req.get(
                    segment_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers={**headers, "Range": "bytes=0-0"},
                )
                if get_resp.status_code < 400:
                    content_range = get_resp.headers.get("Content-Range") or get_resp.headers.get("content-range")
                    if content_range and "/" in content_range:
                        return max(int(content_range.split("/")[-1]), 0)
                    content_length = get_resp.headers.get("Content-Length") or get_resp.headers.get("content-length")
                    if content_length:
                        return max(int(content_length), 0)
                return None
            except Exception:
                return None

        def _sum_media_playlist_bytes(playlist_url, text):
            total_bytes = 0
            probe_urls = []
            seen_map_urls = set()
            pending_byterange_length = None
            lines = [raw_line.strip() for raw_line in text.splitlines()]
            for line in lines:
                if not line:
                    continue
                if line.startswith("#EXT-X-MAP:"):
                    map_match = re.search(r'URI="([^"]+)"', line)
                    if not map_match:
                        return None
                    map_url = urllib.parse.urljoin(playlist_url, map_match.group(1).strip())
                    if map_url not in seen_map_urls:
                        seen_map_urls.add(map_url)
                        probe_urls.append(map_url)
                    continue
                if line.startswith("#EXT-X-BYTERANGE:"):
                    length_part = line.split(":", 1)[1].split("@", 1)[0].strip()
                    try:
                        pending_byterange_length = max(int(length_part), 0)
                    except ValueError:
                        return None
                    continue
                if line.startswith("#"):
                    continue
                if pending_byterange_length is not None:
                    total_bytes += pending_byterange_length
                    pending_byterange_length = None
                    continue
                probe_urls.append(urllib.parse.urljoin(playlist_url, line))
            if pending_byterange_length is not None:
                return None
            if not probe_urls and total_bytes <= 0:
                return None
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                    futures = [executor.submit(_probe_segment_bytes, segment_url) for segment_url in probe_urls]
                    for future in concurrent.futures.as_completed(futures, timeout=120):
                        segment_bytes = future.result()
                        if segment_bytes is None:
                            return None
                        total_bytes += segment_bytes
            except Exception:
                return None
            return total_bytes if total_bytes > 0 else None

        try:
            master_text = _fetch_playlist(url)
        except Exception:
            return None

        playlist_targets = []
        if "#EXT-X-STREAM-INF" in master_text:
            lines = [line.strip() for line in master_text.splitlines()]
            media_groups = {}
            chosen_variant_url = None
            chosen_variant_attrs = {}

            for line in lines:
                if line.startswith("#EXT-X-MEDIA:"):
                    attrs = _parse_attr_list(line)
                    group_id = attrs.get("GROUP-ID")
                    media_type = attrs.get("TYPE")
                    if not group_id or not media_type:
                        continue
                    media_groups.setdefault((media_type, group_id), []).append(attrs)

            for idx, line in enumerate(lines):
                if not line.startswith("#EXT-X-STREAM-INF"):
                    continue
                if idx + 1 >= len(lines):
                    continue
                candidate = lines[idx + 1].strip()
                if not candidate or candidate.startswith("#"):
                    continue
                chosen_variant_url = urllib.parse.urljoin(url, candidate)
                chosen_variant_attrs = _parse_attr_list(line)
                break

            if not chosen_variant_url:
                return None
            playlist_targets.append(chosen_variant_url)

            audio_group = chosen_variant_attrs.get("AUDIO")
            if audio_group:
                audio_entries = media_groups.get(("AUDIO", audio_group), [])
                selected_audio = None
                for entry in audio_entries:
                    if entry.get("DEFAULT", "").upper() == "YES":
                        selected_audio = entry
                        break
                if selected_audio is None:
                    for entry in audio_entries:
                        if entry.get("AUTOSELECT", "").upper() == "YES":
                            selected_audio = entry
                            break
                if selected_audio is None and audio_entries:
                    selected_audio = audio_entries[0]
                audio_uri = (selected_audio or {}).get("URI")
                if audio_uri:
                    playlist_targets.append(urllib.parse.urljoin(url, audio_uri))
        else:
            playlist_targets.append(url)

        total_bytes = 0
        seen_targets = set()
        for playlist_target in playlist_targets:
            normalized_target = _normalize_download_url(playlist_target)
            if normalized_target in seen_targets:
                continue
            seen_targets.add(normalized_target)
            try:
                playlist_text = _fetch_playlist(playlist_target)
            except Exception:
                return None
            playlist_total = _sum_media_playlist_bytes(playlist_target, playlist_text)
            if playlist_total is None:
                return None
            total_bytes += playlist_total

        if total_bytes > 0:
            self._m3u8_total_bytes_cache[cache_key] = total_bytes
            return total_bytes
        return None

    def _get_stable_resume_base(self, url, ext="mp4", resume_key=None):
        normalized_url = _normalize_download_url(resume_key or url) or str(resume_key or url)
        digest = hashlib.sha1(normalized_url.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return os.path.join(tempfile.gettempdir(), f"downloader_resume_{digest}.{ext}")

    def _probe_media_duration_seconds(self, path):
        if not path or not os.path.exists(path):
            return 0.0
        ffprobe_path = os.path.join(_APP_DIR, "ffprobe.exe") if platform.system() == "Windows" else shutil.which("ffprobe") or "ffprobe"
        if not os.path.exists(ffprobe_path) and ffprobe_path == os.path.join(_APP_DIR, "ffprobe.exe"):
            return 0.0
        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            result = subprocess.run(
                [
                    ffprobe_path,
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    path,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                startupinfo=startupinfo,
                creationflags=creationflags,
                timeout=30,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return 0.0
            return max(float(result.stdout.strip()), 0.0)
        except Exception:
            return 0.0

    def _probe_media_info(self, path):
        if not path or not os.path.exists(path):
            return {
                "exists": False,
                "size": 0,
                "duration": 0.0,
                "has_streams": False,
                "valid": False,
                "reason": "missing",
            }
        ffprobe_path = os.path.join(_APP_DIR, "ffprobe.exe") if platform.system() == "Windows" else shutil.which("ffprobe") or "ffprobe"
        size = os.path.getsize(path)
        if not os.path.exists(ffprobe_path) and ffprobe_path == os.path.join(_APP_DIR, "ffprobe.exe"):
            duration = self._probe_media_duration_seconds(path)
            valid = size > 0 and duration > 0
            return {
                "exists": True,
                "size": size,
                "duration": duration,
                "has_streams": duration > 0,
                "valid": valid,
                "reason": "no-ffprobe" if valid else "ffprobe-missing",
            }
        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            result = subprocess.run(
                [
                    ffprobe_path,
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration,size:stream=index,codec_type",
                    "-of",
                    "json",
                    path,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                startupinfo=startupinfo,
                creationflags=creationflags,
                timeout=30,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return {
                    "exists": True,
                    "size": size,
                    "duration": 0.0,
                    "has_streams": False,
                    "valid": False,
                    "reason": "ffprobe-error",
                }
            data = json.loads(result.stdout.strip() or "{}")
            format_info = data.get("format") or {}
            streams = data.get("streams") or []
            duration = max(float(format_info.get("duration") or 0), 0.0)
            probed_size = max(int(float(format_info.get("size") or 0)), 0)
            size = probed_size or size
            has_streams = any((stream.get("codec_type") in ("audio", "video")) for stream in streams)
            valid = has_streams and size > 0
            return {
                "exists": True,
                "size": size,
                "duration": duration,
                "has_streams": has_streams,
                "valid": valid,
                "reason": "ok" if valid else "no-streams",
            }
        except Exception:
            return {
                "exists": True,
                "size": size,
                "duration": 0.0,
                "has_streams": False,
                "valid": False,
                "reason": "ffprobe-error",
            }

    def _concat_media_files(self, paths, out_path):
        first_path, second_path = paths
        ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe") if platform.system() == "Windows" else shutil.which("ffmpeg") or "ffmpeg"
        if not os.path.exists(ffmpeg_path) and ffmpeg_path == os.path.join(_APP_DIR, "ffmpeg.exe"):
            raise FileNotFoundError("ffmpeg.exe was not found in the application directory")
        concat_list = out_path + ".txt"

        def _escape_concat_path(path):
            return path.replace("'", "'\\''")

        with open(concat_list, "w", encoding="utf-8") as f:
            f.write(f"file '{_escape_concat_path(first_path)}'\n")
            f.write(f"file '{_escape_concat_path(second_path)}'\n")

        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            result = subprocess.run(
                [
                    ffmpeg_path,
                    "-y",
                    "-nostdin",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    concat_list,
                    "-c",
                    "copy",
                    out_path,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                startupinfo=startupinfo,
                creationflags=creationflags,
                timeout=300,
            )
            if result.returncode != 0:
                message = (result.stderr or result.stdout or "").strip()
                raise Exception(f"FFmpeg concat failed: {message[:240]}")
        finally:
            try:
                os.remove(concat_list)
            except OSError:
                pass

    def _promote_resume_artifact(self, src_path, dst_path):
        temp_exists = bool(src_path and os.path.exists(src_path) and os.path.getsize(src_path) > 0)
        resume_exists = bool(dst_path and os.path.exists(dst_path) and os.path.getsize(dst_path) > 0)
        if not resume_exists:
            return False
        merged_path = dst_path + ".merged.mp4"
        src_info = self._probe_media_info(src_path) if temp_exists else {"exists": False, "valid": False, "size": 0, "duration": 0.0}
        dst_info = self._probe_media_info(dst_path)
        try:
            if temp_exists and src_info.get("valid") and dst_info.get("valid"):
                self._concat_media_files((src_path, dst_path), merged_path)
                shutil.move(merged_path, src_path)
            elif temp_exists and not src_info.get("valid") and dst_info.get("valid"):
                try:
                    os.remove(src_path)
                except OSError:
                    pass
                shutil.move(dst_path, src_path)
            elif temp_exists and src_info.get("valid") and not dst_info.get("valid"):
                try:
                    os.remove(dst_path)
                except OSError:
                    pass
                return True
            elif temp_exists and not src_info.get("valid") and not dst_info.get("valid"):
                src_size = int(src_info.get("size") or 0)
                dst_size = int(dst_info.get("size") or 0)
                if src_size >= dst_size:
                    try:
                        os.remove(dst_path)
                    except OSError:
                        pass
                    return bool(src_path and os.path.exists(src_path) and os.path.getsize(src_path) > 0)
                try:
                    os.remove(src_path)
                except OSError:
                    pass
                shutil.move(dst_path, src_path)
            else:
                shutil.move(dst_path, src_path)
            for leftover in (dst_path, merged_path):
                if os.path.exists(leftover):
                    try:
                        os.remove(leftover)
                    except OSError:
                        continue
            return True
        except Exception as exc:
            best_candidate = None
            candidate_paths = []
            for candidate in (merged_path, src_path, dst_path):
                if candidate and os.path.exists(candidate):
                    try:
                        candidate_paths.append((os.path.getsize(candidate), candidate))
                    except OSError:
                        continue
            if candidate_paths:
                candidate_paths.sort(reverse=True)
                best_candidate = candidate_paths[0][1]
            if best_candidate and best_candidate != src_path:
                try:
                    if src_path and os.path.exists(src_path):
                        os.remove(src_path)
                    shutil.move(best_candidate, src_path)
                except Exception:
                    pass
            write_error_log(
                "resume artifact promote failure",
                exc,
                src_path=src_path,
                dst_path=dst_path,
                merged_path=merged_path,
                temp_exists=temp_exists,
                resume_exists=resume_exists,
                src_valid=src_info.get("valid"),
                dst_valid=dst_info.get("valid"),
                best_candidate=best_candidate,
            )
            for leftover in (dst_path, merged_path):
                if leftover and leftover != src_path and os.path.exists(leftover):
                    try:
                        os.remove(leftover)
                    except OSError:
                        continue
            return bool(src_path and os.path.exists(src_path) and os.path.getsize(src_path) > 0)

    def _load_resume_progress_info(self, progress_path):
        if not progress_path or not os.path.exists(progress_path):
            return {"seconds": 0, "bytes": 0, "source_url": ""}
        try:
            data = _load_json_with_backup(progress_path, {})
            if not isinstance(data, dict):
                return {"seconds": 0, "bytes": 0, "source_url": ""}
            return {
                "seconds": max(float(data.get("seconds", 0) or 0), 0.0),
                "bytes": max(int(data.get("bytes", 0) or 0), 0),
                "source_url": str(data.get("source_url", "") or ""),
            }
        except Exception:
            return {"seconds": 0, "bytes": 0, "source_url": ""}

    def _load_resume_progress(self, progress_path):
        return self._load_resume_progress_info(progress_path).get("seconds", 0)

    def _save_resume_progress(self, progress_path, seconds, source_url=None, bytes_done=None):
        if not progress_path:
            return
        seconds_value = max(float(seconds or 0), 0.0)
        bytes_value = max(int(bytes_done or 0), 0)
        source_value = str(source_url or "")
        now = time.time()
        with self._resume_progress_lock:
            cached = self._resume_progress_cache.get(progress_path)
            if cached:
                last_seconds = float(cached.get("seconds", 0.0) or 0.0)
                last_bytes = int(cached.get("bytes", 0) or 0)
                last_source = str(cached.get("source_url", "") or "")
                last_saved_at = float(cached.get("saved_at", 0.0) or 0.0)
                if (
                    source_value == last_source
                    and now - last_saved_at < RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS
                    and abs(seconds_value - last_seconds) < RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS
                    and abs(bytes_value - last_bytes) < RESUME_PROGRESS_MIN_BYTES_DELTA
                ):
                    self._resume_progress_cache[progress_path] = {
                        "seconds": seconds_value,
                        "bytes": bytes_value,
                        "source_url": source_value,
                        "saved_at": last_saved_at,
                    }
                    return
        payload = {
            "seconds": seconds_value,
            "bytes": bytes_value,
            "source_url": source_value,
            "updated_at": now,
        }
        try:
            _atomic_json_dump(progress_path, payload)
            with self._resume_progress_lock:
                self._resume_progress_cache[progress_path] = {
                    "seconds": seconds_value,
                    "bytes": bytes_value,
                    "source_url": source_value,
                    "saved_at": now,
                }
        except Exception:
            return

    def _estimate_resume_seconds_from_bytes(self, bytes_done, total_bytes, total_seconds):
        try:
            bytes_value = max(float(bytes_done or 0), 0.0)
            total_bytes_value = max(float(total_bytes or 0), 0.0)
            seconds_value = max(float(total_seconds or 0), 0.0)
            if bytes_value <= 0 or total_bytes_value <= 0 or seconds_value <= 0:
                return 0.0
            return min(seconds_value, seconds_value * (bytes_value / total_bytes_value))
        except (TypeError, ValueError):
            return 0.0

    def _probe_http_download_info(self, url, headers=None, session=None):
        headers = dict(headers or {})
        if "User-Agent" not in headers:
            headers["User-Agent"] = DEFAULT_USER_AGENT
        total_size = 0
        range_supported = False
        content_type = ""
        prefer_curl = _is_anime1_media_url(url) or _is_anime1_media_url(headers.get("Referer", "")) or _is_anime1_media_url(headers.get("Origin", ""))
        if prefer_curl:
            try:
                c_req = get_curl_cffi_requests()
                last_exc = None
                candidate_sessions = []
                if session is not None:
                    candidate_sessions.append(session)
                for browser in ("chrome110", "chrome120"):
                    try:
                        candidate_sessions.append(c_req.Session(impersonate=browser))
                    except Exception:
                        continue
                for candidate_session in candidate_sessions:
                    try:
                        resp = candidate_session.get(
                            url,
                            headers=headers,
                            timeout=20,
                            stream=True,
                        )
                        status = getattr(resp, "status_code", 0)
                        if status >= 400:
                            raise Exception(f"HTTP {status}")
                        resp_headers = resp.headers
                        content_type = (resp_headers.get("Content-Type", "") or "")
                        accept_ranges = (resp_headers.get("Accept-Ranges", "") or "").lower()
                        content_range = resp_headers.get("Content-Range", "") or ""
                        if status == 206 or "bytes" in accept_ranges or content_range:
                            range_supported = True
                        if content_range and "/" in content_range:
                            total_size = max(int(content_range.split("/")[-1]), 0)
                        else:
                            total_size = max(int(resp_headers.get("Content-Length", 0) or 0), 0)
                        try:
                            resp.close()
                        except Exception:
                            pass
                        return {"total_size": total_size, "range_supported": range_supported, "content_type": content_type}
                    except Exception as exc:
                        last_exc = exc
                if last_exc:
                    raise last_exc
            except Exception:
                pass
        try:
            req = urllib.request.Request(url, headers={**headers, "Range": "bytes=0-0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = getattr(resp, "status", resp.getcode())
                resp_headers = resp.headers
                content_type = (resp_headers.get("Content-Type", "") or "")
                accept_ranges = (resp_headers.get("Accept-Ranges", "") or "").lower()
                content_range = resp_headers.get("Content-Range", "") or ""
                if status == 206 or "bytes" in accept_ranges or content_range:
                    range_supported = True
                if content_range and "/" in content_range:
                    total_size = max(int(content_range.split("/")[-1]), 0)
                else:
                    total_size = max(int(resp_headers.get("Content-Length", 0) or 0), 0)
            return {"total_size": total_size, "range_supported": range_supported, "content_type": content_type}
        except Exception:
            try:
                req = urllib.request.Request(url, headers=headers, method="HEAD")
                with urllib.request.urlopen(req, timeout=20) as resp:
                    resp_headers = resp.headers
                    content_type = (resp_headers.get("Content-Type", "") or "")
                    accept_ranges = (resp_headers.get("Accept-Ranges", "") or "").lower()
                    total_size = max(int(resp_headers.get("Content-Length", 0) or 0), 0)
                    range_supported = "bytes" in accept_ranges
            except Exception:
                try:
                    c_req = get_curl_cffi_requests()
                    last_exc = None
                    candidate_sessions = []
                    if session is not None:
                        candidate_sessions.append(session)
                    for browser in ("chrome110", "chrome120"):
                        try:
                            candidate_sessions.append(c_req.Session(impersonate=browser))
                        except Exception:
                            continue
                    for candidate_session in candidate_sessions:
                        try:
                            resp = candidate_session.get(url, headers=headers, timeout=20, stream=True)
                            status = getattr(resp, "status_code", 0)
                            if status >= 400:
                                raise Exception(f"HTTP {status}")
                            resp_headers = resp.headers
                            content_type = (resp_headers.get("Content-Type", "") or "")
                            total_size = max(int(resp_headers.get("Content-Length", 0) or 0), 0)
                            range_supported = "bytes" in (resp_headers.get("Accept-Ranges", "") or "").lower()
                            try:
                                resp.close()
                            except Exception:
                                pass
                            last_exc = None
                            break
                        except Exception as exc:
                            last_exc = exc
                    if last_exc:
                        raise last_exc
                except Exception:
                    pass
            return {"total_size": total_size, "range_supported": range_supported, "content_type": content_type}

    def _download_http_range_part(self, url, headers, start_byte, end_byte, part_path, progress_box, stop_event):
        req_headers = dict(headers or {})
        req_headers["Range"] = f"bytes={start_byte}-{end_byte}"
        req = urllib.request.Request(url, headers=req_headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            with open(part_path, "wb") as f:
                while not stop_event.is_set():
                    chunk = resp.read(262144)
                    if not chunk:
                        break
                    f.write(chunk)
                    progress_box["bytes"] = progress_box["bytes"] + len(chunk)

    def _download_direct_media_audio_with_ffmpeg(self, item_id, url, save_dir, referer=None, origin=None):
        task = self.tasks.get(item_id, {})
        short_name = task.get("short_name") or "Audio"
        safe_name = "".join(ch for ch in short_name if ch not in '\\/:*?"<>|').strip() or "Audio"
        out_path = os.path.join(save_dir, f"{safe_name}.mp3")
        task["filename"] = out_path
        self.update_tree(item_id, "name", os.path.basename(out_path), force=True)
        if os.path.exists(out_path):
            self._mark_existing_file_complete(item_id, t("eta_file_exists") if "eta_file_exists" in I18N_DICT.get(CURRENT_LANG, {}) else "檔案已存在")
            return

        ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe") if platform.system() == "Windows" else shutil.which("ffmpeg") or "ffmpeg"
        if not os.path.exists(ffmpeg_path) and ffmpeg_path == os.path.join(_APP_DIR, "ffmpeg.exe"):
            raise FileNotFoundError("ffmpeg.exe was not found in the application directory")

        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        headers_blob = ""
        if referer or origin:
            headers_blob = f"Referer: {referer or ''}\r\nOrigin: {origin or ''}\r\nUser-Agent: {DEFAULT_USER_AGENT}\r\n"

        temp_out_path = os.path.join(tempfile.gettempdir(), f"{safe_name}_audio_tmp.mp3")
        cmd = [
            ffmpeg_path,
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-progress",
            "pipe:1",
        ]
        if headers_blob:
            cmd += ["-headers", headers_blob]
        cmd += ["-i", url, "-vn", "-acodec", "libmp3lame", "-b:a", "192k", temp_out_path]

        write_error_log("ffmpeg direct audio started", Exception("ffmpeg direct audio started"), url=url, item_id=item_id)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            bufsize=1,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )
        if item_id in self.tasks:
            self.tasks[item_id]["_proc"] = proc
        recent_lines = []
        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                state = self.tasks.get(item_id, {}).get("state")
                if state == "PAUSE_REQUESTED":
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    self.tasks[item_id]["state"] = "PAUSED"
                    self._set_task_paused_ui(item_id)
                    return
                if state == "DELETE_REQUESTED":
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    self.tasks[item_id]["state"] = "DELETED"
                    self._discard_task(item_id)
                    return
                current_size = os.path.getsize(temp_out_path) if os.path.exists(temp_out_path) else 0
                if self._maybe_auto_pause_for_disk_space(item_id, temp_out_path, note=self._disk_full_pause_text()):
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    return
                line = raw_line.strip()
                if not line:
                    continue
                recent_lines.append(line)
                recent_lines = recent_lines[-20:]
                if line == "progress=end":
                    break
            return_code = proc.wait()
        finally:
            if proc.stdout is not None:
                proc.stdout.close()
            current_task = self.tasks.get(item_id)
            if current_task is not None and current_task.get("_proc") is proc:
                current_task["_proc"] = None

        if return_code != 0:
            raise Exception(f"FFmpeg direct audio exited with code {return_code}: {' | '.join(recent_lines)[:240]}")
        if not os.path.exists(temp_out_path) or os.path.getsize(temp_out_path) < 64 * 1024:
            raise Exception("FFmpeg direct audio produced an invalid output artifact")

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except OSError:
                pass
        shutil.move(temp_out_path, out_path)
        self.update_tree(item_id, "progress", "100%", force=True)
        self._mark_task_finished(item_id)
        write_error_log("ffmpeg direct audio finished", Exception("ffmpeg direct audio finished"), url=url, item_id=item_id, output=out_path, bytes=os.path.getsize(out_path))

    def _get_disk_free_bytes(self, target_path):
        try:
            base_dir = target_path if os.path.isdir(target_path) else os.path.dirname(target_path)
            if not base_dir:
                base_dir = _APP_DIR
            return shutil.disk_usage(base_dir).free
        except Exception:
            return None

    def _pause_task_for_disk_full(self, item_id, target_path, free_bytes, required_bytes=None, note=None):
        task = self.tasks.get(item_id)
        if not task:
            return True
        task["state"] = "PAUSED"
        task["disk_full_pause"] = True
        message = note or self._disk_full_pause_text()
        self._set_task_paused_ui(item_id, message)
        write_error_log(
            "disk full auto pause",
            Exception("disk space low"),
            item_id=item_id,
            target_path=target_path,
            free_bytes=free_bytes,
            required_bytes=required_bytes,
        )
        return True

    def _maybe_auto_pause_for_disk_space(self, item_id, target_path, required_bytes=None, note=None):
        free_bytes = self._get_disk_free_bytes(target_path)
        if free_bytes is None:
            return False
        threshold = DISK_SPACE_RESERVE_BYTES
        if required_bytes is not None:
            try:
                required_value = max(int(required_bytes), 0)
            except Exception:
                required_value = 0
            if free_bytes < required_value + threshold:
                return self._pause_task_for_disk_full(item_id, target_path, free_bytes, required_value, note=note)
        elif free_bytes < threshold:
            return self._pause_task_for_disk_full(item_id, target_path, free_bytes, None, note=note)
        return False

    def _disk_full_pause_text(self):
        return t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"

    def _check_resume_disk_space(self, item_id):
        task = self.tasks.get(item_id) or {}
        target_path = task.get("filename") or task.get("temp_filename") or self.save_dir_var.get() or _APP_DIR
        free_bytes = self._get_disk_free_bytes(target_path)
        if free_bytes is None:
            return True
        if free_bytes >= DISK_SPACE_RESERVE_BYTES:
            return True
        reserve_mb = DISK_SPACE_RESERVE_BYTES / (1024 * 1024)
        free_mb = free_bytes / (1024 * 1024)
        warning_body = (
            f"磁碟可用空間不足，無法續傳。\n目前可用：約 {free_mb:.1f} MB\n至少需保留：約 {reserve_mb:.1f} MB"
            if CURRENT_LANG == "zh_TW"
            else f"磁盘可用空间不足，无法续传。\n当前可用：约 {free_mb:.1f} MB\n至少需保留：约 {reserve_mb:.1f} MB"
            if CURRENT_LANG == "zh_CN"
            else f"Insufficient disk space to resume.\nFree space: about {free_mb:.1f} MB\nRequired reserve: about {reserve_mb:.1f} MB"
            if CURRENT_LANG == "en_US"
            else f"空き容量不足のため再開できません。\n現在の空き容量: 約 {free_mb:.1f} MB\n必要な予約容量: 約 {reserve_mb:.1f} MB"
        )
        try:
            self._show_warning(warning_body, parent=self.root)
        except Exception:
            pass
        self._set_task_paused_ui(item_id, self._disk_full_pause_text())
        task["state"] = "PAUSED"
        task["disk_full_pause"] = True
        return False

    def _set_task_paused_ui(self, item_id, message="-"):
        self.update_tree(item_id, "speed_eta", message, force=True)
        self.update_tree(item_id, "status", t("status_paused"), force=True)

    def _set_task_finished_ui(self, item_id, message="-"):
        self.update_tree(item_id, "progress", "100%", force=True)
        self.update_tree(item_id, "status", t("status_done"), force=True)
        self.update_tree(item_id, "speed_eta", message, force=True)

    def _update_task_size_from_file(self, item_id, filename):
        if filename and os.path.exists(filename):
            try:
                self.update_tree(item_id, "size", f"{os.path.getsize(filename) / (1024 * 1024):.1f} MB", force=True)
            except OSError:
                pass

    def _finalize_completed_task(self, task, clear_resume_requested=False):
        task["state"] = "FINISHED"
        if clear_resume_requested:
            task["resume_requested"] = False
        remove_from_state(task["url"])

    def _set_task_downloading_ui(self, item_id, message=None):
        status_text = self._downloading_status_text()
        task = self.tasks.get(item_id)
        if task is not None:
            task["_last_status_text"] = status_text
        self.update_tree(item_id, "status", status_text, force=True)
        if message is not None:
            self.update_tree(item_id, "speed_eta", message, force=True)

    def _processing_status_text(self):
        return t("status_processing") if "status_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中"

    def _set_task_processing_ui(self, item_id, message=None):
        status_text = self._processing_status_text()
        task = self.tasks.get(item_id)
        if task is not None:
            task["_last_status_text"] = status_text
        self.update_tree(item_id, "status", status_text, force=True)
        if message is not None:
            self.update_tree(item_id, "speed_eta", message, force=True)

    def _eta_direct_media_text(self):
        return t("eta_direct_media") if "eta_direct_media" in I18N_DICT.get(CURRENT_LANG, {}) else "直接媒體下載"

    def _eta_found_media_text(self):
        return t("eta_found_media") if "eta_found_media" in I18N_DICT.get(CURRENT_LANG, {}) else "已取得媒體網址"

    def _eta_found_stream_text(self):
        return t("eta_found_stream") if "eta_found_stream" in I18N_DICT.get(CURRENT_LANG, {}) else "已取得串流網址"

    def _eta_site_text(self, key, fallback):
        return t(key) if key in I18N_DICT.get(CURRENT_LANG, {}) else fallback

    def _site_parse_error_text(self, error):
        prefix = t("err_site_parse") if "err_site_parse" in I18N_DICT.get(CURRENT_LANG, {}) else "解析失敗"
        return f"{prefix}: {str(error)[:40]}"

    def _schedule_site_parse_error(self, error, limit=80):
        prefix = t("err_site_parse") if "err_site_parse" in I18N_DICT.get(CURRENT_LANG, {}) else "解析失敗"
        self._schedule_error(f"{prefix}: {str(error)[:limit]}")

    def _set_task_error_ui(self, item_id, message):
        self.update_tree(item_id, "status", t("status_error") if "status_error" in I18N_DICT.get(CURRENT_LANG, {}) else "錯誤", force=True)
        self.update_tree(item_id, "speed_eta", message, force=True)

    def _set_task_queued_ui(self, item_id):
        self.update_tree(item_id, "status", t("status_queued"), force=True)
        self.update_tree(item_id, "progress", "--", force=True)
        self.update_tree(item_id, "size", "-", force=True)
        self.update_tree(item_id, "speed_eta", "-", force=True)

    def _download_http_media(self, item_id, url, out_path, headers=None, session=None):
        headers = dict(headers or {})
        pause_note = self._disk_full_pause_text()
        if self._maybe_auto_pause_for_disk_space(item_id, out_path, note=pause_note):
            return
        if "User-Agent" not in headers:
            headers["User-Agent"] = DEFAULT_USER_AGENT
        prefer_curl_stream = _is_anime1_media_url(url) or _is_anime1_media_url(headers.get("Referer", "")) or _is_anime1_media_url(headers.get("Origin", ""))
        if headers.get("Referer"):
            headers["Referer"] = _safe_header_url(headers.get("Referer"))
        total_info = self._probe_http_download_info(url, headers=headers, session=session)
        total_size = total_info.get("total_size", 0)
        range_supported = bool(total_info.get("range_supported"))
        resume_bytes = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        if prefer_curl_stream:
            if resume_bytes > 0:
                try:
                    os.remove(out_path)
                except OSError:
                    pass
            resume_bytes = 0
            range_supported = False
        if total_size > 0 and resume_bytes >= total_size:
            self._mark_existing_file_complete(item_id, t("eta_file_exists") if "eta_file_exists" in I18N_DICT.get(CURRENT_LANG, {}) else "檔案已存在")
            return
        dl_headers = dict(headers)
        mode = "ab" if resume_bytes > 0 else "wb"
        if resume_bytes > 0:
            dl_headers["Range"] = f"bytes={resume_bytes}-"
        res = None
        stream_iter = None
        stream_response = None
        try:
            if prefer_curl_stream or session is not None:
                raise RuntimeError("prefer curl stream")
            req = urllib.request.Request(url, headers=dl_headers)
            res = urllib.request.urlopen(req, timeout=20)
        except Exception:
            c_req = get_curl_cffi_requests()
            last_exc = None
            candidate_sessions = []
            if session is not None:
                candidate_sessions.append(session)
            for browser in ("chrome110", "chrome120"):
                try:
                    candidate_sessions.append(c_req.Session(impersonate=browser))
                except Exception:
                    continue
            for candidate_session in candidate_sessions:
                try:
                    candidate_response = candidate_session.get(url, headers=dl_headers, timeout=20, stream=True)
                    status = getattr(candidate_response, "status_code", 0)
                    if status >= 400:
                        try:
                            candidate_response.close()
                        except Exception:
                            pass
                        raise Exception(f"HTTP {status}")
                    stream_response = candidate_response
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
            if stream_response is None and last_exc is not None:
                raise last_exc
            stream_iter = stream_response.iter_content(chunk_size=1048576)
        switched_to_multipart = False
        downloaded = resume_bytes
        start_time = time.time()
        last_update_time = start_time
        last_update_bytes = downloaded
        try:
            with open(out_path, mode) as f:
                while True:
                    state = self.tasks[item_id]["state"]
                    if state == "PAUSE_REQUESTED":
                        self.tasks[item_id]["state"] = "PAUSED"
                        self._set_task_paused_ui(item_id)
                        try:
                            if res is not None:
                                res.close()
                            if stream_response is not None:
                                stream_response.close()
                        except Exception:
                            pass
                        return
                    if state == "DELETE_REQUESTED":
                        raise KeyboardInterrupt()
                    required_bytes = max(total_size - downloaded, 0) if total_size > 0 else None
                    if self._maybe_auto_pause_for_disk_space(item_id, out_path, required_bytes=required_bytes, note=pause_note):
                        try:
                            if res is not None:
                                res.close()
                            if stream_response is not None:
                                stream_response.close()
                        except Exception:
                            pass
                        return
                    if not switched_to_multipart and not prefer_curl_stream and range_supported and total_size > 0:
                        elapsed_probe = time.time() - start_time
                        if elapsed_probe >= 2.0:
                            current_speed = max((downloaded - resume_bytes) / max(elapsed_probe, 0.001), 0.0)
                            remaining = max(total_size - downloaded, 0)
                            if current_speed < 1024 * 1024 and remaining > 8 * 1024 * 1024:
                                switched_to_multipart = True
                                break
                    if res is not None:
                        chunk = res.read(1048576)
                    else:
                        try:
                            chunk = next(stream_iter)
                        except StopIteration:
                            chunk = b""
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    task = self.tasks.get(item_id, {})
                    task["downloaded_bytes"] = downloaded
                    if total_size > 0:
                        task["total_bytes"] = total_size
                    now = time.time()
                    if now - last_update_time >= 1.0:
                        speed_bps = max((downloaded - last_update_bytes) / max(now - last_update_time, 0.001), 0.0)
                        last_update_time = now
                        last_update_bytes = downloaded
                        percent = format_progress_percent(downloaded, total_size, cap_at_99=True)
                        if percent is not None:
                            self.update_tree(item_id, "progress", f"{percent:.1f}%", force=True)
                        if total_size > 0:
                            self.update_tree(item_id, "size", f"{total_size / 1024 / 1024:.1f} MB")
                            eta = max((total_size - downloaded) / max(speed_bps, 1.0), 0.0)
                            self.update_tree(item_id, "speed_eta", f"{format_transfer_rate(speed_bps)} | {format_eta(eta)}")
                        else:
                            self.update_tree(item_id, "size", f"{downloaded / 1024 / 1024:.1f} MB")
                            self.update_tree(item_id, "speed_eta", format_transfer_rate(speed_bps))
            if res is not None:
                try:
                    res.close()
                except Exception:
                    pass
            if stream_response is not None:
                try:
                    stream_response.close()
                except Exception:
                    pass
            if switched_to_multipart:
                progress_boxes = []
                stop_event = threading.Event()
                part_paths = []
                remaining_start = downloaded
                remaining_end = total_size - 1
                part_size = max((remaining_end - remaining_start + 1) // 4, 1)
                futures = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    for index in range(4):
                        part_start = remaining_start + index * part_size
                        if part_start > remaining_end:
                            break
                        part_end = remaining_end if index == 3 else min(remaining_end, part_start + part_size - 1)
                        part_path = f"{out_path}.part{index}"
                        part_paths.append(part_path)
                        box = {"bytes": 0}
                        progress_boxes.append(box)
                        futures.append(executor.submit(self._download_http_range_part, url, headers, part_start, part_end, part_path, box, stop_event))
                    while futures:
                        if self.tasks[item_id]["state"] in {"PAUSE_REQUESTED", "DELETE_REQUESTED"}:
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            if self.tasks[item_id]["state"] == "PAUSE_REQUESTED":
                                self.tasks[item_id]["state"] = "PAUSED"
                                self.update_tree(item_id, "status", t("status_paused"), force=True)
                            return
                        multi_downloaded = downloaded + sum(box["bytes"] for box in progress_boxes)
                        required_bytes = max(total_size - multi_downloaded, 0) if total_size > 0 else None
                        if self._maybe_auto_pause_for_disk_space(item_id, out_path, required_bytes=required_bytes, note=pause_note):
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            return
                        done, futures = concurrent.futures.wait(futures, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)
                        now = time.time()
                        multi_downloaded = downloaded + sum(box["bytes"] for box in progress_boxes)
                        elapsed = max(now - start_time, 0.001)
                        speed_bps = max((multi_downloaded - resume_bytes) / elapsed, 0.0)
                        percent = format_progress_percent(multi_downloaded, total_size, cap_at_99=True)
                        if percent is not None:
                            self.update_tree(item_id, "progress", f"{percent:.1f}%")
                            self.update_tree(item_id, "size", f"{total_size / 1024 / 1024:.1f} MB")
                            eta = max((total_size - multi_downloaded) / max(speed_bps, 1.0), 0.0)
                            self.update_tree(item_id, "speed_eta", f"{format_transfer_rate(speed_bps)} | {format_eta(eta)}")
                        time.sleep(0.5)
                with open(out_path, "ab") as main_out:
                    for part_path in part_paths:
                        if not os.path.exists(part_path):
                            continue
                        with open(part_path, "rb") as part_in:
                            shutil.copyfileobj(part_in, main_out)
                for part_path in part_paths:
                    if os.path.exists(part_path):
                        try:
                            os.remove(part_path)
                        except OSError:
                            pass
        except OSError as exc:
            if getattr(exc, "errno", None) == 28:
                self._pause_task_for_disk_full(item_id, out_path, self._get_disk_free_bytes(out_path), None, note=self._disk_full_pause_text())
                return
            return
        except Exception:
            return
        self._mark_task_finished(item_id)

    def _download_m3u8_with_ffmpeg(self, item_id, url, save_dir, is_mp3=False, referer="https://www.movieffm.net/", origin="https://www.movieffm.net"):
        task = self.tasks.get(item_id, {})
        short_name = task.get("short_name") or "Video"
        safe_name = "".join(ch for ch in short_name if ch not in '\\/:*?"<>|').strip() or "Video"
        ext = "mp3" if is_mp3 else "mp4"
        out_path = os.path.join(save_dir, f"{safe_name}.{ext}")
        task["filename"] = out_path
        self.update_tree(item_id, "name", os.path.basename(out_path), force=True)
        if os.path.exists(out_path):
            self._mark_existing_file_complete(item_id, t("eta_file_exists") if "eta_file_exists" in I18N_DICT.get(CURRENT_LANG, {}) else "檔案已存在")
            return

        resume_key = task.get("source_page") or task.get("url") or url
        temp_out_path = self._get_stable_resume_base(url, ext=ext, resume_key=resume_key)
        temp_root, temp_ext = os.path.splitext(temp_out_path)
        resume_out_path = f"{temp_root}.resume{temp_ext}"
        merged_out_path = f"{temp_root}.merged{temp_ext}"
        progress_path = temp_out_path + ".progress.json"
        task["temp_filename"] = temp_out_path

        ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe") if platform.system() == "Windows" else shutil.which("ffmpeg") or "ffmpeg"
        if not os.path.exists(ffmpeg_path) and ffmpeg_path == os.path.join(_APP_DIR, "ffmpeg.exe"):
            raise FileNotFoundError("ffmpeg.exe was not found in the application directory")

        fallback_urls = [url] + list(task.get("fallback_urls", []))
        candidate_urls = []
        for candidate in fallback_urls:
            normalized_candidate = _normalize_download_url(candidate)
            if normalized_candidate and normalized_candidate not in candidate_urls:
                candidate_urls.append(normalized_candidate)

        duration_box = {}
        media_bps_box = {}
        total_bytes_box = {}

        def probe_metadata():
            for candidate in candidate_urls:
                try:
                    duration = self._get_m3u8_duration(candidate, headers={"Referer": referer, "Origin": origin})
                    if duration > 0:
                        duration_box["value"] = duration
                except Exception:
                    pass
                try:
                    total_bytes = self._get_m3u8_total_bytes(candidate, headers={"Referer": referer, "Origin": origin})
                    if total_bytes and total_bytes > 0:
                        total_bytes_box["value"] = total_bytes
                except Exception:
                    pass
                try:
                    media_bps = self._estimate_m3u8_media_bps(candidate, headers={"Referer": referer, "Origin": origin})
                    if media_bps and media_bps > 0:
                        media_bps_box["value"] = media_bps
                except Exception:
                    pass
                if duration_box.get("value") and total_bytes_box.get("value") and media_bps_box.get("value"):
                    return

        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        last_error = None
        headers = f"Referer: {referer}\r\nOrigin: {origin}\r\nUser-Agent: {DEFAULT_USER_AGENT}\r\n"
        total_duration = 0.0
        self._start_daemon_thread(probe_metadata)

        for candidate_url in candidate_urls:
            self._promote_resume_artifact(temp_out_path, resume_out_path)
            stored_info = self._load_resume_progress_info(progress_path)
            partial_info = self._probe_media_info(temp_out_path)
            partial_size = partial_info.get("size", 0)
            partial_duration = partial_info.get("duration", 0.0)
            stored_bytes = int(stored_info.get("bytes", 0) or 0)
            stored_seconds = float(stored_info.get("seconds", 0.0) or 0.0)
            stored_source_url = _normalize_download_url(stored_info.get("source_url", ""))
            current_resume_key = _normalize_download_url(resume_key)
            same_resume_target = stored_source_url == current_resume_key
            partial_reason = partial_info.get("reason")
            partial_valid = bool(partial_info.get("valid"))
            size_consistent = partial_size > 0 and stored_bytes > 0 and abs(partial_size - stored_bytes) <= max(1024 * 1024, int(max(partial_size, stored_bytes) * 0.35))
            usable_sidecar_only_partial = partial_size > 0 and same_resume_target and (partial_valid or partial_reason == "ffprobe-error")
            if stored_seconds > 0 and stored_bytes > 0 and partial_size > 0 and partial_size < stored_bytes:
                stored_seconds = max(0.0, min(stored_seconds, stored_seconds * (partial_size / stored_bytes)))

            if partial_valid and partial_duration > 0:
                base_bytes = partial_size
                resume_seconds = partial_duration
            elif usable_sidecar_only_partial:
                base_bytes = partial_size
                resume_seconds = stored_seconds
            else:
                if partial_size > 0 and (not same_resume_target or (stored_bytes > 0 and not size_consistent)):
                    write_error_log(
                        "resume state reset",
                        Exception("resume metadata mismatch"),
                        url=candidate_url,
                        item_id=item_id,
                        total_duration=duration_box.get("value", 0.0) or total_duration,
                        partial_duration=partial_duration,
                        partial_size=partial_size,
                        partial_valid=partial_valid,
                        partial_reason=partial_reason,
                        usable_sidecar_only_partial=usable_sidecar_only_partial,
                        current_resume_key=current_resume_key,
                        stored_progress=stored_seconds,
                        stored_bytes=stored_bytes,
                        stored_source_url=stored_source_url,
                    )
                base_bytes = 0
                resume_seconds = 0.0
                for stale_path in (temp_out_path, resume_out_path, merged_out_path, progress_path):
                    if os.path.exists(stale_path):
                        try:
                            os.remove(stale_path)
                        except OSError:
                            pass
            if duration_box.get("value"):
                total_duration = duration_box.get("value", 0.0)
            elif total_duration <= 0:
                try:
                    total_duration = self._get_m3u8_duration(candidate_url, headers={"Referer": referer, "Origin": origin})
                except Exception:
                    total_duration = 0.0
            total_bytes = total_bytes_box.get("value")
            if total_duration > 0 and total_bytes and base_bytes > 0:
                estimated_resume_seconds = self._estimate_resume_seconds_from_bytes(base_bytes, total_bytes, total_duration)
                if estimated_resume_seconds > 0:
                    if resume_seconds <= 0:
                        resume_seconds = estimated_resume_seconds
                    else:
                        allowed_drift = max(30.0, total_duration * 0.05)
                        if abs(resume_seconds - estimated_resume_seconds) > allowed_drift:
                            resume_seconds = min(resume_seconds, estimated_resume_seconds)
            average_media_bps = 0.0
            if base_bytes > 0 and resume_seconds > 0:
                average_media_bps = base_bytes / resume_seconds
            if average_media_bps <= 0 and media_bps_box.get("value"):
                average_media_bps = media_bps_box.get("value", 0.0)
            if average_media_bps <= 0:
                average_media_bps = self._estimate_m3u8_media_bps(candidate_url, headers={"Referer": referer, "Origin": origin}) or 0.0

            active_output_path = resume_out_path if resume_seconds > 0 else temp_out_path
            write_error_log(
                "ffmpeg download started",
                Exception("ffmpeg started"),
                url=candidate_url,
                item_id=item_id,
                source_site=task.get("source_site"),
                fallback_count=len(task.get("fallback_urls", [])),
                resume_seconds=resume_seconds,
                base_bytes=base_bytes,
                stored_bytes=stored_info.get("bytes", 0),
                stored_progress=stored_info.get("seconds", 0.0),
                total_duration=total_duration,
                total_bytes=total_bytes,
                average_media_bps=average_media_bps,
            )

            cmd = [
                ffmpeg_path,
                "-y",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-progress",
                "pipe:1",
                "-protocol_whitelist",
                "file,http,https,tcp,tls,crypto,data",
                "-allowed_extensions",
                "ALL",
                "-headers",
                headers,
            ]
            if resume_seconds > 0:
                cmd += ["-ss", f"{resume_seconds:.3f}"]
            cmd += ["-i", candidate_url]
            if is_mp3:
                cmd += ["-vn", "-acodec", "libmp3lame", "-b:a", "192k"]
            else:
                cmd += ["-c", "copy"]
            cmd += [active_output_path]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
                bufsize=1,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
            if item_id in self.tasks:
                self.tasks[item_id]["_proc"] = proc
            progress = {}
            recent_lines = []
            last_ui_update = 0.0
            invalidated_total_bytes = False
            near_complete_since = None
            active_total_bytes = total_bytes_box.get("value") or total_bytes
            try:
                assert proc.stdout is not None
                for raw_line in proc.stdout:
                    state = self.tasks.get(item_id, {}).get("state")
                    if state == "PAUSE_REQUESTED":
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        self.tasks[item_id]["state"] = "PAUSED"
                        self._set_task_paused_ui(item_id)
                        return
                    if state == "DELETE_REQUESTED":
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        self.tasks[item_id]["state"] = "DELETED"
                        self._discard_task(item_id)
                        return
                    active_size = os.path.getsize(active_output_path) if os.path.exists(active_output_path) else 0
                    required_bytes = None
                    if active_total_bytes:
                        required_bytes = max(int(active_total_bytes) - int(base_bytes + active_size), 0)
                    if self._maybe_auto_pause_for_disk_space(item_id, active_output_path, required_bytes=required_bytes, note=self._disk_full_pause_text()):
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        return
                    line = raw_line.strip()
                    if not line:
                        continue
                    recent_lines.append(line)
                    recent_lines = recent_lines[-20:]
                    if "=" in line:
                        key, value = line.split("=", 1)
                        progress[key] = value
                    if "out_time_ms" in progress and progress["out_time_ms"].isdigit():
                        out_ms = int(progress["out_time_ms"])
                        total_done_seconds = resume_seconds + (out_ms / 1_000_000.0)
                        now = time.time()
                        active_total_bytes = None if invalidated_total_bytes else (total_bytes_box.get("value") or total_bytes)
                        active_output_bytes = 0
                        try:
                            if os.path.exists(active_output_path):
                                active_output_bytes = os.path.getsize(active_output_path)
                        except OSError:
                            active_output_bytes = 0
                        current_bytes = base_bytes + active_output_bytes
                        if active_total_bytes and active_total_bytes > 0 and progress.get("progress") != "end":
                            if current_bytes > active_total_bytes * 1.01:
                                estimated_total_bytes = active_total_bytes
                                invalidated_total_bytes = True
                                total_bytes_box["value"] = None
                                total_bytes = None
                                active_total_bytes = None
                                near_complete_since = None
                                write_error_log(
                                    "m3u8 total bytes invalidated",
                                    Exception("actual bytes exceeded estimated total"),
                                    url=candidate_url,
                                    item_id=item_id,
                                    estimated_total_bytes=estimated_total_bytes,
                                    current_bytes=current_bytes,
                                )
                        if active_total_bytes and active_total_bytes > 0:
                            percent = format_progress_percent(current_bytes, active_total_bytes, cap_at_99=True)
                            if percent is not None:
                                if progress.get("progress") != "end":
                                    percent = min(percent, 99.0)
                                    if percent >= 99.0:
                                        if near_complete_since is None:
                                            near_complete_since = now
                                        elif now - near_complete_since >= 5.0:
                                            estimated_total_bytes = active_total_bytes
                                            invalidated_total_bytes = True
                                            total_bytes_box["value"] = None
                                            total_bytes = None
                                            active_total_bytes = None
                                            percent = None
                                            write_error_log(
                                                "m3u8 total bytes invalidated",
                                                Exception("estimated total stalled at 99% before ffmpeg end"),
                                                url=candidate_url,
                                                item_id=item_id,
                                                estimated_total_bytes=estimated_total_bytes,
                                                current_bytes=current_bytes,
                                            )
                                    else:
                                        near_complete_since = None
                                if percent is not None:
                                    self.update_tree(item_id, "progress", f"{percent:.1f}%")
                            if active_total_bytes and active_total_bytes > 0:
                                self.update_tree(item_id, "size", f"{current_bytes / (1024 * 1024):.1f} / {active_total_bytes / (1024 * 1024):.1f} MB")
                        else:
                            near_complete_since = None
                            self.update_tree(item_id, "progress", "--")
                        if not (active_total_bytes and active_total_bytes > 0) and current_bytes > 0:
                            self.update_tree(item_id, "size", f"{current_bytes / (1024 * 1024):.1f} MB")
                        if now - last_ui_update >= 1.0:
                            instant_bps = 0.0
                            if active_output_bytes > 0 and out_ms > 0:
                                instant_bps = active_output_bytes / max(out_ms / 1_000_000.0, 0.001)
                            eta_text = None
                            if active_total_bytes and active_total_bytes > 0 and instant_bps > 0:
                                eta_text = format_eta((active_total_bytes - current_bytes) / instant_bps)
                            elif total_duration > 0 and instant_bps > 0:
                                remaining_seconds = max(total_duration - total_done_seconds, 0.0)
                                eta_text = format_eta(remaining_seconds)
                            if instant_bps > 0 and eta_text:
                                self.update_tree(item_id, "speed_eta", f"{format_transfer_rate(instant_bps)} | {eta_text}")
                            elif instant_bps > 0:
                                self.update_tree(item_id, "speed_eta", format_transfer_rate(instant_bps))
                            last_ui_update = now
                        self._save_resume_progress(progress_path, total_done_seconds, source_url=resume_key, bytes_done=current_bytes)
                    if progress.get("progress") == "end":
                        break
                return_code = proc.wait()
            finally:
                if proc.stdout is not None:
                    proc.stdout.close()
                current_task = self.tasks.get(item_id)
                if current_task is not None and current_task.get("_proc") is proc:
                    current_task["_proc"] = None

            if return_code == 0:
                final_source = None
                base_exists = os.path.exists(temp_out_path)
                resume_exists = os.path.exists(resume_out_path)

                if resume_seconds > 0 and base_exists and resume_exists:
                    try:
                        self._concat_media_files([temp_out_path, resume_out_path], merged_out_path)
                        final_source = merged_out_path
                    except Exception:
                        merged_info = self._probe_media_info(merged_out_path)
                        base_info = self._probe_media_info(temp_out_path)
                        resume_info = self._probe_media_info(resume_out_path)
                        candidates_info = [
                            (merged_out_path, merged_info),
                            (resume_out_path, resume_info),
                            (temp_out_path, base_info),
                        ]
                        candidates_info = [(path, info) for path, info in candidates_info if info.get("exists") and info.get("size", 0) > 0]
                        if candidates_info:
                            candidates_info.sort(key=lambda item: (item[1].get("valid", False), item[1].get("duration", 0.0), item[1].get("size", 0)), reverse=True)
                            final_source = candidates_info[0][0]
                elif resume_exists:
                    final_source = resume_out_path
                elif base_exists:
                    final_source = temp_out_path

                if not final_source or not os.path.exists(final_source):
                    last_error = Exception("FFmpeg completed without producing an output artifact")
                    continue

                final_info = self._probe_media_info(final_source)
                final_size = final_info.get("size", 0)
                final_duration = final_info.get("duration", 0.0)
                if final_size <= 0:
                    last_error = Exception("FFmpeg produced an empty output artifact")
                    continue
                if not is_mp3:
                    looks_truncated = total_duration > 30 and final_duration > 0 and final_duration < min(5.0, total_duration * 0.01)
                    looks_invalid = (not final_info.get("valid")) and final_size < 1024 * 1024
                    if looks_truncated or looks_invalid:
                        last_error = Exception(
                            f"FFmpeg produced an incomplete output artifact: size={final_size} duration={final_duration:.3f}"
                        )
                        write_error_log(
                            "ffmpeg artifact rejected",
                            last_error,
                            url=candidate_url,
                            item_id=item_id,
                            output=final_source,
                            size=final_size,
                            duration=final_duration,
                            total_duration=total_duration,
                        )
                        continue
                if is_mp3 and final_size < 64 * 1024:
                    last_error = Exception(f"FFmpeg produced an unexpectedly tiny audio artifact: size={final_size}")
                    continue

                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                shutil.move(final_source, out_path)

                for stale_path in (temp_out_path, resume_out_path, merged_out_path, progress_path):
                    if stale_path == out_path:
                        continue
                    if os.path.exists(stale_path):
                        try:
                            os.remove(stale_path)
                        except OSError:
                            pass

                self.update_tree(item_id, "progress", "100%", force=True)
                self._mark_task_finished(item_id)
                write_error_log(
                    "ffmpeg download finished",
                    Exception("ffmpeg finished"),
                    url=candidate_url,
                    item_id=item_id,
                    output=out_path,
                    bytes=os.path.getsize(out_path) if os.path.exists(out_path) else 0,
                )
                return

            current_task = self.tasks.get(item_id, {})
            current_state = current_task.get("state")
            stop_reason = current_task.get("_stop_reason")
            if return_code != 0 and (self._is_stop_requested_state(current_state) or stop_reason in STOP_REASONS):
                if self._is_pause_requested_state(current_state) or stop_reason == STOP_REASON_PAUSE:
                    raise StopDownloadException("pause requested")
                raise KeyboardInterrupt()

            last_error = Exception(f"FFmpeg exited with code {return_code}: {' | '.join(recent_lines)[:240]}")
        if last_error:
            raise last_error
        raise Exception("FFmpeg download failed without candidates")

    def pause_selected(self):
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = self.tasks[item_id]["state"]
            if state == "DOWNLOADING" or self._is_pause_requested_state(state):
                self.tasks[item_id]["_stop_reason"] = STOP_REASON_PAUSE
                self.tasks[item_id]["state"] = "PAUSE_REQUESTED"
                self._set_task_paused_ui(item_id)
                proc = self.tasks[item_id].get("_proc")
                if proc is not None:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            elif state == "QUEUED":
                self.tasks[item_id]["state"] = "PAUSED"
                self._set_task_paused_ui(item_id)
                self._schedule_process_queue()
        self.persist_unfinished_state(force=True)

    def resume_selected(self):
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = self.tasks[item_id]["state"]
            if self._is_pause_requested_state(state):
                proc = self.tasks[item_id].get("_proc")
                if proc is not None:
                    try:
                        if proc.poll() is None:
                            continue
                    except Exception:
                        continue
                self.tasks[item_id]["state"] = "PAUSED"
            if self.tasks[item_id]["state"] not in RESUMABLE_TASK_STATES:
                continue
            self.tasks[item_id]["_stop_reason"] = None
            if not self._check_resume_disk_space(item_id):
                continue
            is_mp3 = self.tasks[item_id].get("is_mp3", False)
            source_site = self.tasks[item_id].get("source_site")
            self._start_download_thread(
                self.tasks[item_id]["url"],
                self.tasks[item_id]["short_name"],
                existing_item_id=item_id,
                is_mp3=is_mp3,
                source_site=source_site,
                extra_task_data={
                    "fallback_urls": list(self.tasks[item_id].get("fallback_urls", [])),
                    "source_page": self.tasks[item_id].get("source_page"),
                },
            )

    def delete_selected(self):
        changed = False
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = self.tasks[item_id]["state"]
            remove_from_state(self.tasks[item_id]["url"])
            if state in DELETE_REQUEST_TASK_STATES:
                self._request_task_deletion(item_id, state)
                changed = True
                continue
            self._delete_finished_task(item_id)
            changed = True
        if changed:
            self.persist_unfinished_state(force=True)

    def clear_all_finished(self):
        for item_id in list(self.tree.get_children()):
            if item_id not in self.tasks:
                continue
            if self.tasks[item_id]["state"] != "FINISHED":
                continue
            self.tree.delete(item_id)
            del self.tasks[item_id]
        self.persist_unfinished_state(force=True)

    def persist_unfinished_state(self, force=False):
        entries, signature = self._build_persistable_task_snapshot()
        now = time.time()
        if not force:
            if signature == self._last_state_persist_signature and now - self._last_state_persist_at < STATE_PERSIST_INTERVAL_SECONDS:
                return
            if now - self._last_state_persist_at < STATE_PERSIST_INTERVAL_SECONDS:
                self._last_state_persist_signature = signature
                return
        replace_state_entries(entries)
        self._last_state_persist_signature = signature
        self._last_state_persist_at = now

    def _process_queue(self):
        domain_counts = {}
        source_page_counts = {}
        queued_items = []
        for item_id, task in self.tasks.items():
            if task["state"] == "DOWNLOADING":
                domain, source_page = self._get_task_queue_keys(task)
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
                if source_page:
                    source_page_counts[source_page] = source_page_counts.get(source_page, 0) + 1
            elif task["state"] == "QUEUED":
                queued_items.append(item_id)
        for item_id in queued_items:
            task = self.tasks[item_id]
            domain, source_page = self._get_task_queue_keys(task)
            if domain_counts.get(domain, 0) >= MAX_DOWNLOADS_PER_DOMAIN:
                continue
            if source_page and source_page_counts.get(source_page, 0) >= MAX_DOWNLOADS_PER_SOURCE_PAGE:
                continue
            task["state"] = "DOWNLOADING"
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
            if source_page:
                source_page_counts[source_page] = source_page_counts.get(source_page, 0) + 1
            self._set_task_downloading_ui(item_id)
            self._start_daemon_thread(
                self.download_task,
                task["url"],
                item_id,
                self.save_dir_var.get(),
                self._should_use_impersonation(task["url"], task.get("source_site")),
                task.get("is_mp3", False),
            )

    def _cleanup_temp_files(self, item_id):
        task = self.tasks.get(item_id)
        if not task:
            return
        sys_temp_dir = tempfile.gettempdir()
        if task.get("filename"):
            base_name = os.path.splitext(os.path.basename(task["filename"]))[0]
            escaped_base = glob.escape(base_name)
            patterns = [
                os.path.join(sys_temp_dir, f"*{escaped_base}*.part"),
                os.path.join(sys_temp_dir, f"*{escaped_base}*.ytdl"),
            ]
            for pattern in patterns:
                for path in glob.glob(pattern):
                    try:
                        os.remove(path)
                    except Exception:
                        continue

    def download_task(self, url, item_id, save_dir, use_impersonate, is_mp3=False):
        has_anime1_lock = False
        try:
            url_lower = url.lower()
            if ("//anime1.me/" in url_lower or "//anime1.pw/" in url_lower) and use_impersonate:
                self._set_task_processing_ui(
                    item_id,
                    t("eta_processing") if "eta_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                )
                anime1_dl_lock.acquire()
                has_anime1_lock = True
            self._download_task_internal(url, item_id, save_dir, use_impersonate, is_mp3)
            if has_anime1_lock:
                anime1_dl_lock.release()
                has_anime1_lock = False

            def finish_dl():
                self._schedule_summary_refresh()

            self._schedule_ui_call(finish_dl)
        except (StopDownloadException, KeyboardInterrupt):
            self._handle_stopped_download(item_id)
        except Exception as exc:
            traceback.print_exc()
            write_error_log(
                "download_task outer exception",
                exc,
                url=url,
                item_id=item_id,
                save_dir=save_dir,
                use_impersonate=use_impersonate,
                is_mp3=is_mp3,
            )
            self._set_task_error_ui(
                item_id,
                t("msg_download_error", error=str(exc)[:45]) if "msg_download_error" in I18N_DICT.get(CURRENT_LANG, {}) else summarize_error_message(exc, "err_net", 120),
            )

            def update_err_state():
                task = self.tasks.get(item_id)
                if not task:
                    return
                task["state"] = "ERROR"

            self._schedule_ui_call(update_err_state)
        finally:
            if has_anime1_lock:
                anime1_dl_lock.release()
            self._schedule_process_queue()

    def _download_task_internal(self, url, item_id, save_dir, use_impersonate, is_mp3=False):
        task = self.tasks.get(item_id, {})
        short_name = task.get("short_name", "")
        if not short_name:
            short_name = t("msg_resume_name") if "msg_resume_name" in I18N_DICT.get(CURRENT_LANG, {}) else "未完成項目"

        safe_name = "".join(ch for ch in short_name if ch not in '\\/:*?"<>|').strip() or short_name
        possible_exts = (".mp4", ".mkv", ".webm", ".m4a")
        if is_mp3:
            possible_exts = (".mp3", ".m4a")
        if self._complete_if_output_exists(item_id):
            return

        def progress_hook(d):
            task_state = self.tasks.get(item_id, {}).get("state")
            if task_state == "PAUSE_REQUESTED":
                raise StopDownloadException("pause requested")
            if task_state == "DELETE_REQUESTED":
                raise KeyboardInterrupt()
            status = d.get("status")
            if status == "downloading":
                target_path = d.get("tmpfilename") or d.get("filename") or save_dir
                downloaded = d.get("downloaded_bytes") or 0
                total = d.get("total_bytes") or d.get("total_bytes_estimate")
                required_bytes = max(int(total) - int(downloaded), 0) if total else None
                if self._maybe_auto_pause_for_disk_space(item_id, target_path, required_bytes=required_bytes, note=self._disk_full_pause_text()):
                    raise StopDownloadException("disk space low")
            if status == "finished":
                self.update_tree(item_id, "progress", "100%", force=True)
                self._set_task_processing_ui(item_id)
                return
            if status != "downloading":
                return
            downloaded = d.get("downloaded_bytes")
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            percent = format_progress_percent(downloaded, total) if total else None
            if percent is not None:
                self.update_tree(item_id, "progress", f"{percent:.1f}%")
            speed = d.get("speed")
            eta = d.get("eta")
            speed_text = format_transfer_rate(speed) if speed else "-"
            if eta not in (None, ""):
                speed_text = f"{speed_text} | {format_eta(float(eta))}"
            self.update_tree(item_id, "speed_eta", speed_text)
            status_text = self._downloading_status_text()
            if self.tasks.get(item_id, {}).get("_last_status_text") != status_text:
                self._set_task_downloading_ui(item_id)

        ydl_opts = {
            "color": "never",
            "nopart": False,
            "nocheckcertificate": True,
            "retries": 20,
            "fragment_retries": 20,
            "file_access_retries": 3,
            "skip_unavailable_fragments": False,
            "concurrent_fragment_downloads": 3,
            "paths": {"home": save_dir, "temp": tempfile.gettempdir()},
            "outtmpl": {"default": "%(title)s.%(ext)s"},
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
            "progress_hooks": [progress_hook],
            "ignoreerrors": False,
            "no_warnings": False,
            "quiet": True,
            "http_headers": {
                "User-Agent": DEFAULT_USER_AGENT,
            },
            "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
            "ffmpeg_location": _APP_DIR,
        }
        if is_mp3:
            ydl_opts["format"] = "bestaudio/best"
            ydl_opts["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]

        social_cookie_sources = []

        def _run_yt_dlp(target_url):
            attempt_sources = [None]
            for source in social_cookie_sources:
                if source not in attempt_sources:
                    attempt_sources.append(source)
            last_exc = None
            for source in attempt_sources:
                current_opts = copy.deepcopy(ydl_opts)
                if source:
                    current_opts["cookiesfrombrowser"] = source
                else:
                    current_opts.pop("cookiesfrombrowser", None)
                try:
                    with ytdl_init_lock:
                        ydl = yt_dlp.YoutubeDL(current_opts)
                    ydl.download([target_url])
                    return
                except (StopDownloadException, KeyboardInterrupt):
                    raise
                except Exception as exc:
                    last_exc = exc
                    continue
            if last_exc:
                raise last_exc

        def _set_task_identity(name=None, source_site=None, source_page=None, fallback_urls=None):
            updates = {}
            if name:
                task["short_name"] = name
                self.update_tree(item_id, "name", name, force=True)
                updates["name"] = name
            if source_site:
                task["source_site"] = source_site
                updates["source_site"] = source_site
            if source_page:
                task["source_page"] = source_page
                updates["source_page"] = source_page
            if fallback_urls is not None:
                normalized_urls = [_normalize_download_url(candidate) for candidate in (fallback_urls or [])]
                normalized_urls = [candidate for candidate in normalized_urls if candidate]
                task["fallback_urls"] = normalized_urls
                updates["fallback_urls"] = normalized_urls
            if updates:
                update_state_entry(task["url"], **updates)

        def _extract_html_title(page_text, fallback_name):
            title_m = re.search(r"<title>(.*?)</title>", page_text, re.IGNORECASE | re.DOTALL)
            if not title_m:
                return fallback_name
            raw_title = html.unescape(title_m.group(1)).strip()
            raw_title = re.sub(r"\s+", " ", raw_title)
            for splitter in (" - ", " | ", " – ", " — "):
                if splitter in raw_title:
                    raw_title = raw_title.split(splitter)[0].strip()
                    break
            return raw_title or fallback_name

        def _parse_js_object(blob):
            text = (blob or "").strip().rstrip(";")
            try:
                return json.loads(text)
            except Exception:
                pass
            text = re.sub(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", r'\1"\2":', text)
            text = text.replace("'", '"')
            text = re.sub(r",\s*([}\]])", r"\1", text)
            return json.loads(text)

        parsed_url = urllib.parse.urlparse(url)
        if any(host in parsed_url.netloc for host in ("instagram.com", "facebook.com", "threads.net")):
            social_cookie_sources = [source for source in _detect_browser_cookie_sources() if source and source[0] == "firefox"]
            if social_cookie_sources:
                ydl_opts.setdefault("cookiesfrombrowser", social_cookie_sources[0])
        is_youtube = any(host in parsed_url.netloc for host in ("youtube.com", "youtu.be"))
        if is_youtube:
            use_impersonate = False
            task["source_site"] = "youtube"
            ydl_opts["noplaylist"] = True
            update_state_entry(task["url"], source_site="youtube")

        forced_m3u8_site = _match_forced_m3u8_site(url, task)
        if forced_m3u8_site:
            site_config = FORCED_M3U8_SITE_RULES[forced_m3u8_site]
            referer = site_config["referer"]
            origin = site_config["origin"]
            if forced_m3u8_site == "xiaoyakankan":
                referer = task.get("source_page") or referer
                parsed_ref = urllib.parse.urlparse(referer)
                if parsed_ref.scheme and parsed_ref.netloc:
                    origin = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
            ydl_opts["http_headers"]["Referer"] = referer
            ydl_opts["http_headers"]["Origin"] = origin
            write_error_log(
                "m3u8 route selected",
                Exception(f"route=ffmpeg site={forced_m3u8_site}"),
                url=url,
                item_id=item_id,
                source_site=task.get("source_site"),
                fallback_count=len(task.get("fallback_urls", [])),
            )
            self._download_m3u8_with_ffmpeg(item_id, url, save_dir, is_mp3=is_mp3, referer=referer, origin=origin)
            return

        is_direct_media = any(parsed_url.path.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".webm", ".m4a", ".mp3"))
        if is_direct_media:
            self.update_tree(item_id, "speed_eta", self._eta_direct_media_text(), force=True)
            filename = os.path.basename(parsed_url.path) or "downloaded_file"
            task["filename"] = os.path.join(save_dir, filename)
            self.update_tree(item_id, "name", filename, force=True)
            try:
                self._download_http_media(item_id, url, task["filename"], headers={"User-Agent": DEFAULT_USER_AGENT})
                return
            except Exception as e:
                self.update_tree(item_id, "speed_eta", f"Direct media download failed: {str(e)[:30]}", force=True)

        if "jable.tv" in parsed_url.netloc:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_jable", "正在解析 Jable..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=15)
            m = re.search(r'(https://[^\s"\'\\]+\.m3u8[^\s"\'\\]*)', resp.text)
            if not m:
                m = re.search(r'hlsUrl\s*=\s*["\']([^"\']+\.m3u8.*?)["\']', resp.text)
            if not m:
                raise Exception("Failed to locate hlsUrl on the Jable page")
            url = html.unescape(m.group(1)).strip()
            title_m = re.search(r"<title>(.*?)</title>", resp.text, re.IGNORECASE | re.DOTALL)
            clean_title = short_name
            if title_m:
                raw_title = html.unescape(title_m.group(1)).strip()
                clean_title = raw_title.split(" - ")[0].strip() or short_name
            task["short_name"] = clean_title
            task["source_site"] = "jable"
            update_state_entry(task["url"], name=clean_title, source_site="jable")
            self.update_tree(item_id, "name", clean_title, force=True)
            self._set_task_downloading_ui(item_id, self._eta_found_stream_text())
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=jable"), url=url, item_id=item_id, source_site="jable", fallback_count=0)
            self._download_m3u8_with_ffmpeg(item_id, url, save_dir, is_mp3=is_mp3, referer="https://jable.tv/", origin="https://jable.tv")
            return

        if "njavtv.com" in parsed_url.netloc:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_njavtv", "正在解析 NJAVTV..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=15)
            m = re.search(r"(https://surrit\.com/[^\s\"']+/playlist\.m3u8)", resp.text)
            if not m:
                unpacked = unpack_packed_javascript(resp.text)
                if unpacked:
                    m = re.search(r"(https://surrit\.com/[^\s\"']+/playlist\.m3u8)", unpacked)
            if not m:
                m = re.search(r"source\s*=\s*[\"'](https://surrit\.com/[^\s\"']+/playlist\.m3u8)[\"']", resp.text)
            if not m:
                raise Exception("Failed to locate NJAVTV player script")
            stream_url = html.unescape(m.group(1)).strip()
            slug = parsed_url.path.strip("/").split("/")[-1]
            title = slug.upper()
            code_m = re.search(r"([A-Za-z]+)-(\d+)", slug, re.IGNORECASE)
            if code_m:
                title = f"{code_m.group(1).upper()}-{code_m.group(2)}"
            task["short_name"] = title
            task["source_site"] = "njavtv"
            update_state_entry(task["url"], name=title, source_site="njavtv")
            self.update_tree(item_id, "name", title, force=True)
            self._download_m3u8_with_ffmpeg(item_id, stream_url, save_dir, is_mp3=is_mp3, referer="https://njavtv.com/", origin="https://njavtv.com")
            return

        if "xiaoyakankan.com" in parsed_url.netloc:
            write_error_log("xiaoyakankan parse start", Exception("xiaoyakankan parse start"), url=url, item_id=item_id)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=15, headers={"Referer": url})
            try:
                match = re.search(r"var\s+pp\s*=\s*(\{.*?\})\s*;", resp.text, re.DOTALL)
                if not match:
                    raise Exception("Failed to locate xiaoyakankan pp data")
                data = json.loads(match.group(1))
                lines = data.get("lines") or []
                parsed = urllib.parse.parse_qs(parsed_url.query)
                vod_raw = (parsed.get("vod") or [""])[0]
                vod_key = vod_raw.split("-", 1)[0] if "-" in vod_raw else vod_raw
                episode_index = 0
                if "-" in vod_raw:
                    try:
                        episode_index = max(int(vod_raw.rsplit("-", 1)[-1]), 0)
                    except ValueError:
                        episode_index = 0
                selected_entry = next((row for row in lines if isinstance(row, list) and row and str(row[0]) == vod_key), None)
                if not selected_entry or len(selected_entry) < 4:
                    raise Exception("Failed to select xiaoyakankan line")
                candidates = selected_entry[3]
                if not isinstance(candidates, list) or not candidates:
                    raise Exception("Failed to extract xiaoyakankan episode list")
                if episode_index >= len(candidates):
                    raise Exception("Failed to select xiaoyakankan episode")
                chosen_urls = []
                primary_url = _normalize_download_url(candidates[episode_index])
                if primary_url:
                    chosen_urls.append(primary_url)
                for row in lines:
                    if not isinstance(row, list) or len(row) < 4:
                        continue
                    row_candidates = row[3]
                    if not isinstance(row_candidates, list) or episode_index >= len(row_candidates):
                        continue
                    fallback_url = _normalize_download_url(row_candidates[episode_index])
                    if fallback_url and fallback_url not in chosen_urls:
                        chosen_urls.append(fallback_url)
                m3u8_url = chosen_urls[0] if chosen_urls else None
                fallback_urls = chosen_urls[1:] if len(chosen_urls) > 1 else []
                if not m3u8_url:
                    raise Exception("Failed to extract xiaoyakankan m3u8")
                task["source_site"] = "xiaoyakankan"
                task["fallback_urls"] = [u for u in fallback_urls if u]
                task["source_page"] = url
                update_state_entry(task["url"], source_site="xiaoyakankan", fallback_urls=task["fallback_urls"], source_page=url)
                write_error_log("xiaoyakankan parse success", Exception("xiaoyakankan parse success"), url=url, item_id=item_id, stream_url=m3u8_url, fallback_count=len(task["fallback_urls"]))
                write_error_log("m3u8 route selected", Exception("route=ffmpeg site=xiaoyakankan"), url=m3u8_url, item_id=item_id, source_site="xiaoyakankan", fallback_count=len(task["fallback_urls"]))
                self._download_m3u8_with_ffmpeg(item_id, m3u8_url, save_dir, is_mp3=is_mp3, referer=url, origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
                return
            except Exception as e:
                write_error_log("xiaoyakankan parse failure", e, url=url, item_id=item_id)
                self.update_tree(item_id, "speed_eta", self._site_parse_error_text(e), force=True)

        if "movieffm.net" in parsed_url.netloc and "/drama/" not in parsed_url.path:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_movieffm", "正在解析 MovieFFM 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": "https://www.movieffm.net/"})
            player_match = re.search(r"player_aaaa\s*=\s*(\{.*?\})", resp.text, re.DOTALL)
            if not player_match:
                raise Exception("MovieFFM player data not found")
            player_data = _parse_js_object(player_match.group(1))
            candidates = []
            primary_url = _normalize_download_url(player_data.get("url"))
            if primary_url:
                candidates.append(primary_url)
            for key in ("urls", "backup", "backup_urls", "m3u8_urls"):
                value = player_data.get(key)
                if isinstance(value, list):
                    candidates.extend(_normalize_download_url(candidate) for candidate in value)
            candidates = [candidate for candidate in candidates if candidate]
            if not candidates:
                raise Exception("MovieFFM stream URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="movieffm", source_page=url, fallback_urls=candidates[1:])
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=movieffm"), url=candidates[0], item_id=item_id, source_site="movieffm", fallback_count=len(task.get("fallback_urls", [])))
            self._download_m3u8_with_ffmpeg(item_id, candidates[0], save_dir, is_mp3=is_mp3, referer="https://www.movieffm.net/", origin="https://www.movieffm.net")
            return

        if "gimy" in parsed_url.netloc and ("/detail/" in parsed_url.path or "/voddetail/" in parsed_url.path or "/voddetail2/" in parsed_url.path or "/vod/" in parsed_url.path):
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_gimy", "正在解析 Gimy 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": url}
            resp_text = None
            last_detail_error = None
            for impersonate_name in ("chrome110", "chrome120", "edge101"):
                try:
                    resp_text = c_req.get(url, impersonate=impersonate_name, timeout=15, headers=headers).text
                    break
                except Exception as inner_exc:
                    last_detail_error = inner_exc
            if resp_text is None:
                try:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=20) as resp_obj:
                        resp_text = resp_obj.read().decode("utf-8", "ignore")
                except Exception as fallback_exc:
                    raise fallback_exc from last_detail_error
            base = f"{parsed_url.scheme}://{parsed_url.netloc}"
            matches = list(
                re.finditer(
                    r'href=[\"\'](/(?:(?:vod)?play/[0-9]+\-[0-9]+\-[0-9]+\.html|video/[0-9]+\-[0-9]+\.html(?:#sid=\d+)?|eps/[0-9]+\-[0-9]+(?:\-[0-9]+)?\.html))[\"\'][^>]*>(.*?)</a>',
                    resp_text,
                )
            )
            if not matches:
                raise Exception("Gimy detail page did not expose episode links")
            drama_name = short_name or "Gimy"
            title_match = re.search(r"<title>(.*?)</title>", resp_text, re.IGNORECASE | re.DOTALL)
            if title_match:
                drama_name = html.unescape(title_match.group(1)).split("-")[0].strip() or drama_name
                drama_name = "".join(c for c in drama_name if c not in '\\/:*?"<>|')
            seen_titles = set()
            first_episode_url = None
            first_episode_name = None
            for match in matches:
                link = match.group(1)
                title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
                if not title:
                    continue
                link_lower = link.lower()
                title_lower = title.lower()
                if "yu-gao" in link_lower or "預告" in title or "预告" in title or "trailer" in title_lower or "preview" in title_lower:
                    continue
                normalized_title = re.sub(r"\s+", "", title)
                if normalized_title in seen_titles:
                    continue
                seen_titles.add(normalized_title)
                first_episode_url = urllib.parse.urljoin(base, link)
                first_episode_name = " ".join(part for part in (drama_name, title) if part).strip()
                break
            if not first_episode_url:
                raise Exception("Gimy detail page did not expose a playable episode")
            _set_task_identity(name=first_episode_name or drama_name, source_site="gimy", source_page=url, fallback_urls=[])
            self._download_task_internal(first_episode_url, item_id, save_dir, use_impersonate, is_mp3)
            return

        if "gimy" in parsed_url.netloc and "/eps/" in parsed_url.path:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_gimy", "正在解析 Gimy 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            stream_candidates = []
            last_gimy_error = None
            def gimy_fetch_text(target_url, referer_value, impersonate_name):
                headers = {"Referer": referer_value, "User-Agent": DEFAULT_USER_AGENT}
                try:
                    resp_obj = c_req.get(target_url, impersonate=impersonate_name, timeout=12, headers=headers)
                    return resp_obj.text
                except Exception as inner_exc:
                    last_exc = inner_exc
                try:
                    req = urllib.request.Request(target_url, headers=headers)
                    with urllib.request.urlopen(req, timeout=15) as resp_obj:
                        return resp_obj.read().decode("utf-8", "ignore")
                except Exception as fallback_exc:
                    raise fallback_exc from last_exc
            for gimy_impersonate in ("chrome110", "chrome120", "edge101"):
                try:
                    resp_text = gimy_fetch_text(url, url, gimy_impersonate)
                except Exception as e:
                    last_gimy_error = e
                    continue

                player_match = re.search(r"var\s+player(?:_aaaa|_data)?\s*=\s*(\{.*?\})\s*(?:</script>|;)", resp_text, re.DOTALL)
                if player_match:
                    try:
                        player_data = json.loads(player_match.group(1))
                        stream_url = html.unescape(str(player_data.get("url") or "")).replace("\\/", "/").strip()
                        if stream_url.startswith("//"):
                            stream_url = "https:" + stream_url
                        if stream_url and stream_url not in stream_candidates:
                            stream_candidates.append(_normalize_download_url(stream_url))
                        player_title = (player_data.get("vod_data") or {}).get("vod_name")
                        if player_title:
                            page_title = re.sub(r"\s+", " ", str(player_title)).strip() or page_title
                    except Exception as e:
                        last_gimy_error = e

                direct_match = re.search(r"var\s+url\s*=\s*['\"]([^'\"]+\.m3u8[^'\"]*)['\"]", resp_text, re.IGNORECASE)
                if not direct_match:
                    direct_match = re.search(r'(https?://[^"\']+\.m3u8[^"\']*)', resp_text)
                direct_stream = _normalize_download_url(direct_match.group(1)) if direct_match else None
                if direct_stream and direct_stream not in stream_candidates:
                    stream_candidates.append(direct_stream)

                iframe_urls = []
                for match in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', resp_text, re.IGNORECASE):
                    iframe_url = _normalize_download_url(urllib.parse.urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}", match.group(1)))
                    if iframe_url and iframe_url not in iframe_urls:
                        iframe_urls.append(iframe_url)
                for match in re.finditer(r'/_watch/(\d+)', resp_text, re.IGNORECASE):
                    iframe_url = urllib.parse.urljoin(f"{parsed_url.scheme}://{parsed_url.netloc}", f"/_watch/{match.group(1)}")
                    iframe_url = _normalize_download_url(iframe_url)
                    if iframe_url and iframe_url not in iframe_urls:
                        iframe_urls.append(iframe_url)

                for iframe_url in iframe_urls:
                    try:
                        iframe_text = gimy_fetch_text(iframe_url, url, gimy_impersonate)
                    except Exception as e:
                        last_gimy_error = e
                        continue
                    m3u8_match = re.search(r"var\s+url\s*=\s*['\"]([^'\"]+\.m3u8[^'\"]*)['\"]", iframe_text, re.IGNORECASE)
                    if not m3u8_match:
                        m3u8_match = re.search(r'(https?://[^"\']+\.m3u8[^"\']*)', iframe_text)
                    stream_url = _normalize_download_url(m3u8_match.group(1)) if m3u8_match else None
                    if stream_url and stream_url not in stream_candidates:
                        stream_candidates.append(stream_url)

                if stream_candidates:
                    break
                if "播放失效" in resp_text or "播放失败" in resp_text or '<p class="p-2 text-error"' in resp_text:
                    last_gimy_error = Exception("Gimy episode page reports playback failure")
                    continue
            if not stream_candidates:
                if last_gimy_error:
                    raise last_gimy_error
                raise Exception("Gimy iframe stream URL missing")
            reachable = []
            unreachable = []
            for candidate in stream_candidates:
                try:
                    head_resp = c_req.head(
                        candidate,
                        impersonate="chrome110",
                        timeout=15,
                        headers={"Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/"},
                        allow_redirects=True,
                        verify=False,
                    )
                    status_code = int(getattr(head_resp, "status_code", 0) or 0)
                    content_type = (head_resp.headers.get("Content-Type", "") or "").lower()
                    if 200 <= status_code < 400 and (".m3u8" in candidate.lower() or "mpegurl" in content_type):
                        reachable.append(candidate)
                    else:
                        unreachable.append(candidate)
                except Exception:
                    unreachable.append(candidate)
            ordered_candidates = reachable + [candidate for candidate in unreachable if candidate not in reachable]
            stream_url = ordered_candidates[0]
            page_title = _extract_html_title(resp_text, short_name)
            fallback_urls = ordered_candidates[1:] if len(ordered_candidates) > 1 else []
            _set_task_identity(name=page_title, source_site="gimy", source_page=task.get("source_page") or url, fallback_urls=fallback_urls)
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=gimy"), url=stream_url, item_id=item_id, source_site="gimy", fallback_count=len(fallback_urls))
            self._download_m3u8_with_ffmpeg(item_id, stream_url, save_dir, is_mp3=is_mp3, referer=url, origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
            return

        if "gimy" in parsed_url.netloc and ("/play/" in parsed_url.path or "/vodplay/" in parsed_url.path or "/video/" in parsed_url.path):
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_gimy", "正在解析 Gimy 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            player_match = re.search(r"var\s+player_data\s*=\s*(\{.*?\})\s*(?:<|;)", resp.text, re.DOTALL)
            if not player_match:
                player_match = re.search(r"var\s+player_aaaa\s*=\s*(\{.*?\})\s*(?:</script>|;)", resp.text, re.DOTALL)
            if not player_match:
                raise Exception("Gimy player_data not found")
            player_data = _parse_js_object(player_match.group(1))
            stream_url = _normalize_download_url(player_data.get("url"))
            if not stream_url:
                raise Exception("Gimy stream URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="gimy", source_page=url, fallback_urls=[])
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=gimy"), url=stream_url, item_id=item_id, source_site="gimy", fallback_count=0)
            self._download_m3u8_with_ffmpeg(item_id, stream_url, save_dir, is_mp3=is_mp3, referer=f"{parsed_url.scheme}://{parsed_url.netloc}/", origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
            return

        if "hanime1.me" in parsed_url.netloc and "watch" in parsed_url.path:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_hanime", "正在解析 Hanime1 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": "https://hanime1.me/"})
            source_match = re.search(r'<source\s+[^>]*src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE)
            if not source_match:
                source_match = re.search(r'(https?://[^"\'\s]+\.m3u8[^"\'\s]*)', resp.text)
            stream_url = _normalize_download_url(source_match.group(1)) if source_match else None
            if not stream_url:
                raise Exception("Hanime1 source URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="hanime1", source_page=url, fallback_urls=[])
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=hanime1"), url=stream_url, item_id=item_id, source_site="hanime1", fallback_count=0)
            self._download_m3u8_with_ffmpeg(item_id, stream_url, save_dir, is_mp3=is_mp3, referer="https://hanime1.me/", origin="https://hanime1.me")
            return

        if "missav" in parsed_url.netloc:
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_missav", "正在解析 MissAV 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome120", timeout=20, headers={"Referer": url})
            candidates = _extract_missav_m3u8_candidates(resp.text)
            if not candidates:
                raise Exception("Failed to extract MissAV stream URL")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="missav", source_page=url, fallback_urls=candidates[1:])
            write_error_log("m3u8 route selected", Exception("route=ffmpeg site=missav"), url=candidates[0], item_id=item_id, source_site="missav", fallback_count=len(task.get("fallback_urls", [])))
            self._download_m3u8_with_ffmpeg(item_id, candidates[0], save_dir, is_mp3=is_mp3, referer=url, origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
            return

        if "avjoy.me" in parsed_url.netloc and "/video/" in parsed_url.path:
            self.update_tree(item_id, "speed_eta", self._eta_direct_media_text(), force=True)
            c_req = get_curl_cffi_requests()
            safe_referer = f"{parsed_url.scheme}://{parsed_url.netloc}/"
            resp = c_req.get(url, impersonate="chrome120", timeout=20, headers={"Referer": safe_referer})
            candidates = _extract_candidate_media_urls(resp.text, allowed_exts=(".mp4", ".m3u8", ".mpd"))
            media_url = next((candidate for candidate in candidates if candidate.lower().endswith(".mp4")), None)
            if not media_url:
                media_url = next((candidate for candidate in candidates if any(ext in candidate.lower() for ext in (".m3u8", ".mpd"))), None)
            if not media_url:
                raise Exception("AVJOY media URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            fallback_urls = [u for u in candidates if u != media_url]
            _set_task_identity(name=page_title, source_site="avjoy", source_page=url, fallback_urls=fallback_urls)
            out_path = os.path.join(save_dir, re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() + ".mp4")
            if any(ext in media_url.lower() for ext in (".m3u8", ".mpd")):
                write_error_log("m3u8 route selected", Exception("route=ffmpeg site=avjoy"), url=media_url, item_id=item_id, source_site="avjoy", fallback_count=len(fallback_urls))
                self._download_m3u8_with_ffmpeg(item_id, media_url, save_dir, is_mp3=is_mp3, referer=safe_referer, origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
            elif is_mp3:
                self._download_direct_media_audio_with_ffmpeg(item_id, media_url, save_dir, referer=safe_referer, origin=f"{parsed_url.scheme}://{parsed_url.netloc}")
            else:
                self._download_http_media(item_id, media_url, out_path, headers={"Referer": safe_referer, "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}", "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
            return

        if "threads.net" in parsed_url.netloc or parsed_url.netloc.startswith("www.threads."):
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_threads", "正在解析 Threads 頁面..."), force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            versions_match = re.search(r'\\"video_versions\\"\\:\s*(\[.*?\])', resp.text)
            if not versions_match:
                versions_match = re.search(r'"video_versions"\s*:\s*(\[.*?\])', resp.text)
            if not versions_match:
                raise Exception("Threads video_versions missing")
            video_versions = _parse_js_object(versions_match.group(1))
            if not isinstance(video_versions, list) or not video_versions:
                raise Exception("Threads video_versions empty")
            best = max(video_versions, key=lambda item: int((item or {}).get("width") or 0) * int((item or {}).get("height") or 0))
            media_url = _normalize_download_url((best or {}).get("url"))
            if not media_url:
                raise Exception("Threads video URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="threads", source_page=url, fallback_urls=[])
            out_name = re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() or "threads_video"
            ext = os.path.splitext(urllib.parse.urlparse(media_url).path)[1] or ".mp4"
            self._download_http_media(item_id, media_url, os.path.join(save_dir, out_name + ext), headers={"Referer": url, "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
            return

        if "instagram.com" in parsed_url.netloc and any(part in parsed_url.path for part in ("/reel/", "/p/")):
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_instagram", "正在解析 Instagram 頁面..."), force=True)
            shortcode_m = re.search(r"/(?:reel|p)/([^/?#]+)", url)
            if not shortcode_m:
                raise Exception("Instagram shortcode missing")
            shortcode = shortcode_m.group(1)
            c_req = get_curl_cffi_requests()
            session = c_req.Session(impersonate="chrome110")
            base_headers = {
                "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                "Accept": "*/*",
                "X-IG-App-ID": "936619743392459",
                "X-ASBD-ID": "198387",
                "X-IG-WWW-Claim": "0",
                "Origin": "https://www.instagram.com",
                "Referer": "https://www.instagram.com/",
            }
            page_resp = session.get(url, timeout=20, headers={"User-Agent": ydl_opts["http_headers"]["User-Agent"], "Referer": url})
            candidates = _extract_instagram_media_candidates(page_resp.text)

            if not candidates:
                try:
                    embed_resp = session.get(f"https://www.instagram.com/reel/{shortcode}/embed/captioned/", timeout=20, headers={"User-Agent": ydl_opts["http_headers"]["User-Agent"], "Referer": url})
                    candidates.extend(_extract_instagram_media_candidates(embed_resp.text))
                except Exception:
                    pass

            if not candidates:
                def _shortcode_to_mediaid(shortcode):
                    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
                    media_id = 0
                    for char in shortcode:
                        media_id = media_id * 64 + alphabet.index(char)
                    return str(media_id)

                api_candidates = [
                    f"https://i.instagram.com/api/v1/media/{_shortcode_to_mediaid(shortcode)}/info/",
                    f"https://www.instagram.com/api/v1/media/{_shortcode_to_mediaid(shortcode)}/info/",
                ]
                for api_url in api_candidates:
                    try:
                        api_resp = session.get(api_url, timeout=20, headers=base_headers)
                        if "json" not in str(api_resp.headers.get("content-type", "")).lower():
                            continue
                        data = api_resp.json()
                        nested = []
                        _walk_media_urls(data, nested)
                        candidates.extend(nested)
                    except Exception:
                        continue

            media_url = next((candidate for candidate in candidates if candidate.lower().endswith(".mp4")), None)
            if not media_url:
                media_url = next((candidate for candidate in candidates if any(ext in candidate.lower() for ext in (".m3u8", ".mpd"))), None)
            if media_url:
                page_title = short_name if short_name and short_name != t("msg_resume_name") else f"Instagram_{shortcode}"
                _set_task_identity(name=page_title, source_site="instagram", source_page=url, fallback_urls=[])
                out_path = os.path.join(save_dir, re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() + ".mp4")
                if any(ext in media_url.lower() for ext in (".m3u8", ".mpd")):
                    write_error_log("m3u8 route selected", Exception("route=ffmpeg site=instagram"), url=media_url, item_id=item_id, source_site="instagram", fallback_count=0)
                    self._download_m3u8_with_ffmpeg(item_id, media_url, save_dir, is_mp3=is_mp3, referer="https://www.instagram.com/", origin="https://www.instagram.com")
                elif is_mp3:
                    self._download_direct_media_audio_with_ffmpeg(item_id, media_url, save_dir, referer="https://www.instagram.com/", origin="https://www.instagram.com")
                else:
                    self._download_http_media(item_id, media_url, out_path, headers={"Referer": "https://www.instagram.com/", "Origin": "https://www.instagram.com", "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
                return
            try:
                fallback = _extract_instagram_media_via_savereels(url)
                media_url = _normalize_download_url(fallback.get("media_url", ""))
                fallback_urls = [_normalize_download_url(u) for u in (fallback.get("fallback_urls") or [])]
                fallback_urls = [u for u in fallback_urls if u and u != media_url]
                if media_url:
                    page_title = str(fallback.get("page_title") or "").strip()
                    if not page_title:
                        page_title = short_name if short_name and short_name != t("msg_resume_name") else f"Instagram_{shortcode}"
                    _set_task_identity(name=page_title, source_site="instagram", source_page=url, fallback_urls=fallback_urls)
                    out_path = os.path.join(save_dir, re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() + ".mp4")
                    write_error_log("instagram savereels fallback", Exception("Instagram SaveReels fallback succeeded"), url=url, item_id=item_id)
                    if any(ext in media_url.lower() for ext in (".m3u8", ".mpd")):
                        write_error_log("m3u8 route selected", Exception("route=ffmpeg site=instagram"), url=media_url, item_id=item_id, source_site="instagram", fallback_count=len(fallback_urls))
                        self._download_m3u8_with_ffmpeg(item_id, media_url, save_dir, is_mp3=is_mp3, referer="https://savereels.app/", origin="https://savereels.app")
                    elif is_mp3:
                        self._download_direct_media_audio_with_ffmpeg(item_id, media_url, save_dir, referer="https://savereels.app/", origin="https://savereels.app")
                    else:
                        self._download_http_media(item_id, media_url, out_path, headers={"Referer": "https://savereels.app/", "Origin": "https://savereels.app", "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
                    return
            except Exception as savereels_exc:
                write_error_log("instagram savereels fallback failed", savereels_exc, url=url, item_id=item_id)
            write_error_log("instagram extractor fallback", Exception("Instagram video URL missing; falling back to yt-dlp"), url=url, item_id=item_id)
            self.update_tree(item_id, "speed_eta", "Instagram 直連解析失敗，改用 yt-dlp...", force=True)

        if "facebook.com" in parsed_url.netloc and any(part in parsed_url.path for part in ("/reel/", "/watch/", "/videos/")):
            self.update_tree(item_id, "speed_eta", "正在解析 Facebook 頁面...", force=True)
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"User-Agent": ydl_opts["http_headers"]["User-Agent"], "Referer": url})
            candidates = _extract_facebook_media_candidates(resp.text)
            graphql_payload = _extract_facebook_graphql_payload(resp.text)
            if graphql_payload:
                try:
                    graphql_resp = c_req.post(
                        "https://www.facebook.com/api/graphql/",
                        impersonate="chrome110",
                        timeout=20,
                        headers={
                            "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Referer": url,
                        },
                        data={
                            "av": "0",
                            "__aaid": "0",
                            "__user": "0",
                            "__a": "1",
                            "__comet_req": "15",
                            "fb_api_caller_class": "RelayModern",
                            "fb_api_req_friendly_name": "FBReelsRootWithEntrypointQuery",
                            "variables": graphql_payload["variables"],
                            "doc_id": graphql_payload["doc_id"],
                            "server_timestamps": "true",
                            "lsd": graphql_payload["lsd"],
                        },
                    )
                    data = graphql_resp.json()
                    nested = []
                    _walk_media_urls(data, nested)
                    candidates.extend(nested)
                except Exception:
                    pass
            media_url = next((candidate for candidate in candidates if candidate.lower().endswith(".mp4")), None)
            if not media_url:
                media_url = next((candidate for candidate in candidates if any(ext in candidate.lower() for ext in (".m3u8", ".mpd"))), None)
            if media_url:
                page_title = short_name if short_name and short_name != t("msg_resume_name") else "Facebook_Video"
                _set_task_identity(name=page_title, source_site="facebook", source_page=url, fallback_urls=[])
                out_path = os.path.join(save_dir, re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() + ".mp4")
                if any(ext in media_url.lower() for ext in (".m3u8", ".mpd")):
                    write_error_log("m3u8 route selected", Exception("route=ffmpeg site=facebook"), url=media_url, item_id=item_id, source_site="facebook", fallback_count=0)
                    self._download_m3u8_with_ffmpeg(item_id, media_url, save_dir, is_mp3=is_mp3, referer=url, origin="https://www.facebook.com")
                elif is_mp3:
                    self._download_direct_media_audio_with_ffmpeg(item_id, media_url, save_dir, referer=url, origin="https://www.facebook.com")
                else:
                    self._download_http_media(item_id, media_url, out_path, headers={"Referer": url, "Origin": "https://www.facebook.com", "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
                return
            write_error_log("facebook extractor fallback", Exception("Facebook media URL missing; falling back to yt-dlp"), url=url, item_id=item_id)
            self.update_tree(item_id, "speed_eta", "Facebook 直連解析失敗，改用 yt-dlp...", force=True)

        if "/status/" in parsed_url.path and any(host in parsed_url.netloc for host in ("twitter.com", "x.com", "fxtwitter.com", "vxtwitter.com")):
            self.update_tree(item_id, "speed_eta", self._eta_site_text("eta_site_twitter", "正在解析 Twitter/X 頁面..."), force=True)
            status_id_m = re.search(r"/status/(\d+)", url)
            if not status_id_m:
                raise Exception("Twitter status id missing")
            status_id = status_id_m.group(1)
            screen_name = parsed_url.path.strip("/").split("/", 1)[0]
            api_url = f"https://api.vxtwitter.com/{screen_name}/status/{status_id}"
            c_req = get_curl_cffi_requests()
            resp = c_req.get(api_url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            data = resp.json()
            media_url = None
            media_extended = (data or {}).get("media_extended") or []
            if isinstance(media_extended, dict):
                variants = ((media_extended.get("video_info") or {}).get("variants") or [])
                for variant in variants:
                    if "mp4" in str(variant.get("content_type", "")) and variant.get("url"):
                        media_url = _normalize_download_url(variant.get("url"))
                        if media_url:
                            break
            if not media_url and isinstance(media_extended, list):
                for media in media_extended:
                    candidate = _normalize_download_url(media.get("url"))
                    if candidate:
                        media_url = candidate
                        break
            if not media_url:
                raise Exception("Twitter/X media URL missing")
            page_title = (data or {}).get("text") or short_name or f"X_{status_id}"
            page_title = re.sub(r"\s+", " ", page_title).strip()[:120]
            _set_task_identity(name=page_title, source_site="twitter", source_page=url, fallback_urls=[])
            out_path = os.path.join(save_dir, re.sub(r'[\\/:*?"<>|]+', "_", page_title).strip() + ".mp4")
            self._download_http_media(item_id, media_url, out_path, headers={"Referer": url, "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}", "User-Agent": ydl_opts["http_headers"]["User-Agent"]})
            return

        page_url = url
        if "anime1" in parsed_url.netloc and use_impersonate:
            try:
                if not _is_anime1_episode_url(page_url):
                    raise Exception(f"Anime1 task URL is not a valid episode page: {page_url}")
                c_req = get_curl_cffi_requests()
                anime1_session = c_req.Session(impersonate="chrome110")
                page_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
                page_headers = {"Referer": page_url, "Origin": page_origin, "User-Agent": DEFAULT_USER_AGENT}
                resp = anime1_session.get(page_url, timeout=15, headers=page_headers)
                title_m = re.search(r"<title>(.*?)(?:\s*&#8211;|\s*\u2013|\s*-)", resp.text, re.IGNORECASE | re.DOTALL)
                clean_title = short_name
                if title_m:
                    clean_title = html.unescape(title_m.group(1).strip())
                m_apireq = re.search(r"data-apireq=[\"']([^\"']+)[\"']", resp.text)
                if m_apireq:
                    data_req = urllib.parse.unquote(m_apireq.group(1))
                    api_resp = anime1_session.post("https://v.anime1.me/api", data={"d": data_req}, headers=page_headers, timeout=15)
                    api_data = api_resp.json()
                    if "s" not in api_data and not api_data.get("src"):
                        raise Exception("Anime1 API response did not contain a source URL")
                    v_src = api_data.get("s", api_data.get("src", ""))
                    if isinstance(v_src, list):
                        picked = ""
                        for item in v_src:
                            if isinstance(item, dict):
                                candidate = item.get("src") or item.get("file") or item.get("url")
                            else:
                                candidate = item
                            if isinstance(candidate, str) and candidate.strip():
                                picked = candidate.strip()
                                break
                        v_src = picked
                    if v_src.startswith("//"):
                        v_src = "https:" + v_src
                    url = v_src
                    out_name = clean_title + ".mp4"
                    out_path = os.path.join(save_dir, out_name)
                    self.update_tree(item_id, "name", out_name, force=True)
                    self.update_tree(item_id, "speed_eta", self._eta_found_media_text(), force=True)
                    self._download_http_media(item_id, url, out_path, headers=page_headers, session=anime1_session)
                    return
                direct_mp4 = re.search(r'<source[^>]+src=["\']([^"\']+\.mp4[^"\']*)["\']', resp.text, re.IGNORECASE)
                if direct_mp4:
                    v_src = html.unescape(direct_mp4.group(1).strip())
                    if v_src.startswith("//"):
                        v_src = "https:" + v_src
                    url = v_src
                    out_name = clean_title + ".mp4"
                    out_path = os.path.join(save_dir, out_name)
                    self.update_tree(item_id, "name", out_name, force=True)
                    self.update_tree(item_id, "speed_eta", self._eta_found_media_text(), force=True)
                    self._download_http_media(item_id, url, out_path, headers=page_headers, session=anime1_session)
                    return
                direct_m3u8 = re.search(r'(https?://[^\s"\'\\]+(?:surrit\.com|[^"\']+)\.m3u8[^\s"\']*)', resp.text)
                if direct_m3u8:
                    url = html.unescape(direct_m3u8.group(1))
                    task["short_name"] = clean_title
                    self.update_tree(item_id, "name", clean_title, force=True)
                    self.update_tree(item_id, "speed_eta", self._eta_found_stream_text(), force=True)
                    self._download_m3u8_with_ffmpeg(item_id, url, save_dir, is_mp3=is_mp3, referer=page_url, origin=page_origin)
                    return
                iframe_m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text)
                if not iframe_m:
                    raise Exception("Anime1 page did not expose a direct stream or iframe URL")
                iframe_url = html.unescape(iframe_m.group(1))
                if iframe_url.startswith("//"):
                    iframe_url = "https:" + iframe_url
                self._set_task_processing_ui(
                    item_id,
                    t("eta_processing") if "eta_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                )
                iframe_resp = c_req.get(iframe_url, headers=page_headers, timeout=15, impersonate="chrome110")
                m3u8_m = re.search(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', iframe_resp.text)
                if not m3u8_m:
                    raise Exception("Anime1 iframe did not contain an m3u8 URL")
                url = m3u8_m.group(1)
                task["short_name"] = clean_title
                self.update_tree(item_id, "name", clean_title, force=True)
                self.update_tree(item_id, "speed_eta", self._eta_found_stream_text(), force=True)
                self._download_m3u8_with_ffmpeg(item_id, url, save_dir, is_mp3=is_mp3, referer=page_url, origin=page_origin)
                return
            except (StopDownloadException, KeyboardInterrupt):
                raise
            except Exception as e:
                write_error_log("anime1 custom parser fallback", e, page_url=page_url, item_id=item_id, use_impersonate=use_impersonate)
                if not _is_anime1_episode_url(page_url):
                    raise
                url = page_url
                self.update_tree(item_id, "speed_eta", self._site_parse_error_text(e), force=True)

        try:
            _run_yt_dlp(url)
            if self.tasks.get(item_id, {}).get("state") == "DELETED":
                raise KeyboardInterrupt()
            self._mark_task_finished(item_id)
        except (StopDownloadException, KeyboardInterrupt):
            self._handle_stopped_download(item_id)
            raise
        except Exception as e:
            task = self.tasks.get(item_id, {})
            write_error_log(
                "yt_dlp download failure",
                e,
                url=url,
                item_id=item_id,
                state=task.get("state"),
                use_impersonate=use_impersonate,
                is_mp3=is_mp3,
            )
            if task.get("state") in ("DELETED", "DELETE_REQUESTED"):
                self._discard_task(item_id)
                return
            if task.get("state") in PAUSED_TASK_STATES:
                return
            self._set_task_error_ui(item_id, summarize_error_message(e, "err_net", 120))
            task["state"] = "ERROR"

    def on_closing(self):
        def _kill_children():
            try:
                import psutil

                parent = psutil.Process(os.getpid())
                for child in parent.children(recursive=True):
                    try:
                        child.kill()
                    except Exception:
                        continue
            except Exception:
                return

        current_downloading = self._count_tasks_in_states("DOWNLOADING")
        if current_downloading > 0:
            if self._ask_warning_yesno(t("msg_close_warn", count=current_downloading)):
                try:
                    self.persist_unfinished_state(force=True)
                except Exception:
                    pass
                _kill_children()
                self.root.destroy()
                os._exit(0)
            return
        try:
            self.persist_unfinished_state(force=True)
        except Exception:
            pass
        _kill_children()
        self.root.destroy()
        os._exit(0)


def main():
    if not acquire_single_instance_lock():
        warning_root = tk.Tk()
        warning_root.withdraw()
        warning_root.attributes("-topmost", True)
        try:
            messagebox.showwarning(_warning_title_text_fallback(), t("msg_already_running"), parent=warning_root)
        finally:
            warning_root.destroy()
        return

    try:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] app start build={APP_BUILD} frozen={getattr(sys, 'frozen', False)} app_dir={_APP_DIR}\n")
    except Exception:
        pass

    if TkinterDnD is not None:
        try:
            root = TkinterDnD.Tk()
        except Exception:
            root = tk.Tk()
    else:
        root = tk.Tk()
    app = DownloadManagerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()









