"""Readable restored source for downloader 2122.

This file was rebuilt from the recovered 2122 executable payload and then
hand-repaired into a maintainable Python source file.
"""

from __future__ import annotations

import concurrent.futures
import copy
import ctypes
import base64
import glob
import hashlib
import html
import json
import locale
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.request
import urllib.parse
import weakref
import zipfile
from functools import lru_cache
from pathlib import Path

try:
    from Crypto.Cipher import AES as CryptoAES
except Exception:
    CryptoAES = None

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

yt_dlp = None

try:
    from mega import Mega as MegaClient
except Exception:
    MegaClient = None


APP_BUILD = "20260521-3000"
CURRENT_LANG = "en_US"
if getattr(sys, "frozen", False):
    _APP_DIR = os.path.abspath(os.path.dirname(sys.executable))
else:
    _APP_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(_APP_DIR, "config.json")
STATE_FILE = os.path.join(_APP_DIR, "downloads.json")
ERROR_LOG_FILE = os.path.join(_APP_DIR, "error.log")
TRACE_LOG_FILE = os.path.join(_APP_DIR, "activity.log")
MAX_DOWNLOADS_PER_DOMAIN = 4
MAX_DOWNLOADS_PER_SOURCE_PAGE = MAX_DOWNLOADS_PER_DOMAIN
MAX_DOWNLOADS_PER_SOURCE_PAGE_BY_SITE = {
    "avbebe": 4,
    "movieffm": 3,
}
MAX_DOWNLOADS_PER_SOURCE_SITE = {
    "avbebe": 4,
    "movieffm": 3,
}
LOW_SPEED_CONCURRENCY_THRESHOLD_BPS = 500 * 1024
LOW_SPEED_CONCURRENCY_MAX_DOWNLOADS = 10
RESUME_LOW_SPEED_REANALYZE_DELAY_SECONDS = 120.0
RESUME_LOW_SPEED_REANALYZE_THRESHOLD_BPS = 64 * 1024
MAX_QUEUE_TASKS = 300
DISK_SPACE_RESERVE_BYTES = 256 * 1024 * 1024
STATE_PERSIST_INTERVAL_SECONDS = 2.5
RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS = 2.0
RESUME_PROGRESS_MIN_BYTES_DELTA = 2 * 1024 * 1024
ERROR_LOG_DEDUPE_WINDOW_SECONDS = 2.0
SINGLE_INSTANCE_ACQUIRE_TIMEOUT_SECONDS = 4.0
SINGLE_INSTANCE_ACQUIRE_RETRY_INTERVAL_SECONDS = 0.2
FORCED_SHUTDOWN_EXIT_DELAY_SECONDS = 8.0
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
FFMPEG_WINDOWS_FFMPEG_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
FFMPEG_WINDOWS_FFPROBE_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
FFMPEG_HLS_RECONNECT_OPTIONS = (
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_on_network_error", "1",
    "-reconnect_on_http_error", "4xx,5xx",
    "-reconnect_delay_max", "10",
    "-reconnect_max_retries", "20",
)
FFMPEG_HLS_RW_TIMEOUT_MICROSECONDS = "15000000"
FFMPEG_FAST_HLS_HTTP_SITES = frozenset((
    "18jav",
    "777tv",
    "99itv",
    "goodav17",
    "hohoj",
    "jable",
    "missav",
    "movieffm",
    "njavtv",
    "nnyy",
    "xiaoyakankan",
))
FFMPEG_UNEXPECTED_RETRY_DELAYS = (1.0, 3.0, 6.0)
PARALLEL_HLS_SEGMENT_SITES = frozenset(("movieffm", "avbebe"))
PARALLEL_HLS_SEGMENT_HOST_MARKERS = ("xluuss", "xlzyd.com", "52cute.com", "turboviplay.com")
PARALLEL_HLS_SEGMENT_WORKERS = 16
PARALLEL_HLS_SEGMENT_WORKERS_BY_SITE = {
    "movieffm": 20,
    "avbebe": 20,
}
PARALLEL_HLS_SEGMENT_WORKERS_BY_HOST = {
    "turbosplayer.com": 8,
    "turboviplay.com": 8,
    "googleusercontent.com": 1,
}
PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS = 30
PARALLEL_HLS_SEGMENT_RETRIES = 5
PARALLEL_HLS_GOOGLE_SEGMENT_RETRIES = 20
PARALLEL_HLS_GOOGLE_RETRY_DELAYS = (5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0)
YTDLP_HLS_NATIVE_SOCKET_TIMEOUT = 10.0
YTDLP_HLS_NATIVE_SOCKET_TIMEOUT_BY_SITE = {
    "gimy": 15.0,
    "99itv": 15.0,
    "777tv": 15.0,
    "18jav": 15.0,
    "hohoj": 15.0,
    "nnyy": 15.0,
    "movieffm": 15.0,
    "missav": 15.0,
    "xiaoyakankan": 15.0,
    "jable": 15.0,
}
YTDLP_HLS_NATIVE_CONCURRENT_FRAGMENTS = 12
YTDLP_HLS_NATIVE_CONCURRENT_FRAGMENTS_BY_SITE = {
    "99itv": 18,
    "777tv": 18,
    "18jav": 24,
    "gimy": 32,
    "hohoj": 12,
    "jable": 18,
    "missav": 24,
    "nnyy": 24,
    "movieffm": 32,
    "xiaoyakankan": 18,
}
YTDLP_HLS_NATIVE_USE_MPEGTS_BY_SITE = {
    "99itv": False,
    "777tv": False,
    "18jav": False,
    "gimy": True,
    "hohoj": False,
    "jable": False,
    "movieffm": False,
    "missav": True,
    "nnyy": False,
    "xiaoyakankan": False,
}
YTDLP_GENERIC_CONCURRENT_FRAGMENTS = 12
YTDLP_GENERIC_CONCURRENT_FRAGMENTS_BY_SITE = {
    "18jav": 18,
    "gimy": 24,
    "hohoj": 18,
    "missav": 24,
    "movieffm": 24,
    "mixdrop": 16,
    "njavtv": 18,
    "99itv": 18,
    "nnyy": 18,
}
YTDLP_HTTP_CHUNK_SIZE = 10 * 1024 * 1024
YTDLP_HTTP_CHUNK_SIZE_BY_SITE = {
    "gimy": 10 * 1024 * 1024,
    "goodav17": 10 * 1024 * 1024,
    "movieffm": 10 * 1024 * 1024,
    "mixdrop": 10 * 1024 * 1024,
    "missav": 10 * 1024 * 1024,
}
YTDLP_THROTTLED_RATE_BPS = 0
YTDLP_THROTTLED_RATE_BPS_BY_SITE = {
}
HTTP_MULTIPART_TRIGGER_SECONDS = 0.15
HTTP_MULTIPART_TRIGGER_SPEED_BPS = 6 * 1024 * 1024
HTTP_MULTIPART_MIN_REMAINING_BYTES = 1 * 1024 * 1024
HTTP_MULTIPART_PART_COUNT_DEFAULT = 20
HTTP_MULTIPART_PART_COUNT_BY_SITE = {
    "18jav": 20,
    "anime1": 4,
    "avjoy": 6,
    "gimy": 24,
    "goodav17": 20,
    "hohoj": 20,
    "jable": 20,
    "mixdrop": 18,
    "movieffm": 24,
    "missav": 20,
    "njavtv": 20,
}
HTTP_MULTIPART_IMMEDIATE_MIN_BYTES = 2 * 1024 * 1024
HTTP_STREAM_CHUNK_SIZE = 4 * 1024 * 1024
HTTP_RANGE_CHUNK_SIZE = 4 * 1024 * 1024
HTTP_FILE_COPY_CHUNK_SIZE = 4 * 1024 * 1024
HTTP_MULTIPART_IMMEDIATE_SITES = frozenset((
    "18jav",
    "777tv",
    "99itv",
    "avjoy",
    "gimy",
    "goodav17",
    "hohoj",
    "jable",
    "missav",
    "mixdrop",
    "movieffm",
    "njavtv",
    "nnyy",
    "xiaoyakankan",
))
YTDLP_DIRECT_MANIFEST_EXTRACT_SITES = frozenset((
    "99itv",
    "777tv",
    "movieffm",
    "gimy",
    "missav",
    "nnyy",
    "xiaoyakankan",
    "jable",
    "18jav",
    "18av",
    "hohoj",
))
NATIVE_HLS_ALWAYS_SITES = frozenset(("99itv", "777tv"))
NATIVE_HLS_NEVER_SITES = frozenset(("njavtv", "3kor", "missav"))
NATIVE_HLS_HOST_MARKERS_BY_SITE = {
    "18av": ("streamfastpro",),
    "18jav": ("hshdkshd", "cdnlab.live"),
    "gimy": ("ppqrrs", "qqqrst", "surrit", "oag7h", "vodcnd", "phimgood", "ryiplay"),
    "hohoj": ("ggjav", "cdnlab.live"),
    "jable": ("mushroomtrack", "hls.sb-cd.com", "cdn77.org"),
    "movieffm": ("xluuss", "lzcdn", "letvoss", "subokk", "bdzybf", "ukubf", "ijycnd", "qsstvw", "gsuus", "hhuus", "huyall", "bfllvip"),
    "nnyy": ("lz-cdn", "lzcdn", "ppqrrs", "qqqrst", "surrit"),
    "xiaoyakankan": ("huyall", "ijycnd", "jisuzyv", "bfvvs", "gsuus", "hhuus", "qsstvw", "subokk"),
}
NATIVE_HLS_GLOBAL_HOST_MARKERS = ("qqqrst.com", "ppqrrs.com", "surrit.com")
M3U8_TOTAL_BYTES_PROBE_WORKERS = 4
M3U8_TOTAL_BYTES_PROBE_WORKERS_BY_SITE = {
    "gimy": 4,
    "movieffm": 4,
}
M3U8_EXACT_TOTAL_BYTES_DISABLED_SITES = frozenset(("gimy", "missav", "movieffm"))
FFMPEG_PROGRESS_IO_POLL_INTERVAL_SECONDS = 2.5
FFMPEG_PROGRESS_UI_UPDATE_INTERVAL_SECONDS = 2.0
FFMPEG_PROGRESS_UI_MIN_BYTES_DELTA = 2 * 1024 * 1024
YTDLP_PROGRESS_UI_UPDATE_INTERVAL_SECONDS = 2.25
YTDLP_PROGRESS_UI_MIN_BYTES_DELTA = 1 * 1024 * 1024
FFMPEG_RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS = 5.0
FFMPEG_RESUME_PROGRESS_MIN_BYTES_DELTA = 8 * 1024 * 1024
HLS_RESUME_REWIND_MIN_SECONDS = 6.0
HLS_RESUME_REWIND_MAX_SECONDS = 20.0
HLS_RESUME_REWIND_SEGMENT_MULTIPLIER = 2.5
UI_THROTTLE_INTERVAL_SECONDS = 2.0
STATUS_STYLE_REFRESH_INTERVAL_MS = 600
SUMMARY_REFRESH_INTERVAL_MS = 1200
STARTUP_RESUME_DELAY_MS = 250
STARTUP_RESUME_BATCH_SIZE = 2
STARTUP_RESUME_BATCH_DELAY_MS = 80
STARTUP_AUTO_INSTALL_FFMPEG_DELAY_MS = 1200
SHUTDOWN_ACTIVE_DOWNLOAD_WAIT_SECONDS = 1.2
SHUTDOWN_RESUME_ARTIFACT_WAIT_SECONDS = 0.8
SHUTDOWN_PROCESS_TERMINATE_TIMEOUT_SECONDS = 0.8
SHUTDOWN_PROCESS_KILL_TIMEOUT_SECONDS = 0.8
GIMY_DIRECT_STREAM_FALLBACK_LIMIT = 1
GIMY_EPISODE_PAGE_PARSE_FALLBACK_LIMIT = 2
GIMY_SOURCE_PAGE_REFRESH_LIMIT = 4
GIMY_SAME_PAGE_SOURCE_REFRESH_LIMIT = 1
TERMINAL_TASK_STATES = frozenset(("FINISHED", "DELETED", "DELETE_REQUESTED"))
PAUSED_TASK_STATES = frozenset(("PAUSED", "PAUSE_REQUESTED"))
IMPERSONATION_SITE_MARKERS = ("missav", "gimy", "movieffm", "xiaoyakankan", "jable", "njavtv", "anime1", "avbebe")
DELETE_CLEANUP_TASK_STATES = frozenset(("PAUSED", "QUEUED"))
DELETE_REQUEST_TASK_STATES = frozenset(("DOWNLOADING", "PAUSED", "PAUSE_REQUESTED", "QUEUED"))
STOP_REQUEST_TASK_STATES = frozenset(("PAUSE_REQUESTED", "DELETE_REQUESTED"))
RESUMABLE_TASK_STATES = frozenset(("PAUSED", "ERROR"))
STOP_REASON_PAUSE = "pause"
STOP_REASON_DELETE = "delete"
STOP_REASONS = frozenset((STOP_REASON_PAUSE, STOP_REASON_DELETE))
_ERROR_LOG_RECENT = {}
ERROR_LOG_MAX_ENTRIES = 10
TRACE_LOG_MAX_ENTRIES = 10
TRACE_LOG_CONTEXTS = frozenset((
    "app start",
    "single instance lock denied",
    "single instance lock recovered after retry",
    "m3u8 route selected",
    "preferred native hls route selected",
    "ffmpeg terminate requested",
    "ffmpeg download started",
    "ffmpeg download finished",
    "parallel hls download started",
    "parallel hls download finished",
    "parallel hls purged invalid resume segments",
    "parallel hls skipped missing leading resume segments",
    "windows compatible mp4 remuxed",
    "windows compatible mp4 remux failed",
    "windows compatible mp4 transcoded",
    "ffmpeg direct audio started",
    "ffmpeg direct audio finished",
    "m3u8 total bytes invalidated",
    "download concurrency limit changed",
    "http multipart download started",
    "direct media fallback retry",
    "avjoy media refresh retry",
    "yt-dlp native hls fallback started",
    "yt-dlp native hls fallback finished",
    "resume low speed reanalysis requested",
    "missav parser retry recovered",
    "xiaoyakankan parse start",
    "xiaoyakankan parse success",
    "instagram savereels fallback",
    "instagram extractor fallback",
    "facebook extractor fallback",
))

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
    "missav": {
        "hosts": (),
        "origin": "https://missav.ws",
        "referer": "https://missav.ws/",
    },
    "njavtv": {
        "hosts": ("surrit.com",),
        "origin": "https://njavtv.com",
        "referer": "https://njavtv.com/",
    },
    "nnyy": {
        "hosts": (),
        "origin": "https://nnyy.in",
        "referer": "https://nnyy.in/",
    },
    "3kor": {
        "hosts": ("3kor.com",),
        "origin": "https://3kor.com",
        "referer": "https://3kor.com/",
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

NATIVE_HLS_PREFERRED_HOSTS = frozenset((
    "play.xluuss.com",
    "hd.ijycnd.com",
))
GIMY_RESOLVED_URL_CACHE_TTL_SECONDS = 180

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
        "eta_site_nnyy": "正在解析 努努影院...",
        "eta_site_movieffm": "正在解析 MovieFFM...",
        "eta_site_gimy": "正在解析 Gimy...",
        "eta_site_hanime": "正在解析 Hanime1...",
        "eta_site_missav": "正在解析 MissAV...",
        "eta_site_threads": "正在解析 Threads...",
        "eta_site_instagram": "正在解析 Instagram...",
        "eta_site_twitter": "正在解析 Twitter/X...",
        "eta_site_mega": "正在解析 MEGA...",
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
        "err_ffmpeg": "FFmpeg 下载失败",
        "eta_site_anime1": "正在解析 Anime1 页面...",
        "menu_open_folder": "打开目标文件夹",
        "msg_dnd_warn1": "目前环境无法使用拖放功能。\n\n若要在原生 Python 环境启用，请安装 tkinterdnd2。",
        "msg_dnd_warn2": "启用拖放功能失败：\n{e}",
        "msg_ffmpeg_install_failed": "FFmpeg 安装失败：\n{error}",
        "msg_ffmpeg_installed": "FFmpeg 已安装完成。",
        "msg_ffmpeg_progress_extract": "下载完成，正在解压缩...",
        "msg_ffmpeg_terminal_failed": "无法自动打开 Terminal。\n\n请手动执行 `brew install ffmpeg`。",
        "msg_ffmpeg_terminal_opened": "已打开 Terminal 执行 `brew install ffmpeg`。\n\n安装完成后，请重新加入 MP3 任务。",
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
        "eta_site_nnyy": "正在解析 努努影院...",
        "eta_site_movieffm": "正在解析 MovieFFM...",
        "eta_site_gimy": "正在解析 Gimy...",
        "eta_site_hanime": "正在解析 Hanime1...",
        "eta_site_missav": "正在解析 MissAV...",
        "eta_site_threads": "正在解析 Threads...",
        "eta_site_instagram": "正在解析 Instagram...",
        "eta_site_twitter": "正在解析 Twitter/X...",
        "eta_site_mega": "正在解析 MEGA...",
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
        "eta_site_nnyy": "Resolving Nunu page...",
        "eta_site_mega": "Resolving MEGA page...",
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
        "eta_site_nnyy": "Nunu ページを解析中...",
        "eta_site_mega": "MEGA ページを解析中...",
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


class StopDownloadException(BaseException):
    """Control-flow exception used by the original downloader runtime."""


class ResumeLowSpeedReanalysisException(Exception):
    """Trigger one source-page reanalysis when a resumed transfer stays too slow."""


class ParallelHlsRetryLaterException(Exception):
    """Keep problematic HLS sources out of ffmpeg fallback when segments are rate-limited."""


class ParallelHlsUnsupportedSegmentContentException(Exception):
    """Raised when an HLS playlist points at non-video segment payloads."""


def t(key, **kwargs):
    text = I18N_DICT.get(CURRENT_LANG, {}).get(key, I18N_DICT.get("zh_TW", {}).get(key, key))
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text


def _normalize_state_entry(entry):
    normalized = dict(entry) if isinstance(entry, dict) else {}
    url = _normalize_download_url(normalized.get("url", ""))
    name = str(normalized.get("name", "")).strip()
    if isinstance(url, str) and url.strip():
        parsed = urllib.parse.urlparse(url.strip())
        host = parsed.netloc.lower()
        if ("anime1.me" in host or "anime1.pw" in host) and ("/category/" in parsed.path.lower() or "cat" in urllib.parse.parse_qs(parsed.query, keep_blank_values=True)):
            query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            cat = query.get("cat", [""])[0]
            if cat:
                url = f"https://anime1.pw/?cat={cat}"
    name_text = "" if name is None else str(name)
    stripped_name = name_text.strip()
    suspicious_name = (
        not stripped_name
        or (stripped_name.count("?") / max(len(stripped_name), 1)) > 0.4
        or stripped_name in {"???", "??????"}
        or "�" in stripped_name
    )
    if suspicious_name:
        normalized_name_url = _normalize_download_url(url)
        if normalized_name_url:
            parsed_name_url = urllib.parse.urlparse(normalized_name_url)
            if ("anime1.me" in parsed_name_url.netloc.lower() or "anime1.pw" in parsed_name_url.netloc.lower()) and ("/category/" in parsed_name_url.path.lower() or "cat" in urllib.parse.parse_qs(parsed_name_url.query, keep_blank_values=True)):
                query = urllib.parse.parse_qs(parsed_name_url.query, keep_blank_values=True)
                cat = query.get("cat", [""])[0]
                name = f"{parsed_name_url.netloc} cat={cat}" if cat else parsed_name_url.netloc
            else:
                slug = parsed_name_url.path.rstrip("/").split("/")[-1]
                name = slug or parsed_name_url.netloc
        else:
            name = ""
    normalized["url"] = url
    normalized["name"] = name
    normalized["is_mp3"] = bool(normalized.get("is_mp3", False))
    normalized["resume_requested"] = bool(normalized.get("resume_requested", False))
    source_site = str(normalized.get("source_site", "") or "").strip().lower() or None
    normalized["source_site"] = source_site
    fallback_urls = normalized.get("fallback_urls", [])
    normalized["fallback_urls"] = [u for u in fallback_urls if isinstance(u, str) and u.strip()] if isinstance(fallback_urls, list) else []
    normalized["source_page"] = _normalize_download_url(normalized.get("source_page", ""))
    normalized["resolved_url"] = _normalize_download_url(normalized.get("resolved_url", ""))
    try:
        normalized["resolved_url_saved_at"] = float(normalized.get("resolved_url_saved_at", 0) or 0)
    except (TypeError, ValueError):
        normalized["resolved_url_saved_at"] = 0.0
    page_refresh_candidates = normalized.get("page_refresh_candidates", [])
    normalized["page_refresh_candidates"] = [u for u in page_refresh_candidates if isinstance(u, str) and u.strip()] if isinstance(page_refresh_candidates, list) else []
    normalized["filename"] = str(normalized.get("filename", "") or "").strip()
    normalized["temp_filename"] = str(normalized.get("temp_filename", "") or "").strip()
    return normalized


def _normalize_download_url(url):
    if not isinstance(url, str):
        return ""
    raw = html.unescape(url).strip()
    if not raw:
        return ""
    lowered_raw = raw.lower()
    if raw.startswith("[") and ("url=" in lowered_raw or "pmoive" in lowered_raw):
        return ""
    parsed = urllib.parse.urlsplit(raw)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    if parsed.scheme and not netloc:
        return ""
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


def _parse_mega_public_file_parts(mega_client, url):
    parser = getattr(mega_client, "_parse_url", None)
    if not callable(parser):
        raise RuntimeError("MEGA URL parser is not available")
    parsed = str(parser(url) or "").strip()
    parts = parsed.split("!")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError("Unsupported MEGA public file link")
    return parts[0], parts[1]


def _find_megacmd_get_command():
    candidates = []
    local_app_data = os.environ.get("LOCALAPPDATA", "")
    if local_app_data:
        base_dir = os.path.join(local_app_data, "MEGAcmd")
        candidates.extend(
            [
                os.path.join(base_dir, "mega-get.exe"),
                os.path.join(base_dir, "mega-get.cmd"),
                os.path.join(base_dir, "mega-get.bat"),
            ]
        )
    for name in ("mega-get.exe", "mega-get.cmd", "mega-get.bat", "mega-get"):
        found = shutil.which(name)
        if found:
            candidates.append(found)
    for path in candidates:
        if not path or not os.path.exists(path):
            continue
        lower_path = path.lower()
        if lower_path.endswith((".bat", ".cmd")):
            return [os.environ.get("ComSpec", "cmd.exe"), "/c", path]
        return [path]
    return None


def _dedupe_download_urls(candidates, primary_url=None):
    primary_normalized = _normalize_download_url(primary_url) if primary_url else ""
    deduped = []
    seen = set()
    for candidate in candidates or []:
        normalized = _normalize_download_url(candidate)
        if not normalized or normalized == primary_normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _task_field_value(task, field_name, default=None):
    target = task if task is not None else {}
    return target.get(field_name, default)


def _task_source_site_name(task, fallback_site=""):
    source_site = str(_task_field_value(task, "source_site", "") or "").strip().lower()
    if source_site:
        return source_site
    return str(fallback_site or "").strip().lower()


def _set_task_aux_fields(task, **fields):
    target = task if task is not None else {}
    for key, value in fields.items():
        target[key] = value
    return target


def _task_gimy_refresh_history(task):
    raw_history = _task_field_value(task, "_gimy_refresh_history", [])
    if isinstance(raw_history, str):
        raw_history = [raw_history]
    normalized_history = []
    for candidate in raw_history or []:
        normalized_candidate = _normalize_download_url(candidate)
        if normalized_candidate and normalized_candidate not in normalized_history:
            normalized_history.append(normalized_candidate)
    return normalized_history


def _task_gimy_page_refresh_candidates(task):
    raw_candidates = _task_field_value(task, "_gimy_page_refresh_candidates", None)
    if raw_candidates is None:
        raw_candidates = _task_field_value(task, "page_refresh_candidates", [])
    if isinstance(raw_candidates, str):
        raw_candidates = [raw_candidates]
    normalized_candidates = []
    for candidate in raw_candidates or []:
        normalized_candidate = _normalize_download_url(candidate)
        if normalized_candidate and normalized_candidate not in normalized_candidates:
            normalized_candidates.append(normalized_candidate)
    return normalized_candidates


def _task_gimy_failed_stream_urls(task):
    raw_candidates = _task_field_value(task, "_gimy_failed_stream_urls", [])
    if isinstance(raw_candidates, str):
        raw_candidates = [raw_candidates]
    normalized_candidates = []
    for candidate in raw_candidates or []:
        normalized_candidate = _normalize_download_url(candidate)
        if normalized_candidate and normalized_candidate not in normalized_candidates:
            normalized_candidates.append(normalized_candidate)
    return normalized_candidates


def _task_gimy_failed_stream_hosts(task):
    raw_hosts = _task_field_value(task, "_gimy_failed_stream_hosts", [])
    if isinstance(raw_hosts, str):
        raw_hosts = [raw_hosts]
    normalized_hosts = []
    for host in raw_hosts or []:
        host_text = str(host or "").strip().lower()
        if host_text and host_text not in normalized_hosts:
            normalized_hosts.append(host_text)
    return normalized_hosts


def _filter_gimy_failed_stream_candidates(task, candidates):
    failed_urls = set(_task_gimy_failed_stream_urls(task))
    failed_hosts = set(_task_gimy_failed_stream_hosts(task))
    filtered = []
    for candidate in candidates or []:
        normalized_candidate = _normalize_download_url(candidate)
        if not normalized_candidate:
            continue
        if normalized_candidate in failed_urls:
            continue
        if urllib.parse.urlsplit(normalized_candidate).netloc.lower() in failed_hosts:
            continue
        if normalized_candidate not in filtered:
            filtered.append(normalized_candidate)
    return filtered


def _filter_gimy_candidate_groups(task, *candidate_groups):
    return tuple(
        _filter_gimy_failed_stream_candidates(task, candidates)
        for candidates in candidate_groups
    )


def _is_gimy_stream_refreshable_error(exc):
    if exc is None:
        return False
    lowered = str(exc).lower()
    return (
        "http error 404" in lowered
        or "404 not found" in lowered
        or "failed to establish a new connection" in lowered
        or "connection refused" in lowered
        or "winerror 10061" in lowered
        or "gimy native hls artifact rejected" in lowered
    )


def _filter_gimy_untried_page_candidates(task, candidates):
    tried = set(_task_gimy_refresh_history(task))
    filtered = []
    for candidate in candidates or []:
        normalized_candidate = _normalize_download_url(candidate)
        if normalized_candidate and normalized_candidate not in tried and normalized_candidate not in filtered:
            filtered.append(normalized_candidate)
    return filtered


def _append_normalized_unique_candidates(target_list, *candidates):
    updated = list(target_list or [])
    for candidate in candidates:
        normalized_candidate = _normalize_download_url(candidate)
        if normalized_candidate and normalized_candidate not in updated:
            updated.append(normalized_candidate)
    return updated


def _event_transfer_metrics(info, default_path="", default_downloaded=0, default_total=None):
    info = info or {}
    total = info.get("total_bytes")
    if not total:
        total = info.get("total_bytes_estimate", default_total)
    return (
        str(info.get("tmpfilename") or info.get("filename") or default_path or ""),
        info.get("downloaded_bytes") or default_downloaded,
        total,
    )


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
    try:
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
    finally:
        try:
            session.close()
        except Exception:
            pass


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


def _extract_twitter_media_candidates(payload):
    candidates = []

    def _append_candidate(url):
        candidate = _normalize_download_url(url)
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    if not isinstance(payload, dict):
        return candidates

    media_extended = payload.get("media_extended") or []
    if isinstance(media_extended, dict):
        variants = ((media_extended.get("video_info") or {}).get("variants") or [])
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            candidate = variant.get("url")
            content_type = str(variant.get("content_type") or "").lower()
            if candidate and (
                "mp4" in content_type
                or any(
                    token in _normalize_download_url(candidate).lower()
                    for token in (".m3u8", ".mpd", ".mp4", ".mkv", ".webm", ".mp3", ".m4a")
                )
            ):
                _append_candidate(candidate)
        direct_url = media_extended.get("url")
        media_type = str(media_extended.get("type") or "").lower()
        if direct_url and media_type == "photo":
            _append_candidate(direct_url)
    elif isinstance(media_extended, list):
        for media in media_extended:
            if not isinstance(media, dict):
                continue
            media_type = str(media.get("type") or "").lower()
            candidate = media.get("url")
            if candidate and media_type in {"photo", "video", "gif"}:
                _append_candidate(candidate)

    tweet = payload.get("tweet") if isinstance(payload.get("tweet"), dict) else payload
    media = tweet.get("media") if isinstance(tweet, dict) else None
    if isinstance(media, dict):
        for photo in media.get("photos") or []:
            if isinstance(photo, dict):
                _append_candidate(photo.get("url"))
        for video in media.get("videos") or []:
            if not isinstance(video, dict):
                continue
            _append_candidate(video.get("url"))
            _append_candidate(video.get("thumbnail_url"))
        external = media.get("external")
        if isinstance(external, dict):
            _append_candidate(external.get("url"))

    return candidates


def _extract_missav_m3u8_candidates(page_html):
    raw_html = str(page_html or "")
    html_variants = [raw_html]
    unescaped_html = html.unescape(raw_html)
    if unescaped_html != raw_html:
        html_variants.append(unescaped_html)
    slash_unescaped_html = raw_html.replace("\\/", "/")
    if slash_unescaped_html not in html_variants:
        html_variants.append(slash_unescaped_html)
    fully_unescaped_html = html.unescape(slash_unescaped_html)
    if fully_unescaped_html not in html_variants:
        html_variants.append(fully_unescaped_html)

    candidates = []
    for variant in html_variants:
        candidates.extend(_extract_candidate_media_urls(variant, allowed_exts=(".m3u8", ".mpd")))
        for blob in _extract_json_script_blocks(variant):
            candidates.extend(_extract_candidate_media_urls(blob, allowed_exts=(".m3u8", ".mpd")))
            try:
                parsed = json.loads(blob)
            except Exception:
                parsed = None
            if parsed is not None:
                nested = []
                _walk_media_urls(parsed, nested)
                for candidate in nested:
                    if _looks_like_manifest_url(candidate):
                        candidates.append(candidate)
        direct_m = re.search(r'source\s*=\s*"([^"]+\.m3u8[^"]*)"', variant, re.IGNORECASE)
        if direct_m:
            candidates.append(html.unescape(direct_m.group(1)).strip())
        for match in re.finditer(r'(?:(?:https?:)?//[^"\'\s<]+?\.m3u8(?:\?[^"\'\s<]*)?)', variant, re.IGNORECASE):
            candidate = html.unescape(match.group(0)).replace("\\/", "/").strip()
            if candidate.startswith("//"):
                candidate = "https:" + candidate
            if candidate.startswith("http://") or candidate.startswith("https://"):
                candidates.append(candidate)
        for match in re.finditer(r'(https?:\\?/\\?/[^"\'\s<]+?\.m3u8(?:\?[^"\'\s<]*)?)', variant, re.IGNORECASE):
            candidate = html.unescape(match.group(1)).replace("\\/", "/").strip()
            if candidate.startswith("http://") or candidate.startswith("https://"):
                candidates.append(candidate)
        for match in re.finditer(r'"hls"\s*:\s*"([^"]+\.m3u8[^"]*)"', variant, re.IGNORECASE):
            candidates.append(html.unescape(match.group(1)).replace("\\/", "/").strip())
        for match in re.finditer(r'"playlist"\s*:\s*"([^"]+\.m3u8[^"]*)"', variant, re.IGNORECASE):
            candidates.append(html.unescape(match.group(1)).replace("\\/", "/").strip())
        for match in re.finditer(r'"(?:file|src|source|video)"\s*:\s*"([^"]+\.m3u8[^"]*)"', variant, re.IGNORECASE):
            candidates.append(html.unescape(match.group(1)).replace("\\/", "/").strip())
        packed_tokens_m = re.search(r"m3u8\|([A-Za-z0-9|]+)", variant)
        if packed_tokens_m:
            tokens = ["m3u8"] + packed_tokens_m.group(1).split("|")

            def _decode_packed_missav_template(template):
                def repl(match):
                    token = match.group(1).lower()
                    try:
                        idx = int(token, 16)
                    except ValueError:
                        return match.group(0)
                    if 0 <= idx < len(tokens):
                        return tokens[idx]
                    return match.group(0)

                decoded = re.sub(r"\b([0-9a-f])\b", repl, template, flags=re.IGNORECASE)
                decoded = html.unescape(decoded).replace("\\/", "/").strip()
                if decoded.startswith("//"):
                    decoded = "https:" + decoded
                return decoded

            for packed_template in re.findall(r"\b[0-9a-f]+://[0-9a-f]+\.[0-9a-f]+/[0-9a-f./-]+", variant, re.IGNORECASE):
                decoded = _decode_packed_missav_template(packed_template)
                if _looks_like_manifest_url(decoded):
                    candidates.append(decoded)
    ordered = _dedupe_download_urls(candidates)
    playlist_prefixes = {
        candidate.lower()[: -len("/playlist.m3u8")]
        for candidate in ordered
        if candidate.lower().endswith("/playlist.m3u8")
    }
    filtered = []
    for candidate in ordered:
        lower_candidate = candidate.lower()
        if lower_candidate.endswith("/source/video.m3u8"):
            base_prefix = lower_candidate[: -len("/source/video.m3u8")]
            if base_prefix in playlist_prefixes:
                continue
        filtered.append(candidate)
    return filtered


def _extract_missav_media_candidates(page_html):
    manifest_candidates = _extract_missav_m3u8_candidates(page_html)
    direct_candidates = []
    page_text = str(page_html or "")
    generic_candidates = _extract_candidate_media_urls(page_text, allowed_exts=(".mp4", ".m3u8", ".mpd"))
    inline_script_blobs = []
    if isinstance(page_text, str) and page_text:
        for match in re.finditer(r"<script\b(?![^>]*\bsrc=)[^>]*>(.*?)</script>", page_text, re.IGNORECASE | re.DOTALL):
            block = str(match.group(1) or "").strip()
            if block:
                inline_script_blobs.append(block)
    script_blobs = _extract_json_script_blocks(page_text) + inline_script_blobs
    for blob in script_blobs:
        generic_candidates.extend(_extract_candidate_media_urls(blob, allowed_exts=(".mp4", ".m3u8", ".mpd")))
        try:
            parsed = json.loads(blob)
        except Exception:
            continue
        nested = []
        _walk_media_urls(parsed, nested)
        generic_candidates.extend(nested)
    for candidate in _dedupe_download_urls(generic_candidates):
        if _looks_like_manifest_url(candidate):
            if candidate not in manifest_candidates:
                manifest_candidates.append(candidate)
        elif _looks_like_http_media_url(candidate):
            direct_candidates.append(candidate)
    return {
        "manifests": _dedupe_download_urls(manifest_candidates),
        "direct_media": _dedupe_download_urls(direct_candidates),
    }


def _response_header_value(headers, name, default=""):
    if not headers or not name:
        return default
    try:
        value = headers.get(name)
        if value is not None:
            return value
    except Exception:
        pass
    wanted = str(name).lower()
    try:
        items = headers.items()
    except Exception:
        return default
    for key, value in items:
        if str(key).lower() == wanted:
            return value
    return default


def _http_response_log_fields(resp, prefix=""):
    response_text = str(getattr(resp, "text", "") or "")
    key_prefix = str(prefix or "")
    response_headers = getattr(resp, "headers", {}) or {}
    return {
        f"{key_prefix}status": getattr(resp, "status_code", None),
        f"{key_prefix}final_url": str(getattr(resp, "url", "") or ""),
        f"{key_prefix}content_type": str(_response_header_value(response_headers, "Content-Type")),
        f"{key_prefix}html_size": len(response_text),
    }


def _missav_alternate_page_urls(url, site_root):
    alternates = []
    normalized_url = _normalize_download_url(url) or str(url or "")
    if normalized_url:
        alternates.append(normalized_url)
    try:
        parsed = urllib.parse.urlsplit(normalized_url)
    except Exception:
        return alternates
    path = parsed.path or ""
    clean_path = path.lstrip("/")
    if not clean_path:
        return alternates
    root = str(site_root or f"{parsed.scheme}://{parsed.netloc}").rstrip("/")
    localized_candidates = []
    dm_match = re.match(r"^dm\d+/(.+)$", clean_path, flags=re.IGNORECASE)
    if dm_match:
        stripped_path = dm_match.group(1).strip("/")
        if stripped_path:
            localized_candidates.append(stripped_path)
    if not re.match(r"^(?:en|ja|ko|zh)(?:/|$)", clean_path, flags=re.IGNORECASE):
        localized_candidates.append("en/" + clean_path)
        if dm_match:
            stripped_path = dm_match.group(1).strip("/")
            if stripped_path:
                localized_candidates.append("en/" + stripped_path)
    if not re.match(r"^dm\d+/", clean_path, flags=re.IGNORECASE):
        localized_candidates.extend(f"dm{suffix}/{clean_path}" for suffix in ("39", "85", "58", "55", "45", "43", "41", "31", "22"))
    for candidate_path in localized_candidates:
        candidate = root + "/" + candidate_path
        if candidate not in alternates:
            alternates.append(candidate)
    return alternates


def _fetch_missav_media_candidates_with_retry(c_req, url, site_root, item_id=None):
    headers = _make_site_root_headers(site_root)
    headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    headers["Accept-Language"] = "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    resp = c_req.get(url, impersonate="chrome120", timeout=20, headers=headers)
    media_candidates = _extract_missav_media_candidates(resp.text)
    candidates = media_candidates.get("manifests") or []
    direct_media_candidates = media_candidates.get("direct_media") or []
    if candidates or direct_media_candidates:
        return resp, media_candidates, candidates, direct_media_candidates

    retry_headers = [
        _make_site_root_headers(site_root),
        _make_ytdlp_http_headers(referer=site_root + "/"),
        _make_browser_page_headers(referer=url, origin=site_root),
    ]
    for retry_header in retry_headers:
        retry_header["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        retry_header["Accept-Language"] = "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    retry_error = None
    retry_urls = _missav_alternate_page_urls(url, site_root)
    for retry_index, retry_url in enumerate(retry_urls, start=1):
        for header_index, retry_header in enumerate(retry_headers, start=1):
            try:
                retry_resp = c_req.get(retry_url, impersonate="chrome120", timeout=25, headers=retry_header)
                retry_text = retry_resp.text or ""
                retry_candidates = _extract_missav_media_candidates(retry_text)
                retry_manifests = retry_candidates.get("manifests") or []
                retry_direct = retry_candidates.get("direct_media") or []
                if retry_manifests or retry_direct:
                    write_error_log(
                        "missav parser retry recovered",
                        Exception("MissAV parser recovered after refetch"),
                        item_id=item_id,
                        url=url,
                        retry_url=retry_url,
                        retry_index=retry_index,
                        header_index=header_index,
                        **_http_response_log_fields(resp, "initial_"),
                        **_http_response_log_fields(retry_resp, "retry_"),
                        manifest_count=len(retry_manifests),
                        direct_count=len(retry_direct),
                    )
                    return retry_resp, retry_candidates, retry_manifests, retry_direct
            except Exception as retry_exc:
                retry_error = retry_exc
    if retry_error is not None:
        write_error_log(
            "missav parser retry failed",
            retry_error,
            item_id=item_id,
            url=url,
            **_http_response_log_fields(resp, "initial_"),
        )
    return resp, media_candidates, candidates, direct_media_candidates


def _extract_movieffm_m3u8_candidates(page_html):
    candidates = []
    for match in re.finditer(r"videourl\s*:\s*'([^']+)'", str(page_html or ""), re.IGNORECASE):
        candidate = _normalize_download_url(html.unescape(match.group(1)).strip())
        if candidate and (_looks_like_manifest_url(candidate) or _looks_like_http_media_url(candidate)):
            candidates.append(candidate)
    for match in re.finditer(r"url\s*:\s*'([^']+\.m3u8[^']*)'", str(page_html or ""), re.IGNORECASE):
        candidate = _normalize_download_url(html.unescape(match.group(1)).strip())
        if candidate:
            candidates.append(candidate)
    for match in re.finditer(r'"url"\s*:\s*"([^"]+\.m3u8[^"]*)"', str(page_html or ""), re.IGNORECASE):
        candidate = _normalize_download_url(html.unescape(match.group(1).replace("\\/", "/")).strip())
        if candidate:
            candidates.append(candidate)
    return _dedupe_download_urls(candidates)


def _normalize_mixdrop_watch_url(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return None
    parsed = urllib.parse.urlsplit(normalized)
    host = parsed.netloc.lower()
    if "mixdrop.ag" not in host and "m1xdrop.click" not in host:
        return normalized
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) >= 2 and path_parts[0] in ("e", "f"):
        ref = path_parts[1]
        return urllib.parse.urlunsplit((parsed.scheme or "https", parsed.netloc, f"/e/{ref}", "", ""))
    return normalized


def _is_mixdrop_direct_media(url, source_page=""):
    media_url = _normalize_download_url(url)
    referer_url = _normalize_download_url(source_page)
    if not media_url:
        return False
    media_host = urllib.parse.urlsplit(media_url).netloc.lower()
    referer_host = urllib.parse.urlsplit(referer_url).netloc.lower() if referer_url else ""
    return (
        "mxcontent.net" in media_host
        and any(host in referer_host for host in ("mixdrop.ag", "m1xdrop.click"))
    )


def _is_expired_signed_media_url(url, now_ts=None):
    normalized = _normalize_download_url(url)
    if not normalized:
        return False
    parsed = urllib.parse.urlsplit(normalized)
    host = parsed.netloc.lower()
    if "mxcontent.net" not in host:
        return False
    query_map = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    expiry_values = query_map.get("e", [])
    if not expiry_values:
        return False
    try:
        expiry_ts = int(float(expiry_values[0]))
    except Exception:
        return False
    current_ts = int(now_ts if now_ts is not None else time.time())
    return expiry_ts <= current_ts + 30


def _extract_movieffm_external_source_urls(page_html):
    candidates = []
    allowed_hosts = (
        "mixdrop.ag",
        "m1xdrop.click",
        "dood.so",
        "dood.pm",
        "dood.wf",
        "dood.re",
        "dood.yt",
        "evoload.io",
    )
    raw_html = str(page_html or "")
    normalized_html = html.unescape(raw_html).replace("\\/", "/")

    def _append_candidate(raw_candidate):
        candidate = _normalize_download_url(str(raw_candidate or "").strip())
        host = urllib.parse.urlsplit(candidate).netloc.lower() if candidate else ""
        if not candidate:
            return
        if not any(allowed_host in host for allowed_host in allowed_hosts):
            return
        if candidate not in candidates:
            candidates.append(candidate)

    for match in re.finditer(r'"videos"\s*:\s*\[(.*?)\]', normalized_html, re.IGNORECASE | re.DOTALL):
        block = match.group(1)
        for url_match in re.finditer(r'"url"\s*:\s*"([^"]+)"', block, re.IGNORECASE):
            raw_candidate = url_match.group(1).strip()
            normalized_candidate = _normalize_mixdrop_watch_url(raw_candidate) or raw_candidate
            _append_candidate(normalized_candidate)
    for match in re.finditer(r'href=["\']([^"\']+\?download[^"\']*)["\']', normalized_html, re.IGNORECASE):
        raw_candidate = match.group(1).strip()
        normalized_candidate = _normalize_mixdrop_watch_url(raw_candidate) or raw_candidate
        _append_candidate(normalized_candidate)
    for shortcode_match in re.finditer(r'\[pmoive\b[^\]]*\burl\s*=\s*["\']([^"\']+)["\']', normalized_html, re.IGNORECASE):
        _append_candidate(shortcode_match.group(1))
    for match in re.finditer(r"videourl\s*:\s*'([^']+)'", normalized_html, re.IGNORECASE):
        _append_candidate(match.group(1))
    for match in re.finditer(r'"url"\s*:\s*"((?:https?:)?//[^"]+)"\s*,\s*"type"\s*:\s*"iframe"', normalized_html, re.IGNORECASE):
        _append_candidate(match.group(1))
    return _dedupe_download_urls(candidates)


def _extract_mixdrop_media_candidates(page_html):
    candidates = []
    raw_html = str(page_html or "")
    html_variants = []
    for variant in (
        raw_html,
        html.unescape(raw_html),
        raw_html.replace("\\/", "/"),
        html.unescape(raw_html.replace("\\/", "/")),
    ):
        if variant and variant not in html_variants:
            html_variants.append(variant)
    for variant in html_variants:
        candidates.extend(_extract_candidate_media_urls(variant, allowed_exts=(".mp4", ".m3u8", ".mpd")))
    unpacked = unpack_packed_javascript(raw_html)
    if unpacked:
        candidates.extend(_extract_candidate_media_urls(unpacked, allowed_exts=(".mp4", ".m3u8", ".mpd")))
        for match in re.finditer(r'wurl\s*[:=]\s*["\']([^"\']+)["\']', unpacked, re.IGNORECASE):
            candidate = _normalize_download_url(match.group(1).strip())
            if candidate:
                candidates.append(candidate)
    return _dedupe_download_urls(candidates)


def _extract_player_js_object(page_text, *var_names):
    page_text = str(page_text or "")
    for var_name in var_names:
        if not var_name:
            continue
        pattern = rf"var\s+{re.escape(var_name)}\s*="
        match = re.search(pattern, page_text, re.DOTALL)
        if not match:
            continue
        start = match.end()
        while start < len(page_text) and page_text[start].isspace():
            start += 1
        if start >= len(page_text) or page_text[start] != "{":
            continue
        depth = 0
        in_string = False
        quote_char = ""
        escape = False
        for idx in range(start, len(page_text)):
            ch = page_text[idx]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote_char:
                    in_string = False
            else:
                if ch in ("'", '"'):
                    in_string = True
                    quote_char = ch
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                            return _parse_js_object(page_text[start : idx + 1])
    return None


def _safe_extract_player_js_object(page_text, *var_names):
    try:
        return _extract_player_js_object(page_text, *var_names)
    except Exception:
        return None


def _decode_maccms_player_url(raw_url, encrypt_mode=0):
    decoded_url = str(raw_url or "").strip()
    if not decoded_url:
        return ""
    try:
        encrypt_value = int(encrypt_mode or 0)
    except (TypeError, ValueError):
        encrypt_value = 0
    try:
        if encrypt_value == 1:
            decoded_url = urllib.parse.unquote(decoded_url)
        elif encrypt_value == 2:
            decoded_url = urllib.parse.unquote(base64.b64decode(decoded_url).decode("utf-8", "ignore"))
    except Exception:
        pass
    return _normalize_download_url(decoded_url)


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


def _extract_movieffm_playback_candidates(page_html, fallback_title="MovieFFM"):
    page_text = str(page_html or "")
    player_data = _extract_player_js_object(page_text, "player_aaaa")
    candidates = []
    external_source_urls = []
    if player_data:
        videourls = player_data.get("videourls")
        if isinstance(videourls, list):
            for entry in videourls:
                if not isinstance(entry, dict):
                    continue
                raw_candidate = _normalize_download_url(entry.get("url"))
                if not raw_candidate:
                    continue
                entry_type = str(entry.get("type") or "").strip().lower()
                if entry_type == "iframe":
                    external_source_urls.append(raw_candidate)
                else:
                    candidates.append(raw_candidate)
        primary_url = _normalize_download_url(player_data.get("url"))
        if primary_url and primary_url not in candidates and primary_url not in external_source_urls:
            if _looks_like_manifest_url(primary_url) or _looks_like_http_media_url(primary_url):
                candidates.append(primary_url)
            else:
                external_source_urls.append(primary_url)
        for key in ("urls", "backup", "backup_urls", "m3u8_urls"):
            value = player_data.get(key)
            if isinstance(value, list):
                candidates.extend(value)
    if not candidates:
        candidates = _extract_movieffm_m3u8_candidates(page_text)
    if not candidates:
        external_source_urls = _extract_movieffm_external_source_urls(page_text)
    page_title = _extract_html_title(page_text, fallback_title)
    return (
        page_title,
        _dedupe_download_urls(candidates),
        _dedupe_download_urls(external_source_urls),
        player_data,
    )


def _extract_movieffm_detail_page_urls(page_html, current_url=None):
    current_url = _normalize_download_url(current_url)
    detail_urls = []
    for match in re.finditer(r'href=["\'](https?://www\.movieffm\.net/drama/\d+/)["\']', str(page_html or ""), re.IGNORECASE):
        candidate = _normalize_download_url(html.unescape(match.group(1)).strip())
        if candidate and candidate != current_url:
            detail_urls.append(candidate)
    return _dedupe_download_urls(detail_urls)


def _collect_movieffm_tvshow_detail_pages(page_html, page_url, fallback_title="MovieFFM"):
    text = str(page_html or "")
    show_title = str(fallback_title or "MovieFFM").strip() or "MovieFFM"
    title_match = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    if title_match:
        show_title = html.unescape(title_match.group(1)).split("-")[0].strip() or show_title
        show_title = "".join(c for c in show_title if c not in '\\/:*?"<>|')
    detail_urls = _extract_movieffm_detail_page_urls(text, current_url=page_url)
    detail_pages = []
    seen = set()
    for index, detail_url in enumerate(detail_urls, start=1):
        if detail_url in seen:
            continue
        seen.add(detail_url)
        detail_pages.append((detail_url, f"{show_title} Season {index}"))
    return show_title, detail_pages


def _should_use_ffmpeg_for_movieffm_manifest(stream_url):
    host = urllib.parse.urlsplit(_normalize_download_url(stream_url)).netloc.lower()
    return any(marker in host for marker in (
        "okzy",
        "xluuss",
        "youku.cdn7-okzy.com",
    ))


def _movieffm_manifest_candidate_is_dead(stream_url, referer="https://www.movieffm.net/"):
    normalized_url = _normalize_download_url(stream_url)
    host = urllib.parse.urlsplit(normalized_url).netloc.lower()
    if not normalized_url or "okzy" not in host:
        return False
    try:
        c_req = get_curl_cffi_requests()
        session = c_req.Session(impersonate="chrome120")
        try:
            resp = session.get(
                normalized_url,
                timeout=10,
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Referer": referer or "https://www.movieffm.net/",
                    "Origin": "https://www.movieffm.net",
                    "Accept": "*/*",
                },
            )
            body = str(resp.text or "")
            if resp.status_code >= 400:
                return True
            if "#EXTM3U" in body or normalized_url.lower().endswith(".m3u8"):
                return False
        finally:
            try:
                session.close()
            except Exception:
                pass
    except Exception:
        return True
    return False


def _normalize_movieffm_episode_name(name_text):
    cleaned = re.sub(r"<[^>]+>", "", html.unescape(str(name_text or "")).strip())
    if "\\u" in cleaned or "\\x" in cleaned:
        try:
            cleaned = bytes(cleaned, "utf-8").decode("unicode_escape")
        except Exception:
            pass
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _movieffm_numbered_episode_key(name_text):
    cleaned = re.sub(r"\s+", " ", str(name_text or "").strip())
    match = re.fullmatch(r"0*(\d{1,3})", cleaned)
    if not match:
        return ""
    num = int(match.group(1))
    return f"{num:02d}" if num < 100 else str(num)


_EPISODE_CHINESE_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "兩": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}


def _parse_chinese_episode_number(text):
    cleaned = re.sub(r"\s+", "", str(text or ""))
    if not cleaned:
        return None
    if cleaned == "十":
        return 10
    if cleaned.startswith("十"):
        tail = cleaned[1:]
        if tail and all(char in _EPISODE_CHINESE_DIGITS for char in tail):
            return 10 + _EPISODE_CHINESE_DIGITS.get(tail, 0)
    if cleaned.endswith("十"):
        head = cleaned[:-1]
        if head and all(char in _EPISODE_CHINESE_DIGITS for char in head):
            return _EPISODE_CHINESE_DIGITS.get(head, 0) * 10
    if "十" in cleaned:
        head, _, tail = cleaned.partition("十")
        if (
            head
            and tail
            and all(char in _EPISODE_CHINESE_DIGITS for char in head)
            and all(char in _EPISODE_CHINESE_DIGITS for char in tail)
        ):
            return _EPISODE_CHINESE_DIGITS.get(head, 0) * 10 + _EPISODE_CHINESE_DIGITS.get(tail, 0)
    if all(char in _EPISODE_CHINESE_DIGITS for char in cleaned):
        value = 0
        for char in cleaned:
            value = value * 10 + _EPISODE_CHINESE_DIGITS[char]
        return value
    return None


def _extract_episode_order_number(value):
    text = html.unescape(str(value or ""))
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return None
    for pattern in (
        r"第\s*0*(\d{1,4})\s*[集話章回期]",
        r"\bEP?\s*0*(\d{1,4})\b",
        r"\bE0*(\d{1,4})\b",
        r"\b0*(\d{1,4})\b",
    ):
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                continue
    zh_match = re.search(r"第\s*([零〇一二兩三四五六七八九十]+)\s*[集話章回期]", normalized)
    if zh_match:
        parsed = _parse_chinese_episode_number(zh_match.group(1))
        if parsed is not None:
            return parsed
    return None


def _natural_download_target_key(target):
    if isinstance(target, (tuple, list)):
        url = str(target[0]) if target else ""
        label = " ".join(str(part) for part in target[1:] if part not in (None, ""))
    elif isinstance(target, dict):
        url = str(target.get("url", ""))
        label = " ".join(
            str(part)
            for part in (
                target.get("full_name"),
                target.get("title"),
                target.get("name"),
            )
            if part not in (None, "")
        )
    else:
        url = str(target or "")
        label = url
    number = _extract_episode_order_number(label)
    if number is None:
        number = _extract_episode_order_number(url)
    return (number is None, number if number is not None else 0, label.lower(), url.lower())


def _sort_download_targets_naturally(targets):
    return sorted(list(targets or []), key=_natural_download_target_key)


def _collect_movieffm_drama_episodes(page_html, page_url, fallback_title="MovieFFM"):
    text = str(page_html or "")
    page_url = _normalize_download_url(page_url)
    title_match = re.search(r"<title>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    drama_title = str(fallback_title or "MovieFFM").strip() or "MovieFFM"
    if title_match:
        drama_title = html.unescape(title_match.group(1)).split("-")[0].strip() or drama_title
        drama_title = "".join(c for c in drama_title if c not in '\\/:*?"<>|')
    episodes = []
    seen_urls = set()
    seen_names = set()
    episode_fallbacks = {}

    def add_episode(ep_url, ep_name, allow_direct_m3u8=False):
        if not ep_url:
            return
        cleaned_url = html.unescape(str(ep_url)).replace("\\/", "/").strip()
        cleaned_url = urllib.parse.urljoin(page_url, cleaned_url)
        if not cleaned_url.startswith("http"):
            return
        if cleaned_url.lower().endswith(".m3u8") and not allow_direct_m3u8:
            return
        cleaned_name = _normalize_movieffm_episode_name(ep_name)
        if not cleaned_name:
            tail = urllib.parse.urlparse(cleaned_url).path.rstrip("/").split("/")[-1]
            cleaned_name = tail or "Episode"
        episode_key = _movieffm_numbered_episode_key(cleaned_name)
        if episode_key:
            cleaned_name = episode_key
        else:
            return
        bucket = episode_fallbacks.setdefault(episode_key, [])
        if cleaned_url not in bucket:
            bucket.append(cleaned_url)
        if cleaned_url in seen_urls or episode_key in seen_names:
            return
        full_name = f"{drama_title} {cleaned_name}".strip()
        seen_urls.add(cleaned_url)
        seen_names.add(episode_key)
        episodes.append((cleaned_url, full_name))

    for name, ep_url in re.findall(r'"name"\s*:\s*"([^"]+)"\s*,\s*"url"\s*:\s*"([^"]+)"', text):
        add_episode(ep_url, name, allow_direct_m3u8=ep_url.lower().endswith(".m3u8"))
    for ep_url in re.findall(r'https://www\.movieffm\.net/[^"\']+', text):
        if "/play/" in ep_url or "/vodplay/" in ep_url or "/episode/" in ep_url:
            normalized_episode_url = _normalize_download_url(ep_url)
            if normalized_episode_url:
                parsed_episode_url = urllib.parse.urlparse(normalized_episode_url)
                if ("anime1.me" in parsed_episode_url.netloc.lower() or "anime1.pw" in parsed_episode_url.netloc.lower()) and ("/category/" in parsed_episode_url.path.lower() or "cat" in urllib.parse.parse_qs(parsed_episode_url.query, keep_blank_values=True)):
                    query = urllib.parse.parse_qs(parsed_episode_url.query, keep_blank_values=True)
                    cat = query.get("cat", [""])[0]
                    episode_name = f"{parsed_episode_url.netloc} cat={cat}" if cat else parsed_episode_url.netloc
                else:
                    slug = parsed_episode_url.path.rstrip("/").split("/")[-1]
                    episode_name = slug or parsed_episode_url.netloc
            else:
                episode_name = ""
            add_episode(ep_url, episode_name)
    return drama_title, episodes, episode_fallbacks


@lru_cache(maxsize=256)
def _url_host_resolves(url):
    try:
        host = urllib.parse.urlsplit(str(url or "")).netloc
    except Exception:
        return False
    host = str(host or "").strip().lower()
    if not host:
        return False
    if host.startswith("[") and "]" in host:
        host = host[1:host.index("]")]
    elif ":" in host:
        host = host.rsplit(":", 1)[0]
    try:
        socket.getaddrinfo(host, None)
        return True
    except OSError:
        return False


def _movieffm_stream_priority(url):
    host = urllib.parse.urlsplit(str(url or "")).netloc.lower()
    if "xluuss" in host:
        return 0
    if "lzcdn" in host:
        return 1
    if "letvoss" in host:
        return 2
    if "subokk" in host:
        return 3
    if "ukubf" in host:
        return 4
    if "bdzybf" in host:
        return 50
    return 100


def _select_reachable_stream_candidates(primary_url, fallback_urls=(), source_site=""):
    ordered = []
    seen = set()
    for raw_url in [primary_url, *(fallback_urls or [])]:
        normalized_url = _normalize_download_url(raw_url)
        if not normalized_url or normalized_url in seen:
            continue
        seen.add(normalized_url)
        ordered.append(normalized_url)
    if not ordered:
        return "", [], False
    if source_site == "movieffm":
        ordered = sorted(ordered, key=_movieffm_stream_priority)
    reachable = [candidate for candidate in ordered if _url_host_resolves(candidate)]
    preferred = reachable[0] if reachable else ordered[0]
    remaining = [candidate for candidate in ordered if candidate != preferred]
    if reachable:
        reachable_tail = [candidate for candidate in reachable if candidate != preferred]
        unreachable_tail = [candidate for candidate in remaining if candidate not in reachable_tail]
        remaining = reachable_tail + unreachable_tail
    return preferred, remaining, bool(reachable)


def _extract_nnyy_page_id(page_url):
    normalized_url = _normalize_download_url(page_url)
    if not normalized_url:
        return ""
    match = re.search(r"/(\d+)\.html(?:$|[?#])", urllib.parse.urlsplit(normalized_url).path, re.IGNORECASE)
    return match.group(1) if match else ""


def _extract_nnyy_episode_entries(page_html):
    entries = []
    default_slug = ""
    text = str(page_html or "")
    default_match = re.search(r"on_ep\(['\"]([^'\"]+)['\"]\)", text, re.IGNORECASE)
    if default_match:
        default_slug = str(default_match.group(1) or "").strip()
    for match in re.finditer(r"<([a-z0-9]+)\b[^>]*\bep_slug=['\"]([^'\"]+)['\"][^>]*>(.*?)</\1>", text, re.IGNORECASE | re.DOTALL):
        ep_slug = str(match.group(2) or "").strip()
        ep_name = re.sub(r"<[^>]+>", "", html.unescape(match.group(3) or ""))
        ep_name = re.sub(r"\s+", " ", ep_name).strip() or ep_slug
        if ep_slug:
            entries.append((ep_slug, ep_name))
    for match in re.finditer(r"\bep_slug=['\"]([^'\"]+)['\"][^>]*\btitle=['\"]([^'\"]+)['\"]", text, re.IGNORECASE | re.DOTALL):
        ep_slug = str(match.group(1) or "").strip()
        ep_name = re.sub(r"<[^>]+>", "", html.unescape(match.group(2) or ""))
        ep_name = re.sub(r"\s+", " ", ep_name).strip() or ep_slug
        if ep_slug:
            entries.append((ep_slug, ep_name))
    deduped = []
    seen = set()
    for ep_slug, ep_name in entries:
        if ep_slug in seen:
            continue
        seen.add(ep_slug)
        deduped.append((ep_slug, ep_name))
    if not default_slug and deduped:
        default_slug = deduped[0][0]
    return deduped, default_slug


def _normalize_nnyy_episode_name(ep_name, ep_slug="", pad_width=2):
    raw_name = re.sub(r"\s+", " ", str(ep_name or "")).strip()
    slug_text = str(ep_slug or "").strip()
    width = max(int(pad_width or 0), 2)
    match = re.search(r"^(.*?)(第)\s*(\d+)\s*(集.*)$", raw_name)
    if match:
        prefix, marker, number_text, suffix = match.groups()
        try:
            normalized_number = f"{int(number_text):0{width}d}"
        except ValueError:
            normalized_number = number_text
        return f"{prefix}{marker}{normalized_number}{suffix}".strip()
    slug_match = re.search(r"ep\s*0*(\d+)$", slug_text, re.IGNORECASE)
    if slug_match:
        try:
            normalized_number = f"{int(slug_match.group(1)):0{width}d}"
        except ValueError:
            normalized_number = slug_match.group(1)
        return f"第{normalized_number}集"
    return raw_name or slug_text or "第01集"


def _extract_nnyy_play_candidates(payload_text):
    candidates = []
    payload = {}
    try:
        payload = json.loads(str(payload_text or ""))
    except Exception:
        payload = {}
    plays = payload.get("video_plays") or []
    if isinstance(plays, list):
        for row in plays:
            if not isinstance(row, dict):
                continue
            play_url = _normalize_download_url(row.get("play_data", ""))
            if play_url and _looks_like_manifest_url(play_url):
                candidates.append(play_url)
    return _dedupe_download_urls(candidates)


def _extract_777tv_episode_entries(page_html, base_url="https://play.777tv.ai/"):
    entries = []
    text = str(page_html or "")
    for match in re.finditer(
        r'href=["\']((?:(?:https?:)?//play\.777tv\.ai)?/vod/play/id/\d+/sid/\d+/nid/\d+\.html)["\'][^>]*>(.*?)</a>',
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        play_url = html.unescape(match.group(1) or "").strip()
        if play_url.startswith("//"):
            play_url = "https:" + play_url
        elif play_url.startswith("/"):
            play_url = urllib.parse.urljoin(base_url, play_url)
        play_url = _normalize_download_url(play_url)
        ep_name = re.sub(r"<[^>]+>", "", html.unescape(match.group(2) or ""))
        ep_name = re.sub(r"\s+", " ", ep_name).strip()
        if play_url and (not ep_name or ep_name in {"立即播放", "在线播放", "線上播放", "在线观看"}):
            nid_match = re.search(r"/nid/(\d+)\.html$", urllib.parse.urlsplit(play_url).path, re.IGNORECASE)
            if nid_match:
                try:
                    ep_name = f"第{int(nid_match.group(1)):02d}集"
                except ValueError:
                    ep_name = ""
        if play_url:
            entries.append((play_url, ep_name or default_short_name_for_url(play_url)))
    deduped = []
    seen = set()
    for play_url, ep_name in entries:
        if play_url in seen:
            continue
        seen.add(play_url)
        deduped.append((play_url, ep_name))
    return deduped


def _extract_777tv_playback_candidates(page_html, fallback_title="777TV"):
    text = str(page_html or "")
    player_data = _extract_player_js_object(text, "player_data", "player_aaaa", "player")
    candidates = []
    if player_data:
        for key in ("url", "src", "play_url", "playUrl", "urls", "backup", "backup_urls", "m3u8_urls", "playlist", "sources"):
            value = player_data.get(key)
            if isinstance(value, (list, tuple, set)):
                values = value
            else:
                values = [value]
            for item in values:
                candidate = _normalize_download_url(item)
                if candidate and _looks_like_manifest_url(candidate):
                    candidates.append(candidate)
    if not candidates:
        for match in re.finditer(r'(?:(?:https?:)?//[^"\'\s<]+?\.m3u8(?:\?[^"\'\s<]*)?)', text, re.IGNORECASE):
            candidate = _normalize_download_url(html.unescape(match.group(0)).strip())
            if candidate and _looks_like_manifest_url(candidate):
                candidates.append(candidate)
    page_title = _extract_html_title(text, fallback_title)
    return page_title, _dedupe_download_urls(candidates), player_data


def _extract_3kor_detail_entries(page_html, base_url="https://3kor.com/"):
    entries = []
    text = str(page_html or "")
    for match in re.finditer(r"<li\b[^>]*>(.*?)</li>", text, re.IGNORECASE | re.DOTALL):
        block = match.group(1) or ""
        link_match = re.search(r'href=["\']((?:https?:)?//3kor\.com/detail/\d+\.html|/detail/\d+\.html)["\']', block, re.IGNORECASE)
        if not link_match:
            continue
        detail_url = html.unescape(link_match.group(1) or "").strip()
        if detail_url.startswith("//"):
            detail_url = "https:" + detail_url
        elif detail_url.startswith("/"):
            detail_url = urllib.parse.urljoin(base_url, detail_url)
        detail_url = _normalize_download_url(detail_url)
        title = ""
        title_match = re.search(r'title=["\']([^"\']+)["\']', block, re.IGNORECASE)
        if title_match:
            title = re.sub(r"\s+", " ", html.unescape(title_match.group(1) or "")).strip()
        if not title:
            name_match = re.search(r'<p>\s*<a[^>]+href=["\'][^"\']+/detail/\d+\.html["\'][^>]*>(.*?)</a>\s*</p>', block, re.IGNORECASE | re.DOTALL)
            if name_match:
                title = re.sub(r"<[^>]+>", "", html.unescape(name_match.group(1) or ""))
                title = re.sub(r"\s+", " ", title).strip()
        if detail_url:
            entries.append((detail_url, title or default_short_name_for_url(detail_url)))
    deduped = []
    seen = set()
    for detail_url, title in entries:
        if detail_url in seen:
            continue
        seen.add(detail_url)
        deduped.append((detail_url, title))
    return deduped


def _extract_3kor_play_entries(page_html):
    entries = []
    text = str(page_html or "")
    for match in re.finditer(
        r"bb_a\(['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]*)['\"]\s*,\s*event\)",
        text,
        re.IGNORECASE,
    ):
        play_id = str(match.group(1) or "").strip()
        play_name = re.sub(r"\s+", " ", html.unescape(match.group(2) or "")).strip()
        if play_id:
            entries.append((play_id, play_name or play_id))
    deduped = []
    seen = set()
    for play_id, play_name in entries:
        if play_id in seen:
            continue
        seen.add(play_id)
        deduped.append((play_id, play_name))
    return deduped


def _decrypt_3kor_stream_url(encrypted_text, key="my-to-newhan-2025"):
    if CryptoAES is None:
        raise Exception("3KOR decrypt dependency missing")
    encrypted_text = str(encrypted_text or "").strip()
    if not encrypted_text:
        raise Exception("3KOR encrypted stream missing")
    encrypted_bytes = base64.b64decode(encrypted_text)
    if len(encrypted_bytes) <= 16:
        raise Exception("3KOR encrypted stream invalid")
    raw_key = str(key or "")[:32].ljust(32, "\0").encode("utf-8")
    iv = encrypted_bytes[:16]
    ciphertext = encrypted_bytes[16:]
    cipher = CryptoAES.new(raw_key, CryptoAES.MODE_CBC, iv)
    decrypted = cipher.decrypt(ciphertext)
    if decrypted:
        padding = decrypted[-1]
        if 1 <= int(padding) <= 16:
            decrypted = decrypted[:-int(padding)]
    stream_url = decrypted.decode("utf-8", "ignore").strip()
    return _normalize_download_url(stream_url)


def _clean_99itv_title(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\s*-\s*99i影城.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*-99i影城.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*線上看\s*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/") or str(raw_title or "").strip()


def _clean_18av_title(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\s*[-|,]\s*18AV.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*18AV在線H成人影片.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*,\s*(?:H動畫|H動漫|裏番).*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(raw_title or "").strip()


def _is_18av_preview_media_url(url):
    lowered = str(url or "").strip().lower()
    if not lowered:
        return False
    return any(
        marker in lowered
        for marker in (
            "imgstream1.com",
            "gifb.",
            "fchost1.",
            "fbhost1.",
        )
    )


def _clean_hohoj_title(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"^\[\]\s*", "", cleaned)
    cleaned = re.sub(r"\s+HoHoJ(?:\.TV)?\s+.*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(raw_title or "").strip()


def _clean_javfilms_title(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\s*\|\s*JAV\s*Films.*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(raw_title or "").strip()


def _clean_avjoy_title(raw_title, fallback_title="AVJOY"):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return str(fallback_title or "AVJOY").strip() or "AVJOY"
    cleaned = re.sub(r"\s*[-|]\s*AVJoy.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\[[^\]]+\]\s*", "", cleaned).strip()
    cleaned = re.sub(r"(?i)\b(chinese\s*subtitle|subtitle|subbed)\b", "", cleaned)
    cleaned = cleaned.replace("..", " ").strip(" -|/,._")
    cleaned = re.sub(
        r"\b([A-Za-z]{2,10})[-_. ]?(\d{2,6})\b",
        lambda m: f"{m.group(1).upper()}-{m.group(2)}",
        cleaned,
        count=1,
    )
    return cleaned.strip(" -|/,") or str(fallback_title or "AVJOY").strip() or "AVJOY"


def _clean_avbebe_title(raw_title, fallback_title="Avbebe"):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return str(fallback_title or "Avbebe").strip() or "Avbebe"
    cleaned = re.sub(r"\s*(?:&#8211;|–|-|\|)\s*Avbebe.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+高清H動畫.*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(fallback_title or "Avbebe").strip() or "Avbebe"


def _avbebe_iframe_priority(url):
    host = urllib.parse.urlsplit(_normalize_download_url(url)).netloc.lower()
    if "turbovidhls.com" in host or "turboviplay.com" in host:
        return 0
    if "hgcloud.to" in host:
        return 30
    return 10


def _avbebe_can_retry_iframe_directly(url):
    host = urllib.parse.urlsplit(_normalize_download_url(url)).netloc.lower()
    if "hgcloud.to" in host:
        return False
    return bool(host)


def _avbebe_stream_priority(url):
    host = urllib.parse.urlsplit(_normalize_download_url(url)).netloc.lower()
    if "52cute.com" in host:
        return 0
    if "turboviplay.com" in host or "turbosplayer.com" in host:
        return 10
    if "hgcloud.to" in host:
        return 50
    return 10


def _is_non_video_probe_payload(data, content_type=""):
    lowered_type = str(content_type or "").lower()
    if any(marker in lowered_type for marker in ("text/html", "image/", "application/json")):
        return True
    prefix = (data or b"")[:128].lstrip()
    return prefix.startswith((b"<!DOCTYPE", b"<html", b"{", b"\x89PNG", b"GIF8", b"\xff\xd8\xff"))


def _avbebe_manifest_looks_downloadable(manifest_url, referer=None, origin=None, timeout=12):
    normalized = _normalize_download_url(manifest_url)
    if not normalized:
        return False
    manifest_host = urllib.parse.urlsplit(normalized).netloc.lower()
    strict_probe = any(marker in manifest_host for marker in ("turboviplay.com", "turbosplayer.com"))
    headers = _make_hls_http_headers(referer=referer, origin=origin)
    try:
        req = urllib.request.Request(normalized, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            manifest_text = resp.read(512 * 1024).decode("utf-8", "ignore")
    except Exception:
        return not strict_probe
    if "#EXTM3U" not in manifest_text:
        return False
    base_url = normalized
    lines = [line.strip() for line in manifest_text.splitlines() if line.strip()]
    variant_url = ""
    for index, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            for next_line in lines[index + 1:]:
                if not next_line.startswith("#"):
                    variant_url = urllib.parse.urljoin(base_url, next_line)
                    break
        if variant_url:
            break
    if variant_url:
        try:
            req = urllib.request.Request(variant_url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                manifest_text = resp.read(512 * 1024).decode("utf-8", "ignore")
            base_url = variant_url
            lines = [line.strip() for line in manifest_text.splitlines() if line.strip()]
        except Exception:
            return not strict_probe
    segment_url = ""
    for line in lines:
        if line.startswith("#"):
            continue
        if ".m3u8" in line.lower():
            continue
        segment_url = urllib.parse.urljoin(base_url, line)
        break
    if not segment_url:
        return True
    probe_headers = dict(headers)
    probe_headers.setdefault("Range", "bytes=0-511")
    try:
        req = urllib.request.Request(segment_url, headers=probe_headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_type = str(resp.headers.get("Content-Type") or "").lower()
            data = resp.read(512)
    except Exception:
        segment_host = urllib.parse.urlsplit(_normalize_download_url(segment_url)).netloc.lower()
        if strict_probe or "googleusercontent.com" in segment_host:
            return False
        return True
    return not _is_non_video_probe_payload(data, content_type)


def _avjoy_title_from_url_slug(page_url):
    parsed = urllib.parse.urlsplit(str(page_url or "").strip())
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) < 3 or segments[0].lower() != "video":
        return ""
    slug = urllib.parse.unquote(segments[-1]).strip()
    if not slug:
        return ""
    slug = slug.replace("_", " ").strip(" -|/,._")
    code_match = re.match(r"([A-Za-z]{2,10})-(\d{2,6})(?:-|$)", slug)
    if code_match:
        code = f"{code_match.group(1).upper()}-{code_match.group(2)}"
        tail = slug[code_match.end():].strip(" -|/,._")
        tail = re.sub(r"[-]+", " ", tail)
        return _clean_avjoy_title(f"{code} {tail}".strip(), fallback_title=code)
    slug = re.sub(r"[-]+", " ", slug)
    return _clean_avjoy_title(slug, fallback_title="AVJOY")


def _clean_gimy_title(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = cleaned.replace("線上看", " ").replace("正片", " ").strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s*(?:[-|｜]\s*Gimy.*)$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(raw_title or "").strip()


def _extract_gimy_parse_candidates(parse_text, base_url=""):
    raw_text = str(parse_text or "").strip()
    if not raw_text:
        return []
    candidates = []
    parse_data = None
    try:
        parse_data = json.loads(raw_text)
    except Exception:
        parse_data = None
    if isinstance(parse_data, dict):
        for key in ("url", "video", "playurl"):
            parsed_candidate = _normalize_download_url(parse_data.get(key))
            if parsed_candidate:
                candidates.append(parsed_candidate)
    if not candidates:
        player_like = _safe_extract_player_js_object(raw_text, "player_data", "player_aaaa", "player")
        if isinstance(player_like, dict):
            for key in ("url", "video", "playurl"):
                parsed_candidate = _normalize_download_url(player_like.get(key))
                if parsed_candidate:
                    candidates.append(parsed_candidate)
    if not candidates:
        for stream_url in _extract_m3u8_candidates_from_text(raw_text, base_url=base_url):
            if stream_url not in candidates:
                candidates.append(stream_url)
    if not candidates:
        for media_url in _extract_candidate_media_urls(raw_text, allowed_exts=(".m3u8", ".mp4", ".mpd")):
            normalized_media = _normalize_download_url(media_url)
            if normalized_media and normalized_media not in candidates:
                candidates.append(normalized_media)
    return _dedupe_download_urls(candidates)


def _gimy_stream_priority(url):
    host = urllib.parse.urlsplit(_normalize_download_url(url) or str(url or "")).netloc.lower()
    priority_markers = (
        "xluuss",
        "phimgood",
        "ppqrrs",
        "ryplay",
        "ryiplay",
        "yzzy",
        "hhiklm",
        "jisuziyuanbf",
        "dytt-cinema",
        "modujx",
        "jisutian",
        "jisuzyv",
    )
    for index, marker in enumerate(priority_markers):
        if marker in host:
            return (index, host, str(url or ""))
    return (len(priority_markers), host, str(url or ""))


def _gimy_manifest_default_route(url):
    host = urllib.parse.urlsplit(_normalize_download_url(url) or str(url or "")).netloc.lower()
    generic_markers = (
        "xluuss",
        "phimgood",
        "ppqrrs",
        "ryplay",
        "ryiplay",
        "yzzy",
        "hhiklm",
        "jisuziyuanbf",
        "modujx",
    )
    if any(marker in host for marker in generic_markers):
        return "generic"
    return "ffmpeg"


def _goodav_title_for_display(raw_title):
    cleaned = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(r"\s*-\s*正妹AV.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*VR線上成人影片.*?-\s*", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -|/,") or str(raw_title or "").strip()


def _decode_goodav_embed_media_url(embed_url):
    parsed = urllib.parse.urlsplit(str(embed_url or "").strip())
    query = urllib.parse.parse_qs(parsed.query)
    encoded_url = (query.get("u") or [""])[0]
    if not encoded_url:
        return ""
    try:
        padded = encoded_url + "=" * (-len(encoded_url) % 4)
        decoded = base64.b64decode(padded).decode("utf-8", "ignore")
    except Exception:
        return ""
    return _normalize_download_url(decoded)


def _decode_18av_payload(encoded_value, base_value, xor_value):
    encoded_text = str(encoded_value or "").strip()
    if not encoded_text:
        return ""
    try:
        base_num = int(base_value)
        xor_num = int(xor_value)
    except Exception:
        return ""
    if base_num < 2:
        return ""
    split_char = chr(base_num + 97)
    parts = [part for part in encoded_text.split(split_char) if part]
    decoded_chars = []
    try:
        for part in parts:
            decoded_chars.append(chr(int(part, base_num) ^ xor_num))
    except Exception:
        return ""
    return "".join(decoded_chars)


def _decrypt_18av_player_id(encoded_value, base_value, xor_value, aes_key, aes_iv):
    stage1_payload = _decode_18av_payload(encoded_value, base_value, xor_value)
    if not stage1_payload:
        return "", ""
    key_text = str(aes_key or "")
    iv_text = str(aes_iv or "")
    if not CryptoAES or not key_text or not iv_text:
        return stage1_payload, ""
    try:
        cipher = CryptoAES.new(
            key_text.encode("utf-8"),
            CryptoAES.MODE_CBC,
            iv_text.encode("utf-8"),
        )
        decrypted = cipher.decrypt(base64.b64decode(stage1_payload))
        pad = decrypted[-1] if decrypted else 0
        if 1 <= pad <= 16 and decrypted.endswith(bytes([pad]) * pad):
            decrypted = decrypted[:-pad]
        player_id = decrypted.decode("utf-8", errors="ignore").strip()
        return stage1_payload, player_id
    except Exception:
        return stage1_payload, ""


def _extract_18av_protected_player(page_text):
    html_text = str(page_text or "")
    iframe_prefix_match = re.search(
        r"//18av\.mm-cg\.com/js/player/play\.php\?numresolution=\d+&id=",
        html_text,
        re.IGNORECASE,
    )
    encoded_id_match = re.search(
        r"mvarr\['[^']+'\]\s*=\s*\[\[['\"](?:a_iframe_id_[^'\"]+)['\"],['\"]([^'\"]+)['\"]",
        html_text,
        re.IGNORECASE,
    )
    xor_match = re.search(r"\bhadeedg252\s*=\s*(\d+)", html_text)
    base_match = re.search(r"\bhcdeedg252\s*=\s*(\d+)", html_text)
    key_match = re.search(r"\bargdeqweqweqwe\s*=\s*['\"]([^'\"]+)['\"]", html_text)
    iv_match = re.search(r"\bhdddedg252\s*=\s*['\"]([^'\"]+)['\"]", html_text)
    encoded_id = encoded_id_match.group(1) if encoded_id_match else ""
    decoded_payload_stage1, decoded_payload = _decrypt_18av_player_id(
        encoded_id,
        base_match.group(1) if base_match else "",
        xor_match.group(1) if xor_match else "",
        key_match.group(1) if key_match else "",
        iv_match.group(1) if iv_match else "",
    )
    return {
        "iframe_prefix": iframe_prefix_match.group(0) if iframe_prefix_match else "",
        "encoded_id": encoded_id,
        "decoded_payload_stage1": decoded_payload_stage1,
        "decoded_payload": decoded_payload,
        "xor_value": xor_match.group(1) if xor_match else "",
        "base_value": base_match.group(1) if base_match else "",
        "aes_key": key_match.group(1) if key_match else "",
        "aes_iv": iv_match.group(1) if iv_match else "",
    }


def _resolve_18av_protected_player_media(page_url, page_html):
    protected_player = _extract_18av_protected_player(page_html)
    iframe_prefix = protected_player.get("iframe_prefix") or ""
    if not iframe_prefix:
        return "", "", "", protected_player
    parsed = urllib.parse.urlparse(_normalize_download_url(page_url) or page_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    player_base = urllib.parse.urljoin(site_root + "/", iframe_prefix)
    player_ids = []
    for candidate in (
        protected_player.get("decoded_payload"),
        protected_player.get("encoded_id"),
    ):
        candidate = str(candidate or "").strip()
        if candidate and candidate not in player_ids:
            player_ids.append(candidate)
    if not player_ids:
        player_ids.append("")
    c_req = get_curl_cffi_requests()
    common_headers = _make_site_root_headers(site_root, referer=_normalize_download_url(page_url) or (site_root + "/"))
    last_probe_url = player_base
    for player_id in player_ids:
        probe_url = player_base + urllib.parse.quote(player_id, safe="")
        last_probe_url = probe_url
        try:
            probe_resp = c_req.get(
                probe_url,
                impersonate="chrome120",
                timeout=20,
                headers=common_headers,
                cookies={"javascript_cookie_AgreeTOS_save1": "javascript_cookie_AgreeTOS_save2"},
            )
        except Exception:
            continue
        probe_html = str(getattr(probe_resp, "text", "") or "")
        probe_candidates = _extract_candidate_media_urls(probe_html, allowed_exts=(".m3u8", ".mp4", ".mpd"))
        manifest_candidates = [
            candidate for candidate in probe_candidates
            if _looks_like_manifest_url(candidate) and not candidate.lower().endswith(".m3u8.jpg")
        ]
        direct_media_candidates = [
            candidate for candidate in probe_candidates
            if _looks_like_http_media_url(candidate) and "_preview.mp4" not in candidate.lower() and "/preview.mp4" not in candidate.lower()
        ]
        if manifest_candidates:
            return manifest_candidates[0], "", probe_url, protected_player
        if direct_media_candidates:
            return "", direct_media_candidates[0], probe_url, protected_player
    return "", "", last_probe_url, protected_player


def _resolve_pikpak_nuxt_value(payload, value, seen=None, depth=0):
    if depth > 12:
        return value
    if isinstance(value, int) and 0 <= value < len(payload):
        if seen is None:
            seen = set()
        if value in seen:
            return payload[value]
        seen = set(seen)
        seen.add(value)
        return _resolve_pikpak_nuxt_value(payload, payload[value], seen=seen, depth=depth + 1)
    if isinstance(value, list):
        return [_resolve_pikpak_nuxt_value(payload, item, seen=seen, depth=depth + 1) for item in value]
    if isinstance(value, dict):
        return {
            key: _resolve_pikpak_nuxt_value(payload, item, seen=seen, depth=depth + 1)
            for key, item in value.items()
        }
    return value


def _extract_pikpak_share_entries(page_text):
    script_match = re.search(
        r'<script[^>]+id=["\']__NUXT_DATA__["\'][^>]*>(.*?)</script>',
        str(page_text or ""),
        re.IGNORECASE | re.DOTALL,
    )
    if not script_match:
        return []
    try:
        payload = json.loads(html.unescape(script_match.group(1)))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    if not payload:
        return []
    entries = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        kind = _resolve_pikpak_nuxt_value(payload, item.get("kind"))
        if kind != "drive#file":
            continue
        resolved = {}
        for field in (
            "id",
            "parent_id",
            "name",
            "size",
            "file_extension",
            "mime_type",
            "web_content_link",
            "links",
            "medias",
            "params",
            "apps",
            "file_category",
            "phase",
            "audit",
            "thumbnail_link",
            "icon_link",
        ):
            if field in item:
                resolved[field] = _resolve_pikpak_nuxt_value(payload, item.get(field))
        entries.append(resolved)
    return entries


def _pick_pikpak_primary_video_entry(entries):
    videos = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        file_category = str(entry.get("file_category") or "").strip().upper()
        mime_type = str(entry.get("mime_type") or "").strip().lower()
        file_name = str(entry.get("name") or "").strip().lower()
        if (
            file_category == "VIDEO"
            or mime_type.startswith("video/")
            or file_name.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"))
        ):
            videos.append(entry)
    if not videos:
        return None

    def _video_rank(entry):
        try:
            size = int(str(entry.get("size") or "0"))
        except Exception:
            size = 0
        return size

    return max(videos, key=_video_rank)


def _extract_pikpak_media_candidates(entries):
    candidates = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        direct_url = _normalize_download_url(entry.get("web_content_link"))
        if direct_url and direct_url not in candidates:
            candidates.append(direct_url)
        for source in (entry.get("links"), entry.get("medias"), entry.get("params"), entry.get("apps")):
            nested = []
            _walk_media_urls(source, nested)
            for candidate in nested:
                normalized = _normalize_download_url(candidate)
                if normalized and normalized not in candidates:
                    candidates.append(normalized)
    return candidates


def _extract_html_title(page_text, fallback_name):
    title_m = re.search(r"<title>(.*?)</title>", str(page_text or ""), re.IGNORECASE | re.DOTALL)
    if not title_m:
        return fallback_name
    raw_title = re.sub(r"\s+", " ", str(html.unescape(title_m.group(1)) or "")).strip()
    for splitter in (" - ", " | ", " – ", " — "):
        if splitter in raw_title:
            raw_title = raw_title.split(splitter)[0].strip()
            break
    normalized_title = re.sub(r"\s+", " ", str(raw_title or "")).strip().lower()
    normalized_page = str(page_text or "").lower()
    if (
        any(marker in normalized_title for marker in ("just a moment", "attention required", "checking your browser", "please wait"))
        or any(marker in normalized_page for marker in ("cf-browser-verification", "cf-challenge", "__cf_chl_", "checking your browser before accessing"))
    ):
        return fallback_name
    return raw_title or fallback_name


def _build_gimy_iframe_urls(page_url, player_data):
    player_data = player_data or {}
    play_url = str(player_data.get("url") or "").strip()
    if not play_url:
        return []
    play_from = str(player_data.get("from") or "").strip()
    link_next = str(player_data.get("link_next") or "").strip()
    parsed = urllib.parse.urlsplit(str(page_url or ""))
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'gimy01.tv'}"
    normal_iframe = urllib.parse.urljoin(base, "/aiplayer/dp/")
    iframe_base = normal_iframe
    jctype = "normal"
    if play_from in {"JD4K", "JD2K", "JDQM", "JDHG"}:
        iframe_base = "https://play.gimy01.tv/dp/"
        jctype = play_from
    elif play_from in {"JSYBL", "JSYMG", "JSYQY", "JSYDJ", "JSYHS", "JSYRR", "JSYYK", "JSYTX"}:
        iframe_base = "https://play.gimy01.tv/i/"
        jctype = play_from
    elif play_from in {"djplayer"} or play_url.startswith("JinLiDj-"):
        iframe_base = urllib.parse.urljoin(base, "/aiplayer/jin.php")
        jctype = play_from or "djplayer"
    elif play_from in {"JK2"} or play_url.startswith("JK2-"):
        iframe_base = urllib.parse.urljoin(base, "/aiplayer/")
        jctype = play_from or "JK2"
    elif play_from in {"Disney", "qingshan"}:
        iframe_base = urllib.parse.urljoin(base, "/gimyplayer/")
        jctype = play_from
    params = {
        "url": play_url,
        "jctype": jctype,
    }
    if link_next:
        params["next"] = urllib.parse.urljoin(base, link_next)
    primary = _normalize_download_url(f"{iframe_base}?{urllib.parse.urlencode(params)}")
    urls = [primary] if primary else []
    if iframe_base != normal_iframe:
        fallback = _normalize_download_url(f"{normal_iframe}?{urllib.parse.urlencode({'url': play_url, 'jctype': 'normal'})}")
        if fallback and fallback not in urls:
            urls.append(fallback)
    return urls


def _extract_gimy_inline_iframe_urls(page_html, page_url):
    parsed = urllib.parse.urlsplit(str(page_url or ""))
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'gimy01.tv'}"
    urls = []
    for match in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', str(page_html or ""), re.IGNORECASE):
        iframe_src = html.unescape(str(match.group(1) or "")).replace("\\/", "/").strip()
        if not iframe_src:
            continue
        full_url = _normalize_download_url(urllib.parse.urljoin(base, iframe_src))
        if full_url and full_url not in urls:
            urls.append(full_url)
    return urls


def _looks_like_manifest_url(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return False
    lower = normalized.lower()
    return ".m3u8" in lower or ".mpd" in lower


def _resolve_forced_m3u8_site(normalized_url, task):
    if not normalized_url:
        return None
    source_site = _task_source_site_name(task)
    if source_site == "3kor":
        parsed = urllib.parse.urlsplit(normalized_url)
        if "3kor.com" in parsed.netloc.lower() and "edit-down.php" in parsed.path.lower():
            embedded_url = urllib.parse.parse_qs(parsed.query).get("url", [""])[0]
            if _looks_like_manifest_url(embedded_url):
                return "3kor"
    if not _looks_like_manifest_url(normalized_url):
        return None
    if source_site in FORCED_M3U8_SITE_RULES:
        return source_site
    if _dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=normalized_url):
        return "movieffm"
    hostname = urllib.parse.urlparse(normalized_url).netloc.lower().split(":", 1)[0]
    for site_name, config in FORCED_M3U8_SITE_RULES.items():
        if any(hostname.endswith(host_suffix) for host_suffix in config["hosts"]):
            return site_name
    return None


def _infer_media_extension_from_url(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return ""
    parsed = urllib.parse.urlsplit(normalized)
    candidates = [parsed.path or ""]
    query_map = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    for key in ("response-content-disposition", "filename", "file", "name"):
        for value in query_map.get(key, []):
            candidates.append(str(value))
    for candidate in candidates:
        decoded_candidate = urllib.parse.unquote(str(candidate or ""))
        lowered_candidate = decoded_candidate.lower()
        for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mkv", ".webm", ".m4a", ".mp3"):
            if ext in lowered_candidate:
                return ext
    return ""


def _looks_like_http_media_url(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return False
    if _infer_media_extension_from_url(normalized):
        return True
    parsed = urllib.parse.urlsplit(normalized)
    lower_path = (parsed.path or "").lower()
    return lower_path.endswith((".mp4", ".mkv", ".webm", ".m4a", ".mp3"))


def _split_stream_and_direct_candidates(candidate_urls, skip_preview=True, direct_filter=None):
    manifest_candidates = []
    direct_media_candidates = []
    for candidate in _dedupe_download_urls(candidate_urls):
        candidate_text = str(candidate or "")
        lowered = candidate_text.lower()
        if not candidate_text:
            continue
        if skip_preview and ("_preview.mp4" in lowered or "/preview.mp4" in lowered):
            continue
        if _looks_like_manifest_url(candidate_text):
            if not lowered.endswith(".m3u8.jpg"):
                manifest_candidates.append(candidate_text)
        elif _looks_like_http_media_url(candidate_text) and (direct_filter is None or direct_filter(candidate_text)):
            direct_media_candidates.append(candidate_text)
    return manifest_candidates, direct_media_candidates


def _pick_primary_with_fallbacks(candidates):
    ordered = _dedupe_download_urls(candidates)
    primary = ordered[0] if ordered else ""
    fallbacks = [candidate for candidate in ordered[1:] if candidate != primary] if primary else []
    return primary, fallbacks


def _classify_gimy_stream_candidate(url):
    normalized = _normalize_download_url(url)
    if not normalized:
        return "", ""
    if _looks_like_manifest_url(normalized):
        return "manifest", normalized
    if re.match(r"^https?://", normalized, re.IGNORECASE):
        return "external", normalized
    return "", normalized


SUPPORTED_DOWNLOAD_PAGE_NETLOC_MARKERS = (
    "gimy",
    "movieffm",
    "missav",
    "avjoy",
    "hanime1.me",
    "hanimeone.me",
    "anime1",
    "instagram.com",
    "facebook.com",
    "threads.net",
    "twitter.com",
    "x.com",
    "fxtwitter.com",
    "vxtwitter.com",
    "youtube.com",
    "youtu.be",
    "jable",
    "njavtv",
    "xiaoyakankan",
    "tiktok.com",
)


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


def get_yt_dlp_module():
    global yt_dlp
    if yt_dlp is None:
        try:
            import yt_dlp as yt_dlp_module  # type: ignore
        except Exception as exc:
            raise RuntimeError("yt-dlp is unavailable in this environment") from exc
        yt_dlp = yt_dlp_module
    return yt_dlp


def _ytdlp_retry_sleep_http(*args, **kwargs):
    count = kwargs.get("n")
    if count is None and args:
        count = args[0]
    count = max(int(count or 0), 0)
    return min(float(2 ** max(count - 1, 0)), 10.0)


def _ytdlp_retry_sleep_fragment(*args, **kwargs):
    count = kwargs.get("n")
    if count is None and args:
        count = args[0]
    count = max(int(count or 0), 0)
    return min(float(1.5 * (2 ** max(count - 1, 0))), 12.0)


def _ytdlp_retry_sleep_file_access(*args, **kwargs):
    count = kwargs.get("n")
    if count is None and args:
        count = args[0]
    count = max(int(count or 0), 0)
    return min(float(max(count, 1)), 5.0)


def _make_ytdlp_http_headers(referer=None, origin=None, user_agent=DEFAULT_USER_AGENT):
    headers = {"User-Agent": user_agent}
    if referer:
        headers["Referer"] = referer
    if origin:
        headers["Origin"] = origin
    return headers


def _make_site_root_headers(site_root, referer=None, user_agent=DEFAULT_USER_AGENT):
    root = str(site_root or "").rstrip("/")
    return _make_ytdlp_http_headers(referer=referer or (root + "/" if root else None), origin=root or None, user_agent=user_agent)


def _make_ajax_http_headers(referer=None, origin=None, user_agent=DEFAULT_USER_AGENT):
    headers = _make_ytdlp_http_headers(referer=referer, origin=origin, user_agent=user_agent)
    headers["X-Requested-With"] = "XMLHttpRequest"
    return headers


def _make_browser_page_headers(referer=None, origin=None, user_agent=DEFAULT_USER_AGENT):
    headers = _make_ytdlp_http_headers(referer=referer, origin=origin, user_agent=user_agent)
    headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    return headers


def _make_hls_http_headers(referer=None, origin=None, user_agent=DEFAULT_USER_AGENT):
    headers = _make_ytdlp_http_headers(referer=referer, origin=origin, user_agent=user_agent)
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    headers["Accept-Encoding"] = "identity"
    headers["Connection"] = "keep-alive"
    return headers


def _format_ffmpeg_header_lines(headers):
    return "".join(f"{key}: {value}\r\n" for key, value in (headers or {}).items() if value)


def _make_range_http_headers(headers=None, range_value="bytes=0-0"):
    range_headers = dict(headers or {})
    range_headers["Accept-Encoding"] = "identity"
    if range_value:
        range_headers["Range"] = range_value
    return range_headers


def _apply_ytdlp_audio_postprocessing(ydl_opts):
    ydl_opts["format"] = "bestaudio/best"
    ydl_opts["postprocessors"] = [
        {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
    ]
    return ydl_opts


def _build_ytdlp_download_opts(
    *,
    progress_hook,
    home_dir,
    temp_dir,
    outtmpl,
    concurrent_fragment_downloads,
    http_headers=None,
    format_selector="bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
    merge_output_format="mp4",
    extractor_args=None,
    hls_prefer_native=False,
    hls_use_mpegts=None,
    socket_timeout=None,
    http_chunk_size=None,
    throttled_rate_bps=None,
):
    opts = {
        "color": "never",
        "nopart": False,
        "continuedl": True,
        "nocheckcertificate": True,
        "retries": 20,
        "fragment_retries": 30,
        "extractor_retries": 5,
        "file_access_retries": 10,
        "skip_unavailable_fragments": False,
        "concurrent_fragment_downloads": int(concurrent_fragment_downloads),
        "paths": {"home": home_dir, "temp": temp_dir},
        "outtmpl": {"default": outtmpl},
        "format": format_selector,
        "merge_output_format": merge_output_format,
        "progress_hooks": [progress_hook],
        "ignoreerrors": False,
        "no_warnings": False,
        "quiet": True,
        "http_headers": dict(http_headers or _make_ytdlp_http_headers()),
        "retry_sleep_functions": {
            "http": _ytdlp_retry_sleep_http,
            "fragment": _ytdlp_retry_sleep_fragment,
            "file_access": _ytdlp_retry_sleep_file_access,
            "extractor": _ytdlp_retry_sleep_http,
        },
        "ffmpeg_location": _APP_DIR,
    }
    if extractor_args:
        opts["extractor_args"] = extractor_args
    if hls_prefer_native:
        opts["hls_prefer_native"] = True
    if hls_use_mpegts is not None:
        opts["hls_use_mpegts"] = bool(hls_use_mpegts)
    if socket_timeout is not None:
        opts["socket_timeout"] = float(socket_timeout)
    if http_chunk_size:
        opts["http_chunk_size"] = int(http_chunk_size)
    if throttled_rate_bps:
        opts["throttledratelimit"] = int(throttled_rate_bps)
    return opts


def _site_tuning_value(site, values_by_site, default_value):
    return values_by_site.get((site or "").strip().lower(), default_value)


def _ytdlp_native_concurrency_for_site(site):
    return int(_site_tuning_value(site, YTDLP_HLS_NATIVE_CONCURRENT_FRAGMENTS_BY_SITE, YTDLP_HLS_NATIVE_CONCURRENT_FRAGMENTS))


def _ytdlp_generic_concurrency_for_site(site):
    return int(_site_tuning_value(site, YTDLP_GENERIC_CONCURRENT_FRAGMENTS_BY_SITE, YTDLP_GENERIC_CONCURRENT_FRAGMENTS))


def _ytdlp_http_chunk_size_for_site(site):
    return int(_site_tuning_value(site, YTDLP_HTTP_CHUNK_SIZE_BY_SITE, YTDLP_HTTP_CHUNK_SIZE))


def _ytdlp_throttled_rate_for_site(site):
    return int(_site_tuning_value(site, YTDLP_THROTTLED_RATE_BPS_BY_SITE, YTDLP_THROTTLED_RATE_BPS))


class UIThrottler:
    def __init__(self, root, tree, update_interval=1.0):
        self.root = root
        self.tree = tree
        self.update_interval = update_interval
        self._last_updates = {}
        self._pending_updates = {}
        self._lock = threading.Lock()
        self._flush_scheduled = False
        self._stopped = False
        self._column_index_cache = None

    def _flush_delay_ms(self):
        interval = max(float(self.update_interval or 0.0), 0.12)
        return max(120, int(interval * 1000))

    def update(self, item_id, col, value, force=False):
        self.update_many(item_id, {col: value}, force=force)

    def update_many(self, item_id, updates, force=False):
        if self._stopped:
            return
        if not updates:
            return
        if "status" in updates:
            force = True
        now = time.time()
        with self._lock:
            last_map = self._last_updates.setdefault(item_id, {})
            pending_map = self._pending_updates.setdefault(item_id, {})
            changed = False
            for col, value in updates.items():
                last_val, _last_time = last_map.get(col, (None, 0))
                if not force and value == last_val and pending_map.get(col) == value:
                    continue
                last_map[col] = (value, now)
                pending_map[col] = value
                changed = True
            if not changed:
                if not pending_map:
                    self._pending_updates.pop(item_id, None)
                return
            if self._flush_scheduled:
                return
            self._flush_scheduled = True
        try:
            self.root.after(self._flush_delay_ms(), self._flush_updates)
        except Exception:
            with self._lock:
                self._flush_scheduled = False
            return

    def _column_indexes(self):
        if self._column_index_cache is None:
            try:
                columns = tuple(self.tree["columns"])
            except Exception:
                columns = ()
            self._column_index_cache = {column: index for index, column in enumerate(columns)}
        return self._column_index_cache

    def _flush_updates(self):
        try:
            with self._lock:
                if self._stopped:
                    self._pending_updates = {}
                    self._flush_scheduled = False
                    return
                pending = self._pending_updates
                self._pending_updates = {}
                self._flush_scheduled = False
            column_indexes = self._column_indexes()
            for item_id, updates in pending.items():
                try:
                    current_values = list(self.tree.item(item_id, "values"))
                except tk.TclError:
                    continue
                if len(current_values) < len(column_indexes):
                    current_values.extend([""] * (len(column_indexes) - len(current_values)))
                row_changed = False
                fallback_updates = []
                for col, value in updates.items():
                    index = column_indexes.get(col)
                    if index is None:
                        fallback_updates.append((col, value))
                        continue
                    value_text = "" if value is None else str(value)
                    if current_values[index] == value_text:
                        continue
                    current_values[index] = value_text
                    row_changed = True
                try:
                    if row_changed:
                        self.tree.item(item_id, values=tuple(current_values))
                    for col, value in fallback_updates:
                        current = self.tree.set(item_id, column=col)
                        value_text = "" if value is None else str(value)
                        if current == value_text:
                            continue
                        self.tree.set(item_id, column=col, value=value_text)
                except tk.TclError:
                    continue
        except ResumeLowSpeedReanalysisException:
            raise
        except Exception:
            return

    def stop(self):
        with self._lock:
            self._stopped = True
            self._pending_updates = {}
            self._flush_scheduled = False


class DaemonThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads >= self._max_workers:
            return
        thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
        worker = concurrent.futures.thread._worker
        thread = threading.Thread(
            name=thread_name,
            target=worker,
            args=(weakref.ref(self, weakref_cb), self._work_queue, self._initializer, self._initargs),
        )
        thread.daemon = True
        thread.start()
        self._threads.add(thread)
        concurrent.futures.thread._threads_queues[thread] = self._work_queue


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
        entries = [_normalize_state_entry(entry) for entry in raw]
        seen_temp_paths = set()
        for entry in entries:
            temp_filename = str(entry.get("temp_filename", "") or "").strip()
            if not temp_filename:
                continue
            temp_key = os.path.normcase(os.path.abspath(temp_filename))
            if temp_key in seen_temp_paths:
                entry["temp_filename"] = ""
                entry["resume_requested"] = False
                continue
            seen_temp_paths.add(temp_key)
        return entries
    except Exception:
        return []


def _should_prefer_native_hls(url, task=None):
    parsed = urllib.parse.urlparse(str(url or ""))
    host = str(parsed.netloc or "").strip().lower()
    source_site = _task_source_site_name(task)
    if source_site == "movieffm" and _should_use_ffmpeg_for_movieffm_manifest(url):
        return False
    if source_site in NATIVE_HLS_ALWAYS_SITES:
        return True
    if source_site in NATIVE_HLS_NEVER_SITES:
        return False
    site_markers = NATIVE_HLS_HOST_MARKERS_BY_SITE.get(source_site, ())
    if any(marker in host for marker in site_markers):
        return True
    if any(marker in host for marker in NATIVE_HLS_GLOBAL_HOST_MARKERS):
        return True
    return False


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


def summarize_error_message(exc, fallback_key, limit=120):
    message = RE_ANSI_ESCAPE.sub("", str(exc) if isinstance(exc, str) else str(exc or "")).strip()
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


def format_transfer_size(downloaded_bytes=None, total_bytes=None):
    try:
        downloaded = None if downloaded_bytes is None else max(float(downloaded_bytes or 0), 0.0)
        total = None if total_bytes is None else max(float(total_bytes or 0), 0.0)
    except (TypeError, ValueError):
        return "-"
    if total and total > 0 and downloaded is not None:
        return f"{downloaded / (1024 * 1024):.1f} / {total / (1024 * 1024):.1f} MB"
    if downloaded is not None and downloaded > 0:
        return f"{downloaded / (1024 * 1024):.1f} MB"
    if total and total > 0:
        return f"{total / (1024 * 1024):.1f} MB"
    return "-"


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
    parsed = urllib.parse.urlsplit(str(url or "").strip())
    path_segments = [segment for segment in parsed.path.split("/") if segment]
    short_name = path_segments[-1] if path_segments else ""
    if not short_name:
        short_name = parsed.netloc or (url if isinstance(url, str) else str(url or ""))
    short_name = urllib.parse.unquote(short_name).strip()
    if not short_name:
        short_name = "download"
    if len(short_name) > 35:
        short_name = short_name[:35] + "..."
    if is_mp3:
        short_name = "[MP3] " + short_name
    return short_name


def _safe_output_stem(name, fallback="download", max_chars=150):
    cleaned = re.sub(r"\s+", " ", str(name or "")).strip()
    cleaned = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip(" .-_")
    fallback_text = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", str(fallback or "download")).strip(" .-_") or "download"
    if not cleaned:
        cleaned = fallback_text
    max_len = max(int(max_chars or 0), 32)
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip(" .-_")
    return cleaned or fallback_text


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


def _trim_delimited_log_entries(log_path, max_entries):
    if max_entries <= 0:
        return
    try:
        delimiter_line = "--------------------------------------------------------------------------------"
        with open(log_path, "r", encoding="utf-8") as f:
            log_text = f.read()
        raw_entries = [chunk.strip() for chunk in log_text.split(delimiter_line) if chunk.strip()]
        if len(raw_entries) <= max_entries:
            return
        kept_entries = raw_entries[-max_entries:]
        normalized_text = ""
        for entry in kept_entries:
            normalized_text += entry.rstrip() + f"\n{delimiter_line}\n"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(normalized_text)
    except Exception:
        pass


def write_error_log(context, exc, **extra):
    try:
        now = time.time()
        trace_only = context in TRACE_LOG_CONTEXTS
        log_path = TRACE_LOG_FILE if trace_only else ERROR_LOG_FILE
        signature = (
            log_path,
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
        ]
        if trace_only:
            lines.append(f"message: {exc}")
        else:
            lines.append(f"exception: {type(exc).__name__}: {exc}")
        for key, value in extra.items():
            lines.append(f"{key}: {value}")
        if not trace_only:
            lines.append("traceback:")
            lines.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).rstrip())
        lines.append("--------------------------------------------------------------------------------")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        max_entries = TRACE_LOG_MAX_ENTRIES if log_path == TRACE_LOG_FILE else ERROR_LOG_MAX_ENTRIES
        _trim_delimited_log_entries(log_path, max_entries)
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


_FFMPEG_VERSION_SUMMARY_CACHE = {}


def acquire_single_instance_lock():
    global single_instance_mutex
    if platform.system() != "Windows":
        return True
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        mutex_name = "Global\\AiTestDownloaderSingleInstance"
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        ctypes.set_last_error(0)
        handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not handle:
            return True
        already_exists = ctypes.get_last_error() == 183
        if already_exists:
            kernel32.CloseHandle(handle)
            single_instance_mutex = None
            return False
        single_instance_mutex = handle
        return True
    except Exception:
        return True


def release_single_instance_lock():
    global single_instance_mutex
    if platform.system() != "Windows" or not single_instance_mutex:
        single_instance_mutex = None
        return
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        kernel32.CloseHandle(single_instance_mutex)
    except Exception:
        pass
    finally:
        single_instance_mutex = None


def wait_for_single_instance_lock(timeout_seconds=SINGLE_INSTANCE_ACQUIRE_TIMEOUT_SECONDS, retry_interval_seconds=SINGLE_INSTANCE_ACQUIRE_RETRY_INTERVAL_SECONDS):
    timeout_seconds = max(float(timeout_seconds or 0.0), 0.0)
    retry_interval_seconds = max(float(retry_interval_seconds or 0.0), 0.05)
    deadline = time.time() + timeout_seconds
    last_attempt_failed = False
    while True:
        if acquire_single_instance_lock():
            return True, last_attempt_failed
        last_attempt_failed = True
        if time.time() >= deadline:
            return False, True
        time.sleep(retry_interval_seconds)


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
        self.root.title(f"{t('app_title')} v{APP_BUILD.split('-')[-1]}")
        self.root.geometry("850x680")
        self.root.resizable(True, True)
        self.root.minsize(850, 680)
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
        self._last_reported_domain_limit = None
        self._resume_artifact_locks = {}
        self._resume_artifact_locks_guard = threading.Lock()
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
        self._shutdown_started = False
        self._forced_exit_timer = None
        self._drop_target_widgets = []
        self._active_network_sessions = {}
        self._active_network_sessions_lock = threading.Lock()
        self._startup_resume_pending = False
        self._startup_resume_scheduled = False
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.setup_ui()
        if self.tree is not None:
            self.ui_throttler = UIThrottler(self.root, self.tree, update_interval=UI_THROTTLE_INTERVAL_SECONDS)
            self.throttler = self.ui_throttler
        self.root.after(STARTUP_RESUME_DELAY_MS, self._resume_unfinished_tasks_deferred)
        self.root.after(STARTUP_AUTO_INSTALL_FFMPEG_DELAY_MS, self._auto_install_ffmpeg_if_missing)

    def _auto_install_ffmpeg_if_missing(self):
        if self._shutdown_started:
            return
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
        header_frame.grid(row=0, column=0, padx=20, pady=(12, 4), sticky="ew")
        header_frame.columnconfigure(0, weight=1)
        header_frame.columnconfigure(1, weight=0)
        header_frame.columnconfigure(2, weight=1)
        center_frame = tk.Frame(header_frame, bg="#f4f7fb")
        center_frame.grid(row=0, column=1, sticky="n")
        self.header_title_label = tk.Label(
            center_frame,
            text=t("app_title"),
            font=("Microsoft JhengHei UI", 18, "bold"),
            bg="#f4f7fb",
            fg="#123b5d",
            anchor="center",
            justify="center",
        )
        self.header_title_label.grid(row=0, column=0, sticky="ew")
        self.overview_var = tk.StringVar(value=t("overview_idle"))
        right_frame = tk.Frame(header_frame, bg="#f4f7fb")
        right_frame.grid(row=0, column=2, sticky="ne")
        tk.Label(
            right_frame,
            textvariable=self.overview_var,
            font=("Microsoft JhengHei UI", 9, "bold"),
            bg="#ddebf7",
            fg="#224563",
            padx=10,
            pady=5,
            bd=1,
            relief="solid",
        ).grid(row=0, column=0, sticky="e")

        action_bar = tk.Frame(self.root, bg="#f4f7fb")
        action_bar.grid(row=1, column=0, padx=20, pady=(0, 0), sticky="ew")
        action_bar.columnconfigure(0, weight=1)
        action_button_frame = tk.Frame(action_bar, bg="#f4f7fb")
        action_button_frame.grid(row=0, column=0, sticky="")

        def make_action_btn(text, command, bg):
            return tk.Button(
                action_button_frame,
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
            height=230,
            padx=12,
            pady=14,
            bg="#f8fbff",
            fg="#204a69",
            bd=1,
            relief="groove",
        )
        self.settings_frame = settings_frame
        settings_frame.grid(row=2, column=0, padx=20, pady=(2, 0), sticky="ew")
        settings_frame.grid_propagate(False)
        settings_frame.columnconfigure(1, weight=1)

        self.save_dir_label = tk.Label(settings_frame, text=t("save_dir"), font=("Microsoft JhengHei UI", 9), bg="#f8fbff", fg="#24435b")
        self.save_dir_label.grid(row=0, column=0, sticky="w", pady=(6, 2))
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
        self.input_frame = input_frame
        input_frame.grid(row=1, column=0, columnspan=3, pady=(20, 8), sticky="ew")
        input_frame.columnconfigure(0, weight=1)
        self.new_url_label = tk.Label(input_frame, text=t("new_url"), font=("Microsoft JhengHei UI", 9), bg="#f8fbff", fg="#d96c00")
        self.new_url_label.grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.url_entry = tk.Entry(input_frame, font=("Segoe UI", 9), relief="groove", bd=1)
        self.url_entry.grid(row=1, column=0, sticky="ew", ipady=1)
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
            right_frame,
            text=t("chk_topmost"),
            variable=self.topmost_var,
            command=toggle_topmost,
            font=("Microsoft JhengHei UI", 9),
            fg="#4e5f6d",
            bg="#f4f7fb",
            activebackground="#f4f7fb",
        )
        self.topmost_checkbox.grid(row=1, column=0, pady=(6, 0), sticky="e")

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
        list_frame.grid(row=3, column=0, padx=20, pady=(10, 12), sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        columns = ("name", "progress", "size", "speed_eta", "status")
        self.tree = ttk.Treeview(list_frame, columns=columns, show="headings", style="App.Treeview", selectmode="extended")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self._configure_task_tree_columns()
        self.tree.tag_configure("row_downloading", background="#ecfdf5")
        self.tree.tag_configure("row_queued", background="#eff6ff")
        self.tree.tag_configure("row_paused", background="#fff7ed")
        self.tree.tag_configure("row_done", background="#f3f4f6")
        self.tree.tag_configure("row_finalizing", background="#f5f3ff")
        self.tree.tag_configure("row_error", background="#fef2f2")
        try:
            self.root.after(350, self._register_primary_drop_targets)
        except Exception:
            self._register_primary_drop_targets()
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
        self.tree.bind("<Control-A>", self.select_all_tasks)
        self.tree.bind("<ButtonPress-1>", self._begin_tree_reorder)
        self.tree.bind("<B1-Motion>", self._drag_reorder_tree)
        self.tree.bind("<ButtonRelease-1>", self._end_tree_reorder)
        self.tree.bind("<<TreeviewSelect>>", self._handle_tree_select)
        self.root.bind_all("<Control-a>", self._handle_select_all_shortcut, add="+")
        self.root.bind_all("<Control-A>", self._handle_select_all_shortcut, add="+")
        self.root.bind_all("<Delete>", self._handle_delete_shortcut, add="+")
        self._refresh_ui_summary()

    def _configure_task_tree_columns(self):
        if self.tree is None:
            return
        column_specs = (
            ("name", "col_name", 220, "w"),
            ("progress", "col_progress", 90, "center"),
            ("size", "col_size", 100, "center"),
            ("speed_eta", "col_speed_eta", 180, "center"),
            ("status", "col_status", 120, "center"),
        )
        for column_id, heading_key, width, anchor in column_specs:
            self.tree.heading(column_id, text=t(heading_key))
            self.tree.column(column_id, width=width, anchor=anchor)

    def _register_drop_target_widget(self, widget):
        if widget is None:
            return
        try:
            supported_types = [token for token in (DND_TEXT, DND_FILES) if token]
            if supported_types and hasattr(widget, "drop_target_register"):
                widget.drop_target_register(*supported_types)
                widget.dnd_bind("<<Drop>>", self.handle_drop)
                if widget not in self._drop_target_widgets:
                    self._drop_target_widgets.append(widget)
        except Exception:
            pass

    def _register_primary_drop_targets(self):
        seen = set()
        for widget in (self.root, self.url_entry):
            if widget is None:
                continue
            widget_id = id(widget)
            if widget_id in seen:
                continue
            seen.add(widget_id)
            self._register_drop_target_widget(widget)

    def _unregister_drop_targets(self):
        widgets = list(reversed(getattr(self, "_drop_target_widgets", [])))
        self._drop_target_widgets = []
        for widget in widgets:
            try:
                if hasattr(widget, "dnd_unbind"):
                    widget.dnd_unbind("<<Drop>>")
            except Exception:
                pass
            try:
                if hasattr(widget, "drop_target_unregister"):
                    widget.drop_target_unregister()
            except TypeError:
                try:
                    supported_types = [token for token in (DND_TEXT, DND_FILES) if token]
                    if supported_types and hasattr(widget, "drop_target_unregister"):
                        widget.drop_target_unregister(*supported_types)
                except Exception:
                    pass
            except Exception:
                pass

    def _prepare_ui_for_shutdown(self):
        self._shutdown_started = True
        try:
            if self.ui_throttler is not None:
                self.ui_throttler.stop()
        except Exception:
            pass
        self._pending_status_styles = {}
        self._status_style_flush_scheduled = False
        self._summary_refresh_scheduled = False
        self._queue_process_scheduled = False
        self._startup_resume_pending = False
        self._startup_resume_scheduled = False
        try:
            self.root.unbind_all("<Control-a>")
            self.root.unbind_all("<Control-A>")
            self.root.unbind_all("<Delete>")
        except Exception:
            pass
        try:
            self._unregister_drop_targets()
        except Exception:
            pass

    def _track_network_session(self, session):
        if session is None:
            return None
        try:
            close_method = getattr(session, "close", None)
            if not callable(close_method):
                return session
        except Exception:
            return session
        try:
            with self._active_network_sessions_lock:
                self._active_network_sessions[id(session)] = session
        except Exception:
            pass
        return session

    def _untrack_network_session(self, session):
        if session is None:
            return None
        try:
            with self._active_network_sessions_lock:
                self._active_network_sessions.pop(id(session), None)
        except Exception:
            pass
        return session

    def _close_network_session(self, session):
        if session is None:
            return
        self._untrack_network_session(session)
        try:
            close_method = getattr(session, "close", None)
            if callable(close_method):
                close_method()
        except Exception:
            pass

    def _close_active_network_sessions(self):
        try:
            with self._active_network_sessions_lock:
                sessions = list(self._active_network_sessions.values())
                self._active_network_sessions.clear()
        except Exception:
            sessions = []
        for session in sessions:
            try:
                close_method = getattr(session, "close", None)
                if callable(close_method):
                    close_method()
            except Exception:
                pass

    def _cancel_forced_exit_timer(self):
        exit_timer = getattr(self, "_forced_exit_timer", None)
        self._forced_exit_timer = None
        if exit_timer is None:
            return
        try:
            exit_timer.cancel()
        except Exception:
            pass

    def _begin_tree_reorder(self, event):
        if self.tree is None:
            return
        region = self.tree.identify("region", event.x, event.y)
        column_id = self.tree.identify_column(event.x)
        item_id = self.tree.identify_row(event.y)
        self._tree_drag_anchor = ""
        self._tree_drag_selection = ()
        self._tree_drag_changed = False
        self._tree_drag_last_order = ()
        self._tree_drag_start_xy = None
        self._tree_pointer_selection_anchor = ""
        self._tree_pointer_selection_changed = False
        self._tree_pointer_selection_start_xy = None
        if region not in ("tree", "cell") or not item_id:
            return
        if event.state & 0x0001 or event.state & 0x0004:
            return
        if column_id != "#1":
            self._tree_pointer_selection_anchor = item_id
            self._tree_pointer_selection_start_xy = (int(event.x), int(event.y))
            return
        self._tree_drag_anchor = item_id
        self._tree_drag_start_xy = (int(event.x), int(event.y))

    def _drag_reorder_tree(self, event):
        if self.tree is None:
            return
        anchor = getattr(self, "_tree_drag_anchor", "")
        if anchor:
            start_xy = getattr(self, "_tree_drag_start_xy", None)
            if start_xy:
                dx = abs(int(event.x) - int(start_xy[0]))
                dy = abs(int(event.y) - int(start_xy[1]))
                if dx < 4 and dy < 4:
                    return
            selected = list(getattr(self, "_tree_drag_selection", ()))
            if not selected:
                current_selection = list(self.tree.selection())
                if anchor in current_selection:
                    selected = [
                        entry_id for entry_id in self.tree.get_children()
                        if entry_id in current_selection
                    ]
                else:
                    selected = [anchor]
                self._tree_drag_selection = tuple(selected)
            children = list(self.tree.get_children())
            selected = [entry_id for entry_id in children if entry_id in selected]
            if not selected:
                return
            target = self.tree.identify_row(event.y)
            remaining = [entry_id for entry_id in children if entry_id not in selected]
            if not remaining:
                return "break"
            if target and target in remaining:
                insert_at = remaining.index(target)
                bbox = self.tree.bbox(target)
                if bbox:
                    midpoint = bbox[1] + (bbox[3] / 2.0)
                    if event.y > midpoint:
                        insert_at += 1
            elif target:
                return "break"
            else:
                insert_at = 0 if event.y < 0 else len(remaining)
            new_order = tuple(remaining[:insert_at] + selected + remaining[insert_at:])
            if new_order == tuple(children) or new_order == getattr(self, "_tree_drag_last_order", ()):
                return "break"
            for index, entry_id in enumerate(new_order):
                self.tree.move(entry_id, "", index)
            self.tree.selection_set(selected)
            self._focus_tree_item(selected[0], preserve_selection=True)
            self._tree_drag_changed = True
            self._tree_drag_last_order = new_order
            return "break"
        selection_anchor = getattr(self, "_tree_pointer_selection_anchor", "")
        if not selection_anchor:
            return
        start_xy = getattr(self, "_tree_pointer_selection_start_xy", None)
        if start_xy:
            dx = abs(int(event.x) - int(start_xy[0]))
            dy = abs(int(event.y) - int(start_xy[1]))
            if dx < 4 and dy < 4:
                return
        target = self.tree.identify_row(event.y)
        if not target:
            children = list(self.tree.get_children())
            if not children:
                return
            target = children[0] if event.y < 0 else children[-1]
        ordered_ids = list(self.tree.get_children())
        if selection_anchor not in ordered_ids or target not in ordered_ids:
            return
        anchor_index = ordered_ids.index(selection_anchor)
        target_index = ordered_ids.index(target)
        start_index = min(anchor_index, target_index)
        end_index = max(anchor_index, target_index)
        selected = ordered_ids[start_index:end_index + 1]
        if not selected:
            return
        self.tree.selection_set(selected)
        self._focus_tree_item(target, preserve_selection=True)
        self._tree_pointer_selection_changed = True
        return "break"

    def _rebuild_task_order_from_tree(self):
        if self.tree is None or not self.tasks:
            return
        ordered_ids = [item_id for item_id in self.tree.get_children() if item_id in self.tasks]
        if not ordered_ids:
            return
        if ordered_ids == list(self.tasks.keys()):
            return
        existing_tasks = self.tasks
        reordered_tasks = {item_id: existing_tasks[item_id] for item_id in ordered_ids}
        for item_id, task in existing_tasks.items():
            if item_id not in reordered_tasks:
                reordered_tasks[item_id] = task
        self.tasks = reordered_tasks
        try:
            self.persist_unfinished_state(force=True)
        except Exception:
            pass
        self._schedule_process_queue()

    def _end_tree_reorder(self, event=None):
        if getattr(self, "_tree_drag_changed", False):
            self._rebuild_task_order_from_tree()
            self._refresh_ui_summary()
        self._tree_drag_anchor = ""
        self._tree_drag_selection = ()
        self._tree_drag_changed = False
        self._tree_drag_last_order = ()
        self._tree_drag_start_xy = None
        if getattr(self, "_tree_pointer_selection_changed", False):
            self._refresh_ui_summary()
        self._tree_pointer_selection_anchor = ""
        self._tree_pointer_selection_changed = False
        self._tree_pointer_selection_start_xy = None

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
            self.tree.focus(item_ids[0])
            self.tree.see(item_ids[0])
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
            self.root.title(f"{t('app_title')} v{APP_BUILD.split('-')[-1]}")
        except Exception:
            pass
        if self.header_title_label is not None:
            self.header_title_label.configure(text=t("app_title"))
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
            self._configure_task_tree_columns()
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
        pending_tasks = []
        for task in saved_tasks:
            url = _normalize_download_url(_task_field_value(task, "url", "")) or ""
            if not url:
                continue
            pending_tasks.append(
                (
                    url,
                    (
                        str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
                        or (
                            default_short_name_for_url(
                                (_normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url)),
                                is_mp3=bool(_task_field_value(task, "is_mp3", False)),
                            )
                            if (_normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url))
                            else str(t("msg_resume_name") if "msg_resume_name" in I18N_DICT.get(CURRENT_LANG, {}) else "未完成項目").strip()
                        )
                    ),
                    bool(_task_field_value(task, "is_mp3", False)),
                    _task_source_site_name(task),
                    {
                        "fallback_urls": _dedupe_download_urls(_task_field_value(task, "fallback_urls", [])),
                        "source_page": self._get_task_source_page(task),
                        "resolved_url": (_normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""),
                        "resolved_url_saved_at": float(_task_field_value(task, "resolved_url_saved_at", 0.0) or 0.0),
                        "page_refresh_candidates": _task_gimy_page_refresh_candidates(task),
                        "filename": str(_task_field_value(task, "filename", "") or "").strip(),
                        "temp_filename": str(_task_field_value(task, "temp_filename", "") or "").strip(),
                    },
                )
            )
        if not pending_tasks:
            self._startup_resume_pending = False
            return
        self._startup_resume_pending = True

        def enqueue_batch(start_index=0):
            if self._shutdown_started:
                self._startup_resume_pending = False
                return
            end_index = min(start_index + STARTUP_RESUME_BATCH_SIZE, len(pending_tasks))
            for url, name, is_mp3, source_site, extra_task_data in pending_tasks[start_index:end_index]:
                self._start_download_thread(
                    url,
                    name,
                    is_mp3=is_mp3,
                    source_site=source_site,
                    extra_task_data=extra_task_data,
                    resume_requested=True,
                )
            if end_index < len(pending_tasks):
                self.root.after(STARTUP_RESUME_BATCH_DELAY_MS, lambda: enqueue_batch(end_index))
                return
            self._startup_resume_pending = False
            try:
                self.persist_unfinished_state(force=True)
            except Exception:
                pass
            self._schedule_summary_refresh()
            self._schedule_process_queue(delay=0)

        enqueue_batch(0)

    def _resume_unfinished_tasks_deferred(self):
        if self._shutdown_started or self._startup_resume_scheduled:
            return
        self._startup_resume_scheduled = True
        try:
            self.resume_unfinished_tasks()
        finally:
            self._startup_resume_scheduled = False

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
                html_text = str(resp.text or "")
                links_info = []
                for href, title in re.findall(r'<h2[^>]*>\s*<a href="([^"]+)"[^>]*>(.*?)</a>', html_text, re.IGNORECASE | re.DOTALL):
                    normalized_link = _normalize_download_url(urllib.parse.urljoin(new_url, html.unescape(href)))
                    if not normalized_link or not re.match(r"^https://anime1\.(?:me|pw)/\d+/?$", normalized_link):
                        continue
                    clean_title = html.unescape(re.sub(r"<.*?>", "", title)).replace("&#8211;", "-").strip()
                    if not clean_title:
                        continue
                    links_info.append((normalized_link.rstrip("/"), clean_title))
                if not links_info:
                    for href, title in re.findall(r'<a href="([^"]+)"[^>]*>(.*?)</a>', html_text, re.IGNORECASE | re.DOTALL):
                        normalized_link = _normalize_download_url(urllib.parse.urljoin(new_url, html.unescape(href)))
                        if not normalized_link or not re.match(r"^https://anime1\.(?:me|pw)/\d+/?$", normalized_link):
                            continue
                        clean_title = html.unescape(re.sub(r"<.*?>", "", title)).replace("&#8211;", "-").strip()
                        if not clean_title or not re.search(r"\[\d+\]", clean_title):
                            continue
                        links_info.append((normalized_link.rstrip("/"), clean_title))
                deduped_links_info = []
                seen_links = set()
                for link, title in links_info:
                    if link in seen_links:
                        continue
                    seen_links.add(link)
                    deduped_links_info.append((link, title))
                links_info = deduped_links_info
                if not links_info:
                    self._schedule_warning(t("msg_fetch_anime1_empty"))
                    return

                def enqueue():
                    ordered_links = list(reversed(links_info))
                    targets = self._choose_playlist_targets(ordered_links, ordered_links[0])
                    for link, title in targets:
                        clean_title = html.unescape(title).replace("&#8211;", "-").strip()
                        self._final_add_download(
                            link,
                            is_mp3,
                            clean_title,
                            source_site="anime1",
                            extra_task_data=self._build_extra_task_data(source_page=new_url),
                        )

                self._schedule_ui_call(enqueue)
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

                if not re.search(
                    r'href=[\"\'](/(?:(?:vod)?play/[0-9]+\-[0-9]+\-[0-9]+\.html|video/[0-9]+\-[0-9]+\.html(?:#sid=\d+)?|eps/[0-9]+\-[0-9]+(?:\-[0-9]+)?\.html))[\"\'][^>]*>(.*?)</a>',
                    resp_text,
                ):
                    self._schedule_warning(t("err_site_parse"))
                    return
                drama_name = "Gimy"
                m_title = re.search(r"<title>(.*?)</title>", resp_text)
                if m_title:
                    drama_name = _clean_gimy_title(html.unescape(m_title.group(1)).split("-")[0].strip()) or drama_name
                    drama_name = "".join(c for c in drama_name if c not in '\\/:*?"<>|')
                entries = self._extract_gimy_detail_entries(resp_text, base, drama_name)
                if not entries:
                    self._schedule_warning("Gimy 此頁所有劇集目前播放失效")
                    return

                def enqueue():
                    if self._is_gimy_movie_detail(entries):
                        ordered_entries = sorted(entries, key=lambda entry: self._gimy_movie_source_priority(entry.get("title", "")))
                        primary = ordered_entries[0]
                        fallback_urls = [entry["url"] for entry in ordered_entries[1:] if entry["url"] != primary["url"]]
                        self._final_add_download(
                            primary["url"],
                            is_mp3,
                            drama_name,
                            "gimy",
                            self._build_extra_task_data(source_page=new_url, fallback_urls=fallback_urls),
                        )
                        return
                    episode_entries = self._group_gimy_episode_entries(entries)
                    episodes = [(entry["url"], entry["full_name"]) for entry in episode_entries]
                    targets = self._choose_playlist_targets(episodes, episodes[0])
                    for ep_url, full_name in targets:
                        grouped_entry = next((entry for entry in episode_entries if entry["url"] == ep_url), None)
                        self._final_add_download(
                            ep_url,
                            is_mp3,
                            full_name,
                            "gimy",
                            self._build_extra_task_data(
                                source_page=new_url,
                                fallback_urls=(grouped_entry or {}).get("fallback_urls"),
                            ),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as e:
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(e)[:40]}")

        def fetch_hanime_playlist():
            try:
                parsed = urllib.parse.urlparse(new_url)
                qs = urllib.parse.parse_qs(parsed.query)
                list_id = (qs.get("list") or [""])[0]
                if not list_id:
                    return
                playlist_base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'hanime1.me'}"
                pl_url = urllib.parse.urljoin(playlist_base, "/playlist?list=" + list_id)
                c_req = get_curl_cffi_requests()
                resp = c_req.get(pl_url, impersonate="chrome110", timeout=15)
                seen = set()
                links = []
                for match in re.finditer(r'href=[\"\'](https?://(?:hanime1|hanimeone)\.me/watch\?v=[^\"\']+)[\"\']', resp.text):
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
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(e)[:40]}")

        def fetch_movieffm_drama():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                drama_title, episodes, episode_fallbacks = _collect_movieffm_drama_episodes(resp.text, new_url, "MovieFFM")
                if not episodes:
                    page_title, candidates, external_source_urls, player_data = _extract_movieffm_playback_candidates(resp.text, drama_title)
                    if not candidates and not player_data and not external_source_urls:
                        self._schedule_warning(t("msg_fetch_movieffm_empty"))
                        return

                    def enqueue_single():
                        if not candidates and external_source_urls:
                            preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                                external_source_urls[0],
                                external_source_urls[1:],
                                source_site="movieffm",
                            )
                            if not preferred_url or not has_reachable:
                                self._schedule_warning("MovieFFM 來源主機目前無法解析")
                                return
                            self._final_add_download(
                                preferred_url,
                                is_mp3=is_mp3,
                                custom_name=page_title,
                                source_site="movieffm",
                                extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=ordered_fallbacks),
                            )
                            return
                        if not candidates:
                            self._schedule_warning(t("msg_fetch_movieffm_empty"))
                            return
                        preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                            candidates[0],
                            candidates[1:],
                            source_site="movieffm",
                        )
                        if not preferred_url or not has_reachable:
                            self._schedule_warning("MovieFFM 來源主機目前無法解析")
                            return
                        self._final_add_download(
                            preferred_url,
                            is_mp3=is_mp3,
                            custom_name=page_title,
                            source_site="movieffm",
                            extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=ordered_fallbacks),
                        )

                    self._schedule_ui_call(enqueue_single)
                    return

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, episodes[0])
                    skipped_names = []
                    for ep_url, ep_name in targets:
                        episode_key = _movieffm_numbered_episode_key(ep_name.rsplit(" ", 1)[-1])
                        fallback_urls = [u for u in episode_fallbacks.get(episode_key, []) if u != ep_url]
                        preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(ep_url, fallback_urls, source_site="movieffm")
                        if not preferred_url or not has_reachable:
                            skipped_names.append(ep_name)
                            continue
                        self._final_add_download(
                            preferred_url,
                            is_mp3=is_mp3,
                            custom_name=ep_name,
                            source_site="movieffm",
                            extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=ordered_fallbacks),
                        )
                    if skipped_names:
                        skipped_label = ", ".join(name.rsplit(" ", 1)[-1] for name in skipped_names[:5])
                        more_count = len(skipped_names) - min(len(skipped_names), 5)
                        if more_count > 0:
                            skipped_label = f"{skipped_label} +{more_count}"
                        self._schedule_warning(f"MovieFFM 有 {len(skipped_names)} 集來源無法解析，已略過：{skipped_label}")

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("movieffm parse failure", exc, url=new_url)
                self._schedule_error(t("msg_fetch_movieffm_failed", error=exc))

        def fetch_movieffm_tvshows():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                _, detail_pages = _collect_movieffm_tvshow_detail_pages(resp.text, new_url, "MovieFFM")
                if not detail_pages:
                    self._schedule_warning(t("msg_fetch_movieffm_empty"))
                    return
                episodes = []
                seen_urls = set()
                for detail_url, _season_name in detail_pages:
                    detail_resp = c_req.get(detail_url, impersonate="chrome110", timeout=15, headers={"Referer": detail_url})
                    _drama_title, drama_episodes, episode_fallbacks = _collect_movieffm_drama_episodes(detail_resp.text, detail_url, "MovieFFM")
                    for ep_url, ep_name in drama_episodes:
                        if ep_url in seen_urls:
                            continue
                        seen_urls.add(ep_url)
                        episode_key = _movieffm_numbered_episode_key(ep_name.rsplit(" ", 1)[-1])
                        fallback_urls = [u for u in episode_fallbacks.get(episode_key, []) if u != ep_url]
                        episodes.append((ep_url, ep_name, detail_url, fallback_urls))
                if not episodes:
                    self._schedule_warning(t("msg_fetch_movieffm_empty"))
                    return

                def enqueue():
                    selected_targets = self._choose_playlist_targets(episodes, episodes[0])
                    skipped_names = []
                    for ep_url, ep_name, detail_url, fallback_urls in selected_targets:
                        preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(ep_url, fallback_urls, source_site="movieffm")
                        if not preferred_url or not has_reachable:
                            skipped_names.append(ep_name)
                            continue
                        self._final_add_download(
                            preferred_url,
                            is_mp3=is_mp3,
                            custom_name=ep_name,
                            source_site="movieffm",
                            extra_task_data=self._build_extra_task_data(source_page=detail_url, fallback_urls=ordered_fallbacks),
                        )
                    if skipped_names:
                        skipped_label = ", ".join(name.rsplit(" ", 1)[-1] for name in skipped_names[:5])
                        more_count = len(skipped_names) - min(len(skipped_names), 5)
                        if more_count > 0:
                            skipped_label = f"{skipped_label} +{more_count}"
                        self._schedule_warning(f"MovieFFM 有 {len(skipped_names)} 集來源無法解析，已略過：{skipped_label}")

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("movieffm tvshows parse failure", exc, url=new_url)
                self._schedule_error(t("msg_fetch_movieffm_failed", error=exc))

        def fetch_movieffm_single():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                page_title, candidates, external_source_urls, player_data = _extract_movieffm_playback_candidates(resp.text, "MovieFFM")
                if not candidates and not player_data and not external_source_urls:
                    self._schedule_warning(t("msg_fetch_movieffm_empty"))
                    return

                def enqueue_single():
                    if not candidates and external_source_urls:
                        self._final_add_download(
                            external_source_urls[0],
                            is_mp3=is_mp3,
                            custom_name=page_title,
                            source_site="movieffm",
                            extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=external_source_urls[1:]),
                        )
                        return
                    if not candidates:
                        self._schedule_warning(t("msg_fetch_movieffm_empty"))
                        return
                    self._final_add_download(
                        candidates[0],
                        is_mp3=is_mp3,
                        custom_name=page_title,
                        source_site="movieffm",
                        extra_task_data=self._build_extra_task_data(source_page=new_url, fallback_urls=candidates[1:]),
                    )

                self._schedule_ui_call(enqueue_single)
            except Exception as exc:
                write_error_log("movieffm single parse failure", exc, url=new_url)
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
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(exc)[:120]}")

        def fetch_nnyy_playlist():
            try:
                parsed_url = urllib.parse.urlsplit(new_url)
                if re.search(r"/dianying/\d+\.html$", parsed_url.path, re.IGNORECASE):
                    c_req = get_curl_cffi_requests()
                    resp = c_req.get(
                        new_url,
                        impersonate="chrome110",
                        timeout=20,
                        headers=_make_ytdlp_http_headers(referer="https://nnyy.in/"),
                    )
                    page_title = _extract_html_title(resp.text, "努努影院")
                    page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or "努努影院"
                    self._final_add_download(
                        new_url,
                        is_mp3=is_mp3,
                        custom_name=page_title,
                        source_site="nnyy",
                        extra_task_data=self._build_extra_task_data(source_page=new_url),
                    )
                    return
                if not re.search(r"/(?:dianshiju|dongman|zongyi)/\d+\.html$", parsed_url.path, re.IGNORECASE):
                    self._final_add_download(new_url, is_mp3=is_mp3)
                    return
                c_req = get_curl_cffi_requests()
                resp = c_req.get(
                    new_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers=_make_ytdlp_http_headers(referer="https://nnyy.in/"),
                )
                page_id = _extract_nnyy_page_id(new_url)
                if not page_id:
                    raise Exception("NNYY page id missing")
                episode_entries, default_slug = _extract_nnyy_episode_entries(resp.text)
                if not episode_entries:
                    raise Exception("NNYY episode list missing")
                page_title = _extract_html_title(resp.text, "努努影院")
                page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or "努努影院"
                base_page_url = urllib.parse.urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
                episodes = []
                episode_pad_width = max(2, len(str(len(episode_entries) or 0)))
                default_episode_url = f"{base_page_url}?ep={default_slug}" if default_slug else ""
                for ep_slug, ep_name in episode_entries:
                    ep_url = f"{base_page_url}?ep={urllib.parse.quote(ep_slug)}"
                    normalized_ep_name = _normalize_nnyy_episode_name(ep_name, ep_slug=ep_slug, pad_width=episode_pad_width)
                    display_name = f"{page_title} {normalized_ep_name}".strip()
                    episodes.append((ep_url, display_name))
                episodes = _sort_download_targets_naturally(episodes)
                selected_episode = next((episode for episode in episodes if episode[0] == default_episode_url), episodes[0])

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, selected_episode)
                    for ep_url, ep_name in targets:
                        self._final_add_download(
                            ep_url,
                            is_mp3=is_mp3,
                            custom_name=ep_name,
                            source_site="nnyy",
                            extra_task_data=self._build_extra_task_data(source_page=base_page_url),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("nnyy series parse failure", exc, url=new_url)
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(exc)[:120]}")

        def fetch_777tv_playlist():
            try:
                parsed_url = urllib.parse.urlsplit(new_url)
                if not re.search(r"/vod/detail/id/\d+\.html$", parsed_url.path, re.IGNORECASE):
                    self._final_add_download(new_url, is_mp3=is_mp3)
                    return
                c_req = get_curl_cffi_requests()
                resp = c_req.get(
                    new_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers=_make_ytdlp_http_headers(referer="https://777tv.ai/"),
                )
                episode_entries = _extract_777tv_episode_entries(resp.text)
                if not episode_entries:
                    self._final_add_download(new_url, is_mp3=is_mp3)
                    return
                page_title = _extract_html_title(resp.text, "777TV")
                page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or "777TV"
                episodes = []
                for play_url, ep_name in episode_entries:
                    display_name = f"{page_title} {ep_name}".strip() if ep_name else page_title
                    episodes.append((play_url, display_name))
                episodes = _sort_download_targets_naturally(episodes)

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, episodes[0])
                    for ep_url, ep_name in targets:
                        self._final_add_download(
                            ep_url,
                            is_mp3=is_mp3,
                            custom_name=ep_name,
                            source_site="777tv",
                            extra_task_data=self._build_extra_task_data(source_page=new_url),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("777tv series parse failure", exc, url=new_url)
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(exc)[:120]}")

        def fetch_3kor_list():
            try:
                parsed_url = urllib.parse.urlsplit(new_url)
                if not re.search(r"/list/\d+[^/]*\.html$", parsed_url.path, re.IGNORECASE):
                    self._final_add_download(new_url, is_mp3=is_mp3)
                    return
                c_req = get_curl_cffi_requests()
                resp = c_req.get(
                    new_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers=_make_ytdlp_http_headers(referer="https://3kor.com/"),
                )
                detail_entries = _extract_3kor_detail_entries(resp.text)
                if not detail_entries:
                    raise Exception("3KOR list page did not expose detail links")

                def enqueue():
                    targets = self._choose_playlist_targets(detail_entries, detail_entries[0])
                    for detail_url, detail_name in targets:
                        self._final_add_download(
                            detail_url,
                            is_mp3=is_mp3,
                            custom_name=detail_name,
                            source_site="3kor",
                            extra_task_data=self._build_extra_task_data(source_page=new_url),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("3kor list parse failure", exc, url=new_url)
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(exc)[:120]}")

        def fetch_3kor_detail():
            try:
                parsed_url = urllib.parse.urlsplit(new_url)
                if not re.search(r"/detail/\d+\.html$", parsed_url.path, re.IGNORECASE):
                    self._final_add_download(new_url, is_mp3=is_mp3)
                    return
                c_req = get_curl_cffi_requests()
                resp = c_req.get(
                    new_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers=_make_ytdlp_http_headers(referer="https://3kor.com/"),
                )
                page_title = _extract_html_title(resp.text, "3KOR")
                page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or "3KOR"
                play_entries = _extract_3kor_play_entries(resp.text)
                if not play_entries:
                    self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="3kor")
                    return
                base_page_url = urllib.parse.urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
                episodes = []
                default_episode_url = f"{base_page_url}?play={urllib.parse.quote(play_entries[0][0])}"
                for play_id, play_name in play_entries:
                    episode_url = f"{base_page_url}?play={urllib.parse.quote(play_id)}"
                    display_name = page_title if len(play_entries) == 1 else f"{page_title} {play_name}".strip()
                    episodes.append((episode_url, display_name))
                episodes = _sort_download_targets_naturally(episodes)
                selected_episode = next((episode for episode in episodes if episode[0] == default_episode_url), episodes[0])

                def enqueue():
                    targets = self._choose_playlist_targets(episodes, selected_episode)
                    for episode_url, episode_name in targets:
                        self._final_add_download(
                            episode_url,
                            is_mp3=is_mp3,
                            custom_name=episode_name,
                            source_site="3kor",
                            extra_task_data=self._build_extra_task_data(source_page=base_page_url),
                        )

                self._schedule_ui_call(enqueue)
            except Exception as exc:
                write_error_log("3kor detail parse failure", exc, url=new_url)
                self._schedule_error(f"{self._ui_text('err_site_parse', '解析失敗')}: {str(exc)[:120]}")

        def fetch_99itv_single():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(new_url, impersonate="chrome110", timeout=15, headers={"Referer": new_url})
                page_title = _clean_99itv_title(_extract_html_title(resp.text, "99iTV"))
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="99itv")
            except Exception as exc:
                write_error_log("99itv parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_njavtv_single():
            try:
                c_req = get_curl_cffi_requests()
                resp = c_req.get(
                    new_url,
                    impersonate="chrome110",
                    timeout=15,
                    headers={"Referer": "https://njavtv.com/", "Origin": "https://njavtv.com", "User-Agent": DEFAULT_USER_AGENT},
                )
                parsed_new_url = urllib.parse.urlsplit(new_url)
                slug = parsed_new_url.path.strip("/").split("/")[-1]
                fallback_title = slug.upper()
                code_m = re.search(r"([A-Za-z]+)-(\d+)", slug, re.IGNORECASE)
                if code_m:
                    fallback_title = f"{code_m.group(1).upper()}-{code_m.group(2)}"
                page_title = _extract_html_title(resp.text, fallback_title)
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="njavtv")
            except Exception as exc:
                write_error_log("njavtv parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_18av_single():
            try:
                c_req = get_curl_cffi_requests()
                parsed_new_url = urllib.parse.urlsplit(new_url)
                site_root = f"{parsed_new_url.scheme}://{parsed_new_url.netloc}"
                resp = c_req.get(
                    new_url,
                    impersonate="chrome120",
                    timeout=15,
                    headers=_make_site_root_headers(site_root),
                )
                fallback_title = default_short_name_for_url(new_url, is_mp3=is_mp3) or "18AV"
                page_title = _clean_18av_title(_extract_html_title(resp.text, fallback_title))
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="18av")
            except Exception as exc:
                write_error_log("18av parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_hohoj_single():
            try:
                c_req = get_curl_cffi_requests()
                parsed_new_url = urllib.parse.urlsplit(new_url)
                site_root = f"{parsed_new_url.scheme}://{parsed_new_url.netloc}"
                resp = c_req.get(
                    new_url,
                    impersonate="chrome120",
                    timeout=15,
                    headers=_make_site_root_headers(site_root),
                )
                fallback_title = default_short_name_for_url(new_url, is_mp3=is_mp3) or "HoHoJ"
                page_title = _clean_hohoj_title(_extract_html_title(resp.text, fallback_title))
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="hohoj")
            except Exception as exc:
                write_error_log("hohoj parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_goodav_single():
            try:
                c_req = get_curl_cffi_requests()
                parsed_new_url = urllib.parse.urlsplit(new_url)
                site_root = f"{parsed_new_url.scheme}://{parsed_new_url.netloc}"
                resp = c_req.get(
                    new_url,
                    impersonate="chrome120",
                    timeout=15,
                    headers=_make_site_root_headers(site_root),
                )
                fallback_title = default_short_name_for_url(new_url, is_mp3=is_mp3) or "GoodAV"
                page_title = _goodav_title_for_display(_extract_html_title(resp.text, fallback_title))
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="goodav17")
            except Exception as exc:
                write_error_log("goodav17 parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_javfilms_single():
            try:
                c_req = get_curl_cffi_requests()
                parsed_new_url = urllib.parse.urlsplit(new_url)
                site_root = f"{parsed_new_url.scheme}://{parsed_new_url.netloc}"
                resp = c_req.get(
                    new_url,
                    impersonate="chrome120",
                    timeout=15,
                    headers=_make_site_root_headers(site_root),
                )
                fallback_title = default_short_name_for_url(new_url, is_mp3=is_mp3) or "JAV Films"
                page_title = _clean_javfilms_title(_extract_html_title(resp.text, fallback_title))
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=page_title, source_site="javfilms")
            except Exception as exc:
                write_error_log("javfilms parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3)

        def fetch_mega_single():
            try:
                display_name = default_short_name_for_url(new_url, is_mp3=is_mp3) or "MEGA"
                normalized_mega_url = str(new_url or "").strip().lower()
                parsed_mega_url = urllib.parse.urlparse(normalized_mega_url) if normalized_mega_url else None
                mega_fragment = str(parsed_mega_url.fragment or "").strip().lower() if parsed_mega_url else ""
                is_mega_folder_link = bool(
                    parsed_mega_url
                    and (
                        "/folder/" in parsed_mega_url.path.lower()
                        or mega_fragment.startswith("f!")
                        or mega_fragment.startswith("folder/")
                    )
                )
                if MegaClient is not None and not is_mega_folder_link:
                    mega_client = MegaClient()
                    file_handle, file_key = _parse_mega_public_file_parts(mega_client, new_url)
                    public_info = mega_client.get_public_file_info(file_handle, file_key) or {}
                    public_name = str(public_info.get("name") or "").strip()
                    if public_name:
                        display_name = os.path.splitext(public_name)[0].strip() or public_name
                self._final_add_download(new_url, is_mp3=is_mp3, custom_name=display_name, source_site="mega")
            except Exception as exc:
                write_error_log("mega parse failure", exc, url=new_url)
                self._final_add_download(new_url, is_mp3=is_mp3, source_site="mega")

        lowered = new_url.lower()
        if isinstance(new_url, str) and new_url.strip():
            parsed_new_url = urllib.parse.urlparse(new_url.strip())
            new_url_query = urllib.parse.parse_qs(parsed_new_url.query, keep_blank_values=True)
        else:
            parsed_new_url = None
            new_url_query = {}
        if parsed_new_url and ("anime1.me" in parsed_new_url.netloc.lower() or "anime1.pw" in parsed_new_url.netloc.lower()) and ("/category/" in parsed_new_url.path.lower() or "cat" in new_url_query):
            self._start_background_parse(fetch_anime1)
            return
        if "gimy" in lowered and ("/detail/" in lowered or "/voddetail/" in lowered or "/voddetail2/" in lowered or "/vod/" in lowered):
            self._start_background_parse(fetch_gimy_detail)
            return
        if ("hanime1.me" in lowered or "hanimeone.me" in lowered) and "list=" in lowered:
            self._start_background_parse(fetch_hanime_playlist)
            return
        if "movieffm.net" in lowered and "/tvshows/" in lowered:
            self._start_background_parse(fetch_movieffm_tvshows)
            return
        if "movieffm.net" in lowered and "/drama/" in lowered:
            self._start_background_parse(fetch_movieffm_drama)
            return
        if "movieffm.net" in lowered and "/chinese-subtitles/" in lowered:
            self._start_background_parse(fetch_movieffm_single)
            return
        if "xiaoyakankan.com" in lowered and "/post/" in lowered:
            self._start_background_parse(fetch_xiaoyakankan_post)
            return
        if "nnyy.in" in lowered and re.search(r"/(?:dianshiju|dongman|zongyi)/\d+\.html(?:[?#].*)?$", lowered, re.IGNORECASE):
            self._start_background_parse(fetch_nnyy_playlist)
            return
        if "777tv.ai" in lowered and re.search(r"/vod/detail/id/\d+\.html(?:[?#].*)?$", lowered, re.IGNORECASE):
            self._start_background_parse(fetch_777tv_playlist)
            return
        if "3kor.com" in lowered and re.search(r"/list/\d+[^/]*\.html(?:[?#].*)?$", lowered, re.IGNORECASE):
            self._start_background_parse(fetch_3kor_list)
            return
        if "3kor.com" in lowered and re.search(r"/detail/\d+\.html(?:[?#].*)?$", lowered, re.IGNORECASE):
            self._start_background_parse(fetch_3kor_detail)
            return
        if "99itv.net" in lowered and re.search(r"/vodplay/\d+-\d+-\d+\.html(?:[?#].*)?$", lowered, re.IGNORECASE):
            self._start_background_parse(fetch_99itv_single)
            return
        if "njavtv.com" in lowered:
            self._start_background_parse(fetch_njavtv_single)
            return
        if "18av.mm-cg.com" in lowered:
            self._start_background_parse(fetch_18av_single)
            return
        if "hohoj.tv" in lowered and "/video" in lowered:
            self._start_background_parse(fetch_hohoj_single)
            return
        if "goodav17.com" in lowered and ("/vr_html/" in lowered or "/html/" in lowered):
            self._start_background_parse(fetch_goodav_single)
            return
        if "javfilms.com" in lowered and "/video/" in lowered:
            self._start_background_parse(fetch_javfilms_single)
            return
        if "mega.nz" in lowered or "mega.co.nz" in lowered:
            self._start_background_parse(fetch_mega_single)
            return
        self._final_add_download(new_url, is_mp3=is_mp3)
        return

    def _final_add_download(self, url, is_mp3, custom_name=None, source_site=None, extra_task_data=None):
        url = _normalize_download_url(url)
        if self._count_live_tasks() >= MAX_QUEUE_TASKS:
            self._show_warning(f"Queue limit reached ({MAX_QUEUE_TASKS}).")
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
            task_url = _normalize_download_url(_task_field_value(task, "url", "")) or ""
            if task_url != normalized_url:
                continue
            if bool(_task_field_value(task, "is_mp3", False)) != bool(is_mp3):
                continue
            if self._is_deleted_state(str(_task_field_value(task, "state", "") or "")):
                continue
            return item_id
        return None

    def _choose_playlist_targets(self, episodes, selected_episode=None):
        if not episodes:
            return []
        episodes = _sort_download_targets_naturally(episodes)
        default_target = selected_episode if selected_episode in episodes else episodes[0]
        if len(episodes) > 1 and self._ask_warning_yesno(t("msg_playlist_add_all", count=len(episodes))):
            return episodes
        return [default_target]

    def _extract_gimy_detail_entries(self, resp_text, base, drama_name):
        def _title_kind(title):
            normalized = re.sub(r"\s+", "", (title or "").strip())
            if not normalized:
                return "blank"
            if re.search(r"(第\s*\d+\s*集|EP\s*\d+|E\d+)", normalized, re.IGNORECASE):
                return "episode"
            if normalized in {"正片", "TC", "HD", "HD中字", "HD國語", "HD国语", "HC中字", "搶先版", "抢先版"}:
                return "source"
            return "other"

        def _title_score(title):
            kind = _title_kind(title)
            if kind == "episode":
                return 30
            if kind == "source":
                return 20
            if kind == "other":
                return 10
            return 0

        def _is_promo_title(title):
            normalized = re.sub(r"\s+", "", (title or "").strip())
            if not normalized:
                return False
            kind = _title_kind(normalized)
            if kind in {"episode", "source"}:
                return False
            return len(normalized) >= 12

        matches = list(
            re.finditer(
                r'href=["\'](/(?:(?:vod)?play/[0-9]+\-[0-9]+\-[0-9]+\.html|video/[0-9]+\-[0-9]+\.html(?:#sid=\d+)?|eps/[0-9]+\-[0-9]+(?:\-[0-9]+)?\.html))["\'][^>]*>(.*?)</a>',
                resp_text,
            )
        )
        entry_map = {}
        for match in matches:
            link = match.group(1)
            title = re.sub(r"<[^>]+>", "", match.group(2)).strip()
            link_lower = link.lower()
            title_lower = title.lower()
            if "yu-gao" in link_lower or "預告" in title or "预告" in title or "trailer" in title_lower or "preview" in title_lower:
                continue
            if _is_promo_title(title):
                continue
            ep_url = urllib.parse.urljoin(base, link)
            number_match = re.search(r"/(?:play|eps)/\d+-(\d+)(?:-(\d+))?\.html", link)
            if number_match:
                line_no = int(number_match.group(1))
                episode_no = int(number_match.group(2)) if number_match.group(2) else line_no
                if not number_match.group(2):
                    line_no = 0
            else:
                number_match = re.search(r"/video/\d+-(\d+)\.html", link)
                line_no = 0
                episode_no = int(number_match.group(1)) if number_match else 0
            full_name = " ".join(part for part in (drama_name, title) if part).strip() or drama_name
            candidate = {
                "url": ep_url,
                "title": title,
                "full_name": full_name,
                "line_no": line_no,
                "episode_no": episode_no,
            }
            existing = entry_map.get(ep_url)
            if existing is None or _title_score(title) > _title_score(existing.get("title", "")):
                entry_map[ep_url] = candidate
        return list(entry_map.values())

    def _is_gimy_movie_detail(self, entries):
        if not entries:
            return False
        unique_lines = {entry["line_no"] for entry in entries if entry.get("line_no")}
        max_episode_no = max((entry.get("episode_no") or 0) for entry in entries)
        episode_title_re = re.compile(r"(第\s*\d+\s*集|EP\s*\d+|E\d+)", re.IGNORECASE)
        has_episode_titles = any(episode_title_re.search(entry.get("title", "")) for entry in entries if entry.get("title"))
        return len(unique_lines) >= 2 and max_episode_no <= 3 and not has_episode_titles

    def _gimy_movie_source_priority(self, title):
        title = (title or "").strip()
        if title == "正片":
            return (0, title)
        if "HD中字" in title:
            return (1, title)
        if "HD國語" in title or "HD国语" in title:
            return (2, title)
        if title == "HD" or title.startswith("HD"):
            return (3, title)
        if "中字" in title:
            return (4, title)
        if "國語" in title or "国语" in title:
            return (5, title)
        if "TC" in title:
            return (6, title)
        if "搶先" in title or "抢先" in title:
            return (7, title)
        if "HC" in title:
            return (8, title)
        if not title:
            return (90, title)
        return (50, title)

    def _gimy_episode_group_key(self, entry):
        episode_no = entry.get("episode_no") or 0
        if episode_no > 0:
            return ("episode_no", episode_no)
        title = (entry.get("title") or "").strip()
        title_match = re.search(r"(?:第\s*(\d+)\s*集|EP\s*(\d+)|E(\d+))", title, re.IGNORECASE)
        if title_match:
            for group in title_match.groups():
                if group:
                    return ("episode_title", int(group))
        return ("url", entry.get("url", ""))

    def _gimy_episode_source_priority(self, entry):
        title = (entry.get("title") or "").strip()
        title_rank = 2
        if re.search(r"(?:第\s*\d+\s*集|EP\s*\d+|E\d+)", title, re.IGNORECASE):
            title_rank = 0
        elif title:
            title_rank = 1
        line_no = entry.get("line_no") or 9999
        return (title_rank, line_no, -len(title), entry.get("url", ""))

    def _group_gimy_episode_entries(self, entries):
        episode_groups = {}
        for entry in entries:
            group_key = self._gimy_episode_group_key(entry)
            bucket = episode_groups.setdefault(group_key, [])
            if all(existing.get("url") != entry.get("url") for existing in bucket):
                bucket.append(entry)

        grouped_entries = []
        for bucket in episode_groups.values():
            ordered = sorted(bucket, key=self._gimy_episode_source_priority)
            primary = dict(ordered[0])
            primary["fallback_urls"] = [candidate["url"] for candidate in ordered[1:] if candidate.get("url") != primary.get("url")]
            grouped_entries.append(primary)

        return grouped_entries

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
            queued_status = t("status_queued") if "status_queued" in I18N_DICT.get(CURRENT_LANG, {}) else "排隊中"
            item_id = self.tree.insert("", tk.END, values=(short_name, "0.0%", "-", "-", queued_status))
            self._apply_row_status_style(item_id, queued_status)

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
                title = _extract_html_title(txt, short_name)
                normalized_title = re.sub(r"\s+", " ", str(title or "")).strip().lower()
                normalized_page = str(txt or "").lower()
                if (
                    any(marker in normalized_title for marker in ("just a moment", "attention required", "checking your browser", "please wait"))
                    or any(marker in normalized_page for marker in ("cf-browser-verification", "cf-challenge", "__cf_chl_", "checking your browser before accessing"))
                ):
                    return
                title = "".join(c for c in title if c not in '\\/:*?"<>|')
                if title and str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "") == "QUEUED":
                    cleaned_title = str(title or "").strip()
                    if cleaned_title:
                        self.tasks[item_id]["short_name"] = cleaned_title
                        self.tasks[item_id]["name"] = cleaned_title
                        self._schedule_tree_update(item_id, "name", cleaned_title)

            self._start_daemon_thread(fetch_title)
        else:
            self._set_task_status_mode_ui(item_id, t("status_queued") if "status_queued" in I18N_DICT.get(CURRENT_LANG, {}) else "排隊中", clear_metrics=True)
            if source_site is None:
                source_site = _task_source_site_name(self.tasks.get(item_id, {}))
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
        task_data["is_mp3"] = bool(_task_field_value(task_data, "is_mp3", False))
        task_data["source_site"] = _task_source_site_name(task_data)
        normalized_extra = self._build_extra_task_data(
            source_page=self._get_task_source_page(task_data),
            fallback_urls=_dedupe_download_urls(_task_field_value(task_data, "fallback_urls", []), primary_url=url),
            resolved_url=(_normalize_download_url(_task_field_value(task_data, "resolved_url", "")) or ""),
            resolved_url_saved_at=float(_task_field_value(task_data, "resolved_url_saved_at", 0.0) or 0.0),
            page_refresh_candidates=_task_gimy_page_refresh_candidates(task_data),
        )
        task_data["fallback_urls"] = normalized_extra.get("fallback_urls", [])
        task_data["source_page"] = normalized_extra.get("source_page", "")
        task_data["resume_requested"] = bool(existing_item_id is not None or resume_requested)
        _set_task_aux_fields(task_data, _stop_reason=None)
        self.tasks[item_id] = task_data
        if self._startup_resume_pending:
            return
        try:
            self.persist_unfinished_state()
        except Exception:
            pass
        self._schedule_summary_refresh()
        self._schedule_process_queue()

    def update_tree(self, item_id, col, value, force=False):
        self.update_tree_many(item_id, {col: value}, force=force)

    def update_tree_many(self, item_id, updates, force=False):
        throttler = self.ui_throttler or self.throttler
        if throttler and self.tree is not None and updates:
            throttler.update_many(item_id, updates, force=force)
            if "status" in updates:
                status_text = updates.get("status")
                self._schedule_status_style(item_id, status_text)
                self._schedule_summary_refresh()

    def _schedule_status_style(self, item_id, status_text):
        if self._shutdown_started:
            return
        self._pending_status_styles[item_id] = status_text
        if self._status_style_flush_scheduled:
            return
        self._status_style_flush_scheduled = True
        try:
            self.root.after(STATUS_STYLE_REFRESH_INTERVAL_MS, self._flush_status_styles)
        except Exception:
            self._status_style_flush_scheduled = False

    def _flush_status_styles(self):
        pending = self._pending_status_styles
        self._pending_status_styles = {}
        self._status_style_flush_scheduled = False
        for item_id, status_text in pending.items():
            self._apply_row_status_style(item_id, status_text)

    def _schedule_summary_refresh(self):
        if self._shutdown_started:
            return
        if self._summary_refresh_scheduled:
            return
        self._summary_refresh_scheduled = True
        try:
            self.root.after(SUMMARY_REFRESH_INTERVAL_MS, self._flush_summary_refresh)
        except Exception:
            self._summary_refresh_scheduled = False

    def _flush_summary_refresh(self):
        self._summary_refresh_scheduled = False
        self._refresh_ui_summary()

    def _schedule_process_queue(self, delay=0):
        if self._shutdown_started:
            return
        if self._queue_process_scheduled:
            return
        self._queue_process_scheduled = True
        try:
            self.root.after(delay, self._flush_process_queue)
        except Exception:
            self._queue_process_scheduled = False

    def _flush_process_queue(self):
        self._queue_process_scheduled = False
        if self._shutdown_started:
            return
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
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        filename = primary_value or secondary_value or ""
        if filename and str(filename).lower().endswith(".mp4") and not bool(_task_field_value(task, "_windows_mp4_compat_done", False)):
            self._ensure_windows_compatible_mp4(item_id, filename)
            _set_task_aux_fields(task, _windows_mp4_compat_done=True)
        if filename and os.path.exists(filename):
            try:
                self._set_task_named_column_text(item_id, "size", format_transfer_size(os.path.getsize(filename)))
            except OSError:
                pass
        self._set_task_status_mode_ui(item_id, t("status_done") if "status_done" in I18N_DICT.get(CURRENT_LANG, {}) else "完成", complete_progress=True)
        _set_task_aux_fields(task, state="FINISHED")
        remove_from_state(_normalize_download_url(_task_field_value(task, "url", "")) or "")

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
        state = str(_task_field_value(task, "state", "") or "")
        if state in ("DELETED", "DELETE_REQUESTED"):
            self._discard_task(item_id)
            return
        if state == "PAUSE_REQUESTED":
            self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
            _set_task_aux_fields(task, state="PAUSED", _stop_reason=None)
            return

    def _is_live_task(self, task):
        return str(_task_field_value(task, "state", "") or "") not in TERMINAL_TASK_STATES

    def _iter_live_tasks(self):
        for task in self.tasks.values():
            if self._is_live_task(task):
                yield task

    def _collect_state_counts(self):
        counts = {"DOWNLOADING": 0, "QUEUED": 0, "PAUSED": 0, "ERROR": 0}
        for task in self.tasks.values():
            if str(_task_field_value(task, "state", "") or "") == "DOWNLOADING":
                counts["DOWNLOADING"] += 1
            elif str(_task_field_value(task, "state", "") or "") == "QUEUED":
                counts["QUEUED"] += 1
            elif str(_task_field_value(task, "state", "") or "") in PAUSED_TASK_STATES:
                counts["PAUSED"] += 1
            elif str(_task_field_value(task, "state", "") or "") == "ERROR":
                counts["ERROR"] += 1
        return counts

    def _show_warning(self, message, parent=None):
        messagebox.showwarning(t("msg_warning") if t("msg_warning") != "msg_warning" else "警告", message, parent=parent)

    def _schedule_warning(self, message, parent=None):
        self._schedule_ui_call(lambda msg=message, target=parent: self._show_warning(msg, parent=target))

    def _reject_unsupported_task_page(self, item_id, warning_message):
        self._schedule_warning(warning_message)
        self._schedule_ui_call(lambda _item_id=item_id: (self._discard_task(_item_id), self._delete_tree_item(_item_id)))

    def _ask_warning_yesno(self, message, parent=None):
        return messagebox.askyesno(t("msg_warning") if t("msg_warning") != "msg_warning" else "警告", message, parent=parent)

    def _show_error(self, message, parent=None):
        messagebox.showerror(t("msg_error") if t("msg_error") != "msg_error" else "錯誤", message, parent=parent)

    def _schedule_error(self, message, parent=None):
        self._schedule_ui_call(lambda msg=message, target=parent: self._show_error(msg, parent=target))

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
        if self._shutdown_started:
            return
        self.root.after(0, callback)

    def _schedule_tree_update(self, item_id, col, value, force=False):
        self._schedule_ui_call(
            lambda _item_id=item_id, _col=col, _value=value, _force=force: self.update_tree(_item_id, _col, _value, force=_force)
        )

    def _tree_has_focus(self):
        if self.tree is None:
            return False
        try:
            focus_widget = self.root.focus_get()
        except Exception:
            focus_widget = None
        return focus_widget is self.tree

    def _handle_select_all_shortcut(self, event=None):
        if not self._tree_has_focus():
            return
        return self.select_all_tasks(event)

    def _handle_delete_shortcut(self, event=None):
        if not self._tree_has_focus():
            return
        return self._handle_delete_key(event)

    def _handle_delete_key(self, _event=None):
        if not self._tree_has_focus():
            return
        self.delete_selected()
        return "break"

    def _handle_tree_select(self, _event=None):
        self._refresh_ui_summary()

    def _focus_tree_item(self, item_id, preserve_selection=False):
        if not self.tree or not item_id:
            return
        if not preserve_selection:
            self.tree.selection_set(item_id)
        self.tree.focus(item_id)
        self.tree.see(item_id)

    def _build_extra_task_data(self, source_page=None, fallback_urls=None, resolved_url=None, resolved_url_saved_at=None, page_refresh_candidates=None):
        data = {}
        if source_page:
            data["source_page"] = source_page
        if fallback_urls:
            data["fallback_urls"] = _dedupe_download_urls(fallback_urls)
        if resolved_url:
            data["resolved_url"] = _normalize_download_url(resolved_url)
        if resolved_url_saved_at is not None:
            try:
                data["resolved_url_saved_at"] = float(resolved_url_saved_at or 0)
            except (TypeError, ValueError):
                data["resolved_url_saved_at"] = 0.0
        if page_refresh_candidates:
            data["page_refresh_candidates"] = _dedupe_download_urls(page_refresh_candidates)
        return data

    def _get_task_source_page(self, task, fallback_url=""):
        source_page = _normalize_download_url(_task_field_value(task, "source_page", ""))
        if source_page:
            return source_page
        return _normalize_download_url(fallback_url)

    def _get_existing_file_size(self, path):
        try:
            if path and os.path.exists(path):
                return os.path.getsize(path)
        except OSError:
            return 0
        return 0

    def _has_nonempty_file(self, path, min_bytes=1):
        return self._get_existing_file_size(path) >= max(int(min_bytes or 0), 1)

    def _update_task_state_entry(self, task, **fields):
        url = _normalize_download_url(_task_field_value(task, "url", "")) or ""
        if not url:
            return
        updates = {}
        if "name" in fields and fields["name"]:
            updates["name"] = str(fields["name"]).strip()
        if "source_site" in fields and fields["source_site"]:
            updates["source_site"] = _task_source_site_name({"source_site": fields["source_site"]})
        if "source_page" in fields and fields["source_page"] is not None:
            updates["source_page"] = _normalize_download_url(fields["source_page"])
        if "fallback_urls" in fields and fields["fallback_urls"] is not None:
            updates["fallback_urls"] = _dedupe_download_urls(fields["fallback_urls"], primary_url=url)
        if "resolved_url" in fields and fields["resolved_url"] is not None:
            updates["resolved_url"] = _normalize_download_url(fields["resolved_url"])
        if "resolved_url_saved_at" in fields and fields["resolved_url_saved_at"] is not None:
            try:
                updates["resolved_url_saved_at"] = float(fields["resolved_url_saved_at"] or 0)
            except (TypeError, ValueError):
                updates["resolved_url_saved_at"] = 0.0
        if "page_refresh_candidates" in fields and fields["page_refresh_candidates"] is not None:
            updates["page_refresh_candidates"] = _dedupe_download_urls(fields["page_refresh_candidates"])
        if updates:
            update_state_entry(url, **updates)

    def _set_cached_resolved_link_state(self, task, resolved_url=None, resolved_url_saved_at=None, page_refresh_candidates=None, clear_source_refresh_history=False):
        if not task:
            return
        normalized_page_refresh_candidates = None
        aux_updates = {}
        state_updates = {}
        if resolved_url is not None:
            normalized_resolved_url = _normalize_download_url(resolved_url)
            aux_updates["resolved_url"] = normalized_resolved_url
            state_updates["resolved_url"] = normalized_resolved_url
        if resolved_url_saved_at is not None:
            try:
                normalized_saved_at = float(resolved_url_saved_at or 0)
            except (TypeError, ValueError):
                normalized_saved_at = 0.0
            aux_updates["resolved_url_saved_at"] = normalized_saved_at
            state_updates["resolved_url_saved_at"] = normalized_saved_at
        if page_refresh_candidates is not None:
            normalized_page_refresh_candidates = _dedupe_download_urls(page_refresh_candidates)
            aux_updates["page_refresh_candidates"] = normalized_page_refresh_candidates
            aux_updates["_gimy_page_refresh_candidates"] = normalized_page_refresh_candidates
            state_updates["page_refresh_candidates"] = normalized_page_refresh_candidates
        if clear_source_refresh_history:
            aux_updates["_gimy_source_refresh_history"] = []
        if aux_updates:
            _set_task_aux_fields(task, **aux_updates)
        if state_updates:
            self._update_task_state_entry(task, **state_updates)

    def _cache_task_resolved_link(self, task, resolved_url, fallback_urls=None, page_refresh_candidates=None):
        if not task:
            return
        updates = {}
        normalized_resolved_url = _normalize_download_url(resolved_url)
        if normalized_resolved_url:
            resolved_url_saved_at = time.time()
            self._set_cached_resolved_link_state(
                task,
                resolved_url=normalized_resolved_url,
                resolved_url_saved_at=resolved_url_saved_at,
                page_refresh_candidates=page_refresh_candidates,
            )
            updates["resolved_url"] = normalized_resolved_url
            updates["resolved_url_saved_at"] = resolved_url_saved_at
        elif page_refresh_candidates is not None:
            self._set_cached_resolved_link_state(
                task,
                page_refresh_candidates=page_refresh_candidates,
            )
            updates["page_refresh_candidates"] = _dedupe_download_urls(page_refresh_candidates)
        if fallback_urls is not None:
            updates["fallback_urls"] = _dedupe_download_urls(fallback_urls, primary_url=_normalize_download_url(_task_field_value(task, "url", "")) or "")
        if updates:
            self._update_task_state_entry(task, **updates)

    def _log_m3u8_route_selected(self, task, item_id, media_url, source_site=None, fallback_urls=None):
        site = _task_source_site_name(task, fallback_site=source_site or "")
        effective_fallback_urls = fallback_urls if fallback_urls is not None else _dedupe_download_urls(_task_field_value(task, "fallback_urls", []))
        self._cache_task_resolved_link(
            task,
            media_url,
            fallback_urls=effective_fallback_urls,
            page_refresh_candidates=_task_gimy_page_refresh_candidates(task) if site == "gimy" else None,
        )
        fallback_count = len(
                    _dedupe_download_urls(
                effective_fallback_urls,
                primary_url=media_url,
            )
        )
        write_error_log(
            "m3u8 route selected",
            Exception(f"route=ffmpeg site={site or 'unknown'}"),
            url=media_url,
            item_id=item_id,
            source_site=site or None,
            fallback_count=fallback_count,
        )

    def _build_ffmpeg_runtime_fields(self, ffmpeg_path, retry_count=0, cmd=None, ffmpeg_version=None, **extra):
        fields = {
            "ffmpeg_path": ffmpeg_path,
            "ffmpeg_version": ffmpeg_version or None,
            "retry_count": retry_count,
        }
        if cmd is not None:
            preview_parts = []
            skip_next_header_value = False
            for part in cmd:
                if skip_next_header_value:
                    preview_parts.append("<headers>")
                    skip_next_header_value = False
                    continue
                text = str(part)
                if text == "-headers":
                    preview_parts.append(text)
                    skip_next_header_value = True
                    continue
                preview_parts.append(text)
            fields["cmd_preview"] = " ".join(preview_parts)[:400]
        fields.update(extra)
        return fields

    def _log_ffmpeg_event(self, title, exc, task, item_id, media_url, **extra):
        write_error_log(
            title,
            exc,
            url=media_url,
            item_id=item_id,
            source_site=_task_source_site_name(task) or None,
            fallback_count=len(_dedupe_download_urls(_task_field_value(task, "fallback_urls", []))),
            **extra,
        )

    def _invalidate_m3u8_total_bytes(self, item_id, media_url, total_bytes_box, reason, estimated_total_bytes, current_bytes):
        total_bytes_box["value"] = None
        write_error_log(
            "m3u8 total bytes invalidated",
            Exception(str(reason)),
            url=media_url,
            item_id=item_id,
            estimated_total_bytes=estimated_total_bytes,
            current_bytes=current_bytes,
        )

    def _should_invalidate_m3u8_total_bytes(self, current_bytes, estimated_total_bytes):
        return bool(estimated_total_bytes and estimated_total_bytes > 0 and current_bytes > estimated_total_bytes * 1.01)

    def _should_invalidate_stalled_m3u8_total(self, percent, progress_state, near_complete_since, now):
        if percent is None or progress_state == "end":
            return False, None
        if percent < 99.0:
            return False, None
        if near_complete_since is None:
            return False, now
        if now - near_complete_since >= 5.0:
            return True, near_complete_since
        return False, near_complete_since

    def _terminate_ffmpeg_process(self, task, item_id, proc, media_url, reason, **extra):
        self._log_ffmpeg_event(
            "ffmpeg terminate requested",
            Exception(str(reason)),
            task,
            item_id,
            media_url,
            reason=reason,
            state=str(_task_field_value(task, "state", "") or ""),
            stop_reason=_task_field_value(task, "_stop_reason", None),
            **extra,
        )
        try:
            proc.terminate()
        except Exception:
            pass

    def _is_stop_requested_state(self, state):
        return state in STOP_REQUEST_TASK_STATES

    def _is_delete_requested_state(self, state):
        return state == "DELETE_REQUESTED"

    def _is_pause_requested_state(self, state):
        return state == "PAUSE_REQUESTED"

    def _is_queued_state(self, state):
        return state == "QUEUED"

    def _is_downloading_state(self, state):
        return state == "DOWNLOADING"

    def _is_deleted_state(self, state):
        return state == "DELETED"

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
        for task in self._iter_live_tasks():
            url = _normalize_download_url(_task_field_value(task, "url", "")) or ""
            if not url:
                continue
            is_mp3 = bool(_task_field_value(task, "is_mp3", False))
            name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
            if not name:
                fallback_display_url = (_normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url))
                if fallback_display_url:
                    name = default_short_name_for_url(
                        fallback_display_url,
                        is_mp3=bool(_task_field_value(task, "is_mp3", is_mp3)),
                    )
            source_site = _task_source_site_name(task)
            fallback_urls = tuple(_dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url))
            source_page = self._get_task_source_page(task)
            entries_append(
                {
                    "url": url,
                    "name": name,
                    "is_mp3": is_mp3,
                    "resume_requested": bool(_task_field_value(task, "resume_requested", False)),
                    "source_site": source_site or None,
                    "fallback_urls": list(fallback_urls),
                    "source_page": source_page,
                    "resolved_url": (_normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""),
                    "resolved_url_saved_at": float(_task_field_value(task, "resolved_url_saved_at", 0.0) or 0.0),
                    "page_refresh_candidates": list(_task_gimy_page_refresh_candidates(task)),
                    "filename": str(_task_field_value(task, "filename", "") or "").strip(),
                    "temp_filename": str(_task_field_value(task, "temp_filename", "") or "").strip(),
                }
            )
            signature_append((url, name, is_mp3, bool(_task_field_value(task, "resume_requested", False)), source_site, fallback_urls, source_page, (_normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""), float(_task_field_value(task, "resolved_url_saved_at", 0.0) or 0.0), tuple(_task_gimy_page_refresh_candidates(task)), str(_task_field_value(task, "filename", "") or "").strip(), str(_task_field_value(task, "temp_filename", "") or "").strip()))
        return entries, tuple(signature_parts)

    def _mark_existing_file_complete(self, item_id, message):
        task = self.tasks.get(item_id)
        if task:
            primary_value = str(_task_field_value(task, "filename") or "").strip()
            secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
            filename = primary_value or secondary_value or ""
        else:
            filename = ""
        if filename and os.path.exists(filename):
            try:
                self._set_task_named_column_text(item_id, "size", format_transfer_size(os.path.getsize(filename)))
            except OSError:
                pass
        self._set_task_status_mode_ui(item_id, t("status_done") if "status_done" in I18N_DICT.get(CURRENT_LANG, {}) else "完成", message, complete_progress=True)
        if task:
            _set_task_aux_fields(task, state="FINISHED", resume_requested=False)
            remove_from_state(_normalize_download_url(_task_field_value(task, "url", "")) or "")

    def _has_resume_artifact_state(self, task, save_dir=None, is_mp3=None):
        if not task:
            return False
        effective_is_mp3 = bool(_task_field_value(task, "is_mp3", False)) if is_mp3 is None else bool(is_mp3)
        effective_save_dir = save_dir or self.save_dir_var.get()
        ext = "mp3" if effective_is_mp3 else "mp4"
        source_url = (_normalize_download_url(_task_field_value(task, "resolved_url", "")) or "") or (_normalize_download_url(_task_field_value(task, "url", "")) or "") or self._get_task_source_page(task)
        resume_keys = []
        for candidate in (
            _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(source_url),
            self._get_task_source_page(task, fallback_url=source_url),
            source_url,
        ):
            normalized_candidate = _normalize_download_url(candidate)
            if normalized_candidate and normalized_candidate not in resume_keys:
                resume_keys.append(normalized_candidate)
        output_identity_key = self._build_resume_output_identity_key(
            task,
            ext=ext,
            save_dir=effective_save_dir,
            fallback_name="Audio" if effective_is_mp3 else "Video",
        )
        if output_identity_key and output_identity_key not in resume_keys:
            resume_keys.insert(0, output_identity_key)
        explicit_output_path = str(_task_field_value(task, "filename", "") or "").strip()
        explicit_temp_path = str(_task_field_value(task, "temp_filename", "") or "").strip()
        if explicit_temp_path:
            temp_root, temp_ext = os.path.splitext(explicit_temp_path)
            candidate_paths = [explicit_temp_path, f"{explicit_temp_path}.resume", f"{explicit_temp_path}.merged"]
            if temp_ext:
                candidate_paths.extend([f"{temp_root}.resume{temp_ext}", f"{temp_root}.merged{temp_ext}"])
            if any(self._has_nonempty_file(candidate_path) for candidate_path in candidate_paths) and self._resume_artifact_matches_keys(explicit_temp_path, resume_keys):
                return True
        if explicit_output_path:
            path = explicit_output_path
            root, ext = os.path.splitext(path)
            if (
                _task_source_site_name(task) in ("gimy", "movieffm", "missav", "njavtv", "xiaoyakankan", "99itv")
                and self._has_nonempty_file(path)
                and str(_task_field_value(task, "state", "") or "") != "FINISHED"
            ):
                return True
            candidate_paths = [f"{path}.resume", f"{path}.merged"]
            if ext:
                candidate_paths.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
            if any(self._has_nonempty_file(candidate_path) for candidate_path in candidate_paths) and self._resume_artifact_matches_keys(path, resume_keys):
                return True
        if not source_url or not effective_save_dir:
            return False
        try:
            temp_out_path, _, _ = self._resolve_resume_artifact_base(
                task,
                source_url,
                ext=ext,
                save_dir=effective_save_dir,
                fallback_name="Audio" if effective_is_mp3 else "Video",
            )
        except Exception:
            return False
        temp_root, temp_ext = os.path.splitext(temp_out_path)
        candidate_paths = (
            temp_out_path,
            f"{temp_root}.resume{temp_ext}",
            f"{temp_root}.merged{temp_ext}",
        )
        if any(self._has_nonempty_file(candidate_path) for candidate_path in candidate_paths) and self._resume_artifact_matches_keys(temp_out_path, resume_keys):
            return True
        return False

    def _is_resume_download_active(self, task, save_dir=None, is_mp3=False, include_cached_resolved=False):
        if not task:
            return False
        if bool(_task_field_value(task, "resume_requested", False)):
            return True
        if include_cached_resolved and (_normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""):
            return True
        return self._has_resume_artifact_state(task, save_dir=save_dir, is_mp3=is_mp3)

    def _select_manifest_download_route(self, target_url, task, save_dir, is_mp3=False, default_route="generic", force_ffmpeg=False):
        if force_ffmpeg:
            return "ffmpeg"
        resume_active = self._is_resume_download_active(task, save_dir=save_dir, is_mp3=is_mp3)
        if resume_active:
            parsed = urllib.parse.urlparse(str(target_url or ""))
            host = str(parsed.netloc or "").strip().lower()
            source_site = _task_source_site_name(task)
            if not (source_site == "18av" and "streamfastpro" in host):
                return "ffmpeg"
        if _should_prefer_native_hls(target_url, task):
            return "native"
        if resume_active:
            return "generic"
        if default_route == "ffmpeg":
            return "ffmpeg"
        return "generic"

    def _can_accept_existing_output(self, task, item_id, output_path, temp=False):
        if not self._has_nonempty_file(output_path):
            return False
        if temp:
            return True
        if self._is_incomplete_hls_video_artifact(task, output_path):
            write_error_log(
                "existing output rejected",
                Exception("existing output appears incomplete"),
                item_id=item_id,
                output=output_path,
                source_site=_task_source_site_name(task) or None,
            )
            return False
        return True

    def _is_incomplete_hls_video_artifact(self, task, output_path, expected_duration=None):
        if _task_source_site_name(task) not in ("gimy", "movieffm", "missav", "njavtv", "xiaoyakankan", "99itv"):
            return False
        if not output_path or str(output_path).lower().endswith((".mp3", ".m4a")):
            return False
        info = self._probe_media_info(output_path)
        if not info.get("exists") or not info.get("size"):
            return True
        if not info.get("valid"):
            return True
        duration = float(info.get("duration", 0.0) or 0.0)
        size = int(info.get("size", 0) or 0)
        if size < 1024 * 1024 and duration < 60.0:
            return True
        try:
            expected_total_bytes = max(int(task.get("total_bytes", 0) or 0), 0)
        except Exception:
            expected_total_bytes = 0
        if expected_total_bytes > 20 * 1024 * 1024 and size > 0 and size < expected_total_bytes * 0.8:
            return True
        expected_seconds = max(float(expected_duration or 0.0), 0.0)
        try:
            ext = os.path.splitext(output_path)[1].lstrip(".") or "mp4"
            resume_keys = []
            for candidate in (
                _normalize_download_url(_task_field_value(task, "url", "")) or "",
                self._get_task_source_page(task, fallback_url=_normalize_download_url(_task_field_value(task, "url", "")) or ""),
                _normalize_download_url(_task_field_value(task, "url", "")) or "",
            ):
                normalized_candidate = _normalize_download_url(candidate)
                if normalized_candidate and normalized_candidate not in resume_keys:
                    resume_keys.append(normalized_candidate)
            resume_key = resume_keys[0] if resume_keys else self._get_task_resolved_url(task, fallback_url=_normalize_download_url(_task_field_value(task, "url", "")) or "")
            temp_root = self._get_stable_resume_base(_normalize_download_url(_task_field_value(task, "url", "")) or "", ext=ext, resume_key=resume_key)
            progress_info = self._load_resume_progress_info(f"{temp_root}.part.progress.json")
            if self._resume_progress_matches(progress_info, resume_keys, f"{temp_root}.part.progress.json"):
                expected_seconds = max(expected_seconds, float(progress_info.get("seconds", 0.0) or 0.0))
        except Exception:
            pass
        if expected_seconds > 300.0 and duration > 0.0 and duration + max(60.0, expected_seconds * 0.2) < expected_seconds:
            return True
        if expected_seconds > 300.0 and duration <= 0.0:
            return True
        return False

    def _count_live_tasks(self):
        count = 0
        for _ in self._iter_live_tasks():
            count += 1
        return count

    def _complete_if_output_exists(self, item_id):
        task = self.tasks.get(item_id, {})
        if self._has_resume_artifact_state(task):
            return False
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        explicit_output = primary_value or secondary_value or ""
        if self._can_accept_existing_output(task, item_id, explicit_output):
            self._set_task_output_file(task, item_id, explicit_output)
            self._mark_existing_file_complete(item_id, self._ui_text("msg_file_exists", "檔案已存在"))
            return True
        short_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
        if not short_name or short_name == "Queued":
            return False
        save_dir = self.save_dir_var.get()
        possible_exts = [".mp4", ".mkv", ".webm", ".mp3", ".m4a"]
        message = self._ui_text("msg_file_exists", "檔案已存在")
        safe_name = _safe_output_stem(short_name, fallback="download")
        for ext in possible_exts:
            candidate_output = os.path.join(save_dir, f"{safe_name}{ext}")
            if self._can_accept_existing_output(task, item_id, candidate_output):
                self._set_task_output_file(task, item_id, candidate_output)
                self._mark_existing_file_complete(item_id, message)
                return True
        return False

    def _set_task_output_file(self, task, item_id, output_path):
        _set_task_aux_fields(task, filename=output_path)
        self._set_task_named_column_text(item_id, "name", os.path.basename(str(output_path or "").strip()) or "")

    def _task_output_path_or_default(self, task, default_path=""):
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        fallback_value = str(default_path or "").strip()
        return primary_value or secondary_value or fallback_value

    def _log_ytdlp_native_hls_finished(self, task, item_id, url, output_path=None):
        final_output_path = self._task_output_path_or_default(task, output_path)
        write_error_log(
            "yt-dlp native hls fallback finished",
            Exception("yt-dlp native hls fallback finished"),
            url=url,
            item_id=item_id,
            source_site=_task_source_site_name(task) or None,
            output=final_output_path,
            bytes=self._get_existing_file_size(final_output_path),
        )

    def _set_output_path_and_complete_if_exists(self, task, item_id, output_path, message=None, temp=False):
        if temp:
            _set_task_aux_fields(task, temp_filename=output_path)
        else:
            self._set_task_output_file(task, item_id, output_path)
        if self._can_accept_existing_output(task, item_id, output_path, temp=temp):
            self._mark_existing_file_complete(item_id, message or self._ui_text("eta_file_exists", "檔案已存在"))
            return True
        return False

    def _find_output_file_candidate(self, save_dir, safe_name, preferred_ext=None):
        if not save_dir or not safe_name:
            return ""
        possible_exts = []
        if preferred_ext:
            possible_exts.append(preferred_ext if str(preferred_ext).startswith(".") else f".{preferred_ext}")
        possible_exts.extend([".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".m4v", ".ts"])
        seen = set()
        for ext in possible_exts:
            if ext in seen:
                continue
            seen.add(ext)
            candidate = os.path.join(save_dir, f"{safe_name}{ext}")
            if self._has_nonempty_file(candidate):
                return candidate
        pattern = os.path.join(save_dir, f"{glob.escape(safe_name)}*")
        matches = []
        for path in glob.glob(pattern):
            base_name = os.path.basename(path)
            if base_name.endswith((".part", ".ytdl")):
                continue
            if self._has_nonempty_file(path):
                matches.append(path)
        if not matches:
            return ""
        matches.sort(key=lambda path: (self._get_existing_file_size(path), os.path.getmtime(path)), reverse=True)
        return matches[0]

    def _collect_yt_dlp_output_candidates(self, info, save_dir, safe_name, preferred_ext=None):
        candidates = []
        seen = set()

        def add_candidate(path):
            clean_path = str(path or "").strip()
            if not clean_path:
                return
            if not os.path.isabs(clean_path) and save_dir:
                clean_path = os.path.join(save_dir, clean_path)
            norm_path = os.path.normcase(os.path.abspath(clean_path))
            if norm_path in seen:
                return
            seen.add(norm_path)
            candidates.append(clean_path)

        if isinstance(info, dict):
            add_candidate(info.get("filepath"))
            add_candidate(info.get("_filename"))
            add_candidate(info.get("filename"))
            try:
                add_candidate(get_yt_dlp_module().YoutubeDL({}).prepare_filename(info))
            except Exception:
                pass
            for requested in info.get("requested_downloads") or []:
                if not isinstance(requested, dict):
                    continue
                add_candidate(requested.get("filepath"))
                add_candidate(requested.get("_filename"))
                add_candidate(requested.get("filename"))
        fallback_candidate = self._find_output_file_candidate(save_dir, safe_name, preferred_ext=preferred_ext)
        add_candidate(fallback_candidate)
        return candidates

    def _resolve_yt_dlp_output_path(self, info, save_dir, safe_name, out_path, preferred_ext=None):
        for candidate in self._collect_yt_dlp_output_candidates(info, save_dir, safe_name, preferred_ext=preferred_ext):
            if self._has_nonempty_file(candidate):
                return candidate
        if self._has_nonempty_file(out_path):
            return out_path
        return ""

    def _wait_for_yt_dlp_output_path(self, info, save_dir, safe_name, out_path, preferred_ext=None, timeout_seconds=12.0):
        deadline = time.time() + max(float(timeout_seconds or 0.0), 0.0)
        while True:
            resolved = self._resolve_yt_dlp_output_path(info, save_dir, safe_name, out_path, preferred_ext=preferred_ext)
            if resolved:
                return resolved
            if time.time() >= deadline:
                return ""
            time.sleep(0.5)

    def _extract_domain(self, url):
        domain = urllib.parse.urlparse(url).netloc
        if not domain:
            domain = url.split("/")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        return domain

    def _should_use_impersonation(self, url, source_site=None):
        site = str(source_site or "").strip().lower()
        if site in IMPERSONATION_SITE_MARKERS:
            return True
        domain = self._extract_domain(url)
        return any(marker in domain for marker in IMPERSONATION_SITE_MARKERS)

    def _request_task_deletion(self, item_id, state):
        _set_task_aux_fields(self.tasks[item_id], state="DELETE_REQUESTED", _stop_reason=STOP_REASON_DELETE)
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

    def _get_m3u8_total_bytes(self, url, headers=None, task=None):
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
            if self._shutdown_started:
                return None
            try:
                head_resp = c_req.head(
                    segment_url,
                    impersonate="chrome110",
                    timeout=15,
                    headers=_make_range_http_headers(headers, range_value=None),
                )
                if head_resp.status_code < 400:
                    content_length = _response_header_value(head_resp.headers, "Content-Length")
                    if content_length:
                        return max(int(content_length), 0)
            except Exception:
                pass
            try:
                get_resp = c_req.get(
                    segment_url,
                    impersonate="chrome110",
                    timeout=20,
                    headers=_make_range_http_headers(headers),
                )
                if get_resp.status_code < 400:
                    content_range = _response_header_value(get_resp.headers, "Content-Range")
                    if content_range and "/" in content_range:
                        return max(int(content_range.split("/")[-1]), 0)
                    content_length = _response_header_value(get_resp.headers, "Content-Length")
                    if content_length:
                        return max(int(content_length), 0)
                return None
            except Exception:
                return None

        def _sum_media_playlist_bytes(playlist_url, text):
            if self._shutdown_started:
                return None
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
            executor = None
            try:
                executor = DaemonThreadPoolExecutor(
                    max_workers=int(
                        M3U8_TOTAL_BYTES_PROBE_WORKERS_BY_SITE.get(
                            _task_source_site_name(task),
                            M3U8_TOTAL_BYTES_PROBE_WORKERS,
                        )
                    )
                )
                futures = [executor.submit(_probe_segment_bytes, segment_url) for segment_url in probe_urls]
                for future in concurrent.futures.as_completed(futures, timeout=120):
                    if self._shutdown_started:
                        for pending_future in futures:
                            pending_future.cancel()
                        return None
                    segment_bytes = future.result()
                    if segment_bytes is None:
                        return None
                    total_bytes += segment_bytes
            except Exception:
                return None
            finally:
                if executor is not None:
                    try:
                        executor.shutdown(wait=not self._shutdown_started, cancel_futures=True)
                    except Exception:
                        pass
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

    def _build_resume_output_identity_key(self, task, ext="mp4", save_dir=None, fallback_name="Video"):
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        preferred_output = primary_value or ""
        if preferred_output:
            normalized_output = os.path.normcase(os.path.abspath(preferred_output))
            return f"output::{normalized_output}"
        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or fallback_name or "").strip()
        safe_name = _safe_output_stem(name, fallback=fallback_name)
        if not safe_name:
            return ""
        output_dir = save_dir or self.save_dir_var.get() or _APP_DIR
        try:
            output_dir = os.path.abspath(os.path.expanduser(str(output_dir or _APP_DIR)))
        except Exception:
            output_dir = str(output_dir or _APP_DIR)
        normalized_ext = str(ext or "mp4").lstrip(".") or "mp4"
        candidate_output = os.path.normcase(os.path.join(output_dir, f"{safe_name}.{normalized_ext}"))
        source_key = (
            _normalize_download_url(_task_field_value(task, "source_page", ""))
            or _normalize_download_url(_task_field_value(task, "url", ""))
            or ""
        )
        source_site = (_task_source_site_name(task) or "").strip().lower()
        if source_site:
            return f"{source_site}::{candidate_output}::{source_key}"
        return f"output::{candidate_output}"

    def _normalize_resume_state_id(self, progress_path):
        if not progress_path:
            return ""
        try:
            return os.path.normcase(os.path.abspath(str(progress_path)))
        except Exception:
            return str(progress_path or "")

    def _has_resume_artifact_family(self, base_path):
        if not base_path:
            return False
        root, ext = os.path.splitext(base_path)
        candidate_paths = [
            base_path,
            base_path + ".progress.json",
        ]
        if ext:
            candidate_paths.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
        return any(self._has_nonempty_file(candidate_path) or os.path.exists(candidate_path) for candidate_path in candidate_paths)

    def _find_resume_artifact_base_by_source_keys(self, resume_keys, ext="mp4"):
        normalized_keys = set(self._normalize_resume_match_keys(resume_keys))
        if not normalized_keys:
            return ""
        normalized_ext = str(ext or "mp4").lstrip(".") or "mp4"
        progress_pattern = os.path.join(tempfile.gettempdir(), f"downloader_resume_*.{normalized_ext}.progress.json")
        for progress_path in glob.glob(progress_pattern):
            try:
                progress_info = self._load_resume_progress_info(progress_path)
                if not self._resume_progress_matches(progress_info, list(normalized_keys), progress_path):
                    continue
                base_path = progress_path[: -len(".progress.json")]
                if self._has_resume_artifact_family(base_path):
                    return base_path
            except Exception:
                continue
        return ""

    def _resume_artifact_matches_keys(self, base_path, resume_keys):
        if not base_path:
            return False
        root, ext = os.path.splitext(base_path)
        media_candidates = [base_path]
        if ext:
            media_candidates.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
        if not any(self._has_nonempty_file(candidate_path) for candidate_path in media_candidates):
            return False
        progress_path = str(base_path or "") + ".progress.json"
        if not os.path.exists(progress_path):
            return not os.path.basename(str(base_path or "")).lower().startswith("downloader_resume_")
        progress_info = self._load_resume_progress_info(progress_path)
        has_identity = bool(progress_info.get("source_url") or progress_info.get("resume_id"))
        if os.path.basename(str(base_path or "")).lower().startswith("downloader_resume_") and not has_identity:
            return False
        return (not has_identity) or self._resume_progress_matches(progress_info, resume_keys, progress_path)

    def _resolve_resume_artifact_base(self, task, url, ext="mp4", save_dir=None, fallback_name="Video"):
        resume_keys = []
        for candidate in (
            _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url),
            self._get_task_source_page(task, fallback_url=url),
            url,
        ):
            normalized_candidate = _normalize_download_url(candidate)
            if normalized_candidate and normalized_candidate not in resume_keys:
                resume_keys.append(normalized_candidate)
        output_identity_key = self._build_resume_output_identity_key(task, ext=ext, save_dir=save_dir, fallback_name=fallback_name)
        if output_identity_key and output_identity_key not in resume_keys:
            resume_keys.insert(0, output_identity_key)
        primary_key = resume_keys[0] if resume_keys else (_normalize_download_url(url) or str(url or ""))
        explicit_temp_path = str(_task_field_value(task, "temp_filename", "") or "").strip()
        if explicit_temp_path and self._resume_artifact_matches_keys(explicit_temp_path, resume_keys):
            return explicit_temp_path, primary_key, resume_keys
        explicit_output_path = str(_task_field_value(task, "filename", "") or "").strip()
        if explicit_output_path and self._resume_artifact_matches_keys(explicit_output_path, resume_keys):
            return explicit_output_path, primary_key, resume_keys
        primary_base = self._get_stable_resume_base(url, ext=ext, resume_key=primary_key)
        for legacy_key in resume_keys[1:]:
            legacy_base = self._get_stable_resume_base(url, ext=ext, resume_key=legacy_key)
            if legacy_base != primary_base and self._resume_artifact_matches_keys(legacy_base, resume_keys):
                return legacy_base, primary_key, resume_keys
        discovered_base = self._find_resume_artifact_base_by_source_keys(resume_keys, ext=ext)
        if discovered_base and discovered_base != primary_base:
            return discovered_base, primary_key, resume_keys
        return primary_base, primary_key, resume_keys

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
                    "format=format_name,duration,size:stream=index,codec_type,codec_name,profile,pix_fmt,level,codec_tag_string",
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
                "format_name": str(format_info.get("format_name") or ""),
                "streams": streams,
                "has_streams": has_streams,
                "valid": valid,
                "reason": "ok" if valid else "no-streams",
            }
        except Exception:
            return {
                "exists": True,
                "size": size,
                "duration": 0.0,
                "format_name": "",
                "streams": [],
                "has_streams": False,
                "valid": False,
                "reason": "ffprobe-error",
            }

    def _is_windows_friendly_mp4_info(self, info):
        if not info or not info.get("valid"):
            return False
        streams = info.get("streams") or []
        video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
        audio_streams = [stream for stream in streams if stream.get("codec_type") == "audio"]
        if not video_streams:
            return False
        video = video_streams[0]
        video_codec = str(video.get("codec_name") or "").lower()
        pix_fmt = str(video.get("pix_fmt") or "").lower()
        video_tag = str(video.get("codec_tag_string") or "").lower()
        if video_codec != "h264" or (pix_fmt and pix_fmt != "yuv420p"):
            return False
        if video_tag and video_tag not in ("avc1", "avc3"):
            return False
        for audio in audio_streams[:1]:
            audio_codec = str(audio.get("codec_name") or "").lower()
            if audio_codec not in ("aac", "mp3"):
                return False
        return True

    def _run_mp4_compat_ffmpeg(self, cmd, timeout_seconds=1800):
        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            startupinfo=startupinfo,
            creationflags=creationflags,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise Exception(message[:400] or f"ffmpeg exited with code {result.returncode}")

    def _replace_file_with_retry(self, src_path, dst_path, attempts=24, delay_seconds=0.25):
        last_exc = None
        for _attempt in range(max(int(attempts or 1), 1)):
            try:
                os.replace(src_path, dst_path)
                return
            except OSError as exc:
                last_exc = exc
                time.sleep(max(float(delay_seconds or 0.0), 0.05))
        raise last_exc or OSError("replace failed")

    def _ensure_windows_compatible_mp4(self, item_id, path):
        if not path or not str(path).lower().endswith(".mp4") or not os.path.exists(path):
            return
        ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe") if platform.system() == "Windows" else shutil.which("ffmpeg") or "ffmpeg"
        if not os.path.exists(ffmpeg_path) and ffmpeg_path == os.path.join(_APP_DIR, "ffmpeg.exe"):
            return
        task = self.tasks.get(item_id, {})
        info = self._probe_media_info(path)
        if not info.get("valid"):
            raise Exception("MP4 output is not a valid media file")
        root, ext = os.path.splitext(path)
        compat_path = f"{root}.windows_compat.tmp{ext or '.mp4'}"
        transcode_path = f"{root}.windows_transcode.tmp{ext or '.mp4'}"
        for stale_path in (compat_path, transcode_path):
            try:
                if os.path.exists(stale_path):
                    os.remove(stale_path)
            except OSError:
                pass
        copy_cmd = [
            ffmpeg_path,
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path,
            "-map",
            "0:v:0?",
            "-map",
            "0:a:0?",
            "-dn",
            "-sn",
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-movflags",
            "+faststart",
            "-brand",
            "mp42",
            "-tag:v",
            "avc1",
            compat_path,
        ]
        try:
            self._run_mp4_compat_ffmpeg(copy_cmd)
            compat_info = self._probe_media_info(compat_path)
            original_duration = float(info.get("duration", 0.0) or 0.0)
            compat_duration = float(compat_info.get("duration", 0.0) or 0.0)
            if not compat_info.get("valid"):
                raise Exception("compat remux produced invalid media")
            if original_duration > 300.0 and compat_duration + max(30.0, original_duration * 0.02) < original_duration:
                raise Exception(f"compat remux duration mismatch: duration={compat_duration:.3f} expected={original_duration:.3f}")
            if self._is_windows_friendly_mp4_info(compat_info):
                self._replace_file_with_retry(compat_path, path)
                write_error_log(
                    "windows compatible mp4 remuxed",
                    Exception("windows compatible mp4 remuxed"),
                    item_id=item_id,
                    output=path,
                    source_site=_task_source_site_name(task) or None,
                    video_codec=(compat_info.get("streams") or [{}])[0].get("codec_name") if compat_info.get("streams") else None,
                    bytes=self._get_existing_file_size(path),
                )
                return
        except Exception as exc:
            write_error_log(
                "windows compatible mp4 remux failed",
                exc,
                item_id=item_id,
                output=path,
                source_site=_task_source_site_name(task) or None,
            )
            try:
                if os.path.exists(compat_path):
                    os.remove(compat_path)
            except OSError:
                pass
        transcode_cmd = [
            ffmpeg_path,
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            path,
            "-map",
            "0:v:0?",
            "-map",
            "0:a:0?",
            "-dn",
            "-sn",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "high",
            "-level",
            "4.1",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-movflags",
            "+faststart",
            "-brand",
            "mp42",
            transcode_path,
        ]
        self._run_mp4_compat_ffmpeg(transcode_cmd, timeout_seconds=7200)
        transcode_info = self._probe_media_info(transcode_path)
        if not self._is_windows_friendly_mp4_info(transcode_info):
            raise Exception("windows compatible transcode failed validation")
        self._replace_file_with_retry(transcode_path, path)
        write_error_log(
            "windows compatible mp4 transcoded",
            Exception("windows compatible mp4 transcoded"),
            item_id=item_id,
            output=path,
            source_site=_task_source_site_name(task) or None,
            bytes=self._get_existing_file_size(path),
        )

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

    def _resume_artifact_lock_for(self, *paths):
        key_source = next((str(path or "") for path in paths if path), "")
        key = os.path.abspath(key_source) if key_source else "__resume_artifact__"
        try:
            root, ext = os.path.splitext(key)
            if root.endswith(".resume"):
                key = root[:-7] + ext
        except Exception:
            pass
        with self._resume_artifact_locks_guard:
            lock = self._resume_artifact_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._resume_artifact_locks[key] = lock
            return lock

    def _promote_resume_artifact(self, src_path, dst_path):
        with self._resume_artifact_lock_for(src_path, dst_path):
            return self._promote_resume_artifact_locked(src_path, dst_path)

    def _promote_resume_artifact_locked(self, src_path, dst_path):
        temp_exists = self._has_nonempty_file(src_path)
        resume_exists = self._has_nonempty_file(dst_path)
        if not resume_exists:
            return False
        merged_path = dst_path + ".merged.mp4"
        src_info = self._probe_media_info(src_path) if temp_exists else {"exists": False, "valid": False, "size": 0, "duration": 0.0}
        dst_info = self._probe_media_info(dst_path)
        try:
            if temp_exists and src_info.get("valid") and dst_info.get("valid"):
                self._concat_media_files((src_path, dst_path), merged_path)
                self._move_file_with_retry(merged_path, src_path)
            elif temp_exists and not src_info.get("valid") and dst_info.get("valid"):
                try:
                    os.remove(src_path)
                except OSError:
                    pass
                self._move_file_with_retry(dst_path, src_path)
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
                    return self._has_nonempty_file(src_path)
                try:
                    os.remove(src_path)
                except OSError:
                    pass
                self._move_file_with_retry(dst_path, src_path)
            else:
                self._move_file_with_retry(dst_path, src_path)
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
                    self._move_file_with_retry(best_candidate, src_path)
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
            return self._has_nonempty_file(src_path)

    def _select_best_resume_artifact_state(self, candidate_paths, persisted=None):
        persisted = persisted or {}
        best_state = {
            "seconds": max(float(persisted.get("seconds", 0.0) or 0.0), 0.0),
            "bytes": max(int(persisted.get("bytes", 0) or 0), 0),
            "path": "",
            "valid": False,
        }
        scored_candidates = []
        for candidate_path in candidate_paths:
            if not self._has_nonempty_file(candidate_path):
                continue
            info = self._probe_media_info(candidate_path)
            candidate_size = max(int(info.get("size", 0) or 0), 0)
            candidate_seconds = max(float(info.get("duration", 0.0) or 0.0), 0.0)
            candidate_valid = bool(info.get("valid"))
            scored_candidates.append((
                candidate_valid,
                candidate_seconds,
                candidate_size,
                candidate_path,
            ))
        if not scored_candidates:
            return best_state
        scored_candidates.sort(reverse=True)
        best_valid, best_seconds, best_bytes, best_path = scored_candidates[0]
        best_state.update({
            "seconds": best_seconds,
            "bytes": best_bytes,
            "path": best_path,
            "valid": bool(best_valid),
        })
        return best_state

    def _move_file_with_retry(self, src_path, dst_path, attempts=24, delay_seconds=0.5):
        last_error = None
        total_attempts = max(int(attempts or 1), 1)
        for attempt in range(total_attempts):
            try:
                shutil.move(src_path, dst_path)
                return True
            except PermissionError as exc:
                last_error = exc
                if attempt >= total_attempts - 1:
                    break
                time.sleep(max(float(delay_seconds or 0.0), 0.0))
            except OSError as exc:
                if getattr(exc, "winerror", None) != 32:
                    raise
                last_error = exc
                if attempt >= total_attempts - 1:
                    break
                time.sleep(max(float(delay_seconds or 0.0), 0.0))
            except Exception:
                raise
        if last_error:
            raise last_error
        return False

    def _remove_artifact_paths(self, *paths):
        for artifact_path in paths:
            if not artifact_path:
                continue
            try:
                if os.path.exists(artifact_path):
                    os.remove(artifact_path)
            except OSError:
                continue

    def _remove_yt_dlp_fragment_artifacts(self, output_root):
        root = str(output_root or "").strip()
        if not root:
            return
        for pattern in (
            f"{root}.part*",
            f"{root}.*.part*",
            f"{root}.ytdl",
            f"{root}.ytdl.*",
            f"{root}.*.ytdl",
            f"{root}.f*.part*",
        ):
            for artifact_path in glob.glob(pattern):
                try:
                    if os.path.exists(artifact_path):
                        os.remove(artifact_path)
                except OSError:
                    continue

    def _reset_resume_artifacts(self, *paths):
        self._remove_artifact_paths(*paths)
        return 0, 0.0

    def _load_resume_progress_info(self, progress_path):
        if not progress_path or not os.path.exists(progress_path):
            return {"seconds": 0, "bytes": 0, "source_url": "", "resume_id": ""}
        try:
            data = _load_json_with_backup(progress_path, {})
            if not isinstance(data, dict):
                return {"seconds": 0, "bytes": 0, "source_url": "", "resume_id": ""}
            return {
                "seconds": max(float(data.get("seconds", 0) or 0), 0.0),
                "bytes": max(int(data.get("bytes", 0) or 0), 0),
                "source_url": str(data.get("source_url", "") or ""),
                "resume_id": str(data.get("resume_id", "") or ""),
            }
        except Exception:
            return {"seconds": 0, "bytes": 0, "source_url": "", "resume_id": ""}

    def _normalize_resume_match_keys(self, resume_keys):
        keys = []
        for candidate in resume_keys or []:
            raw_candidate = str(candidate or "").strip()
            normalized_candidate = _normalize_download_url(raw_candidate) or raw_candidate
            if normalized_candidate and normalized_candidate not in keys:
                keys.append(normalized_candidate)
        return keys

    def _resume_progress_matches(self, stored_info, resume_keys, progress_path):
        stored_info = stored_info or {}
        normalized_keys = set(self._normalize_resume_match_keys(resume_keys))
        stored_source_raw = str(stored_info.get("source_url", "") or "").strip()
        stored_source = _normalize_download_url(stored_source_raw) or stored_source_raw
        if stored_source and stored_source in normalized_keys:
            return True
        if stored_source:
            return False
        current_resume_id = self._normalize_resume_state_id(progress_path)
        stored_resume_id = str(stored_info.get("resume_id", "") or "")
        if current_resume_id and stored_resume_id and current_resume_id == stored_resume_id:
            return not any(
                str(key or "").startswith(("output::", "movieffm::", "gimy::", "missav::", "hohoj::"))
                for key in normalized_keys
            )
        return False

    def _load_resume_progress(self, progress_path):
        return self._load_resume_progress_info(progress_path).get("seconds", 0)

    def _should_prefer_lower_resume_progress(self, new_seconds, new_bytes, old_seconds, old_bytes):
        try:
            new_seconds_value = max(float(new_seconds or 0.0), 0.0)
            new_bytes_value = max(int(new_bytes or 0), 0)
            old_seconds_value = max(float(old_seconds or 0.0), 0.0)
            old_bytes_value = max(int(old_bytes or 0), 0)
        except (TypeError, ValueError):
            return False
        if new_seconds_value <= 0.0 or old_seconds_value <= 0.0:
            return False
        if new_seconds_value >= old_seconds_value:
            return False
        if old_seconds_value - new_seconds_value < 15.0:
            return False
        byte_tolerance = max(16 * 1024 * 1024, int(old_bytes_value * 0.05))
        if new_bytes_value > old_bytes_value + byte_tolerance:
            return False
        return True

    def _save_resume_progress(
        self,
        progress_path,
        seconds,
        source_url=None,
        bytes_done=None,
        min_interval_seconds=RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS,
        min_bytes_delta=RESUME_PROGRESS_MIN_BYTES_DELTA,
    ):
        if not progress_path:
            return
        seconds_value = max(float(seconds or 0), 0.0)
        bytes_value = max(int(bytes_done or 0), 0)
        source_value = str(source_url or "")
        resume_id_value = self._normalize_resume_state_id(progress_path)
        min_interval_seconds = max(float(min_interval_seconds or 0.0), 0.0)
        min_bytes_delta = max(int(min_bytes_delta or 0), 0)
        now = time.time()
        cached = None
        with self._resume_progress_lock:
            cached = self._resume_progress_cache.get(progress_path)
            if cached:
                last_seconds = float(cached.get("seconds", 0.0) or 0.0)
                last_bytes = int(cached.get("bytes", 0) or 0)
                last_source = str(cached.get("source_url", "") or "")
                last_resume_id = str(cached.get("resume_id", "") or "")
                last_saved_at = float(cached.get("saved_at", 0.0) or 0.0)
                same_resume_target = (
                    (source_value and source_value == last_source)
                    or (resume_id_value and resume_id_value == last_resume_id)
                )
                if same_resume_target:
                    if self._should_prefer_lower_resume_progress(seconds_value, bytes_value, last_seconds, last_bytes):
                        pass
                    else:
                        seconds_value = max(seconds_value, last_seconds)
                        bytes_value = max(bytes_value, last_bytes)
                if (
                    same_resume_target
                    and now - last_saved_at < min_interval_seconds
                    and abs(seconds_value - last_seconds) < min_interval_seconds
                    and abs(bytes_value - last_bytes) < min_bytes_delta
                ):
                    self._resume_progress_cache[progress_path] = {
                        "seconds": seconds_value,
                        "bytes": bytes_value,
                        "source_url": source_value,
                        "resume_id": resume_id_value,
                        "saved_at": last_saved_at,
                    }
                    return
        should_load_persisted = True
        if cached:
            cached_source = str(cached.get("source_url", "") or "")
            cached_resume_id = str(cached.get("resume_id", "") or "")
            if (
                (not source_value or source_value == cached_source)
                or (resume_id_value and resume_id_value == cached_resume_id)
            ):
                should_load_persisted = False
        if should_load_persisted:
            persisted = self._load_resume_progress_info(progress_path)
            persisted_source = str(persisted.get("source_url", "") or "")
            persisted_resume_id = str(persisted.get("resume_id", "") or "")
            if (
                (source_value and persisted_source == source_value)
                or (resume_id_value and persisted_resume_id == resume_id_value)
            ):
                persisted_seconds = float(persisted.get("seconds", 0.0) or 0.0)
                persisted_bytes = int(persisted.get("bytes", 0) or 0)
                if self._should_prefer_lower_resume_progress(seconds_value, bytes_value, persisted_seconds, persisted_bytes):
                    pass
                else:
                    seconds_value = max(seconds_value, persisted_seconds)
                    bytes_value = max(bytes_value, persisted_bytes)
        payload = {
            "seconds": seconds_value,
            "bytes": bytes_value,
            "source_url": source_value,
            "resume_id": resume_id_value,
            "updated_at": now,
        }
        try:
            _atomic_json_dump(progress_path, payload)
            with self._resume_progress_lock:
                self._resume_progress_cache[progress_path] = {
                    "seconds": seconds_value,
                    "bytes": bytes_value,
                    "source_url": source_value,
                    "resume_id": resume_id_value,
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

    def _sanitize_resume_seconds(self, resume_seconds, total_duration):
        try:
            seconds_value = max(float(resume_seconds or 0.0), 0.0)
            duration_value = max(float(total_duration or 0.0), 0.0)
        except (TypeError, ValueError):
            return 0.0
        if seconds_value <= 0 or duration_value <= 0:
            return seconds_value
        if seconds_value < duration_value:
            return seconds_value
        tail_seconds = min(5.0, max(1.0, duration_value * 0.01))
        return max(duration_value - tail_seconds, 0.0)

    def _get_m3u8_resume_rewind_seconds(self, url, headers=None):
        headers = {
            "Referer": (headers or {}).get("Referer"),
            "Origin": (headers or {}).get("Origin"),
        }
        try:
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=15, headers=headers)
            if resp.status_code != 200:
                return HLS_RESUME_REWIND_MIN_SECONDS
            text = resp.text or ""
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
                        return HLS_RESUME_REWIND_MIN_SECONDS
                    text = resp.text or ""
            target_duration = 0.0
            target_match = re.search(r"#EXT-X-TARGETDURATION:(\d+)", text)
            if target_match:
                try:
                    target_duration = float(target_match.group(1))
                except ValueError:
                    target_duration = 0.0
            extinf_values = []
            for value in re.findall(r"#EXTINF:([0-9.]+)", text):
                try:
                    extinf_values.append(float(value))
                except ValueError:
                    continue
                if len(extinf_values) >= 20:
                    break
            segment_hint = max([target_duration] + extinf_values + [0.0])
            if segment_hint <= 0.0:
                return HLS_RESUME_REWIND_MIN_SECONDS
            rewind_seconds = segment_hint * HLS_RESUME_REWIND_SEGMENT_MULTIPLIER
            return min(max(rewind_seconds, HLS_RESUME_REWIND_MIN_SECONDS), HLS_RESUME_REWIND_MAX_SECONDS)
        except Exception:
            return HLS_RESUME_REWIND_MIN_SECONDS

    def _get_resume_checkpoint_seconds(
        self,
        output_path,
        estimated_seconds,
        total_duration=0.0,
        base_offset_seconds=0.0,
        trim_initial_seconds=0.0,
    ):
        estimated_value = max(float(estimated_seconds or 0.0), 0.0)
        total_duration_value = max(float(total_duration or 0.0), 0.0)
        base_offset_value = max(float(base_offset_seconds or 0.0), 0.0)
        trim_initial_value = max(float(trim_initial_seconds or 0.0), 0.0)
        probed_seconds = self._probe_media_duration_seconds(output_path)
        if probed_seconds > 0.0:
            probed_seconds = base_offset_value + max(probed_seconds - trim_initial_value, 0.0)
            if total_duration_value > 0.0:
                probed_seconds = self._sanitize_resume_seconds(probed_seconds, total_duration_value)
            return min(estimated_value, probed_seconds) if estimated_value > 0.0 else probed_seconds
        if total_duration_value > 0.0 and estimated_value > 0.0:
            return self._sanitize_resume_seconds(estimated_value, total_duration_value)
        return estimated_value

    def _should_reject_resumed_ffmpeg_artifact(self, task, final_size, final_duration, base_bytes, total_duration):
        try:
            final_size_value = max(int(final_size or 0), 0)
            base_bytes_value = max(int(base_bytes or 0), 0)
            final_duration_value = max(float(final_duration or 0.0), 0.0)
            total_duration_value = max(float(total_duration or 0.0), 0.0)
        except (TypeError, ValueError):
            return False
        if _task_source_site_name(task) != "gimy":
            return False
        if base_bytes_value <= 0 or final_size_value <= 0:
            return False
        if final_size_value > base_bytes_value + max(2 * 1024 * 1024, int(base_bytes_value * 0.01)):
            return False
        if total_duration_value > 0 and final_duration_value >= max(total_duration_value * 0.98, total_duration_value - 5.0):
            return False
        return True

    def _should_keep_stored_resume_seconds(self, stored_seconds, probed_seconds, stored_bytes, partial_size):
        try:
            stored_seconds_value = max(float(stored_seconds or 0.0), 0.0)
            probed_seconds_value = max(float(probed_seconds or 0.0), 0.0)
            stored_bytes_value = max(int(stored_bytes or 0), 0)
            partial_size_value = max(int(partial_size or 0), 0)
        except (TypeError, ValueError):
            return False
        if stored_seconds_value <= 0.0 or probed_seconds_value <= 0.0:
            return False
        if stored_seconds_value <= probed_seconds_value:
            return False
        seconds_drift = stored_seconds_value - probed_seconds_value
        allowed_drift = max(8.0, probed_seconds_value * 0.005)
        if seconds_drift > allowed_drift:
            return False
        byte_tolerance = max(8 * 1024 * 1024, int(max(stored_bytes_value, partial_size_value) * 0.03))
        if abs(stored_bytes_value - partial_size_value) > byte_tolerance:
            return False
        return True

    def _probe_http_download_info(self, url, headers=None, session=None):
        headers = dict(headers or {})
        if "User-Agent" not in headers:
            headers["User-Agent"] = DEFAULT_USER_AGENT
        total_size = 0
        range_supported = False
        content_type = ""
        disable_ssl_verify = (
            "vr.goodav17.com" in urllib.parse.urlparse(str(url or "")).netloc.lower()
            or "ggjav.com" in urllib.parse.urlparse(str(url or "")).netloc.lower()
        )
        anime1_header_candidates = [url, headers.get("Referer", ""), headers.get("Origin", "")]
        prefer_curl = disable_ssl_verify or any(
            isinstance(candidate, str)
            and candidate.strip()
            and (
                "anime1.me" in candidate.lower()
                or "anime1.pw" in candidate.lower()
                or ".v.anime1.me" in candidate.lower()
            )
            for candidate in anime1_header_candidates
        )
        if prefer_curl:
            transient_sessions = []
            try:
                c_req = get_curl_cffi_requests()
                last_exc = None
                candidate_sessions = []
                if session is not None:
                    candidate_sessions.append(session)
                for browser in ("chrome110", "chrome120"):
                    try:
                        candidate_session = c_req.Session(impersonate=browser)
                        candidate_sessions.append(candidate_session)
                        transient_sessions.append(candidate_session)
                    except Exception:
                        continue
                for candidate_session in candidate_sessions:
                    try:
                        resp = candidate_session.get(
                            url,
                            headers=headers,
                            timeout=20,
                            stream=True,
                            verify=not disable_ssl_verify,
                        )
                        status = getattr(resp, "status_code", 0)
                        if status >= 400:
                            raise Exception(f"HTTP {status}")
                        resp_headers = resp.headers
                        content_type = _response_header_value(resp_headers, "Content-Type")
                        accept_ranges = str(_response_header_value(resp_headers, "Accept-Ranges")).lower()
                        content_range = _response_header_value(resp_headers, "Content-Range")
                        if status == 206 or "bytes" in accept_ranges or content_range:
                            range_supported = True
                        if content_range and "/" in content_range:
                            total_size = max(int(content_range.split("/")[-1]), 0)
                        else:
                            total_size = max(int(_response_header_value(resp_headers, "Content-Length", 0) or 0), 0)
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
            finally:
                for transient_session in transient_sessions:
                    try:
                        transient_session.close()
                    except Exception:
                        pass
        try:
            req = urllib.request.Request(url, headers=_make_range_http_headers(headers))
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = getattr(resp, "status", resp.getcode())
                resp_headers = resp.headers
                content_type = _response_header_value(resp_headers, "Content-Type")
                accept_ranges = str(_response_header_value(resp_headers, "Accept-Ranges")).lower()
                content_range = _response_header_value(resp_headers, "Content-Range")
                if status == 206 or "bytes" in accept_ranges or content_range:
                    range_supported = True
                if content_range and "/" in content_range:
                    total_size = max(int(content_range.split("/")[-1]), 0)
                else:
                    total_size = max(int(_response_header_value(resp_headers, "Content-Length", 0) or 0), 0)
            return {"total_size": total_size, "range_supported": range_supported, "content_type": content_type}
        except Exception:
            transient_sessions = []
            try:
                req = urllib.request.Request(url, headers=headers, method="HEAD")
                with urllib.request.urlopen(req, timeout=20) as resp:
                    resp_headers = resp.headers
                    content_type = _response_header_value(resp_headers, "Content-Type")
                    accept_ranges = str(_response_header_value(resp_headers, "Accept-Ranges")).lower()
                    total_size = max(int(_response_header_value(resp_headers, "Content-Length", 0) or 0), 0)
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
                            candidate_session = c_req.Session(impersonate=browser)
                            candidate_sessions.append(candidate_session)
                            transient_sessions.append(candidate_session)
                        except Exception:
                            continue
                    for candidate_session in candidate_sessions:
                        try:
                            resp = candidate_session.get(
                                url,
                                headers=headers,
                                timeout=20,
                                stream=True,
                                verify=not disable_ssl_verify,
                            )
                            status = getattr(resp, "status_code", 0)
                            if status >= 400:
                                raise Exception(f"HTTP {status}")
                            resp_headers = resp.headers
                            content_type = _response_header_value(resp_headers, "Content-Type")
                            total_size = max(int(_response_header_value(resp_headers, "Content-Length", 0) or 0), 0)
                            range_supported = "bytes" in str(_response_header_value(resp_headers, "Accept-Ranges")).lower()
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
                finally:
                    for transient_session in transient_sessions:
                        try:
                            transient_session.close()
                        except Exception:
                            pass
            return {"total_size": total_size, "range_supported": range_supported, "content_type": content_type}

    def _download_http_range_part(self, url, headers, start_byte, end_byte, part_path, progress_box, stop_event):
        req_headers = _make_range_http_headers(headers, f"bytes={start_byte}-{end_byte}")
        parsed_download_url = urllib.parse.urlparse(str(url or ""))
        download_netloc = parsed_download_url.netloc.lower()
        disable_ssl_verify = (
            "vr.goodav17.com" in download_netloc
            or "ggjav.com" in download_netloc
        )
        prefer_curl_transport = disable_ssl_verify or ("avjoy.me" in download_netloc and "media-cdn" in download_netloc)
        expected_size = max(int(end_byte) - int(start_byte) + 1, 0)
        if prefer_curl_transport:
            c_req = get_curl_cffi_requests()
            last_exc = None
            candidate_sessions = []
            owned_sessions = []
            for browser in ("chrome120", "chrome110"):
                try:
                    candidate_session = c_req.Session(impersonate=browser)
                    candidate_sessions.append(candidate_session)
                    owned_sessions.append(candidate_session)
                except Exception:
                    continue
            response = None
            try:
                for attempt in range(4):
                    for candidate_session in candidate_sessions:
                        try:
                            candidate_response = candidate_session.get(
                                url,
                                headers=req_headers,
                                timeout=30,
                                stream=True,
                                verify=False,
                            )
                            status = getattr(candidate_response, "status_code", 0)
                            if status >= 400:
                                try:
                                    candidate_response.close()
                                except Exception:
                                    pass
                                raise Exception(f"HTTP {status}")
                            if expected_size > 0 and status != 206:
                                try:
                                    candidate_response.close()
                                except Exception:
                                    pass
                                raise Exception(f"HTTP range request returned {status or 'unknown'}")
                            response = candidate_response
                            last_exc = None
                            break
                        except Exception as exc:
                            last_exc = exc
                    if response is not None or stop_event.is_set() or self._shutdown_started:
                        break
                    time.sleep(min(0.4 * (attempt + 1), 1.5))
                if response is None and last_exc is not None:
                    raise last_exc
                with open(part_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=HTTP_RANGE_CHUNK_SIZE):
                        if stop_event.is_set() or self._shutdown_started:
                            break
                        if not chunk:
                            continue
                        f.write(chunk)
                        progress_box["bytes"] = progress_box["bytes"] + len(chunk)
                actual_size = self._get_existing_file_size(part_path)
                if expected_size > 0 and actual_size != expected_size and not stop_event.is_set() and not self._shutdown_started:
                    raise Exception(f"HTTP range part incomplete: expected {expected_size}, got {actual_size}")
            finally:
                try:
                    if response is not None:
                        response.close()
                except Exception:
                    pass
                for candidate_session in owned_sessions:
                    try:
                        candidate_session.close()
                    except Exception:
                        pass
            return
        req = urllib.request.Request(url, headers=req_headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = int(getattr(resp, "status", 0) or resp.getcode() or 0)
            if expected_size > 0 and status != 206:
                raise Exception(f"HTTP range request returned {status or 'unknown'}")
            with open(part_path, "wb") as f:
                while not stop_event.is_set() and not self._shutdown_started:
                    chunk = resp.read(HTTP_RANGE_CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    progress_box["bytes"] = progress_box["bytes"] + len(chunk)
        actual_size = self._get_existing_file_size(part_path)
        if expected_size > 0 and actual_size != expected_size and not stop_event.is_set() and not self._shutdown_started:
            raise Exception(f"HTTP range part incomplete: expected {expected_size}, got {actual_size}")

    def _download_direct_media_audio_with_ffmpeg(self, item_id, url, save_dir, referer=None, origin=None):
        task = self.tasks.get(item_id, {})
        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "Audio" or "").strip()
        safe_name = _safe_output_stem(name, fallback="Audio")
        out_path = os.path.join(save_dir, f"{safe_name}.mp3")
        if self._set_output_path_and_complete_if_exists(task, item_id, out_path):
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
            _set_task_aux_fields(self.tasks[item_id], _proc=proc)
        recent_lines = []
        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
                if self._is_pause_requested_state(state):
                    self._terminate_ffmpeg_process(self.tasks.get(item_id, {}), item_id, proc, url, "pause_requested")
                    _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                    self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                    return
                if self._is_delete_requested_state(state):
                    self._terminate_ffmpeg_process(self.tasks.get(item_id, {}), item_id, proc, url, "delete_requested")
                    _set_task_aux_fields(self.tasks[item_id], state="DELETED")
                    self._discard_task(item_id)
                    return
                current_size = self._get_existing_file_size(temp_out_path)
                if self._maybe_auto_pause_for_disk_space(item_id, temp_out_path, note=t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"):
                    self._terminate_ffmpeg_process(
                        self.tasks.get(item_id, {}),
                        item_id,
                        proc,
                        url,
                        "disk_full_auto_pause",
                        bytes=current_size,
                    )
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
            if current_task is not None and _task_field_value(current_task, "_proc", None) is proc:
                _set_task_aux_fields(current_task, _proc=None)

        if return_code != 0:
            raise Exception(f"FFmpeg direct audio exited with code {return_code}: {' | '.join(recent_lines)[:240]}")
        if not self._has_nonempty_file(temp_out_path, min_bytes=64 * 1024):
            raise Exception("FFmpeg direct audio produced an invalid output artifact")

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except OSError:
                pass
        self._move_file_with_retry(temp_out_path, out_path)
        self._set_task_named_column_text(item_id, "progress", "100%")
        self._mark_task_finished(item_id)
        write_error_log("ffmpeg direct audio finished", Exception("ffmpeg direct audio finished"), url=url, item_id=item_id, output=out_path, bytes=self._get_existing_file_size(out_path))

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
        _set_task_aux_fields(task, state="PAUSED", disk_full_pause=True)
        message = note or (t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停")
        self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停", message)
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

    def _check_resume_disk_space(self, item_id):
        task = self.tasks.get(item_id) or {}
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        target_path = primary_value or secondary_value or str(self.save_dir_var.get() or _APP_DIR)
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
        self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停", t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停")
        _set_task_aux_fields(task, state="PAUSED", disk_full_pause=True)
        return False

    def _set_task_named_column_text(self, item_id, column, value):
        self.update_tree(item_id, column, value, force=True)

    def _set_task_status_mode_ui(self, item_id, status_text, message=None, complete_progress=False, clear_metrics=False):
        updates = {}
        if complete_progress:
            updates["progress"] = "100%"
        task = self.tasks.get(item_id)
        if task is not None:
            _set_task_aux_fields(task, _last_status_text=str(status_text))
        updates["status"] = status_text
        if message is not None:
            updates["speed_eta"] = message
        if updates:
            self.update_tree_many(item_id, updates, force=True)
        if clear_metrics:
            self.update_tree_many(item_id, {"progress": "--", "size": "-", "speed_eta": "-"}, force=True)

    def _ui_text(self, key, fallback):
        return t(key) if key in I18N_DICT.get(CURRENT_LANG, {}) else fallback

    def _set_task_parse_ui(self, item_id, key=None, fallback="", message=None, error=None):
        if error is not None:
            self._set_task_named_column_text(item_id, "speed_eta", f"{self._ui_text('err_site_parse', '解析失敗')}: {str(error)[:40]}")
            return
        if message is not None:
            self._set_task_named_column_text(item_id, "speed_eta", message)
            return
        self._set_task_named_column_text(item_id, "speed_eta", self._ui_text(key, fallback))

    def _set_task_active_transfer_ui(self, task, item_id, downloaded_bytes, total_bytes=None, speed_bps=None, eta_seconds=None, cap_at_99=False):
        _set_task_aux_fields(task, downloaded_bytes=downloaded_bytes, total_bytes=total_bytes)
        try:
            value = max(float(speed_bps or 0.0), 0.0)
        except Exception:
            value = 0.0
        _set_task_aux_fields(task, _last_speed_bps=value)
        updates = {
            "size": format_transfer_size(downloaded_bytes, total_bytes),
        }
        percent = format_progress_percent(downloaded_bytes, total_bytes, cap_at_99=cap_at_99) if total_bytes else None
        if percent is not None:
            updates["progress"] = f"{percent:.1f}%"
        speed_text = format_transfer_rate(speed_bps) if speed_bps else "-"
        if eta_seconds not in (None, ""):
            speed_text = f"{speed_text} | {format_eta(float(eta_seconds))}"
        updates["speed_eta"] = speed_text
        status_text = t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中"
        if str(_task_field_value(self.tasks.get(item_id, {}), "_last_status_text", "") or "") != status_text:
            if task is not None:
                _set_task_aux_fields(task, _last_status_text=str(status_text))
            updates["status"] = status_text
        self.update_tree_many(item_id, updates, force=True)

    def _effective_domain_download_limit(self):
        active_tasks = []
        for task in self.tasks.values():
            if self._is_downloading_state(str(_task_field_value(task, "state", "") or "")):
                active_tasks.append(task)
        if not active_tasks:
            return MAX_DOWNLOADS_PER_DOMAIN
        if len(active_tasks) >= LOW_SPEED_CONCURRENCY_MAX_DOWNLOADS:
            return LOW_SPEED_CONCURRENCY_MAX_DOWNLOADS
        measured_speeds = []
        for task in active_tasks:
            try:
                speed_bps = max(float(_task_field_value(task, "_last_speed_bps", 0.0) or 0.0), 0.0)
            except Exception:
                speed_bps = 0.0
            if speed_bps > 0:
                measured_speeds.append(speed_bps)
        if not measured_speeds:
            return MAX_DOWNLOADS_PER_DOMAIN
        if all(speed_bps < LOW_SPEED_CONCURRENCY_THRESHOLD_BPS for speed_bps in measured_speeds):
            return LOW_SPEED_CONCURRENCY_MAX_DOWNLOADS
        return MAX_DOWNLOADS_PER_DOMAIN

    def _source_download_limits(self, source_site, domain_limit):
        site = (source_site or "").strip().lower()
        source_page_limit = int(MAX_DOWNLOADS_PER_SOURCE_PAGE_BY_SITE.get(site, MAX_DOWNLOADS_PER_SOURCE_PAGE))
        source_site_limit = int(MAX_DOWNLOADS_PER_SOURCE_SITE.get(site, MAX_DOWNLOADS_PER_DOMAIN))
        if source_page_limit == MAX_DOWNLOADS_PER_SOURCE_PAGE:
            source_page_limit = domain_limit
        if source_site_limit == MAX_DOWNLOADS_PER_DOMAIN:
            source_site_limit = domain_limit
        return source_page_limit, source_site_limit

    def _log_domain_limit_change(self, domain_limit, active_count, queued_count):
        try:
            normalized_limit = int(domain_limit)
        except Exception:
            return
        if self._last_reported_domain_limit == normalized_limit:
            return
        previous_limit = self._last_reported_domain_limit
        self._last_reported_domain_limit = normalized_limit
        write_error_log(
            "download concurrency limit changed",
            Exception("download concurrency limit changed"),
            previous_limit=previous_limit,
            domain_limit=normalized_limit,
            active_count=active_count,
            queued_count=queued_count,
            low_speed_threshold_bps=LOW_SPEED_CONCURRENCY_THRESHOLD_BPS,
            low_speed_max_downloads=LOW_SPEED_CONCURRENCY_MAX_DOWNLOADS,
        )

    def _make_yt_dlp_progress_hook(self, task, item_id, save_dir):
        last_ui_update = 0.0
        last_ui_bytes = 0

        def progress_hook(d):
            nonlocal last_ui_update, last_ui_bytes
            task_state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            if self._is_pause_requested_state(task_state):
                raise StopDownloadException("pause requested")
            if self._is_delete_requested_state(task_state):
                raise KeyboardInterrupt()
            status = str((d or {}).get("status", "") or "")
            if status == "downloading":
                target_path, downloaded, total = _event_transfer_metrics(d, default_path=save_dir, default_downloaded=0)
                required_bytes = max(int(total) - int(downloaded), 0) if total else None
                if self._maybe_auto_pause_for_disk_space(item_id, target_path, required_bytes=required_bytes, note=t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"):
                    raise StopDownloadException("disk space low")
            if status == "finished":
                return
            if status != "downloading":
                return
            _target_path, downloaded, total = _event_transfer_metrics(d, default_path=save_dir, default_downloaded=0)
            speed = (d or {}).get("speed")
            eta = (d or {}).get("eta")
            now = time.time()
            should_refresh_ui = (
                downloaded <= 0
                or total is None
                or downloaded >= (total or 0)
                or (downloaded - last_ui_bytes) >= YTDLP_PROGRESS_UI_MIN_BYTES_DELTA
                or (now - last_ui_update) >= YTDLP_PROGRESS_UI_UPDATE_INTERVAL_SECONDS
            )
            if should_refresh_ui:
                self._set_task_active_transfer_ui(
                    task,
                    item_id,
                    downloaded,
                    total_bytes=total,
                    speed_bps=speed,
                    eta_seconds=eta,
                )
                last_ui_update = now
                last_ui_bytes = downloaded
                if self._should_trigger_resume_low_speed_reanalysis(
                    task,
                    _normalize_download_url(_task_field_value(task, "url", "")) or "",
                    save_dir,
                    speed,
                    is_mp3=bool(_task_field_value(task, "is_mp3", False)),
                    now=now,
                ):
                    raise ResumeLowSpeedReanalysisException("resume transfer stayed below the low-speed threshold")

        return progress_hook

    def _download_mega_with_megacmd(self, item_id, url, save_dir, output_path=None, total_size=None):
        task = self.tasks.get(item_id, {})
        mega_get_cmd = _find_megacmd_get_command()
        if not mega_get_cmd:
            raise RuntimeError("MEGAcmd is not available")

        cmd = list(mega_get_cmd) + [url, save_dir]
        write_error_log(
            "mega cmd download started",
            Exception("mega cmd download started"),
            url=url,
            item_id=item_id,
            source_site="mega",
            save_dir=save_dir,
            output_path=output_path,
            total_size=total_size,
            cmd_preview=" ".join(str(part) for part in cmd),
        )

        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )
        _set_task_aux_fields(task, _proc=proc)
        last_bytes = 0
        last_time = time.time()
        try:
            while proc.poll() is None:
                state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
                if self._is_pause_requested_state(state):
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception:
                        pass
                    _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                    self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                    write_error_log(
                        "mega cmd terminate requested",
                        Exception("mega cmd terminate requested"),
                        url=url,
                        item_id=item_id,
                        source_site="mega",
                        output_path=output_path,
                        reason="pause_requested",
                    )
                    return
                if self._is_delete_requested_state(state):
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except Exception:
                        pass
                    write_error_log(
                        "mega cmd terminate requested",
                        Exception("mega cmd terminate requested"),
                        url=url,
                        item_id=item_id,
                        source_site="mega",
                        output_path=output_path,
                        reason="delete_requested",
                    )
                    raise KeyboardInterrupt()
                if output_path and total_size and total_size > 0 and os.path.exists(output_path):
                    downloaded = self._get_existing_file_size(output_path)
                    now = time.time()
                    speed_bps = max((downloaded - last_bytes) / max(now - last_time, 0.001), 0.0)
                    last_bytes = downloaded
                    last_time = now
                    eta = max((total_size - downloaded) / max(speed_bps, 1.0), 0.0)
                    self._set_task_active_transfer_ui(
                        task,
                        item_id,
                        downloaded,
                        total_bytes=total_size,
                        speed_bps=speed_bps,
                        eta_seconds=eta,
                        cap_at_99=True,
                    )
                time.sleep(1.0)
        finally:
            _set_task_aux_fields(task, _proc=None)

        return_code = proc.returncode
        if return_code != 0:
            raise RuntimeError(f"MEGAcmd exited with code {return_code}")

        write_error_log(
            "mega cmd download finished",
            Exception("mega cmd download finished"),
            url=url,
            item_id=item_id,
            source_site="mega",
            output_path=output_path,
            total_size=total_size,
        )

    def _download_mega_public_file(self, item_id, url, save_dir, is_mp3=False):
        task = self.tasks.get(item_id, {})
        normalized_mega_url = str(url or "").strip().lower()
        parsed_mega_url = urllib.parse.urlparse(normalized_mega_url) if normalized_mega_url else None
        mega_fragment = str(parsed_mega_url.fragment or "").strip().lower() if parsed_mega_url else ""
        is_folder_link = bool(
            parsed_mega_url
            and (
                "/folder/" in parsed_mega_url.path.lower()
                or mega_fragment.startswith("f!")
                or mega_fragment.startswith("folder/")
            )
        )
        if is_folder_link and is_mp3:
            raise RuntimeError("MEGA folder links do not support MP3 mode yet")
        mega_client = MegaClient() if MegaClient is not None else None
        public_info = {}
        safe_name = ""
        out_path = ""
        source_out_path = ""
        total_size = None

        if not is_folder_link and mega_client is not None:
            file_handle, file_key = _parse_mega_public_file_parts(mega_client, url)
            public_info = mega_client.get_public_file_info(file_handle, file_key) or {}
            raw_name = str(public_info.get("name") or "").strip()
            if not raw_name:
                raw_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
                if not raw_name:
                    fallback_display_url = (_normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url))
                    if fallback_display_url:
                        raw_name = default_short_name_for_url(
                            fallback_display_url,
                            is_mp3=bool(_task_field_value(task, "is_mp3", is_mp3)),
                        )
                raw_name = raw_name or "mega_download.bin"
            safe_name = _safe_output_stem(raw_name, fallback="mega_download")
            if not os.path.splitext(safe_name)[1]:
                safe_name += ".bin"
            source_out_path = os.path.join(save_dir, safe_name)
            if is_mp3:
                out_path = os.path.join(save_dir, f"{os.path.splitext(safe_name)[0]}.mp3")
                if self._set_output_path_and_complete_if_exists(task, item_id, out_path):
                    return
                _set_task_aux_fields(task, temp_filename=source_out_path)
            else:
                _set_task_aux_fields(task, filename=source_out_path)
                self._set_task_named_column_text(item_id, "name", os.path.basename(str(source_out_path or "").strip()) or "")
                out_path = source_out_path
            try:
                total_size = int(public_info.get("size") or 0)
            except Exception:
                total_size = None
        else:
            safe_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
            if not safe_name:
                fallback_display_url = (_normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url))
                if fallback_display_url:
                    safe_name = default_short_name_for_url(
                        fallback_display_url,
                        is_mp3=bool(_task_field_value(task, "is_mp3", is_mp3)),
                    )
            safe_name = safe_name or "MEGA folder"

        display_name = os.path.splitext(safe_name)[0] if is_mp3 and safe_name else safe_name
        normalized_name = str(display_name or "").strip()
        if normalized_name:
            task["short_name"] = normalized_name
            task["name"] = normalized_name
        self._set_task_named_column_text(item_id, "name", normalized_name)
        source_updates = {}
        normalized_site = "mega"
        task["source_site"] = normalized_site
        source_updates["source_site"] = normalized_site
        normalized_page = _normalize_download_url(url)
        if normalized_page:
            task["source_page"] = normalized_page
            source_updates["source_page"] = normalized_page
        normalized_urls = _dedupe_download_urls([], primary_url=_normalize_download_url(_task_field_value(task, "url", "")) or "")
        task["fallback_urls"] = normalized_urls
        source_updates["fallback_urls"] = normalized_urls
        self._update_task_state_entry(task, name=normalized_name, **source_updates)

        if total_size and total_size > 0:
            if total_size and total_size > 0:
                _set_task_aux_fields(task, downloaded_bytes=0, total_bytes=total_size)
                self._set_task_named_column_text(item_id, "size", format_transfer_size(0, total_size))
            if (not is_mp3) and out_path and self._get_existing_file_size(out_path) >= total_size:
                self._mark_existing_file_complete(item_id, self._ui_text("eta_file_exists", "檔案已存在"))
                return

        self._set_task_status_mode_ui(item_id, t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中", "MEGA 下載中")
        mega_get_cmd = _find_megacmd_get_command()
        if mega_get_cmd:
            self._download_mega_with_megacmd(item_id, url, save_dir, output_path=source_out_path or out_path or None, total_size=total_size)
            if is_mp3:
                if not source_out_path or not os.path.exists(source_out_path):
                    raise RuntimeError("MEGA source file missing after MEGAcmd download")
                self._download_direct_media_audio_with_ffmpeg(item_id, source_out_path, save_dir)
                try:
                    os.remove(source_out_path)
                except OSError:
                    pass
                _set_task_aux_fields(task, temp_filename=None)
                return
            self._mark_task_finished(item_id)
            return

        if MegaClient is None:
            raise RuntimeError("MEGA support package is not available")
        if is_folder_link:
            raise RuntimeError("MEGA folder links require MEGAcmd for stable download support")

        write_error_log(
            "mega public download started",
            Exception("mega public download started"),
            url=url,
            item_id=item_id,
            source_site="mega",
            output_path=out_path,
            total_size=total_size,
            backend="mega-py-v2",
        )
        downloaded_path = mega_client.download_url(url, dest_path=save_dir, dest_filename=safe_name)
        if downloaded_path:
            final_path = os.path.abspath(str(downloaded_path))
            source_out_path = final_path
            if not is_mp3:
                _set_task_aux_fields(task, filename=final_path)
                self._set_task_named_column_text(item_id, "name", os.path.basename(str(final_path or "").strip()) or "")
                out_path = final_path
        write_error_log(
            "mega public download finished",
            Exception("mega public download finished"),
            url=url,
            item_id=item_id,
            source_site="mega",
            output_path=out_path,
            backend="mega-py-v2",
        )
        if is_mp3:
            if not source_out_path or not os.path.exists(source_out_path):
                raise RuntimeError("MEGA source file missing after public download")
            self._download_direct_media_audio_with_ffmpeg(item_id, source_out_path, save_dir)
            try:
                os.remove(source_out_path)
            except OSError:
                pass
            _set_task_aux_fields(task, temp_filename=None)
            return
        self._mark_task_finished(item_id)

    def _download_http_media(self, item_id, url, out_path, headers=None, session=None):
        headers = dict(headers or {})
        task = self.tasks.get(item_id, {})
        self._cache_task_resolved_link(task, url, fallback_urls=_dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url))
        pause_note = t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"
        if self._maybe_auto_pause_for_disk_space(item_id, out_path, note=pause_note):
            return
        if "User-Agent" not in headers:
            headers["User-Agent"] = DEFAULT_USER_AGENT
        disable_ssl_verify = (
            "vr.goodav17.com" in urllib.parse.urlparse(str(url or "")).netloc.lower()
            or "ggjav.com" in urllib.parse.urlparse(str(url or "")).netloc.lower()
        )
        anime1_header_candidates = [url, headers.get("Referer", ""), headers.get("Origin", "")]
        prefer_curl_stream = disable_ssl_verify or any(
            isinstance(candidate, str)
            and candidate.strip()
            and (
                "anime1.me" in candidate.lower()
                or "anime1.pw" in candidate.lower()
                or ".v.anime1.me" in candidate.lower()
            )
            for candidate in anime1_header_candidates
        )
        if headers.get("Referer"):
            try:
                parsed_referer = urllib.parse.urlsplit(str(headers.get("Referer") or ""))
            except Exception:
                headers["Referer"] = str(headers.get("Referer") or "")
            else:
                if not parsed_referer.scheme or not parsed_referer.netloc:
                    headers["Referer"] = str(headers.get("Referer") or "")
                else:
                    safe_path = urllib.parse.quote(parsed_referer.path or "/", safe="/%")
                    headers["Referer"] = urllib.parse.urlunsplit((parsed_referer.scheme, parsed_referer.netloc, safe_path, parsed_referer.query, ""))
        total_info = self._probe_http_download_info(url, headers=headers, session=session)
        total_size = total_info.get("total_size", 0)
        range_supported = bool(total_info.get("range_supported"))
        resume_bytes = self._get_existing_file_size(out_path)
        if prefer_curl_stream:
            if resume_bytes > 0:
                try:
                    os.remove(out_path)
                except OSError:
                    pass
            resume_bytes = 0
            range_supported = False
        if total_size > 0 and resume_bytes >= total_size:
            self._mark_existing_file_complete(item_id, self._ui_text("eta_file_exists", "檔案已存在"))
            return
        immediate_multipart = (
            not prefer_curl_stream
            and range_supported
            and total_size > 0
            and resume_bytes <= 0
            and _task_source_site_name(task) in HTTP_MULTIPART_IMMEDIATE_SITES
            and max(int(total_size or 0), 0) >= HTTP_MULTIPART_IMMEDIATE_MIN_BYTES
        )
        mode = "ab" if resume_bytes > 0 else "wb"
        dl_headers = _make_range_http_headers(headers, f"bytes={resume_bytes}-") if resume_bytes > 0 else dict(headers)
        res = None
        stream_iter = None
        stream_response = None
        owned_sessions = []
        try:
            if immediate_multipart:
                raise RuntimeError("prefer immediate multipart")
            if prefer_curl_stream or session is not None:
                raise RuntimeError("prefer curl stream")
            req = urllib.request.Request(url, headers=dl_headers)
            res = urllib.request.urlopen(req, timeout=20)
        except Exception:
            if immediate_multipart:
                stream_iter = None
            else:
                c_req = get_curl_cffi_requests()
                last_exc = None
                candidate_sessions = []
                if session is not None:
                    candidate_sessions.append(session)
                for browser in ("chrome110", "chrome120"):
                    try:
                        candidate_session = c_req.Session(impersonate=browser)
                        candidate_sessions.append(candidate_session)
                        owned_sessions.append(candidate_session)
                    except Exception:
                        continue
                for candidate_session in candidate_sessions:
                    try:
                        candidate_response = candidate_session.get(
                            url,
                            headers=dl_headers,
                            timeout=20,
                            stream=True,
                            verify=not disable_ssl_verify,
                        )
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
                stream_iter = stream_response.iter_content(chunk_size=HTTP_STREAM_CHUNK_SIZE)
        response_status = None
        try:
            if res is not None:
                response_status = int(getattr(res, "status", 0) or res.getcode() or 0)
            elif stream_response is not None:
                response_status = int(getattr(stream_response, "status_code", 0) or 0)
        except Exception:
            response_status = None
        if resume_bytes > 0 and response_status != 206:
            mode = "wb"
            resume_bytes = 0
        switched_to_multipart = False
        downloaded = resume_bytes
        start_time = time.time()
        last_update_time = start_time
        last_update_bytes = downloaded
        try:
            with open(out_path, mode) as f:
                while True:
                    state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
                    if state == "PAUSE_REQUESTED":
                        _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                        self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
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
                    if immediate_multipart and not switched_to_multipart:
                        switched_to_multipart = True
                        break
                    if not switched_to_multipart and not prefer_curl_stream and range_supported and total_size > 0:
                        elapsed_probe = time.time() - start_time
                        if elapsed_probe >= HTTP_MULTIPART_TRIGGER_SECONDS:
                            current_speed = max((downloaded - resume_bytes) / max(elapsed_probe, 0.001), 0.0)
                            remaining = max(total_size - downloaded, 0)
                            if current_speed < HTTP_MULTIPART_TRIGGER_SPEED_BPS and remaining > HTTP_MULTIPART_MIN_REMAINING_BYTES:
                                switched_to_multipart = True
                                break
                    if res is not None:
                        chunk = res.read(HTTP_STREAM_CHUNK_SIZE)
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
                    _set_task_aux_fields(task, downloaded_bytes=downloaded, total_bytes=(total_size if total_size > 0 else None))
                    now = time.time()
                    if now - last_update_time >= 1.0:
                        speed_bps = max((downloaded - last_update_bytes) / max(now - last_update_time, 0.001), 0.0)
                        last_update_time = now
                        last_update_bytes = downloaded
                        try:
                            value = max(float(speed_bps or 0.0), 0.0)
                        except Exception:
                            value = 0.0
                        _set_task_aux_fields(task, _last_speed_bps=value)
                        eta = max((total_size - downloaded) / max(speed_bps, 1.0), 0.0) if total_size > 0 else None
                        self._set_task_active_transfer_ui(
                            task,
                            item_id,
                            downloaded,
                            total_bytes=(total_size if total_size > 0 else None),
                            speed_bps=speed_bps,
                            eta_seconds=eta,
                            cap_at_99=True,
                        )
                        if self._should_trigger_resume_low_speed_reanalysis(
                            task,
                            _normalize_download_url(_task_field_value(task, "url", "")) or "",
                            os.path.dirname(out_path) or _APP_DIR,
                            speed_bps,
                            now=now,
                        ):
                            raise ResumeLowSpeedReanalysisException("resume transfer stayed below the low-speed threshold")
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
                remaining_size = max(total_size - downloaded, 0)
                source_site = _task_source_site_name(task)
                part_count = int(
                    HTTP_MULTIPART_PART_COUNT_BY_SITE.get(
                        source_site,
                        HTTP_MULTIPART_PART_COUNT_DEFAULT,
                    )
                )
                if remaining_size and remaining_size < 16 * 1024 * 1024:
                    part_count = min(part_count, 4)
                elif remaining_size and remaining_size < 64 * 1024 * 1024:
                    part_count = min(part_count, 12)
                part_count = min(max(2, part_count), 24)
                part_size = max((remaining_end - remaining_start + 1) // part_count, 1)
                write_error_log(
                    "http multipart download started",
                    Exception("http multipart download started"),
                    url=url,
                    item_id=item_id,
                    source_site=source_site,
                    part_count=part_count,
                    remaining_size=remaining_size,
                    part_size=part_size,
                    resume_bytes=resume_bytes,
                    total_size=total_size,
                )
                futures = []
                executor = None
                part_specs = []
                try:
                    executor = DaemonThreadPoolExecutor(max_workers=part_count)
                    for index in range(part_count):
                        part_start = remaining_start + index * part_size
                        if part_start > remaining_end:
                            break
                        part_end = remaining_end if index == part_count - 1 else min(remaining_end, part_start + part_size - 1)
                        normalized_out_path = os.path.abspath(str(out_path or "download"))
                        digest = hashlib.sha1(normalized_out_path.encode("utf-8", errors="ignore")).hexdigest()[:16]
                        part_path = os.path.join(tempfile.gettempdir(), f"downloader_http_{digest}.part{index}")
                        part_paths.append(part_path)
                        part_specs.append((part_path, part_start, part_end))
                        box = {"bytes": 0}
                        progress_boxes.append(box)
                        futures.append(executor.submit(self._download_http_range_part, url, headers, part_start, part_end, part_path, box, stop_event))
                    while futures:
                        if self._shutdown_started:
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            return
                        current_task_state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
                        if current_task_state in {"PAUSE_REQUESTED", "DELETE_REQUESTED"}:
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            if current_task_state == "PAUSE_REQUESTED":
                                _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                                self._set_task_named_column_text(item_id, "status", t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                            return
                        multi_downloaded = downloaded + sum(box["bytes"] for box in progress_boxes)
                        required_bytes = max(total_size - multi_downloaded, 0) if total_size > 0 else None
                        if self._maybe_auto_pause_for_disk_space(item_id, out_path, required_bytes=required_bytes, note=pause_note):
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            return
                        done, futures = concurrent.futures.wait(futures, timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)
                        for done_future in done:
                            part_error = done_future.exception()
                            if part_error is not None:
                                stop_event.set()
                                for future in futures:
                                    future.cancel()
                                raise part_error
                        now = time.time()
                        multi_downloaded = downloaded + sum(box["bytes"] for box in progress_boxes)
                        elapsed = max(now - start_time, 0.001)
                        speed_bps = max((multi_downloaded - resume_bytes) / elapsed, 0.0)
                        try:
                            value = max(float(speed_bps or 0.0), 0.0)
                        except Exception:
                            value = 0.0
                        _set_task_aux_fields(task, _last_speed_bps=value)
                        eta = max((total_size - multi_downloaded) / max(speed_bps, 1.0), 0.0)
                        self._set_task_active_transfer_ui(
                            task,
                            item_id,
                            multi_downloaded,
                            total_bytes=total_size,
                            speed_bps=speed_bps,
                            eta_seconds=eta,
                            cap_at_99=True,
                        )
                        if self._should_trigger_resume_low_speed_reanalysis(
                            task,
                            _normalize_download_url(_task_field_value(task, "url", "")) or "",
                            os.path.dirname(out_path) or _APP_DIR,
                            speed_bps,
                            now=now,
                        ):
                            stop_event.set()
                            for future in futures:
                                future.cancel()
                            raise ResumeLowSpeedReanalysisException("resume transfer stayed below the low-speed threshold")
                        time.sleep(0.5)
                finally:
                    if executor is not None:
                        try:
                            executor.shutdown(wait=not self._shutdown_started, cancel_futures=True)
                        except Exception:
                            pass
                for part_path, part_start, part_end in part_specs:
                    expected_part_size = max(part_end - part_start + 1, 0)
                    actual_part_size = self._get_existing_file_size(part_path)
                    if expected_part_size > 0 and actual_part_size != expected_part_size:
                        raise Exception(f"HTTP multipart part incomplete: expected {expected_part_size}, got {actual_part_size}")
                with open(out_path, "ab") as main_out:
                    for part_path in part_paths:
                        with open(part_path, "rb") as part_in:
                            shutil.copyfileobj(part_in, main_out, length=HTTP_FILE_COPY_CHUNK_SIZE)
                for part_path in part_paths:
                    if os.path.exists(part_path):
                        try:
                            os.remove(part_path)
                        except OSError:
                            pass
        except OSError as exc:
            if getattr(exc, "errno", None) == 28:
                free_bytes = self._get_disk_free_bytes(out_path)
                self._pause_task_for_disk_full(item_id, out_path, free_bytes, None, note=t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停")
                return
            raise
        except Exception:
            raise
        finally:
            for owned_session in owned_sessions:
                self._close_network_session(owned_session)
            if session is not None:
                self._close_network_session(session)
        self._mark_task_finished(item_id)

    def _resolve_direct_media_output_path(self, media_url, save_dir, display_name, default_ext=".mp4"):
        ext = os.path.splitext(urllib.parse.urlparse(str(media_url or "")).path)[1] or default_ext
        out_name = _safe_output_stem(display_name, fallback="video")
        return os.path.join(save_dir, out_name + ext), out_name + ext

    def _download_direct_or_audio_media(self, item_id, media_url, save_dir, display_name, is_mp3=False, referer=None, origin=None, headers=None, session=None, default_ext=".mp4"):
        if is_mp3:
            self._download_direct_media_audio_with_ffmpeg(item_id, media_url, save_dir, referer=referer, origin=origin)
            return None
        out_path, _ = self._resolve_direct_media_output_path(media_url, save_dir, display_name, default_ext=default_ext)
        media_headers = dict(headers or _make_ytdlp_http_headers(referer=referer, origin=origin))
        self._download_http_media(item_id, media_url, out_path, headers=media_headers, session=session)
        return out_path

    def _is_retryable_media_download_error(self, exc):
        text = str(exc or "").lower()
        retry_markers = (
            "http 403",
            "http 404",
            "http 410",
            "http 429",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
            "service temporarily unavailable",
            "temporarily unavailable",
            "timed out",
            "timeout",
        )
        return any(marker in text for marker in retry_markers)

    def _fetch_avjoy_media_candidates(self, page_url, fallback_name="AVJOY"):
        parsed = urllib.parse.urlsplit(str(page_url or ""))
        site_root = f"{parsed.scheme or 'https'}://{parsed.netloc or 'avjoy.me'}/"
        headers = _make_ytdlp_http_headers(referer=site_root)
        c_req = get_curl_cffi_requests()
        last_exc = None
        for browser in ("chrome120", "chrome110", "edge101"):
            try:
                resp = c_req.get(page_url, impersonate=browser, timeout=20, headers=headers)
                page_text = str(getattr(resp, "text", "") or "")
                candidates = _extract_candidate_media_urls(page_text, allowed_exts=(".mp4", ".m3u8", ".mpd"))
                unpacked = unpack_packed_javascript(page_text)
                if unpacked:
                    candidates.extend(_extract_candidate_media_urls(unpacked, allowed_exts=(".mp4", ".m3u8", ".mpd")))
                candidates = _dedupe_download_urls(candidates)
                slug_title = _avjoy_title_from_url_slug(page_url)
                title = _clean_avjoy_title(_extract_html_title(page_text, slug_title or fallback_name), slug_title or fallback_name)
                title_code_only = bool(re.fullmatch(r"[A-Z]{2,10}-\d{2,6}", str(title or "").strip()))
                if slug_title and (title_code_only or len(str(title or "")) < len(slug_title) * 0.45):
                    title = slug_title
                if candidates:
                    return title, candidates
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc is not None:
            raise last_exc
        return fallback_name, []

    def _select_avjoy_media_candidate(self, candidates, failed_urls=None):
        failed_set = set(_dedupe_download_urls(failed_urls or []))
        ordered = [candidate for candidate in _dedupe_download_urls(candidates) if candidate not in failed_set]
        if not ordered:
            return "", []
        manifest_candidates, direct_candidates = _split_stream_and_direct_candidates(ordered)
        media_url, fallback_urls = _pick_primary_with_fallbacks(direct_candidates)
        if not media_url:
            media_url, fallback_urls = _pick_primary_with_fallbacks(manifest_candidates)
        if not media_url:
            return "", []
        remaining = [candidate for candidate in ordered if candidate != media_url and candidate not in fallback_urls]
        return media_url, _dedupe_download_urls(fallback_urls + remaining)

    def _download_routed_media_url(self, task, item_id, media_url, save_dir, display_name, is_mp3=False, source_site=None, fallback_urls=None, referer=None, origin=None, manifest_downloader=None, manifest_default_route="ffmpeg", headers=None, session=None, default_ext=".mp4", allow_audio_extract=True, persist_direct_output=False, show_direct_filename=False):
        site = source_site or _task_source_site_name(task)
        fallbacks = list(fallback_urls or [])
        if _looks_like_manifest_url(media_url):
            self._log_m3u8_route_selected(task, item_id, media_url, source_site=site, fallback_urls=fallbacks)
            if manifest_downloader is None:
                self._download_m3u8_with_ffmpeg(item_id, media_url, save_dir, is_mp3=is_mp3, referer=referer, origin=origin)
            else:
                manifest_downloader(
                    media_url,
                    referer=referer,
                    origin=origin,
                    default_route=manifest_default_route,
                )
            return
        self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "撌脣?敺?擃雯?"))
        if persist_direct_output and not (is_mp3 and allow_audio_extract):
            out_path, out_name = self._resolve_direct_media_output_path(media_url, save_dir, display_name, default_ext=default_ext)
            _set_task_aux_fields(task, filename=out_path)
            if show_direct_filename:
                self._set_task_named_column_text(item_id, "name", out_name)
        direct_candidates = _dedupe_download_urls([media_url] + fallbacks)
        last_error = None
        for index, candidate_url in enumerate(direct_candidates):
            try:
                if _looks_like_manifest_url(candidate_url):
                    self._log_m3u8_route_selected(task, item_id, candidate_url, source_site=site, fallback_urls=direct_candidates[index + 1 :])
                    if manifest_downloader is None:
                        self._download_m3u8_with_ffmpeg(item_id, candidate_url, save_dir, is_mp3=is_mp3, referer=referer, origin=origin)
                    else:
                        manifest_downloader(
                            candidate_url,
                            referer=referer,
                            origin=origin,
                            default_route=manifest_default_route,
                        )
                    return
                self._download_direct_or_audio_media(
                    item_id,
                    candidate_url,
                    save_dir,
                    display_name,
                    is_mp3=is_mp3 and allow_audio_extract,
                    referer=referer,
                    origin=origin,
                    headers=headers,
                    session=session,
                    default_ext=default_ext,
                )
                return
            except (
                StopDownloadException,
                KeyboardInterrupt,
                ResumeLowSpeedReanalysisException,
                ParallelHlsRetryLaterException,
                ParallelHlsUnsupportedSegmentContentException,
            ):
                raise
            except Exception as exc:
                last_error = exc
                if index >= len(direct_candidates) - 1 or not self._is_retryable_media_download_error(exc):
                    raise
                write_error_log(
                    "direct media fallback retry",
                    Exception("direct media fallback retry"),
                    item_id=item_id,
                    url=candidate_url,
                    next_url=direct_candidates[index + 1],
                    source_site=site,
                    reason=str(exc)[:240],
                )
        if last_error:
            raise last_error

    def _gimy_direct_media_headers(self, referer, origin):
        return {"Referer": referer, "Origin": origin, "User-Agent": DEFAULT_USER_AGENT}

    def _parallel_hls_workers_for_site(self, source_site, url=""):
        site = (source_site or "").strip().lower()
        workers = int(PARALLEL_HLS_SEGMENT_WORKERS_BY_SITE.get(site, PARALLEL_HLS_SEGMENT_WORKERS))
        normalized_url = _normalize_download_url(url)
        host = urllib.parse.urlsplit(normalized_url).netloc.lower() if normalized_url else ""
        for marker, host_workers in PARALLEL_HLS_SEGMENT_WORKERS_BY_HOST.items():
            if marker in host:
                workers = min(workers, int(host_workers))
                break
        return max(workers, 1)

    def _parallel_hls_workers_for_segments(self, source_site, media_url, segments):
        workers = self._parallel_hls_workers_for_site(source_site, media_url)
        segment_hosts = {
            urllib.parse.urlsplit(_normalize_download_url(segment.get("url", "")) or "").netloc.lower()
            for segment in (segments or [])
        }
        for marker, host_workers in PARALLEL_HLS_SEGMENT_WORKERS_BY_HOST.items():
            if any(marker in host for host in segment_hosts):
                workers = min(workers, int(host_workers))
        return max(workers, 1)

    def _should_try_parallel_hls_segments(self, url, task):
        source_site = _task_source_site_name(task)
        if source_site not in PARALLEL_HLS_SEGMENT_SITES:
            return False
        normalized_url = _normalize_download_url(url)
        if not normalized_url:
            return False
        host = urllib.parse.urlsplit(normalized_url).netloc.lower()
        lowered = normalized_url.lower()
        return any(marker in host or marker in lowered for marker in PARALLEL_HLS_SEGMENT_HOST_MARKERS)

    def _parse_hls_attribute_list(self, raw_value):
        attrs = {}
        for match in re.finditer(r'([A-Z0-9-]+)=("[^"]*"|[^,]*)', str(raw_value or ""), re.IGNORECASE):
            value = match.group(2).strip()
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            attrs[match.group(1).strip().upper()] = value
        return attrs

    def _fetch_hls_text_for_parallel(self, url, headers):
        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
        with urllib.request.urlopen(urllib.request.Request(url, headers=request_headers), timeout=PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS) as resp:
            return resp.read().decode("utf-8", "ignore")

    def _resolve_parallel_hls_media_playlist(self, url, headers):
        playlist_url = _normalize_download_url(url)
        playlist_text = self._fetch_hls_text_for_parallel(playlist_url, headers)
        lines = [line.strip() for line in playlist_text.splitlines() if line.strip()]
        variants = []
        for index, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF"):
                continue
            attrs = self._parse_hls_attribute_list(line.split(":", 1)[1] if ":" in line else "")
            for next_line in lines[index + 1:]:
                if next_line.startswith("#"):
                    continue
                media_url = urllib.parse.urljoin(playlist_url, next_line)
                try:
                    bandwidth = int(attrs.get("AVERAGE-BANDWIDTH") or attrs.get("BANDWIDTH") or 0)
                except Exception:
                    bandwidth = 0
                resolution = str(attrs.get("RESOLUTION") or "")
                resolution_match = re.search(r"(\d+)\s*x\s*(\d+)", resolution, re.IGNORECASE)
                pixel_count = 0
                if resolution_match:
                    try:
                        pixel_count = int(resolution_match.group(1) or 0) * int(resolution_match.group(2) or 0)
                    except Exception:
                        pixel_count = 0
                variants.append((bandwidth, pixel_count, media_url))
                break
        if variants:
            _bandwidth, _pixel_count, media_url = max(variants, key=lambda item: (item[0], item[1]))
            return media_url, self._fetch_hls_text_for_parallel(media_url, headers)
        return playlist_url, playlist_text

    def _parse_parallel_hls_segments(self, playlist_url, playlist_text):
        lines = [line.strip() for line in str(playlist_text or "").splitlines() if line.strip()]
        if not any(line.startswith("#EXT-X-ENDLIST") for line in lines):
            raise Exception("parallel HLS only supports complete VOD playlists")
        active_key = None
        sequence_number = 0
        pending_duration = 0.0
        segments = []
        for line in lines:
            if line.startswith("#EXT-X-MEDIA-SEQUENCE"):
                try:
                    sequence_number = int(line.split(":", 1)[1].strip())
                except Exception:
                    sequence_number = 0
            elif line.startswith("#EXT-X-KEY"):
                attrs = self._parse_hls_attribute_list(line.split(":", 1)[1] if ":" in line else "")
                method = str(attrs.get("METHOD", "") or "").upper()
                if method and method != "AES-128":
                    raise Exception(f"parallel HLS unsupported key method: {method}")
                key_uri = attrs.get("URI", "")
                active_key = {
                    "method": method,
                    "uri": urllib.parse.urljoin(playlist_url, key_uri) if key_uri else "",
                    "iv": attrs.get("IV", ""),
                }
            elif line.startswith("#EXTINF"):
                try:
                    pending_duration = float(line.split(":", 1)[1].split(",", 1)[0].strip())
                except Exception:
                    pending_duration = 0.0
            elif not line.startswith("#"):
                segments.append({
                    "index": len(segments),
                    "sequence": sequence_number + len(segments),
                    "url": urllib.parse.urljoin(playlist_url, line),
                    "duration": pending_duration,
                    "key": dict(active_key or {}),
                })
                pending_duration = 0.0
        if not segments:
            raise Exception("parallel HLS playlist has no media segments")
        return segments

    def _fetch_parallel_hls_keys(self, segments, headers):
        key_cache = {}
        for segment in segments:
            key_url = (segment.get("key") or {}).get("uri")
            if not key_url or key_url in key_cache:
                continue
            request_headers = dict(headers or {})
            request_headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
            with urllib.request.urlopen(urllib.request.Request(key_url, headers=request_headers), timeout=PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS) as resp:
                key_data = resp.read()
            if len(key_data) != 16:
                raise Exception("parallel HLS AES key has invalid length")
            key_cache[key_url] = key_data
        return key_cache

    def _is_valid_parallel_hls_segment_data(self, data, content_type=""):
        if not data or len(data) < 16:
            return False
        lowered_type = str(content_type or "").lower()
        if self._is_unsupported_parallel_hls_segment_payload(data, lowered_type):
            return False
        if data[0] == 0x47:
            return True
        if data[4:8] in (b"ftyp", b"moof", b"mdat", b"styp"):
            return True
        prefix = data[:128].lstrip()
        return "video/" in lowered_type or "application/octet-stream" in lowered_type

    def _is_unsupported_parallel_hls_segment_payload(self, data, content_type=""):
        lowered_type = str(content_type or "").lower()
        if any(marker in lowered_type for marker in ("text/html", "image/", "application/json")):
            return True
        prefix = (data or b"")[:128].lstrip()
        return prefix.startswith((b"<!DOCTYPE", b"<html", b"{", b"\x89PNG", b"GIF8", b"\xff\xd8\xff"))

    def _is_valid_parallel_hls_part_file(self, path):
        if not self._has_nonempty_file(path):
            return False
        try:
            with open(path, "rb") as part_f:
                head = part_f.read(512)
            return self._is_valid_parallel_hls_segment_data(head, content_type="application/octet-stream")
        except Exception:
            return False

    def _preflight_parallel_hls_segments(self, segments, headers):
        google_segments = [
            segment
            for segment in (segments or [])
            if "googleusercontent.com" in urllib.parse.urlsplit(_normalize_download_url(segment.get("url", "")) or "").netloc.lower()
        ]
        if not google_segments:
            return
        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
        for segment in google_segments[:3]:
            segment_url = _normalize_download_url(segment.get("url", ""))
            if not segment_url:
                continue
            try:
                req = urllib.request.Request(segment_url, headers=request_headers, method="HEAD")
                with urllib.request.urlopen(req, timeout=PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS) as resp:
                    content_type = str(resp.headers.get("Content-Type") or "").lower()
            except Exception:
                continue
            if "image/" in content_type:
                raise ParallelHlsUnsupportedSegmentContentException(
                    f"Google-backed HLS returned non-video segment content: {content_type}"
                )

    def _probe_unsupported_parallel_hls_segment(self, segment, headers):
        segment_url = _normalize_download_url((segment or {}).get("url", ""))
        if not segment_url:
            return False
        segment_host = urllib.parse.urlsplit(segment_url).netloc.lower()
        if "googleusercontent.com" not in segment_host:
            return False
        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
        request_headers.setdefault("Range", "bytes=0-511")
        try:
            with urllib.request.urlopen(urllib.request.Request(segment_url, headers=request_headers), timeout=PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS) as resp:
                content_type = str(resp.headers.get("Content-Type") or "").lower()
                data = resp.read(512)
        except Exception:
            return False
        return self._is_unsupported_parallel_hls_segment_payload(data, content_type)

    def _drop_unsupported_edge_parallel_hls_segments(self, segments, headers, max_probe=5):
        ordered = list(segments or [])
        if not ordered:
            return ordered, 0, 0
        start = 0
        end = len(ordered)
        probe_limit = max(int(max_probe or 0), 0)
        while start < end and start < probe_limit and self._probe_unsupported_parallel_hls_segment(ordered[start], headers):
            start += 1
        trailing_checked = 0
        while end > start and trailing_checked < probe_limit and self._probe_unsupported_parallel_hls_segment(ordered[end - 1], headers):
            end -= 1
            trailing_checked += 1
        return ordered[start:end], start, len(ordered) - end

    def _drop_missing_leading_resume_hls_segments(self, segments, part_dir, max_skip=5, min_existing_after=10):
        ordered = list(segments or [])
        if not ordered or not part_dir or not os.path.isdir(part_dir):
            return ordered, 0
        probe_limit = min(max(int(max_skip or 0), 0), len(ordered))
        required_existing = max(int(min_existing_after or 0), 1)
        for skip_count in range(1, probe_limit + 1):
            skipped = ordered[:skip_count]
            if any(
                self._is_valid_parallel_hls_part_file(os.path.join(part_dir, f"{int(segment['index']):06d}.ts"))
                for segment in skipped
            ):
                break
            following = ordered[skip_count: skip_count + required_existing]
            if len(following) < required_existing:
                break
            if all(
                self._is_valid_parallel_hls_part_file(os.path.join(part_dir, f"{int(segment['index']):06d}.ts"))
                for segment in following
            ):
                return ordered[skip_count:], skip_count
        return ordered, 0

    def _purge_invalid_parallel_hls_resume_parts(self, part_dir, sample_limit=24):
        if not part_dir or not os.path.isdir(part_dir):
            return 0
        try:
            part_files = sorted(
                os.path.join(part_dir, name)
                for name in os.listdir(part_dir)
                if name.lower().endswith(".ts")
            )
        except OSError:
            return 0
        if not part_files:
            return 0
        sampled_files = part_files[:max(int(sample_limit or 0), 1)]
        for path in sampled_files:
            try:
                with open(path, "rb") as part_f:
                    head = part_f.read(512)
            except OSError:
                continue
            if self._is_unsupported_parallel_hls_segment_payload(head, "application/octet-stream"):
                count = len(part_files)
                shutil.rmtree(part_dir, ignore_errors=True)
                return count
        return 0

    def _parallel_hls_iv_for_segment(self, key_info, sequence):
        raw_iv = str((key_info or {}).get("iv", "") or "").strip()
        if raw_iv:
            if raw_iv.lower().startswith("0x"):
                raw_iv = raw_iv[2:]
            return bytes.fromhex(raw_iv.zfill(32)[-32:])
        return int(sequence).to_bytes(16, byteorder="big", signed=False)

    def _remove_parallel_hls_aes_padding(self, data):
        if not data:
            return data
        pad_len = data[-1]
        if not (1 <= int(pad_len) <= 16):
            return data
        if len(data) < pad_len or data[-pad_len:] != bytes([pad_len]) * pad_len:
            return data
        unpadded = data[:-pad_len]
        # MPEG-TS packets are 188 bytes. Only strip AES PKCS#7 padding when it
        # restores packet alignment; otherwise preserve the decrypted bytes.
        if unpadded and len(unpadded) % 188 == 0:
            return unpadded
        return data

    def _download_parallel_hls_segment(self, segment, part_path, headers, key_cache, stop_event):
        if stop_event.is_set() or self._shutdown_started:
            raise StopDownloadException("stop requested")
        if self._is_valid_parallel_hls_part_file(part_path):
            return os.path.getsize(part_path)
        if os.path.exists(part_path):
            try:
                os.remove(part_path)
            except OSError:
                pass
        request_headers = dict(headers or {})
        request_headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
        segment_url = str(segment.get("url") or "")
        segment_host = urllib.parse.urlsplit(_normalize_download_url(segment_url) or "").netloc.lower()
        is_google_segment = "googleusercontent.com" in segment_host
        retry_count = PARALLEL_HLS_GOOGLE_SEGMENT_RETRIES if is_google_segment else PARALLEL_HLS_SEGMENT_RETRIES
        last_exc = None
        for attempt in range(retry_count):
            try:
                if stop_event.is_set() or self._shutdown_started:
                    raise StopDownloadException("stop requested")
                with urllib.request.urlopen(urllib.request.Request(segment["url"], headers=request_headers), timeout=PARALLEL_HLS_SEGMENT_TIMEOUT_SECONDS) as resp:
                    content_type = str(resp.headers.get("Content-Type") or "").lower()
                    data = resp.read()
                if not self._is_valid_parallel_hls_segment_data(data, content_type=content_type):
                    if is_google_segment and self._is_unsupported_parallel_hls_segment_payload(data, content_type):
                        raise ParallelHlsUnsupportedSegmentContentException(
                            f"Google-backed HLS returned non-video segment content: {content_type or 'unknown'}"
                        )
                    raise Exception(f"invalid HLS segment content: {content_type or 'unknown'}")
                key_info = segment.get("key") or {}
                key_url = key_info.get("uri")
                if key_url:
                    if CryptoAES is None:
                        raise Exception("parallel HLS AES dependency missing")
                    cipher = CryptoAES.new(
                        key_cache[key_url],
                        CryptoAES.MODE_CBC,
                        self._parallel_hls_iv_for_segment(key_info, segment.get("sequence", segment.get("index", 0))),
                    )
                    data = self._remove_parallel_hls_aes_padding(cipher.decrypt(data))
                temp_part_path = f"{part_path}.tmp"
                with open(temp_part_path, "wb") as out_f:
                    out_f.write(data)
                os.replace(temp_part_path, part_path)
                return len(data)
            except StopDownloadException:
                raise
            except ParallelHlsUnsupportedSegmentContentException:
                raise
            except Exception as exc:
                last_exc = exc
                if is_google_segment:
                    delay = PARALLEL_HLS_GOOGLE_RETRY_DELAYS[min(attempt, len(PARALLEL_HLS_GOOGLE_RETRY_DELAYS) - 1)]
                    time.sleep(delay)
                elif int(getattr(exc, "code", 0) or 0) == 429:
                    time.sleep(min(2.0 * (attempt + 1), 12.0))
                else:
                    time.sleep(min(0.5 * (attempt + 1), 3.0))
        raise last_exc or Exception("parallel HLS segment download failed")

    def _remux_parallel_hls_transport_stream(self, ffmpeg_path, source_ts_path, out_path):
        startupinfo = None
        creationflags = 0
        if platform.system() == "Windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(
            [
                ffmpeg_path,
                "-y",
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-fflags",
                "+genpts",
                "-i",
                source_ts_path,
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                out_path,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            startupinfo=startupinfo,
            creationflags=creationflags,
            timeout=900,
        )
        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise Exception(f"parallel HLS remux failed: {message[:240]}")

    def _try_parallel_hls_segment_download(self, item_id, url, out_path, temp_out_path, progress_path, headers, ffmpeg_path, ffmpeg_version=""):
        task = self.tasks.get(item_id, {})
        if not self._should_try_parallel_hls_segments(url, task):
            return False
        if bool(_task_field_value(task, "is_mp3", False)):
            return False
        media_url, playlist_text = self._resolve_parallel_hls_media_playlist(url, headers)
        segments = self._parse_parallel_hls_segments(media_url, playlist_text)
        original_segment_count = len(segments)
        segments, skipped_leading_segments, skipped_trailing_segments = self._drop_unsupported_edge_parallel_hls_segments(segments, headers)
        if skipped_leading_segments or skipped_trailing_segments:
            write_error_log(
                "parallel hls skipped unsupported edge segments",
                Exception("parallel HLS skipped non-video edge segments"),
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                original_segments=original_segment_count,
                skipped_leading_segments=skipped_leading_segments,
                skipped_trailing_segments=skipped_trailing_segments,
                remaining_segments=len(segments),
            )
            self._set_task_parse_ui(
                item_id,
                message=f"已跳過來源非影片片段，開始下載 {len(segments)}/{original_segment_count} 段...",
            )
        if not segments:
            raise ParallelHlsUnsupportedSegmentContentException("HLS playlist contains no usable video segments")
        part_dir = f"{os.path.splitext(temp_out_path)[0]}.segments"
        os.makedirs(part_dir, exist_ok=True)
        if _task_source_site_name(task) == "avbebe":
            purged_invalid_parts = self._purge_invalid_parallel_hls_resume_parts(part_dir)
            if purged_invalid_parts:
                os.makedirs(part_dir, exist_ok=True)
                write_error_log(
                    "parallel hls purged invalid resume segments",
                    Exception("parallel HLS purged invalid resume segments"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    purged_segments=purged_invalid_parts,
                    part_dir=part_dir,
                )
                self._set_task_parse_ui(
                    item_id,
                    message=f"已清除錯誤續傳片段 {purged_invalid_parts} 段，重新開始下載...",
                )
            segments, skipped_missing_leading_segments = self._drop_missing_leading_resume_hls_segments(segments, part_dir)
            if skipped_missing_leading_segments:
                write_error_log(
                    "parallel hls skipped missing leading resume segments",
                    Exception("parallel HLS skipped missing leading resume segments"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    original_segments=original_segment_count,
                    skipped_missing_leading_segments=skipped_missing_leading_segments,
                    remaining_segments=len(segments),
                    part_dir=part_dir,
                )
                self._set_task_parse_ui(
                    item_id,
                    message=f"續傳缺少開頭 {skipped_missing_leading_segments} 段，已跳過並開始合併...",
                )
        if not segments:
            raise ParallelHlsUnsupportedSegmentContentException("HLS playlist contains no usable resume segments")
        if any((segment.get("key") or {}).get("uri") for segment in segments) and CryptoAES is None:
            return False
        key_cache = self._fetch_parallel_hls_keys(segments, headers)
        transport_path = f"{os.path.splitext(temp_out_path)[0]}.parallel.ts"
        merged_path = f"{os.path.splitext(temp_out_path)[0]}.parallel.mp4"
        stop_event = threading.Event()
        total_segments = len(segments)
        has_google_segments = any(
            "googleusercontent.com" in urllib.parse.urlsplit(_normalize_download_url(segment.get("url", "")) or "").netloc.lower()
            for segment in segments
        )
        try:
            self._preflight_parallel_hls_segments(segments, headers)
        except ParallelHlsUnsupportedSegmentContentException as exc:
            write_error_log(
                "parallel hls unsupported segment content",
                exc,
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                segments=total_segments,
                google_segments=has_google_segments,
            )
            raise
        total_duration = sum(max(float(segment.get("duration", 0.0) or 0.0), 0.0) for segment in segments)
        completed_bytes = 0
        completed_duration = 0.0
        completed_segments = 0
        started_at = time.time()
        completed_lock = threading.Lock()
        worker_count = min(self._parallel_hls_workers_for_segments(_task_source_site_name(task), media_url, segments), total_segments)
        self._log_ffmpeg_event(
            "parallel hls download started",
            Exception("parallel hls started"),
            task,
            item_id,
            media_url,
            segments=total_segments,
            workers=worker_count,
            total_duration=total_duration,
            **self._build_ffmpeg_runtime_fields(ffmpeg_path, ffmpeg_version=ffmpeg_version),
        )

        def _part_path(segment):
            return os.path.join(part_dir, f"{int(segment['index']):06d}.ts")

        existing_segments = [segment for segment in segments if self._is_valid_parallel_hls_part_file(_part_path(segment))]
        if existing_segments:
            completed_bytes = sum(self._get_existing_file_size(_part_path(segment)) for segment in existing_segments)
            completed_duration = sum(max(float(segment.get("duration", 0.0) or 0.0), 0.0) for segment in existing_segments)
            completed_segments = len(existing_segments)
            if total_duration > 0:
                percent = min((completed_duration / total_duration) * 100.0, 99.0)
                self.update_tree_many(item_id, {
                    "progress": f"{percent:.1f}%",
                    "size": f"{completed_segments}/{total_segments}",
                    "speed_eta": f"續傳已載入 {completed_segments}/{total_segments} 段",
                }, force=True)
            self._save_resume_progress(
                progress_path,
                completed_duration,
                source_url=_normalize_download_url(url) or url,
                bytes_done=completed_bytes,
                total_bytes=completed_bytes,
            )

        def _download_one(segment):
            nonlocal completed_bytes, completed_duration, completed_segments
            task_state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            if self._is_pause_requested_state(task_state) or self._is_delete_requested_state(task_state):
                stop_event.set()
                raise StopDownloadException("stop requested")
            part_size = self._download_parallel_hls_segment(segment, _part_path(segment), headers, key_cache, stop_event)
            with completed_lock:
                completed_bytes += int(part_size or 0)
                completed_duration += max(float(segment.get("duration", 0.0) or 0.0), 0.0)
                completed_segments += 1
                elapsed = max(time.time() - started_at, 0.001)
                speed_bps = completed_bytes / elapsed
                eta = elapsed / completed_segments * (total_segments - completed_segments) if completed_segments and total_segments > completed_segments else None
                if total_duration > 0:
                    percent = min((completed_duration / total_duration) * 100.0, 99.0)
                    self.update_tree_many(item_id, {
                        "progress": f"{percent:.1f}%",
                        "size": f"{completed_segments}/{total_segments}",
                        "speed_eta": f"{format_transfer_rate(speed_bps)} | {format_eta(eta)}" if eta else format_transfer_rate(speed_bps),
                    }, force=True)
                self._save_resume_progress(
                    progress_path,
                    completed_duration,
                    source_url=_normalize_download_url(url) or url,
                    bytes_done=completed_bytes,
                    min_interval_seconds=RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS,
                    min_bytes_delta=RESUME_PROGRESS_MIN_BYTES_DELTA,
                )
            return part_size

        try:
            with DaemonThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(_download_one, segment) for segment in segments if not self._is_valid_parallel_hls_part_file(_part_path(segment))]
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            with open(transport_path, "wb") as merged_f:
                for segment in segments:
                    part_path = _part_path(segment)
                    if not self._is_valid_parallel_hls_part_file(part_path):
                        raise Exception("parallel HLS segment missing after download")
                    with open(part_path, "rb") as part_f:
                        shutil.copyfileobj(part_f, merged_f, length=HTTP_FILE_COPY_CHUNK_SIZE)
            self._remux_parallel_hls_transport_stream(ffmpeg_path, transport_path, merged_path)
            final_info = self._probe_media_info(merged_path)
            final_duration = float(final_info.get("duration", 0.0) or 0.0)
            if not final_info.get("valid") or int(final_info.get("size", 0) or 0) <= 0:
                raise Exception("parallel HLS remux produced invalid output")
            if total_duration > 300.0 and final_duration > 0.0 and final_duration + max(60.0, total_duration * 0.02) < total_duration:
                raise Exception(f"parallel HLS output duration mismatch: duration={final_duration:.3f} expected={total_duration:.3f}")
            if total_duration > 300.0 and final_duration <= 0.0:
                raise Exception(f"parallel HLS output duration missing: expected={total_duration:.3f}")
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with self._resume_artifact_lock_for(merged_path, out_path):
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                self._move_file_with_retry(merged_path, out_path, attempts=48, delay_seconds=0.5)
            self._remove_artifact_paths(temp_out_path, transport_path, progress_path)
            shutil.rmtree(part_dir, ignore_errors=True)
            self._set_task_named_column_text(item_id, "progress", "100%")
            self._mark_task_finished(item_id)
            self._log_ffmpeg_event(
                "parallel hls download finished",
                Exception("parallel hls finished"),
                task,
                item_id,
                media_url,
                output=out_path,
                bytes=self._get_existing_file_size(out_path),
                segments=total_segments,
                workers=worker_count,
            )
            return True
        except StopDownloadException:
            stop_event.set()
            raise
        except Exception as exc:
            stop_event.set()
            log_title = "parallel hls google retry later" if has_google_segments else "parallel hls fallback to ffmpeg"
            write_error_log(
                log_title,
                exc,
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                segments=total_segments,
                workers=worker_count,
                google_segments=has_google_segments,
            )
            if has_google_segments:
                raise ParallelHlsRetryLaterException(str(exc)[:240] or "Google-backed HLS segments are temporarily rate-limited")
            return False

    def _download_m3u8_with_ytdlp_native(self, item_id, url, save_dir, is_mp3=False, referer="https://www.movieffm.net/", origin="https://www.movieffm.net"):
        yt_dlp_module = get_yt_dlp_module()
        if yt_dlp_module is None:
            raise RuntimeError("yt-dlp module is not available")
        task = self.tasks.get(item_id, {})
        source_site = _task_source_site_name(task)
        if source_site == "gimy":
            referer = self._get_task_source_page(task, fallback_url=referer) or referer
            parsed_ref = urllib.parse.urlsplit(referer)
            if parsed_ref.scheme and parsed_ref.netloc:
                origin = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
        temp_out_path, resume_key, resume_keys = self._resolve_resume_artifact_base(
            task,
            url,
            ext="mp3" if is_mp3 else "mp4",
            save_dir=save_dir,
            fallback_name="Audio" if is_mp3 else "Video",
        )
        temp_root, temp_ext = os.path.splitext(temp_out_path)
        resume_out_path = f"{temp_root}.resume{temp_ext}"
        merged_out_path = f"{temp_root}.merged{temp_ext}"
        progress_path = temp_out_path + ".progress.json"
        self._cache_task_resolved_link(
            task,
            url,
            fallback_urls=_dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url),
            page_refresh_candidates=_task_gimy_page_refresh_candidates(task) if _task_source_site_name(task) == "gimy" else None,
        )
        source_site = _task_source_site_name(task)
        native_options = {
            "socket_timeout": float(
                YTDLP_HLS_NATIVE_SOCKET_TIMEOUT_BY_SITE.get(
                    source_site,
                    YTDLP_HLS_NATIVE_SOCKET_TIMEOUT,
                )
            ),
            "concurrent_fragment_downloads": _ytdlp_native_concurrency_for_site(source_site),
            "hls_use_mpegts": bool(YTDLP_HLS_NATIVE_USE_MPEGTS_BY_SITE.get(source_site, True)),
        }
        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "Video" or "").strip()
        safe_name = _safe_output_stem(name, fallback="Video")
        ext = "mp3" if is_mp3 else "mp4"
        out_path = os.path.join(save_dir, f"{safe_name}.{ext}")
        if self._set_output_path_and_complete_if_exists(task, item_id, out_path):
            return

        progress_hook = self._make_yt_dlp_progress_hook(task, item_id, save_dir)

        native_work_dir = tempfile.gettempdir()
        ydl_opts = _build_ytdlp_download_opts(
            progress_hook=progress_hook,
            home_dir=native_work_dir,
            temp_dir=native_work_dir,
            outtmpl=f"{os.path.splitext(temp_out_path)[0]}.%(ext)s",
            concurrent_fragment_downloads=native_options["concurrent_fragment_downloads"],
            format_selector="best",
            http_headers=_make_hls_http_headers(referer=referer, origin=origin),
            hls_prefer_native=True,
            hls_use_mpegts=native_options["hls_use_mpegts"],
            socket_timeout=native_options["socket_timeout"],
            throttled_rate_bps=_ytdlp_throttled_rate_for_site(source_site),
        )
        if is_mp3:
            _apply_ytdlp_audio_postprocessing(ydl_opts)
        write_error_log(
            "yt-dlp native hls fallback started",
            Exception("yt-dlp native hls fallback started"),
            url=url,
            item_id=item_id,
            source_site=_task_source_site_name(task) or None,
            referer=referer,
            origin=origin,
            socket_timeout=native_options["socket_timeout"],
            concurrent_fragment_downloads=ydl_opts["concurrent_fragment_downloads"],
            throttled_rate_bps=ydl_opts.get("throttledratelimit"),
            hls_use_mpegts=ydl_opts["hls_use_mpegts"],
        )
        info = None
        pre_existing_output = self._has_nonempty_file(out_path)
        try:
            with yt_dlp_module.YoutubeDL(ydl_opts) as ydl:
                if (
                    _looks_like_manifest_url(url)
                    and _task_source_site_name(task) in YTDLP_DIRECT_MANIFEST_EXTRACT_SITES
                    and _task_source_site_name(task) != "missav"
                ):
                    info = ydl.extract_info(url, download=True)
                elif _looks_like_manifest_url(url):
                    protocol = "m3u8_native" if ".m3u8" in urllib.parse.urlsplit(url).path.lower() else "http_dash_segments"
                    direct_info = {
                        "id": safe_name,
                        "title": safe_name,
                        "url": url,
                        "ext": "mp4" if not is_mp3 else "m4a",
                        "protocol": protocol,
                        "http_headers": dict(ydl_opts.get("http_headers") or {}),
                        "webpage_url": referer,
                        "original_url": url,
                    }
                    info = ydl.process_ie_result(direct_info, download=True)
                else:
                    info = ydl.extract_info(url, download=True)
        except Exception:
            self._remove_yt_dlp_fragment_artifacts(os.path.splitext(temp_out_path)[0])
            raise
        actual_output = self._wait_for_yt_dlp_output_path(info, native_work_dir, safe_name, out_path, preferred_ext=ext)
        if actual_output:
            if os.path.normcase(os.path.abspath(actual_output)) != os.path.normcase(os.path.abspath(out_path)):
                try:
                    if not os.path.exists(out_path):
                        self._move_file_with_retry(actual_output, out_path)
                        actual_output = out_path
                except Exception:
                    pass
            _set_task_aux_fields(task, filename=actual_output)
            self._set_task_named_column_text(item_id, "name", os.path.basename(str(actual_output or "").strip()) or "")
        final_output_path = self._task_output_path_or_default(task, out_path)
        if self._has_nonempty_file(final_output_path) and not is_mp3:
            final_info = self._probe_media_info(final_output_path)
            final_size = int(final_info.get("size", 0) or 0)
            final_duration = float(final_info.get("duration", 0.0) or 0.0)
            expected_duration = 0.0
            try:
                if (
                    _task_source_site_name(task) == "movieffm"
                    or final_size < 64 * 1024 * 1024
                    or final_duration <= 0.0
                ):
                    expected_duration = self._get_m3u8_duration(
                        url,
                        headers=_make_hls_http_headers(referer=referer, origin=origin),
                    )
            except Exception:
                expected_duration = 0.0
            if self._is_incomplete_hls_video_artifact(task, final_output_path, expected_duration=expected_duration):
                artifact_exc = Exception(
                    f"{_task_source_site_name(task) or 'hls'} native hls artifact rejected: size={final_size} duration={final_duration:.3f} expected={expected_duration:.3f}"
                )
                write_error_log(
                    "native hls artifact rejected",
                    artifact_exc,
                    url=url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    output=final_output_path,
                    size=final_size,
                    duration=final_duration,
                    expected_duration=expected_duration,
                    pre_existing_output=pre_existing_output,
                )
                if pre_existing_output:
                    self._remove_artifact_paths(temp_out_path, resume_out_path, merged_out_path, progress_path)
                else:
                    self._remove_artifact_paths(final_output_path, temp_out_path, resume_out_path, merged_out_path, progress_path)
                raise artifact_exc
        if self._has_nonempty_file(final_output_path):
            self._mark_task_finished(item_id)
            self._log_ytdlp_native_hls_finished(task, item_id, url, final_output_path)
            return
        if self._complete_if_output_exists(item_id):
            self._log_ytdlp_native_hls_finished(task, item_id, url, out_path)
            return
        write_error_log(
            "yt-dlp native hls fallback output missing",
            Exception("yt-dlp native hls fallback output missing"),
            url=url,
            item_id=item_id,
            source_site=_task_source_site_name(task) or None,
            expected_output=out_path,
            detected_candidates=self._collect_yt_dlp_output_candidates(info, native_work_dir, safe_name, preferred_ext=ext),
            save_dir=save_dir,
            temp_dir=native_work_dir,
        )
        raise FileNotFoundError("yt-dlp native HLS fallback did not produce an output file")

    def _download_m3u8_with_ffmpeg(self, item_id, url, save_dir, is_mp3=False, referer="https://www.movieffm.net/", origin="https://www.movieffm.net", _unexpected_retry_count=0, _native_fallback_done=False):
        task = self.tasks.get(item_id, {})
        source_site = _task_source_site_name(task)
        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "Video" or "").strip()
        safe_name = _safe_output_stem(name, fallback="Video")
        ext = "mp3" if is_mp3 else "mp4"
        out_path = os.path.join(save_dir, f"{safe_name}.{ext}")
        if self._set_output_path_and_complete_if_exists(task, item_id, out_path):
            return
        temp_out_path, resume_key, resume_keys = self._resolve_resume_artifact_base(
            task,
            url,
            ext=ext,
            save_dir=save_dir,
            fallback_name="Audio" if is_mp3 else "Video",
        )
        temp_root, temp_ext = os.path.splitext(temp_out_path)
        resume_out_path = f"{temp_root}.resume{temp_ext}"
        merged_out_path = f"{temp_root}.merged{temp_ext}"
        progress_path = temp_out_path + ".progress.json"
        _set_task_aux_fields(task, temp_filename=temp_out_path)

        ffmpeg_path = os.path.join(_APP_DIR, "ffmpeg.exe") if platform.system() == "Windows" else shutil.which("ffmpeg") or "ffmpeg"
        if not os.path.exists(ffmpeg_path) and ffmpeg_path == os.path.join(_APP_DIR, "ffmpeg.exe"):
            raise FileNotFoundError("ffmpeg.exe was not found in the application directory")
        clean_ffmpeg_path = str(ffmpeg_path or "").strip()
        ffmpeg_version = ""
        if clean_ffmpeg_path:
            cached_ffmpeg_version = _FFMPEG_VERSION_SUMMARY_CACHE.get(clean_ffmpeg_path)
            if cached_ffmpeg_version is not None:
                ffmpeg_version = cached_ffmpeg_version
            else:
                try:
                    startupinfo = None
                    creationflags = 0
                    if platform.system() == "Windows":
                        startupinfo = subprocess.STARTUPINFO()
                        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                        startupinfo.wShowWindow = 0
                        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    version_proc = subprocess.run(
                        [clean_ffmpeg_path, "-version"],
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="ignore",
                        timeout=15,
                        startupinfo=startupinfo,
                        creationflags=creationflags,
                    )
                    for line in (version_proc.stdout or "").splitlines():
                        ffmpeg_version = line.strip()[:160]
                        if ffmpeg_version:
                            break
                except Exception:
                    ffmpeg_version = ""
                _FFMPEG_VERSION_SUMMARY_CACHE[clean_ffmpeg_path] = ffmpeg_version

        raw_fallback_urls = _dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url)
        direct_fallback_urls = [
            candidate
            for candidate in raw_fallback_urls
            if any(
                token in _normalize_download_url(candidate).lower()
                for token in (".m3u8", ".mpd", ".mp4", ".mkv", ".webm", ".mp3", ".m4a")
            )
        ]
        page_refresh_candidates = _task_gimy_page_refresh_candidates(task) if source_site == "gimy" else [candidate for candidate in raw_fallback_urls if candidate not in direct_fallback_urls]
        gimy_failed_stream_urls = _task_gimy_failed_stream_urls(task) if source_site == "gimy" else []
        gimy_failed_stream_hosts = _task_gimy_failed_stream_hosts(task) if source_site == "gimy" else []
        detail_refresh_done = bool(_task_field_value(task, "_gimy_detail_refresh_done", False))
        if source_site == "gimy" and len(direct_fallback_urls) > GIMY_DIRECT_STREAM_FALLBACK_LIMIT:
            direct_fallback_urls = direct_fallback_urls[:GIMY_DIRECT_STREAM_FALLBACK_LIMIT]
        if gimy_failed_stream_urls:
            direct_fallback_urls = [
                candidate for candidate in direct_fallback_urls
                if _normalize_download_url(candidate) not in gimy_failed_stream_urls
            ]
        if gimy_failed_stream_hosts:
            direct_fallback_urls = [
                candidate for candidate in direct_fallback_urls
                if urllib.parse.urlsplit(_normalize_download_url(candidate) or "").netloc.lower() not in gimy_failed_stream_hosts
            ]
        candidate_urls = _dedupe_download_urls([url] + direct_fallback_urls)
        failed_primary_candidate_url = ""

        def _remember_gimy_failed_stream(stream_url):
            if source_site != "gimy":
                return
            normalized_stream_url = _normalize_download_url(stream_url)
            if not normalized_stream_url:
                return
            failed_stream_urls = list(_task_gimy_failed_stream_urls(task))
            if normalized_stream_url not in failed_stream_urls:
                failed_stream_urls.append(normalized_stream_url)
                _set_task_aux_fields(task, _gimy_failed_stream_urls=failed_stream_urls)
            failed_stream_host = urllib.parse.urlsplit(normalized_stream_url).netloc.lower()
            if failed_stream_host:
                failed_stream_hosts = list(_task_gimy_failed_stream_hosts(task))
                if failed_stream_host not in failed_stream_hosts:
                    failed_stream_hosts.append(failed_stream_host)
                    _set_task_aux_fields(task, _gimy_failed_stream_hosts=failed_stream_hosts)

        def _gimy_detail_rebuild_after_stream_failure(refresh_history, source_page_url, exc_obj):
            if source_site != "gimy" or detail_refresh_done:
                return None
            detail_page_candidates = []
            for page_url in (
                source_page_url,
                _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url),
                url,
            ):
                normalized_page_url = _normalize_download_url(page_url)
                if not normalized_page_url:
                    continue
                parsed = urllib.parse.urlsplit(normalized_page_url)
                base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'gimy01.tv'}"
                path = parsed.path or ""
                for pattern in (r"/eps/(\d+)-\d+(?:-\d+)?\.html", r"/(?:play|vodplay|video)/(\d+)-\d+(?:-\d+)?\.html"):
                    match = re.search(pattern, path)
                    if not match:
                        continue
                    vod_id = match.group(1)
                    for relative_path in (f"/vod/{vod_id}.html", f"/detail/{vod_id}.html", f"/voddetail/{vod_id}.html"):
                        normalized_candidate = _normalize_download_url(urllib.parse.urljoin(base, relative_path))
                        if normalized_candidate and normalized_candidate not in detail_page_candidates:
                            detail_page_candidates.append(normalized_candidate)
            if not detail_page_candidates:
                return None
            detail_refresh_url = detail_page_candidates[0]
            detail_refresh_history = list(_task_gimy_refresh_history(task))
            for attempted_page in (
                _normalize_download_url(_task_field_value(task, "url", "")) or "",
                source_page_url,
            ):
                normalized_attempted_page = _normalize_download_url(attempted_page)
                if (
                    normalized_attempted_page
                    and re.search(r"/(?:eps|play|vodplay|video)/", urllib.parse.urlsplit(normalized_attempted_page).path, re.IGNORECASE)
                    and normalized_attempted_page not in detail_refresh_history
                ):
                    detail_refresh_history.append(normalized_attempted_page)
            if detail_refresh_url not in detail_refresh_history:
                detail_refresh_history.append(detail_refresh_url)
            _set_task_aux_fields(
                task,
                _gimy_detail_refresh_done=True,
                _gimy_refresh_history=detail_refresh_history,
                _gimy_source_refresh_history=[],
            )
            self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在重建 Gimy 播放線...")
            write_error_log(
                "gimy detail page rebuild",
                Exception("rebuilding gimy episode sources from detail page"),
                item_id=item_id,
                stream_url=url,
                refresh_url=detail_refresh_url,
                refresh_attempts=len(refresh_history) + 1,
                fallback_count=len(raw_fallback_urls),
                original_exception=repr(exc_obj) if exc_obj is not None else None,
            )
            return self._download_task_internal(
                detail_refresh_url,
                item_id,
                save_dir,
                self._should_use_impersonation(_normalize_download_url(_task_field_value(task, "url", "")) or "", _task_source_site_name(task)),
                is_mp3,
            )

        def _gimy_refresh_after_stream_failure(exc_obj):
            current_page_url = _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url)
            source_page_url = self._get_task_source_page(task, fallback_url=current_page_url)
            raw_refresh_history = _task_field_value(task, "_gimy_source_refresh_history", [])
            if isinstance(raw_refresh_history, str):
                raw_refresh_history = [raw_refresh_history]
            refresh_history = []
            for candidate in raw_refresh_history or []:
                normalized_candidate = _normalize_download_url(candidate)
                if normalized_candidate and normalized_candidate not in refresh_history:
                    refresh_history.append(normalized_candidate)
            current_candidate_urls = [candidate for candidate in (current_page_url, source_page_url) if candidate]
            for current_candidate_url in current_candidate_urls:
                normalized_current_candidate = _normalize_download_url(current_candidate_url)
                if normalized_current_candidate and normalized_current_candidate not in refresh_history:
                    refresh_history.append(normalized_current_candidate)

            parse_refresh_history = _task_gimy_refresh_history(task)
            available_refresh_candidates = []
            for candidate in page_refresh_candidates:
                normalized_candidate = _normalize_download_url(candidate)
                if (
                    normalized_candidate
                    and normalized_candidate not in refresh_history
                    and normalized_candidate not in parse_refresh_history
                ):
                    available_refresh_candidates.append(normalized_candidate)

            refresh_url = next((candidate for candidate in available_refresh_candidates if candidate), "")
            if not refresh_url:
                normalized_source_page = _normalize_download_url(source_page_url)
                normalized_current_page = _normalize_download_url(current_page_url)
                same_page_refresh_attempts = refresh_history.count(normalized_source_page) if normalized_source_page else 0
                if normalized_source_page and (
                    normalized_source_page not in refresh_history
                    or (
                        not available_refresh_candidates
                        and normalized_source_page != normalized_current_page
                        and same_page_refresh_attempts < GIMY_SOURCE_PAGE_REFRESH_LIMIT
                    )
                    or (
                        not available_refresh_candidates
                        and normalized_source_page == normalized_current_page
                        and same_page_refresh_attempts < GIMY_SAME_PAGE_SOURCE_REFRESH_LIMIT
                    )
                ):
                    refresh_url = normalized_source_page

            _set_task_aux_fields(task, resolved_url="", resolved_url_saved_at=0.0)
            self._update_task_state_entry(task, resolved_url="", resolved_url_saved_at=0.0)

            if refresh_url and refresh_url != _normalize_download_url(url) and len(refresh_history) < GIMY_SOURCE_PAGE_REFRESH_LIMIT:
                gimy_parse_history = _task_gimy_refresh_history(task)
                if (
                    refresh_url
                    and "/eps/" in urllib.parse.urlsplit(_normalize_download_url(refresh_url)).path.lower()
                    and refresh_url not in gimy_parse_history
                ):
                    gimy_parse_history = gimy_parse_history + [refresh_url]
                _set_task_aux_fields(
                    task,
                    _gimy_source_refresh_history=refresh_history + [refresh_url],
                    _gimy_refresh_history=gimy_parse_history,
                    _gimy_detail_refresh_done=False,
                )
                self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在重新取得 Gimy 串流...")
                write_error_log(
                    "gimy source page refresh",
                    exc_obj,
                    item_id=item_id,
                    stream_url=url,
                    refresh_url=refresh_url,
                    fallback_count=len(raw_fallback_urls),
                    refresh_attempts=len(refresh_history) + 1,
                )
                return self._download_task_internal(
                    refresh_url,
                    item_id,
                    save_dir,
                    self._should_use_impersonation(_normalize_download_url(_task_field_value(task, "url", "")) or "", _task_source_site_name(task)),
                    is_mp3,
                )
            if source_site == "gimy" and not available_refresh_candidates and not refresh_url:
                detail_rebuild = _gimy_detail_rebuild_after_stream_failure(refresh_history, source_page_url, exc_obj)
                if detail_rebuild is not None:
                    return detail_rebuild
            return None

        if _should_prefer_native_hls(url, task) and source_site != "missav" and not _native_fallback_done:
            write_error_log(
                "preferred native hls route selected",
                Exception("preferred native hls route selected"),
                url=url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                referer=referer,
                origin=origin,
            )
            try:
                return self._download_m3u8_with_ytdlp_native(
                    item_id,
                    url,
                    save_dir,
                    is_mp3=is_mp3,
                    referer=referer,
                    origin=origin,
                )
            except ResumeLowSpeedReanalysisException:
                raise
            except Exception as native_exc:
                _remember_gimy_failed_stream(url)
                if source_site == "gimy":
                    if _is_gimy_stream_refreshable_error(native_exc):
                        refreshed = _gimy_refresh_after_stream_failure(native_exc)
                        if refreshed is not None:
                            return refreshed
                    failed_primary_candidate_url = _normalize_download_url(url)
                    write_error_log(
                        "gimy native hls handoff to ffmpeg",
                        native_exc,
                        url=url,
                        item_id=item_id,
                        source_site=source_site,
                    )
                    _native_fallback_done = True
                else:
                    write_error_log(
                        "native hls handoff to ffmpeg",
                        native_exc,
                        url=url,
                        item_id=item_id,
                        source_site=source_site,
                    )
                    _native_fallback_done = True

        duration_box = {}
        media_bps_box = {}
        total_bytes_box = {}
        manifest_headers = _make_hls_http_headers(referer=referer, origin=origin)

        def probe_metadata():
            for candidate in candidate_urls:
                try:
                    duration = self._get_m3u8_duration(candidate, headers=manifest_headers)
                    if duration > 0:
                        duration_box["value"] = duration
                except Exception:
                    pass
                if _task_source_site_name(task) not in M3U8_EXACT_TOTAL_BYTES_DISABLED_SITES:
                    try:
                        total_bytes = self._get_m3u8_total_bytes(candidate, headers=manifest_headers, task=task)
                        if total_bytes and total_bytes > 0:
                            total_bytes_box["value"] = total_bytes
                    except Exception:
                        pass
                try:
                    media_bps = self._estimate_m3u8_media_bps(candidate, headers=manifest_headers)
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
        headers = _format_ffmpeg_header_lines(manifest_headers)
        total_duration = 0.0
        self._start_daemon_thread(probe_metadata)

        if not is_mp3 and self._should_try_parallel_hls_segments(url, task):
            try:
                parallel_finished = self._try_parallel_hls_segment_download(
                    item_id,
                    url,
                    out_path,
                    temp_out_path,
                    progress_path,
                    manifest_headers,
                    ffmpeg_path,
                    ffmpeg_version=ffmpeg_version,
                )
            except (
                StopDownloadException,
                KeyboardInterrupt,
                ResumeLowSpeedReanalysisException,
                ParallelHlsRetryLaterException,
                ParallelHlsUnsupportedSegmentContentException,
            ):
                raise
            except Exception as exc:
                parallel_finished = False
                write_error_log(
                    "parallel hls setup fallback to ffmpeg",
                    exc,
                    url=url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                )
            if parallel_finished:
                return

        ffmpeg_candidate_urls = list(candidate_urls)
        skip_ffmpeg_same_failed_primary = False
        if failed_primary_candidate_url:
            alternate_candidates = [
                candidate for candidate in ffmpeg_candidate_urls
                if _normalize_download_url(candidate) != failed_primary_candidate_url
            ]
            failed_candidates = [
                candidate for candidate in ffmpeg_candidate_urls
                if _normalize_download_url(candidate) == failed_primary_candidate_url
            ]
            if alternate_candidates:
                ffmpeg_candidate_urls = alternate_candidates + failed_candidates
            elif source_site == "gimy":
                skip_ffmpeg_same_failed_primary = True

        for candidate_url in ffmpeg_candidate_urls:
            if skip_ffmpeg_same_failed_primary and _normalize_download_url(candidate_url) == failed_primary_candidate_url:
                continue
            self._promote_resume_artifact(temp_out_path, resume_out_path)
            stored_info = self._load_resume_progress_info(progress_path)
            partial_info = self._probe_media_info(temp_out_path)
            partial_size = partial_info.get("size", 0)
            partial_duration = partial_info.get("duration", 0.0)
            stored_bytes = int(stored_info.get("bytes", 0) or 0)
            stored_seconds = float(stored_info.get("seconds", 0.0) or 0.0)
            stored_source_url = _normalize_download_url(stored_info.get("source_url", ""))
            current_resume_key = _normalize_download_url(resume_key)
            same_resume_target = self._resume_progress_matches(stored_info, resume_keys, progress_path)
            partial_reason = partial_info.get("reason")
            partial_valid = bool(partial_info.get("valid"))
            size_consistent = partial_size > 0 and stored_bytes > 0 and abs(partial_size - stored_bytes) <= max(1024 * 1024, int(max(partial_size, stored_bytes) * 0.35))
            usable_sidecar_only_partial = partial_size > 0 and same_resume_target and (partial_valid or partial_reason == "ffprobe-error")
            if partial_valid and partial_duration > 0:
                base_bytes = partial_size
                resume_seconds = partial_duration
                resume_mode = "media_probe"
                if same_resume_target and self._should_keep_stored_resume_seconds(stored_seconds, resume_seconds, stored_bytes, partial_size):
                    resume_seconds = stored_seconds
                    resume_mode = "media_probe_persisted"
                elif same_resume_target and stored_seconds > resume_seconds:
                    self._save_resume_progress(
                        progress_path,
                        partial_duration,
                        source_url=resume_key,
                        bytes_done=partial_size,
                    )
            elif usable_sidecar_only_partial:
                base_bytes = partial_size
                resume_seconds = stored_seconds
                resume_mode = "sidecar_only"
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
                    if same_resume_target and partial_valid and partial_duration > 0:
                        base_bytes = partial_size
                        resume_seconds = partial_duration
                        resume_mode = "media_probe_recovered"
                        self._save_resume_progress(
                            progress_path,
                            partial_duration,
                            source_url=resume_key,
                            bytes_done=partial_size,
                        )
                    else:
                        base_bytes, resume_seconds = self._reset_resume_artifacts(temp_out_path, resume_out_path, merged_out_path, progress_path)
                        resume_mode = "reset"
                else:
                    base_bytes, resume_seconds = self._reset_resume_artifacts(temp_out_path, resume_out_path, merged_out_path, progress_path)
                    resume_mode = "reset"
            if duration_box.get("value"):
                total_duration = duration_box.get("value", 0.0)
            elif total_duration <= 0:
                try:
                    total_duration = self._get_m3u8_duration(candidate_url, headers=manifest_headers)
                except Exception:
                    total_duration = 0.0
            total_bytes = total_bytes_box.get("value")
            if total_duration > 0 and total_bytes and base_bytes > 0:
                estimated_resume_seconds = self._estimate_resume_seconds_from_bytes(base_bytes, total_bytes, total_duration)
                if estimated_resume_seconds > 0:
                    if resume_seconds <= 0:
                        resume_seconds = estimated_resume_seconds
                    elif resume_mode == "sidecar_only":
                        allowed_drift = max(30.0, total_duration * 0.05)
                        if abs(resume_seconds - estimated_resume_seconds) > allowed_drift:
                            resume_seconds = min(resume_seconds, estimated_resume_seconds)
            if total_duration > 0 and resume_seconds > 0:
                resume_seconds = self._sanitize_resume_seconds(resume_seconds, total_duration)
            resume_anchor_seconds = resume_seconds
            resume_seek_seconds = resume_anchor_seconds
            resume_overlap_seconds = 0.0
            if resume_anchor_seconds > 0:
                rewind_seconds = self._get_m3u8_resume_rewind_seconds(
                    candidate_url,
                    headers=manifest_headers,
                )
                if rewind_seconds > 0:
                    resume_seek_seconds = max(resume_anchor_seconds - rewind_seconds, 0.0)
                    resume_overlap_seconds = max(resume_anchor_seconds - resume_seek_seconds, 0.0)
            average_media_bps = 0.0
            if base_bytes > 0 and resume_seconds > 0:
                average_media_bps = base_bytes / resume_seconds
            if average_media_bps <= 0 and media_bps_box.get("value"):
                average_media_bps = media_bps_box.get("value", 0.0)
            if average_media_bps <= 0:
                average_media_bps = self._estimate_m3u8_media_bps(candidate_url, headers=manifest_headers) or 0.0
            resume_overlap_bytes = 0
            if average_media_bps > 0 and resume_overlap_seconds > 0:
                resume_overlap_bytes = max(int(average_media_bps * resume_overlap_seconds), 0)

            active_output_path = resume_out_path if resume_anchor_seconds > 0 else temp_out_path
            self._log_ffmpeg_event(
                "ffmpeg download started",
                Exception("ffmpeg started"),
                task,
                item_id,
                candidate_url,
                resume_seconds=resume_anchor_seconds,
                resume_seek_seconds=resume_seek_seconds,
                resume_overlap_seconds=resume_overlap_seconds,
                base_bytes=base_bytes,
                stored_bytes=stored_info.get("bytes", 0),
                stored_progress=stored_info.get("seconds", 0.0),
                total_duration=total_duration,
                total_bytes=total_bytes,
                average_media_bps=average_media_bps,
                referer=referer,
                origin=origin,
                **self._build_ffmpeg_runtime_fields(
                    ffmpeg_path,
                    retry_count=_unexpected_retry_count,
                    ffmpeg_version=ffmpeg_version,
                ),
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
                "-user_agent",
                DEFAULT_USER_AGENT,
            ]
            use_fast_hls_http = _task_source_site_name(task) in FFMPEG_FAST_HLS_HTTP_SITES
            cmd += [
                "-protocol_whitelist",
                "file,http,https,tcp,tls,crypto,data",
                "-allowed_extensions",
                "ALL",
                "-allowed_segment_extensions",
                "ALL",
                "-rw_timeout",
                FFMPEG_HLS_RW_TIMEOUT_MICROSECONDS,
                "-http_persistent",
                "1" if use_fast_hls_http else "0",
                "-headers",
                headers,
            ]
            if use_fast_hls_http:
                cmd += [
                    "-http_multiple",
                    "1",
                    "-http_seekable",
                    "0",
                    "-multiple_requests",
                    "1",
                    "-seg_max_retry",
                    "3",
                ]
            if referer:
                cmd += ["-referer", referer]
            if use_fast_hls_http:
                cmd += [
                    "-extension_picky",
                    "0",
                ]
            cmd += list(FFMPEG_HLS_RECONNECT_OPTIONS)
            if resume_seek_seconds > 0:
                cmd += ["-ss", f"{resume_seek_seconds:.3f}"]
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
                _set_task_aux_fields(self.tasks[item_id], _proc=proc)
            progress = {}
            recent_lines = []
            last_ui_update = 0.0
            last_io_poll = 0.0
            last_progress_ui_bytes = -1
            last_checkpoint_at = 0.0
            last_checkpoint_bytes = -1
            cached_active_output_bytes = 0
            invalidated_total_bytes = False
            near_complete_since = None
            active_total_bytes = total_bytes_box.get("value") or total_bytes

            def poll_active_output_bytes(now=None, force=False):
                nonlocal last_io_poll, cached_active_output_bytes, active_total_bytes
                now = time.time() if now is None else now
                if not force and (now - last_io_poll) < FFMPEG_PROGRESS_IO_POLL_INTERVAL_SECONDS:
                    return cached_active_output_bytes
                try:
                    cached_active_output_bytes = self._get_existing_file_size(active_output_path) if os.path.exists(active_output_path) else 0
                except OSError:
                    cached_active_output_bytes = 0
                required_bytes = None
                if active_total_bytes:
                    required_bytes = max(int(active_total_bytes) - int(base_bytes + cached_active_output_bytes), 0)
                if self._maybe_auto_pause_for_disk_space(item_id, active_output_path, required_bytes=required_bytes, note=t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"):
                    self._terminate_ffmpeg_process(
                        self.tasks.get(item_id, {}),
                        item_id,
                        proc,
                        candidate_url,
                        "disk_full_auto_pause",
                        required_bytes=required_bytes,
                        bytes=base_bytes + cached_active_output_bytes,
                    )
                    raise StopDownloadException("disk space low")
                last_io_poll = now
                return cached_active_output_bytes

            try:
                assert proc.stdout is not None
                for raw_line in proc.stdout:
                    state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
                    if state == "PAUSE_REQUESTED":
                        self._terminate_ffmpeg_process(self.tasks.get(item_id, {}), item_id, proc, candidate_url, "pause_requested")
                        _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                        self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                        return
                    if state == "DELETE_REQUESTED":
                        self._terminate_ffmpeg_process(self.tasks.get(item_id, {}), item_id, proc, candidate_url, "delete_requested")
                        _set_task_aux_fields(self.tasks[item_id], state="DELETED")
                        self._discard_task(item_id)
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
                        resumed_output_seconds = max(out_ms / 1_000_000.0, 0.0)
                        effective_resumed_seconds = max(resumed_output_seconds - resume_overlap_seconds, 0.0)
                        total_done_seconds = max(resume_anchor_seconds, resume_anchor_seconds + effective_resumed_seconds)
                        now = time.time()
                        active_total_bytes = None if invalidated_total_bytes else (total_bytes_box.get("value") or total_bytes)
                        ui_time_due = (
                            progress.get("progress") == "end"
                            or last_progress_ui_bytes < 0
                            or (now - last_ui_update) >= FFMPEG_PROGRESS_UI_UPDATE_INTERVAL_SECONDS
                        )
                        checkpoint_due = (
                            progress.get("progress") == "end"
                            or last_checkpoint_bytes < 0
                            or (now - last_checkpoint_at) >= FFMPEG_RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS
                        )
                        should_poll_bytes = ui_time_due or checkpoint_due
                        try:
                            if should_poll_bytes:
                                active_output_bytes = poll_active_output_bytes(
                                    now=now,
                                    force=(progress.get("progress") == "end"),
                                )
                            else:
                                active_output_bytes = cached_active_output_bytes
                        except StopDownloadException:
                            return
                        resumed_bytes = active_output_bytes
                        if resume_overlap_bytes > 0:
                            resumed_bytes = max(active_output_bytes - min(active_output_bytes, resume_overlap_bytes), 0)
                        current_bytes = base_bytes + resumed_bytes
                        should_refresh_progress_ui = (
                            progress.get("progress") == "end"
                            or ui_time_due
                            or last_progress_ui_bytes < 0
                            or abs(current_bytes - last_progress_ui_bytes) >= FFMPEG_PROGRESS_UI_MIN_BYTES_DELTA
                        )
                        if active_total_bytes and active_total_bytes > 0 and progress.get("progress") != "end":
                            if self._should_invalidate_m3u8_total_bytes(current_bytes, active_total_bytes):
                                estimated_total_bytes = active_total_bytes
                                invalidated_total_bytes = True
                                total_bytes = None
                                active_total_bytes = None
                                near_complete_since = None
                                self._invalidate_m3u8_total_bytes(
                                    item_id,
                                    candidate_url,
                                    total_bytes_box,
                                    "actual bytes exceeded estimated total",
                                    estimated_total_bytes,
                                    current_bytes,
                                )
                        progress_percent = None
                        duration_percent = None
                        if total_duration > 0 and total_done_seconds > 0:
                            duration_done_seconds = min(total_done_seconds, total_duration)
                            duration_percent = format_progress_percent(duration_done_seconds, total_duration, cap_at_99=True)
                            if duration_percent is not None and progress.get("progress") != "end":
                                duration_percent = min(duration_percent, 99.0)
                        if active_total_bytes and active_total_bytes > 0:
                            percent = format_progress_percent(current_bytes, active_total_bytes, cap_at_99=True)
                            if percent is not None:
                                if progress.get("progress") != "end":
                                    percent = min(percent, 99.0)
                                    should_invalidate_stalled_total, near_complete_since = self._should_invalidate_stalled_m3u8_total(
                                        percent,
                                        progress.get("progress"),
                                        near_complete_since,
                                        now,
                                    )
                                    if should_invalidate_stalled_total:
                                        estimated_total_bytes = active_total_bytes
                                        invalidated_total_bytes = True
                                        total_bytes = None
                                        active_total_bytes = None
                                        percent = None
                                        near_complete_since = None
                                        self._invalidate_m3u8_total_bytes(
                                            item_id,
                                            candidate_url,
                                            total_bytes_box,
                                            "estimated total stalled at 99% before ffmpeg end",
                                            estimated_total_bytes,
                                            current_bytes,
                                        )
                                else:
                                    near_complete_since = None
                            if active_total_bytes and active_total_bytes > 0 and should_refresh_progress_ui:
                                row_updates = {"size": format_transfer_size(current_bytes, active_total_bytes)}
                            else:
                                row_updates = {}
                            if duration_percent is None:
                                progress_percent = percent
                        else:
                            row_updates = {}
                            near_complete_since = None
                            if duration_percent is None and should_refresh_progress_ui:
                                row_updates["progress"] = "--"
                        if duration_percent is not None:
                            progress_percent = duration_percent
                        if progress_percent is not None and should_refresh_progress_ui:
                            row_updates["progress"] = f"{progress_percent:.1f}%"
                        if not (active_total_bytes and active_total_bytes > 0) and current_bytes > 0:
                            if should_refresh_progress_ui:
                                row_updates["size"] = format_transfer_size(current_bytes)
                        if now - last_ui_update >= FFMPEG_PROGRESS_UI_UPDATE_INTERVAL_SECONDS:
                            instant_bps = 0.0
                            if active_output_bytes > 0 and out_ms > 0:
                                instant_bps = active_output_bytes / max(out_ms / 1_000_000.0, 0.001)
                            try:
                                value = max(float(instant_bps or 0.0), 0.0)
                            except Exception:
                                value = 0.0
                            _set_task_aux_fields(task, _last_speed_bps=value)
                            eta_seconds = None
                            if active_total_bytes and active_total_bytes > 0 and instant_bps > 0:
                                eta_seconds = max((active_total_bytes - current_bytes) / instant_bps, 0.0)
                            elif total_duration > 0 and instant_bps > 0:
                                eta_seconds = max(total_duration - total_done_seconds, 0.0)
                            if instant_bps > 0 and eta_seconds is not None:
                                row_updates["speed_eta"] = f"{format_transfer_rate(instant_bps) if instant_bps else '-'} | {format_eta(float(eta_seconds))}"
                            elif instant_bps > 0:
                                row_updates["speed_eta"] = format_transfer_rate(instant_bps) if instant_bps else "-"
                            last_ui_update = now
                            if self._should_trigger_resume_low_speed_reanalysis(
                                task,
                                _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(candidate_url),
                                save_dir,
                                instant_bps,
                                is_mp3=is_mp3,
                                now=now,
                            ):
                                self._terminate_ffmpeg_process(
                                    self.tasks.get(item_id, {}),
                                    item_id,
                                    proc,
                                    candidate_url,
                                    "resume_low_speed_reanalysis",
                                    bytes=current_bytes,
                                    speed_bps=instant_bps,
                                )
                                raise ResumeLowSpeedReanalysisException("resume transfer stayed below the low-speed threshold")
                        if row_updates:
                            self.update_tree_many(item_id, row_updates, force=True)
                        if should_refresh_progress_ui:
                            last_progress_ui_bytes = current_bytes
                        if (
                            progress.get("progress") == "end"
                            or checkpoint_due
                            or last_checkpoint_bytes < 0
                            or abs(current_bytes - last_checkpoint_bytes) >= FFMPEG_RESUME_PROGRESS_MIN_BYTES_DELTA
                        ):
                            checkpoint_seconds = self._get_resume_checkpoint_seconds(
                                active_output_path,
                                total_done_seconds,
                                total_duration=total_duration,
                                base_offset_seconds=(resume_seek_seconds if resume_anchor_seconds > 0 else 0.0),
                                trim_initial_seconds=(resume_overlap_seconds if resume_anchor_seconds > 0 else 0.0),
                            )
                            self._save_resume_progress(
                                progress_path,
                                checkpoint_seconds,
                                source_url=resume_key,
                                bytes_done=current_bytes,
                                min_interval_seconds=FFMPEG_RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS,
                                min_bytes_delta=FFMPEG_RESUME_PROGRESS_MIN_BYTES_DELTA,
                            )
                            last_checkpoint_at = now
                            last_checkpoint_bytes = current_bytes
                    if progress.get("progress") == "end":
                        break
                return_code = proc.wait()
            finally:
                if proc.stdout is not None:
                    proc.stdout.close()
                current_task = self.tasks.get(item_id)
                if current_task is not None and _task_field_value(current_task, "_proc", None) is proc:
                    _set_task_aux_fields(current_task, _proc=None)

            if return_code == 0:
                final_source = None
                base_exists = os.path.exists(temp_out_path)
                resume_exists = os.path.exists(resume_out_path)

                if resume_anchor_seconds > 0 and base_exists and resume_exists:
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
                    if _task_source_site_name(task) == "gimy":
                        looks_truncated = looks_truncated or (
                            total_duration > 300.0
                            and final_duration > 0.0
                            and final_duration < total_duration * 0.8
                        )
                        looks_invalid = looks_invalid or (
                            final_size < 5 * 1024 * 1024 and final_duration < 300.0
                        )
                        looks_invalid = looks_invalid or self._should_reject_resumed_ffmpeg_artifact(
                            task,
                            final_size,
                            final_duration,
                            base_bytes,
                            total_duration,
                        )
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
                        self._remove_artifact_paths(temp_out_path, resume_out_path, merged_out_path, progress_path, final_source)
                        continue
                if is_mp3 and final_size < 64 * 1024:
                    last_error = Exception(f"FFmpeg produced an unexpectedly tiny audio artifact: size={final_size}")
                    continue

                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                with self._resume_artifact_lock_for(final_source, out_path):
                    if os.path.exists(out_path):
                        try:
                            os.remove(out_path)
                        except OSError:
                            pass
                    self._move_file_with_retry(final_source, out_path, attempts=48, delay_seconds=0.5)

                self._remove_artifact_paths(
                    *(stale_path for stale_path in (temp_out_path, resume_out_path, merged_out_path, progress_path) if stale_path != out_path)
                )

                self._set_task_named_column_text(item_id, "progress", "100%")
                self._mark_task_finished(item_id)
                self._log_ffmpeg_event(
                    "ffmpeg download finished",
                    Exception("ffmpeg finished"),
                    task,
                    item_id,
                    candidate_url,
                    output=out_path,
                    bytes=self._get_existing_file_size(out_path),
                )
                return

            current_task = self.tasks.get(item_id, {})
            current_state = str(_task_field_value(current_task, "state", "") or "")
            stop_reason = _task_field_value(current_task, "_stop_reason", None)
            if return_code != 0 and (self._is_stop_requested_state(current_state) or stop_reason in STOP_REASONS):
                if self._is_pause_requested_state(current_state) or stop_reason == STOP_REASON_PAUSE:
                    raise StopDownloadException("pause requested")
                raise KeyboardInterrupt()

            if return_code == 15 and _unexpected_retry_count < len(FFMPEG_UNEXPECTED_RETRY_DELAYS):
                self._log_ffmpeg_event(
                    "ffmpeg unexpected termination retry",
                    Exception("ffmpeg exited with code 15 without a stop request"),
                    current_task,
                    item_id,
                    candidate_url,
                    return_code=return_code,
                    state=current_state,
                    stop_reason=stop_reason,
                    recent_output=" | ".join(recent_lines)[:240],
                    **self._build_ffmpeg_runtime_fields(
                        ffmpeg_path,
                        retry_count=_unexpected_retry_count + 1,
                        cmd=cmd,
                        ffmpeg_version=ffmpeg_version,
                    ),
                )
                retry_index = min(_unexpected_retry_count, len(FFMPEG_UNEXPECTED_RETRY_DELAYS) - 1)
                time.sleep(FFMPEG_UNEXPECTED_RETRY_DELAYS[retry_index])
                return self._download_m3u8_with_ffmpeg(
                    item_id,
                    candidate_url,
                    save_dir,
                    is_mp3=is_mp3,
                    referer=referer,
                    origin=origin,
                    _unexpected_retry_count=_unexpected_retry_count + 1,
                    _native_fallback_done=_native_fallback_done,
                )

            if return_code == 15 and not _native_fallback_done and _task_source_site_name(current_task) == "gimy":
                write_error_log(
                    "ffmpeg handoff to yt-dlp native",
                    Exception("ffmpeg code 15 fallback to yt-dlp native"),
                    url=candidate_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(current_task) or None,
                    fallback_count=len(_dedupe_download_urls(_task_field_value(current_task, "fallback_urls", []))),
                    return_code=return_code,
                    state=current_state,
                    stop_reason=stop_reason,
                    recent_output=" | ".join(recent_lines)[:240],
                    **self._build_ffmpeg_runtime_fields(
                        ffmpeg_path,
                        retry_count=_unexpected_retry_count,
                        cmd=cmd,
                        ffmpeg_version=ffmpeg_version,
                    ),
                )
                try:
                    return self._download_m3u8_with_ytdlp_native(
                        item_id,
                        candidate_url,
                        save_dir,
                        is_mp3=is_mp3,
                        referer=referer,
                        origin=origin,
                    )
                except Exception as native_exc:
                    _remember_gimy_failed_stream(candidate_url)
                    if source_site == "gimy" and _is_gimy_stream_refreshable_error(native_exc):
                        refreshed = _gimy_refresh_after_stream_failure(native_exc)
                        if refreshed is not None:
                            return refreshed
                    raise

            _remember_gimy_failed_stream(candidate_url)
            last_error = Exception(f"FFmpeg exited with code {return_code}: {' | '.join(recent_lines)[:240]}")
        if last_error and source_site == "gimy":
            refreshed = _gimy_refresh_after_stream_failure(last_error)
            if refreshed is not None:
                return refreshed
        if source_site == "gimy" and not last_error:
            exhausted_exc = Exception("gimy stream candidates exhausted without a successful ffmpeg handoff")
            refreshed = _gimy_refresh_after_stream_failure(exhausted_exc)
            if refreshed is not None:
                return refreshed
            last_error = Exception("FFmpeg download failed without candidates")
        if last_error:
            raise last_error
        raise Exception("FFmpeg download failed without candidates")

    def pause_selected(self):
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            if state == "DOWNLOADING" or self._is_pause_requested_state(state):
                _set_task_aux_fields(self.tasks[item_id], state="PAUSE_REQUESTED", _stop_reason=STOP_REASON_PAUSE)
                self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                proc = _task_field_value(self.tasks[item_id], "_proc", None)
                if proc is not None:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            elif state == "QUEUED":
                _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
                self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
                self._schedule_process_queue()
        self.persist_unfinished_state(force=True)

    def resume_selected(self):
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            if self._is_pause_requested_state(state):
                proc = _task_field_value(self.tasks[item_id], "_proc", None)
                if proc is not None:
                    try:
                        if proc.poll() is None:
                            continue
                    except Exception:
                        continue
                _set_task_aux_fields(self.tasks[item_id], state="PAUSED")
            if str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "") not in RESUMABLE_TASK_STATES:
                continue
            _set_task_aux_fields(self.tasks[item_id], _stop_reason=None)
            if not self._check_resume_disk_space(item_id):
                continue
            task = self.tasks[item_id]
            is_mp3 = bool(_task_field_value(task, "is_mp3", False))
            source_site = _task_source_site_name(task)
            self._start_download_thread(
                _normalize_download_url(_task_field_value(task, "url", "")) or "",
                (
                    str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
                    or (
                        default_short_name_for_url(
                            (_normalize_download_url(_task_field_value(task, "url", "")) or ""),
                            is_mp3=bool(_task_field_value(task, "is_mp3", is_mp3)),
                        )
                        if (_normalize_download_url(_task_field_value(task, "url", "")) or "")
                        else ""
                    )
                ),
                existing_item_id=item_id,
                is_mp3=is_mp3,
                source_site=source_site,
                extra_task_data=self._build_extra_task_data(
                    source_page=self._get_task_source_page(task),
                    fallback_urls=_dedupe_download_urls(_task_field_value(task, "fallback_urls", [])),
                    resolved_url=(_normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""),
                    resolved_url_saved_at=float(_task_field_value(task, "resolved_url_saved_at", 0.0) or 0.0),
                    page_refresh_candidates=_task_gimy_page_refresh_candidates(task),
                ),
            )

    def delete_selected(self):
        changed = False
        for item_id in self._selected_task_ids():
            if item_id not in self.tasks:
                continue
            state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            remove_from_state(_normalize_download_url(_task_field_value(self.tasks[item_id], "url", "")) or "")
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
            if str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "") != "FINISHED":
                continue
            self._delete_finished_task(item_id)
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

    def _flush_task_resume_artifacts(self, task):
        primary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        secondary_value = str(_task_field_value(task, "filename") or "").strip()
        temp_path = primary_value or secondary_value or ""
        if not temp_path:
            return
        progress_path = temp_path + ".progress.json"
        persisted = self._load_resume_progress_info(progress_path)
        resume_keys = []
        for candidate in (
            _normalize_download_url(_task_field_value(task, "url", "")) or "",
            self._get_task_source_page(task, fallback_url=_normalize_download_url(_task_field_value(task, "url", "")) or ""),
            _normalize_download_url(_task_field_value(task, "url", "")) or "",
        ):
            normalized_candidate = _normalize_download_url(candidate)
            if normalized_candidate and normalized_candidate not in resume_keys:
                resume_keys.append(normalized_candidate)
        source_url = str(persisted.get("source_url", "") or (resume_keys[0] if resume_keys else ""))
        root, ext = os.path.splitext(temp_path)
        candidate_paths = [temp_path]
        if ext:
            candidate_paths.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
        best_state = self._select_best_resume_artifact_state(candidate_paths, persisted=persisted)
        best_seconds = best_state.get("seconds", 0.0)
        best_bytes = best_state.get("bytes", 0)
        if best_seconds > 0.0 or best_bytes > 0:
            self._save_resume_progress(progress_path, best_seconds, source_url=source_url, bytes_done=best_bytes)

    def _prepare_shutdown_resume_state(self):
        self._request_shutdown_stop_for_active_tasks()
        for item_id, task, proc in list(self._iter_active_process_handles()):
            media_url = _normalize_download_url(_task_field_value(task, "url", "")) or ""
            try:
                self._terminate_ffmpeg_process(task, item_id, proc, media_url, "app_closing")
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass

    def _wait_for_shutdown_downloads(self, timeout_seconds=SHUTDOWN_ACTIVE_DOWNLOAD_WAIT_SECONDS):
        deadline = time.time() + max(float(timeout_seconds or 0.0), 0.0)
        while time.time() < deadline:
            active_found = False
            for task in self.tasks.values():
                proc = _task_field_value(task, "_proc", None)
                if proc is None:
                    continue
                try:
                    if proc.poll() is None:
                        active_found = True
                        break
                except Exception:
                    active_found = True
                    break
            if not active_found:
                break
            time.sleep(0.05)

    def _wait_for_shutdown_resume_artifacts(self, timeout_seconds=SHUTDOWN_RESUME_ARTIFACT_WAIT_SECONDS, stable_polls=2):
        deadline = time.time() + max(float(timeout_seconds or 0.0), 0.0)
        last_snapshot = None
        stable_count = 0
        while time.time() < deadline:
            snapshot = []
            for task in self._iter_live_tasks():
                primary_value = str(_task_field_value(task, "temp_filename") or "").strip()
                secondary_value = str(_task_field_value(task, "filename") or "").strip()
                temp_path = primary_value or secondary_value or ""
                if not temp_path:
                    continue
                root, ext = os.path.splitext(temp_path)
                candidate_paths = [temp_path]
                if ext:
                    candidate_paths.extend([f"{root}.resume{ext}", f"{root}.merged{ext}"])
                for candidate_path in candidate_paths:
                    try:
                        if os.path.exists(candidate_path):
                            snapshot.append((candidate_path, os.path.getsize(candidate_path)))
                    except OSError:
                        continue
            snapshot.sort()
            snapshot = tuple(snapshot)
            if snapshot == last_snapshot:
                stable_count += 1
                if stable_count >= max(int(stable_polls or 1), 1):
                    break
            else:
                last_snapshot = snapshot
                stable_count = 0
            time.sleep(0.08)

    def _flush_live_resume_state(self):
        for task in self._iter_live_tasks():
            try:
                self._flush_task_resume_artifacts(task)
            except Exception:
                continue

    def _iter_active_process_handles(self):
        seen = set()
        for item_id, task in self.tasks.items():
            proc = _task_field_value(task, "_proc", None)
            if proc is None:
                continue
            proc_id = id(proc)
            if proc_id in seen:
                continue
            seen.add(proc_id)
            yield item_id, task, proc

    def _terminate_process_handle(self, proc, timeout_seconds=3.0):
        if proc is None:
            return
        try:
            if proc.poll() is not None:
                return
        except Exception:
            return
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=max(float(timeout_seconds or 0.0), 0.1))
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=1.5)
            except Exception:
                pass
        for stream_name in ("stdout", "stderr", "stdin"):
            try:
                stream = getattr(proc, stream_name, None)
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    def _request_shutdown_stop_for_active_tasks(self):
        for item_id, task in self.tasks.items():
            state = str(_task_field_value(task, "state", "") or "")
            if state == "DOWNLOADING":
                _set_task_aux_fields(task, state="PAUSE_REQUESTED", _stop_reason=STOP_REASON_PAUSE, resume_requested=True)
                self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")
            elif state == "QUEUED":
                _set_task_aux_fields(task, state="PAUSED", _stop_reason=None, resume_requested=True)
                self._set_task_status_mode_ui(item_id, t("status_paused") if "status_paused" in I18N_DICT.get(CURRENT_LANG, {}) else "已暫停")

    def _force_kill_child_processes(self):
        tracked_processes = list(self._iter_active_process_handles())
        tracked_pairs = []
        for _item_id, task, proc in tracked_processes:
            try:
                if proc is not None and proc.poll() is None:
                    tracked_pairs.append((task, proc))
            except Exception:
                continue
        for _task, proc in tracked_pairs:
            try:
                proc.terminate()
            except Exception:
                pass
        deadline = time.time() + SHUTDOWN_PROCESS_TERMINATE_TIMEOUT_SECONDS
        while tracked_pairs and time.time() < deadline:
            remaining = []
            for task, proc in tracked_pairs:
                try:
                    if proc.poll() is None:
                        remaining.append((task, proc))
                except Exception:
                    continue
            if not remaining:
                tracked_pairs = []
                break
            tracked_pairs = remaining
            time.sleep(0.05)
        for task, proc in tracked_pairs:
            try:
                proc.kill()
            except Exception:
                pass
        kill_deadline = time.time() + SHUTDOWN_PROCESS_KILL_TIMEOUT_SECONDS
        while tracked_pairs and time.time() < kill_deadline:
            remaining = []
            for task, proc in tracked_pairs:
                try:
                    if proc.poll() is None:
                        remaining.append((task, proc))
                except Exception:
                    continue
            if not remaining:
                tracked_pairs = []
                break
            tracked_pairs = remaining
            time.sleep(0.05)
        for _item_id, task, proc in tracked_processes:
            try:
                self._terminate_process_handle(proc, timeout_seconds=0.1)
            finally:
                try:
                    if _task_field_value(task, "_proc", None) is proc:
                        _set_task_aux_fields(task, _proc=None)
                except Exception:
                    pass
        try:
            import psutil

            parent = psutil.Process(os.getpid())
            child_processes = list(parent.children(recursive=True))
            for child in child_processes:
                try:
                    child.terminate()
                except Exception:
                    continue
            _gone, alive = psutil.wait_procs(child_processes, timeout=SHUTDOWN_PROCESS_TERMINATE_TIMEOUT_SECONDS)
            for child in alive:
                try:
                    child.kill()
                except Exception:
                    continue
            try:
                psutil.wait_procs(alive, timeout=SHUTDOWN_PROCESS_KILL_TIMEOUT_SECONDS)
            except Exception:
                pass
        except Exception:
            return

    def _process_queue(self):
        if self._shutdown_started:
            return
        domain_limit = self._effective_domain_download_limit()
        domain_counts = {}
        source_page_counts = {}
        source_site_counts = {}
        queued_items = []
        for item_id, task in self.tasks.items():
            if self._is_downloading_state(str(_task_field_value(task, "state", "") or "")):
                domain, source_page = (
                    self._extract_domain(_normalize_download_url(_task_field_value(task, "url", "")) or ""),
                    self._get_task_source_page(task),
                )
                source_site = _task_source_site_name(task)
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
                if source_page:
                    source_page_counts[source_page] = source_page_counts.get(source_page, 0) + 1
                if source_site:
                    source_site_counts[source_site] = source_site_counts.get(source_site, 0) + 1
            elif self._is_queued_state(str(_task_field_value(task, "state", "") or "")):
                queued_items.append(item_id)
        self._log_domain_limit_change(domain_limit, sum(domain_counts.values()), len(queued_items))
        for item_id in queued_items:
            task = self.tasks[item_id]
            domain, source_page = (
                self._extract_domain(_normalize_download_url(_task_field_value(task, "url", "")) or ""),
                self._get_task_source_page(task),
            )
            source_site = _task_source_site_name(task)
            source_page_limit, source_site_limit = self._source_download_limits(source_site, domain_limit)
            if domain_counts.get(domain, 0) >= domain_limit:
                continue
            if source_page and source_page_counts.get(source_page, 0) >= source_page_limit:
                continue
            if source_site and source_site_counts.get(source_site, 0) >= source_site_limit:
                continue
            _set_task_aux_fields(task, state="DOWNLOADING")
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
            if source_page:
                source_page_counts[source_page] = source_page_counts.get(source_page, 0) + 1
            if source_site:
                source_site_counts[source_site] = source_site_counts.get(source_site, 0) + 1
            self._set_task_status_mode_ui(item_id, t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中")
            self._start_daemon_thread(
                self.download_task,
                _normalize_download_url(_task_field_value(task, "url", "")) or "",
                item_id,
                self.save_dir_var.get(),
                self._should_use_impersonation(_normalize_download_url(_task_field_value(task, "url", "")) or "", _task_source_site_name(task)),
                bool(_task_field_value(task, "is_mp3", False)),
            )

    def _cleanup_temp_files(self, item_id):
        task = self.tasks.get(item_id)
        if not task:
            return
        sys_temp_dir = tempfile.gettempdir()
        primary_value = str(_task_field_value(task, "filename") or "").strip()
        secondary_value = str(_task_field_value(task, "temp_filename") or "").strip()
        filename = primary_value or secondary_value or ""
        if filename:
            base_name = os.path.splitext(os.path.basename(filename))[0]
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

    def _prepare_resume_low_speed_watch(self, task, source_url, save_dir, is_mp3=False):
        watching_resume = self._is_resume_download_active(
            task,
            save_dir=save_dir,
            is_mp3=is_mp3,
            include_cached_resolved=True,
        )
        source_page = self._get_task_source_page(task, fallback_url=source_url)
        if watching_resume and source_page:
            _set_task_aux_fields(
                task,
                _resume_low_speed_watch_started_at=time.time(),
                _resume_low_speed_reanalysis_attempted=False,
            )
        else:
            _set_task_aux_fields(
                task,
                _resume_low_speed_watch_started_at=0.0,
                _resume_low_speed_reanalysis_attempted=False,
            )

    def _should_trigger_resume_low_speed_reanalysis(self, task, source_url, save_dir, speed_bps, is_mp3=False, now=None):
        if task is None:
            return False
        if bool(_task_field_value(task, "_resume_low_speed_reanalysis_attempted", False)):
            return False
        started_at = float(_task_field_value(task, "_resume_low_speed_watch_started_at", 0.0) or 0.0)
        if started_at <= 0:
            return False
        source_page = self._get_task_source_page(task, fallback_url=source_url)
        if not source_page:
            return False
        now = time.time() if now is None else now
        if (now - started_at) < RESUME_LOW_SPEED_REANALYZE_DELAY_SECONDS:
            return False
        try:
            normalized_speed = max(float(speed_bps or 0.0), 0.0)
        except Exception:
            normalized_speed = 0.0
        if normalized_speed <= 0 or normalized_speed >= RESUME_LOW_SPEED_REANALYZE_THRESHOLD_BPS:
            return False
        _set_task_aux_fields(
            task,
            _resume_low_speed_reanalysis_attempted=True,
            _resume_low_speed_watch_started_at=0.0,
        )
        write_error_log(
            "resume low speed reanalysis requested",
            Exception("resume low speed reanalysis requested"),
            url=source_url,
            item_id=_task_field_value(task, "item_id", None),
            source_site=_task_source_site_name(task) or None,
            source_page=source_page,
            speed_bps=normalized_speed,
            threshold_bps=RESUME_LOW_SPEED_REANALYZE_THRESHOLD_BPS,
            elapsed_seconds=max(now - started_at, 0.0),
            is_mp3=bool(is_mp3),
        )
        return True

    def _retry_source_after_cached_link_failure(self, task, item_id, source_url, save_dir, use_impersonate, is_mp3, expired=False):
        self._set_cached_resolved_link_state(
            task,
            resolved_url="",
            resolved_url_saved_at=0.0,
            page_refresh_candidates=[],
            clear_source_refresh_history=True,
        )
        if expired:
            self._set_task_parse_ui(item_id, message="已記錄連結已過期，重新分析頁面...")
        else:
            self._set_task_parse_ui(item_id, message="已記錄連結失效，重新分析頁面...")
        retry_url = self._get_task_source_page(task, fallback_url=source_url)
        if _task_source_site_name(task) == "movieffm" and retry_url and retry_url != source_url:
            self._download_task_internal(retry_url, item_id, save_dir, use_impersonate, is_mp3)
            return
        self._download_task_internal(source_url, item_id, save_dir, use_impersonate, is_mp3)

    def _download_with_cached_resolved_link(self, task, item_id, source_url, cached_resolved_url, save_dir, use_impersonate, is_mp3):
        self._set_task_parse_ui(item_id, message="使用已記錄下載連結續傳...")
        normalized_source_url = _normalize_download_url(source_url)
        if normalized_source_url and not self._get_task_source_page(task, fallback_url=""):
            _set_task_aux_fields(task, source_page=normalized_source_url)
            self._update_task_state_entry(task, source_page=normalized_source_url)
        if _task_source_site_name(task) == "99itv":
            source_page_url = self._get_task_source_page(task, fallback_url=source_url)
            parsed_source_page = urllib.parse.urlparse(source_page_url or "")
            if source_page_url and "99itv.net" in parsed_source_page.netloc:
                try:
                    c_req = get_curl_cffi_requests()
                    site_root = f"{parsed_source_page.scheme or 'https'}://{parsed_source_page.netloc}"
                    resp = c_req.get(source_page_url, impersonate="chrome110", timeout=20, headers={"Referer": site_root + "/"})
                    refreshed_title = _clean_99itv_title(_extract_html_title(resp.text, str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "99iTV" or "").strip() or "99iTV"))
                    if refreshed_title:
                        task["short_name"] = refreshed_title
                        task["name"] = refreshed_title
                        self._set_task_named_column_text(item_id, "name", refreshed_title)
                        self._update_task_state_entry(task, name=refreshed_title, source_site="99itv", source_page=source_page_url)
                except Exception:
                    pass
        if _is_expired_signed_media_url(cached_resolved_url):
            write_error_log(
                "cached resolved url expired",
                Exception("signed direct media URL expired"),
                item_id=item_id,
                source_url=source_url,
                resolved_url=cached_resolved_url,
                source_site=_task_source_site_name(task) or None,
            )
            self._retry_source_after_cached_link_failure(task, item_id, source_url, save_dir, use_impersonate, is_mp3, expired=True)
            return True
        if not _url_host_resolves(cached_resolved_url):
            write_error_log(
                "cached resolved url host unresolved",
                Exception("cached resolved media host unresolved"),
                item_id=item_id,
                source_url=source_url,
                resolved_url=cached_resolved_url,
                source_site=_task_source_site_name(task) or None,
            )
            self._retry_source_after_cached_link_failure(task, item_id, source_url, save_dir, use_impersonate, is_mp3, expired=False)
            return True
        try:
            self._download_task_internal(cached_resolved_url, item_id, save_dir, use_impersonate, is_mp3)
            return True
        except (ParallelHlsRetryLaterException, ParallelHlsUnsupportedSegmentContentException) as cached_exc:
            write_error_log(
                "cached resolved url rejected",
                cached_exc,
                item_id=item_id,
                source_url=source_url,
                resolved_url=cached_resolved_url,
                source_site=_task_source_site_name(task) or None,
            )
            self._set_cached_resolved_link_state(
                task,
                resolved_url="",
                resolved_url_saved_at=0.0,
                page_refresh_candidates=[],
                clear_source_refresh_history=True,
            )
            self._set_task_parse_ui(item_id, message="已清除錯誤下載連結，請重新加入或重試來源頁...")
            raise
        except (
            StopDownloadException,
            KeyboardInterrupt,
            ResumeLowSpeedReanalysisException,
        ):
            raise
        except Exception as cached_exc:
            write_error_log(
                "cached resolved url failed",
                cached_exc,
                item_id=item_id,
                source_url=source_url,
                resolved_url=cached_resolved_url,
                source_site=_task_source_site_name(task) or None,
            )
            self._retry_source_after_cached_link_failure(task, item_id, source_url, save_dir, use_impersonate, is_mp3, expired=False)
            return True

    def _should_resume_with_cached_resolved_link(self, task, source_url, save_dir, is_mp3=False):
        cached_resolved_url = _normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""
        if not cached_resolved_url:
            return False
        if self._is_resume_download_active(task, save_dir=save_dir, is_mp3=is_mp3, include_cached_resolved=True):
            return True
        ext = "mp3" if is_mp3 else "mp4"
        temp_out_path, _, _ = self._resolve_resume_artifact_base(
            task,
            source_url,
            ext=ext,
            save_dir=save_dir,
            fallback_name="Audio" if is_mp3 else "Video",
        )
        temp_root, temp_ext = os.path.splitext(temp_out_path)
        candidate_paths = (
            temp_out_path,
            f"{temp_root}.resume{temp_ext}",
            f"{temp_root}.merged{temp_ext}",
        )
        if any(self._has_nonempty_file(candidate_path) for candidate_path in candidate_paths):
            return self._resume_artifact_matches_keys(temp_out_path, [source_url, cached_resolved_url])
        return False

    def download_task(self, url, item_id, save_dir, use_impersonate, is_mp3=False):
        has_anime1_lock = False
        try:
            task = self.tasks.get(item_id, {})
            cached_resolved_url = _normalize_download_url(_task_field_value(task, "resolved_url", "")) or ""
            self._prepare_resume_low_speed_watch(task, url, save_dir, is_mp3=is_mp3)
            url_lower = url.lower()
            if ("//anime1.me/" in url_lower or "//anime1.pw/" in url_lower) and use_impersonate:
                self._set_task_status_mode_ui(
                    item_id,
                    t("status_processing") if "status_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                    t("eta_processing") if "eta_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                )
                anime1_dl_lock.acquire()
                has_anime1_lock = True
            if self._should_resume_with_cached_resolved_link(task, url, save_dir, is_mp3):
                self._download_with_cached_resolved_link(
                    task,
                    item_id,
                    url,
                    cached_resolved_url,
                    save_dir,
                    use_impersonate,
                    is_mp3,
                )
            else:
                source_site = _task_source_site_name(task)
                source_page_url = self._get_task_source_page(task, fallback_url=url)
                if source_site == "movieffm" and source_page_url and source_page_url != url and not _url_host_resolves(url):
                    self._set_task_parse_ui(item_id, message="已記錄來源失效，重新分析頁面...")
                    self._download_task_internal(source_page_url, item_id, save_dir, use_impersonate, is_mp3)
                else:
                    self._download_task_internal(url, item_id, save_dir, use_impersonate, is_mp3)
            if has_anime1_lock:
                anime1_dl_lock.release()
                has_anime1_lock = False

            def finish_dl():
                self._schedule_summary_refresh()

            self._schedule_ui_call(finish_dl)
        except ResumeLowSpeedReanalysisException:
            if has_anime1_lock:
                anime1_dl_lock.release()
                has_anime1_lock = False
            self._retry_source_after_cached_link_failure(task, item_id, url, save_dir, use_impersonate, is_mp3, expired=False)
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
            self._set_task_status_mode_ui(
                item_id,
                t("status_error") if "status_error" in I18N_DICT.get(CURRENT_LANG, {}) else "錯誤",
                t("msg_download_error", error=str(exc)[:45]) if "msg_download_error" in I18N_DICT.get(CURRENT_LANG, {}) else summarize_error_message(exc, "err_net", 120),
            )

            def update_err_state():
                task = self.tasks.get(item_id)
                if not task:
                    return
                _set_task_aux_fields(task, state="ERROR")

            self._schedule_ui_call(update_err_state)
        finally:
            if has_anime1_lock:
                anime1_dl_lock.release()
            self._schedule_process_queue()

    def _download_task_internal(self, url, item_id, save_dir, use_impersonate, is_mp3=False):
        task = self.tasks.get(item_id, {})
        short_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or (t("msg_resume_name") if "msg_resume_name" in I18N_DICT.get(CURRENT_LANG, {}) else "未完成項目") or "").strip()
        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or short_name or "").strip()
        safe_name = _safe_output_stem(name, fallback=short_name)
        possible_exts = (".mp4", ".mkv", ".webm", ".m4a")
        if is_mp3:
            possible_exts = (".mp3", ".m4a")
        if self._complete_if_output_exists(item_id):
            return

        def progress_hook(d):
            task_state = str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "")
            if self._is_pause_requested_state(task_state):
                raise StopDownloadException("pause requested")
            if self._is_delete_requested_state(task_state):
                raise KeyboardInterrupt()
            status = str((d or {}).get("status", "") or "")
            if status == "downloading":
                target_path, downloaded, total = _event_transfer_metrics(d, default_path=save_dir, default_downloaded=0)
                required_bytes = max(int(total) - int(downloaded), 0) if total else None
                if self._maybe_auto_pause_for_disk_space(item_id, target_path, required_bytes=required_bytes, note=t("msg_disk_full_pause") if "msg_disk_full_pause" in I18N_DICT.get(CURRENT_LANG, {}) else "磁碟空間不足，自動暫停"):
                    raise StopDownloadException("disk space low")
            if status == "finished":
                return
            if status != "downloading":
                return
            _target_path, downloaded, total = _event_transfer_metrics(d, default_path=save_dir, default_downloaded=0)
            speed = (d or {}).get("speed")
            eta = (d or {}).get("eta")
            self._set_task_active_transfer_ui(
                task,
                item_id,
                downloaded,
                total_bytes=total,
                speed_bps=speed,
                eta_seconds=eta,
            )

        ydl_opts = _build_ytdlp_download_opts(
            progress_hook=progress_hook,
            home_dir=save_dir,
            temp_dir=tempfile.gettempdir(),
            outtmpl="%(title)s.%(ext)s",
            concurrent_fragment_downloads=_ytdlp_generic_concurrency_for_site(_task_source_site_name(task)),
            http_headers=_make_ytdlp_http_headers(),
            extractor_args={"youtube": {"player_client": ["android", "web"]}},
            http_chunk_size=_ytdlp_http_chunk_size_for_site(_task_source_site_name(task)),
            throttled_rate_bps=_ytdlp_throttled_rate_for_site(_task_source_site_name(task)),
        )
        if is_mp3:
            _apply_ytdlp_audio_postprocessing(ydl_opts)

        social_cookie_sources = []
        yt_dlp_module = None

        def _run_yt_dlp(target_url):
            if yt_dlp_module is None:
                module = get_yt_dlp_module()
            else:
                module = yt_dlp_module
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
                        ydl = module.YoutubeDL(current_opts)
                    ydl.download([target_url])
                    return
                except (StopDownloadException, KeyboardInterrupt, ResumeLowSpeedReanalysisException):
                    raise
                except Exception as exc:
                    last_exc = exc
                    continue
            if last_exc:
                raise last_exc

        def _download_manifest_with_generic_ytdlp(target_url, referer=None, origin=None):
            ydl_opts["http_headers"] = _make_hls_http_headers(referer=referer, origin=origin)
            ydl_opts["outtmpl"] = {"default": f"{safe_name}.%(ext)s"}
            _run_yt_dlp(target_url)
            if str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "") == "DELETED":
                raise KeyboardInterrupt()
            self._mark_task_finished(item_id)

        def _download_manifest_with_site_strategy(
            target_url,
            referer=None,
            origin=None,
            default_route="generic",
            force_ffmpeg=False,
        ):
            # Keep manifest resume behavior consistent across supported sites:
            # every supported site first resolves through one shared route selector,
            # then dispatches to ffmpeg/native/generic from the same decision point.
            selected_route = self._select_manifest_download_route(
                target_url,
                task,
                save_dir,
                is_mp3=is_mp3,
                default_route=default_route,
                force_ffmpeg=force_ffmpeg,
            )
            if selected_route == "ffmpeg":
                self._download_m3u8_with_ffmpeg(
                    item_id,
                    target_url,
                    save_dir,
                    is_mp3=is_mp3,
                    referer=referer,
                    origin=origin,
                )
                return
            if selected_route == "native":
                try:
                    self._download_m3u8_with_ytdlp_native(
                        item_id,
                        target_url,
                        save_dir,
                        is_mp3=is_mp3,
                        referer=referer,
                        origin=origin,
                    )
                except (StopDownloadException, KeyboardInterrupt, ResumeLowSpeedReanalysisException):
                    raise
                except Exception as native_exc:
                    write_error_log(
                        "native hls handoff to ffmpeg",
                        native_exc,
                        url=target_url,
                        item_id=item_id,
                        source_site=_task_source_site_name(task) or None,
                        selected_route=selected_route,
                    )
                    self._download_m3u8_with_ffmpeg(
                        item_id,
                        target_url,
                        save_dir,
                        is_mp3=is_mp3,
                        referer=referer,
                        origin=origin,
                        _native_fallback_done=True,
                    )
                return
            _download_manifest_with_generic_ytdlp(target_url, referer=referer, origin=origin)

        def _set_task_identity(name=None, source_site=None, source_page=None, fallback_urls=None):
            updates = {}
            if name:
                normalized_name = str(name or "").strip()
                if normalized_name:
                    task["short_name"] = normalized_name
                    task["name"] = normalized_name
                    self._set_task_named_column_text(item_id, "name", normalized_name)
                    updates["name"] = normalized_name
            if source_site:
                normalized_site = str(source_site).strip().lower()
                task["source_site"] = normalized_site
                updates["source_site"] = normalized_site
            if source_page:
                normalized_page = _normalize_download_url(source_page)
                if normalized_page:
                    task["source_page"] = normalized_page
                    updates["source_page"] = normalized_page
            if fallback_urls is not None:
                normalized_urls = _dedupe_download_urls(fallback_urls, primary_url=_normalize_download_url(_task_field_value(task, "url", "")) or "")
                task["fallback_urls"] = normalized_urls
                updates["fallback_urls"] = normalized_urls
            if updates:
                self._update_task_state_entry(task, **updates)

        def _dispatch_extracted_media_candidates(
            candidate_urls,
            display_name,
            source_site,
            source_page=None,
            referer=None,
            origin=None,
            direct_filter=None,
            manifest_default_route="ffmpeg",
            headers=None,
            session=None,
            prefer_direct=False,
            missing_message="media URL missing",
        ):
            manifest_candidates, direct_media_candidates = _split_stream_and_direct_candidates(
                candidate_urls,
                direct_filter=direct_filter,
            )
            stream_url, stream_fallback_urls = _pick_primary_with_fallbacks(manifest_candidates)
            media_url, media_fallback_urls = _pick_primary_with_fallbacks(direct_media_candidates)
            if prefer_direct and media_url:
                _set_task_identity(
                    name=display_name,
                    source_site=source_site,
                    source_page=source_page or url,
                    fallback_urls=media_fallback_urls + manifest_candidates,
                )
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    display_name,
                    is_mp3=is_mp3,
                    source_site=source_site,
                    fallback_urls=media_fallback_urls + manifest_candidates,
                    referer=referer or url,
                    origin=origin,
                    headers=headers,
                    session=session,
                )
                return True
            if stream_url:
                _set_task_identity(
                    name=display_name,
                    source_site=source_site,
                    source_page=source_page or url,
                    fallback_urls=stream_fallback_urls,
                )
                self._download_routed_media_url(
                    task,
                    item_id,
                    stream_url,
                    save_dir,
                    display_name,
                    is_mp3=is_mp3,
                    source_site=source_site,
                    fallback_urls=stream_fallback_urls,
                    referer=referer or url,
                    origin=origin,
                    manifest_downloader=_download_manifest_with_site_strategy,
                    manifest_default_route=manifest_default_route,
                    headers=headers,
                    session=session,
                )
                return True
            if media_url:
                _set_task_identity(
                    name=display_name,
                    source_site=source_site,
                    source_page=source_page or url,
                    fallback_urls=media_fallback_urls,
                )
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    display_name,
                    is_mp3=is_mp3,
                    source_site=source_site,
                    fallback_urls=media_fallback_urls,
                    referer=referer or url,
                    origin=origin,
                    headers=headers,
                    session=session,
                )
                return True
            raise Exception(missing_message)

        def _add_m3u8_candidate(candidates, value, base_url=""):
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    _add_m3u8_candidate(candidates, item, base_url=base_url)
                return
            if isinstance(value, dict):
                for item in value.values():
                    _add_m3u8_candidate(candidates, item, base_url=base_url)
                return
            if not isinstance(value, str):
                return
            stream_url = html.unescape(value).replace("\\/", "/").strip()
            if not stream_url:
                return
            if stream_url.startswith("//"):
                stream_url = "https:" + stream_url
            elif base_url and stream_url.startswith("/"):
                stream_url = urllib.parse.urljoin(base_url, stream_url)
            normalized_url = _normalize_download_url(stream_url)
            if not normalized_url or ".m3u8" not in normalized_url.lower():
                return
            if normalized_url not in candidates:
                candidates.append(normalized_url)

        def _collect_player_m3u8_candidates(player_data, base_url=""):
            candidates = []
            if not isinstance(player_data, dict):
                return candidates
            for key in (
                "url",
                "src",
                "source",
                "play_url",
                "playUrl",
                "urls",
                "backup",
                "backup_urls",
                "m3u8_urls",
                "line_urls",
                "lineUrls",
                "playlist",
                "sources",
            ):
                if key in player_data:
                    _add_m3u8_candidate(candidates, player_data.get(key), base_url=base_url)
            extras = player_data.get("vod_data") or {}
            if isinstance(extras, dict):
                for key in ("url", "play_url", "playUrl", "sources"):
                    if key in extras:
                        _add_m3u8_candidate(candidates, extras.get(key), base_url=base_url)
            return _dedupe_download_urls(candidates)

        def _extract_m3u8_candidates_from_text(page_text, base_url=""):
            page_text = str(page_text or "")
            candidates = []
            for pattern in (
                r"var\s+url\s*=\s*['\"]([^'\"]+\.m3u8[^'\"]*)['\"]",
                r"['\"](https?://[^\"'\s]+\.m3u8[^\"'\s]*)['\"]",
                r"(https?://[^\"'\s]+\.m3u8[^\"'\s]*)",
            ):
                for match in re.finditer(pattern, page_text, re.IGNORECASE):
                    _add_m3u8_candidate(candidates, match.group(1), base_url=base_url)
            return _dedupe_download_urls(candidates)

        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.netloc.lower().endswith("mega.nz") or parsed_url.netloc.lower().endswith("mega.co.nz"):
            self._set_task_parse_ui(item_id, key="eta_site_mega", fallback="正在解析 MEGA...")
            self._download_mega_public_file(item_id, url, save_dir, is_mp3=is_mp3)
            return
        if any(host in parsed_url.netloc for host in ("instagram.com", "facebook.com", "threads.net")):
            preferred_social_browsers = ("edge", "chrome", "firefox")
            detected_social_sources = _detect_browser_cookie_sources()
            social_cookie_sources = [
                source
                for browser in preferred_social_browsers
                for source in detected_social_sources
                if source and source[0] == browser
            ]
            if social_cookie_sources:
                ydl_opts.setdefault("cookiesfrombrowser", social_cookie_sources[0])
        is_youtube = any(host in parsed_url.netloc for host in ("youtube.com", "youtu.be"))
        if is_youtube:
            use_impersonate = False
            ydl_opts["noplaylist"] = True
            _set_task_identity(source_site="youtube")

        normalized_url = _normalize_download_url(url)
        forced_m3u8_site = _resolve_forced_m3u8_site(normalized_url, task)
        if forced_m3u8_site:
            site_config = FORCED_M3U8_SITE_RULES[forced_m3u8_site]
            referer = site_config["referer"]
            origin = site_config["origin"]
            if forced_m3u8_site in ("missav", "movieffm", "xiaoyakankan"):
                referer = self._get_task_source_page(task, fallback_url=referer) or referer
                parsed_ref = urllib.parse.urlparse(referer)
                if parsed_ref.scheme and parsed_ref.netloc:
                    origin = f"{parsed_ref.scheme}://{parsed_ref.netloc}"
            ydl_opts["http_headers"] = _make_hls_http_headers(referer=referer, origin=origin)
            self._log_m3u8_route_selected(task, item_id, url, source_site=forced_m3u8_site)
            _download_manifest_with_site_strategy(
                url,
                referer=referer,
                origin=origin,
                default_route="ffmpeg",
            )
            return

        if _looks_like_manifest_url(url):
            source_page_url = self._get_task_source_page(task, fallback_url="") or ""
            referer = source_page_url or url
            parsed_referer = urllib.parse.urlparse(referer)
            if parsed_referer.scheme and parsed_referer.netloc:
                origin = f"{parsed_referer.scheme}://{parsed_referer.netloc}"
            else:
                origin = f"{parsed_url.scheme}://{parsed_url.netloc}" if parsed_url.scheme and parsed_url.netloc else ""
            self._log_m3u8_route_selected(
                task,
                item_id,
                url,
                source_site=_task_source_site_name(task),
                fallback_urls=_dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url),
            )
            _download_manifest_with_site_strategy(
                url,
                referer=referer,
                origin=origin,
                force_ffmpeg=(
                    _task_source_site_name(task) == "movieffm"
                    and _should_use_ffmpeg_for_movieffm_manifest(url)
                ),
            )
            return

        inferred_direct_media_ext = _infer_media_extension_from_url(url)
        is_direct_media = bool(inferred_direct_media_ext) or any(parsed_url.path.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mkv", ".webm", ".m4a", ".mp3"))
        if is_direct_media:
            self._set_task_parse_ui(item_id, key="eta_direct_media", fallback=self._ui_text("eta_direct_media", "直接媒體下載"))
            filename = os.path.basename(parsed_url.path) or "downloaded_file"
            use_task_title_filename = _task_source_site_name(task) == "avjoy" or not os.path.splitext(filename)[1]
            if use_task_title_filename and inferred_direct_media_ext:
                name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "downloaded_file" or "").strip()
                if _task_source_site_name(task) == "avjoy":
                    name = _clean_avjoy_title(name, fallback_title=default_short_name_for_url(_task_field_value(task, "source_page") or _task_field_value(task, "url") or url))
                safe_name = _safe_output_stem(name, fallback="downloaded_file")
                filename = f"{safe_name}{inferred_direct_media_ext}"
            output_path = os.path.join(save_dir, filename)
            _set_task_aux_fields(task, filename=output_path)
            self._set_task_named_column_text(item_id, "name", filename)
            direct_headers = {"User-Agent": DEFAULT_USER_AGENT}
            direct_referer = self._get_task_source_page(task)
            derived_mixdrop_watch_url = ""
            if "mxcontent.net" in parsed_url.netloc.lower():
                task = task or {}
                candidate_urls = [
                    _normalize_download_url(_task_field_value(task, "source_page", "")),
                    _normalize_download_url(url),
                    _normalize_download_url(_task_field_value(task, "url", "")),
                ]
                candidate_urls.extend(
                    _dedupe_download_urls(
                        _task_field_value(task, "fallback_urls", []),
                        primary_url=_task_field_value(task, "url", ""),
                    )
                )
                for candidate in candidate_urls:
                    if not candidate:
                        continue
                    normalized_candidate = _normalize_mixdrop_watch_url(candidate)
                    candidate_host = urllib.parse.urlsplit(normalized_candidate).netloc.lower() if normalized_candidate else ""
                    if normalized_candidate and any(mix_host in candidate_host for mix_host in ("mixdrop.ag", "m1xdrop.click")):
                        derived_mixdrop_watch_url = normalized_candidate
                        break
                if not derived_mixdrop_watch_url:
                    preferred_mixdrop_host = ""
                    for candidate in (
                        self._get_task_source_page(task),
                        _normalize_download_url(_task_field_value(task, "url", "")) or "",
                    ):
                        candidate_host = urllib.parse.urlsplit(_normalize_download_url(candidate)).netloc.lower() if candidate else ""
                        if "m1xdrop.click" in candidate_host:
                            preferred_mixdrop_host = "m1xdrop.click"
                            break
                        if "mixdrop.ag" in candidate_host:
                            preferred_mixdrop_host = "mixdrop.ag"
                            break
                    normalized_media_url = _normalize_download_url(url)
                    if normalized_media_url:
                        parsed_media_url = urllib.parse.urlsplit(normalized_media_url)
                        media_host = parsed_media_url.netloc.lower()
                        if "mxcontent.net" in media_host:
                            ref_match = re.search(
                                r"/([A-Za-z0-9]+)\.(?:mp4|mkv|webm|m4a|mp3)(?:$|\?)",
                                parsed_media_url.path,
                                re.IGNORECASE,
                            )
                            if ref_match:
                                target_host = preferred_mixdrop_host or "mixdrop.ag"
                                derived_mixdrop_watch_url = f"https://{target_host}/e/{ref_match.group(1)}"
                if derived_mixdrop_watch_url:
                    direct_referer = derived_mixdrop_watch_url
            direct_session = None
            if direct_referer:
                direct_headers["Referer"] = direct_referer
                parsed_referer = urllib.parse.urlsplit(direct_referer)
                if parsed_referer.scheme and parsed_referer.netloc:
                    direct_headers["Origin"] = f"{parsed_referer.scheme}://{parsed_referer.netloc}"
            if _is_mixdrop_direct_media(url, direct_referer):
                c_req = get_curl_cffi_requests()
                direct_session = self._track_network_session(c_req.Session(impersonate="chrome120"))
                warmup_referer = _normalize_download_url(_task_field_value(task, "url", "")) or "https://www.movieffm.net/"
                try:
                    direct_session.get(
                        direct_referer,
                        headers=_make_ytdlp_http_headers(referer=warmup_referer),
                        timeout=20,
                    )
                except Exception:
                    try:
                        direct_session.get(
                            direct_referer,
                            headers=_make_ytdlp_http_headers(referer=direct_referer),
                            timeout=20,
                        )
                    except Exception:
                        direct_session = None
            try:
                self._download_http_media(item_id, url, (str(_task_field_value(task, "filename") or "").strip() or str(_task_field_value(task, "temp_filename") or "").strip() or ""), headers=direct_headers, session=direct_session)
                return
            except Exception as e:
                if _is_mixdrop_direct_media(url, direct_referer):
                    raise
                self._set_task_parse_ui(item_id, message=f"Direct media download failed: {str(e)[:30]}")

        if "jable.tv" in parsed_url.netloc:
            self._set_task_parse_ui(item_id, key="eta_site_jable", fallback="正在解析 Jable...")
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
            _set_task_identity(name=clean_title, source_site="jable")
            self._set_task_status_mode_ui(item_id, t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中", self._ui_text("eta_found_stream", "已取得串流網址"))
            self._log_m3u8_route_selected(task, item_id, url, source_site="jable", fallback_urls=[])
            _download_manifest_with_site_strategy(
                url,
                referer="https://jable.tv/",
                origin="https://jable.tv",
                default_route="ffmpeg",
            )
            return

        if "njavtv.com" in parsed_url.netloc:
            self._set_task_parse_ui(item_id, key="eta_site_njavtv", fallback="正在解析 NJAVTV...")
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
            title = _extract_html_title(resp.text, title)
            _set_task_identity(name=title, source_site="njavtv")
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="njavtv", fallback_urls=[])
            _download_manifest_with_site_strategy(
                stream_url,
                referer=url,
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                default_route=_gimy_manifest_default_route(stream_url),
            )
            return

        if "nnyy.in" in parsed_url.netloc and re.search(r"/(?:dianying|dianshiju|zongyi|dongman)/\d+\.html$", parsed_url.path, re.IGNORECASE):
            self._set_task_parse_ui(item_id, key="eta_site_nnyy", fallback="正在解析 努努影院...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(
                url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer="https://nnyy.in/"),
            )
            page_id = _extract_nnyy_page_id(url)
            if not page_id:
                raise Exception("NNYY page id missing")
            episode_entries, default_slug = _extract_nnyy_episode_entries(resp.text)
            query_slug = urllib.parse.parse_qs(parsed_url.query).get("ep", [""])[0].strip()
            selected_slug = query_slug or default_slug or (episode_entries[0][0] if episode_entries else "")
            if not selected_slug:
                raise Exception("NNYY episode slug missing")
            api_url = f"https://nnyy.in/_gp/{page_id}/{selected_slug}"
            api_resp = c_req.get(
                api_url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ajax_http_headers(referer=url),
            )
            candidates = _extract_nnyy_play_candidates(api_resp.text)
            if not candidates:
                raise Exception("NNYY stream URL missing")
            page_title = _extract_html_title(resp.text, short_name or "努努影院")
            episode_pad_width = max(2, len(str(len(episode_entries) or 0)))
            selected_episode_name = next((name for slug, name in episode_entries if slug == selected_slug), "")
            selected_episode_name = _normalize_nnyy_episode_name(selected_episode_name, ep_slug=selected_slug, pad_width=episode_pad_width)
            display_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
            auto_generated_name = default_short_name_for_url(url, is_mp3=is_mp3)
            if (
                not display_name
                or display_name == auto_generated_name
                or is_auto_generated_short_name(url, display_name, is_mp3=is_mp3)
                or display_name.lower().endswith(".html")
            ):
                display_name = f"{page_title} {selected_episode_name}".strip() if selected_episode_name else page_title
            fallback_urls = candidates[1:]
            _set_task_identity(name=display_name, source_site="nnyy", source_page=url, fallback_urls=fallback_urls)
            self._log_m3u8_route_selected(task, item_id, candidates[0], source_site="nnyy", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                candidates[0],
                referer=url,
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                default_route="ffmpeg",
                force_ffmpeg=True,
            )
            return

        if "777tv.ai" in parsed_url.netloc and re.search(r"/vod/detail/id/\d+\.html$", parsed_url.path, re.IGNORECASE):
            self._set_task_parse_ui(item_id, message="正在解析 777TV...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(
                url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer="https://777tv.ai/"),
            )
            page_title = _extract_html_title(resp.text, short_name or "777TV")
            page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or (short_name or "777TV")
            episode_entries = _extract_777tv_episode_entries(resp.text)
            if episode_entries:
                episodes = []
                for play_url, ep_name in episode_entries:
                    display_name = f"{page_title} {ep_name}".strip() if ep_name else page_title
                    episodes.append((play_url, display_name))
                episodes = _sort_download_targets_naturally(episodes)
                primary_url, primary_name = episodes[0]
                fallback_urls = [episode_url for episode_url, _episode_name in episodes if episode_url != primary_url]
                _set_task_identity(
                    name=str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip() or primary_name,
                    source_site="777tv",
                    source_page=url,
                    fallback_urls=fallback_urls,
                )
                self._download_task_internal(primary_url, item_id, save_dir, use_impersonate, is_mp3)
                return
            page_title, candidates, _player_data = _extract_777tv_playback_candidates(resp.text, page_title)
            if not candidates:
                raise Exception("777TV detail page did not expose episode links")
            _set_task_identity(name=page_title, source_site="777tv", source_page=url, fallback_urls=candidates[1:])
            self._log_m3u8_route_selected(task, item_id, candidates[0], source_site="777tv", fallback_urls=candidates[1:])
            _download_manifest_with_site_strategy(
                candidates[0],
                referer="https://777tv.ai/",
                origin="https://777tv.ai",
                default_route="ffmpeg",
            )
            return

        if "play.777tv.ai" in parsed_url.netloc and re.search(r"/vod/play/id/\d+/sid/\d+/nid/\d+\.html$", parsed_url.path, re.IGNORECASE):
            self._set_task_parse_ui(item_id, message="正在解析 777TV...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(
                url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer=self._get_task_source_page(task, fallback_url="https://777tv.ai/") or "https://777tv.ai/"),
            )
            page_title, candidates, _player_data = _extract_777tv_playback_candidates(resp.text, short_name or "777TV")
            if not candidates:
                raise Exception("777TV stream URL missing")
            display_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip() or page_title or short_name or "777TV"
            fallback_urls = candidates[1:]
            _set_task_identity(name=display_name, source_site="777tv", source_page=url, fallback_urls=fallback_urls)
            self._log_m3u8_route_selected(task, item_id, candidates[0], source_site="777tv", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                candidates[0],
                referer=url,
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                default_route="ffmpeg",
            )
            return

        if "3kor.com" in parsed_url.netloc and re.search(r"/list/\d+[^/]*\.html$", parsed_url.path, re.IGNORECASE):
            self._set_task_parse_ui(item_id, message="正在解析 3KOR...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(
                url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer="https://3kor.com/"),
            )
            detail_entries = _extract_3kor_detail_entries(resp.text)
            if not detail_entries:
                raise Exception("3KOR list page did not expose detail links")
            primary_url, primary_name = detail_entries[0]
            fallback_urls = [detail_url for detail_url, _detail_name in detail_entries if detail_url != primary_url]
            _set_task_identity(
                name=str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip() or primary_name,
                source_site="3kor",
                source_page=url,
                fallback_urls=fallback_urls,
            )
            self._download_task_internal(primary_url, item_id, save_dir, use_impersonate, is_mp3)
            return

        if "3kor.com" in parsed_url.netloc and re.search(r"/detail/\d+\.html$", parsed_url.path, re.IGNORECASE):
            self._set_task_parse_ui(item_id, message="正在解析 3KOR...")
            c_req = get_curl_cffi_requests()
            detail_url = urllib.parse.urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
            resp = c_req.get(
                detail_url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer="https://3kor.com/"),
            )
            page_title = _extract_html_title(resp.text, short_name or "3KOR")
            page_title = "".join(c for c in page_title if c not in '\\/:*?"<>|').strip() or (short_name or "3KOR")
            play_entries = _extract_3kor_play_entries(resp.text)
            if not play_entries:
                raise Exception("3KOR play entry missing")
            requested_play_id = urllib.parse.parse_qs(parsed_url.query).get("play", [""])[0].strip()
            selected_play_id, selected_play_name = next((entry for entry in play_entries if entry[0] == requested_play_id), play_entries[0])
            api_url = f"https://3kor.com/u/u1.php?ud={urllib.parse.quote(selected_play_id, safe='')}"
            encrypted_stream = c_req.get(
                api_url,
                impersonate="chrome110",
                timeout=20,
                headers=_make_ytdlp_http_headers(referer=detail_url),
            ).text
            direct_stream_url = _decrypt_3kor_stream_url(encrypted_stream, "my-to-newhan-2025")
            if not direct_stream_url:
                raise Exception("3KOR stream URL missing")
            stream_url = urllib.parse.urljoin(
                "https://3kor.com/",
                "/m3/edit-down.php?url=" + urllib.parse.quote(direct_stream_url, safe=':/?&=%'),
            )
            display_name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or "").strip()
            if not display_name:
                display_name = page_title if len(play_entries) == 1 else f"{page_title} {selected_play_name}".strip()
            fallback_urls = [
                f"{detail_url}?play={urllib.parse.quote(play_id)}"
                for play_id, _play_name in play_entries
                if play_id != selected_play_id
            ]
            _set_task_identity(name=display_name, source_site="3kor", source_page=detail_url, fallback_urls=fallback_urls)
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="3kor", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                stream_url,
                referer=detail_url,
                origin="https://3kor.com",
                default_route="ffmpeg",
            )
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
                _set_task_identity(source_site="xiaoyakankan", fallback_urls=[u for u in fallback_urls if u], source_page=url)
                task_fallback_urls = _dedupe_download_urls(_task_field_value(task, "fallback_urls", []))
                write_error_log("xiaoyakankan parse success", Exception("xiaoyakankan parse success"), url=url, item_id=item_id, stream_url=m3u8_url, fallback_count=len(task_fallback_urls))
                self._log_m3u8_route_selected(task, item_id, m3u8_url, source_site="xiaoyakankan", fallback_urls=task_fallback_urls)
                _download_manifest_with_site_strategy(
                    m3u8_url,
                    referer=url,
                    origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    default_route="ffmpeg",
                )
                return
            except Exception as e:
                write_error_log("xiaoyakankan parse failure", e, url=url, item_id=item_id)
                self._set_task_parse_ui(item_id, error=e)

        if "movieffm.net" in parsed_url.netloc and "/drama/" not in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_site_movieffm", fallback="正在解析 MovieFFM 頁面...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": "https://www.movieffm.net/"})
            def _movieffm_api_fallback_candidates(page_text, page_link):
                post_id_match = re.search(r"\bpostid\s*:\s*(\d+)", str(page_text or ""), re.IGNORECASE)
                post_id = str(post_id_match.group(1) or "").strip() if post_id_match else ""
                if not post_id:
                    return [], []
                api_base = f"https://www.movieffm.net/wp-json/dooplayer/v1/post/{post_id}"
                manifest_candidates = []
                external_candidates = []
                page_headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": page_link}
                for source_index in range(0, 8):
                    api_url = f"{api_base}?type=movie&source={source_index}"
                    try:
                        api_resp = c_req.get(api_url, impersonate="chrome110", timeout=15, headers=page_headers)
                        api_data = api_resp.json()
                    except Exception:
                        continue
                    embed_url = _normalize_download_url((api_data or {}).get("embed_url"))
                    embed_type = str((api_data or {}).get("type") or "").strip().lower()
                    if not embed_url:
                        continue
                    if embed_type == "iframe":
                        external_candidates.append(embed_url)
                        continue
                    if "movieffm.net/ap/" in embed_url:
                        try:
                            ap_resp = c_req.get(embed_url, impersonate="chrome110", timeout=20, headers=page_headers)
                            extracted = _extract_movieffm_m3u8_candidates(ap_resp.text)
                            if not extracted:
                                extracted = _extract_candidate_media_urls(ap_resp.text, allowed_exts=(".mp4", ".m3u8", ".mpd"))
                            for candidate in extracted:
                                normalized_candidate = _normalize_download_url(candidate)
                                if normalized_candidate:
                                    manifest_candidates.append(normalized_candidate)
                        except Exception:
                            pass
                        continue
                    if _looks_like_manifest_url(embed_url) or _looks_like_http_media_url(embed_url):
                        manifest_candidates.append(embed_url)
                return _dedupe_download_urls(manifest_candidates), _dedupe_download_urls(external_candidates)
            if "/tvshows/" in parsed_url.path:
                _, detail_pages = _collect_movieffm_tvshow_detail_pages(resp.text, url, short_name or "MovieFFM")
                if not detail_pages:
                    raise Exception("MovieFFM tvshows page did not expose detail pages")
                detail_url, _season_name = detail_pages[0]
                detail_resp = c_req.get(detail_url, impersonate="chrome110", timeout=20, headers={"Referer": detail_url})
                _drama_title, episodes, episode_fallbacks = _collect_movieffm_drama_episodes(detail_resp.text, detail_url, short_name or "MovieFFM")
                if not episodes:
                    raise Exception("MovieFFM tvshows detail page did not expose episode links")
                primary_url, primary_name = episodes[0]
                episode_key = _movieffm_numbered_episode_key(primary_name.rsplit(" ", 1)[-1])
                fallback_urls = [u for u in episode_fallbacks.get(episode_key, []) if u != primary_url]
                preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                    primary_url,
                    fallback_urls,
                    source_site="movieffm",
                )
                if not preferred_url or not has_reachable:
                    raise Exception("MovieFFM stream host did not resolve")
                _set_task_identity(
                    name=primary_name,
                    source_site="movieffm",
                    source_page=detail_url,
                    fallback_urls=ordered_fallbacks,
                )
                self._download_task_internal(preferred_url, item_id, save_dir, use_impersonate, is_mp3)
                return
            page_title, candidates, external_source_urls, player_data = _extract_movieffm_playback_candidates(resp.text, short_name)
            api_candidates, api_external_source_urls = _movieffm_api_fallback_candidates(resp.text, url)
            for candidate in api_candidates:
                if candidate not in candidates:
                    candidates.append(candidate)
            for candidate in api_external_source_urls:
                if candidate not in external_source_urls:
                    external_source_urls.append(candidate)
            if not candidates and not player_data and not external_source_urls:
                raise Exception("MovieFFM player data not found")
            candidates = [
                candidate for candidate in candidates
                if not _movieffm_manifest_candidate_is_dead(candidate, referer=url)
            ]
            if not candidates and external_source_urls:
                preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                    external_source_urls[0],
                    external_source_urls[1:],
                    source_site="movieffm",
                )
                if not preferred_url or not has_reachable:
                    raise Exception("MovieFFM stream host did not resolve")
                _set_task_identity(name=page_title, source_site="movieffm", source_page=url, fallback_urls=ordered_fallbacks)
                return self._download_task_internal(preferred_url, item_id, save_dir, use_impersonate=use_impersonate, is_mp3=is_mp3)
            if not candidates:
                raise Exception("MovieFFM stream unavailable")
            preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                candidates[0],
                candidates[1:],
                source_site="movieffm",
            )
            if not preferred_url or not has_reachable:
                raise Exception("MovieFFM stream host did not resolve")
            _set_task_identity(name=page_title, source_site="movieffm", source_page=url, fallback_urls=ordered_fallbacks)
            self._log_m3u8_route_selected(task, item_id, preferred_url, source_site="movieffm", fallback_urls=ordered_fallbacks)
            parsed_page = urllib.parse.urlsplit(url)
            page_origin = f"{parsed_page.scheme}://{parsed_page.netloc}" if parsed_page.scheme and parsed_page.netloc else "https://www.movieffm.net"
            _download_manifest_with_site_strategy(
                preferred_url,
                referer=url,
                origin=page_origin,
                force_ffmpeg=_should_use_ffmpeg_for_movieffm_manifest(preferred_url),
            )
            return

        if "movieffm.net" in parsed_url.netloc and "/drama/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_site_movieffm", fallback="正在解析 MovieFFM 頁面...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            def _movieffm_api_fallback_candidates(page_text, page_link):
                post_id_match = re.search(r"\bpostid\s*:\s*(\d+)", str(page_text or ""), re.IGNORECASE)
                post_id = str(post_id_match.group(1) or "").strip() if post_id_match else ""
                if not post_id:
                    return [], []
                api_base = f"https://www.movieffm.net/wp-json/dooplayer/v1/post/{post_id}"
                manifest_candidates = []
                external_candidates = []
                page_headers = {"User-Agent": DEFAULT_USER_AGENT, "Referer": page_link}
                for source_index in range(0, 8):
                    api_url = f"{api_base}?type=movie&source={source_index}"
                    try:
                        api_resp = c_req.get(api_url, impersonate="chrome110", timeout=15, headers=page_headers)
                        api_data = api_resp.json()
                    except Exception:
                        continue
                    embed_url = _normalize_download_url((api_data or {}).get("embed_url"))
                    embed_type = str((api_data or {}).get("type") or "").strip().lower()
                    if not embed_url:
                        continue
                    if embed_type == "iframe":
                        external_candidates.append(embed_url)
                        continue
                    if "movieffm.net/ap/" in embed_url:
                        try:
                            ap_resp = c_req.get(embed_url, impersonate="chrome110", timeout=20, headers=page_headers)
                            extracted = _extract_movieffm_m3u8_candidates(ap_resp.text)
                            if not extracted:
                                extracted = _extract_candidate_media_urls(ap_resp.text, allowed_exts=(".mp4", ".m3u8", ".mpd"))
                            for candidate in extracted:
                                normalized_candidate = _normalize_download_url(candidate)
                                if normalized_candidate:
                                    manifest_candidates.append(normalized_candidate)
                        except Exception:
                            pass
                        continue
                    if _looks_like_manifest_url(embed_url) or _looks_like_http_media_url(embed_url):
                        manifest_candidates.append(embed_url)
                return _dedupe_download_urls(manifest_candidates), _dedupe_download_urls(external_candidates)
            drama_title, episodes, episode_fallbacks = _collect_movieffm_drama_episodes(resp.text, url, short_name or "MovieFFM")
            if not episodes:
                page_title, candidates, external_source_urls, player_data = _extract_movieffm_playback_candidates(resp.text, drama_title or short_name)
                api_candidates, api_external_source_urls = _movieffm_api_fallback_candidates(resp.text, url)
                for candidate in api_candidates:
                    if candidate not in candidates:
                        candidates.append(candidate)
                for candidate in api_external_source_urls:
                    if candidate not in external_source_urls:
                        external_source_urls.append(candidate)
                if not candidates and not player_data and not external_source_urls:
                    raise Exception("MovieFFM detail page did not expose episode links")
                candidates = [
                    candidate for candidate in candidates
                    if not _movieffm_manifest_candidate_is_dead(candidate, referer=url)
                ]
                if not candidates and external_source_urls:
                    preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(
                        external_source_urls[0],
                        external_source_urls[1:],
                        source_site="movieffm",
                    )
                    if not preferred_url or not has_reachable:
                        raise Exception("MovieFFM stream host did not resolve")
                    _set_task_identity(name=page_title, source_site="movieffm", source_page=url, fallback_urls=ordered_fallbacks)
                    return self._download_task_internal(preferred_url, item_id, save_dir, use_impersonate=use_impersonate, is_mp3=is_mp3)
                if not candidates:
                    raise Exception("MovieFFM detail page stream unavailable")
                preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(candidates[0], candidates[1:], source_site="movieffm")
                if not preferred_url or not has_reachable:
                    raise Exception("MovieFFM stream host did not resolve")
                _set_task_identity(name=page_title, source_site="movieffm", source_page=url, fallback_urls=ordered_fallbacks)
                self._log_m3u8_route_selected(task, item_id, preferred_url, source_site="movieffm", fallback_urls=ordered_fallbacks)
                parsed_page = urllib.parse.urlsplit(url)
                page_origin = f"{parsed_page.scheme}://{parsed_page.netloc}" if parsed_page.scheme and parsed_page.netloc else "https://www.movieffm.net"
                _download_manifest_with_site_strategy(
                    preferred_url,
                    referer=url,
                    origin=page_origin,
                    force_ffmpeg=_should_use_ffmpeg_for_movieffm_manifest(preferred_url),
                )
                return
            primary_url, primary_name = episodes[0]
            episode_key = _movieffm_numbered_episode_key(primary_name.rsplit(" ", 1)[-1])
            fallback_urls = [u for u in episode_fallbacks.get(episode_key, []) if u != primary_url]
            preferred_url, ordered_fallbacks, has_reachable = _select_reachable_stream_candidates(primary_url, fallback_urls, source_site="movieffm")
            if not preferred_url or not has_reachable:
                raise Exception("MovieFFM episode stream host did not resolve")
            _set_task_identity(
                name=primary_name,
                source_site="movieffm",
                source_page=url,
                fallback_urls=ordered_fallbacks,
            )
            self._download_task_internal(preferred_url, item_id, save_dir, use_impersonate, is_mp3)
            return

        if "gimy" in parsed_url.netloc and ("/detail/" in parsed_url.path or "/voddetail/" in parsed_url.path or "/voddetail2/" in parsed_url.path or "/vod/" in parsed_url.path):
            self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在解析 Gimy 頁面...")
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
            if not re.search(
                r'href=[\"\'](/(?:(?:vod)?play/[0-9]+\-[0-9]+\-[0-9]+\.html|video/[0-9]+\-[0-9]+\.html(?:#sid=\d+)?|eps/[0-9]+\-[0-9]+(?:\-[0-9]+)?\.html))[\"\'][^>]*>(.*?)</a>',
                resp_text,
            ):
                raise Exception("Gimy detail page did not expose episode links")
            drama_name = _clean_gimy_title(short_name or "Gimy")
            title_match = re.search(r"<title>(.*?)</title>", resp_text, re.IGNORECASE | re.DOTALL)
            if title_match:
                drama_name = _clean_gimy_title(html.unescape(title_match.group(1)).split("-")[0].strip()) or drama_name
                drama_name = "".join(c for c in drama_name if c not in '\\/:*?"<>|')
            entries = self._extract_gimy_detail_entries(resp_text, base, drama_name)
            if not entries:
                raise Exception("Gimy detail page did not expose a playable episode")
            refresh_history = _task_gimy_refresh_history(task)
            if self._is_gimy_movie_detail(entries):
                ordered_entries = sorted(entries, key=lambda entry: self._gimy_movie_source_priority(entry.get("title", "")))
                primary = next(
                    (
                        entry for entry in ordered_entries
                        if _normalize_download_url(entry.get("url")) not in refresh_history
                    ),
                    ordered_entries[0],
                )
                first_episode_url = primary["url"]
                first_episode_name = drama_name
                fallback_urls = [
                    entry["url"] for entry in ordered_entries
                    if entry["url"] != primary["url"]
                ]
                ordered_episode_urls = [first_episode_url] + [candidate for candidate in fallback_urls if candidate != first_episode_url]
            else:
                episode_entries = self._group_gimy_episode_entries(entries)
                primary = next(
                    (
                        entry for entry in episode_entries
                        if _normalize_download_url(entry.get("url")) not in refresh_history
                    ),
                    episode_entries[0],
                )
                first_episode_url = primary["url"]
                first_episode_name = primary["full_name"]
                fallback_urls = list(primary.get("fallback_urls", []))
                for entry in episode_entries:
                    entry_url = entry.get("url")
                    if entry_url and entry_url != first_episode_url and entry_url not in fallback_urls:
                        fallback_urls.append(entry_url)
                ordered_episode_urls = [first_episode_url] + [candidate for candidate in fallback_urls if candidate != first_episode_url]
            _set_task_aux_fields(task, _gimy_source_refresh_history=[])
            _set_task_identity(name=_clean_gimy_title(first_episode_name or drama_name), source_site="gimy", source_page=url, fallback_urls=fallback_urls)
            last_episode_error = None
            for attempt_index, episode_url in enumerate(ordered_episode_urls):
                try:
                    self._download_task_internal(episode_url, item_id, save_dir, use_impersonate, is_mp3)
                    return
                except Exception as episode_exc:
                    last_episode_error = episode_exc
                    episode_exc_text = str(episode_exc or "")
                    if "Gimy stream URL missing" not in episode_exc_text and "Gimy iframe stream URL missing" not in episode_exc_text:
                        raise
                    refresh_history = _append_normalized_unique_candidates(
                        _task_gimy_refresh_history(task),
                        episode_url,
                    )
                    _set_task_aux_fields(task, _gimy_refresh_history=refresh_history)
                    if attempt_index < len(ordered_episode_urls) - 1:
                        continue
                    raise
            if last_episode_error is not None:
                raise last_episode_error
            return

        if "gimy" in parsed_url.netloc and "/eps/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在解析 Gimy 頁面...")
            c_req = get_curl_cffi_requests()
            stream_candidates = []
            direct_fallback_candidates = []
            external_source_urls = []
            deferred_episode_urls = []
            gimy_failed_stream_urls = set(_task_gimy_failed_stream_urls(task))
            gimy_failed_stream_hosts = set(_task_gimy_failed_stream_hosts(task))
            last_gimy_error = None
            page_title = _clean_gimy_title(short_name or "Gimy")
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
            def extend_gimy_stream_candidates(page_link, page_text, referer_value, impersonate_name):
                local_candidates = []
                local_direct_urls = []
                local_external_urls = []
                local_player_data = None
                local_error = None
                local_title = ""
                try:
                    local_player_data = _safe_extract_player_js_object(page_text, "player_data", "player_aaaa", "player")
                except Exception as inner_exc:
                    local_error = inner_exc
                if local_player_data:
                    direct_url = _normalize_download_url(local_player_data.get("url"))
                    if direct_url:
                        if direct_url.lower().endswith(".m3u8"):
                            local_direct_urls.append(direct_url)
                        elif re.match(r"^https?://", direct_url, re.IGNORECASE):
                            local_external_urls.append(direct_url)
                    for candidate_url in _collect_player_m3u8_candidates(local_player_data, base_url=page_link):
                        if candidate_url not in local_candidates:
                            local_candidates.append(candidate_url)
                    player_title = (local_player_data.get("vod_data") or {}).get("vod_name")
                    if player_title:
                        local_title = re.sub(r"\s+", " ", str(player_title)).strip()
                iframe_urls = _extract_gimy_inline_iframe_urls(page_text, page_link)
                if local_player_data:
                    for iframe_url in _build_gimy_iframe_urls(page_link, local_player_data):
                        if iframe_url not in iframe_urls:
                            iframe_urls.append(iframe_url)
                for iframe_url in iframe_urls:
                    try:
                        iframe_text = gimy_fetch_text(iframe_url, referer_value, impersonate_name)
                    except Exception as inner_exc:
                        local_error = inner_exc
                        continue
                    iframe_player_data = _safe_extract_player_js_object(iframe_text, "player_data", "player_aaaa", "player")
                    if iframe_player_data:
                        iframe_direct_url = _normalize_download_url(iframe_player_data.get("url"))
                        if iframe_direct_url:
                            candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(iframe_direct_url)
                            if candidate_kind == "manifest" and normalized_candidate and normalized_candidate not in local_direct_urls:
                                local_direct_urls.append(normalized_candidate)
                            elif candidate_kind == "external" and normalized_candidate and normalized_candidate not in local_external_urls:
                                local_external_urls.append(normalized_candidate)
                        for candidate_url in _collect_player_m3u8_candidates(iframe_player_data, base_url=iframe_url):
                            if candidate_url not in local_candidates:
                                local_candidates.append(candidate_url)
                    for stream_url in _extract_m3u8_candidates_from_text(iframe_text, base_url=iframe_url):
                        if stream_url not in local_candidates:
                            local_candidates.append(stream_url)
                    parse_source = urllib.parse.parse_qs(urllib.parse.urlsplit(iframe_url).query).get("url", [""])[0]
                    if parse_source and "parse.php" in iframe_text:
                        parse_api = urllib.parse.urljoin(iframe_url, f"parse.php?url={urllib.parse.quote(parse_source, safe='')}")
                        try:
                            parse_text = gimy_fetch_text(parse_api, iframe_url, impersonate_name)
                            for parsed_candidate in _extract_gimy_parse_candidates(parse_text, base_url=parse_api):
                                candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(parsed_candidate)
                                if candidate_kind == "manifest":
                                    if normalized_candidate not in local_candidates:
                                        local_candidates.append(normalized_candidate)
                                elif candidate_kind == "external":
                                    if normalized_candidate not in local_external_urls:
                                        local_external_urls.append(normalized_candidate)
                        except Exception as inner_exc:
                            local_error = inner_exc
                for direct_stream in _extract_m3u8_candidates_from_text(page_text, base_url=page_link):
                    if direct_stream not in local_candidates:
                        local_candidates.append(direct_stream)
                return {
                    "candidates": [
                        candidate for candidate in _dedupe_download_urls(local_candidates)
                        if candidate not in gimy_failed_stream_urls
                        and urllib.parse.urlsplit(candidate).netloc.lower() not in gimy_failed_stream_hosts
                    ],
                    "direct_urls": _dedupe_download_urls(local_direct_urls),
                    "external_urls": _dedupe_download_urls(local_external_urls),
                    "title": local_title,
                    "error": local_error,
                }
            for gimy_impersonate in ("chrome110", "chrome120", "edge101"):
                try:
                    resp_text = gimy_fetch_text(url, url, gimy_impersonate)
                except Exception as e:
                    last_gimy_error = e
                    continue
                current_result = extend_gimy_stream_candidates(url, resp_text, url, gimy_impersonate)
                if current_result["error"] is not None:
                    last_gimy_error = current_result["error"]
                if current_result["title"]:
                    page_title = _clean_gimy_title(current_result["title"]) or page_title
                for candidate_url in current_result["candidates"]:
                    if candidate_url not in stream_candidates:
                        stream_candidates.append(candidate_url)
                for candidate_url in current_result["direct_urls"]:
                    if candidate_url not in direct_fallback_candidates:
                        direct_fallback_candidates.append(candidate_url)
                for candidate_url in current_result["external_urls"]:
                    if candidate_url not in external_source_urls:
                        external_source_urls.append(candidate_url)
                parsed_page = urllib.parse.urlsplit(str(url or ""))
                page_base = f"{parsed_page.scheme or 'https'}://{parsed_page.netloc or 'gimy01.tv'}"
                current_nid = None
                current_match = re.search(r"/eps/\d+-(\d+)(?:-(\d+))?\.html", str(url or ""))
                if current_match:
                    current_nid = current_match.group(2) or current_match.group(1)
                alternate_episode_urls = []
                for match in re.finditer(r'href=["\'](/eps/\d+-(\d+)(?:-(\d+))?\.html)["\']', str(resp_text or ""), re.IGNORECASE):
                    relative_url = match.group(1)
                    nid = match.group(3) or match.group(2)
                    if current_nid and nid != current_nid:
                        continue
                    full_url = _normalize_download_url(urllib.parse.urljoin(page_base, relative_url))
                    if not full_url or full_url == _normalize_download_url(url):
                        continue
                    if full_url not in alternate_episode_urls:
                        alternate_episode_urls.append(full_url)
                for alternate_episode_url in alternate_episode_urls:
                    if alternate_episode_url not in deferred_episode_urls:
                        deferred_episode_urls.append(alternate_episode_url)
                if not (stream_candidates or direct_fallback_candidates or external_source_urls):
                    for alternate_episode_url in alternate_episode_urls[:18]:
                        try:
                            alternate_text = gimy_fetch_text(alternate_episode_url, url, gimy_impersonate)
                        except Exception as e:
                            last_gimy_error = e
                            continue
                        alternate_result = extend_gimy_stream_candidates(alternate_episode_url, alternate_text, url, gimy_impersonate)
                        if alternate_result["error"] is not None:
                            last_gimy_error = alternate_result["error"]
                        for candidate_url in alternate_result["candidates"]:
                            if candidate_url not in stream_candidates:
                                stream_candidates.append(candidate_url)
                        for candidate_url in alternate_result["direct_urls"]:
                            if candidate_url not in direct_fallback_candidates:
                                direct_fallback_candidates.append(candidate_url)
                        for candidate_url in alternate_result["external_urls"]:
                            if candidate_url not in external_source_urls:
                                external_source_urls.append(candidate_url)

                if (
                    stream_candidates
                    or direct_fallback_candidates
                    or any(_looks_like_http_media_url(candidate) for candidate in external_source_urls)
                ):
                    break
                if "播放失效" in resp_text or "播放失败" in resp_text or '<p class="p-2 text-error"' in resp_text:
                    last_gimy_error = Exception("Gimy episode page reports playback failure")
                    continue
            raw_direct_fallback_candidates = _dedupe_download_urls(direct_fallback_candidates)
            raw_external_source_urls = _dedupe_download_urls(external_source_urls)
            direct_fallback_candidates, external_source_urls = _filter_gimy_candidate_groups(
                task,
                direct_fallback_candidates,
                external_source_urls,
            )
            ordered_direct_candidates = _dedupe_download_urls(stream_candidates + direct_fallback_candidates)
            if not ordered_direct_candidates and raw_direct_fallback_candidates:
                ordered_direct_candidates = raw_direct_fallback_candidates
            if not external_source_urls and raw_external_source_urls:
                external_source_urls = raw_external_source_urls
            preferred_media_urls = [candidate for candidate in external_source_urls if _looks_like_http_media_url(candidate)]
            if preferred_media_urls:
                media_url = preferred_media_urls[0]
                direct_media_fallback_urls = [
                    candidate
                    for candidate in (preferred_media_urls[1:] + ordered_direct_candidates + deferred_episode_urls)
                    if candidate and candidate != media_url and candidate != url
                ]
                _set_task_aux_fields(task, _gimy_page_refresh_candidates=_filter_gimy_untried_page_candidates(task, deferred_episode_urls), _gimy_source_refresh_history=[])
                _set_task_identity(
                    name=page_title,
                    source_site="gimy",
                    source_page=url,
                    fallback_urls=direct_media_fallback_urls,
                )
                if is_mp3:
                    self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                    self._download_direct_media_audio_with_ffmpeg(
                        item_id,
                        media_url,
                        save_dir,
                        referer=url,
                        origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    )
                    return
                media_ext = _infer_media_extension_from_url(media_url) or ".mp4"
                name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or page_title or "Video" or "").strip()
                out_path, out_name = self._resolve_direct_media_output_path(media_url, save_dir, name, default_ext=media_ext)
                _set_task_aux_fields(task, filename=out_path)
                self._set_task_named_column_text(item_id, "name", out_name)
                self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                self._download_http_media(
                    item_id,
                    media_url,
                    out_path,
                    headers=self._gimy_direct_media_headers(url, f"{parsed_url.scheme}://{parsed_url.netloc}"),
                )
                return
            supported_external_pages = [
                candidate for candidate in external_source_urls
                if any(
                    marker in urllib.parse.urlsplit(_normalize_download_url(candidate)).netloc.lower()
                    for marker in SUPPORTED_DOWNLOAD_PAGE_NETLOC_MARKERS
                )
            ]
            if not ordered_direct_candidates and supported_external_pages:
                external_url = supported_external_pages[0]
                fallback_urls = [candidate for candidate in (supported_external_pages[1:] + deferred_episode_urls) if candidate and candidate != external_url]
                _set_task_aux_fields(task, _gimy_page_refresh_candidates=_filter_gimy_untried_page_candidates(task, deferred_episode_urls), _gimy_source_refresh_history=[])
                _set_task_identity(name=_clean_gimy_title(page_title), source_site="gimy", source_page=url, fallback_urls=fallback_urls)
                self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                self._download_task_internal(external_url, item_id, save_dir, use_impersonate, is_mp3)
                return
            if not ordered_direct_candidates:
                current_page_url = _normalize_download_url(_task_field_value(task, "url", "")) or _normalize_download_url(url)
                source_page_url = self._get_task_source_page(task, fallback_url=current_page_url)
                refresh_history = _task_gimy_refresh_history(task)
                for current_candidate_url in (current_page_url, source_page_url):
                    normalized_current_candidate = _normalize_download_url(current_candidate_url)
                    if normalized_current_candidate and normalized_current_candidate not in refresh_history:
                        refresh_history.append(normalized_current_candidate)
                episode_refresh_attempts = sum(
                    1 for candidate in refresh_history
                    if "/eps/" in urllib.parse.urlsplit(_normalize_download_url(candidate)).path.lower()
                )
                if "/eps/" in urllib.parse.urlsplit(_normalize_download_url(current_page_url or url)).path.lower():
                    episode_refresh_attempts = max(episode_refresh_attempts - 1, 0)
                available_episode_page_candidates = []
                for candidate in deferred_episode_urls:
                    normalized_candidate = _normalize_download_url(candidate)
                    if normalized_candidate and normalized_candidate not in refresh_history:
                        available_episode_page_candidates.append(normalized_candidate)
                if available_episode_page_candidates and episode_refresh_attempts < GIMY_EPISODE_PAGE_PARSE_FALLBACK_LIMIT:
                    refresh_url = available_episode_page_candidates[0]
                    _set_task_aux_fields(
                        task,
                        _gimy_refresh_history=refresh_history + [refresh_url],
                        _gimy_page_refresh_candidates=[
                            candidate for candidate in available_episode_page_candidates[1:]
                            if candidate != refresh_url
                        ],
                        _gimy_failed_stream_urls=[],
                        _gimy_failed_stream_hosts=[],
                    )
                    self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在重新取得 Gimy 串流...")
                    write_error_log(
                        "gimy episode page refresh",
                        Exception("refreshing gimy episode page after parse-stage source mismatch"),
                        item_id=item_id,
                        page_url=current_page_url or url,
                        refresh_url=refresh_url,
                        refresh_attempts=episode_refresh_attempts + 1,
                        deferred_count=len(available_episode_page_candidates),
                )
                return self._download_task_internal(refresh_url, item_id, save_dir, use_impersonate, is_mp3)
            if not ordered_direct_candidates and not bool(_task_field_value(task, "_gimy_detail_refresh_done", False)):
                detail_page_candidates = []
                for page_url in (
                    self._get_task_source_page(task, fallback_url=url) or url,
                    current_page_url,
                    source_page_url,
                    url,
                ):
                    normalized_page_url = _normalize_download_url(page_url)
                    if not normalized_page_url:
                        continue
                    parsed = urllib.parse.urlsplit(normalized_page_url)
                    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'gimy01.tv'}"
                    path = parsed.path or ""
                    for pattern in (r"/eps/(\d+)-\d+(?:-\d+)?\.html", r"/(?:play|vodplay|video)/(\d+)-\d+(?:-\d+)?\.html"):
                        match = re.search(pattern, path)
                        if not match:
                            continue
                        vod_id = match.group(1)
                        for relative_path in (f"/vod/{vod_id}.html", f"/detail/{vod_id}.html", f"/voddetail/{vod_id}.html"):
                            normalized_candidate = _normalize_download_url(urllib.parse.urljoin(base, relative_path))
                            if normalized_candidate and normalized_candidate not in detail_page_candidates:
                                detail_page_candidates.append(normalized_candidate)
                if detail_page_candidates:
                    detail_refresh_url = detail_page_candidates[0]
                    normalized_detail_refresh_url = _normalize_download_url(detail_refresh_url)
                    detail_refresh_history = list(refresh_history)
                    if normalized_detail_refresh_url and normalized_detail_refresh_url not in detail_refresh_history:
                        detail_refresh_history.append(normalized_detail_refresh_url)
                    _set_task_aux_fields(
                        task,
                        _gimy_detail_refresh_done=True,
                        _gimy_refresh_history=detail_refresh_history,
                        _gimy_page_refresh_candidates=[],
                        _gimy_failed_stream_urls=[],
                        _gimy_failed_stream_hosts=[],
                    )
                    self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在解析 Gimy...")
                    write_error_log(
                        "gimy detail page rebuild",
                        Exception("rebuilding gimy episode sources after parse-stage source mismatch"),
                        item_id=item_id,
                        page_url=url,
                        refresh_url=detail_refresh_url,
                        deferred_count=len(deferred_episode_urls),
                    )
                    return self._download_task_internal(detail_refresh_url, item_id, save_dir, use_impersonate, is_mp3)
                if available_episode_page_candidates:
                    for alternate_episode_url in available_episode_page_candidates[:18]:
                        try:
                            alternate_text = gimy_fetch_text(alternate_episode_url, url, gimy_impersonate)
                        except Exception as e:
                            last_gimy_error = e
                            continue
                        alternate_result = extend_gimy_stream_candidates(alternate_episode_url, alternate_text, url, gimy_impersonate)
                        if alternate_result["error"] is not None:
                            last_gimy_error = alternate_result["error"]
                        for candidate_url in alternate_result["candidates"]:
                            if candidate_url not in stream_candidates:
                                stream_candidates.append(candidate_url)
                        for candidate_url in alternate_result["direct_urls"]:
                            if candidate_url not in direct_fallback_candidates:
                                direct_fallback_candidates.append(candidate_url)
                        for candidate_url in alternate_result["external_urls"]:
                            if candidate_url not in external_source_urls:
                                external_source_urls.append(candidate_url)
                        if (
                            stream_candidates
                            or direct_fallback_candidates
                            or any(_looks_like_http_media_url(candidate) for candidate in external_source_urls)
                        ):
                            break
                if stream_candidates or direct_fallback_candidates or external_source_urls:
                    raw_direct_fallback_candidates = _dedupe_download_urls(direct_fallback_candidates)
                    raw_external_source_urls = _dedupe_download_urls(external_source_urls)
                    ordered_direct_candidates = _dedupe_download_urls(stream_candidates + direct_fallback_candidates)
                    if not ordered_direct_candidates and raw_direct_fallback_candidates:
                        ordered_direct_candidates = raw_direct_fallback_candidates
                    if not external_source_urls and raw_external_source_urls:
                        external_source_urls = raw_external_source_urls
                    preferred_media_urls = [candidate for candidate in external_source_urls if _looks_like_http_media_url(candidate)]
                    if preferred_media_urls:
                        media_url = preferred_media_urls[0]
                        direct_media_fallback_urls = [
                            candidate
                            for candidate in (preferred_media_urls[1:] + ordered_direct_candidates + deferred_episode_urls)
                            if candidate and candidate != media_url and candidate != url
                        ]
                        _set_task_aux_fields(task, _gimy_page_refresh_candidates=_filter_gimy_untried_page_candidates(task, deferred_episode_urls), _gimy_source_refresh_history=[])
                        _set_task_identity(
                            name=page_title,
                            source_site="gimy",
                            source_page=self._get_task_source_page(task, fallback_url=url) or url,
                            fallback_urls=direct_media_fallback_urls,
                        )
                        if is_mp3:
                            self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                            self._download_direct_media_audio_with_ffmpeg(
                                item_id,
                                media_url,
                                save_dir,
                                referer=url,
                                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                            )
                            return
                        media_ext = _infer_media_extension_from_url(media_url) or ".mp4"
                        name = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or page_title or "Video" or "").strip()
                        out_path, out_name = self._resolve_direct_media_output_path(media_url, save_dir, name, default_ext=media_ext)
                        _set_task_aux_fields(task, filename=out_path)
                        self._set_task_named_column_text(item_id, "name", out_name)
                        self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                        self._download_http_media(
                            item_id,
                            media_url,
                            out_path,
                            headers=self._gimy_direct_media_headers(url, f"{parsed_url.scheme}://{parsed_url.netloc}"),
                        )
                        return
                    supported_external_pages = [
                        candidate for candidate in external_source_urls
                        if any(
                            marker in urllib.parse.urlsplit(_normalize_download_url(candidate)).netloc.lower()
                            for marker in SUPPORTED_DOWNLOAD_PAGE_NETLOC_MARKERS
                        )
                    ]
                    if not ordered_direct_candidates and supported_external_pages:
                        external_url = supported_external_pages[0]
                        fallback_urls = [candidate for candidate in (supported_external_pages[1:] + deferred_episode_urls) if candidate and candidate != external_url]
                        _set_task_aux_fields(task, _gimy_page_refresh_candidates=_filter_gimy_untried_page_candidates(task, deferred_episode_urls), _gimy_source_refresh_history=[])
                        _set_task_identity(name=page_title, source_site="gimy", source_page=self._get_task_source_page(task, fallback_url=url) or url, fallback_urls=fallback_urls)
                        self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                        self._download_task_internal(external_url, item_id, save_dir, use_impersonate, is_mp3)
                        return
                if last_gimy_error:
                    raise last_gimy_error
                raise Exception("Gimy iframe stream URL missing")
            if not ordered_direct_candidates:
                if last_gimy_error:
                    raise last_gimy_error
                raise Exception("Gimy iframe stream URL missing")
            reachable = []
            unreachable = []
            for candidate in ordered_direct_candidates:
                try:
                    probe_resp = c_req.get(
                        candidate,
                        impersonate="chrome110",
                        timeout=15,
                        headers={
                            "Referer": url,
                            "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
                        },
                        allow_redirects=True,
                        verify=False,
                    )
                    status_code = int(getattr(probe_resp, "status_code", 0) or 0)
                    content_type = str(_response_header_value(getattr(probe_resp, "headers", {}) or {}, "Content-Type")).lower()
                    probe_text = str(getattr(probe_resp, "text", "") or "")[:2048]
                    if (
                        200 <= status_code < 400
                        and (
                            "#EXTM3U" in probe_text
                            or "mpegurl" in content_type
                            or "application/vnd.apple.mpegurl" in content_type
                        )
                    ):
                        reachable.append(candidate)
                    else:
                        unreachable.append(candidate)
                except Exception:
                    unreachable.append(candidate)
            reachable = sorted(_dedupe_download_urls(reachable), key=_gimy_stream_priority)
            unreachable = sorted(
                [candidate for candidate in _dedupe_download_urls(unreachable) if candidate not in reachable],
                key=_gimy_stream_priority,
            )
            ordered_candidates = reachable + unreachable
            stream_url = ordered_candidates[0]
            page_title = _clean_gimy_title(_extract_html_title(resp_text, short_name))
            deferred_fallback_urls = [
                candidate
                for candidate in (deferred_episode_urls + external_source_urls)
                if candidate and candidate not in ordered_candidates and candidate != url
            ]
            fallback_urls = (ordered_candidates[1:] if len(ordered_candidates) > 1 else []) + deferred_fallback_urls
            _set_task_aux_fields(
                task,
                _gimy_page_refresh_candidates=_filter_gimy_untried_page_candidates(task, deferred_fallback_urls),
            )
            _set_task_aux_fields(task, _gimy_source_refresh_history=[])
            _set_task_identity(name=page_title, source_site="gimy", source_page=url, fallback_urls=fallback_urls)
            self._set_task_status_mode_ui(item_id, t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中", self._ui_text("eta_found_stream", "已取得串流網址"))
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="gimy", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                stream_url,
                referer=url,
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                default_route=_gimy_manifest_default_route(stream_url),
            )
            return

        if "gimy" in parsed_url.netloc and ("/play/" in parsed_url.path or "/vodplay/" in parsed_url.path or "/video/" in parsed_url.path):
            self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在解析 Gimy 頁面...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            player_data = _safe_extract_player_js_object(resp.text, "player_data", "player_aaaa")
            direct_fallback_candidates = []
            external_source_urls = []
            if player_data:
                direct_url = _normalize_download_url(player_data.get("url"))
                if direct_url:
                    candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(direct_url)
                    if candidate_kind == "manifest" and normalized_candidate:
                        direct_fallback_candidates.append(normalized_candidate)
                    elif candidate_kind == "external" and normalized_candidate:
                        external_source_urls.append(normalized_candidate)
            candidates = _collect_player_m3u8_candidates(player_data, base_url=url) if player_data else []
            for candidate_url in _extract_m3u8_candidates_from_text(resp.text, base_url=url):
                if candidate_url not in candidates:
                    candidates.append(candidate_url)
            iframe_urls = _extract_gimy_inline_iframe_urls(resp.text, url)
            if player_data:
                for iframe_url in _build_gimy_iframe_urls(url, player_data):
                    if iframe_url not in iframe_urls:
                        iframe_urls.append(iframe_url)
            for iframe_url in iframe_urls:
                try:
                    iframe_resp = c_req.get(iframe_url, impersonate="chrome110", timeout=12, headers={"Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/"})
                    iframe_text = iframe_resp.text
                except Exception:
                    continue
                iframe_player_data = _safe_extract_player_js_object(iframe_text, "player_data", "player_aaaa", "player")
                if iframe_player_data:
                    iframe_direct_url = _normalize_download_url(iframe_player_data.get("url"))
                    if iframe_direct_url:
                        candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(iframe_direct_url)
                        if candidate_kind == "manifest" and normalized_candidate and normalized_candidate not in direct_fallback_candidates:
                            direct_fallback_candidates.append(normalized_candidate)
                        elif candidate_kind == "external" and normalized_candidate and normalized_candidate not in external_source_urls:
                            external_source_urls.append(normalized_candidate)
                    for candidate_url in _collect_player_m3u8_candidates(iframe_player_data, base_url=iframe_url):
                        if candidate_url not in candidates:
                            candidates.append(candidate_url)
                for candidate_url in _extract_m3u8_candidates_from_text(iframe_text, base_url=iframe_url):
                    if candidate_url not in candidates:
                        candidates.append(candidate_url)
                parse_source = urllib.parse.parse_qs(urllib.parse.urlsplit(iframe_url).query).get("url", [""])[0]
                if parse_source and "parse.php" in iframe_text:
                    parse_api = urllib.parse.urljoin(iframe_url, f"parse.php?url={urllib.parse.quote(parse_source, safe='')}")
                    try:
                        parse_resp = c_req.get(parse_api, impersonate="chrome110", timeout=12, headers={"Referer": iframe_url})
                        parse_data = None
                        for parsed_candidate in _extract_gimy_parse_candidates(parse_resp.text, base_url=parse_api):
                            candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(parsed_candidate)
                            if candidate_kind == "manifest" and normalized_candidate and normalized_candidate not in candidates:
                                candidates.append(normalized_candidate)
                            elif candidate_kind == "external" and normalized_candidate and normalized_candidate not in external_source_urls:
                                external_source_urls.append(normalized_candidate)
                    except Exception:
                        parse_data = None
                    if isinstance(parse_data, dict):
                        for key in ("url", "video", "playurl"):
                            parsed_candidate = _normalize_download_url(parse_data.get(key))
                            if not parsed_candidate:
                                continue
                            candidate_kind, normalized_candidate = _classify_gimy_stream_candidate(parsed_candidate)
                            if candidate_kind == "manifest" and normalized_candidate and normalized_candidate not in candidates:
                                candidates.append(normalized_candidate)
                            elif candidate_kind == "external" and normalized_candidate and normalized_candidate not in external_source_urls:
                                external_source_urls.append(normalized_candidate)
            candidates = _dedupe_download_urls(candidates)
            candidates, direct_fallback_candidates, external_source_urls = _filter_gimy_candidate_groups(
                task,
                candidates,
                direct_fallback_candidates,
                external_source_urls,
            )
            stream_url = candidates[0] if candidates else None
            if not stream_url:
                supported_external_pages = [
                    candidate for candidate in external_source_urls
                    if any(
                        marker in urllib.parse.urlsplit(_normalize_download_url(candidate)).netloc.lower()
                        for marker in SUPPORTED_DOWNLOAD_PAGE_NETLOC_MARKERS
                    )
                ]
                if supported_external_pages:
                    external_url = supported_external_pages[0]
                    fallback_urls = [candidate for candidate in supported_external_pages[1:] if candidate and candidate != external_url]
                    _set_task_aux_fields(task, _gimy_page_refresh_candidates=[], _gimy_source_refresh_history=[])
                    _set_task_identity(name=_clean_gimy_title(_extract_html_title(resp.text, short_name)), source_site="gimy", source_page=url, fallback_urls=fallback_urls)
                    self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                    self._download_task_internal(external_url, item_id, save_dir, use_impersonate, is_mp3)
                    return
                page_refresh_candidates = _task_gimy_page_refresh_candidates(task)
                fallback_episode_candidates = [
                    candidate
                    for candidate in _dedupe_download_urls(_task_field_value(task, "fallback_urls", []), primary_url=url)
                    if "/eps/" in urllib.parse.urlsplit(_normalize_download_url(candidate)).path.lower()
                ]
                next_episode_candidates = _filter_gimy_untried_page_candidates(
                    task,
                    list(page_refresh_candidates) + fallback_episode_candidates,
                )
                if next_episode_candidates:
                    refresh_url = next_episode_candidates[0]
                    refresh_history = _append_normalized_unique_candidates(
                        _task_gimy_refresh_history(task),
                        url,
                        refresh_url,
                    )
                    remaining_candidates = [candidate for candidate in next_episode_candidates[1:] if candidate != refresh_url]
                    _set_task_aux_fields(
                        task,
                        _gimy_refresh_history=refresh_history,
                        _gimy_page_refresh_candidates=remaining_candidates,
                        _gimy_source_refresh_history=[],
                        resolved_url="",
                        resolved_url_saved_at=0.0,
                    )
                    self._update_task_state_entry(task, resolved_url="", resolved_url_saved_at=0.0, page_refresh_candidates=remaining_candidates)
                    self._set_task_parse_ui(item_id, key="eta_site_gimy", fallback="正在重新取得 Gimy 串流...")
                    write_error_log(
                        "gimy play page retry",
                        Exception("retrying alternate gimy play page after stream URL missing"),
                        item_id=item_id,
                        page_url=url,
                        refresh_url=refresh_url,
                        remaining_candidates=len(remaining_candidates),
                    )
                    self._download_task_internal(refresh_url, item_id, save_dir, use_impersonate, is_mp3)
                    return
                raise Exception("Gimy stream URL missing")
            page_title = _clean_gimy_title(_extract_html_title(resp.text, short_name))
            fallback_urls = (candidates[1:] if len(candidates) > 1 else []) + [
                candidate for candidate in direct_fallback_candidates
                if candidate and candidate != stream_url and candidate not in candidates
            ] + [
                candidate for candidate in external_source_urls
                if candidate and candidate not in candidates
            ]
            _set_task_aux_fields(task, _gimy_page_refresh_candidates=[])
            _set_task_aux_fields(task, _gimy_source_refresh_history=[])
            _set_task_identity(name=page_title, source_site="gimy", source_page=url, fallback_urls=fallback_urls)
            self._set_task_status_mode_ui(item_id, t("status_downloading") if "status_downloading" in I18N_DICT.get(CURRENT_LANG, {}) else "下載中", self._ui_text("eta_found_stream", "已取得串流網址"))
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="gimy", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                stream_url,
                referer=f"{parsed_url.scheme}://{parsed_url.netloc}/",
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                default_route=_gimy_manifest_default_route(stream_url),
            )
            return

        if parsed_url.netloc and ("hanime1.me" in parsed_url.netloc or "hanimeone.me" in parsed_url.netloc) and "watch" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_site_hanime", fallback="正在解析 Hanime1 頁面...")
            c_req = get_curl_cffi_requests()
            hanime_site_root = f"{parsed_url.scheme or 'https'}://{parsed_url.netloc}"
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": hanime_site_root + "/"})
            source_match = re.search(r'<source\s+[^>]*src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE)
            if not source_match:
                source_match = re.search(r'(https?://[^"\'\s]+\.m3u8[^"\'\s]*)', resp.text)
            stream_url = _normalize_download_url(source_match.group(1)) if source_match else None
            if not stream_url:
                raise Exception("Hanime1 source URL missing")
            page_title = _extract_html_title(resp.text, short_name)
            _set_task_identity(name=page_title, source_site="hanime1", source_page=url, fallback_urls=[])
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="hanime1", fallback_urls=[])
            _download_manifest_with_site_strategy(
                stream_url,
                referer=hanime_site_root + "/",
                origin=hanime_site_root,
                default_route="ffmpeg",
            )
            return

        if "99itv.net" in parsed_url.netloc and "/vodplay/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback="正在解析 99iTV 頁面...")
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme or 'https'}://{parsed_url.netloc}"
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers={"Referer": site_root + "/"})
            player_data = _safe_extract_player_js_object(resp.text, "player_data", "player_aaaa", "player")
            stream_url = ""
            if player_data:
                stream_url = _decode_maccms_player_url(player_data.get("url"), player_data.get("encrypt"))
                if not stream_url:
                    candidates = _collect_player_m3u8_candidates(player_data, base_url=url)
                    stream_url = candidates[0] if candidates else ""
            if not stream_url:
                stream_candidates = _extract_m3u8_candidates_from_text(resp.text, base_url=url)
                stream_url = stream_candidates[0] if stream_candidates else ""
            if not stream_url:
                raise Exception("99iTV source URL missing")
            page_title = _clean_99itv_title(_extract_html_title(resp.text, short_name or "99iTV"))
            _set_task_identity(name=page_title, source_site="99itv", source_page=url, fallback_urls=[])
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="99itv", fallback_urls=[])
            _download_manifest_with_site_strategy(
                stream_url,
                referer=url,
                origin=site_root,
                default_route="ffmpeg",
            )
            return

        if ("turbovidhls.com" in parsed_url.netloc or "turboviplay.com" in parsed_url.netloc) and ("/t/" in parsed_url.path or "/embed/" in parsed_url.path):
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback="Parsing Avbebe playable iframe...")
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme or 'https'}://{parsed_url.netloc}"
            iframe_headers = _make_ytdlp_http_headers(referer=task.get("source_page") or "https://avbebe.com/", origin=site_root)
            resp = c_req.get(url, impersonate="chrome120", timeout=20, headers=iframe_headers)
            stream_candidates = _dedupe_download_urls(_extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mpd")))
            stream_url = stream_candidates[0] if stream_candidates else ""
            if not stream_url:
                write_error_log(
                    "avbebe playable iframe candidates missing",
                    Exception("Avbebe playable iframe did not expose a manifest URL"),
                    item_id=item_id,
                    url=url,
                    **_http_response_log_fields(resp),
                )
                raise Exception("Failed to extract Avbebe playable iframe stream URL")
            page_title = _clean_avbebe_title(_extract_html_title(resp.text, task.get("name") or short_name or "Avbebe"), task.get("name") or short_name or "Avbebe")
            fallback_urls = _dedupe_download_urls(stream_candidates[1:], primary_url=stream_url)
            _set_task_identity(name=page_title, source_site="avbebe", source_page=task.get("source_page") or url, fallback_urls=fallback_urls)
            self._log_m3u8_route_selected(task, item_id, stream_url, source_site="avbebe", fallback_urls=fallback_urls)
            _download_manifest_with_site_strategy(
                stream_url,
                referer=url,
                origin=site_root,
                default_route="ffmpeg",
                force_ffmpeg=True,
            )
            return

        if "avbebe.com" in parsed_url.netloc and "/archives/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback="正在解析 Avbebe...")
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme or 'https'}://{parsed_url.netloc}"
            page_headers = _make_ytdlp_http_headers(referer=site_root + "/", origin=site_root)
            resp = c_req.get(url, impersonate="chrome120", timeout=20, headers=page_headers)
            page_title = _clean_avbebe_title(_extract_html_title(resp.text, short_name or "Avbebe"), short_name or "Avbebe")
            media_candidates = _dedupe_download_urls(_extract_candidate_media_urls(resp.text, allowed_exts=(".mp4", ".m3u8", ".mpd")))
            candidate_referers = {_normalize_download_url(candidate): url for candidate in media_candidates}
            iframe_candidates = []
            for iframe_src in re.findall(r"<iframe[^>]+src=[\"']([^\"']+)[\"']", resp.text, re.IGNORECASE):
                iframe_url = _normalize_download_url(urllib.parse.urljoin(url, html.unescape(iframe_src).replace("\\/", "/")))
                if iframe_url and not any(ad_marker in iframe_url.lower() for ad_marker in ("adserver", "widgets", "juicyads")):
                    iframe_candidates.append(iframe_url)
            iframe_candidates = sorted(_dedupe_download_urls(iframe_candidates), key=_avbebe_iframe_priority)
            for iframe_url in iframe_candidates:
                try:
                    iframe_headers = _make_ytdlp_http_headers(referer=url, origin=site_root)
                    iframe_resp = c_req.get(iframe_url, impersonate="chrome120", timeout=20, headers=iframe_headers)
                    iframe_media_candidates = _extract_candidate_media_urls(
                        iframe_resp.text,
                        allowed_exts=(".mp4", ".m3u8", ".mpd"),
                    )
                    for candidate in iframe_media_candidates:
                        normalized_candidate = _normalize_download_url(candidate)
                        if not normalized_candidate:
                            continue
                        media_candidates.append(normalized_candidate)
                        candidate_referers[normalized_candidate] = iframe_url
                except Exception as exc:
                    write_error_log(
                        "avbebe iframe parser failed",
                        exc,
                        item_id=item_id,
                        url=url,
                        iframe_url=iframe_url,
                    )
            media_candidates = _dedupe_download_urls(media_candidates)
            stream_candidates = sorted(
                [candidate for candidate in media_candidates if _looks_like_manifest_url(candidate)],
                key=_avbebe_stream_priority,
            )
            direct_candidates = [candidate for candidate in media_candidates if _looks_like_http_media_url(candidate) and not _looks_like_manifest_url(candidate)]
            valid_stream_candidates = []
            for candidate in stream_candidates:
                candidate_referer = candidate_referers.get(_normalize_download_url(candidate), url) or url
                candidate_parts = urllib.parse.urlsplit(candidate_referer)
                candidate_origin = f"{candidate_parts.scheme}://{candidate_parts.netloc}" if candidate_parts.scheme and candidate_parts.netloc else site_root
                if _avbebe_manifest_looks_downloadable(candidate, referer=candidate_referer, origin=candidate_origin):
                    valid_stream_candidates.append(candidate)
                    continue
                write_error_log(
                    "avbebe rejected non-video stream candidate",
                    Exception("Avbebe stream candidate returned non-video segment content"),
                    item_id=item_id,
                    url=url,
                    candidate_url=candidate,
                    candidate_referer=candidate_referer,
                )
            stream_candidates = valid_stream_candidates
            stream_url = stream_candidates[0] if stream_candidates else ""
            if stream_url:
                fallback_urls = _dedupe_download_urls(stream_candidates[1:] + direct_candidates + iframe_candidates, primary_url=stream_url)
                stream_referer = candidate_referers.get(_normalize_download_url(stream_url), url) or url
                referer_parts = urllib.parse.urlsplit(stream_referer)
                stream_origin = f"{referer_parts.scheme}://{referer_parts.netloc}" if referer_parts.scheme and referer_parts.netloc else site_root
                _set_task_identity(name=page_title, source_site="avbebe", source_page=url, fallback_urls=fallback_urls)
                self._log_m3u8_route_selected(task, item_id, stream_url, source_site="avbebe", fallback_urls=fallback_urls)
                _download_manifest_with_site_strategy(
                    stream_url,
                    referer=stream_referer,
                    origin=stream_origin,
                    default_route="ffmpeg",
                    force_ffmpeg=True,
                )
                return
            if direct_candidates:
                direct_url = direct_candidates[0]
                fallback_urls = _dedupe_download_urls(direct_candidates[1:] + iframe_candidates, primary_url=direct_url)
                direct_referer = candidate_referers.get(_normalize_download_url(direct_url), url) or url
                direct_parts = urllib.parse.urlsplit(direct_referer)
                direct_origin = f"{direct_parts.scheme}://{direct_parts.netloc}" if direct_parts.scheme and direct_parts.netloc else site_root
                _set_task_identity(name=page_title, source_site="avbebe", source_page=url, fallback_urls=fallback_urls)
                self._download_routed_media_url(
                    task,
                    item_id,
                    direct_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="avbebe",
                    fallback_urls=fallback_urls,
                    referer=direct_referer,
                    origin=direct_origin,
                    headers=page_headers,
                )
                return
            if iframe_candidates:
                retryable_iframe_candidates = [candidate for candidate in iframe_candidates if _avbebe_can_retry_iframe_directly(candidate)]
                iframe_url = retryable_iframe_candidates[0] if retryable_iframe_candidates else ""
                if not iframe_url:
                    write_error_log(
                        "avbebe iframe candidates unsupported",
                        Exception("Avbebe iframe fallback only found unsupported host pages"),
                        item_id=item_id,
                        url=url,
                        iframe_candidates=iframe_candidates,
                    )
                    raise Exception("Failed to extract Avbebe stream URL")
                fallback_urls = _dedupe_download_urls(retryable_iframe_candidates[1:], primary_url=iframe_url)
                _set_task_identity(name=page_title, source_site="avbebe", source_page=url, fallback_urls=fallback_urls)
                self._set_task_parse_ui(item_id, message="Avbebe 直連不可用，改用網頁可播放分流...")
                self._download_task_internal(iframe_url, item_id, save_dir, use_impersonate=use_impersonate, is_mp3=is_mp3)
                return
            write_error_log(
                "avbebe parser candidates missing",
                Exception("Avbebe parser found no usable media candidates"),
                item_id=item_id,
                url=url,
                **_http_response_log_fields(resp),
                has_flowplayer="flowplayer" in (resp.text or "").lower(),
                has_data_item="data-item" in (resp.text or "").lower(),
                iframe_count=len(iframe_candidates),
            )
            raise Exception("Failed to extract Avbebe stream URL")

        if "missav" in parsed_url.netloc:
            self._set_task_parse_ui(item_id, key="eta_site_missav", fallback="正在解析 MissAV 頁面...")
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp, media_candidates, candidates, direct_media_candidates = _fetch_missav_media_candidates_with_retry(
                c_req,
                url,
                site_root,
                item_id=item_id,
            )
            if not candidates and not direct_media_candidates:
                write_error_log(
                    "missav parser candidates missing",
                    Exception("MissAV parser found no usable media candidates"),
                    item_id=item_id,
                    url=url,
                    **_http_response_log_fields(resp),
                    has_next_data="__NEXT_DATA__" in (resp.text or ""),
                    has_playlist_token="playlist" in (resp.text or "").lower(),
                    has_m3u8_token=".m3u8" in (resp.text or "").lower(),
                )
                raise Exception("Failed to extract MissAV stream URL")
            page_title = _extract_html_title(resp.text, short_name)
            if direct_media_candidates and not candidates:
                direct_media_url = direct_media_candidates[0]
                _set_task_identity(name=page_title, source_site="missav", source_page=url, fallback_urls=direct_media_candidates[1:])
                self._set_task_parse_ui(item_id, key="eta_direct_media", fallback=self._ui_text("eta_direct_media", "直接媒體下載"))
                self._download_direct_media(item_id, direct_media_url, save_dir, is_mp3=is_mp3, referer=url)
                return
            _set_task_identity(name=page_title, source_site="missav", source_page=url, fallback_urls=candidates[1:] or direct_media_candidates)
            self._log_m3u8_route_selected(task, item_id, candidates[0], source_site="missav")
            _download_manifest_with_site_strategy(
                candidates[0],
                referer=url,
                origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
            )
            return

        if "ppp.porn" in parsed_url.netloc and "/v/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = re.sub(r"\s+", " ", str(_extract_html_title(resp.text, short_name or "PPP.Porn") or "")).strip()
            if page_title:
                page_title = re.sub(r"\s*-\s*PPP\.Porn\s*\|.*$", "", page_title, flags=re.IGNORECASE)
                page_title = page_title.strip(" -|/") or str(_extract_html_title(resp.text, short_name or "PPP.Porn") or "").strip()
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            _dispatch_extracted_media_candidates(
                candidate_urls,
                page_title,
                "ppp.porn",
                source_page=url,
                referer=url,
                origin=site_root,
                manifest_default_route="ffmpeg",
                missing_message="PPP.Porn media URL missing",
            )
            return

        if "hohoj.tv" in parsed_url.netloc and parsed_url.path.startswith("/video"):
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = _clean_hohoj_title(_extract_html_title(resp.text, short_name or "HoHoJ"))
            embed_url = urllib.parse.urljoin(url, f"/embed?{parsed_url.query}") if parsed_url.query else ""
            embed_match = re.search(r'<iframe[^>]+class=["\']player["\'][^>]+src=["\']([^"\']+)', resp.text, re.IGNORECASE)
            if embed_match:
                embed_url = urllib.parse.urljoin(url, embed_match.group(1))
            if not embed_url:
                raise Exception("HoHoJ embed URL missing")
            embed_resp = c_req.get(
                embed_url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root, referer=url),
            )
            candidate_urls = _extract_candidate_media_urls(embed_resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            _dispatch_extracted_media_candidates(
                candidate_urls,
                page_title,
                "hohoj",
                source_page=url,
                referer=embed_url,
                origin=site_root,
                manifest_default_route="generic",
                missing_message="HoHoJ media URL missing",
            )
            return

        if "goodav17.com" in parsed_url.netloc and ("/vr_html/" in parsed_url.path or "/html/" in parsed_url.path):
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = _goodav_title_for_display(_extract_html_title(resp.text, short_name or "GoodAV"))
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            for iframe_src in re.findall(r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text, re.IGNORECASE):
                iframe_url = urllib.parse.urljoin(url, iframe_src)
                decoded_embed_media = _decode_goodav_embed_media_url(iframe_url)
                if decoded_embed_media:
                    candidate_urls.append(decoded_embed_media)
                if "ggjav.com/main/embed" in iframe_url.lower():
                    try:
                        embed_resp = c_req.get(
                            iframe_url,
                            impersonate="chrome120",
                            timeout=20,
                            headers=_make_site_root_headers(site_root, referer=url),
                        )
                        candidate_urls.extend(
                            _extract_candidate_media_urls(embed_resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
                        )
                    except Exception:
                        pass
            _dispatch_extracted_media_candidates(
                candidate_urls,
                page_title,
                "goodav17",
                source_page=url,
                referer=url,
                origin=site_root,
                direct_filter=lambda candidate: (
                    "vr.goodav17.com/media/" in candidate.lower()
                    or ("ggjav.com" in urllib.parse.urlparse(candidate).netloc.lower() and candidate.lower().endswith(".mp4"))
                ),
                manifest_default_route="generic",
                missing_message="GoodAV media URL missing",
            )
            return

        if "javfilms.com" in parsed_url.netloc and "/video/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = _clean_javfilms_title(_extract_html_title(resp.text, short_name or "JAV Films"))
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            manifest_candidates, direct_media_candidates = _split_stream_and_direct_candidates(
                candidate_urls,
                direct_filter=lambda candidate: "cc3001.dmm.co.jp" in candidate.lower() and candidate.lower().endswith(".mp4"),
            )
            dmm_video_id_match = re.search(r"/get/video/([A-Za-z0-9_-]+)", str(resp.text or ""), flags=re.IGNORECASE)
            if dmm_video_id_match:
                dmm_video_id = str(dmm_video_id_match.group(1) or "").strip()
            else:
                parsed_javfilms_url = urllib.parse.urlparse(str(url or ""))
                dmm_video_segments = [segment for segment in parsed_javfilms_url.path.split("/") if segment]
                dmm_video_id = str(dmm_video_segments[-1] or "").strip() if dmm_video_segments else ""
            stream_url, stream_fallback_urls = _pick_primary_with_fallbacks(manifest_candidates)
            media_url, media_fallback_urls = _pick_primary_with_fallbacks(direct_media_candidates)
            if stream_url:
                _set_task_identity(name=page_title, source_site="javfilms", source_page=url, fallback_urls=stream_fallback_urls)
                self._log_m3u8_route_selected(task, item_id, stream_url, source_site="javfilms", fallback_urls=stream_fallback_urls)
                _download_manifest_with_site_strategy(
                    stream_url,
                    referer=url,
                    origin=site_root,
                    default_route="generic",
                )
                return
            if media_url:
                _set_task_identity(name=page_title, source_site="javfilms", source_page=url, fallback_urls=media_fallback_urls)
                direct_headers = _make_site_root_headers(site_root, referer=url)
                direct_session = None
                parsed_media_url = urllib.parse.urlparse(str(media_url or ""))
                if (
                    "cc3001.dmm.co.jp" in parsed_media_url.netloc.lower()
                    and "/litevideo/freepv/" in parsed_media_url.path.lower()
                    and parsed_media_url.path.lower().endswith(".mp4")
                ):
                    if not dmm_video_id:
                        raise Exception("JAV Films protected DMM preview unavailable")
                    dmm_content_url = (
                        f"https://video.dmm.co.jp/av/content/?id={str(dmm_video_id or '').strip()}"
                        if str(dmm_video_id or "").strip()
                        else ""
                    )
                    declared_url = (
                        "https://www.dmm.co.jp/age_check/=/declared=yes/?rurl="
                        + urllib.parse.quote(dmm_content_url, safe="")
                    ) if dmm_content_url else ""
                    if not dmm_content_url or not declared_url:
                        raise Exception("JAV Films protected DMM preview unavailable")
                    direct_session = c_req.Session(impersonate="chrome120")
                    try:
                        direct_session.get(
                            declared_url,
                            headers=_make_ytdlp_http_headers(referer="https://www.dmm.co.jp/"),
                            allow_redirects=True,
                            timeout=30,
                        )
                        probe_resp = direct_session.get(
                            media_url,
                            headers=_make_ytdlp_http_headers(referer=dmm_content_url, origin="https://video.dmm.co.jp"),
                            allow_redirects=True,
                            timeout=20,
                            stream=True,
                        )
                        probe_status = getattr(probe_resp, "status_code", 0)
                        probe_content_type = _response_header_value(getattr(probe_resp, "headers", {}) or {}, "Content-Type")
                        try:
                            probe_resp.close()
                        except Exception:
                            pass
                        if probe_status >= 400 or "text/html" in probe_content_type.lower():
                            raise Exception("JAV Films protected DMM preview unavailable")
                    except Exception:
                        try:
                            direct_session.close()
                        except Exception:
                            pass
                        direct_session = None
                        raise
                    direct_headers = _make_ytdlp_http_headers(referer=dmm_content_url, origin="https://video.dmm.co.jp")
                try:
                    self._download_direct_or_audio_media(
                        item_id,
                        media_url,
                        save_dir,
                        page_title,
                        is_mp3=is_mp3,
                        referer=direct_headers.get("Referer", url),
                        origin=direct_headers.get("Origin", site_root),
                        headers=direct_headers,
                        session=direct_session if not is_mp3 else None,
                    )
                finally:
                    if direct_session is not None:
                        try:
                            direct_session.close()
                        except Exception:
                            pass
                return
            raise Exception("JAV Films media URL missing")

        if "18jav.tv" in parsed_url.netloc and "/videos/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = re.sub(r"\s+", " ", str(_extract_html_title(resp.text, short_name or "18JAV") or "")).strip()
            if page_title:
                page_title = re.sub(r"\s*[-|]\s*18JAV.*$", "", page_title, flags=re.IGNORECASE)
                page_title = page_title.strip(" -|/") or str(_extract_html_title(resp.text, short_name or "18JAV") or "").strip()
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            _dispatch_extracted_media_candidates(
                candidate_urls,
                page_title,
                "18jav",
                source_page=url,
                referer=url,
                origin=site_root,
                direct_filter=lambda candidate: not _is_18av_preview_media_url(candidate),
                missing_message="18JAV media URL missing",
            )
            return

        if "18av.mm-cg.com" in parsed_url.netloc and (
            "/animation_content/" in parsed_url.path
            or "/CensoredAnimation_content/" in parsed_url.path
            or "/UncensoredAnimation_content/" in parsed_url.path
            or "/amateurjav_content/" in parsed_url.path
            or "/chinese_content/" in parsed_url.path
            or "/reducing-mosaic_content/" in parsed_url.path
            or "/censored_content/" in parsed_url.path
            or "/uncensored_content/" in parsed_url.path
        ):
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = _clean_18av_title(_extract_html_title(resp.text, short_name or "18AV"))
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            manifest_candidates, direct_media_candidates = _split_stream_and_direct_candidates(
                candidate_urls,
                direct_filter=lambda candidate: not _is_18av_preview_media_url(candidate),
            )
            stream_url, stream_fallback_urls = _pick_primary_with_fallbacks(manifest_candidates)
            page_media_url, page_media_fallback_urls = _pick_primary_with_fallbacks(direct_media_candidates)
            if stream_url:
                _set_task_identity(name=page_title, source_site="18av", source_page=url, fallback_urls=stream_fallback_urls)
                self._log_m3u8_route_selected(task, item_id, stream_url, source_site="18av", fallback_urls=stream_fallback_urls)
                _download_manifest_with_site_strategy(
                    stream_url,
                    referer=url,
                    origin=site_root,
                )
                return
            stream_url, media_url, player_probe_url, protected_player = _resolve_18av_protected_player_media(url, resp.text)
            if stream_url:
                _set_task_identity(name=page_title, source_site="18av", source_page=url, fallback_urls=[])
                self._log_m3u8_route_selected(task, item_id, stream_url, source_site="18av", fallback_urls=[])
                _download_manifest_with_site_strategy(
                    stream_url,
                    referer=url,
                    origin=site_root,
                )
                return
            if media_url:
                _set_task_identity(name=page_title, source_site="18av", source_page=url, fallback_urls=[])
                self._download_direct_or_audio_media(
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    referer=url,
                    origin=site_root,
                )
                return
            if page_media_url:
                _set_task_identity(name=page_title, source_site="18av", source_page=url, fallback_urls=page_media_fallback_urls)
                self._download_direct_or_audio_media(
                    item_id,
                    page_media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    referer=url,
                    origin=site_root,
                )
                return
            _set_task_identity(name=page_title, source_site="18av", source_page=url, fallback_urls=[])
            write_error_log(
                "18av protected player unavailable",
                Exception("18AV protected player endpoint returned empty shell"),
                item_id=item_id,
                url=url,
                source_site="18av",
                player_probe_url=player_probe_url or None,
                iframe_prefix=protected_player.get("iframe_prefix") or None,
                encoded_player_id=protected_player.get("encoded_id") or None,
                decoded_payload=protected_player.get("decoded_payload") or None,
                payload_base=protected_player.get("base_value") or None,
                payload_xor=protected_player.get("xor_value") or None,
                aes_key=protected_player.get("aes_key") or None,
                aes_iv=protected_player.get("aes_iv") or None,
            )
            raise Exception("18AV protected player unavailable")

        if "mypikpak.com" in parsed_url.netloc and parsed_url.path.startswith("/s/"):
            self._set_task_parse_ui(item_id, key="eta_found_stream", fallback="正在解析 PikPak 分享頁...")
            c_req = get_curl_cffi_requests()
            site_root = f"{parsed_url.scheme}://{parsed_url.netloc}"
            resp = c_req.get(
                url,
                impersonate="chrome120",
                timeout=20,
                headers=_make_site_root_headers(site_root),
            )
            page_title = re.sub(r"\s+", " ", str(_extract_html_title(resp.text, short_name or "PikPak") or "")).strip()
            if page_title:
                page_title = re.sub(r"\s+Shared by\s+.*?\|\s*PikPak.*$", "", page_title, flags=re.IGNORECASE)
                page_title = re.sub(r"\s*\|\s*PikPak.*$", "", page_title, flags=re.IGNORECASE)
                page_title = page_title.strip(" -|/") or str(_extract_html_title(resp.text, short_name or "PikPak") or "").strip()
            share_entries = _extract_pikpak_share_entries(resp.text)
            primary_entry = _pick_pikpak_primary_video_entry(share_entries)
            if primary_entry:
                raw_name = str((primary_entry or {}).get("name") or "").strip()
                if raw_name:
                    stem = os.path.splitext(raw_name)[0].strip()
                    display_name = stem or raw_name
                else:
                    display_name = str(page_title or "").strip()
            else:
                display_name = page_title
            candidate_urls = _extract_candidate_media_urls(resp.text, allowed_exts=(".m3u8", ".mp4", ".mpd"))
            candidate_urls.extend(_extract_pikpak_media_candidates(share_entries))
            if candidate_urls:
                _dispatch_extracted_media_candidates(
                    candidate_urls,
                    display_name,
                    "pikpak",
                    source_page=url,
                    referer=url,
                    origin=site_root,
                    manifest_default_route="ffmpeg",
                    missing_message="PikPak media URL missing",
                )
                return
            _set_task_identity(name=display_name, source_site="pikpak", source_page=url, fallback_urls=[])
            write_error_log(
                "pikpak protected share unavailable",
                Exception("PikPak protected share API requires device_id and captcha_token"),
                item_id=item_id,
                url=url,
                source_site="pikpak",
                share_entry_count=len(share_entries),
                video_name=(primary_entry or {}).get("name") if isinstance(primary_entry, dict) else None,
                has_nuxt_data="__NUXT_DATA__" in (resp.text or ""),
            )
            raise Exception("PikPak protected share requires browser verification")

        if "avjoy.me" in parsed_url.netloc and "/video/" in parsed_url.path:
            self._set_task_parse_ui(item_id, key="eta_direct_media", fallback=self._ui_text("eta_direct_media", "直接媒體下載"))
            safe_referer = f"{parsed_url.scheme}://{parsed_url.netloc}/"
            page_title, candidates = self._fetch_avjoy_media_candidates(url, fallback_name=short_name)
            media_url, fallback_urls = self._select_avjoy_media_candidate(candidates)
            if not media_url:
                raise Exception("AVJOY media URL missing")
            _set_task_identity(name=page_title, source_site="avjoy", source_page=url, fallback_urls=fallback_urls)
            try:
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="avjoy",
                    fallback_urls=fallback_urls,
                    referer=safe_referer,
                    origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    manifest_downloader=_download_manifest_with_site_strategy,
                )
            except (StopDownloadException, KeyboardInterrupt, ResumeLowSpeedReanalysisException):
                raise
            except Exception as exc:
                if not self._is_retryable_media_download_error(exc):
                    raise
                failed_urls = _dedupe_download_urls([media_url] + fallback_urls)
                fresh_title, fresh_candidates = self._fetch_avjoy_media_candidates(url, fallback_name=page_title or short_name)
                fresh_media_url, fresh_fallback_urls = self._select_avjoy_media_candidate(fresh_candidates, failed_urls=failed_urls)
                if not fresh_media_url:
                    raise
                page_title = fresh_title or page_title
                _set_task_identity(name=page_title, source_site="avjoy", source_page=url, fallback_urls=fresh_fallback_urls)
                write_error_log(
                    "avjoy media refresh retry",
                    Exception("avjoy media refresh retry"),
                    item_id=item_id,
                    url=url,
                    failed_url=media_url,
                    refreshed_url=fresh_media_url,
                    source_site="avjoy",
                    reason=str(exc)[:240],
                    refreshed_count=len(fresh_candidates),
                )
                self._download_routed_media_url(
                    task,
                    item_id,
                    fresh_media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="avjoy",
                    fallback_urls=fresh_fallback_urls,
                    referer=safe_referer,
                    origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    manifest_downloader=_download_manifest_with_site_strategy,
                )
            return

        if "evoload.io" in parsed_url.netloc:
            self._set_task_parse_ui(item_id, key="eta_site_movieffm", fallback="正在解析 MovieFFM 外部來源...")
            c_req = get_curl_cffi_requests()
            source_page_referer = self._get_task_source_page(task) or "https://www.movieffm.net/"
            evoload_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
            evoload_session = self._track_network_session(c_req.Session(impersonate="chrome120"))
            page_headers = _make_browser_page_headers(referer=source_page_referer, origin="https://www.movieffm.net")
            resp = evoload_session.get(url, timeout=20, headers=page_headers)
            page_variants = [resp.text]
            redirect_match = re.search(r"redirect_link\s*=\s*'([^']+)'", str(resp.text or ""), re.IGNORECASE)
            if redirect_match:
                redirect_base = html.unescape(str(redirect_match.group(1) or "").strip())
                redirect_url = _normalize_download_url(redirect_base + "fp=-5")
                if redirect_url and redirect_url != url:
                    try:
                        redirect_resp = evoload_session.get(
                            redirect_url,
                            timeout=20,
                            headers=_make_browser_page_headers(referer=url, origin=evoload_origin),
                        )
                        page_variants.append(redirect_resp.text)
                    except Exception:
                        pass
            media_candidates = []
            for page_text in page_variants:
                media_candidates.extend(_extract_candidate_media_urls(page_text, allowed_exts=(".mp4", ".m3u8", ".mpd")))
                unpacked = unpack_packed_javascript(page_text)
                if unpacked:
                    media_candidates.extend(_extract_candidate_media_urls(unpacked, allowed_exts=(".mp4", ".m3u8", ".mpd")))
            media_candidates = _dedupe_download_urls(media_candidates)
            media_url = next((candidate for candidate in media_candidates if _looks_like_manifest_url(candidate)), None)
            if not media_url:
                media_url = next((candidate for candidate in media_candidates if _looks_like_http_media_url(candidate)), None)
            parked_domain_markers = (
                "assets.abovedomains.com",
                "forsale.min.js",
                "domain may be for sale",
                "this domain is for sale",
                "buy this domain",
            )
            if not media_url and any(
                any(marker in str(page_text or "").lower() for marker in parked_domain_markers)
                for page_text in page_variants
            ):
                raise Exception("EvoLoad source unavailable")
            page_title = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or short_name or "").strip() or short_name or "MovieFFM"
            _dispatch_extracted_media_candidates(
                media_candidates,
                page_title,
                _task_source_site_name(task) or "movieffm",
                source_page=url,
                referer=url,
                origin=evoload_origin,
                manifest_default_route="ffmpeg",
                session=evoload_session,
                missing_message="EvoLoad media URL missing",
            )
            return

        if any(host in parsed_url.netloc for host in ("mixdrop.ag", "m1xdrop.click")):
            self._set_task_parse_ui(item_id, key="eta_site_movieffm", fallback="正在解析外部播放來源...")
            c_req = get_curl_cffi_requests()
            source_page_referer = self._get_task_source_page(task) or "https://www.movieffm.net/"
            watch_url = _normalize_mixdrop_watch_url(url) or url
            mixdrop_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
            mixdrop_session = self._track_network_session(c_req.Session(impersonate="chrome120"))
            resp = mixdrop_session.get(watch_url, timeout=20, headers=_make_ytdlp_http_headers(referer=source_page_referer))
            candidates = _extract_mixdrop_media_candidates(resp.text)
            page_title = str(_task_field_value(task, "short_name") or _task_field_value(task, "name") or short_name or "").strip() or short_name or _extract_html_title(resp.text, short_name)
            _dispatch_extracted_media_candidates(
                candidates,
                page_title,
                _task_source_site_name(task) or "movieffm",
                source_page=watch_url,
                referer=watch_url,
                origin=mixdrop_origin,
                manifest_default_route="ffmpeg",
                session=mixdrop_session,
                prefer_direct=True,
                missing_message="MixDrop media URL missing",
            )
            return

        if "threads.net" in parsed_url.netloc or parsed_url.netloc.startswith("www.threads."):
            self._set_task_parse_ui(item_id, key="eta_site_threads", fallback="正在解析 Threads 頁面...")
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
            self._download_routed_media_url(
                task,
                item_id,
                media_url,
                save_dir,
                page_title,
                source_site="threads",
                referer=url,
                headers=_make_ytdlp_http_headers(referer=url, user_agent=ydl_opts["http_headers"]["User-Agent"]),
            )
            return

        if "instagram.com" in parsed_url.netloc and any(part in parsed_url.path for part in ("/reel/", "/p/")):
            self._set_task_parse_ui(item_id, key="eta_site_instagram", fallback="正在解析 Instagram 頁面...")
            shortcode_m = re.search(r"/(?:reel|p)/([^/?#]+)", url)
            if not shortcode_m:
                raise Exception("Instagram shortcode missing")
            shortcode = shortcode_m.group(1)
            c_req = get_curl_cffi_requests()
            session = self._track_network_session(c_req.Session(impersonate="chrome110"))
            base_headers = {
                "User-Agent": ydl_opts["http_headers"]["User-Agent"],
                "Accept": "*/*",
                "X-IG-App-ID": "936619743392459",
                "X-ASBD-ID": "198387",
                "X-IG-WWW-Claim": "0",
                "Origin": "https://www.instagram.com",
                "Referer": "https://www.instagram.com/",
            }
            page_resp = session.get(url, timeout=20, headers=_make_ytdlp_http_headers(referer=url, user_agent=ydl_opts["http_headers"]["User-Agent"]))
            candidates = _extract_instagram_media_candidates(page_resp.text)

            if not candidates:
                try:
                    embed_resp = session.get(f"https://www.instagram.com/reel/{shortcode}/embed/captioned/", timeout=20, headers=_make_ytdlp_http_headers(referer=url, user_agent=ydl_opts["http_headers"]["User-Agent"]))
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
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="instagram",
                    fallback_urls=[],
                    referer="https://www.instagram.com/",
                    origin="https://www.instagram.com",
                    manifest_downloader=_download_manifest_with_site_strategy,
                )
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
                    write_error_log("instagram savereels fallback", Exception("Instagram SaveReels fallback succeeded"), url=url, item_id=item_id)
                    self._download_routed_media_url(
                        task,
                        item_id,
                        media_url,
                        save_dir,
                        page_title,
                        is_mp3=is_mp3,
                        source_site="instagram",
                        fallback_urls=fallback_urls,
                        referer="https://savereels.app/",
                        origin="https://savereels.app",
                        manifest_downloader=_download_manifest_with_site_strategy,
                    )
                    return
            except Exception as savereels_exc:
                write_error_log("instagram savereels fallback failed", savereels_exc, url=url, item_id=item_id)
            write_error_log("instagram extractor fallback", Exception("Instagram video URL missing; falling back to yt-dlp"), url=url, item_id=item_id)
            self._set_task_parse_ui(item_id, message="Instagram 直連解析失敗，改用 yt-dlp...")

        if "facebook.com" in parsed_url.netloc and any(part in parsed_url.path for part in ("/reel/", "/watch/", "/videos/")):
            self._set_task_parse_ui(item_id, key="eta_site_facebook", fallback="正在解析 Facebook 頁面...")
            c_req = get_curl_cffi_requests()
            resp = c_req.get(url, impersonate="chrome110", timeout=20, headers=_make_ytdlp_http_headers(referer=url, user_agent=ydl_opts["http_headers"]["User-Agent"]))
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
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="facebook",
                    fallback_urls=[],
                    referer=url,
                    origin="https://www.facebook.com",
                    manifest_downloader=_download_manifest_with_site_strategy,
                )
                return
            write_error_log("facebook extractor fallback", Exception("Facebook media URL missing; falling back to yt-dlp"), url=url, item_id=item_id)
            self._set_task_parse_ui(item_id, message="Facebook 直連解析失敗，改用 yt-dlp...")

        if "/status/" in parsed_url.path and any(host in parsed_url.netloc for host in ("twitter.com", "x.com", "fxtwitter.com", "vxtwitter.com")):
            self._set_task_parse_ui(item_id, key="eta_site_twitter", fallback="正在解析 Twitter/X 頁面...")
            status_id_m = re.search(r"/status/(\d+)", url)
            if not status_id_m:
                raise Exception("Twitter status id missing")
            status_id = status_id_m.group(1)
            screen_name = parsed_url.path.strip("/").split("/", 1)[0]
            api_url = f"https://api.vxtwitter.com/{screen_name}/status/{status_id}"
            c_req = get_curl_cffi_requests()
            resp = c_req.get(api_url, impersonate="chrome110", timeout=20, headers={"Referer": url})
            data = resp.json()
            media_candidates = _extract_twitter_media_candidates(data)
            media_url = next((candidate for candidate in media_candidates if _looks_like_manifest_url(candidate)), None)
            if not media_url:
                media_url = next((candidate for candidate in media_candidates if _infer_media_extension_from_url(candidate) in (".mp4", ".mkv", ".webm", ".m4a", ".mp3")), None)
            if not media_url:
                media_url = next((candidate for candidate in media_candidates if _infer_media_extension_from_url(candidate) in (".jpg", ".jpeg", ".png", ".gif", ".webp")), None)
            if not media_url and media_candidates:
                media_url = media_candidates[0]
            if not media_url:
                raise Exception("Twitter/X media URL missing")
            tweet = (data or {}).get("tweet") if isinstance((data or {}).get("tweet"), dict) else (data or {})
            page_title = (tweet or {}).get("text") or short_name or f"X_{status_id}"
            page_title = re.sub(r"\s+", " ", page_title).strip()[:120]
            fallback_urls = [candidate for candidate in media_candidates if candidate != media_url]
            _set_task_identity(name=page_title, source_site="twitter", source_page=url, fallback_urls=fallback_urls)
            media_ext = _infer_media_extension_from_url(media_url)
            if not media_ext:
                media_ext = ".jpg" if "pbs.twimg.com" in media_url.lower() else ".mp4"
            if _looks_like_manifest_url(media_url):
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="twitter",
                    fallback_urls=fallback_urls,
                    referer=url,
                    origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    manifest_downloader=_download_manifest_with_site_strategy,
                )
            else:
                self._download_routed_media_url(
                    task,
                    item_id,
                    media_url,
                    save_dir,
                    page_title,
                    is_mp3=is_mp3,
                    source_site="twitter",
                    fallback_urls=fallback_urls,
                    referer=url,
                    origin=f"{parsed_url.scheme}://{parsed_url.netloc}",
                    default_ext=media_ext,
                    allow_audio_extract=media_ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"),
                )
            return

        page_url = url
        if "anime1" in parsed_url.netloc and use_impersonate:
            try:
                if not page_url or not re.match(r"^https://anime1\.(?:me|pw)/\d+/?$", _normalize_download_url(page_url) or ""):
                    raise Exception(f"Anime1 task URL is not a valid episode page: {page_url}")
                c_req = get_curl_cffi_requests()
                anime1_session = self._track_network_session(c_req.Session(impersonate="chrome110"))
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
                    self._set_task_named_column_text(item_id, "name", out_name)
                    self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
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
                    self._set_task_named_column_text(item_id, "name", out_name)
                    self._set_task_parse_ui(item_id, key="eta_found_media", fallback=self._ui_text("eta_found_media", "已取得媒體網址"))
                    self._download_http_media(item_id, url, out_path, headers=page_headers, session=anime1_session)
                    return
                direct_m3u8 = re.search(r'(https?://[^\s"\'\\]+(?:surrit\.com|[^"\']+)\.m3u8[^\s"\']*)', resp.text)
                if direct_m3u8:
                    url = html.unescape(direct_m3u8.group(1))
                    _set_task_identity(name=clean_title)
                    self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
                    _download_manifest_with_site_strategy(
                        url,
                        referer=page_url,
                        origin=page_origin,
                        default_route="ffmpeg",
                    )
                    return
                iframe_m = re.search(r'<iframe[^>]+src=["\']([^"\']+)["\']', resp.text)
                if not iframe_m:
                    raise Exception("Anime1 page did not expose a direct stream or iframe URL")
                iframe_url = html.unescape(iframe_m.group(1))
                if iframe_url.startswith("//"):
                    iframe_url = "https:" + iframe_url
                self._set_task_status_mode_ui(
                    item_id,
                    t("status_processing") if "status_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                    t("eta_processing") if "eta_processing" in I18N_DICT.get(CURRENT_LANG, {}) else "整理中",
                )
                iframe_resp = c_req.get(iframe_url, headers=page_headers, timeout=15, impersonate="chrome110")
                m3u8_m = re.search(r'(https?://[^\s"\']+\.m3u8[^\s"\']*)', iframe_resp.text)
                if not m3u8_m:
                    raise Exception("Anime1 iframe did not contain an m3u8 URL")
                url = m3u8_m.group(1)
                _set_task_identity(name=clean_title)
                self._set_task_parse_ui(item_id, key="eta_found_stream", fallback=self._ui_text("eta_found_stream", "已取得串流網址"))
                _download_manifest_with_site_strategy(
                    url,
                    referer=page_url,
                    origin=page_origin,
                    default_route="ffmpeg",
                )
                return
            except (StopDownloadException, KeyboardInterrupt):
                raise
            except Exception as e:
                write_error_log("anime1 custom parser fallback", e, page_url=page_url, item_id=item_id, use_impersonate=use_impersonate)
                if not page_url or not re.match(r"^https://anime1\.(?:me|pw)/\d+/?$", _normalize_download_url(page_url) or ""):
                    raise
                url = page_url
                self._set_task_parse_ui(item_id, error=e)

        try:
            _run_yt_dlp(url)
            if str(_task_field_value(self.tasks.get(item_id, {}), "state", "") or "") == "DELETED":
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
                state=str(_task_field_value(task, "state", "") or ""),
                use_impersonate=use_impersonate,
                is_mp3=is_mp3,
            )
            if str(_task_field_value(task, "state", "") or "") in ("DELETED", "DELETE_REQUESTED"):
                self._discard_task(item_id)
                return
            if str(_task_field_value(task, "state", "") or "") in PAUSED_TASK_STATES:
                return
            self._set_task_status_mode_ui(item_id, t("status_error") if "status_error" in I18N_DICT.get(CURRENT_LANG, {}) else "錯誤", summarize_error_message(e, "err_net", 120))
            _set_task_aux_fields(task, state="ERROR")

    def on_closing(self):
        if self._shutdown_started:
            return
        current_downloading = self._count_tasks_in_states("DOWNLOADING")
        if current_downloading > 0:
            if self._ask_warning_yesno(t("msg_close_warn", count=current_downloading)):
                try:
                    self._prepare_shutdown_resume_state()
                    self._wait_for_shutdown_downloads()
                    self._wait_for_shutdown_resume_artifacts()
                    self._flush_live_resume_state()
                    self.persist_unfinished_state(force=True)
                except Exception:
                    pass
                self._finalize_process_shutdown()
            return
        try:
            self._wait_for_shutdown_resume_artifacts(timeout_seconds=0.5, stable_polls=2)
            self._flush_live_resume_state()
            self.persist_unfinished_state(force=True)
        except Exception:
            pass
        self._finalize_process_shutdown()

    def _finalize_process_shutdown(self):
        if self._shutdown_started:
            return
        try:
            exit_timer = threading.Timer(FORCED_SHUTDOWN_EXIT_DELAY_SECONDS, os._exit, args=(0,))
            exit_timer.daemon = True
            exit_timer.start()
            self._forced_exit_timer = exit_timer
        except Exception:
            self._forced_exit_timer = None
        try:
            self._prepare_ui_for_shutdown()
        except Exception:
            pass
        try:
            self._close_active_network_sessions()
        except Exception:
            pass
        try:
            self.root.withdraw()
        except Exception:
            pass
        try:
            self._force_kill_child_processes()
        except Exception:
            pass
        try:
            with self._resume_progress_lock:
                self._resume_progress_cache.clear()
        except Exception:
            pass
        try:
            release_single_instance_lock()
        except Exception:
            pass
        try:
            self.root.quit()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass


def main():
    try:
        lock_acquired, recovered_after_retry = wait_for_single_instance_lock()
        if not lock_acquired:
            try:
                write_error_log(
                    "single instance lock denied",
                    Exception("single instance lock denied"),
                    app_dir=_APP_DIR,
                )
            except Exception:
                pass
            warning_root = tk.Tk()
            warning_root.withdraw()
            warning_root.attributes("-topmost", True)
            try:
                messagebox.showwarning(t("msg_warning") if t("msg_warning") != "msg_warning" else "警告", t("msg_already_running"), parent=warning_root)
            finally:
                warning_root.destroy()
            return
        if recovered_after_retry:
            try:
                write_error_log(
                    "single instance lock recovered after retry",
                    Exception("single instance lock recovered after retry"),
                    app_dir=_APP_DIR,
                )
            except Exception:
                pass

        try:
            write_error_log(
                "app start",
                Exception("app start"),
                frozen=getattr(sys, "frozen", False),
                app_dir=_APP_DIR,
            )
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
        try:
            root.mainloop()
        finally:
            release_single_instance_lock()
    except Exception as exc:
        write_error_log(
            "startup failure",
            exc,
            frozen=getattr(sys, "frozen", False),
            app_dir=_APP_DIR,
            python=sys.executable,
        )
        try:
            fallback_root = tk.Tk()
            fallback_root.withdraw()
            fallback_root.attributes("-topmost", True)
            try:
                messagebox.showerror(t("msg_error") if t("msg_error") != "msg_error" else "錯誤", f"啟動失敗：{exc}", parent=fallback_root)
            finally:
                fallback_root.destroy()
        except Exception:
            pass
        try:
            release_single_instance_lock()
        except Exception:
            pass


if __name__ == "__main__":
    main()










