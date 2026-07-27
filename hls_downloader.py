import os
import time
import random
import glob
import shutil
import platform
import subprocess
import threading
import urllib.parse
import concurrent.futures

try:
    from Crypto.Cipher import AES as CryptoAES
except Exception:
    try:
        from Cryptodome.Cipher import AES as CryptoAES
    except Exception:
        CryptoAES = None

class HlsDownloader:
    def __init__(self, app, item_id, url, out_path, temp_out_path, progress_path, headers, ffmpeg_path, ffmpeg_version=""):
        self.app = app
        self.item_id = item_id
        self.url = url
        self.out_path = out_path
        self.temp_out_path = temp_out_path
        self.progress_path = progress_path
        self.headers = headers
        self.ffmpeg_path = ffmpeg_path
        self.ffmpeg_version = ffmpeg_version
        
        # Thread local session storage
        self.thread_local = threading.local()
        
    def download(self) -> bool:
        # Import lazily to avoid circular dependencies
        from downloader import (
            StopDownloadException,
            ParallelHlsUnsupportedSegmentContentException,
            ResumeLowSpeedReanalysisException,
            ParallelHlsRetryLaterException,
            DaemonThreadPoolExecutor,
            _normalize_download_url,
            _task_field_value,
            _set_task_aux_fields,
            _task_source_site_name,
            write_error_log,
            get_curl_cffi_requests,
            format_transfer_rate,
            format_eta,
            PARALLEL_HLS_RESUME_VALIDATION_VERSION,
            PARALLEL_HLS_MAX_SEGMENTS_FOR_NATIVE,
            PARALLEL_HLS_FAST_TRANSPORT_REMUX_MIN_SEGMENTS,
            PARALLEL_HLS_IN_FLIGHT_MULTIPLIER,
            PARALLEL_HLS_SCHEDULER_POLL_SECONDS,
            PARALLEL_HLS_SHORT_PLAYLIST_NO_PROGRESS_SEGMENTS,
            PARALLEL_HLS_SHORT_PLAYLIST_NO_PROGRESS_DELAY_SECONDS,
            FFMPEG_PROGRESS_UI_UPDATE_INTERVAL_SECONDS,
            FFMPEG_PROGRESS_UI_MIN_BYTES_DELTA,
            RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS,
            RESUME_PROGRESS_MIN_BYTES_DELTA,
            SLOW_SOURCE_REANALYZE_DELAY_SECONDS,
            RESUME_LOW_SPEED_REANALYZE_DELAY_SECONDS,
            HTTP_FILE_COPY_CHUNK_SIZE,
            _is_no_space_left_error,
            _ffmpeg_should_retry_with_audio_transcode,
            _dedupe_download_urls,
            PARALLEL_HLS_SLOW_CANDIDATE_RETRY_THRESHOLD_BPS_BY_SITE,
            PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_SECONDS,
            PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_BYTES,
            PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_SEGMENTS,
            PARALLEL_HLS_GOOGLE_RETRY_DELAYS,
            _summarize_log_exception
        )
        
        app = self.app
        item_id = self.item_id
        url = self.url
        out_path = self.out_path
        temp_out_path = self.temp_out_path
        progress_path = self.progress_path
        headers = self.headers
        ffmpeg_path = self.ffmpeg_path
        ffmpeg_version = self.ffmpeg_version

        parallel_hls_entered_at = time.time()
        task = app.tasks.get(item_id, {})
        task = app._ensure_task_active_transfer_state(item_id, task, reason="parallel_hls")
        if getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False):
            raise StopDownloadException("shutdown requested")
        if not app._should_try_parallel_hls_segments(url, task):
            return False
        if bool(_task_field_value(task, "is_mp3", False)):
            return False
        media_url, playlist_text = app._resolve_parallel_hls_media_playlist(url, headers)
        playlist_resolved_at = time.time()
        if getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False):
            raise StopDownloadException("shutdown requested")
            
        segments = app._parse_parallel_hls_segments(media_url, playlist_text)
        original_segment_count = len(segments)
        if original_segment_count > PARALLEL_HLS_MAX_SEGMENTS_FOR_NATIVE:
            write_error_log(
                "parallel hls skipped huge playlist",
                Exception("parallel HLS skipped huge playlist"),
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                segments=original_segment_count,
                max_segments=PARALLEL_HLS_MAX_SEGMENTS_FOR_NATIVE,
            )
            return False
        segments, skipped_leading_segments, skipped_trailing_segments = app._drop_unsupported_edge_parallel_hls_segments(segments, headers)
        segments_ready_at = time.time()
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
            app._set_task_parse_ui(
                item_id,
                message=f"已跳過來源非影片片段，開始下載 {len(segments)}/{original_segment_count} 段...",
            )
        if not segments:
            raise ParallelHlsUnsupportedSegmentContentException("HLS playlist contains no usable video segments")
        part_dir = f"{os.path.splitext(temp_out_path)[0]}.segments"
        os.makedirs(part_dir, exist_ok=True)
        if _task_source_site_name(task) == "avbebe":
            purged_invalid_parts = app._purge_invalid_parallel_hls_resume_parts(part_dir)
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
                app._set_task_parse_ui(
                    item_id,
                    message=f"已清除錯誤續傳片段 {purged_invalid_parts} 段，重新開始下載...",
                )
            segments, skipped_missing_leading_segments = app._drop_missing_leading_resume_hls_segments(segments, part_dir)
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
                app._set_task_parse_ui(
                    item_id,
                    message=f"續傳缺少開會 {skipped_missing_leading_segments} 段，已跳過並開始合併...",
                )
        if not segments:
            raise ParallelHlsUnsupportedSegmentContentException("HLS playlist contains no usable resume segments")
            
        stop_event = threading.Event()
        app._register_parallel_hls_stop_event(item_id, stop_event)
        key_cache = {}
        sessions_to_clean = []
        sessions_lock = threading.Lock()
        transport_path = f"{os.path.splitext(temp_out_path)[0]}.parallel.ts"
        merged_path = f"{os.path.splitext(temp_out_path)[0]}.parallel.mp4"
        concat_list_path = f"{os.path.splitext(temp_out_path)[0]}.parallel.ffconcat"
        total_segments = len(segments)
        total_duration = sum(max(float(segment.get("duration", 0.0) or 0.0), 0.0) for segment in segments)
        hls_host, representative_segment_url = app._dominant_parallel_hls_segment_host(media_url, segments)
        if representative_segment_url:
            app._set_task_active_media_url(task, representative_segment_url)

        def _part_path(segment):
            return os.path.join(part_dir, f"{int(segment['index']):06d}.ts")

        stored_progress_info = app._load_resume_progress_info(progress_path)
        stored_hls_info = stored_progress_info.get("progress_info", {}) if isinstance(stored_progress_info.get("progress_info", {}), dict) else {}
        stored_hls_total_segments = 0
        stored_hls_total_duration = 0.0
        stored_hls_completed_segments = 0
        stored_hls_resume_validation_version = 0
        if str(stored_hls_info.get("type", "") or "") == "parallel_hls":
            try:
                stored_hls_total_segments = max(int(stored_hls_info.get("hls_total_segments", 0) or 0), 0)
            except Exception:
                stored_hls_total_segments = 0
            try:
                stored_hls_total_duration = max(float(stored_hls_info.get("hls_total_duration_seconds", 0.0) or 0.0), 0.0)
            except Exception:
                stored_hls_total_duration = 0.0
            try:
                stored_hls_completed_segments = max(int(stored_hls_info.get("hls_completed_segments", 0) or 0), 0)
            except Exception:
                stored_hls_completed_segments = 0
            try:
                stored_hls_resume_validation_version = max(int(stored_hls_info.get("hls_resume_validation_version", 0) or 0), 0)
            except Exception:
                stored_hls_resume_validation_version = 0
        hls_total_duration_mismatch = False
        if stored_hls_total_duration > 0.0 and total_duration > 0.0:
            duration_tolerance = max(2.0, float(total_duration or 0.0) * 0.005)
            hls_total_duration_mismatch = abs(stored_hls_total_duration - float(total_duration or 0.0)) > duration_tolerance
        if (stored_hls_total_segments and stored_hls_total_segments != int(total_segments or 0)) or hls_total_duration_mismatch:
            stale_parts = 0
            try:
                stale_parts = len(glob.glob(os.path.join(part_dir, "*.ts")))
            except Exception:
                stale_parts = 0
            shutil.rmtree(part_dir, ignore_errors=True)
            os.makedirs(part_dir, exist_ok=True)
            app._remove_artifact_paths(progress_path)
            write_error_log(
                "parallel hls purged mismatched resume metadata",
                Exception("parallel HLS purged mismatched resume metadata"),
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                stored_total_segments=stored_hls_total_segments,
                current_total_segments=int(total_segments or 0),
                stored_total_duration_seconds=round(stored_hls_total_duration, 3),
                current_total_duration_seconds=round(float(total_duration or 0.0), 3),
                duration_mismatch=bool(hls_total_duration_mismatch),
                stale_parts=stale_parts,
                part_dir=part_dir,
            )

        completed_segment_indexes = set()
        existing_segments = []
        existing_segment_sizes = {}
        resume_scan_started_at = time.time()
        try:
            existing_part_file_names = {
                name
                for name in os.listdir(part_dir)
                if str(name or "").lower().endswith(".ts")
            }
        except OSError:
            existing_part_file_names = set()
        expected_part_file_names = set()
        for segment in segments:
            try:
                expected_part_file_names.add(f"{int(segment['index']):06d}.ts")
            except Exception:
                continue
        orphan_part_file_names = sorted(existing_part_file_names - expected_part_file_names)
        if orphan_part_file_names:
            removed_orphan_parts = 0
            failed_orphan_parts = 0
            removed_orphan_part_names = []
            for part_name in orphan_part_file_names:
                part_path = os.path.join(part_dir, part_name)
                try:
                    os.remove(part_path)
                    removed_orphan_parts += 1
                    removed_orphan_part_names.append(part_name)
                except OSError:
                    failed_orphan_parts += 1
                    continue
            if removed_orphan_parts:
                write_error_log(
                    "parallel hls purged orphan resume segments",
                    Exception("parallel HLS purged orphan resume segments"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    removed_orphan_parts=removed_orphan_parts,
                    failed_orphan_parts=failed_orphan_parts,
                    orphan_sample=", ".join(orphan_part_file_names[:8]),
                    part_dir=part_dir,
                )
                existing_part_file_names.difference_update(removed_orphan_part_names)
        resume_fast_scan_used = False
        present_expected_part_file_names = existing_part_file_names.intersection(expected_part_file_names)
        resume_metadata_matches = (
            stored_hls_total_segments == int(total_segments or 0)
            and not hls_total_duration_mismatch
        )
        resume_fast_scan_allowed = (
            resume_metadata_matches
            and stored_hls_resume_validation_version >= PARALLEL_HLS_RESUME_VALIDATION_VERSION
            and stored_hls_completed_segments > 0
            and len(present_expected_part_file_names) == stored_hls_completed_segments
        )
        if resume_fast_scan_allowed:
            fast_scan_empty_part = False
            for segment in segments:
                try:
                    part_name = f"{int(segment['index']):06d}.ts"
                except Exception:
                    continue
                if part_name not in present_expected_part_file_names:
                    continue
                part_path = os.path.join(part_dir, part_name)
                part_size = app._get_existing_file_size(part_path)
                if part_size <= 0:
                    fast_scan_empty_part = True
                    break
                existing_segments.append(segment)
                segment_index = int(segment["index"])
                completed_segment_indexes.add(segment_index)
                existing_segment_sizes[segment_index] = part_size
            resume_fast_scan_used = (
                not fast_scan_empty_part
                and len(existing_segments) == stored_hls_completed_segments
            )
            if not resume_fast_scan_used:
                existing_segments.clear()
                completed_segment_indexes.clear()
                existing_segment_sizes.clear()
        if not resume_fast_scan_used:
            invalid_part_file_names = []
            for segment in segments:
                try:
                    part_name = f"{int(segment['index']):06d}.ts"
                except Exception:
                    continue
                part_path = os.path.join(part_dir, part_name)
                if part_name not in existing_part_file_names:
                    continue
                if app._is_valid_parallel_hls_part_file(part_path):
                    existing_segments.append(segment)
                    segment_index = int(segment["index"])
                    completed_segment_indexes.add(segment_index)
                    existing_segment_sizes[segment_index] = app._get_existing_file_size(part_path)
                else:
                    invalid_part_file_names.append(part_name)
            if invalid_part_file_names:
                removed_invalid_parts = 0
                failed_invalid_parts = 0
                removed_invalid_part_names = []
                for part_name in invalid_part_file_names:
                    part_path = os.path.join(part_dir, part_name)
                    try:
                        os.remove(part_path)
                        removed_invalid_parts += 1
                        removed_invalid_part_names.append(part_name)
                    except OSError:
                        failed_invalid_parts += 1
                        continue
                if removed_invalid_parts:
                    write_error_log(
                        "parallel hls purged invalid resume segments",
                        Exception("parallel HLS purged invalid resume segments"),
                        url=media_url,
                        item_id=item_id,
                        source_site=_task_source_site_name(task) or None,
                        removed_invalid_parts=removed_invalid_parts,
                        failed_invalid_parts=failed_invalid_parts,
                        invalid_sample=", ".join(invalid_part_file_names[:8]),
                        part_dir=part_dir,
                    )
                    existing_part_file_names.difference_update(removed_invalid_part_names)
        resume_existing_segment_count = len(existing_segments)
        if stored_hls_total_segments and stored_hls_total_segments == int(total_segments or 0):
            if stored_hls_completed_segments and stored_hls_completed_segments != resume_existing_segment_count:
                actual_completed_bytes = 0
                actual_completed_duration = 0.0
                try:
                    actual_completed_bytes = sum(
                        int(existing_segment_sizes.get(int(segment["index"]), 0) or 0)
                        for segment in existing_segments
                    )
                    actual_completed_duration = sum(max(float(segment.get("duration", 0.0) or 0.0), 0.0) for segment in existing_segments)
                except Exception:
                    actual_completed_bytes = 0
                    actual_completed_duration = 0.0
                if resume_existing_segment_count > 0:
                    app._save_resume_progress(
                        progress_path,
                        actual_completed_duration,
                        source_url=_normalize_download_url(url) or url,
                        bytes_done=actual_completed_bytes,
                        progress_info={
                            "type": "parallel_hls",
                            "hls_total_segments": int(total_segments or 0),
                            "hls_completed_segments": int(resume_existing_segment_count or 0),
                            "hls_total_duration_seconds": round(float(total_duration or 0.0), 3),
                            "hls_resume_validation_version": int(PARALLEL_HLS_RESUME_VALIDATION_VERSION),
                        },
                        min_interval_seconds=0.0,
                        min_bytes_delta=0,
                        force=True,
                    )
                else:
                    app._remove_artifact_paths(progress_path)
                write_error_log(
                    "parallel hls resume metadata differs from parts",
                    Exception("parallel HLS resume metadata differs from actual part files"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    stored_completed_segments=stored_hls_completed_segments,
                    existing_part_segments=resume_existing_segment_count,
                    total_segments=int(total_segments or 0),
                    part_dir=part_dir,
                )
                write_error_log(
                    "parallel hls resume progress corrected from parts",
                    Exception("parallel HLS resume progress corrected from actual part files"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    stored_completed_segments=stored_hls_completed_segments,
                    corrected_completed_segments=resume_existing_segment_count,
                    corrected_bytes=actual_completed_bytes,
                    corrected_duration_seconds=round(actual_completed_duration, 3),
                    progress_path=progress_path,
                    progress_removed=resume_existing_segment_count <= 0,
                )
        resume_scan_finished_at = time.time()
        has_google_segments = any(
            "googleusercontent.com" in urllib.parse.urlsplit(_normalize_download_url(segment.get("url", "")) or "").netloc.lower()
            for segment in segments
        )
        source_site = _task_source_site_name(task)
        prefer_curl_segments = source_site in (
            "99itv",
            "18av",
            "movieffm",
            "avbebe",
            "85xvideo",
            "bestjavporn",
            "dramasq",
            "getav",
            "gimy",
            "goodav17",
            "hayav",
            "hohoj",
            "ikanbot",
            "jable",
            "javdock",
            "missav",
            "njav",
            "njavtv",
            "nnyy",
            "olevod",
            "supjav",
            "thanju",
            "tinyavideo",
            "xiaoyakankan",
        )
        try:
            preflight_started_at = time.time()
            if any((segment.get("key") or {}).get("uri") for segment in segments) and CryptoAES is None:
                return False
            key_cache = app._fetch_parallel_hls_keys(segments, headers, stop_event=stop_event)
            if getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False) or stop_event.is_set():
                raise StopDownloadException("shutdown requested")
            app._preflight_parallel_hls_segments(
                segments,
                headers,
                sample_limit=1 if resume_existing_segment_count else 3,
                stop_event=stop_event,
                prefer_curl=prefer_curl_segments,
            )
            if getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False) or stop_event.is_set():
                raise StopDownloadException("shutdown requested")
            preflight_finished_at = time.time()
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
            
        completed_bytes = 0
        completed_duration = 0.0
        completed_segments = 0
        started_at = time.time()
        last_segment_ui_update = 0.0
        last_segment_ui_bytes = 0
        completed_lock = threading.Lock()
        if existing_segments:
            completed_bytes = sum(
                int(existing_segment_sizes.get(int(segment["index"]), 0) or 0)
                for segment in existing_segments
            )
            completed_duration = sum(max(float(segment.get("duration", 0.0) or 0.0), 0.0) for segment in existing_segments)
            completed_segments = len(existing_segments)
            write_error_log(
                "parallel hls resume segments loaded",
                Exception("parallel HLS resume segments loaded"),
                url=media_url,
                item_id=item_id,
                source_site=_task_source_site_name(task) or None,
                loaded_segments=completed_segments,
                total_segments=total_segments,
                loaded_bytes=completed_bytes,
                loaded_duration_seconds=round(completed_duration, 3),
                resume_fast_scan_used=bool(resume_fast_scan_used),
                resume_validation_version=int(stored_hls_resume_validation_version or 0),
                part_dir=part_dir,
            )
            if total_duration > 0:
                percent = min((completed_duration / total_duration) * 100.0, 99.0)
                app.update_tree_many(item_id, {
                    "progress": f"{percent:.1f}%",
                    "size": f"{completed_segments}/{total_segments}",
                    "speed_eta": f"續傳已載入 {completed_segments}/{total_segments} 段",
                }, force=True)
            app._save_resume_progress(
                progress_path,
                completed_duration,
                source_url=_normalize_download_url(url) or url,
                bytes_done=completed_bytes,
                progress_info={
                    "type": "parallel_hls",
                    "hls_total_segments": int(total_segments or 0),
                    "hls_completed_segments": int(completed_segments or 0),
                    "hls_total_duration_seconds": round(float(total_duration or 0.0), 3),
                    "hls_resume_validation_version": int(PARALLEL_HLS_RESUME_VALIDATION_VERSION),
                },
            )

        pending_segments = [segment for segment in segments if int(segment["index"]) not in completed_segment_indexes]
        pending_segment_count = len(pending_segments)
        session_start_completed_bytes = int(completed_bytes or 0)
        session_start_completed_segments = int(completed_segments or 0)
        last_segment_speed_update = time.time()
        last_segment_speed_bytes = int(completed_bytes or 0)
        last_progress_activity_at = time.time()
        last_progress_completed_segments = int(completed_segments or 0)
        worker_plan_segments = pending_segments
        worker_plan = app._parallel_hls_worker_plan(
            source_site,
            media_url,
            worker_plan_segments,
            total_segment_count=total_segments,
        )
        worker_count = (
            min(int(worker_plan.get("workers", 1)), pending_segment_count)
            if pending_segment_count > 0
            else 0
        )
        hls_host_active_downloads = app._active_hls_downloads_for_host(hls_host)
        hls_host_worker_budget = app._hls_host_worker_budget(hls_host)
        try:
            representative_segment_url = str((worker_plan_segments or segments or [{}])[0].get("url") or media_url)
        except Exception:
            representative_segment_url = media_url
        segment_timeout_seconds = app._parallel_hls_segment_timeout(representative_segment_url, stop_event=stop_event)
        segment_retry_count = app._parallel_hls_segment_retry_count(representative_segment_url)
        _set_task_aux_fields(
            task,
            _parallel_hls_total_segments=int(total_segments),
            _parallel_hls_completed_segments=int(completed_segments),
            _parallel_hls_pending_segments=max(int(total_segments) - int(completed_segments), 0),
            _parallel_hls_workers=int(worker_count),
            _parallel_hls_updated_at=time.time(),
        )
        parallel_start_logged_at = time.time()
        try:
            route_selected_at = float(_task_field_value(task, "_m3u8_route_selected_at", 0.0) or 0.0)
        except Exception:
            route_selected_at = 0.0
            
        if pending_segment_count > 0:
            app._log_ffmpeg_event(
                "parallel hls download started",
                Exception("parallel hls started"),
                task,
                item_id,
                media_url,
                segments=total_segments,
                pending_segments=pending_segment_count,
                completed_segments_at_start=completed_segments,
                workers=worker_count,
                route_start_delay_seconds=app._m3u8_route_start_delay_seconds(task),
                route_selected_to_parallel_entry_seconds=(round(max(parallel_hls_entered_at - route_selected_at, 0.0), 3) if route_selected_at > 0 else 0.0),
                parallel_entry_to_download_start_seconds=round(max(parallel_start_logged_at - parallel_hls_entered_at, 0.0), 3),
                playlist_resolve_seconds=round(max(playlist_resolved_at - parallel_hls_entered_at, 0.0), 3),
                segment_parse_seconds=round(max(segments_ready_at - playlist_resolved_at, 0.0), 3),
                resume_scan_seconds=round(max(resume_scan_finished_at - resume_scan_started_at, 0.0), 3),
                resume_fast_scan_used=bool(resume_fast_scan_used),
                resume_validation_version=int(stored_hls_resume_validation_version or 0),
                preflight_seconds=round(max(preflight_finished_at - preflight_started_at, 0.0), 3),
                requested_workers=int(worker_plan.get("requested_workers", worker_count) or worker_count),
                site_worker_cap=int(worker_plan.get("site_worker_cap", 0) or 0),
                host_worker_cap=int(worker_plan.get("host_worker_cap", 0) or 0),
                host_worker_marker=str(worker_plan.get("host_worker_marker", "") or ""),
                per_task_worker_budget=int(worker_plan.get("per_task_worker_budget", hls_host_worker_budget) or hls_host_worker_budget),
                worker_budget_limited=bool(worker_plan.get("budget_limited", False)),
                single_task_boost_applied=bool(worker_plan.get("boost_applied", False)),
                boost_segment_count=int(worker_plan.get("boost_segment_count", 0) or 0),
                boost_worker_cap=int(worker_plan.get("boost_worker_cap", 0) or 0),
                tail_worker_cap=int(worker_plan.get("tail_worker_cap", 0) or 0),
                tail_worker_shrink_applied=bool(worker_plan.get("tail_worker_shrink_applied", False)),
                resume_tail_batch=bool(worker_plan.get("resume_tail_batch", False)),
                hls_host=hls_host,
                manifest_host=urllib.parse.urlsplit(_normalize_download_url(media_url) or "").netloc.lower(),
                segment_timeout_seconds=round(float(segment_timeout_seconds or 0.0), 3),
                segment_retry_count=int(segment_retry_count or 0),
                hls_host_active_downloads=hls_host_active_downloads,
                hls_host_worker_budget=hls_host_worker_budget,
                total_duration=total_duration,
                **app._build_ffmpeg_runtime_fields(ffmpeg_path, ffmpeg_version=ffmpeg_version),
            )

        def _should_retry_slow_parallel_candidate(speed_bps, session_bytes, session_segments, now):
            source_site = _task_source_site_name(task)
            threshold_bps = int(PARALLEL_HLS_SLOW_CANDIDATE_RETRY_THRESHOLD_BPS_BY_SITE.get(source_site, 500 * 1024) or 0)
            if threshold_bps <= 0 or float(speed_bps or 0.0) >= threshold_bps:
                return False
            if bool(_task_field_value(task, "_parallel_hls_slow_candidate_retry_attempted", False)):
                return False
            if max(float(now or time.time()) - float(started_at or 0.0), 0.0) < PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_SECONDS:
                return False
            if int(session_bytes or 0) < PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_BYTES:
                return False
            if int(session_segments or 0) < PARALLEL_HLS_SLOW_CANDIDATE_RETRY_MIN_SEGMENTS:
                return False
            alternate_candidates = _dedupe_download_urls(
                list(_task_field_value(task, "fallback_urls", []) or [])
                + list(_task_field_value(task, "page_refresh_candidates", []) or []),
                primary_url=media_url,
            )
            if not alternate_candidates:
                return False
            _set_task_aux_fields(task, _parallel_hls_slow_candidate_retry_attempted=True)
            write_error_log(
                "parallel hls slow candidate retry requested",
                ResumeLowSpeedReanalysisException("parallel HLS candidate stayed below site speed threshold"),
                url=media_url,
                item_id=item_id,
                source_site=source_site or None,
                hls_host=hls_host,
                speed_bps=int(speed_bps or 0),
                threshold_bps=threshold_bps,
                elapsed_seconds=round(max(float(now or time.time()) - float(started_at or 0.0), 0.0), 3),
                session_segment_bytes=int(session_bytes or 0),
                session_completed_segments=int(session_segments or 0),
                alternate_candidate_count=len(alternate_candidates),
            )
            return True

        def _download_one(segment):
            nonlocal completed_bytes, completed_duration, completed_segments, last_segment_ui_update, last_segment_ui_bytes, last_segment_speed_update, last_segment_speed_bytes, last_progress_activity_at, last_progress_completed_segments, dynamic_in_flight_limit
            if getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False):
                stop_event.set()
                raise StopDownloadException("shutdown requested")
            task_state = str(_task_field_value(app.tasks.get(item_id, {}), "state", "") or "")
            if app._is_pause_requested_state(task_state) or app._is_delete_requested_state(task_state):
                stop_event.set()
                raise StopDownloadException("stop requested")
            if app._maybe_auto_pause_for_disk_space(item_id, out_path, note=app._disk_full_pause_note()):
                stop_event.set()
                raise StopDownloadException("disk space low")
            session = getattr(self.thread_local, "session", None)
            if session is not None and getattr(session, "_closed", False):
                session = None
            if session is None:
                c_req = get_curl_cffi_requests()
                session = c_req.Session(impersonate="chrome120")
                try:
                    from curl_cffi import CurlOpt
                    session.curl_options = {
                        CurlOpt.BUFFERSIZE: 262144,
                        CurlOpt.TCP_NODELAY: 1,
                        CurlOpt.DNS_CACHE_TIMEOUT: 600,
                        CurlOpt.PIPEWAIT: 1,
                    }
                except Exception:
                    session.curl_options = {
                        98: 262144,
                        121: 1,
                        92: 600,
                        237: 1,
                    }
                try:
                    session = app._track_network_session(session)
                except Exception:
                    pass
                self.thread_local.session = session
                with sessions_lock:
                    sessions_to_clean.append(session)
            t0 = time.time()
            try:
                part_size = app._download_parallel_hls_segment(
                    segment,
                    _part_path(segment),
                    headers,
                    key_cache,
                    stop_event,
                    prefer_curl=prefer_curl_segments,
                    session=session,
                )
                dur = max(time.time() - t0, 0.001)
                with completed_lock:
                    if dur > 5.0:
                        dynamic_in_flight_limit = max(2, dynamic_in_flight_limit // 2)
                    elif dur < 2.0:
                        dynamic_in_flight_limit = min(dynamic_in_flight_limit + 1, max_in_flight_limit)
            except OSError as exc:
                with completed_lock:
                    dynamic_in_flight_limit = max(2, dynamic_in_flight_limit // 2)
                if _is_no_space_left_error(exc):
                    stop_event.set()
                    free_bytes = app._get_disk_free_bytes(out_path)
                    app._pause_task_for_disk_full(item_id, out_path, free_bytes, None, note=app._disk_full_pause_note())
                    raise StopDownloadException("disk space low")
                self.thread_local.session = None
                raise
            except StopDownloadException:
                raise
            except Exception:
                with completed_lock:
                    dynamic_in_flight_limit = max(2, dynamic_in_flight_limit // 2)
                self.thread_local.session = None
                raise
            if app._maybe_auto_pause_for_disk_space(item_id, out_path, note=app._disk_full_pause_note()):
                stop_event.set()
                raise StopDownloadException("disk space low")
            with completed_lock:
                completed_segment_indexes.add(int(segment["index"]))
                completed_bytes += int(part_size or 0)
                completed_duration += max(float(segment.get("duration", 0.0) or 0.0), 0.0)
                completed_segments += 1
                last_progress_activity_at = now = time.time()
                last_progress_completed_segments = int(completed_segments or 0)
                _set_task_aux_fields(
                    task,
                    downloaded_bytes=int(completed_bytes or 0),
                    _parallel_hls_total_segments=int(total_segments),
                    _parallel_hls_completed_segments=int(completed_segments),
                    _parallel_hls_pending_segments=max(int(total_segments) - int(completed_segments), 0),
                    _parallel_hls_workers=int(worker_count),
                    _parallel_hls_updated_at=time.time(),
                )
                elapsed = max(now - started_at, 0.001)
                session_segment_bytes = max(int(completed_bytes or 0) - int(session_start_completed_bytes or 0), 0)
                session_completed_segments = max(int(completed_segments or 0) - int(session_start_completed_segments or 0), 0)
                speed_bps = session_segment_bytes / elapsed if session_segment_bytes > 0 else 0.0
                if _should_retry_slow_parallel_candidate(speed_bps, session_segment_bytes, session_completed_segments, now):
                    stop_event.set()
                    raise ResumeLowSpeedReanalysisException("parallel HLS candidate too slow; trying next candidate")
                is_complete = completed_segments >= total_segments
                should_refresh_progress_ui = (
                    is_complete
                    or last_segment_ui_update <= 0.0
                    or (now - last_segment_ui_update) >= FFMPEG_PROGRESS_UI_UPDATE_INTERVAL_SECONDS
                    or abs(completed_bytes - last_segment_ui_bytes) >= FFMPEG_PROGRESS_UI_MIN_BYTES_DELTA
                )
                if total_duration > 0 and should_refresh_progress_ui:
                    interval_seconds = max(now - last_segment_speed_update, 0.001)
                    interval_bytes = max(int(completed_bytes or 0) - int(last_segment_speed_bytes or 0), 0)
                    display_speed_bps = interval_bytes / interval_seconds if interval_bytes > 0 else speed_bps
                    last_segment_speed_update = now
                    last_segment_speed_bytes = int(completed_bytes or 0)
                    average_segment_bytes = (session_segment_bytes / session_completed_segments) if session_completed_segments > 0 else 0.0
                    remaining_bytes = max(int(average_segment_bytes * max(total_segments - completed_segments, 0)), 0)
                    eta = (remaining_bytes / max(display_speed_bps, 1.0)) if remaining_bytes > 0 and display_speed_bps > 0 else None
                    app._set_task_last_speed(task, display_speed_bps)
                    percent = min((completed_duration / total_duration) * 100.0, 99.0)
                    app.update_tree_many(item_id, {
                        "progress": f"{percent:.1f}%",
                        "size": f"{completed_segments}/{total_segments}",
                        "speed_eta": f"{format_transfer_rate(display_speed_bps)} | {format_eta(eta)}" if eta else format_transfer_rate(display_speed_bps),
                    }, force=False)
                    last_segment_ui_update = now
                    last_segment_ui_bytes = completed_bytes
                if should_refresh_progress_ui:
                    app._save_resume_progress(
                        progress_path,
                        completed_duration,
                        source_url=_normalize_download_url(url) or url,
                        bytes_done=completed_bytes,
                        progress_info={
                            "type": "parallel_hls",
                            "hls_total_segments": int(total_segments or 0),
                            "hls_completed_segments": int(completed_segments or 0),
                            "hls_total_duration_seconds": round(float(total_duration or 0.0), 3),
                            "hls_resume_validation_version": int(PARALLEL_HLS_RESUME_VALIDATION_VERSION),
                        },
                        min_interval_seconds=RESUME_PROGRESS_PERSIST_INTERVAL_SECONDS,
                        min_bytes_delta=RESUME_PROGRESS_MIN_BYTES_DELTA,
                    )
            return part_size

        shutdown_finalize_guard_logged = False

        def _all_segments_ready_for_finalization():
            try:
                with completed_lock:
                    return total_segments > 0 and len(completed_segment_indexes) >= total_segments
            except Exception:
                return False

        def _shutdown_can_finalize_completed_segments(current_task_state=""):
            nonlocal shutdown_finalize_guard_logged
            if app._is_delete_requested_state(current_task_state) or app._is_pause_requested_state(current_task_state):
                return False
            if not (getattr(app, "_shutdown_stop_requested", False) or getattr(app, "_shutdown_started", False) or stop_event.is_set()):
                return False
            if not _all_segments_ready_for_finalization():
                return False
            if not shutdown_finalize_guard_logged:
                shutdown_finalize_guard_logged = True
                try:
                    app._log_ffmpeg_event(
                        "parallel hls shutdown finalize guard activated",
                        Exception("parallel HLS has all segments during shutdown; finalizing output"),
                        task,
                        item_id,
                        media_url,
                        segments=total_segments,
                        completed_segments=len(completed_segment_indexes),
                        shutdown_started=bool(getattr(app, "_shutdown_started", False)),
                        shutdown_stop_requested=bool(getattr(app, "_shutdown_stop_requested", False)),
                    )
                except Exception:
                    pass
            return True

        try:
            remux_strategy = "concat"
            if pending_segments:
                max_in_flight_limit = max(32, int(worker_count))
                executor = DaemonThreadPoolExecutor(max_workers=max_in_flight_limit)
                pending_iter = iter(pending_segments)
                in_flight = set()
                stop_requested = False
                dynamic_in_flight_limit = min(4, worker_count)

                def _submit_next_segment():
                    try:
                        next_segment = next(pending_iter)
                    except StopIteration:
                        return False
                    in_flight.add(executor.submit(_download_one, next_segment))
                    return True

                try:
                    for _ in range(min(dynamic_in_flight_limit, len(pending_segments))):
                        if stop_event.is_set() or getattr(app, "_shutdown_stop_requested", False) or app._shutdown_started:
                            if _shutdown_can_finalize_completed_segments():
                                stop_event.clear()
                                break
                            stop_requested = True
                            raise StopDownloadException("stop requested")
                        if not _submit_next_segment():
                            break
                    while in_flight:
                        current_task_state = str(_task_field_value(app.tasks.get(item_id, {}), "state", "") or "")
                        if stop_event.is_set() or app._is_pause_requested_state(current_task_state) or app._is_delete_requested_state(current_task_state) or getattr(app, "_shutdown_stop_requested", False) or app._shutdown_started:
                            if _shutdown_can_finalize_completed_segments(current_task_state):
                                stop_event.clear()
                                break
                            stop_requested = True
                            stop_event.set()
                            for pending_future in in_flight:
                                pending_future.cancel()
                            raise StopDownloadException("stop requested")
                        done, in_flight = concurrent.futures.wait(
                            in_flight,
                            timeout=PARALLEL_HLS_SCHEDULER_POLL_SECONDS,
                            return_when=concurrent.futures.FIRST_COMPLETED,
                        )
                        if not done:
                            now = time.time()
                            try:
                                stalled_seconds = max(now - float(last_progress_activity_at or started_at), 0.0)
                            except Exception:
                                stalled_seconds = 0.0
                            no_progress_reanalysis_delay = min(
                                float(SLOW_SOURCE_REANALYZE_DELAY_SECONDS),
                                float(RESUME_LOW_SPEED_REANALYZE_DELAY_SECONDS),
                            )
                            short_playlist_grace_applied = False
                            if 0 < int(total_segments or 0) <= int(PARALLEL_HLS_SHORT_PLAYLIST_NO_PROGRESS_SEGMENTS):
                                short_playlist_grace_applied = True
                                no_progress_reanalysis_delay = max(
                                    no_progress_reanalysis_delay,
                                    float(PARALLEL_HLS_SHORT_PLAYLIST_NO_PROGRESS_DELAY_SECONDS),
                                )
                            if stalled_seconds >= no_progress_reanalysis_delay:
                                current_completed = int(completed_segments or 0)
                                if current_completed <= int(last_progress_completed_segments or 0) and app._should_trigger_resume_low_speed_reanalysis(
                                    task,
                                    _normalize_download_url(_task_field_value(task, "url", "")) or media_url,
                                    os.path.dirname(out_path) or app._app_dir_fallback(),
                                    0.0,
                                    now=now,
                                    allow_zero_progress=True,
                                ):
                                    stop_event.set()
                                    for pending_future in in_flight:
                                        pending_future.cancel()
                                    write_error_log(
                                        "parallel hls no progress reanalysis requested",
                                        Exception("parallel HLS made no segment progress and requested source reanalysis"),
                                        url=media_url,
                                        item_id=item_id,
                                        source_site=_task_source_site_name(task) or None,
                                        stalled_seconds=round(stalled_seconds, 3),
                                        no_progress_reanalysis_delay_seconds=round(float(no_progress_reanalysis_delay), 3),
                                        short_playlist_grace_applied=bool(short_playlist_grace_applied),
                                        completed_segments=current_completed,
                                        total_segments=total_segments,
                                        in_flight=len(in_flight),
                                    )
                                    raise ResumeLowSpeedReanalysisException("parallel HLS made no progress; reanalyzing source")
                            continue
                        for future in done:
                            if stop_event.is_set() or getattr(app, "_shutdown_stop_requested", False) or app._shutdown_started:
                                current_task_state = str(_task_field_value(app.tasks.get(item_id, {}), "state", "") or "")
                                if _shutdown_can_finalize_completed_segments(current_task_state):
                                    stop_event.clear()
                                    break
                                stop_requested = True
                                raise StopDownloadException("stop requested")
                            future.result()
                        if stop_event.is_set() or getattr(app, "_shutdown_stop_requested", False) or app._shutdown_started:
                            current_task_state = str(_task_field_value(app.tasks.get(item_id, {}), "state", "") or "")
                            if _shutdown_can_finalize_completed_segments(current_task_state):
                                stop_event.clear()
                                break
                            stop_requested = True
                            raise StopDownloadException("stop requested")
                        while len(in_flight) < dynamic_in_flight_limit:
                            if stop_event.is_set() or getattr(app, "_shutdown_stop_requested", False) or app._shutdown_started:
                                current_task_state = str(_task_field_value(app.tasks.get(item_id, {}), "state", "") or "")
                                if _shutdown_can_finalize_completed_segments(current_task_state):
                                    stop_event.clear()
                                    break
                                stop_requested = True
                                raise StopDownloadException("stop requested")
                            if not _submit_next_segment():
                                break
                except BaseException:
                    stop_requested = True
                    stop_event.set()
                    for future in in_flight:
                        future.cancel()
                    raise
                finally:
                    executor.shutdown(wait=not stop_requested and not app._shutdown_started, cancel_futures=True)
                    with sessions_lock:
                        for s in sessions_to_clean:
                            try:
                                app._close_network_session(s)
                            except Exception:
                                pass
            else:
                app._log_ffmpeg_event(
                    "parallel hls resume complete before download",
                    Exception("parallel HLS resume has all segments; remux only"),
                    task,
                    item_id,
                    media_url,
                    segments=total_segments,
                    completed_segments_at_start=completed_segments,
                    part_dir=part_dir,
                    **app._build_ffmpeg_runtime_fields(ffmpeg_path, ffmpeg_version=ffmpeg_version),
                )
            app.update_tree_many(item_id, {
                "progress": "99.9%",
                "size": f"{completed_segments}/{total_segments}",
                "speed_eta": "合併影片中...",
            }, force=True)
            current_task_state_before_remux = str(_task_field_value(app.tasks.get(item_id, task), "state", "") or "")
            if (
                _all_segments_ready_for_finalization()
                and (
                    getattr(app, "_shutdown_stop_requested", False)
                    or getattr(app, "_shutdown_started", False)
                    or stop_event.is_set()
                )
                and not app._is_delete_requested_state(current_task_state_before_remux)
            ):
                with completed_lock:
                    deferred_completed_segments = int(completed_segments or 0)
                    deferred_completed_bytes = int(completed_bytes or 0)
                    deferred_completed_duration = float(completed_duration or 0.0)
                app._save_resume_progress(
                    progress_path,
                    deferred_completed_duration,
                    source_url=_normalize_download_url(url) or url,
                    bytes_done=deferred_completed_bytes,
                    progress_info={
                        "type": "parallel_hls",
                        "hls_total_segments": int(total_segments or 0),
                        "hls_completed_segments": int(deferred_completed_segments or 0),
                        "hls_total_duration_seconds": round(float(total_duration or 0.0), 3),
                        "hls_resume_validation_version": int(PARALLEL_HLS_RESUME_VALIDATION_VERSION),
                    },
                    min_interval_seconds=0.0,
                    min_bytes_delta=0,
                    force=True,
                )
                app._log_ffmpeg_event(
                    "parallel hls remux deferred during shutdown",
                    Exception("parallel HLS remux deferred until next startup"),
                    task,
                    item_id,
                    media_url,
                    completed_segments=deferred_completed_segments,
                    total_segments=total_segments,
                    completed_bytes=deferred_completed_bytes,
                    completed_duration_seconds=round(deferred_completed_duration, 3),
                    progress_path=progress_path,
                )
                raise StopDownloadException("shutdown requested before remux")
            ordered_part_paths = []
            for segment in segments:
                part_path = _part_path(segment)
                if int(segment["index"]) not in completed_segment_indexes or not app._has_nonempty_file(part_path):
                    raise Exception("parallel HLS segment missing after download")
                ordered_part_paths.append(part_path)
            prefer_transport_remux = int(total_segments or 0) >= int(PARALLEL_HLS_FAST_TRANSPORT_REMUX_MIN_SEGMENTS)

            def _build_transport_stream_for_remux():
                with open(transport_path, "wb") as merged_f:
                    for part_path in ordered_part_paths:
                        with open(part_path, "rb") as part_f:
                            shutil.copyfileobj(part_f, merged_f, length=HTTP_FILE_COPY_CHUNK_SIZE)

            app.update_tree_many(item_id, {"speed_eta": "正在合併影音檔案，請稍候..."}, force=True)
            with app._ffmpeg_remux_lock:
                if prefer_transport_remux:
                    try:
                        _build_transport_stream_for_remux()
                        app._remux_parallel_hls_transport_stream(ffmpeg_path, transport_path, merged_path)
                        remux_strategy = "transport-fast"
                    except Exception as transport_exc:
                        write_error_log(
                            "parallel hls transport remux fallback to concat",
                            transport_exc,
                            url=media_url,
                            item_id=item_id,
                            source_site=_task_source_site_name(task) or None,
                            segments=total_segments,
                        )
                        app._remux_parallel_hls_segment_files(ffmpeg_path, ordered_part_paths, concat_list_path, merged_path)
                        remux_strategy = "concat-after-transport-fallback"
                else:
                    try:
                        app._remux_parallel_hls_segment_files(ffmpeg_path, ordered_part_paths, concat_list_path, merged_path)
                    except Exception as concat_exc:
                        write_error_log(
                            "parallel hls concat remux fallback to transport",
                            concat_exc,
                            url=media_url,
                            item_id=item_id,
                            source_site=_task_source_site_name(task) or None,
                            segments=total_segments,
                        )
                        _build_transport_stream_for_remux()
                        app._remux_parallel_hls_transport_stream(ffmpeg_path, transport_path, merged_path)
                        remux_strategy = "transport"
                final_info = app._probe_media_info(merged_path)
            final_duration = float(final_info.get("duration", 0.0) or 0.0)
            if not final_info.get("valid") or int(final_info.get("size", 0) or 0) <= 0:
                raise Exception("parallel HLS remux produced invalid output")
            if app._is_incomplete_hls_video_artifact(task, merged_path, expected_duration=total_duration):
                raise Exception("parallel HLS remux produced incomplete output")
            if total_duration > 300.0 and final_duration > 0.0 and final_duration + max(60.0, total_duration * 0.02) < total_duration:
                raise Exception(f"parallel HLS output duration mismatch: duration={final_duration:.3f} expected={total_duration:.3f}")
            if total_duration > 300.0 and final_duration <= 0.0:
                raise Exception(f"parallel HLS output duration missing: expected={total_duration:.3f}")
            current_task = app.tasks.get(item_id, task)
            current_state = str(_task_field_value(current_task, "state", "") or "")
            if current_state in ("DELETED", "DELETE_REQUESTED"):
                app._discard_deleted_task(item_id)
                raise KeyboardInterrupt()
            if (
                app._is_pause_requested_state(current_state)
                and pending_segment_count <= 0
                and app._has_nonempty_file(merged_path)
            ):
                task = current_task
                _set_task_aux_fields(task, state="DOWNLOADING", _stop_reason=None, resume_requested=True, _manual_pause_requested=False)
                app._update_task_state_entry(task, state="DOWNLOADING", resume_requested=True, _stop_reason=None)
                write_error_log(
                    "parallel hls completed remux finalizing during shutdown",
                    Exception("parallel HLS completed remux is being finalized during shutdown"),
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    segments=total_segments,
                    completed_segments=completed_segments,
                    pending_segments=pending_segment_count,
                    shutdown_started=bool(app._shutdown_started),
                )
            else:
                task = app._ensure_task_can_continue(item_id)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with app._resume_artifact_lock_for(merged_path, out_path):
                if not os.path.exists(merged_path) and app._has_nonempty_file(out_path):
                    pass
                else:
                    if os.path.exists(out_path):
                        try:
                            os.remove(out_path)
                        except OSError:
                            pass
                    app._move_file_with_retry(merged_path, out_path, attempts=48, delay_seconds=0.5)
            app._remove_artifact_paths(temp_out_path, transport_path, concat_list_path, progress_path)
            shutil.rmtree(part_dir, ignore_errors=True)
            app._set_task_output_file(task, item_id, out_path)
            app._set_task_named_column_text(item_id, "progress", "100%")
            if not app._mark_task_finished(item_id):
                raise Exception("parallel HLS output failed final validation")
            logged_output_path = app._task_output_path_or_default(task, out_path)
            logged_output_size = app._get_existing_file_size(logged_output_path)
            elapsed_seconds = max(time.time() - started_at, 0.001)
            session_segment_bytes = max(int(completed_bytes or 0) - session_start_completed_bytes, 0)
            session_completed_segments = max(int(completed_segments or 0) - session_start_completed_segments, 0)
            session_segment_average_speed_bps = int(session_segment_bytes / elapsed_seconds) if session_segment_bytes > 0 else 0
            _completion_elapsed_seconds, output_full_file_average_speed_bps, output_effective_average_speed_bps = app._set_task_completion_average_speed(
                task,
                logged_output_size,
                started_at,
                preferred_speed_bps=session_segment_average_speed_bps,
            )
            app._log_ffmpeg_event(
                "parallel hls download finished",
                Exception("parallel hls finished"),
                task,
                item_id,
                media_url,
                output=logged_output_path,
                bytes=logged_output_size,
                segments=total_segments,
                workers=worker_count,
                hls_host=hls_host,
                hls_host_active_downloads=hls_host_active_downloads,
                hls_host_worker_budget=hls_host_worker_budget,
                tail_worker_cap=int(worker_plan.get("tail_worker_cap", 0) or 0),
                tail_worker_shrink_applied=bool(worker_plan.get("tail_worker_shrink_applied", False)),
                resume_tail_batch=bool(worker_plan.get("resume_tail_batch", False)),
                remux_strategy=remux_strategy,
                elapsed_seconds=round(elapsed_seconds, 3),
                session_segment_bytes=session_segment_bytes,
                session_completed_segments=session_completed_segments,
                session_segment_average_speed_bps=session_segment_average_speed_bps,
                output_effective_average_speed_bps=output_effective_average_speed_bps,
                output_full_file_average_speed_bps=output_full_file_average_speed_bps,
            )
            return True
        except StopDownloadException:
            stop_event.set()
            try:
                with completed_lock:
                    interrupted_completed_segments = int(completed_segments or 0)
                    interrupted_completed_bytes = int(completed_bytes or 0)
                    interrupted_completed_duration = float(completed_duration or 0.0)
                interrupted_elapsed_seconds = max(time.time() - started_at, 0.001)
                interrupted_session_bytes = max(interrupted_completed_bytes - session_start_completed_bytes, 0)
                interrupted_session_segments = max(interrupted_completed_segments - session_start_completed_segments, 0)
                interrupted_average_speed_bps = int(interrupted_session_bytes / interrupted_elapsed_seconds) if interrupted_session_bytes > 0 else 0
                app._save_resume_progress(
                    progress_path,
                    interrupted_completed_duration,
                    source_url=_normalize_download_url(url) or url,
                    bytes_done=interrupted_completed_bytes,
                    progress_info={
                        "type": "parallel_hls",
                        "hls_total_segments": int(total_segments or 0),
                        "hls_completed_segments": int(interrupted_completed_segments or 0),
                        "hls_total_duration_seconds": round(float(total_duration or 0.0), 3),
                        "hls_resume_validation_version": int(PARALLEL_HLS_RESUME_VALIDATION_VERSION),
                    },
                    min_interval_seconds=0.0,
                    min_bytes_delta=0,
                    force=True,
                )
                current_state = str(_task_field_value(app.tasks.get(item_id, task), "state", "") or "")
                app._log_ffmpeg_event(
                    "parallel hls download interrupted",
                    Exception("parallel hls interrupted"),
                    app.tasks.get(item_id, task),
                    item_id,
                    media_url,
                    completed_segments=interrupted_completed_segments,
                    pending_segments=max(int(total_segments or 0) - interrupted_completed_segments, 0),
                    completed_bytes=interrupted_completed_bytes,
                    completed_duration_seconds=round(interrupted_completed_duration, 3),
                    segments=total_segments,
                    workers=worker_count,
                    elapsed_seconds=round(interrupted_elapsed_seconds, 3),
                    session_segment_bytes=interrupted_session_bytes,
                    session_completed_segments=interrupted_session_segments,
                    session_segment_average_speed_bps=interrupted_average_speed_bps,
                    hls_host=hls_host,
                    hls_host_active_downloads=hls_host_active_downloads,
                    hls_host_worker_budget=hls_host_worker_budget,
                    tail_worker_cap=int(worker_plan.get("tail_worker_cap", 0) or 0),
                    tail_worker_shrink_applied=bool(worker_plan.get("tail_worker_shrink_applied", False)),
                    resume_tail_batch=bool(worker_plan.get("resume_tail_batch", False)),
                    state=current_state,
                    shutdown_started=bool(app._shutdown_started),
                    resume_progress_saved=os.path.exists(progress_path),
                    progress_path=progress_path,
                )
            except Exception:
                pass
            raise
        except RuntimeError as exc:
            stop_event.set()
            if "interpreter shutdown" in str(exc).lower():
                raise StopDownloadException("application is shutting down")
            raise
        except Exception as exc:
            stop_event.set()
            if _is_no_space_left_error(exc):
                free_bytes = app._get_disk_free_bytes(out_path)
                app._pause_task_for_disk_full(item_id, out_path, free_bytes, None, note=app._disk_full_pause_note())
                raise StopDownloadException("disk space low")
            try:
                task = app.tasks.get(item_id, task)
                existing_info = app._probe_media_info(out_path) if app._has_nonempty_file(out_path) else {}
                existing_duration = float(existing_info.get("duration", 0.0) or 0.0)
                existing_size = int(existing_info.get("size", 0) or 0)
                existing_valid = bool(existing_info.get("valid")) and existing_size > 0
                duration_matches = (
                    total_duration <= 300.0
                    or existing_duration <= 0.0
                    or existing_duration + max(60.0, total_duration * 0.02) >= total_duration
                )
                if (
                    existing_valid
                    and duration_matches
                    and not app._is_incomplete_hls_video_artifact(task, out_path, expected_duration=total_duration)
                ):
                    app._set_task_output_file(task, item_id, out_path)
                    app._set_task_named_column_text(item_id, "progress", "100%")
                    if app._mark_task_finished(item_id):
                        app._log_ffmpeg_event(
                            "parallel hls output already finalized",
                            Exception("parallel hls output already exists after remux race"),
                            task,
                            item_id,
                            media_url,
                            output=out_path,
                            bytes=existing_size,
                            segments=total_segments,
                            workers=worker_count,
                            hls_host=hls_host,
                            hls_host_active_downloads=hls_host_active_downloads,
                            hls_host_worker_budget=hls_host_worker_budget,
                            tail_worker_cap=int(worker_plan.get("tail_worker_cap", 0) or 0),
                            tail_worker_shrink_applied=bool(worker_plan.get("tail_worker_shrink_applied", False)),
                            resume_tail_batch=bool(worker_plan.get("resume_tail_batch", False)),
                            original_error=str(exc)[:240],
                        )
                        return True
            except Exception:
                pass
            if isinstance(exc, ResumeLowSpeedReanalysisException):
                write_error_log(
                    "parallel hls slow candidate retry next",
                    exc,
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    segments=total_segments,
                    workers=worker_count,
                    hls_host=hls_host,
                    fallback_count=len(_task_field_value(task, "fallback_urls", []) or []),
                    page_refresh_candidate_count=len(_task_field_value(task, "page_refresh_candidates", []) or []),
                )
                return False
            fallback_count = len(_task_field_value(task, "fallback_urls", []) or [])
            page_refresh_candidate_count = len(_task_field_value(task, "page_refresh_candidates", []) or [])
            if not has_google_segments and (fallback_count > 0 or page_refresh_candidate_count > 0):
                write_error_log(
                    "parallel hls candidate failed retry next",
                    exc,
                    url=media_url,
                    item_id=item_id,
                    source_site=_task_source_site_name(task) or None,
                    segments=total_segments,
                    workers=worker_count,
                    hls_host=hls_host,
                    fallback_count=fallback_count,
                    page_refresh_candidate_count=page_refresh_candidate_count,
                )
                return False
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
        finally:
            app._unregister_parallel_hls_stop_event(item_id, stop_event)
