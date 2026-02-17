#!/usr/bin/env python3
"""Native filesystem timestamp scanner.

This module is intended to complement the existing repo-centric working tree scan
and the ZFS snapshot scan by allowing a *filesystem-rooted* sweep.

Key goals:
- Be OS-aware: on Linux/Unix we primarily support ext2/3/4; on Windows we support NTFS.
- Avoid surprising scans: callers choose scan roots explicitly (or default to a safe base).
- Scale: use scandir-based traversal, coarse excludes, and optional heartbeats.

The emitted rows are compatible with the unified suite's normalized event schema.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


DEFAULT_NATIVE_FS_EXCLUDES = {
    # Common dependency/venv caches
    '.venv',
    'venv',
    'env',
    '__pypackages__',
    '.tox',
    '.nox',
    'node_modules',
    '.pnpm-store',
    '.yarn',
    '.cache',
    '.config',
    '.mypy_cache',
    '.pytest_cache',
    '.ruff_cache',
    '__pycache__',
    # VCS
    '.git',
    '.hg',
    '.ipfs',
    '.svn',
}


@dataclass(frozen=True)
class ScanRootInfo:
    scan_root: Path
    mountpoint: Optional[Path]
    filesystem_type: Optional[str]
    allowed: bool
    reason: Optional[str]


def _unescape_fstab_path(value: str) -> str:
    # /proc/mounts escapes spaces and tabs.
    return (
        (value or '')
        .replace('\\040', ' ')
        .replace('\\011', '\t')
        .replace('\\012', '\n')
        .replace('\\134', '\\')
    )


def _linux_mount_table() -> List[Tuple[Path, str]]:
    mounts: List[Tuple[Path, str]] = []
    try:
        with open('/proc/self/mounts', 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                parts = (line or '').split()
                if len(parts) < 3:
                    continue
                mountpoint = _unescape_fstab_path(parts[1])
                fstype = (parts[2] or '').strip()
                if not mountpoint:
                    continue
                mounts.append((Path(mountpoint), fstype))
    except Exception:
        return []

    # Sort by mountpoint length (most specific first) for prefix matching.
    mounts.sort(key=lambda t: len(str(t[0])), reverse=True)
    return mounts


def _linux_mount_for_path(path: Path) -> Tuple[Optional[Path], Optional[str]]:
    try:
        path_resolved = path.resolve()
    except Exception:
        path_resolved = path

    mounts = _linux_mount_table()
    if not mounts:
        return None, None

    for mp, fstype in mounts:
        try:
            # Use relative_to to ensure path boundary correctness.
            path_resolved.relative_to(mp)
            return mp, fstype
        except Exception:
            continue
    return None, None


def _windows_filesystem_type_for_path(path: Path) -> Optional[str]:
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:
        return None

    p = str(path)
    try:
        drive, _ = os.path.splitdrive(p)
    except Exception:
        drive = ''
    if not drive:
        return None
    root = drive.rstrip('\\/') + '\\'

    GetVolumeInformationW = ctypes.windll.kernel32.GetVolumeInformationW
    GetVolumeInformationW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
        wintypes.DWORD,
    ]
    GetVolumeInformationW.restype = wintypes.BOOL

    fs_name_buf = ctypes.create_unicode_buffer(256)
    vol_name_buf = ctypes.create_unicode_buffer(256)
    serial = wintypes.DWORD()
    max_comp_len = wintypes.DWORD()
    flags = wintypes.DWORD()

    ok = GetVolumeInformationW(
        root,
        vol_name_buf,
        len(vol_name_buf),
        ctypes.byref(serial),
        ctypes.byref(max_comp_len),
        ctypes.byref(flags),
        fs_name_buf,
        len(fs_name_buf),
    )
    if not ok:
        return None
    fs = (fs_name_buf.value or '').strip()
    return fs or None


def describe_scan_root(scan_root: Path, *, force: bool = False) -> ScanRootInfo:
    """Return filesystem details for a scan root and whether we should scan it."""
    scan_root = Path(scan_root)
    platform = sys.platform

    if platform.startswith('linux'):
        mountpoint, fstype = _linux_mount_for_path(scan_root)
        fstype_norm = (fstype or '').lower().strip() or None
        # We can safely traverse and stat() files on any local filesystem.
        # This gate exists primarily to avoid surprising scans on network/remote FS.
        # Linux support focus: ext2-4 and ZFS.
        allowed_types = {'ext2', 'ext3', 'ext4', 'zfs'}
        if force:
            return ScanRootInfo(scan_root, mountpoint, fstype_norm, True, None)
        if fstype_norm in allowed_types:
            return ScanRootInfo(scan_root, mountpoint, fstype_norm, True, None)
        reason = f"unsupported filesystem type on linux: {fstype_norm or 'unknown'}"
        return ScanRootInfo(scan_root, mountpoint, fstype_norm, False, reason)

    if platform.startswith('win'):
        fstype = _windows_filesystem_type_for_path(scan_root)
        fstype_norm = (fstype or '').upper().strip() or None
        if force:
            return ScanRootInfo(scan_root, None, fstype_norm, True, None)
        if fstype_norm == 'NTFS':
            return ScanRootInfo(scan_root, None, fstype_norm, True, None)
        reason = f"unsupported filesystem type on windows: {fstype_norm or 'unknown'}"
        return ScanRootInfo(scan_root, None, fstype_norm, False, reason)

    # Other platforms (macOS, etc.) are currently out of scope.
    if force:
        return ScanRootInfo(scan_root, None, None, True, None)
    return ScanRootInfo(scan_root, None, None, False, f"unsupported platform: {platform}")


def _should_skip_dir_name(name: str, exclude_set: set[str]) -> bool:
    if not name:
        return True
    return name in exclude_set


def iter_native_fs_file_events(
    *,
    scan_root: Path,
    user: str,
    excludes: Sequence[str],
    follow_symlinks: bool = False,
    max_files: int = 0,
    progress_every_seconds: float = 30.0,
    force: bool = False,
) -> Iterator[Dict]:
    """Yield filesystem mtime events under scan_root.

    This is a fast scandir-based traversal with coarse directory-name excludes.

    Args:
        scan_root: Directory to scan.
        user: Attribution user for events.
        excludes: Directory names to exclude.
        follow_symlinks: Whether to follow symlinked directories/files.
        max_files: If > 0, stop after emitting this many file events.
        progress_every_seconds: Heartbeat interval (0 disables).
    """

    scan_root = Path(scan_root)
    exclude_set = {str(x).strip() for x in (excludes or []) if str(x).strip()}

    info = describe_scan_root(scan_root, force=force)
    if not info.allowed:
        return

    u = (user or '').strip() or 'Unknown'
    emitted = 0
    last_heartbeat = time.time()

    # Use a manual stack for better control over errors and symlinks.
    stack: List[Path] = [scan_root]

    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    name = entry.name

                    try:
                        is_dir = entry.is_dir(follow_symlinks=follow_symlinks)
                    except OSError:
                        is_dir = False

                    if is_dir:
                        if _should_skip_dir_name(name, exclude_set):
                            continue
                        try:
                            p = Path(entry.path)
                        except Exception:
                            continue
                        stack.append(p)
                        continue

                    try:
                        is_file = entry.is_file(follow_symlinks=follow_symlinks)
                    except OSError:
                        is_file = False
                    if not is_file:
                        continue

                    try:
                        st = entry.stat(follow_symlinks=follow_symlinks)
                    except (FileNotFoundError, PermissionError, OSError):
                        continue

                    mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
                    try:
                        rel = str(Path(entry.path).resolve().relative_to(scan_root.resolve())).replace('\\', '/')
                    except Exception:
                        try:
                            rel = str(Path(entry.path).relative_to(scan_root)).replace('\\', '/')
                        except Exception:
                            rel = str(entry.path).replace('\\', '/')

                    yield {
                        'repository': str(scan_root),
                        'author': u,
                        'attributed_user': u,
                        'user': u,
                        'copilot_involved': False,
                        'invoker_source': 'native_fs',
                        'email': '',
                        'event_timestamp': mtime.isoformat(),
                        'status': 'FS',
                        'file': rel,
                        'commit': None,
                        'source': 'native_fs',
                        'scan_root': str(scan_root),
                        'mountpoint': str(info.mountpoint) if info.mountpoint else None,
                        'filesystem_type': info.filesystem_type,
                    }

                    emitted += 1
                    if max_files and emitted >= max_files:
                        return

                    if progress_every_seconds and progress_every_seconds > 0:
                        now = time.time()
                        if now - last_heartbeat >= float(progress_every_seconds):
                            print(f"[native-fs] root={scan_root} files={emitted}")
                            last_heartbeat = now
        except (PermissionError, FileNotFoundError, NotADirectoryError):
            continue


def collect_native_fs_events(
    *,
    scan_roots: Sequence[Path],
    user: str,
    excludes: Sequence[str],
    follow_symlinks: bool = False,
    max_files: int = 0,
    progress_every_seconds: float = 30.0,
    force: bool = False,
    row_sink: Optional[callable] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """Collect native filesystem mtime events for multiple roots.

    Returns:
        (rows, scan_summary)
    """

    rows: List[Dict] = []
    summary: List[Dict] = []

    for root in scan_roots or []:
        root_path = Path(root).expanduser().resolve()
        info = describe_scan_root(root_path, force=force)
        summary.append(
            {
                'event_timestamp': datetime.now(timezone.utc).isoformat(),
                'scan_root': str(root_path),
                'mountpoint': str(info.mountpoint) if info.mountpoint else None,
                'filesystem_type': info.filesystem_type,
                'allowed': bool(info.allowed),
                'reason': info.reason,
                'follow_symlinks': bool(follow_symlinks),
                'max_files': int(max_files or 0),
            }
        )

        if not info.allowed:
            continue

        # Only keep in memory if no sink is provided.
        for row in iter_native_fs_file_events(
            scan_root=root_path,
            user=user,
            excludes=excludes,
            follow_symlinks=follow_symlinks,
            max_files=max_files,
            progress_every_seconds=progress_every_seconds,
            force=force,
        ):
            if row_sink is not None:
                try:
                    row_sink(row)
                except Exception:
                    # Sink failures should not crash scanning; fallback to in-memory.
                    rows.append(row)
            else:
                rows.append(row)

    return rows, summary
