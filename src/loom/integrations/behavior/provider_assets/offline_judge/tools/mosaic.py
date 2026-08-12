#!/usr/bin/env python3
# Copyright (c) 2023 Stanford Vision and Learning Group
# SPDX-License-Identifier: MIT
#
# Derived and modified from OmniGibson agentic_sweep/tools/mosaic.py. This Loom
# port removes ambient root discovery and accepts only signed Pipeline video roots.
"""Build one three-camera mosaic from one direct signed Pipeline video root."""

from __future__ import annotations

import argparse
import re
import stat
import subprocess
from pathlib import Path
from typing import Any

CAMS = (("head", "HEAD"), ("left_wrist", "LEFT WRIST"), ("right_wrist", "RIGHT WRIST"))
FPS = 30.0
TILE = 400
_DIRECT_ROOT = re.compile(
    r"^/inputs/(?:rollout|dataset)/payload/videos/task-(?:[0-9]{4}|[1-9][0-9]{4,9})$"
)
_EPISODE = re.compile(r"^episode_[0-9]{8}$")
_FONTS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
)


def _font(size: int) -> Any:
    from PIL import ImageFont  # type: ignore[import-not-found]

    for path in _FONTS:
        if Path(path).is_file():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _real_directory(path: Path, *, label: str) -> Path:
    if path.as_posix() != str(path) or not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be a canonical absolute path")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"{label} contains a symlink")
    if not path.is_dir():
        raise ValueError(f"{label} must be a directory")
    return path


def validate_video_root(raw: str) -> Path:
    if _DIRECT_ROOT.fullmatch(raw) is None:
        raise ValueError("--video-root is not one of the two signed direct-root shapes")
    root = _real_directory(Path(raw), label="--video-root")
    expected = {f"observation.images.rgb.{camera}" for camera, _ in CAMS}
    observed = {item.name for item in root.iterdir()}
    if observed != expected:
        raise ValueError("--video-root does not contain exactly the three camera directories")
    for child in root.iterdir():
        _real_directory(child, label="camera directory")
    return root


def validate_scratch_path(raw: str, *, label: str, directory: bool) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path.as_posix() != raw or ".." in path.parts:
        raise ValueError(f"{label} must be a canonical absolute path")
    try:
        path.relative_to("/scratch")
    except ValueError as exc:
        raise ValueError(f"{label} must be beneath /scratch") from exc
    if path == Path("/scratch") or (not directory and path.name in {"", ".", ".."}):
        raise ValueError(f"{label} must be a child of /scratch")
    current = Path("/scratch")
    for part in path.relative_to("/scratch").parts[:-1 if not directory else None]:
        current /= part
        if current.exists() and stat.S_ISLNK(current.lstat().st_mode):
            raise ValueError(f"{label} contains a symlink")
    if path.exists() and stat.S_ISLNK(path.lstat().st_mode):
        raise ValueError(f"{label} is a symlink")
    return path


def parse_frames(raw: str) -> list[int]:
    pieces = raw.split(",")
    if not pieces or any(not item.isascii() or not item.isdigit() for item in pieces):
        raise ValueError("--frames must be comma-separated canonical uints")
    frames = [int(item) for item in pieces]
    if any(str(value) != item for value, item in zip(frames, pieces, strict=True)):
        raise ValueError("--frames contains a noncanonical uint")
    if frames != sorted(set(frames)) or not 1 <= len(frames) <= 32:
        raise ValueError("--frames must contain 1..32 sorted unique values")
    return frames


def _episode_files(root: Path, episode: str) -> tuple[Path, Path, Path]:
    if _EPISODE.fullmatch(episode) is None:
        raise ValueError("--episode must use the exact episode_NNNNNNNN form")
    paths = (
        root / "observation.images.rgb.head" / f"{episode}.mp4",
        root / "observation.images.rgb.left_wrist" / f"{episode}.mp4",
        root / "observation.images.rgb.right_wrist" / f"{episode}.mp4",
    )
    for path in paths:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise ValueError("selected episode must be three regular MP4 files")
    return paths


def _frame_count(path: Path) -> int:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=nb_frames",
        "-of",
        "csv=p=0",
        str(path),
    ]
    value = subprocess.run(command, check=True, capture_output=True, text=True).stdout.strip()
    if not value.isdigit() or int(value) <= 0:
        fallback = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-count_frames",
            "-show_entries",
            "stream=nb_read_frames",
            "-of",
            "csv=p=0",
            str(path),
        ]
        value = subprocess.run(
            fallback, check=True, capture_output=True, text=True
        ).stdout.strip()
    if not value.isdigit() or int(value) <= 0:
        raise ValueError("ffprobe did not return a positive frame count")
    return int(value)


def _cached_frame(video: Path, frame: int, cache: Path) -> Path:
    camera = video.parent.name
    target = cache / f"{camera}__{video.stem}" / f"{frame:012d}.jpg"
    if target.is_file() and not stat.S_ISLNK(target.lstat().st_mode):
        return target
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    subprocess.run(
        [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            str(video),
            "-vf",
            f"select=eq(n\\,{frame})",
            "-frames:v",
            "1",
            "-q:v",
            "3",
            str(target),
        ],
        check=True,
    )
    if not target.is_file() or stat.S_ISLNK(target.lstat().st_mode):
        raise ValueError("ffmpeg did not create the expected private cache frame")
    return target


def build(video_paths: tuple[Path, Path, Path], frames: list[int], out: Path, cache: Path) -> None:
    from PIL import Image, ImageDraw

    font = _font(20)
    bar = 34
    label_width = 150
    canvas = Image.new("RGB", (label_width + len(frames) * TILE, 3 * (TILE + bar)), (18, 18, 18))
    draw = ImageDraw.Draw(canvas)
    for row, ((_, title), video) in enumerate(zip(CAMS, video_paths, strict=True)):
        y = row * (TILE + bar)
        draw.rectangle([0, y, canvas.width, y + bar - 1], fill=(0, 0, 0))
        draw.text((8, y + 5), title, fill=(120, 220, 255), font=font)
        for column, frame in enumerate(frames):
            with Image.open(_cached_frame(video, frame, cache)) as source:
                image = source.convert("RGB").resize((TILE, TILE), Image.Resampling.LANCZOS)
            x = label_width + column * TILE
            canvas.paste(image, (x, y + bar))
            draw.text((x + 6, y + 6), f"f{frame}", fill=(255, 255, 0), font=font)
            draw.rectangle([x, y + bar, x + TILE - 1, y + bar + TILE - 1], outline=(70, 70, 70))
    out.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    canvas.save(out, quality=88)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--video-root", required=True)
    parser.add_argument("--episode", required=True)
    parser.add_argument("--frames", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--cache-dir", required=True)
    args = parser.parse_args(argv)
    try:
        root = validate_video_root(args.video_root)
        out = validate_scratch_path(args.out, label="--out", directory=False)
        cache = validate_scratch_path(args.cache_dir, label="--cache-dir", directory=True)
        frames = parse_frames(args.frames)
        videos = _episode_files(root, args.episode)
        total = _frame_count(videos[0])
        outside = [frame for frame in frames if frame >= total]
        if outside:
            raise ValueError(f"frames outside 0..{total - 1}: {outside}")
        build(videos, frames, out, cache)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        parser.error(str(exc))
    print(f"wrote {out} episode={args.episode} 3 rows: HEAD / LEFT WRIST / RIGHT WRIST")
    print("COLUMN -> FRAME (authoritative; do not read frame numbers off the image):")
    for index, frame in enumerate(frames, 1):
        print(f"  col {index}: frame {frame} ({frame / FPS:.2f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
