"""Tests for dd-dogic pipeline."""

import os
import tempfile
from pathlib import Path

import pytest

from dd_dogic.nodes import (
    DetectNode,
    CleanNode,
    ALL_SOURCE_FORMATS,
    ALL_TARGET_FORMATS,
    _has_pandoc,
)
from dd_dogic.pipeline import convert


# --- Unit tests for individual nodes ---


def test_clean_node_hyphenation():
    from pocoflow import Store
    node = CleanNode()
    store = Store(data={"markdown": "docu-\n  ment"})
    node._run(store)
    assert store["markdown"] == "docu-ment"


def test_clean_node_blank_lines():
    from pocoflow import Store
    node = CleanNode()
    store = Store(data={"markdown": "a\n\n\n\n\nb"})
    node._run(store)
    assert store["markdown"] == "a\n\nb"


def test_detect_node_missing_file():
    from pocoflow import Store
    node = DetectNode()
    store = Store(data={"source": "/nonexistent/file.pdf", "target": "out.md"})
    with pytest.raises(FileNotFoundError):
        node._run(store)


def test_detect_node_unsupported_source():
    from pocoflow import Store
    fd, path = tempfile.mkstemp(suffix=".xyz")
    os.close(fd)
    try:
        node = DetectNode()
        store = Store(data={"source": path, "target": "out.md"})
        with pytest.raises(ValueError, match="Unsupported source"):
            node._run(store)
    finally:
        Path(path).unlink(missing_ok=True)


# --- Integration: md → html (passthrough + format, no external tools) ---


def test_md_to_html():
    src_fd, src_path = tempfile.mkstemp(suffix=".md")
    os.close(src_fd)
    tgt_fd, tgt_path = tempfile.mkstemp(suffix=".html")
    os.close(tgt_fd)
    try:
        Path(src_path).write_text("# Hello\n\nWorld", encoding="utf-8")
        output = convert(src_path, tgt_path)
        assert Path(output).exists()
        content = Path(output).read_text()
        assert "<h1>" in content
    finally:
        Path(src_path).unlink(missing_ok=True)
        Path(tgt_path).unlink(missing_ok=True)


def test_md_to_pdf():
    src_fd, src_path = tempfile.mkstemp(suffix=".md")
    os.close(src_fd)
    tgt_fd, tgt_path = tempfile.mkstemp(suffix=".pdf")
    os.close(tgt_fd)
    try:
        Path(src_path).write_text("# Test\n\nContent here.", encoding="utf-8")
        output = convert(src_path, tgt_path)
        assert Path(output).exists()
        assert Path(output).stat().st_size > 0
    finally:
        Path(src_path).unlink(missing_ok=True)
        Path(tgt_path).unlink(missing_ok=True)


def test_md_to_md():
    """Passthrough + clean + write as text."""
    src_fd, src_path = tempfile.mkstemp(suffix=".md")
    os.close(src_fd)
    tgt_fd, tgt_path = tempfile.mkstemp(suffix=".md")
    os.close(tgt_fd)
    try:
        Path(src_path).write_text("hello\n\n\n\n\nworld", encoding="utf-8")
        output = convert(src_path, tgt_path)
        result = Path(output).read_text()
        assert result == "hello\n\nworld"
    finally:
        Path(src_path).unlink(missing_ok=True)
        Path(tgt_path).unlink(missing_ok=True)


def test_format_registry():
    """Sanity check: all format sets are non-empty."""
    assert len(ALL_SOURCE_FORMATS) > 3
    assert len(ALL_TARGET_FORMATS) > 3
