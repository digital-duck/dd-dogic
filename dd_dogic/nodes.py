"""PocoFlow nodes for the dd-dogic document conversion pipeline.

Pipeline: Detect → Extract → Clean → Format

dd-dogic doesn't reinvent anything — it's a thin orchestration wrapper
around existing tools:
    pypdf   — fast PDF text extraction
    docling — layout-aware PDF extraction (IBM)
    pandoc  — universal document converter (DOCX, EPUB, RST, LaTeX, ...)

Each node follows the nano-ETL pattern:
    prep(store)   → read inputs from store
    exec(inputs)  → do the work (pure, no side-effects)
    post(store, prep_result, exec_result) → write results, return action
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from pocoflow import Node

# --- Format registry ---------------------------------------------------

# Formats dd-extract handles natively (PDF → markdown)
PDF_FORMATS = {".pdf"}

# Formats that are already text/markdown (just read them)
TEXT_FORMATS = {".md", ".txt", ".markdown"}

# Formats pandoc can convert FROM → markdown
PANDOC_SOURCE_FORMATS = {".docx", ".epub", ".rst", ".latex", ".tex", ".odt", ".rtf", ".html"}

# Formats pandoc can convert TO from markdown
PANDOC_TARGET_FORMATS = {".docx", ".epub", ".rst", ".latex", ".tex", ".odt", ".rtf"}

# Formats dd-format handles natively (markdown → output)
DD_FORMAT_TARGETS = {".pdf", ".html", ".docx"}

# Everything we support
ALL_SOURCE_FORMATS = PDF_FORMATS | TEXT_FORMATS | PANDOC_SOURCE_FORMATS
ALL_TARGET_FORMATS = DD_FORMAT_TARGETS | TEXT_FORMATS | PANDOC_TARGET_FORMATS


def _has_pandoc() -> bool:
    return shutil.which("pandoc") is not None


class DetectNode(Node):
    """Detect input/output formats and decide the pipeline route.

    Routes:
        "extract"     → PDF source, use dd-extract (pypdf/docling)
        "pandoc"      → use pandoc to convert to markdown
        "passthrough"  → source is already text/markdown
    """

    def prep(self, store: Any) -> dict:
        return {
            "source": store["source"],
            "target": store["target"],
        }

    def exec(self, prep_result: Any) -> dict:
        source = Path(prep_result["source"])
        target = Path(prep_result["target"])

        src_ext = source.suffix.lower()
        tgt_ext = target.suffix.lower()

        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source}")

        # Determine extraction route
        if src_ext in TEXT_FORMATS:
            route = "passthrough"
        elif src_ext in PDF_FORMATS:
            route = "extract"
        elif src_ext in PANDOC_SOURCE_FORMATS:
            if not _has_pandoc():
                raise RuntimeError(
                    f"Format {src_ext} requires pandoc but it's not installed. "
                    "Install from: https://pandoc.org/installing.html"
                )
            route = "pandoc"
        else:
            raise ValueError(
                f"Unsupported source format: {src_ext}  "
                f"(supported: {sorted(ALL_SOURCE_FORMATS)})"
            )

        # Validate target format
        if tgt_ext not in ALL_TARGET_FORMATS and tgt_ext not in TEXT_FORMATS:
            raise ValueError(
                f"Unsupported target format: {tgt_ext}  "
                f"(supported: {sorted(ALL_TARGET_FORMATS | TEXT_FORMATS)})"
            )

        return {"src_ext": src_ext, "tgt_ext": tgt_ext, "route": route}

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["src_ext"] = exec_result["src_ext"]
        store["tgt_ext"] = exec_result["tgt_ext"]
        return exec_result["route"]


class ExtractNode(Node):
    """Extract markdown from PDF using dd-extract (pypdf or docling)."""

    def prep(self, store: Any) -> dict:
        return {
            "source": store["source"],
            "engine": store.get("engine", "pypdf"),
            "max_chars": store.get("max_chars", 0),
        }

    def exec(self, prep_result: Any) -> str:
        from dd_extract import PDFExtractor

        kwargs = {"engine": prep_result["engine"]}
        if prep_result["max_chars"] > 0:
            kwargs["max_chars"] = prep_result["max_chars"]

        extractor = PDFExtractor(**kwargs)
        return extractor.from_file(prep_result["source"])

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["markdown"] = exec_result
        return "default"


class PandocExtractNode(Node):
    """Extract markdown from DOCX/EPUB/RST/etc using pandoc."""

    def prep(self, store: Any) -> str:
        return store["source"]

    def exec(self, prep_result: Any) -> str:
        result = subprocess.run(
            ["pandoc", prep_result, "-t", "markdown", "--wrap=none"],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pandoc failed: {result.stderr.strip()}")
        return result.stdout

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["markdown"] = exec_result
        return "default"


class PassthroughNode(Node):
    """Read a text/markdown file as-is."""

    def prep(self, store: Any) -> str:
        return store["source"]

    def exec(self, prep_result: Any) -> str:
        return Path(prep_result).read_text(encoding="utf-8")

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["markdown"] = exec_result
        return "default"


class CleanNode(Node):
    """Clean and normalize extracted markdown.

    Fixes common extraction artifacts:
    - Excessive blank lines
    - Broken hyphenated line breaks
    - Trailing whitespace
    """

    def prep(self, store: Any) -> str:
        return store["markdown"]

    def exec(self, prep_result: Any) -> str:
        text = prep_result
        text = re.sub(r"-\s*\n\s*", "-", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = "\n".join(line.rstrip() for line in text.split("\n"))
        return text.strip()

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["markdown"] = exec_result
        return "default"


class FormatNode(Node):
    """Convert markdown to target format.

    Uses dd-format for PDF/HTML/DOCX, pandoc for everything else.
    """

    def prep(self, store: Any) -> dict:
        return {
            "markdown": store["markdown"],
            "target": store["target"],
            "tgt_ext": store["tgt_ext"],
            "title": store.get("title", ""),
        }

    def exec(self, prep_result: Any) -> str:
        md = prep_result["markdown"]
        target = prep_result["target"]
        tgt_ext = prep_result["tgt_ext"]
        title = prep_result["title"]

        if tgt_ext in TEXT_FORMATS:
            Path(target).write_text(md, encoding="utf-8")

        elif tgt_ext == ".html":
            from dd_format import markdown_to_html
            markdown_to_html(md, target, title=title)

        elif tgt_ext == ".pdf":
            from dd_format import markdown_to_pdf
            markdown_to_pdf(md, target, title=title)

        elif tgt_ext == ".docx":
            from dd_format import markdown_to_docx
            markdown_to_docx(md, target, title=title)

        elif tgt_ext in PANDOC_TARGET_FORMATS:
            # Let pandoc handle the rest (epub, rst, latex, odt, rtf)
            result = subprocess.run(
                ["pandoc", "-f", "markdown", "-o", target,
                 *(["--metadata", f"title={title}"] if title else [])],
                input=md, capture_output=True, text=True, timeout=120,
            )
            if result.returncode != 0:
                raise RuntimeError(f"pandoc failed: {result.stderr.strip()}")

        else:
            raise ValueError(f"Unsupported target format: {tgt_ext}")

        return target

    def post(self, store: Any, prep_result: Any, exec_result: Any) -> str:
        store["output_path"] = exec_result
        return "default"
