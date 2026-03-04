"""Pipeline builder: wire PocoFlow nodes into a document conversion flow.

The pipeline graph:

    DetectNode ──extract────→ ExtractNode ──────→ CleanNode → FormatNode
               ├─pandoc─────→ PandocExtractNode ─→ CleanNode
               └─passthrough→ PassthroughNode ──→ CleanNode

dd-dogic is a thin wrapper — detection picks the best tool
(pypdf, docling, pandoc), the rest is just plumbing.
"""

from __future__ import annotations

from pathlib import Path

from pocoflow import Flow, Store

from dd_dogic.nodes import (
    CleanNode,
    DetectNode,
    ExtractNode,
    FormatNode,
    PandocExtractNode,
    PassthroughNode,
)


def build_flow(checkpoint_dir: str | None = None) -> tuple[Flow, dict[str, object]]:
    """Build and return the dd-dogic conversion flow."""
    detect = DetectNode()
    extract = ExtractNode()
    pandoc_extract = PandocExtractNode()
    passthrough = PassthroughNode()
    clean = CleanNode()
    fmt = FormatNode()

    detect.then("extract", extract)
    detect.then("pandoc", pandoc_extract)
    detect.then("passthrough", passthrough)
    extract.then("default", clean)
    pandoc_extract.then("default", clean)
    passthrough.then("default", clean)
    clean.then("default", fmt)

    flow = Flow(
        start=detect,
        flow_name="dd-dogic",
        checkpoint_dir=checkpoint_dir,
        max_steps=10,
    )

    nodes = {
        "detect": detect,
        "extract": extract,
        "pandoc_extract": pandoc_extract,
        "passthrough": passthrough,
        "clean": clean,
        "format": fmt,
    }
    return flow, nodes


def convert(
    source: str,
    target: str,
    engine: str = "pypdf",
    max_chars: int = 0,
    title: str = "",
    clean: bool = True,
) -> str:
    """Convert a document from one format to another.

    Parameters
    ----------
    source : str
        Path to the input file.
    target : str
        Path for the output file.
    engine : str
        PDF extraction engine: 'pypdf' or 'docling'.
    max_chars : int
        Truncate extracted text to N chars (0 = no limit).
    title : str
        Document title for formatted output.
    clean : bool
        Whether to run the cleanup step (default True).

    Returns
    -------
    str
        The output file path.
    """
    flow, nodes = build_flow()

    if not clean:
        fmt_node = nodes["format"]
        nodes["extract"].then("default", fmt_node)
        nodes["pandoc_extract"].then("default", fmt_node)
        nodes["passthrough"].then("default", fmt_node)

    store = Store(
        data={
            "source": str(Path(source).resolve()),
            "target": str(Path(target).resolve()),
            "engine": engine,
            "max_chars": max_chars,
            "title": title or Path(source).stem,
        },
        name="dd-dogic",
    )

    result = flow.run(store)
    return result["output_path"]
