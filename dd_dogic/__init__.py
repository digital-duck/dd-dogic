"""dd-dogic: universal document converter pipeline (dog + logic).

Extract → Transform → Format, orchestrated by PocoFlow.

Usage:
    dd-dogic paper.pdf paper.md          # extract only
    dd-dogic notes.md report.pdf         # format only
    dd-dogic paper.pdf summary.html      # extract + format
    dd-dogic paper.pdf report.docx       # extract + format
"""

from dd_dogic.pipeline import convert, build_flow

__all__ = ["convert", "build_flow"]
__version__ = "0.1.0"
