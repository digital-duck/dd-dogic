"""dd-dogic CLI: universal document converter.

Usage like ImageMagick convert:
    dd-dogic input.pdf output.md
    dd-dogic input.pdf output.html
    dd-dogic notes.md report.pdf
    dd-dogic paper.pdf report.docx --engine docling
"""

from __future__ import annotations

import click

from dd_dogic.pipeline import convert


@click.command()
@click.argument("source", type=click.Path(exists=True))
@click.argument("target", type=click.Path())
@click.option(
    "--engine",
    type=click.Choice(["pypdf", "docling"]),
    default="pypdf",
    show_default=True,
    help="PDF extraction engine.",
)
@click.option(
    "--max-chars",
    default=0,
    type=int,
    show_default=True,
    help="Truncate extracted text to N chars (0 = unlimited).",
)
@click.option(
    "--title",
    default="",
    help="Document title for formatted output.",
)
@click.option(
    "--no-clean",
    is_flag=True,
    default=False,
    help="Skip the markdown cleanup step.",
)
def main(source: str, target: str, engine: str, max_chars: int, title: str, no_clean: bool) -> None:
    """Convert a document from one format to another.

    \b
    Supported conversions:
      PDF  -> md, html, pdf, docx      (extract + format)
      MD   -> html, pdf, docx          (format only)
      TXT  -> html, pdf, docx          (format only)
      DOCX -> md, html, pdf            (pandoc + format)

    \b
    Examples:
      dd-dogic paper.pdf paper.md
      dd-dogic paper.pdf digest.html
      dd-dogic paper.pdf report.docx --engine docling
      dd-dogic notes.md notes.pdf --title "My Notes"
    """
    click.echo(f"  {source} → {target}")

    output = convert(
        source=source,
        target=target,
        engine=engine,
        max_chars=max_chars,
        title=title,
        clean=not no_clean,
    )

    click.echo(f"  Done: {output}")


if __name__ == "__main__":
    main()
