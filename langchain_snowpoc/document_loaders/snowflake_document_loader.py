import io
import logging
import os
import re
from typing import Iterator

from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document
from langchain_core.documents.base import Blob
from pypdf import PdfReader
from snowflake.snowpark.session import Session

from langchain_snowpoc.documents import SnowBlob, SnowBlobLoader

logger = logging.getLogger(__name__)


class BaseSnowDocumentLoader(BaseLoader):
    """An example document loader that reads a file line by line."""

    def __init__(self, blob: Blob) -> None:
        """Initialize the loader with a file path.

        Args:
            blob: blob to load
        """
        self.blob = blob

    def lazy_load(self) -> Iterator[Document]:
        raise Exception("Not implemented yet")


class SnowPDFPageDocumentLoader(BaseSnowDocumentLoader):
    def lazy_load(self) -> Iterator[Document]:
        """A lazy loader that returns a pdf file page by page."""

        with self.blob.as_bytes_io() as f:
            reader = PdfReader(io.BytesIO(f.read()))

            for i in range(len(reader.pages)):
                page = reader.pages[i]
                text = re.sub(
                    "([ ]{2,})",
                    " ",
                    page.extract_text(extraction_mode="layout").replace("\0", " "),
                )
                yield Document(
                    page_content=text,
                    metadata={"page": i, "source": self.blob.metadata},
                )


class SnowPDFDocumentLoader(BaseSnowDocumentLoader):
    def lazy_load(self) -> Iterator[Document]:
        """A lazy loader that reads a pdf file page by page
        and returns all pages as one Document.

        """

        with self.blob.as_bytes_io() as f:
            reader = PdfReader(io.BytesIO(f.read()))
            text = ""
            for page in reader.pages:
                text = text + re.sub(
                    "([ ]{2,})",
                    " ",
                    page.extract_text(extraction_mode="layout").replace("\0", " "),
                )
            yield Document(
                page_content=text,
                metadata={
                    "number_of_pdf_pages": len(reader.pages),
                    "source": self.blob.metadata,
                },
            )


if __name__ == "__main__":

    session = Session.builder.config(
        "connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME")
    ).create()

    print("=" * 50)
    print("==    " + "Single pdf")
    blob = SnowBlob(
        path="@CORTEX_DB.PUBLIC.documents/descriptions/bikes/Mondracer_Infant_Bike.pdf",
        session=session,
    )
    print(blob)

    print("=" * 50)
    print("==    " + "Multiple pdfs")
    for f in SnowPDFDocumentLoader(blob).lazy_load():
        print(str(f)[:40])

    print("=" * 50)
    print("==    " + "Multiple documents")
    sbl = SnowBlobLoader("@CORTEX_DB.PUBLIC.documents", session=session)

    for blob in sbl.yield_blobs():
        for page in SnowPDFDocumentLoader(blob).lazy_load():
            print(page.metadata)
            print(page.page_content[:40])
            print()

    print("=" * 50)
    print("==    " + "Paged documents")

    for blob in sbl.yield_blobs():
        for page in SnowPDFPageDocumentLoader(blob).lazy_load():
            print(page.metadata)
            print(page.page_content[:40])
            print()
