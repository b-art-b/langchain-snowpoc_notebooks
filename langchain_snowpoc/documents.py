from __future__ import annotations

import contextlib
import logging
import mimetypes
import os
import re
from io import BufferedReader, BytesIO
from pathlib import PurePath
from typing import Generator, Iterable, Optional, Union

from langchain_community.document_loaders.blob_loaders.schema import Blob, BlobLoader
from langchain_core.documents.base import Blob
from snowflake.snowpark.session import Session

PathLike = Union[str, PurePath]

logger = logging.getLogger(__name__)


class SnowBlob(Blob):
    """Blob represents raw data by either reference or value.

    Provides an interface to materialize the blob in different representations, and
    help to decouple the development of data loaders from the downstream parsing of
    the raw data.

    Inspired by: https://developer.mozilla.org/en-US/docs/Web/API/Blob

    Example: Initialize a blob from in-memory data

        .. code-block:: python

            from langchain_snowpoc.documents import SnowBlob

            blob = SnowBlob.from_data(b"Hello, world!")

            # Read the blob as a string
            print(blob.as_string())

            # Read the blob as bytes
            print(blob.as_bytes())

            # Read the blob as a byte stream
            with blob.as_bytes_io() as f:
                print(f.read())

    Example: Load from memory and specify mime-type and metadata

        .. code-block:: python

            from langchain_snowpoc.documents import SnowBlob

            blob = SnowBlob.from_data(
                data=b"Hello, world!",
                mime_type="text/plain",
                metadata={"source": "https://example.com"}
            )

    Example: Load the blob from a file

        .. code-block:: python

            import os
            from langchain_snowpoc.documents import SnowBlob
            from snowflake.snowpark.session import Session

            session = Session.builder.config(
                "connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME")
            ).create()

            blob = SnowBlob.from_path(
                "@CORTEX_DB.PUBLIC.TST/README.md",
                session=session,
            )

            # Read the blob as a string
            print(blob.as_string())

            # Read the blob as bytes
            print(blob.as_bytes())

            # Read the blob as a byte stream
            with blob.as_bytes_io() as f:
                print(f.read())
    """

    session: Union[Session, None]
    """Snowflake session to use"""

    def as_string(self) -> str:
        """Read data as a string."""
        if self.data is None and self.path and self.session:
            with self.session.file.get_stream(self.path, decompress=False) as f:
                return f.read().decode(self.encoding)
        elif isinstance(self.data, bytes):
            return self.data.decode(self.encoding)
        elif isinstance(self.data, str):
            return self.data
        else:
            raise ValueError(f"Unable to get string for blob {self}")

    def as_bytes(self) -> bytes:
        """Read data as bytes."""
        if isinstance(self.data, bytes):
            return self.data
        elif isinstance(self.data, str):
            return self.data.encode(self.encoding)
        elif self.data is None and self.path and self.session:
            with self.session.file.get_stream(self.path, decompress=False) as f:
                return f.read()
        else:
            raise ValueError(f"Unable to get bytes for blob {self}")

    @contextlib.contextmanager
    def as_bytes_io(self) -> Generator[Union[BytesIO, BufferedReader], None, None]:
        """Read data as a byte stream."""
        if isinstance(self.data, bytes):
            yield BytesIO(self.data)
        elif self.data is None and self.path and self.session:
            with self.session.file.get_stream(self.path, decompress=False) as f:
                yield f
        else:
            raise NotImplementedError(f"Unable to convert blob {self}")

    @classmethod
    def from_path(
        cls,
        path: PathLike,
        *,
        encoding: str = "utf-8",
        mime_type: Optional[str] = None,
        guess_type: bool = True,
        metadata: Optional[dict] = None,
        session: Session = None,
    ) -> Blob:
        """Load the blob from a path like object.

        Args:
            path: path like object to file to be read
            encoding: Encoding to use if decoding the bytes into a string
            mime_type: if provided, will be set as the mime-type of the data
            guess_type: If True, the mimetype will be guessed from the file extension,
                        if a mime-type was not provided
            metadata: Metadata to associate with the blob

        Returns:
            Blob instance
        """
        if mime_type is None and guess_type:
            _mimetype = mimetypes.guess_type(path)[0] if guess_type else None
        else:
            _mimetype = mime_type
        # We do not load the data immediately, instead we treat the blob as a
        # reference to the underlying data.
        return cls(
            data=None,
            mimetype=_mimetype,
            encoding=encoding,
            path=path,
            metadata=metadata if metadata is not None else {},
            session=session,
        )

    @classmethod
    def from_data(
        cls,
        data: Union[str, bytes],
        *,
        encoding: str = "utf-8",
        mime_type: Optional[str] = None,
        path: Optional[str] = None,
        metadata: Optional[dict] = None,
        session: Session = None,
    ) -> Blob:
        """Initialize the blob from in-memory data.

        Args:
            data: the in-memory data associated with the blob
            encoding: Encoding to use if decoding the bytes into a string
            mime_type: if provided, will be set as the mime-type of the data
            path: if provided, will be set as the source from which the data came
            metadata: Metadata to associate with the blob

        Returns:
            Blob instance
        """
        return cls(
            data=data,
            mimetype=mime_type,
            encoding=encoding,
            path=path,
            metadata=metadata if metadata is not None else {},
            session=session,
        )


class SnowBlobLoader(BlobLoader):
    def __init__(
        self,
        url: Union[str, "AnyPath"],
        *,
        pattern: str = ".*",
        session: Session = None,
    ) -> None:

        self.pattern = pattern
        self.url = url
        self.session = session
        self.stage = self.url

    @property
    def stage(self):
        return self._stage

    @stage.setter
    def stage(self, value):
        stage = re.search(r"([@~%][\w]+[\w.]+)", value, flags=re.IGNORECASE)
        if not stage:
            raise Exception("Did not find correct pattern for stage name")
        self._stage = stage[0]

    @stage.deleter
    def stage(self):
        del self._stage

    def _get_files(self):
        return self.session.sql(f"LIST {self.url} PATTERN='{self.pattern}'").collect()

    def yield_blobs(
        self,
    ) -> Iterable[Blob]:
        """Yield blobs that match the requested pattern."""

        for f in iter(self._get_files()):
            # Row(name='tst/README.md', size=1888, md5='4e26b7ea3bff13ad9306e5d0e8cfd903', last_modified='Fri, 23 Aug 2024 20:33:38 GMT')
            full_file_path = (
                self.stage + "/" + "/".join(f.name.split("/")[1:])
            )  # name contains also stage name

            yield SnowBlob.from_path(
                full_file_path,
                metadata={
                    "name": f.name,
                    "size": f.size,
                    "md5": f.md5,
                    "last_modified": f.last_modified,
                },
                session=self.session,
            )


class SnowBlobDeltaLoader(BlobLoader):
    """Loads blobs from a stream/directory table"""

    def __init__(
        self,
        stream_name: str,  # name of a stream table
        stage_name: str,
        *,
        pattern: str = "",
        session: Session = None,
    ) -> None:

        self.pattern = pattern
        self.table_name = stream_name
        self.session = session
        self.stage = stage_name

    def _get_files(self):
        return self.session.sql(
            f"SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED,"
            " MD5, ETAG, FILE_URL,"
            " METADATA$ACTION as METADATA_ACTION,"
            " METADATA$ISUPDATE as METADATA_ISUPDATE,"
            " METADATA$ROW_ID as METADATA_ROW_ID,"
            f" FROM {self.table_name}"
            f" WHERE RELATIVE_PATH LIKE '%{self.pattern}%'"
        ).collect()

    def yield_blobs(
        self,
    ) -> Iterable[Blob]:
        """Yield blobs that match the requested pattern.

        ```
        Row(
            RELATIVE_PATH='descriptions/bikes/Mondracer_Infant_Bike.pdf',
            SIZE=30551,
            LAST_MODIFIED=datetime.datetime(2024, 9, 2, 13, 12, 11, tzinfo=pytz.FixedOffset(-420)),
            MD5='48b3bd292756253e446e4692844f137f',
            ETAG='48b3bd292756253e446e4692844f137f',
            FILE_URL='https://xxx.snowflakecomputing.com/xxx/2fMondracer_Infant_Bike%2epdf',
            METADATA_ACTION='INSERT',
            METADATA_ISUPDATE=False,
            METADATA_ROW_ID=''
        )
        ```

        """

        for f in iter(self._get_files()):
            full_file_path = "@" + os.path.join(self.stage, f.RELATIVE_PATH)
            logger.debug(full_file_path)

            yield SnowBlob.from_path(
                full_file_path,
                metadata={
                    "name": f.RELATIVE_PATH,
                    "size": f.SIZE,
                    "md5": f.MD5,
                    "last_modified": f.LAST_MODIFIED.strftime("%Y/%m/%d %H:%M:%S"),
                },
                session=self.session,
            )


if __name__ == "__main__":

    session = Session.builder.config(
        "connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME")
    ).create()

    blob = SnowBlob.from_path("@CORTEX_DB.PUBLIC.TST/README.md", session=session)
    print(blob)
    print(blob.as_string()[:20])

    blob = SnowBlob(path="@CORTEX_DB.PUBLIC.TST/README.md", session=session)
    print(blob)
    print(blob.as_string()[:20])

    print("# Read the blob as bytes")
    print(blob.as_bytes()[:20])

    print("# Read the blob as a byte stream")
    with blob.as_bytes_io() as f:
        print(f.read()[:20])

    blob = SnowBlob.from_data(data=b"Elo!")
    print(blob)
    print(blob.as_string()[:20])

    loader = SnowBlobLoader("@CORTEX_DB.PUBLIC.TST", session=session, pattern=".*py")

    for b in loader.yield_blobs():
        print(b.as_string()[:50])

    loader = SnowBlobDeltaLoader(
        stream_name="TST_STREAM",
        stage_name="TST",
        session=session,
        pattern="",
    )
    for b in loader.yield_blobs():
        print(b.as_string()[:50])
