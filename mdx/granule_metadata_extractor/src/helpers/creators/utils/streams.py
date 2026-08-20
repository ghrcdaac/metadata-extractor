"""Helper utils for wrapping file inputs in useful streams."""
import io
from os import PathLike

def as_seekable_hdf5_stream(source):
    """
    Return a seekable binary stream suitable for h5py.

    Accepted inputs:
      - S3/botocore StreamingBody
      - local binary file object
      - bytes / bytearray
      - local filename or pathlib.Path
    """
    if isinstance(source, (str, PathLike)):
        with open(source, "rb") as local_file:
            return io.BytesIO(local_file.read())

    if isinstance(source, (bytes, bytearray)):
        return io.BytesIO(source)

    if hasattr(source, "read"):
        return io.BytesIO(source.read())

    raise TypeError(
        "Expected a path, bytes, or binary file-like object; "
        f"got {type(source).__name__}"
    )

import gzip
import io
from os import PathLike


def as_text_stream(source, gzipped=False, encoding="utf-8"):
    """
    Return a text stream suitable for parsing LMA files.

    Accepted inputs:
      - S3/botocore StreamingBody
      - local binary file object
      - local text file object
      - bytes / bytearray
      - local filename or pathlib.Path
    """

    if isinstance(source, (str, PathLike)):
        if gzipped:
            return gzip.open(source, mode="rt", encoding=encoding)
        return open(source, mode="rt", encoding=encoding)

    if isinstance(source, (bytes, bytearray)):
        binary_stream = io.BytesIO(source)
    elif isinstance(source, io.TextIOBase):
        return source
    elif hasattr(source, "read"):
        binary_stream = source
    else:
        raise TypeError(
            "Expected a path, bytes, or file-like object; "
            f"got {type(source).__name__}"
        )

    if gzipped:
        binary_stream = gzip.GzipFile(
            fileobj=binary_stream,
            mode="rb",
        )

    return io.TextIOWrapper(
        binary_stream,
        encoding=encoding,
    )