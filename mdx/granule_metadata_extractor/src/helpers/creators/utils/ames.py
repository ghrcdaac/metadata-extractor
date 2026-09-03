from __future__ import annotations

from io import TextIOBase, TextIOWrapper
from os import PathLike
import re
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Iterator, BinaryIO, TextIO, TypeAlias, Literal

AmesSource: TypeAlias = str | PathLike[str] | BinaryIO | TextIO

@dataclass(frozen=True)
class Ames1001Header:
    header_lines: int
    ffi: int
    originator: str
    organization: str
    source: str
    mission: str
    volume: int
    total_volumes: int
    data_date: date
    revision_date: date
    independent_interval: float
    independent_name: str
    scales: tuple[float, ...]
    missing_values: tuple[float, ...]
    variable_names: tuple[str, ...]
    special_comments: tuple[str, ...]
    normal_comments: tuple[str, ...]


@dataclass(frozen=True)
class Ames1001Record:
    # The independent variable, UT seconds in this file.
    independent: float

    # Primary values exactly as stored in the file.
    raw_values: tuple[float, ...]

    # Primary values after VSCAL is applied.
    # Missing values are represented by None.
    values: tuple[float | None, ...]

    annotation: str | None = None


class _LineReader:
    """Track the physical line number while reading."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.line_number = 0

    def read_line(self, description: str) -> str:
        line = self.stream.readline()
        # Strip comments
        line = re.sub(r";.*", "", line)

        if line == "":
            raise ValueError(
                f"Unexpected EOF while reading {description} "
                f"after physical line {self.line_number}"
            )

        self.line_number += 1
        return line.rstrip("\r\n")

    def remaining_lines(self) -> Iterator[str]:
        for line in self.stream:
            self.line_number += 1
            yield line.rstrip("\r\n")

def _split_fields(line: str, file_format: str) -> list[str]:
    if file_format == "Ames":
        return line.split()

    if file_format == "ICARTT":
        return [field.strip() for field in line.split(",")]

    raise ValueError(f"Unsupported FFI 1001 format: {file_format!r}")

def _parse_line(
    line: str,
    expected_count: int,
    converter,
    description: str,
    *,
    file_format='Ames'
):
    fields = _split_fields(line, file_format)

    if len(fields) != expected_count:
        raise ValueError(
            f"Expected {expected_count} value(s) for {description}, "
            f"got {len(fields)}: {line!r}"
        )

    try:
        return tuple(converter(field) for field in fields)
    except ValueError as exc:
        raise ValueError(
            f"Invalid value for {description}: {line!r}"
        ) from exc


def _read_numeric_block(
    reader: _LineReader,
    expected_count: int,
    description: str,
    *,
    file_format: str = 'Ames'
) -> tuple[float, ...]:
    """
    Read a header array such as VSCAL or VMISS.

    These arrays may also wrap across multiple physical lines.
    """

    values: list[float] = []

    while len(values) < expected_count:
        line = reader.read_line(description)
        fields = _split_fields(line, file_format)
        try:
            values.extend(float(field) for field in fields)
        except ValueError as exc:
            raise ValueError(
                f"Invalid number in {description} at physical line "
                f"{reader.line_number}: {line!r}"
            ) from exc

    if len(values) != expected_count:
        raise ValueError(
            f"Expected {expected_count} values for {description}, "
            f"got {len(values)}"
        )

    return tuple(values)

def _read_variable_name(reader, index, file_format):
    line = reader.read_line(f"primary-variable name {index + 1}")

    if file_format == "ICARTT":
        return line.split(",", maxsplit=1)[0].strip()

    return line

def _read_ames_1001_header(reader: _LineReader, file_format='Ames') -> Ames1001Header:
    header_lines, ffi = _parse_line(
        reader.read_line("NLHEAD and FFI"),
        2,
        int,
        "NLHEAD and FFI",
        file_format=file_format,
    )

    if ffi != 1001:
        raise ValueError(f"Expected NASA Ames FFI 1001, found FFI {ffi}")

    originator = reader.read_line("originator")
    organization = reader.read_line("organization")
    source = reader.read_line("source description")
    mission = reader.read_line("mission description")

    volume, total_volumes = _parse_line(
        reader.read_line("volume numbers"),
        2,
        int,
        "IVOL and NVOL",
        file_format=file_format
    )

    date_parts = _parse_line(
        reader.read_line("data and revision dates"),
        6,
        int,
        "DATE and RDATE",
        file_format=file_format
    )

    data_date = date(*date_parts[:3])
    revision_date = date(*date_parts[3:])

    (independent_interval,) = _parse_line(
        reader.read_line("independent-variable interval"),
        1,
        float,
        "DX",
        file_format=file_format
    )

    independent_name = reader.read_line(
        "independent-variable name"
    )

    (variable_count,) = _parse_line(
        reader.read_line("primary-variable count"),
        1,
        int,
        "NV",
        file_format=file_format
    )

    # VSCAL and VMISS each contain NV values.
    scales = _read_numeric_block(
        reader,
        variable_count,
        "VSCAL",
        file_format=file_format
    )

    missing_values = _read_numeric_block(
        reader,
        variable_count,
        "VMISS",
        file_format=file_format
    )

    # The next NV physical lines describe the primary variables.
    variable_names = tuple(
        _read_variable_name(reader, index, file_format)
        for index in range(variable_count)
    )

    (special_comment_count,) = _parse_line(
        reader.read_line("special-comment count"),
        1,
        int,
        "NSCOML",
        file_format=file_format
    )

    special_comments = tuple(
        reader.read_line(f"special comment {index + 1}")
        for index in range(special_comment_count)
    )

    (normal_comment_count,) = _parse_line(
        reader.read_line("normal-comment count"),
        1,
        int,
        "NNCOML",
        file_format=file_format
    )

    normal_comments = tuple(
        reader.read_line(f"normal comment {index + 1}")
        for index in range(normal_comment_count)
    )

    # NLHEAD includes the first line and says how many physical
    # header lines appear before the first data value.
    if reader.line_number != header_lines:
        raise ValueError(
            f"NLHEAD says the header contains {header_lines} lines, "
            f"but parsing consumed {reader.line_number}"
        )

    return Ames1001Header(
        header_lines=header_lines,
        ffi=ffi,
        originator=originator,
        organization=organization,
        source=source,
        mission=mission,
        volume=volume,
        total_volumes=total_volumes,
        data_date=data_date,
        revision_date=revision_date,
        independent_interval=independent_interval,
        independent_name=independent_name,
        scales=scales,
        missing_values=missing_values,
        variable_names=variable_names,
        special_comments=special_comments,
        normal_comments=normal_comments,
    )

def _iter_ames_1001_records(
    reader: _LineReader,
    header: Ames1001Header,
    *,
    file_format: str = 'Ames'
) -> Iterator[Ames1001Record]:
    # One independent value plus NV primary values.
    expected_count = 1 + len(header.variable_names)

    current: list[float] = []
    record_start_line: int | None = None

    for line in reader.remaining_lines():
        if not line.strip():
            continue

        if not current:
            record_start_line = reader.line_number

        fields = _split_fields(line, file_format)
        needed = expected_count - len(current)

        # Anything after the required numeric values on the final
        # physical line is treated as a record annotation.
        numeric_fields = fields[:needed]
        trailing_fields = fields[needed:]

        try:
            current.extend(float(field) for field in numeric_fields)
        except ValueError as exc:
            raise ValueError(
                f"Non-numeric value in record beginning at physical "
                f"line {record_start_line}: {line!r}"
            ) from exc

        if len(current) == expected_count:
            independent = current[0]
            raw_values = tuple(current[1:])

            scaled_values = tuple(
                None if raw == missing else raw * scale
                for raw, scale, missing in zip(
                    raw_values,
                    header.scales,
                    header.missing_values,
                )
            )

            yield Ames1001Record(
                independent=independent,
                raw_values=raw_values,
                values=scaled_values,
                annotation=" ".join(trailing_fields) or None,
            )

            current = []
            record_start_line = None

    if current:
        raise ValueError(
            f"Incomplete final record beginning at physical line "
            f"{record_start_line}: expected {expected_count} numeric "
            f"values, found {len(current)}"
        )

FFI1001Format: TypeAlias = Literal["Ames", "ICARTT"]
FFI1001Result: TypeAlias = tuple[
    Ames1001Header,
    Iterator[Ames1001Record],
]

def _read_ames_1001(
    stream: TextIO,
    file_format: str = 'Ames'
) -> tuple[Ames1001Header, Iterator[Ames1001Record]]:
    reader = _LineReader(stream)
    header = _read_ames_1001_header(reader, file_format=file_format)
    records = _iter_ames_1001_records(reader, header, file_format=file_format)

    return header, records

@contextmanager
def _open_1001(
    source: AmesSource,
    *,
    encoding: str,
    file_format: FFI1001Format,
) -> Iterator[FFI1001Result]:
    """
    Open and parse an FFI 1001 file.

    Caller-owned streams remain open when this context exits. Records must
    be consumed inside the context block.
    """
    if isinstance(source, (str, PathLike)):
        with open(
            source,
            "rt",
            encoding=encoding,
            newline=None,
        ) as stream:
            yield _read_ames_1001(
                stream,
                file_format=file_format,
            )
        return

    if isinstance(source, TextIOBase):
        # The stream has already been decoded, so encoding is inapplicable.
        yield _read_ames_1001(
            source,
            file_format=file_format,
        )
        return

    # Adapt a caller-owned binary stream, including an S3 StreamingBody,
    # without closing it when parsing finishes.
    wrapper = TextIOWrapper(
        source,
        encoding=encoding,
        newline=None,
    )

    try:
        yield _read_ames_1001(
            wrapper,
            file_format=file_format,
        )
    finally:
        wrapper.detach()

def open_ames_1001(
    source: AmesSource,
    *,
    encoding: str = "ascii",
) -> AbstractContextManager[FFI1001Result]:
    """Open a whitespace-delimited NASA Ames FFI 1001 file."""
    return _open_1001(
        source,
        encoding=encoding,
        file_format="Ames",
    )

def open_icartt_1001(
    source: AmesSource,
    *,
    encoding: str = "ascii",
) -> AbstractContextManager[FFI1001Result]:
    """Open a comma-delimited ICARTT FFI 1001 file."""
    return _open_1001(
        source,
        encoding=encoding,
        file_format="ICARTT",
    )