from __future__ import annotations
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, BinaryIO, Iterable, TextIO

import numpy as np

from .streams import as_text_stream


AVAPS_D_DTYPE = np.dtype(
    [
        # MDX relevant fields
        ("timestamp", "datetime64[ms]"),
        ("longitude_deg", "f8"),
        ("latitude_deg", "f8"),

        # Raw AVAPS D data fields
        ("record_type", "U9"),
        ("source_flag", "U3"),
        ("sonde_id", "U24"),
        ("pressure_mb", "f8"),
        ("temperature_c", "f8"),
        ("relative_humidity_pct", "f8"),
        ("wind_direction_deg", "f8"),
        ("wind_speed_m_s", "f8"),
        ("vertical_velocity_m_s", "f8"),
        ("gps_altitude_m", "f8"),
        ("gps_satellites", "f8"),
        ("rh_sensor_1_pct", "f8"),
        ("rh_sensor_2_pct", "f8"),
    ]
)


AVAPS_D_MISSING_SENTINELS: tuple[float | None, ...] = (
    9999.0,   # Pressure
    99.0,     # Temperature
    999.0,    # Relative humidity
    999.0,    # Wind direction
    999.0,    # Wind speed
    999.0,    # Vertical velocity
    999.0,    # Longitude
    99.0,     # Latitude
    99999.0,  # GPS altitude
    99.0,     # GPS satellites
    999.0,    # RH sensor 1
    999.0,    # RH sensor 2
)


ASPEN_DTYPE = np.dtype(
    [
        # MDX relevant fields
        ("timestamp", "datetime64[ms]"),
        ("longitude_deg", "f8"),
        ("latitude_deg", "f8"),

        # ASPEN data fields
        ("time_offset_s", "f8"),
        ("pressure_mb", "f8"),
        ("temperature_c", "f8"),
        ("dewpoint_c", "f8"),
        ("relative_humidity_pct", "f8"),
        ("u_wind_m_s", "f8"),
        ("v_wind_m_s", "f8"),
        ("wind_speed_m_s", "f8"),
        ("wind_direction_deg", "f8"),
        ("dz_m_s", "f8"),
        ("range_km", "f8"),
        ("angle_deg", "f8"),
        ("altitude_m", "f8"),
        ("quality_pressure", "f8"),
        ("quality_temperature", "f8"),
        ("quality_humidity", "f8"),
        ("quality_u_wind", "f8"),
        ("quality_v_wind", "f8"),
        ("quality_wind", "f8"),
    ]
)


ASPEN_MISSING_SENTINELS: tuple[float | None, ...] = (
    9999.0,   # Time
    9999.0,   # Pressure
    999.0,    # Temperature
    999.0,    # Dew point
    999.0,    # Relative humidity
    999.0,    # U wind
    999.0,    # V wind
    999.0,    # Wind speed
    999.0,    # Wind direction
    99.0,     # dZ
    999.0,    # Longitude
    999.0,    # Latitude
    999.0,    # Range
    999.0,    # Angle
    99999.0,  # Altitude
    99.0,     # Qp
    99.0,     # Qt
    99.0,     # Qh
    99.0,     # Qu
    99.0,     # Qv
    99.0,     # Quv
)


DropsondeSource = (
    str
    | os.PathLike[str]
    | bytes
    | bytearray
    | BinaryIO
    | TextIO
)


@dataclass(frozen=True)
class AvapsFileMetadata:
    """
    Dataclass for AVAPS file metadata, comments, and statistics.
    """

    fields: dict[str, str]
    event_records: tuple[str, ...]
    version_records: tuple[str, ...]
    format_records: tuple[str, ...]
    time_offset_records: tuple[str, ...]
    other_records: tuple[str, ...]
    time_zone: str
    input_record_count: int
    returned_record_count: int
    excluded_required_field_count: int
    malformed_record_count: int


@dataclass(frozen=True)
class AspenFileMetadata:
    """
    Dataclass for AVAPS file metadata, comments, and statistics.
    """

    fields: dict[str, str]
    time_zone: str
    input_record_count: int
    returned_record_count: int
    excluded_required_field_count: int
    malformed_record_count: int
    launch_time_utc: datetime


@dataclass(frozen=True)
class DropsondeResult:
    """
    Dataclass for AVAPS file contents. Only measurements which contain valid
    longitude, latitude, and time are included.

    Attributes:
    format: "aspen" or "avaps_d"
    metadata: File metadata
    data: numpy array containing measurement data. 
          First three values of each line are datetime, longitude and latitude.
          Each row follows the order defined in AVAPS_D_DTYPE or ASPEN_D_TYPE.
    """

    format: str
    metadata: AvapsFileMetadata | AspenFileMetadata
    data: np.ndarray


def _parse_avaps_datetime(date_text: str, time_text: str) -> datetime | None:
    """
    Parses a raw AVAPS YYMMDD date and HHMMSS.s time and returns datetime object.
    """

    date_value = date_text.strip()
    time_value = time_text.strip()

    if not re.fullmatch(r"\d{6}", date_value):
        return None

    if not re.fullmatch(r"\d{6}(?:\.\d+)?", time_value):
        return None

    yy = int(date_value[0:2])
    month = int(date_value[2:4])
    day = int(date_value[4:6])
    if yy >= 70:
        year = 1900 + yy
    else:
        year = 2000 + yy

    hour = int(time_value[0:2])
    minute = int(time_value[2:4])
    second = float(time_value[4:])

    if minute > 59 or second >= 60.0:
        return None
    try:
        midnight = datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None

    return midnight + timedelta(
        hours=hour,
        minutes=minute,
        seconds=second,
    )


def _str_to_float(text: str, missing_sentinel: float | None = None) -> float:
    """
    Convert text to a float or return NaN.
    """

    value = text.strip()
    if not value:
        return math.nan
    try:
        result = float(value)
    except ValueError:
        return math.nan

    if (
        missing_sentinel is not None
        and math.isclose(
            result,
            missing_sentinel,
            rel_tol=0.0,
            abs_tol=1.0e-9,
        )
    ):
        return math.nan

    return result


def _dash_spans(line: str) -> list[tuple[int, int]]:
    """
    Extract fixed-width column spans from separator runs.
    A column separator must contain at least two consecutive hyphens. This
    ignores the single hyphen in transport identifiers such as "AVAPS-T01".
    """

    return [
        (match.start(), match.end())
        for match in re.finditer(r"-{2,}", line)
    ]


def _slice_fixed_fields(line: str, spans: Iterable[tuple[int, int]]) -> list[str]:
    """
    Extract fixed-width fields from one line.
    """

    return [
        line[start:end].strip()
        for start, end in spans
    ]


def _valid_coordinates(longitude: float, latitude: float) -> bool:
    """
    Validate required longitude and latitude values.
    """

    return (
        math.isfinite(longitude)
        and math.isfinite(latitude)
        and -180.0 <= longitude <= 180.0
        and -90.0 <= latitude <= 90.0
    )


def _read_avaps_d_lines(lines: Iterable[str]) -> DropsondeResult:
    """
    Parse an iterable of decoded raw AVAPS D file lines.
    """

    rows: list[tuple[Any, ...]] = []
    com_fields: dict[str, str] = {}
    event_records: list[dict[str, Any]] = []
    version_records: list[str] = []
    format_records: list[str] = []
    time_offset_records: list[str] = []
    other_records: list[str] = []

    # The dashed COM row describes 15 fields beginning with Sonde ID:
    # ID, date, time, then 12 numeric measurements.
    column_spans: list[tuple[int, int]] | None = None

    total_record_count = 0
    excluded_required_field_count = 0
    malformed_record_count = 0

    for line in lines:
        stripped = line.strip()

        if not stripped:
            continue

        if stripped.startswith("AVAPS-T01 COM"):
            spans = _dash_spans(line)

            # Capture the standard fixed-width data layout from the COM
            # separator row.
            if len(spans) >= 15:
                column_spans = spans[:15]

            # Text following the COM prefix can contain trailing metadata as a
            # key/value pair. Heading rows do not contain a colon and are not
            # added to the parsed COM dictionary.
            com_body = line[len("AVAPS-T01 COM"):].strip()

            if ":" in com_body:
                key, _, value = com_body.partition(":")
                key = key.strip()
                value = value.strip()

                if key:
                    com_fields[key] = value

            continue

        if stripped.startswith(
            (
                "AVAPS-T01 STA",
                "AVAPS-T01 LAU",
                "AVAPS-T01 END",
            )
        ):

            event = _parse_avaps_event_record(stripped)
            event_records.append(event)
            continue

        if stripped.startswith("AVAPS-T01 VER"):
            version_records.append(stripped)
            continue

        if stripped.startswith("AVAPS-T01 FMT"):
            format_records.append(stripped)
            continue

        if stripped.startswith("AVAPS-T01 TOF"):
            time_offset_records.append(stripped)
            continue

        if not stripped.startswith("AVAPS-D01"):
            other_records.append(stripped)
            continue

        total_record_count += 1

        parsed = _parse_avaps_d_record(line, column_spans=column_spans)

        if parsed is None:
            malformed_record_count += 1
            continue

        (
            timestamp,
            record_type,
            source_flag,
            sonde_id,
            measurements,
        ) = parsed

        longitude_deg = measurements[6]
        latitude_deg = measurements[7]

        if not _valid_coordinates(longitude_deg, latitude_deg):
            excluded_required_field_count += 1
            continue

        rows.append(
            (
                np.datetime64(timestamp, "ms"),
                longitude_deg,
                latitude_deg,
                record_type,
                source_flag,
                sonde_id,
                measurements[0],   # Pressure
                measurements[1],   # Temperature
                measurements[2],   # Relative humidity
                measurements[3],   # Wind direction
                measurements[4],   # Wind speed
                measurements[5],   # Vertical velocity
                measurements[8],   # GPS altitude
                measurements[9],   # GPS satellites
                measurements[10],  # RH sensor 1
                measurements[11]   # RH sensor 2
            )
        )

    metadata = AvapsFileMetadata(
        fields=com_fields,
        event_records=tuple(event_records),
        version_records=tuple(version_records),
        format_records=tuple(format_records),
        time_offset_records=tuple(time_offset_records),
        other_records=tuple(other_records),
        time_zone="UTC",
        input_record_count=total_record_count,
        returned_record_count=len(rows),
        excluded_required_field_count=excluded_required_field_count,
        malformed_record_count=malformed_record_count
    )

    return DropsondeResult(
        format="avaps_d",
        metadata=metadata,
        data=np.asarray(rows, dtype=AVAPS_D_DTYPE),
    )


def _parse_avaps_d_record(
    line: str,
    *,
    column_spans: list[tuple[int, int]] | None,
) -> (tuple[datetime, str, str, str, list[float]] | None):
    """
    Parse a AVAPS-D01 measurement record.
    Returns None when the line structure or timestamp is invalid. Missing
    nonrequired measurements are returned as NaN.
    """

    if column_spans is not None:
        first_data_column = column_spans[0][0]

        # The prefix before Sonde ID contains the measurement record type and
        # source/quality flag.
        prefix_fields = line[:first_data_column].split()
        fixed_fields = _slice_fixed_fields(
            line,
            column_spans,
        )

        if len(prefix_fields) < 2 or len(fixed_fields) != 15:
            return None

        record_type = prefix_fields[0]
        source_flag = prefix_fields[1]

        sonde_id = fixed_fields[0]
        date_text = fixed_fields[1]
        time_text = fixed_fields[2]
        measurement_fields = fixed_fields[3:15]

    else:
        # A raw record consists of five identifying/time fields followed by
        # 12 numeric measurement fields.
        fields = line.split()

        if len(fields) < 17:
            return None

        record_type = fields[0]
        source_flag = fields[1]
        sonde_id = fields[2]
        date_text = fields[3]
        time_text = fields[4]
        measurement_fields = fields[5:17]

    if record_type != "AVAPS-D01":
        return None

    timestamp = _parse_avaps_datetime(date_text, time_text)

    if timestamp is None:
        # Invalid or empty date/time makes the record unusable for temporal
        # bounds and therefore excludes it.
        return None

    if len(measurement_fields) != 12:
        return None

    measurements = [
        _str_to_float(field, sentinel)
        for field, sentinel in zip(
            measurement_fields,
            AVAPS_D_MISSING_SENTINELS,
        )
    ]

    return (
        timestamp,
        record_type,
        source_flag,
        sonde_id,
        measurements,
    )


def _parse_avaps_event_record(line: str) -> dict[str, Any]:
    """
    Parse an AVAPS STA, LAU, or END event record.
    Malformed event records are still returned with their raw record
    and a None timestamp.
    """

    fields = line.split()

    event: dict[str, Any] = {
        "raw": line,
        "event_type": None,
        "sonde_id": None,
        "date": None,
        "time": None,
        "timestamp_utc": None,
    }

    if len(fields) < 5:
        return event

    transport_record = fields[0]
    event_type = fields[1]
    sonde_id = fields[2]
    date_text = fields[3]
    time_text = fields[4]

    event["transport_record"] = transport_record
    event["event_type"] = event_type
    event["sonde_id"] = sonde_id
    event["date"] = date_text
    event["time"] = time_text
    event["timestamp_utc"] = _parse_avaps_datetime(
        date_text,
        time_text
    )

    return event


def read_avaps_d_file(source: DropsondeSource, *, encoding: str = "ascii") -> DropsondeResult:
    """
    Read a raw NOAA AVAPS D-file.
    Only AVAPS-D01 measurement records are considered data records. Records
    are excluded if date/time, longitude, or latitude is missing or invalid.
    Other missing measurements are retained as NaN.
    """

    stream = as_text_stream(source, encoding=encoding)
    return _read_avaps_d_lines(stream)


def _parse_aspen_launch_time(text: str) -> datetime | None:
    """
    Parse ASPEN launch-time value from header line.
    """

    match = re.search(
        r"(\d{4})\s*,\s*"
        r"(\d{1,2})\s*,\s*"
        r"(\d{1,2})\s*,\s*"
        r"(\d{1,2}):"
        r"(\d{1,2}):"
        r"(\d{1,2}(?:\.\d+)?)",
        text
    )

    if match is None:
        return None

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = float(match.group(6))

    if minute > 59 or second >= 60.0:
        return None

    try:
        midnight = datetime(year, month, day)
    except ValueError:
        return None

    return midnight + timedelta(
        hours=hour,
        minutes=minute,
        seconds=second,
    )


def _read_aspen_lines(lines: Iterable[str]) -> DropsondeResult:
    """
    Parse an iterable of decoded ASPEN file lines.
    """

    parsed_header: dict[str, str] = {}
    raw_header_lines: list[str] = []
    rows: list[tuple[Any, ...]] = []

    launch_time: datetime | None = None
    column_spans: list[tuple[int, int]] | None = None
    data_started = False
    table_found = False
    input_record_count = 0
    excluded_required_field_count = 0
    malformed_record_count = 0

    for line in lines:
        stripped = line.strip()

        if not data_started:
            # Preserve all content preceding and including the table separator.
            # This retains the original header exactly enough for downstream
            # inspection without storing the data rows a second time.
            raw_header_lines.append(line)

            if ":" in line:
                # Split only on the first colon because header values can
                # contain times such as 20:04:09.
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()

                if key:
                    parsed_header[key] = value

                    if key.startswith("GMT Launch Time"):
                        launch_time = _parse_aspen_launch_time(value)

            # A standard ASPEN separator has 21 runs of hyphens, one for each
            # table column.
            spans = _dash_spans(line)

            if len(spans) >= 21:
                column_spans = spans[:21]
                data_started = True
                table_found = True

            continue

        if not stripped:
            continue

        input_record_count += 1

        # The separator-derived spans preserve empty fixed-width columns.
        # The whitespace fallback supports nonstandard files while providing
        # less protection against embedded empty columns.
        if column_spans is not None:
            fields = _slice_fixed_fields(line, column_spans)
        else:
            fields = stripped.split()

        if len(fields) != 21:
            malformed_record_count += 1
            continue

        values = [
            _str_to_float(field, sentinel)
            for field, sentinel in zip(
                fields,
                ASPEN_MISSING_SENTINELS,
            )
        ]

        time_offset_s = values[0]
        longitude_deg = values[10]
        latitude_deg = values[11]

        # Time, longitude, and latitude are required for the intended spatial
        # and temporal bounds use case.
        if not math.isfinite(time_offset_s):
            excluded_required_field_count += 1
            continue

        if not _valid_coordinates(
            longitude_deg,
            latitude_deg,
        ):
            excluded_required_field_count += 1
            continue

        if launch_time is None:
            raise ValueError(
                "ASPEN data contains records with valid coordinates, but "
                "the GMT Launch Time header is missing or invalid"
            )

        timestamp = launch_time + timedelta(
            seconds=time_offset_s
        )

        rows.append(
            (
                np.datetime64(timestamp, "ms"),
                longitude_deg,
                latitude_deg,
                time_offset_s,
                values[1],   # Pressure
                values[2],   # Temperature
                values[3],   # Dew point
                values[4],   # Relative humidity
                values[5],   # U wind
                values[6],   # V wind
                values[7],   # Wind speed
                values[8],   # Wind direction
                values[9],   # dZ
                values[12],  # Range
                values[13],  # Angle
                values[14],  # Altitude
                values[15],  # Qp
                values[16],  # Qt
                values[17],  # Qh
                values[18],  # Qu
                values[19],  # Qv
                values[20],  # Quv
            )
        )

    if not table_found:
        raise ValueError(
            "No ASPEN data-table separator with 21 columns was found"
        )

    metadata = AspenFileMetadata(
        fields=parsed_header,
        time_zone="UTC",
        input_record_count=input_record_count,
        returned_record_count=len(rows),
        excluded_required_field_count=excluded_required_field_count,
        malformed_record_count=malformed_record_count,
        launch_time_utc=launch_time
    )

    return DropsondeResult(
        format="aspen",
        metadata=metadata,
        data=np.asarray(rows, dtype=ASPEN_DTYPE),
    )


def read_aspen_file(source: DropsondeSource) -> DropsondeResult:
    """
    Read an ASPEN QC sounding file.
    """

    stream = as_text_stream(source)
    return _read_aspen_lines(stream)