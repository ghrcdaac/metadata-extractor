"""Read LMA (Lightning Mapping Array) files."""
from __future__ import annotations
import re
from datetime import datetime
from dataclasses import dataclass
from typing import TextIO, Any, Iterator

@dataclass()
class LMAHeader:
    """Header for LMA (Lightning Mapping Array) file."""
    start_time: datetime
    analyzed_duration: int|float
    coordinate_center: tuple[float, float, float]
    coordinate_frame: str
    maximum_lma_diameter_km: float
    station_info: dict[str, dict[str, Any]]
    num_stations: int
    location: str
    analysis_program: str
    analysis_program_version: str
    data_fields: list[str]
    data_formats: list[str]

@dataclass()
class LMARecord:
    """LMA (Lightning Mapping Array) file."""
    time: float
    latitude: float
    longitude: float
    altitude: float
    reduced_chi_squared: float
    power: float
    mask: bytes

def _parse_lma_header(headers: list[tuple[str, str]]) -> LMAHeader:
    header_info = {
        'Analysis program': ['analysis_program', str],
        'Analysis program version': ['analysis_program_version', str],
        'Data start time': ['start_time', datetime],
        'Number of seconds analyzed': ['analyzed_duration', int],
        'Coordinate center (lat,lon,alt)': ['coordinate_center', (tuple, float)],
        'Coordinate frame': ['coordinate_frame', str],
        'Number of stations': ['num_stations', int],
        'Maximum diameter of LMA (km)': ['maximum_lma_diameter_km', float],
        'Data': ['data_fields', list],
        'Data format': ['data_formats', tuple],
        'Location': ['location', str],
    }
    station_info_str = 'Station information'
    station_info_fields = []
    station_info = {}

    header_values = {}

    for header, item in headers:
        if header in header_info:
            header_name, header_type = header_info[header]
            if isinstance(header_type, tuple):
                header_type, header_subtype = header_type
            else:
                header_subtype = str

            if header_type == int:
                item = int(item)
            elif header_type == float:
                item = float(item)
            elif header_type == datetime:
                item = datetime.strptime(item, "%m/%d/%y %H:%M:%S")

            elif header_type == tuple:
                item = tuple(item.split())
                if header_subtype:
                    item = tuple([header_subtype(i) for i in item])
            elif header_type == list:
                item = re.split(r',\s+', item)
                if header_subtype:
                    item = [header_subtype(i) for i in item]
            header_values[header_name] = item
        elif header == station_info_str:
            station_info_fields = re.split(r',\s+', item)
        elif header == 'Sta_info':
            fields = re.split(r',\s+', item)
            if len(fields) != len(station_info_fields):
                continue
            sta_info = dict(zip(station_info_fields, fields))
            if 'id' not in sta_info:
                continue
            sta_id = station_info['id']
            station_info[sta_id] = sta_info
    header_values['station_info'] = station_info

    return LMAHeader(**header_values)

def _read_lma_header(fs: TextIO):
    headers = []
    for line in fs:
        line = line.strip()

        if line == "*** data ***":
            # End of header
            return _parse_lma_header(headers)
        elif m := re.match(r'^([^:]+):\s*(.*)$', line):
            header, item = m.groups()
            header = header.strip()
            item = item.strip()
            headers.append((header, item))

    return _parse_lma_header(headers)

def _parse_lma_record(
        fields: list[str],
        field_names: list[str],
        field_info: dict[str, list[Any]],
):
    if len(fields) != len(field_names):
        raise ValueError(f"Mismatched data and data labels: {fields} does not match {field_names}")

    record_values = {}

    fields_z = zip(field_names, fields)
    for field_name, field_value in fields_z:
        if field_name not in field_info:
            raise ValueError(f"Unknown field {field_name!r}")
        field_attr, field_type = field_info[field_name]
        if field_name == 'mask':
            field_value = int(field_value, 16)
        elif field_type == float:
            field_value = float(field_value)
        elif field_type == int:
            field_value = int(field_value)

        record_values[field_attr] = field_value

    return LMARecord(**record_values)

def _iter_lma_records(
        stream: TextIO,
        header: LMAHeader
) -> Iterator[LMARecord]:

    field_info = {
        'time (UT sec of day)': ['time', float],
        'lat': ['latitude', float],
        'lon': ['longitude', float],
        'alt(m)': ['altitude', float],
        'reduced chi^2': ['reduced_chi_squared', float],
        'P(dBW)': ['power', float],
        'mask': ['mask', int],
    }

    field_names = header.data_fields

    for line in stream:
        line = line.strip()
        if not line:
            continue
        fields = re.split(r'\s+', line)
        record = _parse_lma_record(fields, field_names, field_info)
        yield record


def read_lma_file(
    stream,
) -> tuple[LMAHeader, Iterator[LMARecord]]:
    header = _read_lma_header(stream)
    records = _iter_lma_records(stream, header)

    return header, records