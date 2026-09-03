from datetime import datetime, time, timedelta, timezone
from utils.mdx import MDX
from utils.ames import open_icartt_1001
from utils.streams import as_seekable_hdf5_stream
import numpy as np
import h5netcdf
from netCDF4 import num2date
from typing import Any

short_name = "avapsnurture"
provider_path = "avapsnurture/"

def valid_values(variable):
    """Return finite values valid under this NetCDF variable's own metadata."""
    values = np.asarray(variable[:])
    invalid = ~np.isfinite(values)

    for attr_name in ("_FillValue", "missing_value"):
        fill_value = variable.attrs.get(attr_name)
        if fill_value is not None:
            for value in np.atleast_1d(fill_value):
                invalid |= values == value

    valid_range = variable.attrs.get("valid_range")
    if valid_range is not None:
        low, high = np.asarray(valid_range)
        invalid |= (values < low) | (values > high)

    return values[~invalid]

def as_utc_datetime(value):
    return datetime(
        value.year,
        value.month,
        value.day,
        value.hour,
        value.minute,
        value.second,
        value.microsecond,
        tzinfo=timezone.utc,
    )

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.nav_lookup = {}

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream=None) -> dict[str, Any]:
        if filename.lower().endswith(".ict"):
            # Process as Ames
            return self.process_ames(filename, stream)
        elif filename.lower().endswith(".nc"):
            # Process as NetCDF4
            return self.process_netcdf(filename, stream)
        else:
            return {}

    def process_ames(self, filename: str, stream=None) -> dict[str, Any]:
        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        max_lon = float("-inf")
        min_lon = float("inf")
        max_lat = float("-inf")
        min_lat = float("inf")

        with open_icartt_1001(stream, encoding="utf-8") as (header, records):
            latitude_index = header.variable_names.index(
                "Latitude"
            )
            longitude_index = header.variable_names.index(
                "Longitude"
            )
            beginning_of_day = datetime.combine(
                header.data_date,
                time.min,
                tzinfo=timezone.utc,
            )

            for record in records:
                timestamp = beginning_of_day + timedelta(
                    seconds=record.independent
                )

                latitude = record.values[latitude_index]
                longitude = record.values[longitude_index]
                if not isinstance(latitude, float) or not isinstance(longitude, float):
                    continue
                if not -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    continue

                min_lat = min(min_lat, latitude)
                max_lat = max(max_lat, latitude)
                min_lon = min(min_lon, longitude)
                max_lon = max(max_lon, longitude)
                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

        return {
            "start": start_time,
            "end": end_time,
            "north": max_lat,
            "south": min_lat,
            "east": max_lon,
            "west": min_lon,
            "format": "UTF-8"
        }

    def process_netcdf(self, filename: str, stream=None) -> dict[str, Any]:
        file_buffer = as_seekable_hdf5_stream(stream)

        with h5netcdf.File(file_buffer, mode="r") as nc:
            lat = valid_values(nc.variables["lat"])
            lon = valid_values(nc.variables["lon"])
            time = valid_values(nc.variables["time"])

            if not len(lat) or not len(lon) or not len(time):
                raise ValueError("AVAPS granule has no valid latitude, longitude, or time values")

            start_raw = float(np.min(time))
            end_raw = float(np.max(time))

            time_var = nc.variables["time"]
            start, end = num2date(
                [start_raw, end_raw],
                units=time_var.attrs["units"],
                calendar=time_var.attrs.get("calendar", "standard"),
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=True,
            )

        return {
            "start": as_utc_datetime(start),
            "end": as_utc_datetime(end),
            "north": float(np.max(lat)),
            "south": float(np.min(lat)),
            "east": float(np.max(lon)),
            "west": float(np.min(lon)),
            "format": "netCDF-4",
        }

if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')