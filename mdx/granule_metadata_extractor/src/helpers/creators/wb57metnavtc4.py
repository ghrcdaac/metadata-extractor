from datetime import datetime, timedelta, timezone
import io
import re
from typing import Any
from pathlib import PurePosixPath
from utils.mdx import MDX

short_name = "wb57metnavtc4"
provider_path = "wb57metnavtc4/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.nav_lookup = {}

    def main(self):
        self.build_navigation_lookup(provider_path)
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream) -> dict[str, Any]:
        date_key = self.get_date_key(filename)

        spatial = self.nav_lookup[date_key]
        temporal = self.read_temporal_bounds(stream)

        return {
            "start": temporal["start_time"],
            "end": temporal["end_time"],
            "north": spatial["max_lat"],
            "south": spatial["min_lat"],
            "east": spatial["max_lon"],
            "west": spatial["min_lon"],
            "format": "ASCII"
        }

    @staticmethod
    def get_date_key(filename):
        """Parse date key from filename."""
        match = re.search(r"_(\d{8})\.txt$", filename)

        if not match:
            raise ValueError(f"Could not extract date from {filename}")

        return match.group(1)

    def build_navigation_lookup(self, provider_path: str, bucket: str = 'ghrcw-private'):
        """Pre-read spatial-aware NP files in dataset and store GPS bounds by date."""
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=provider_path
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if "_NP_" not in key or not key.endswith(".txt"):
                    continue

                response = self.s3.get_object(
                    Bucket=bucket,
                    Key=key
                )

                stream = response["Body"]
                filename = PurePosixPath(key).name

                nav_dict = self.read_spatial_bounds(stream)

                date_key = self.get_date_key(filename)
                self.nav_lookup[date_key] = nav_dict

        return self.nav_lookup

    @staticmethod
    def read_spatial_bounds(stream) -> dict[str, float]:
        """Read data file and compute spatial bounds."""
        with io.TextIOWrapper(stream, encoding="utf-8") as f:
            # First line of Ames format is, e.g. "32 1001", number of [header lines] [file format]
            header_lines, file_format = map(int, next(f).split())

            if file_format != 1001:
                raise ValueError(f"Unsupported NASA Ames format: {file_format}")

            for _ in range(header_lines - 2):
                next(f)

            columns = next(f).split()

            lat_idx = columns.index("gLat")
            lon_idx = columns.index("gLon")
            ut_idx = columns.index("UT")
            min_lat = float("inf")
            max_lat = float("-inf")
            min_lon = float("inf")
            max_lon = float("-inf")

            for line in f:
                values = line.split()

                lat = float(values[lat_idx])
                lon = float(values[lon_idx])

                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)

            if min_lat == float("inf"):
                raise ValueError("No valid latitude/longitude values found")

        nav_dict ={
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
        }
        return nav_dict

    @staticmethod
    def read_temporal_bounds(stream) -> dict[str, float]:
        """Read data file and compute temporal bounds (start and end date)."""
        with io.TextIOWrapper(stream, encoding="utf-8") as f:
            # First line of Ames format is, e.g. "32 1001", number of [header lines] [file format]
            header_lines, file_format = map(int, next(f).split())
            current_line = 1

            if file_format != 1001:
                raise ValueError(f"Unsupported NASA Ames format: {file_format}")

            oname = next(f).strip()
            org = next(f).strip()
            sname = next(f).strip()
            mname = next(f).strip()
            current_line += 4

            ivol, nvol = map(int, next(f).split())
            current_line += 1

            year, month, day, ryear, rmonth, rday = map(int, next(f).split())
            current_line += 1

            base_time = datetime(year, month, day, tzinfo=timezone.utc)
            date_key = f"{year:04d}{month:02d}{day:02d}"

            for _ in range(header_lines - current_line - 1):
                next(f)

            columns = next(f).split()

            ut_idx = columns.index("UT")
            min_ut = float("inf")
            max_ut = float("-inf")

            for line in f:
                values = line.split()

                if not values:
                    continue

                ut = float(values[ut_idx])

                min_ut = min(min_ut, ut)
                max_ut = max(max_ut, ut)

            if min_ut == float("inf"):
                raise ValueError("No valid UT values found")

            start_time = base_time + timedelta(seconds=min_ut)
            end_time = base_time + timedelta(seconds=max_ut)

        temp_dict = {
            "start_time": start_time,
            "end_time": end_time,
            "base_time": base_time,
            "date_key": date_key,
        }
        return temp_dict