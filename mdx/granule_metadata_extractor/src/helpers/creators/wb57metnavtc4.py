from datetime import datetime, timedelta, timezone
import io
import re
from typing import Any
from pathlib import PurePosixPath
import boto3
from utils.mdx import MDX

short_name = "wb57metnavtc4"
provider_path = "wb57metnavtc4/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.nav_lookup = {}

    def main(self):
        self.nav_lookup = self.build_navigation_lookup(provider_path)
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream) -> dict[str, Any]:
        date_key = self.get_date_key(filename)

        if date_key not in self.nav_lookup:
            raise ValueError(
                f"No navigation data found for {filename}"
                f"(date {date_key})"
            )
        spatial = self.nav_lookup[date_key]
        temporal = self.read_temporal_bounds(stream)

        if date_key != temporal["date_key"]:
            raise ValueError(
                f"Filename date {date_key} does not match"
                f"NASA Ames DATE {temporal['date_key']} for {filename}"
            )

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
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        nav_lookup = {}

        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=provider_path
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if "_NP_" not in key or not key.endswith(".txt"):
                    continue

                response = s3_client.get_object(
                    Bucket=bucket,
                    Key=key
                )

                stream = response["Body"]
                filename = PurePosixPath(key).name

                nav_dict = self.read_spatial_bounds(stream)

                date_key = self.get_date_key(filename)
                nav_lookup[date_key] = nav_dict

        print("Found navigation data for these dates: ", list(nav_lookup.keys()))

        return nav_lookup

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
            # NP files containing spatial data all appear currently to be well-formed,
            # so I won't worry about any special handling here.
            if not {"UT", "gLat", "gLon"}.issubset(columns):
                raise ValueError("Did not find expected column headers 'UT', 'gLat', 'gLon'."
                                 "Instead parsed columns " + columns + ". File may be malformed.")

            lat_idx = columns.index("gLat")
            lon_idx = columns.index("gLon")
            # ut_idx = columns.index("UT")
            min_lat = float("inf")
            max_lat = float("-inf")
            min_lon = float("inf")
            max_lon = float("-inf")

            for line in f:
                values = line.split()

                if not values:
                    continue

                lat = float(values[lat_idx])
                lon = float(values[lon_idx])

                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    continue

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
    def read_temporal_bounds(stream) -> dict[str, Any]:
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

            columns = []
            for _ in range(header_lines - current_line - 1):
                next_line = next(f)
                try_columns = next_line.strip().split()
                if "UT" in try_columns:
                    # File is malformed and prematurely found column headers
                    columns = try_columns
                    break

            # In well formed file, should arrive here and parse column headers normally
            if not columns:
                columns = next(f).strip().split()
            if not "UT" in columns:
                raise ValueError("Did not find expected column header 'UT'."
                                 "Instead parsed columns " + columns + ". File may be malformed.")

            ut_idx = columns.index("UT")
            min_ut = float("inf")
            max_ut = float("-inf")

            for line in f:
                values = line.strip().split()

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

if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
