"""Creator for DC-8 BBIR TC-4 (dc8bbirtc4)."""
from datetime import datetime, time, timedelta, timezone
import re
from typing import Any
import os
from utils.mdx import MDX
from utils.ames import open_ames_1001
from pprint import pprint

short_name = "dc8bbirtc4"
provider_path = "dc8bbirtc4/"
nav_lookup_local_path = ""

# dc8bbirtc4 (TC4 BBIR DC-8) contains no nav data;
# nav data needs to be looked up from TC4 NAV DC-8 (dc8navtc4)
# which is already out of ghrc-private.
# nav_lookup is hard-coded here because it's easier, not because it's elegant.
nav_lookup = {
 '20070628': {'max_lat': 40.138952,
              'max_lon': -119.374708,
              'min_lat': 38.44974,
              'min_lon': -126.368228},
 '20070630': {'max_lat': 44.426224,
              'max_lon': -121.488932,
              'min_lat': 38.643756,
              'min_lon': -127.532152},
 '20070702': {'max_lat': 38.794256,
              'max_lon': -117.093816,
              'min_lat': 31.694268,
              'min_lon': -125.133096},
 '20070713': {'max_lat': 38.679943,
              'max_lon': -84.20351,
              'min_lat': 9.726677,
              'min_lon': -121.400814},
 '20070717': {'max_lat': 10.00803,
              'max_lon': -80.030766,
              'min_lat': -3.376579,
              'min_lon': -84.826298},
 '20070721': {'max_lat': 12.294216,
              'max_lon': -72.433205,
              'min_lat': 2.468834,
              'min_lon': -85.591049},
 '20070722': {'max_lat': 15.941677,
              'max_lon': -78.158283,
              'min_lat': 5.919914,
              'min_lon': -85.819702},
 '20070724': {'max_lat': 10.007858,
              'max_lon': -84.144459,
              'min_lat': 5.012512,
              'min_lon': -86.060028},
 '20070728': {'max_lat': 16.579227,
              'max_lon': -81.294193,
              'min_lat': 8.705292,
              'min_lon': -88.796482},
 '20070729': {'max_lat': 10.585155,
              'max_lon': -78.218193,
              'min_lat': -6.471634,
              'min_lon': -85.170479},
 '20070731': {'max_lat': 10.619144,
              'max_lon': -82.360039,
              'min_lat': 7.928867,
              'min_lon': -89.509563},
 '20070803': {'max_lat': 13.876762,
              'max_lon': -80.040035,
              'min_lat': 5.130787,
              'min_lon': -86.32061},
 '20070805': {'max_lat': 10.00906,
              'max_lon': -76.675987,
              'min_lat': 5.282021,
              'min_lon': -84.434395},
 '20070806': {'max_lat': 10.01215,
              'max_lon': -84.131927,
              'min_lat': -3.001328,
              'min_lon': -92.314682},
 '20070808': {'max_lat': 10.008202,
              'max_lon': -70.252419,
              'min_lat': 1.433201,
              'min_lon': -85.174427},
 '20070810': {'max_lat': 39.2169,
              'max_lon': -84.136219,
              'min_lat': 9.953098,
              'min_lon': -121.400471}
}


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.nav_lookup = nav_lookup

    def main(self):
        # Nav lookup is already done and no longer needed here, but left here
        # for reference and reuse
        #self.nav_lookup = self.build_navigation_lookup(provider_path)
        print(f"Nav data available for {list(nav_lookup.keys())}")

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
        match = re.search(r"(\d{8})(?:_\d{6})?\.txt$", filename)

        if not match:
            raise ValueError(f"Could not extract date from {filename}")

        return match.group(1)

    @staticmethod
    def read_temporal_bounds(stream) -> dict[str, Any]:
        """Read data file and compute temporal bounds (start and end date)."""

        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)

        with open_ames_1001(stream) as (header, records):

            beginning_of_day = datetime.combine(
                header.data_date,
                time.min,
                tzinfo=timezone.utc,
            )
            date_key = f"{header.data_date.year:04d}{header.data_date.month:02d}{header.data_date.day:02d}"

            for record in records:
                timestamp = beginning_of_day + timedelta(
                    seconds=record.independent
                )

                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

        temp_dict = {
            "start_time": start_time,
            "end_time": end_time,
            "base_time": header.data_date,
            "date_key": date_key,
        }
        return temp_dict

if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')