from datetime import datetime, time, timedelta, timezone
import re
from typing import Any

from utils.mdx import MDX
from utils.ames import open_ames_1001

short_name = "dc8avocettc4"
provider_path = "dc8avocettc4/"

# Most files in this collection are missing nav data,
# so coordinates from dc8navtc4 are used.
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
        self.nav_lookup = {}

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream = None) -> dict[str, Any]:

        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        max_lon = float("-inf")
        min_lon = float("inf")
        max_lat = float("-inf")
        min_lat = float("inf")

        with open_ames_1001(stream) as (header, records):
            
            midnight_utc = datetime.combine(header.data_date, time.min, tzinfo=timezone.utc)

            for record in records:
                timestamp = midnight_utc + timedelta(seconds=record.independent)
                date_str = re.search(r"(\d{8})(?:_\d{6})?\.txt$", filename).group(1)
                spatial_bounds = nav_lookup.get(date_str)
                
                min_lat = min(min_lat, spatial_bounds['min_lat'])
                max_lat = max(max_lat, spatial_bounds['max_lat'])
                min_lon = min(min_lon, spatial_bounds['min_lon'])
                max_lon = max(max_lon, spatial_bounds['max_lon'])
                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

        return {
            "start": start_time,
            "end": end_time,
            "north": max_lat,
            "south": min_lat,
            "east": max_lon,
            "west": min_lon,
            "format": "ASCII"
        }

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

if __name__ == '__main__':
    MDXProcessing().main()