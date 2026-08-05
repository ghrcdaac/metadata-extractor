from datetime import datetime, time, timedelta, timezone
from utils.mdx import MDX
from utils.ames import open_ames_1001
from typing import Any

short_name = "er2navtc4"
provider_path = "er2navtc4/"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.nav_lookup = {}

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream = None) -> dict[str, Any]:

        # PDF files in this dataset contain no extractable data
        if not filename.endswith('.txt'):
            return {}

        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        max_lon = float("-inf")
        min_lon = float("inf")
        max_lat = float("-inf")
        min_lat = float("inf")

        with open_ames_1001(stream) as (header, records):

            latitude_index = header.variable_names.index(
                'INU latitude (deg)'
            )
            longitude_index = header.variable_names.index(
                'INU longitude (deg)'
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
            "format": "ASCII"
        }


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')