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

        def valid_coordinates(lat: float, lon: float) -> bool:
            return (
                    math.isfinite(lat)
                    and math.isfinite(lon)
                    and -90 <= lat <= 90
                    and -180 <= lon <= 180
            )

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

            inu_latitude_index = header.variable_names.index(
                "INU latitude (deg)"
            )
            inu_longitude_index = header.variable_names.index(
                "INU longitude (deg)"
            )
            gps_latitude_index = header.variable_names.index(
                "GPS latitude (deg)"
            )
            gps_longitude_index = header.variable_names.index(
                "GPS longitude (deg)"
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

                inu_lat = record.values[inu_latitude_index]
                inu_lon = record.values[inu_longitude_index]
                gps_lat = record.values[gps_latitude_index]
                gps_lon = record.values[gps_longitude_index]

                gps_valid = valid_coordinates(gps_lat, gps_lon)
                inu_valid = valid_coordinates(inu_lat, inu_lon)

                if gps_valid:
                    latitude = gps_lat
                    longitude = gps_lon

                    if inu_valid and (
                        abs(gps_lat - inu_lat) > 0.1
                        or abs(gps_lon - inu_lon) > 0.1
                    ):
                        print(
                            f"INU/GPS mismatch at {timestamp}: "
                            f"INU=({inu_lat}, {inu_lon}), "
                            f"GPS=({gps_lat}, {gps_lon})"
                        )

                elif inu_valid:
                    latitude = inu_lat
                    longitude = inu_lon

                else:
                    continue

                if gps_valid and inu_valid and (
                    abs(inu_lat - gps_lat) > 0.1 or abs(inu_lon - gps_lon) > 0.1
                ):
                    print(
                        f"Coordinate mismatch at {timestamp}: "
                        f"INU=({inu_lat}, {inu_lon}), "
                        f"GPS=({gps_lat}, {gps_lon})"
                    )

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