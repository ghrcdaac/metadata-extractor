"""Lookup creator for ER-2 LIP Ferry and Dry Run (er2lipfdr)."""
from datetime import datetime, timezone
from utils.mdx import MDX
from pyhdf.HDF import HDF, HC
from pyhdf.VS import VS
from utils.streams import as_local_file
from typing import Any
import math

short_name = "er2lipfdr"
provider_path = "er2lipfdr/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.lookup = {}

    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    @staticmethod
    def decode_hdf_time(t) -> datetime:
        """Decode HDF4 time to datetime."""
        sec, minute, hour, day, month, year, *_ = t

        return datetime(
            year=1900 + year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=sec,
            tzinfo=timezone.utc,
        )

    def process(self, filename: str, stream=None) -> dict[str, Any]:

        file_ext = "." + filename.split(".")[-1]
        # as_local_file wrapper saves stream to a local file if it is a stream,
        # and returns a str path.
        # Because HDF expects a C-string (char *) path, and we might receive
        # an S3StreamingBody from process_collection.
        with as_local_file(stream, file_ext) as file_path:
            hdf = HDF(file_path, HC.READ)

            try:
                vs = hdf.vstart()

                try:
                    # Read temporal metadata
                    summary = vs.attach("summary")

                    try:
                        # These HDF files are being very finicky about extracting
                        # multiple fields at once
                        summary.setfields("startIRIG_B")
                        start_irig  = summary.read(1)[0][0]
                        summary.seek(0)
                        summary.setfields("stopIRIG_B")
                        stop_irig = summary.read(1)[0][0]

                    finally:
                        summary.detach()

                    # Read spatial metadata
                    navigation = vs.attach("navigation")

                    try:
                        navigation.setfields("latitude")
                        latitudes = [rec[0] for rec in navigation.read(navigation._nrecs)]
                        navigation.seek(0)
                        navigation.setfields("longitude")
                        longitudes = [rec[0] for rec in navigation.read(navigation._nrecs)]

                    except Exception as e:
                        print(e)

                    finally:
                        navigation.detach()

                finally:
                    vs.end()

            finally:
                hdf.close()

            # Sift out invalid OR NODATA values
            coordinates = [
                (lat, lon)
                for lat, lon in zip(latitudes, longitudes)
                if (
                    math.isfinite(lat)
                    and math.isfinite(lon)
                    and -90 <= lat <= 90
                    and -180 <= lon <= 180
                    and not (lat == 0.0 and lon == 0.0)
                )
            ]

            if not coordinates:
                raise ValueError("No valid navigation coordinates found")

            latitudes, longitudes = zip(*coordinates)

            min_lat = min(latitudes)
            max_lat = max(latitudes)
            min_lon = min(longitudes)
            max_lon = max(longitudes)

            start_time = self.decode_hdf_time(start_irig)
            end_time = self.decode_hdf_time(stop_irig)

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