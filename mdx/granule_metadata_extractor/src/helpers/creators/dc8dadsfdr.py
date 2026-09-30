"""Lookup creator for DC-8 DADS FDR (dc8dadsfdr)."""
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
from utils.streams import as_text_stream
import re

short_name = "dc8dadsfdr"
provider_path = "dc8dadsfdr/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        self.file_type = 'ASCII'
        return self.read_metadata_ascii(filename, file_obj_stream)

    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        # C day, UTC, lat, lon, pitch, roll, wind speed
        # D wind direction, true airspeed, ground speed,
        #   true heading, drift angle, pressure altitude,
        #   radar altitude, two dew/frost-point temperatures
        # E static air temp, total air temp, IR surface temp,
        #   calculated static air temp, indicated airspeed,
        #   vertical speed, distance-to-go, time-to-go, alignment status
        # F cabin altitude, pressure, Mach, cross-track distance,
        #   desired track, track-angle error, track angle, specific humidity
        # G water-vapor partial pressure, RH wrt ice, RH wrt water,
        #   saturation vapor pressures, several solar elevation/azimuth quantities
        # H GPS UTC, GPS lat/lon, GPS altitude, GPS vertical speed, spare A/D
        # I solar angles, waypoint lat/lon, potential temperature, specific humidity

        file_buffer = as_text_stream(file_obj_stream)

        minTime = datetime.min
        maxTime = datetime.max
        north = 90.0
        south = -90.0
        east = 180.0
        west = -180.0
        for lines in file_buffer:
            pass

        return {
            "start": minTime,
            "end": maxTime,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
        }


    def main(self):
        # start_time = time.time()
        self.process_collection(short_name, provider_path)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
