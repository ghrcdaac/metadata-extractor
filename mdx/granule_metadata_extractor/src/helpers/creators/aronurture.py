"""Creator for ARU NURTURE."""
from utils.mdx import MDX
from utils.streams import as_seekable_binary_stream
from datetime import datetime, timezone, timedelta
import h5netcdf
import re

short_name = "aronurture"
provider_path = "aronurture/"

GPS_EPOCH = datetime(1980, 1, 6, 0, 0, 0, tzinfo=timezone.utc)

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    @staticmethod
    def parse_geospatial_deg(value):
        match = re.match(r"^(-?\d+\.\d+)", value)
        if match:
            return float(match.group(1))
        raise ValueError(f"Not a valid latitude or longitude: {value!r}")

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        file_buffer = as_seekable_binary_stream(file_obj_stream)

        with h5netcdf.File(file_buffer, "r") as nc:
            attrs = nc.attrs

            start = GPS_EPOCH + timedelta(seconds=int(attrs["start_time"]))
            end = GPS_EPOCH + timedelta(seconds=int(attrs["end_time"]))
            north = self.parse_geospatial_deg(attrs["geospatial_lat_max"])
            south = self.parse_geospatial_deg(attrs["geospatial_lat_min"])
            east = self.parse_geospatial_deg(attrs["geospatial_lon_max"])
            west = self.parse_geospatial_deg(attrs["geospatial_lon_min"])

        return {
            'start': start,
            'end': end,
            'north': north,
            'south': south,
            'east': east,
            'west': west,
            'format': 'netCDF-4',
        }

    def main(self):
        self.process_collection(short_name, provider_path, max_concurrent=5)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()