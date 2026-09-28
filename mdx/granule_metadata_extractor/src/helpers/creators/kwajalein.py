"""Creator for Kwajalein Polarimetric Radar (kwajalein)."""
from utils.mdx import MDX
from utils.streams import as_seekable_binary_stream
import numpy as np
from datetime import datetime, timezone, timedelta
import h5netcdf
import re
from pyproj import Geod

short_name = "kwajalein"
provider_path = "kwajalein__1/"

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
        if filename.endswith(".gz"):
            # Handle gzipped case
            gzipped = True
        else:
            gzipped = False
        if not re.search(r'\.cf(?:\.gz)?$', filename):
            return {}

        file_buffer = as_seekable_binary_stream(file_obj_stream, gzipped=gzipped)

        with h5netcdf.File(file_buffer, "r") as nc:
            attrs = nc.attrs

            # Get the radar position
            longitude = nc.variables["longitude"][()]
            latitude = nc.variables["latitude"][()]

            # Get radar range
            radar_range_gates = nc["range"].shape[0]
            # First gate is 600 m; each increments by 200 m
            radar_range_max_m = 600 + (radar_range_gates - 1) * 200

            # Get the geode
            geod = Geod(ellps="WGS84")

            # Calculate the lat and lon extents
            azimuths = np.linspace(0.0, 360.0, 3600, endpoint=False)

            lons, lats, _ = geod.fwd(
                np.full_like(azimuths, longitude),
                np.full_like(azimuths, latitude),
                azimuths,
                np.full_like(azimuths, radar_range_max_m)
            )

            north = lats.max()
            south = lats.min()
            east = lons.max()
            west = lons.min()

            start = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_start"][:]).decode("ascii"))
            end = datetime.fromisoformat(
                b"".join(nc.variables["time_coverage_end"][:]).decode("ascii"))


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