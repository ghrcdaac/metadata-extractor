"""Lookup creator for ER-2 NAST-I FDR (er2nastifdr)."""
from utils.mdx import MDX
from utils.streams import as_text_stream
import re
import tarfile
import io
import xarray as xr
from datetime import datetime, timedelta

short_name = "er2nastifdr"
provider_path = "er2nastifdr/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process_nc_particle(self, stream):
        with xr.open_dataset(stream, engine="scipy") as ds:
            lat = ds["Latitude"]
            lon = ds["Longitude"]
            north = float(lat.max())
            south = float(lat.min())
            east = float(lon.max())
            west = float(lon.min())
            times = ds["time_offset"].values
            start = times.min().astype("datetime64[us]").item()
            end = times.max().astype("datetime64[us]").item()
            return {
                'start': start,
                'end': end,
                'north': north,
                'south': south,
                'east': east,
                'west': west,
            }

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        start = datetime.max
        end = datetime.min
        north = -90.0
        south = 90.0
        east = -180.0
        west = 180.0

        with tarfile.open(fileobj=file_obj_stream, mode="r|") as tar:
            for member in tar:
                if not member.isfile() or not member.name.lower().endswith(".nc"):
                    continue

                extracted = tar.extractfile(member)

                if extracted is None:
                    continue

                with extracted:
                    nc_bytes = io.BytesIO(extracted.read())

                part_md = self.process_nc_particle(nc_bytes)

                if part_md['start'] < start:
                    start = part_md['start']
                if part_md['end'] > end:
                    end = part_md['end']
                if part_md['north'] > north:
                    north = part_md['north']
                if part_md['south'] < south:
                    south = part_md['south']
                if part_md['east'] > east:
                    east = part_md['east']
                if part_md['west'] < west:
                    west = part_md['west']

        return {
            'start': start,
            'end': end,
            'north': north,
            'south': south,
            'east': east,
            'west': west,
            'format': 'netcdf',
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
