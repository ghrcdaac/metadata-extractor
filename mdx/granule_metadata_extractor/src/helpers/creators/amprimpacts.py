# create lookup zip for amprimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "amprimpacts"
provider_path = "amprimpacts/fieldCampaigns/impacts/AMPR/data/"
file_type = "netCDF-4"


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
        return self.get_nc_metadata(filename, file_obj_stream)


    def get_nc_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['Lat'][:])
        lon = np.array(nc['Lon'][:])
        tm = np.array(nc['Time'][:])
        tm_units = nc.variables['Time'].units

        tmstr = tm_units.split()[2] + 'T' + tm_units.split()[3]
        reftm = datetime.strptime(tmstr,'%Y-%m-%dT%H:%M:%S.%f')

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time = reftm + timedelta(seconds=min(tm))
        end_time = reftm + timedelta(seconds=max(tm))

        nc.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": file_type
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
