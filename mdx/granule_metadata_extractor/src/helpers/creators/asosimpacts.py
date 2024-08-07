# create lookup zip for asosimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "asosimpacts"
provider_path = "asosimpacts/fieldCampaigns/impacts/ASOS/data/"
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
        lat = float(nc.getncattr('site_latitude'))
        lon = float(nc.getncattr('site_longitude'))
        sec = nc['time'][:]
        ref_ymd = nc.variables['time'].getncattr('long_name').split()[2]
        start_time = datetime.strptime(ref_ymd, "%Y-%m-%d") + timedelta(seconds=float(min(sec)))
        end_time = datetime.strptime(ref_ymd, "%Y-%m-%d") + timedelta(seconds=float(max(sec)))
        north, south, east, west  = [round(lat+0.001, 3), round(lat-0.001, 3),
                                     round(lon+0.001, 3), round(lon-0.001, 3)]

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
