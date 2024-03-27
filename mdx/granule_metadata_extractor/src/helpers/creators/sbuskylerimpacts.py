# create lookup zip for parprbimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np
import pyart

short_name = "sbuskylerimpacts"
provider_path = "sbuskylerimpacts/fieldCampaigns/impacts/SBU_SKYLER/data/"
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
        Extract temporal and spatial metadata from netCDF-3 files
        """
        radar = pyart.io.read_cfradial("in-mem-file", mode='r', memory=file_obj_stream.read())

        lat = radar.gate_latitude['data'][:]
        lon = radar.gate_longitude['data'][:]
        sec = radar.time['data'][:]


        tkn = filename.split('/')[-1].split('_')
        dt0 = datetime.strptime(f"{tkn[3]}{tkn[4]}",'%Y%m%d%H%M%S')


        minTime = dt0 + timedelta(seconds=int(min(sec)))
        maxTime = dt0 + timedelta(seconds=int(max(sec)))
        north, south, east, west = [lat.max(), lat.min(), lon.max(), lon.min()]

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
