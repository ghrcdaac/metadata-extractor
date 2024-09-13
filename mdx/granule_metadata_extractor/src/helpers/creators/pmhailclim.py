# create lookup zip for pmhailclim 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "pmhailclim"
provider_path = "pmhailclim/"
file_type = "netCDF-3"

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
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = nc.variables['latitude'][:]
        lon = nc.variables['longitude'][:]
        north, south, east, west = [lat.max(),lat.min(),lon.max(),lon.min()]

        sat_obs = filename.split('/')[-1].split('_')[0] #TRMM,COMBO,GPM

        #TRMM: Jan 1998 – Sept 2014
        #GPM: Apr 2014-March 2022
        #Combo: Jan 1998 – March 2022

        #Updated in Apr. 2024:
        #TRMM: Jan 1998 – Sept 2014
        #GPM: Apr 2014-March 2023
        #Combo: Jan 1998 – March 2023

        if sat_obs == 'TRMM':
           start_time = datetime(1998,1,1)
           end_time = datetime.strptime('09/30/2014 23:59:59', '%m/%d/%Y %H:%M:%S')
        elif sat_obs == 'GPM':
           start_time = datetime(2014,4,1)
           end_time = datetime.strptime('03/31/2023 23:59:59', '%m/%d/%Y %H:%M:%S')
        elif sat_obs == 'COMBO':
           start_time = datetime(1998,1,1)
           end_time = datetime.strptime('03/31/2023 23:59:59', '%m/%d/%Y %H:%M:%S')

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
