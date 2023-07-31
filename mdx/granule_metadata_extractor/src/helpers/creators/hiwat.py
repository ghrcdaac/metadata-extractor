# hiwat is being used to template new approach of creating lookup zip
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re
import os

from netCDF4 import Dataset
import numpy as np
from datetime import datetime, timedelta

short_name = "hiwat"
provider_path = "hiwat/"
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
        test = self.read_metadata_nc(filename, file_obj_stream)
        print(test)
        return test

    def read_metadata_nc(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF files
        """
        # open nc file
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        #file_path = 'wrfout_d02_2017-04-04_06-00-00'
        #nc['XTIME'].units
        #'minutes since 2017-04-02 18:00:00'

        utc_minute = nc.variables['XTIME'][:]
        time_att = nc.variables['XTIME'].getncattr('units')
        utc_ref = datetime.strptime(time_att,'minutes since %Y-%m-%d %H:%M:%S')
        lat = nc.variables['XLAT'][:]
        lon = nc.variables['XLONG'][:]

        #missing values are set to nan
        north, south, east, west = [float(np.max(lat)), float(np.min(lat)),
                                    float(np.max(lon)), float(np.min(lon))]

        start_time = utc_ref + timedelta(minutes=float(np.min(utc_minute)))
        end_time = utc_ref + timedelta(minutes=float(np.max(utc_minute)))

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
