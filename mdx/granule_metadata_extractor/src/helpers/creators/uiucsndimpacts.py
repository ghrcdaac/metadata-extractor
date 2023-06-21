# 2dimpacts is being used to template new approach of creating lookup zip
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

short_name = "uiucsndimpacts"
provider_path = "uiucsndimpacts/fieldCampaigns/impacts/UIUC_soundings/data/"
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
        Extract temporal and spatial metadata from netCDF-3 files
        """
        # open nc file
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        # start datetime and location information are stored in global attributes
        sttime = nc.getncattr('start_datetime')
        loc = nc.getncattr('location')
        # sounding time duration is kept in Time variable
        timebuf = np.array(nc.variables['Time'])
        # get time range
        minsec, maxsec = [np.min(timebuf), np.max(timebuf)]
        # start time string is the first string in sttime
        reftime = datetime.strptime(sttime.split()[0], "%Y-%m-%dT%H:%M:%SZ")
        start_time, end_time = [reftime + timedelta(seconds=minsec), reftime + timedelta(seconds=maxsec)]

        # get lat lon location from loc string, bounding box in this case is point
        regex = "^([0-9]{2}.[0-9]{3}).*([0-9]{2}.[0-9]{3}).*"
        ret = re.search(regex, loc)
        lat, lon = [float(ret.group(1)), float(ret.group(2))]
        # lon unit is degrees west, need to negate
        # checked all sample files, no file with degrees south or degrees east exists
        # since this is point, expand 0.01 to create a bounding box
        north, south, east, west = [lat+0.01, lat-0.01, -lon+0.01, -lon-0.01]

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
