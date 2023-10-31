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

short_name = "sbusndimpacts"
provider_path = "wrfimpacts/fieldCampaigns/impacts/SBU_Mobile_Soundings/data/"
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
        test = self.read_metadata_nc(filename, file_obj_stream)
        print(test)
        return test

    def read_metadata_nc(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-3 files
        """
        #nc = Dataset(filename)
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        time_min = nc.variables['XTIME'][:][0]
        time_att = nc.variables['XTIME'].getncattr('units')
        tkn = time_att.split()
        dtstr = tkn[2] + 'T' + tkn[3]
        lat = nc.variables['XLAT'][:]
        lon = nc.variables['XLONG'][:]

        #missing values are set to nan
        north, south, east, west = [float(np.max(lat)), float(np.min(lat)),
                                    float(np.max(lon)), float(np.min(lon))]

        start_time = datetime.strptime(dtstr, "%Y-%m-%dT%H:%M:%S") + timedelta(minutes=float(time_min))
        end_time = None
        if '_d01_' in filename or '_d02_' in filename:
            end_time = start_time + timedelta(seconds=3 * 3600 - 1)
        elif '_d03_' in filename:
            end_time = start_time + timedelta(seconds=3600 - 1)

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


    def read_metadata_nc(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from netCDF-3 files
        """
        nc = Dataset("in-mem-file", model='r', memory=file_obj_stream.read())
        date_str = filename.split('/')[-1].split('_')[2]
        reftime = datetime.strptime(date_str,"%Y%m%d")

        # get time, latitude and longitude variables
        utc_sec = nc.variables['time'][:]
        lat = nc.variables['latitude'][:]
        lon = nc.variables['longitude'][:]
        nc.close()

        # get time range
        minsec, maxsec = [int(np.min(utc_sec)),int(np.max(utc_sec))]
        start_time, end_time = [reftime + timedelta(seconds=minsec), reftime + timedelta(seconds=maxsec)]

        # get bounding box as minlat, maxlat, minlon, maxlon
        south, north, west, east = [np.min(lat), np.max(lat), np.min(lon), np.max(lon)]

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
