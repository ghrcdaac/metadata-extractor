# create lookup zip for raxpolimpacts 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np
from math import radians, degrees, sin, cos, asin, acos, sqrt

short_name = "raxpolimpacts"
provider_path = "raxpolimpacts/"
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
        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        #Raxpol radar range ~ 75 km
        radar_range = 75.
        #Earth radius ~ 6371 km
        radar_lat = float(data['latitude'][0])
        radar_lon = float(data['longitude'][0])
        mlat = radians(radar_lat)
        mlon = radians(radar_lon)
        plat = mlat
        dlon = degrees(acos((cos(radar_range/6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
        dlat = degrees(radar_range/6371.)
        north, south, east, west = [radar_lat+dlat, radar_lat-dlat,
                                    radar_lon+dlon, radar_lon-dlon]
        start_time = datetime.strptime(data.start_time,'%Y-%m-%d %H:%M:%S.%f') #'start_time: 2022-01-29 17:22:25.000'
        end_time = datetime.strptime(data.end_time,'%Y-%m-%d %H:%M:%S.%f') #'end_time: 2022-01-29 17:22:25.006'

        data.close()
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
