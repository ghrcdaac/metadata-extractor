# create lookup zip for sbuskylerimpacts 2023 data 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np
from math import radians, degrees, sin, cos, asin, acos, sqrt

try:
    import pyart
except ImportError:
    pyart = None

short_name = "sbuskylerimpacts"
provider_path = "sbuskylerimpacts/fieldCampaigns/impacts/SBU_SKYLER/data/2023/"
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
        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        #SBU SKYLER radar range ~ 50 km
        #Earth radius ~ 6371 km
        mlat = radians(data.Latitude)
        mlon = radians(data.Longitude)
        plat = mlat
        dlon = degrees(acos((cos(50./6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
        dlat = degrees(50./6371.)
        north, south, east, west = [data.Latitude+dlat, data.Latitude-dlat,
                                    data.Longitude+dlon, data.Longitude-dlon]

        #Time: unit: seconds since 1970-01-01 00:00:00
        sec = np.array(data['Time'][:])
        start_time = datetime(1970,1,1) + timedelta(seconds=int(min(sec)))
        end_time = datetime(1970,1,1) + timedelta(seconds=int(max(sec)))

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
