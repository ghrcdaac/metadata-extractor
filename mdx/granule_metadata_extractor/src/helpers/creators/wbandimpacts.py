# create lookup zip for wbandimpacts 
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

short_name = "wbandimpacts"
provider_path = "wbandimpacts__1/"
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
        #if 'IMPACTS_ACHV_WBAND_RHI_UCONN20230217_014003Z_to_20230217_014205Z_dR50m.nc' in filename:
        #    start_time = datetime(2100,1,1)
        #    end_time = datetime(1900,1,1)
        #    north = -90.
        #    south = 90.
        #    east = -180.
        #    west = 180.
        #    return {
        #    "start": start_time,
        #    "end": end_time,
        #    "north": north,
        #    "south": south,
        #    "east": east,
        #    "west": west,
        #    "format": file_type
        #     }

        data = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())

        #Raxpol radar range ~ 50 km
        radar_range = 50.
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

        tkn=data['time_coverage_start'][:]
        tkn=tkn[tkn.mask==False]
        t0=[x.decode("utf-8") for x in tkn]
        t0_str = ''.join(t0) #'2023-01-25T11:46:08Z'
        start_time = datetime.strptime(t0_str,'%Y-%m-%dT%H:%M:%SZ') 


        tkn=data['time_coverage_end'][:]
        tkn=tkn[tkn.mask==False]
        t1=[x.decode("utf-8") for x in tkn]
        t1_str = ''.join(t1) #'2023-01-25T11:46:08Z'
        end_time = datetime.strptime(t1_str,'%Y-%m-%dT%H:%M:%SZ') 

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
