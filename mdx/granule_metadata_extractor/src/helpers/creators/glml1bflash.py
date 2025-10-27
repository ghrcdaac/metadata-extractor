# create lookup zip for glml1bflash 
# for all future collections
from datetime import datetime, timedelta
from .utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "glml1bflash"
provider_path = "glml1bflash/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-4"

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        if filename.endswith('nc'):
           self.file_type = "netCDF-4"
           return self.get_nc_metadata(filename, file_obj_stream)
        else: #hdf5
           self.file_type = "HDF-5"
           return self.get_h5_metadata(filename, file_obj_stream)

    def get_nc_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from netCDF-4 files
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['event_lat'][:])
        lon = np.array(nc['event_lon'][:])
        tm = np.array(nc['event_time'][:]) #seconds since Jan. 1, 2000

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time = datetime(2000,1,1) + timedelta(seconds=min(tm))
        end_time = datetime(2000,1,1) + timedelta(seconds=max(tm))

        nc.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
        }


    def get_h5_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from HDF-5 files
        """
        tkn = filename.split('/')[-1].split('.h5')[0] #GLM16_flashes_3_18
        group_name = ''.join([tkn,'_inc_SGFs']) #GLM16_flashes_3_18_inc_SGFs

        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc[group_name]['lat'][:]).flatten()
        lon = np.array(nc[group_name]['lon'][:]).flatten()
        t0 = np.array(nc[group_name]['raw_start_time'][:]).flatten() #seconds since Jan. 1, 2000
        t1 = np.array(nc[group_name]['raw_stop_time'][:]).flatten()  #seconds since Jan. 1, 2000
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time = datetime(2000,1,1) + timedelta(seconds=float(min(t0)))
        end_time = datetime(2000,1,1) + timedelta(seconds=float(max(t1)))

        nc.close()
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
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
