# create lookup zip for crsaloft
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

from netCDF4 import Dataset
import numpy as np

short_name = "crsaloft"
provider_path = "aloftcrs/"
file_type = "HDF-5"


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
        return self.get_hdf5_metadata(filename, file_obj_stream)


    def get_hdf5_metadata(self, filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from HDF-5 files
        """
        h5 = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        navgrp = h5.groups['Navigation']
        navdatagroup = navgrp.groups['Data']
        lat = navdatagroup.variables['Latitude'][:]
        lon = navdatagroup.variables['Longitude'][:]
        tgrp = h5.groups['Time']
        tdatagrp = tgrp.groups['Data']
        tm = tdatagrp.variables['TimeUTC'][:]

        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        start_time = datetime(1970,1,1) + timedelta(seconds=int(np.nanmin(tm)))
        end_time = datetime(1970,1,1) + timedelta(seconds=int(np.nanmax(tm)))

        h5.close()
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
        self.process_collection(short_name, provider_path, max_concurrent=2)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
